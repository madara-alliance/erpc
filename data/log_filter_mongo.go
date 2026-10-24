package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var (
	ErrLogFilterUnavailable = errors.New("log filter missing, expired, uncertain, or busy")
	ErrLogFilterLeaseLost   = errors.New("log filter lease lost")
)

// LogFilterRecord binds a public ID to exactly one provider-owned native filter.
// The scope prevents one project, network, or authenticated caller from using
// another caller's filter even if a public ID is disclosed.
type LogFilterRecord struct {
	ID         string    `bson:"_id"`
	ProjectID  string    `bson:"projectId"`
	NetworkID  string    `bson:"networkId"`
	CallerID   string    `bson:"callerId"`
	UpstreamID string    `bson:"upstreamId"`
	NativeID   string    `bson:"nativeId"`
	Criteria   []byte    `bson:"criteria"`
	CreatedAt  time.Time `bson:"createdAt"`
	ExpiresAt  time.Time `bson:"expiresAt"`
	Generation int64     `bson:"generation"`
	LeaseToken string    `bson:"leaseToken"`
	LeaseUntil time.Time `bson:"leaseUntil"`
	Uncertain  bool      `bson:"uncertain"`
}

type LogFilterStore interface {
	Create(context.Context, LogFilterRecord) error
	Acquire(context.Context, string, string, string, string, bool) (LogFilterRecord, error)
	Release(context.Context, LogFilterRecord, bool) error
	Delete(context.Context, LogFilterRecord) error
	Close(context.Context) error
}

type MongoLogFilterStore struct {
	client *mongo.Client
	items  *mongo.Collection
	ttl    time.Duration
}

// NewMongoLogFilterStore requires majority-acknowledged writes. A filter ID is
// returned to the caller only after its binding is durable in MongoDB.
func NewMongoLogFilterStore(ctx context.Context, uri, database, collection string, ttl time.Duration) (*MongoLogFilterStore, error) {
	if uri == "" || database == "" || collection == "" || ttl < time.Minute {
		return nil, errors.New("invalid MongoDB log filter store configuration")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri).
		SetServerSelectionTimeout(3 * time.Second).
		SetWriteConcern(writeconcern.Majority()).
		SetReadConcern(readconcern.Majority()))
	if err != nil {
		return nil, fmt.Errorf("create MongoDB log filter client: %T", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("MongoDB log filter store unavailable: %T", err)
	}
	store := &MongoLogFilterStore{client: client, items: client.Database(database).Collection(collection), ttl: ttl}
	_, err = store.items.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expiresAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	})
	if err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("create MongoDB log filter TTL index: %T", err)
	}
	return store, nil
}

func (s *MongoLogFilterStore) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

func (s *MongoLogFilterStore) Create(ctx context.Context, rec LogFilterRecord) error {
	now := time.Now().UTC()
	rec.CreatedAt = now
	rec.ExpiresAt = now.Add(s.ttl)
	rec.LeaseUntil = time.Unix(0, 0).UTC()
	_, err := s.items.InsertOne(ctx, rec)
	if err != nil {
		return fmt.Errorf("persist log filter binding: %w", err)
	}
	return nil
}

// Acquire atomically fences concurrent operations on one filter. The caller
// must bound its upstream RPC below the lease duration and release afterward.
func (s *MongoLogFilterStore) Acquire(ctx context.Context, id, projectID, networkID, callerID string, consuming bool) (LogFilterRecord, error) {
	var rec LogFilterRecord
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return rec, fmt.Errorf("generate log filter lease token: %w", err)
	}
	filter := bson.M{
		"_id": id, "projectId": projectID, "networkId": networkID, "callerId": callerID,
		"uncertain": false,
		"$expr": bson.M{"$and": bson.A{
			bson.M{"$gt": bson.A{"$expiresAt", "$$NOW"}},
			bson.M{"$lte": bson.A{"$leaseUntil", "$$NOW"}},
		}},
	}
	update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.M{
		"leaseToken": hex.EncodeToString(tokenBytes),
		"leaseUntil": bson.M{"$dateAdd": bson.M{"startDate": "$$NOW", "unit": "second", "amount": 45}},
		"uncertain":  consuming,
		"generation": bson.M{"$add": bson.A{"$generation", 1}},
	}}}}
	err := s.items.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&rec)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return rec, ErrLogFilterUnavailable
	}
	if err != nil {
		return rec, fmt.Errorf("acquire log filter lease: %w", err)
	}
	return rec, nil
}

// Release only succeeds for the current generation. A pod paused past lease
// expiry cannot overwrite the next owner's state.
func (s *MongoLogFilterStore) Release(ctx context.Context, rec LogFilterRecord, uncertain bool) error {
	set := bson.M{
		"leaseToken": "", "leaseUntil": time.Unix(0, 0).UTC(),
		"expiresAt": bson.M{"$dateAdd": bson.M{"startDate": "$$NOW", "unit": "millisecond", "amount": s.ttl.Milliseconds()}},
		"uncertain": uncertain,
	}
	result, err := s.items.UpdateOne(ctx, bson.M{"_id": rec.ID, "generation": rec.Generation, "leaseToken": rec.LeaseToken}, mongo.Pipeline{bson.D{{Key: "$set", Value: set}}})
	if err != nil {
		return fmt.Errorf("release log filter lease: %w", err)
	}
	if result.MatchedCount != 1 {
		return ErrLogFilterLeaseLost
	}
	return nil
}

func (s *MongoLogFilterStore) Delete(ctx context.Context, rec LogFilterRecord) error {
	result, err := s.items.DeleteOne(ctx, bson.M{"_id": rec.ID, "generation": rec.Generation, "leaseToken": rec.LeaseToken})
	if err != nil {
		return fmt.Errorf("delete log filter binding: %w", err)
	}
	if result.DeletedCount != 1 {
		return ErrLogFilterLeaseLost
	}
	return nil
}
