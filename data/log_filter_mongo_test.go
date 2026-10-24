package data

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestMongoLogFilterStoreCrossPodLease(t *testing.T) {
	uri := os.Getenv("ERPC_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("set ERPC_TEST_MONGODB_URI to run the MongoDB integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	collection := "erpc_log_filter_test_" + time.Now().UTC().Format("20060102150405")
	a, err := NewMongoLogFilterStore(ctx, uri, "erpc_test", collection, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close(context.Background()) }()
	b, err := NewMongoLogFilterStore(ctx, uri, "erpc_test", collection, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close(context.Background()) }()
	defer func() { _ = a.items.Drop(context.Background()) }()

	record := LogFilterRecord{ID: "0xtest", ProjectID: "p", NetworkID: "evm:1", CallerID: "u", UpstreamID: "alchemy", NativeID: "0xnative", Criteria: []byte(`{"address":"0x1"}`)}
	if err := a.Create(ctx, record); err != nil {
		t.Fatal(err)
	}
	first, err := a.Acquire(ctx, record.ID, "p", "evm:1", "u", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.NativeID != record.NativeID || first.UpstreamID != record.UpstreamID {
		t.Fatalf("binding changed across pod: %+v", first)
	}
	if _, err := b.Acquire(ctx, record.ID, "p", "evm:1", "u", true); !errors.Is(err, ErrLogFilterUnavailable) {
		t.Fatalf("concurrent pod acquired leased filter: %v", err)
	}
	if err := a.Release(ctx, first, false); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(ctx, record.ID, "p", "evm:1", "other", true); !errors.Is(err, ErrLogFilterUnavailable) {
		t.Fatalf("other tenant acquired filter: %v", err)
	}
	second, err := b.Acquire(ctx, record.ID, "p", "evm:1", "u", true)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf("generation did not advance: %d -> %d", first.Generation, second.Generation)
	}
	if err := a.Release(ctx, first, false); !errors.Is(err, ErrLogFilterLeaseLost) {
		t.Fatalf("stale pod changed filter: %v", err)
	}
	if err := b.Release(ctx, second, true); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Acquire(ctx, record.ID, "p", "evm:1", "u", true); !errors.Is(err, ErrLogFilterUnavailable) {
		t.Fatalf("uncertain poll was silently retried: %v", err)
	}
}
