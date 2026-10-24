package common

import (
	"testing"
	"time"
)

func TestMongoLogFilterConfigDefaultsAndValidation(t *testing.T) {
	cfg := &DatabaseConfig{LogFilters: &MongoLogFilterConfig{URIEnv: "ERPC_FILTER_MONGODB_URI", Database: "erpc"}}
	if err := cfg.SetDefaults("test"); err != nil {
		t.Fatal(err)
	}
	if cfg.LogFilters.Collection != "erpc_log_filters" || cfg.LogFilters.TTL.Duration() != 24*time.Hour {
		t.Fatalf("unexpected log filter defaults: %+v", cfg.LogFilters)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.LogFilters.URIEnv = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("missing MongoDB URI environment variable name was accepted")
	}
}
