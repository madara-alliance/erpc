package erpc

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/health"
	"github.com/erpc/erpc/thirdparty"
	"github.com/erpc/erpc/upstream"
	"github.com/erpc/erpc/util"
	"github.com/h2non/gock"
	"github.com/rs/zerolog/log"
)

func init() { util.ConfigureTestLogger() }

func TestSharedLogFilterSurvivesPodChange(t *testing.T) {
	uri := os.Getenv("ERPC_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("set ERPC_TEST_MONGODB_URI to run the MongoDB integration test")
	}
	util.ResetGock()
	defer util.ResetGock()
	util.SetupMocksForEvmStatePoller()
	var nativePolls atomic.Int32
	var nativeReads atomic.Int32
	var nativeUninstalls atomic.Int32
	gock.New("http://rpc1.localhost").Post("").
		Filter(func(r *http.Request) bool { return strings.Contains(util.SafeReadBody(r), `"method":"eth_newFilter"`) }).
		Times(1).Reply(200).JSON(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": "0xnative"})
	gock.New("http://rpc1.localhost").Post("").
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			if strings.Contains(body, `"method":"eth_getFilterChanges"`) && strings.Contains(body, `"0xnative"`) {
				nativePolls.Add(1)
				return true
			}
			return false
		}).Times(1).Reply(200).JSON(map[string]interface{}{"jsonrpc": "2.0", "id": 2, "result": []interface{}{}})
	gock.New("http://rpc1.localhost").Post("").
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			if strings.Contains(body, `"method":"eth_getFilterLogs"`) && strings.Contains(body, `"0xnative"`) {
				nativeReads.Add(1)
				return true
			}
			return false
		}).Times(1).Reply(200).JSON(map[string]interface{}{"jsonrpc": "2.0", "id": 3, "result": []interface{}{}})
	gock.New("http://rpc1.localhost").Post("").
		Filter(func(r *http.Request) bool {
			body := util.SafeReadBody(r)
			if strings.Contains(body, `"method":"eth_uninstallFilter"`) && strings.Contains(body, `"0xnative"`) {
				nativeUninstalls.Add(1)
				return true
			}
			return false
		}).Times(1).Reply(200).JSON(map[string]interface{}{"jsonrpc": "2.0", "id": 4, "result": true})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, err := data.NewMongoLogFilterStore(ctx, uri, "erpc_test", "filters", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close(context.Background()) }()
	b, err := data.NewMongoLogFilterStore(ctx, uri, "erpc_test", "filters", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close(context.Background()) }()
	upcfg := []*common.UpstreamConfig{
		{Id: "alchemy", Type: common.UpstreamTypeEvm, Endpoint: "http://rpc1.localhost", Evm: &common.EvmUpstreamConfig{ChainId: 123}},
		{Id: "infura", Type: common.UpstreamTypeEvm, Endpoint: "http://rpc2.localhost", Evm: &common.EvmUpstreamConfig{ChainId: 123}},
	}
	newNetwork := func(store data.LogFilterStore) *Network {
		cfg := &common.NetworkConfig{Architecture: common.ArchitectureEvm, Evm: &common.EvmNetworkConfig{ChainId: 123, SharedLogFilters: true}}
		if err := cfg.SetDefaults(upcfg, nil); err != nil {
			t.Fatal(err)
		}
		rates, err := upstream.NewRateLimitersRegistry(ctx, &common.RateLimiterConfig{}, &log.Logger)
		if err != nil {
			t.Fatal(err)
		}
		tracker := health.NewTracker(&log.Logger, "test", time.Minute)
		vendors := thirdparty.NewVendorsRegistry()
		providers, err := thirdparty.NewProvidersRegistry(&log.Logger, vendors, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		sharedCfg := &common.SharedStateConfig{Connector: &common.ConnectorConfig{Driver: common.DriverMemory, Memory: &common.MemoryConnectorConfig{MaxItems: 10000, MaxTotalSize: "100MB"}}}
		if err := sharedCfg.SetDefaults("test"); err != nil {
			t.Fatal(err)
		}
		shared, err := data.NewSharedStateRegistry(ctx, &log.Logger, sharedCfg)
		if err != nil {
			t.Fatal(err)
		}
		registry := upstream.NewUpstreamsRegistry(ctx, &log.Logger, "test", upcfg, shared, rates, vendors, providers, nil, tracker, nil)
		n, err := NewNetwork(ctx, &log.Logger, "test", cfg, rates, registry, tracker, nil)
		if err != nil {
			t.Fatal(err)
		}
		registry.Bootstrap(ctx)
		if err := registry.PrepareUpstreamsForNetwork(ctx, n.networkId); err != nil {
			t.Fatal(err)
		}
		if err := n.Bootstrap(ctx); err != nil {
			t.Fatal(err)
		}
		registry.OverrideOrderForTest(n.networkId)
		n.logFilterStore = store
		return n
	}
	podA := newNetwork(a)
	podB := newNetwork(b)
	create := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{"address":"0x1"}]}`))
	create.SetDirectives(&common.RequestDirectives{UseUpstream: "alchemy"})
	created, err := podA.Forward(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	jcreated, err := created.JsonRpcResponse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var publicID string
	if err := json.Unmarshal(jcreated.GetResultBytes(), &publicID); err != nil || publicID == "" || publicID == "0xnative" {
		t.Fatalf("invalid public ID %q: %v", publicID, err)
	}
	pollBody, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 2, "method": "eth_getFilterChanges", "params": []string{publicID}})
	if _, err := podB.Forward(ctx, common.NewNormalizedRequest(pollBody)); err != nil {
		t.Fatal(err)
	}
	if nativePolls.Load() != 1 {
		t.Fatalf("pod B did not poll the saved native filter: %d", nativePolls.Load())
	}
	filterCall := func(pod *Network, id int, method string) error {
		body, err := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": id, "method": method, "params": []string{publicID}})
		if err != nil {
			return err
		}
		_, err = pod.Forward(ctx, common.NewNormalizedRequest(body))
		return err
	}
	if err := filterCall(podA, 3, "eth_getFilterLogs"); err != nil {
		t.Fatal(err)
	}
	if nativeReads.Load() != 1 {
		t.Fatalf("pod A did not read the saved native filter: %d", nativeReads.Load())
	}
	if err := filterCall(podB, 4, "eth_uninstallFilter"); err != nil {
		t.Fatal(err)
	}
	if nativeUninstalls.Load() != 1 {
		t.Fatalf("pod B did not uninstall the saved native filter: %d", nativeUninstalls.Load())
	}
	if err := filterCall(podA, 5, "eth_getFilterChanges"); err == nil {
		t.Fatal("poll after uninstall succeeded")
	}
}
