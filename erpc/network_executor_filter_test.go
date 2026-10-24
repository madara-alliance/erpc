package erpc

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
)

func TestNetworkExecutorNativeFilterDoesNotRetryOrHedge(t *testing.T) {
	logger := zerolog.Nop()
	ex, err := NewNetworkExecutor(&common.NetworkFailsafeConfig{
		Retry: &common.RetryPolicyConfig{MaxAttempts: 3},
		Hedge: &common.HedgePolicyConfig{MaxCount: 2},
	}, &logger, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"eth_newFilter", "eth_getFilterChanges", "eth_getFilterLogs", "eth_uninstallFilter"} {
		t.Run(method, func(t *testing.T) {
			req := common.NewNormalizedRequest([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":[]}`, method)))
			var calls atomic.Int32
			var sweeps atomic.Int32
			uncertain := errors.New("upstream response lost")
			forward := func(context.Context, *common.NormalizedRequest) (*common.NormalizedResponse, error) {
				calls.Add(1)
				return nil, uncertain
			}
			sweep := func(context.Context, *common.NormalizedRequest) (*common.NormalizedResponse, error) {
				sweeps.Add(1)
				return nil, errors.New("unexpected upstream sweep")
			}
			_, got := ex.Run(context.Background(), req, forward, sweep)
			if !errors.Is(got, uncertain) || calls.Load() != 1 || sweeps.Load() != 0 {
				t.Fatalf("got error %v after %d calls and %d sweeps; want one dispatch", got, calls.Load(), sweeps.Load())
			}
		})
	}
}
