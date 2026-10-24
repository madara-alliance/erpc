package upstream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/failsafe"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Internal requests (e.g. the wsupstream adapter's subscription RPCs) must not
// feed the upstream breaker; the same failure from a client request opens it.
func TestUpstreamExecutor_InternalRequestsSkipBreaker(t *testing.T) {
	lg := zerolog.Nop()
	serverErr := common.NewErrEndpointServerSideException(errors.New("boom"), nil, 500)
	inner := func(context.Context, bool) (*common.NormalizedResponse, error) { return nil, serverErr }

	run := func(internal bool) failsafe.State {
		ex, err := NewUpstreamExecutor(&common.FailsafeConfig{
			CircuitBreaker: &common.CircuitBreakerPolicyConfig{
				FailureThresholdCount:    1,
				FailureThresholdCapacity: 1,
				HalfOpenAfter:            common.Duration(time.Hour),
				SuccessThresholdCount:    1,
				SuccessThresholdCapacity: 1,
			},
		}, &lg)
		require.NoError(t, err)
		req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`))
		if internal {
			req.SetDirectives(&common.RequestDirectives{IsInternal: true})
		}
		_, err = ex.Run(context.Background(), req, inner)
		require.Error(t, err)
		return ex.Breaker().State()
	}

	require.Equal(t, failsafe.StateClosed, run(true))
	require.Equal(t, failsafe.StateOpen, run(false))
}

func TestUpstreamExecutorNativePollDoesNotRetryOrHedge(t *testing.T) {
	lg := zerolog.Nop()
	ex, err := NewUpstreamExecutor(&common.UpstreamFailsafeConfig{
		Retry: &common.RetryPolicyConfig{MaxAttempts: 3},
		Hedge: &common.HedgePolicyConfig{MaxCount: 2},
	}, &lg)
	require.NoError(t, err)
	req := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getFilterChanges","params":["0x1"]}`))
	uncertain := errors.New("response lost")
	var calls atomic.Int32
	_, got := ex.Run(context.Background(), req, func(context.Context, bool) (*common.NormalizedResponse, error) {
		calls.Add(1)
		return nil, uncertain
	})
	require.ErrorIs(t, got, uncertain)
	require.Equal(t, int32(1), calls.Load())
}
