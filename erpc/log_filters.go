package erpc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
)

type sharedLogFilterBypassKey struct{}

func bypassSharedLogFilter(ctx context.Context) bool {
	value, _ := ctx.Value(sharedLogFilterBypassKey{}).(bool)
	return value
}

func newPublicFilterID() (string, error) {
	buf := make([]byte, 28)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate public filter ID: %w", err)
	}
	return "0x65727063" + hex.EncodeToString(buf), nil // "erpc" marker, then 224 random bits
}

func isPublicLogFilterID(id string) bool {
	id = strings.ToLower(id)
	if len(id) != 66 || !strings.HasPrefix(id, "0x65727063") {
		return false
	}
	_, err := hex.DecodeString(id[2:])
	return err == nil
}

func filterLogID(id string) string {
	hash := sha256.Sum256([]byte(id))
	return hex.EncodeToString(hash[:8])
}

func (n *Network) forwardSharedLogFilter(ctx context.Context, req *common.NormalizedRequest, method string) (*common.NormalizedResponse, error) {
	if strings.EqualFold(method, "eth_newFilter") {
		return n.createSharedLogFilter(ctx, req)
	}
	jreq, err := req.JsonRpcRequest(ctx)
	if err != nil {
		return nil, err
	}
	if len(jreq.Params) == 1 {
		if id, ok := jreq.Params[0].(string); ok && !isPublicLogFilterID(id) {
			// Block and pending filters remain provider-native until they have
			// their own recovery semantics.
			return n.Forward(context.WithValue(ctx, sharedLogFilterBypassKey{}, true), req)
		}
	}
	return n.callSharedLogFilter(ctx, req, method)
}

func (n *Network) createSharedLogFilter(ctx context.Context, req *common.NormalizedRequest) (*common.NormalizedResponse, error) {
	jreq, err := req.JsonRpcRequest(ctx)
	if err != nil {
		return nil, err
	}
	if len(jreq.Params) != 1 {
		return nil, errors.New("eth_newFilter requires one filter criteria object")
	}
	criteria, err := json.Marshal(jreq.Params[0])
	if err != nil {
		return nil, fmt.Errorf("encode log filter criteria: %w", err)
	}
	publicID, err := newPublicFilterID()
	if err != nil {
		return nil, err
	}
	// Preserve the existing policy's creation choice; only the returned ID is virtual.
	resp, err := n.Forward(context.WithValue(ctx, sharedLogFilterBypassKey{}, true), req)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Upstream() == nil {
		return nil, errors.New("eth_newFilter returned without an upstream")
	}
	jresp, err := resp.JsonRpcResponse(ctx)
	if err != nil {
		return nil, fmt.Errorf("parse native log filter ID: %w", err)
	}
	var nativeID string
	if err := json.Unmarshal(jresp.GetResultBytes(), &nativeID); err != nil || nativeID == "" {
		return nil, errors.New("eth_newFilter returned an invalid native filter ID")
	}
	rec := data.LogFilterRecord{
		ID: publicID, ProjectID: n.projectId, NetworkID: n.networkId, CallerID: req.UserId(),
		UpstreamID: resp.UpstreamId(), NativeID: nativeID, Criteria: criteria,
	}
	if err := n.logFilterStore.Create(ctx, rec); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cleanupReq := common.NewNormalizedRequestFromJsonRpcRequest(common.NewJsonRpcRequest("eth_uninstallFilter", []interface{}{nativeID}))
		cleanupReq.SetNetwork(n)
		_, _ = resp.Upstream().Forward(cleanupCtx, cleanupReq, false, false)
		return nil, fmt.Errorf("store log filter before returning ID: %w", err)
	}
	result, _ := json.Marshal(publicID)
	jresp.SetResult(result)
	n.logger.Info().Str("filter", filterLogID(publicID)).Str("upstreamId", rec.UpstreamID).Msg("shared log filter created")
	return resp, nil
}

func (n *Network) callSharedLogFilter(ctx context.Context, req *common.NormalizedRequest, method string) (*common.NormalizedResponse, error) {
	if err := n.acquireRateLimitPermit(ctx, req); err != nil {
		return nil, err
	}
	if err := n.prepareRequest(ctx, req); err != nil {
		return nil, err
	}
	jreq, err := req.JsonRpcRequest(ctx)
	if err != nil {
		return nil, err
	}
	if len(jreq.Params) != 1 {
		return nil, fmt.Errorf("%s requires one filter ID", method)
	}
	publicID, ok := jreq.Params[0].(string)
	if !ok || publicID == "" {
		return nil, fmt.Errorf("%s requires a string filter ID", method)
	}
	publicID = strings.ToLower(publicID)
	consuming := !strings.EqualFold(method, "eth_getFilterLogs")
	rec, err := n.logFilterStore.Acquire(ctx, publicID, n.projectId, n.networkId, req.UserId(), consuming)
	if err != nil {
		return nil, fmt.Errorf("shared log filter unavailable: %w", err)
	}
	var target common.Upstream
	for _, upstream := range n.upstreamsRegistry.GetNetworkUpstreams(ctx, n.networkId) {
		if upstream.Id() == rec.UpstreamID {
			target = upstream
			break
		}
	}
	if target == nil {
		_ = n.logFilterStore.Release(ctx, rec, true)
		return nil, fmt.Errorf("log filter upstream %q is no longer configured", rec.UpstreamID)
	}
	// The Mongo lease lasts 45s. End this RPC well before expiry so another pod
	// cannot start a concurrent consuming poll while this request is live.
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	callCtx = common.WithWireParams(callCtx, req, []interface{}{rec.NativeID})
	resp, callErr := n.doForward(callCtx, target, req, true, false)
	if callErr == nil && resp == nil {
		callErr = errors.New("bound upstream returned no filter response")
	}
	if strings.EqualFold(method, "eth_uninstallFilter") && callErr == nil {
		if err := n.logFilterStore.Delete(ctx, rec); err != nil {
			return nil, fmt.Errorf("uninstall filter binding: %w", err)
		}
	} else {
		uncertain := callErr != nil && !strings.EqualFold(method, "eth_getFilterLogs")
		if err := n.logFilterStore.Release(ctx, rec, uncertain); err != nil {
			return nil, errors.Join(callErr, fmt.Errorf("release shared log filter: %w", err))
		}
	}
	if callErr != nil {
		return nil, fmt.Errorf("%s on bound upstream %q: %w", method, rec.UpstreamID, callErr)
	}
	// Keep the native ID out of INFO logs and response metadata.
	n.logger.Info().Str("filter", filterLogID(publicID)).Str("upstreamId", rec.UpstreamID).
		Int64("generation", rec.Generation).Str("method", method).Msg("shared log filter call")
	return resp, nil
}
