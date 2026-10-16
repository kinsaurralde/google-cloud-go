/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/spanner/internal/testutil"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func parseChannelIDFromHeader(md metadata.MD) uint64 {
	vals := md.Get(xSpannerRequestIDHeader)
	if len(vals) == 0 {
		return 0
	}
	parts := strings.Split(vals[0], ".")
	if len(parts) < 4 {
		return 0
	}
	ch, _ := strconv.ParseUint(parts[3], 10, 64)
	return ch
}

func overrideRecoveryConfigForTest(t *testing.T, interval time.Duration, minProbes int, minDuration time.Duration, peerFn func(*peer.Peer) bool) {
	t.Helper()
	origProbeInterval := recoveryProbeInterval
	origMinProbes := recoveryMinProbes
	origMinDuration := recoveryMinDuration
	origPeerFn := isDirectPathPeerFn
	recoveryProbeInterval = interval
	recoveryMinProbes = minProbes
	recoveryMinDuration = minDuration
	if peerFn != nil {
		isDirectPathPeerFn = peerFn
	}
	t.Cleanup(func() {
		recoveryProbeInterval = origProbeInterval
		recoveryMinProbes = origMinProbes
		recoveryMinDuration = origMinDuration
		isDirectPathPeerFn = origPeerFn
	})
}

func startCloudPathTestServer(t *testing.T, spannerSrv sppb.SpannerServer, opts ...grpc.ServerOption) (string, *atomic.Int32) {
	t.Helper()
	cpGrpcServer := grpc.NewServer(opts...)
	sppb.RegisterSpannerServer(cpGrpcServer, spannerSrv)
	cpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go cpGrpcServer.Serve(cpLis)
	t.Cleanup(cpGrpcServer.Stop)

	var cloudDials atomic.Int32
	origDial := dialFallbackCloudPathFn
	dialFallbackCloudPathFn = func(ctx context.Context, dialOpts ...option.ClientOption) (gtransport.ConnPool, error) {
		cloudDials.Add(1)
		return gtransport.DialPool(ctx, append(dialOpts,
			option.WithEndpoint(cpLis.Addr().String()),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)...)
	}
	t.Cleanup(func() { dialFallbackCloudPathFn = origDial })
	return cpLis.Addr().String(), &cloudDials
}

func drainRowIterator(iter *RowIterator) error {
	defer iter.Stop()
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func TestEEF_ReadWriteTransaction_ChannelAffinity(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")

	interceptorTracker := newInterceptorTracker()
	clientOpts := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithUnaryInterceptor(interceptorTracker.unaryClientInterceptor)),
		option.WithGRPCDialOption(grpc.WithStreamInterceptor(interceptorTracker.streamClientInterceptor)),
	}

	_, client, teardown := setupMockedTestServerWithConfigAndClientOptions(t, ClientConfig{
		DisableNativeMetrics: true,
		NumChannels:          4,
	}, clientOpts)
	defer teardown()

	ctx := context.Background()
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *ReadWriteTransaction) error {
		for i := 0; i < 4; i++ {
			if err := drainRowIterator(txn.Query(ctx, Statement{SQL: testutil.SelectFooFromBar})); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReadWriteTransaction failed: %v", err)
	}

	if len(interceptorTracker.streamClientRequestIDSegments) < 4 {
		t.Fatalf("expected at least 4 streaming query RPCs, got %d", len(interceptorTracker.streamClientRequestIDSegments))
	}
	firstCh := interceptorTracker.streamClientRequestIDSegments[0].ChannelID
	if firstCh < 1 || firstCh > 4 {
		t.Fatalf("expected channelID in [1, 4], got %d", firstCh)
	}
	for _, seg := range interceptorTracker.streamClientRequestIDSegments[1:] {
		if seg.ChannelID != firstCh {
			t.Fatalf("transaction streaming queries hopped channels: first on %d, later on %d", firstCh, seg.ChannelID)
		}
	}
	commitSeg := interceptorTracker.unaryClientRequestIDSegments[len(interceptorTracker.unaryClientRequestIDSegments)-1]
	if commitSeg.ChannelID != firstCh {
		t.Fatalf("transaction Commit ran on channel %d, want %d", commitSeg.ChannelID, firstCh)
	}
}

func TestEEF_ReadWriteTransaction_MidTxFallback(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")

	var dpBroken atomic.Bool
	var dpSeenCh, cpSeenCh []uint64
	var mu sync.Mutex

	dpUnary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if dpBroken.Load() && !strings.HasSuffix(info.FullMethod, "CreateSession") {
			return nil, status.Error(codes.Unavailable, "DirectPath blackhole")
		}
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			mu.Lock()
			dpSeenCh = append(dpSeenCh, parseChannelIDFromHeader(md))
			mu.Unlock()
		}
		return handler(ctx, req)
	}
	dpStream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if dpBroken.Load() {
			return status.Error(codes.Unavailable, "DirectPath blackhole")
		}
		if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
			mu.Lock()
			dpSeenCh = append(dpSeenCh, parseChannelIDFromHeader(md))
			mu.Unlock()
		}
		return handler(srv, ss)
	}

	dpServer, clientOpts, dpTeardown := testutil.NewMockedSpannerInMemTestServer(
		t,
		grpc.ChainUnaryInterceptor(dpUnary),
		grpc.ChainStreamInterceptor(dpStream),
	)
	defer dpTeardown()

	_, cloudDials := startCloudPathTestServer(t, dpServer.TestSpanner,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				mu.Lock()
				cpSeenCh = append(cpSeenCh, parseChannelIDFromHeader(md))
				mu.Unlock()
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
				mu.Lock()
				cpSeenCh = append(cpSeenCh, parseChannelIDFromHeader(md))
				mu.Unlock()
			}
			return handler(srv, ss)
		}),
	)

	ctx := context.Background()
	client, err := NewClientWithConfig(ctx, "projects/p/instances/i/databases/d", ClientConfig{
		DisableNativeMetrics: true,
		NumChannels:          4,
	}, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if got := cloudDials.Load(); got != 0 {
		t.Fatalf("CloudPath dials at startup = %d, want 0", got)
	}

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *ReadWriteTransaction) error {
		if err := drainRowIterator(txn.Query(ctx, Statement{SQL: testutil.SelectFooFromBar})); err != nil {
			return err
		}
		if !client.sc.fallbackState.inFallback() {
			client.sc.fallbackState.primarySuccesses.Store(0)
			client.sc.fallbackState.primaryFailures.Store(1)
			client.sc.fallbackState.rateCheck()
		}
		return drainRowIterator(txn.Query(ctx, Statement{SQL: testutil.SelectFooFromBar}))
	})
	if err != nil {
		t.Fatalf("ReadWriteTransaction across mid-tx fallback failed: %v", err)
	}

	if got := cloudDials.Load(); got != 1 {
		t.Fatalf("CloudPath dials after single-slot transaction fallback = %d, want 1", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(dpSeenCh) == 0 || len(cpSeenCh) == 0 {
		t.Fatalf("expected RPCs on both DirectPath (%v) and CloudPath (%v)", dpSeenCh, cpSeenCh)
	}
	txSlotID := dpSeenCh[len(dpSeenCh)-1]
	if txSlotID == 0 {
		t.Fatalf("DirectPath transaction RPC had channelID 0: %v", dpSeenCh)
	}
	for _, ch := range cpSeenCh {
		if ch != txSlotID {
			t.Fatalf("CloudPath RPC channelID = %d, want transaction's slot %d", ch, txSlotID)
		}
	}
}

func TestEEF_DynamicChannelPool_FallbackSlot(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")

	var dpSeenCh, cpSeenCh []uint64
	var mu sync.Mutex

	dpServer, clientOpts, dpTeardown := testutil.NewMockedSpannerInMemTestServer(
		t,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				mu.Lock()
				dpSeenCh = append(dpSeenCh, parseChannelIDFromHeader(md))
				mu.Unlock()
			}
			return handler(ctx, req)
		}),
	)
	defer dpTeardown()

	startCloudPathTestServer(t, dpServer.TestSpanner,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				mu.Lock()
				cpSeenCh = append(cpSeenCh, parseChannelIDFromHeader(md))
				mu.Unlock()
			}
			return handler(ctx, req)
		}),
	)

	ctx := context.Background()
	client, err := NewClientWithConfig(ctx, "projects/p/instances/i/databases/d", ClientConfig{
		DisableNativeMetrics: true,
		DynamicChannelPoolConfig: DynamicChannelPoolConfig{
			DCPEnabled:         true,
			DCPInitialChannels: 2,
			DCPMinChannels:     1,
			DCPMaxChannels:     4,
		},
	}, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *ReadWriteTransaction) error {
		if _, err := txn.Update(ctx, Statement{SQL: testutil.UpdateBarSetFoo}); err != nil {
			return err
		}
		if !client.sc.fallbackState.inFallback() {
			client.sc.fallbackState.primarySuccesses.Store(0)
			client.sc.fallbackState.primaryFailures.Store(1)
			client.sc.fallbackState.rateCheck()
		}
		_, err := txn.Update(ctx, Statement{SQL: testutil.UpdateBarSetFoo})
		return err
	})
	if err != nil {
		t.Fatalf("DCP ReadWriteTransaction across fallback failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(cpSeenCh) < 2 {
		t.Fatalf("expected Update and Commit on CloudPath, got %v", cpSeenCh)
	}
	entryID := dpSeenCh[len(dpSeenCh)-1]
	for _, ch := range cpSeenCh {
		if ch != entryID {
			t.Fatalf("DCP CloudPath RPC used channel %d, want entry %d", ch, entryID)
		}
	}
}

func TestEEF_PerSlotProbingAndRecovery(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")
	t.Setenv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY", "true")

	overrideRecoveryConfigForTest(t, 5*time.Millisecond, 3, 10*time.Millisecond, func(*peer.Peer) bool { return true })

	var dpBroken atomic.Bool
	var dpQueries, cpQueries atomic.Int32

	dpServer, clientOpts, dpTeardown := testutil.NewMockedSpannerInMemTestServer(
		t,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if dpBroken.Load() && !strings.HasSuffix(info.FullMethod, "CreateSession") {
				return nil, status.Error(codes.Unavailable, "DirectPath down")
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if dpBroken.Load() {
				return status.Error(codes.Unavailable, "DirectPath down")
			}
			if strings.HasSuffix(info.FullMethod, "ExecuteStreamingSql") {
				dpQueries.Add(1)
			}
			return handler(srv, ss)
		}),
	)
	defer dpTeardown()

	startCloudPathTestServer(t, dpServer.TestSpanner,
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if strings.HasSuffix(info.FullMethod, "ExecuteStreamingSql") {
				cpQueries.Add(1)
			}
			return handler(srv, ss)
		}),
	)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	ctx := context.Background()
	client, err := NewClientWithConfig(ctx, "projects/p/instances/i/databases/d", ClientConfig{
		DisableNativeMetrics:       true,
		NumChannels:                1,
		OpenTelemetryMeterProvider: mp,
	}, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	runQuery := func() error {
		return drainRowIterator(client.Single().Query(ctx, Statement{SQL: testutil.SelectFooFromBar}))
	}

	if err := runQuery(); err != nil {
		t.Fatalf("initial DirectPath query failed: %v", err)
	}
	if dpQueries.Load() != 1 || cpQueries.Load() != 0 {
		t.Fatalf("counts before fallback: dp=%d cp=%d, want dp=1 cp=0", dpQueries.Load(), cpQueries.Load())
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := client.sc.fallbackState.probeSession.Load().(string); v != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	dpBroken.Store(true)
	failCtx, failCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	_ = drainRowIterator(client.Single().Query(failCtx, Statement{SQL: testutil.SelectFooFromBar}))
	failCancel()
	client.sc.fallbackState.primarySuccesses.Store(0)
	client.sc.fallbackState.rateCheck()

	if err := runQuery(); err != nil {
		t.Fatalf("CloudPath query after fallback failed: %v", err)
	}
	if cpQueries.Load() == 0 {
		t.Fatal("expected query to route to CloudPath while DirectPath is down")
	}

	dpBroken.Store(false)
	dpBeforeRecovery := dpQueries.Load()
	recovered := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := runQuery(); err != nil {
			t.Fatalf("query during recovery failed: %v", err)
		}
		if dpQueries.Load() > dpBeforeRecovery {
			recovered = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !recovered {
		t.Fatal("slot did not recover to DirectPath after DirectPath healed")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	seenMetrics := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			seenMetrics[m.Name] = true
		}
	}
	for _, wantName := range []string{metricNameEEFFallbackCount, metricNameEEFCallStatus, metricNameEEFProbeResult} {
		if !seenMetrics[wantName] {
			t.Fatalf("expected metric %q to be recorded, got %v", wantName, seenMetrics)
		}
	}
}

func TestEEF_DCP_ScaleUpWhileInFallback_RecoverAndScaleDown(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")
	t.Setenv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY", "true")

	overrideRecoveryConfigForTest(t, 5*time.Millisecond, 2, 5*time.Millisecond, func(*peer.Peer) bool { return true })

	var dpBroken atomic.Bool
	var dpRPCs, cpRPCs atomic.Int32

	dpServer, clientOpts, dpTeardown := testutil.NewMockedSpannerInMemTestServer(
		t,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if dpBroken.Load() && !strings.HasSuffix(info.FullMethod, "CreateSession") {
				return nil, status.Error(codes.Unavailable, "DirectPath down")
			}
			dpRPCs.Add(1)
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if dpBroken.Load() {
				return status.Error(codes.Unavailable, "DirectPath down")
			}
			dpRPCs.Add(1)
			return handler(srv, ss)
		}),
	)
	defer dpTeardown()
	addSelect1Result(dpServer)

	startCloudPathTestServer(t, dpServer.TestSpanner,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			cpRPCs.Add(1)
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			cpRPCs.Add(1)
			return handler(srv, ss)
		}),
	)

	ctx := context.Background()
	client, err := NewClientWithConfig(ctx, "projects/p/instances/i/databases/d", ClientConfig{
		DisableNativeMetrics: true,
		DynamicChannelPoolConfig: DynamicChannelPoolConfig{
			DCPEnabled:                           true,
			DCPInitialChannels:                   1,
			DCPMinChannels:                       1,
			DCPMaxChannels:                       4,
			DCPMinRPCPerChannel:                  1,
			DCPMaxRPCPerChannel:                  2,
			DCPScaleUpCooldown:                   1 * time.Millisecond,
			DCPDrainIdleGrace:                    1 * time.Millisecond,
			DCPDownscaleConsecutiveLowLoadChecks: 1,
		},
	}, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client.sc.dynamicPool.hasPrimeSession() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	dpBroken.Store(true)
	client.sc.fallbackState.primarySuccesses.Store(0)
	client.sc.fallbackState.primaryFailures.Store(1)
	client.sc.fallbackState.rateCheck()

	dcp := client.sc.dynamicPool
	entries := dcp.getEntries()
	entries[0].unaryLoad.Store(10)
	dcp.totalRPCLoad.Store(10)
	dcp.scaleUp()
	entries[0].unaryLoad.Store(0)
	dcp.totalRPCLoad.Store(0)

	if got := dcp.Num(); got < 2 {
		t.Fatalf("DCP Num() after scaleUp in fallback = %d, want >= 2", got)
	}
	if cpRPCs.Load() == 0 {
		t.Fatal("expected DCP channel priming during fallback to execute over CloudPath")
	}

	dpBroken.Store(false)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !client.sc.fallbackState.inFallback() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if client.sc.fallbackState.inFallback() {
		t.Fatal("expected fallbackState.inFallback() to reset to false after DCP slots recovered")
	}

	dcp.removeEntries(dcp.Num() - 1)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		client.sc.fallbackState.slotsMu.Lock()
		nSlots := len(client.sc.fallbackState.slots)
		client.sc.fallbackState.slotsMu.Unlock()
		if nSlots == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	client.sc.fallbackState.slotsMu.Lock()
	nSlots := len(client.sc.fallbackState.slots)
	client.sc.fallbackState.slotsMu.Unlock()
	if nSlots != 1 {
		t.Fatalf("registered fallbackSlots after DCP scale-down = %d, want 1", nSlots)
	}
}

func TestEEF_Stress_ConcurrentFlappingAndTransactions(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")
	t.Setenv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY", "true")

	overrideRecoveryConfigForTest(t, 3*time.Millisecond, 2, 5*time.Millisecond, func(*peer.Peer) bool { return true })

	var dpBroken atomic.Bool
	var dpCount, cpCount atomic.Int32

	dpServer, clientOpts, dpTeardown := testutil.NewMockedSpannerInMemTestServer(
		t,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if dpBroken.Load() && !strings.HasSuffix(info.FullMethod, "CreateSession") {
				return nil, status.Error(codes.Unavailable, "DirectPath down")
			}
			dpCount.Add(1)
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if dpBroken.Load() {
				return status.Error(codes.Unavailable, "DirectPath down")
			}
			dpCount.Add(1)
			return handler(srv, ss)
		}),
	)
	defer dpTeardown()

	startCloudPathTestServer(t, dpServer.TestSpanner,
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			cpCount.Add(1)
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			cpCount.Add(1)
			return handler(srv, ss)
		}),
	)

	ctx := context.Background()
	client, err := NewClientWithConfig(ctx, "projects/p/instances/i/databases/d", ClientConfig{
		DisableNativeMetrics: true,
		NumChannels:          4,
	}, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for i := 0; i < 4; i++ {
		if err := drainRowIterator(client.Single().Query(ctx, Statement{SQL: testutil.SelectFooFromBar})); err != nil {
			t.Fatalf("warmup query failed: %v", err)
		}
	}

	stopCh := make(chan struct{})
	var flapDone sync.WaitGroup
	flapDone.Add(1)
	go func() {
		defer flapDone.Done()
		for cycle := 0; cycle < 3; cycle++ {
			select {
			case <-stopCh:
				return
			default:
			}
			dpBroken.Store(true)
			client.sc.fallbackState.primarySuccesses.Store(0)
			client.sc.fallbackState.primaryFailures.Store(4)
			client.sc.fallbackState.rateCheck()
			time.Sleep(15 * time.Millisecond)

			dpBroken.Store(false)
			time.Sleep(25 * time.Millisecond)
		}
	}()

	const numWorkers = 12
	const opsPerWorker = 8
	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers*opsPerWorker)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for op := 0; op < opsPerWorker; op++ {
				var err error
				if (workerID+op)%2 == 0 {
					_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *ReadWriteTransaction) error {
						_, err := txn.Update(ctx, Statement{SQL: testutil.UpdateBarSetFoo})
						return err
					})
				} else {
					err = drainRowIterator(client.Single().Query(ctx, Statement{SQL: testutil.SelectFooFromBar}))
				}
				if err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(stopCh)
	flapDone.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent worker failed during DirectPath flapping: %v", err)
	}
	if dpCount.Load() == 0 || cpCount.Load() == 0 {
		t.Fatalf("expected RPCs on both DirectPath (%d) and CloudPath (%d)", dpCount.Load(), cpCount.Load())
	}
}

func TestEEF_RuleAndProbeEdgeCases(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY", "true")

	overrideRecoveryConfigForTest(t, time.Hour, 2, 0, nil)

	var slot2Broken atomic.Bool
	dpServer1, opts1, teardown1 := testutil.NewMockedSpannerInMemTestServer(t)
	defer teardown1()

	dpGrpc2 := grpc.NewServer(
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if slot2Broken.Load() {
				return nil, status.Error(codes.Unavailable, "slot2 DirectPath down")
			}
			return handler(ctx, req)
		}),
	)
	sppb.RegisterSpannerServer(dpGrpc2, dpServer1.TestSpanner)
	dpLis2, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go dpGrpc2.Serve(dpLis2)
	defer dpGrpc2.Stop()

	ctx := context.Background()
	pool1, err := gtransport.DialPool(ctx, opts1...)
	if err != nil {
		t.Fatal(err)
	}
	defer pool1.Close()
	pool2, err := gtransport.DialPool(ctx,
		option.WithEndpoint(dpLis2.Addr().String()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()

	state := newFallbackState("projects/p/instances/i/databases/d", nil)
	defer state.close()

	dialFallback := func(ctx context.Context) (gtransport.ConnPool, error) {
		return gtransport.DialPool(ctx, opts1...)
	}
	slot1 := newFallbackSlot(pool1, state, dialFallback)
	defer slot1.Close()
	slot2 := newFallbackSlot(pool2, state, dialFallback)
	defer slot2.Close()

	state.reportPrimaryStatus(slot1, nil)
	state.reportPrimaryStatus(slot1, status.Error(codes.Unavailable, "transient"))
	state.rateCheck()
	if state.inFallback() || slot1.isInFallback() || slot2.isInFallback() {
		t.Fatal("expected 50% error rate not to trip fallback")
	}

	state.reportPrimaryStatus(slot1, status.Error(codes.Unavailable, "outage"))
	state.rateCheck()
	if !state.inFallback() || !slot1.isInFallback() || !slot2.isInFallback() {
		t.Fatal("expected 100% error rate to trip fallback on all slots")
	}

	if got := slot1.probeDirectPath(ctx); got != codes.FailedPrecondition.String() {
		t.Fatalf("probeDirectPath with no session = %q, want %q", got, codes.FailedPrecondition.String())
	}

	sessClient := sppb.NewSpannerClient(pool1)
	sess, err := sessClient.CreateSession(ctx, &sppb.CreateSessionRequest{
		Database: "projects/p/instances/i/databases/d",
		Session:  &sppb.Session{Multiplexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	state.setProbeSession(sess.GetName())

	isDirectPathPeerFn = func(*peer.Peer) bool { return true }
	if slot1.probeOnce(ctx) {
		t.Fatal("expected first probe not to recover when minProbes=2")
	}
	if slot1.consecutiveProbes != 1 {
		t.Fatalf("slot1.consecutiveProbes = %d, want 1", slot1.consecutiveProbes)
	}
	isDirectPathPeerFn = func(*peer.Peer) bool { return false }
	if got := slot1.probeDirectPath(ctx); got != "NOT_DIRECTPATH" {
		t.Fatalf("probeDirectPath without ALTS = %q, want NOT_DIRECTPATH", got)
	}
	slot1.probeOnce(ctx)
	if slot1.consecutiveProbes != 0 {
		t.Fatalf("slot1.consecutiveProbes after NOT_DIRECTPATH = %d, want 0", slot1.consecutiveProbes)
	}

	isDirectPathPeerFn = func(*peer.Peer) bool { return true }
	slot2Broken.Store(true)
	slot1.probeOnce(ctx)
	if !slot1.probeOnce(ctx) {
		t.Fatal("expected slot1 to recover on 2nd consecutive probe")
	}
	slot2.probeOnce(ctx)
	if slot1.isInFallback() {
		t.Fatal("expected slot1 to have recovered to DirectPath")
	}
	if !slot2.isInFallback() {
		t.Fatal("expected slot2 to remain in fallback while its probes fail")
	}
	if state.inFallback() {
		t.Fatal("expected state.inFallback() to reset to false on first recovered slot")
	}

	slot2Broken.Store(false)
	if slot2.probeOnce(ctx) || slot2.consecutiveProbes != 1 {
		t.Fatalf("expected slot2 to be at 1/2 probes, got %d", slot2.consecutiveProbes)
	}
	state.reportPrimaryStatus(slot1, status.Error(codes.Unavailable, "outage again"))
	state.rateCheck()
	if !state.inFallback() || !slot1.isInFallback() || !slot2.isInFallback() {
		t.Fatal("expected rateCheck during partial recovery to put slot1 back into fallback")
	}
	if slot2.consecutiveProbes != 1 {
		t.Fatalf("expected slot2 probe progress to be preserved (1), got %d", slot2.consecutiveProbes)
	}
}
