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

// TestEEF_ReadWriteTransaction_ChannelAffinity verifies that with DirectPath and EEF
// enabled, all RPCs within a single ReadWriteTransaction stay pinned to the same
// non-zero channel slot (Stage 2).
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
			iter := txn.Query(ctx, Statement{SQL: testutil.SelectFooFromBar})
			for {
				_, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					iter.Stop()
					return err
				}
			}
			iter.Stop()
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

// TestEEF_ReadWriteTransaction_MidTxFallback verifies that:
// 1. CloudPath is NOT dialed at startup before fallback occurs.
// 2. When DirectPath fails mid-transaction and fallback trips, the in-flight
//    ReadWriteTransaction switches to CloudPath on the same slot and preserves
//    its channelID for Commit (Stage 3).
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

	cpGrpcServer := grpc.NewServer(
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
	sppb.RegisterSpannerServer(cpGrpcServer, dpServer.TestSpanner)
	cpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go cpGrpcServer.Serve(cpLis)
	defer cpGrpcServer.Stop()

	var cloudDials atomic.Int32
	origDial := dialFallbackCloudPathFn
	dialFallbackCloudPathFn = func(ctx context.Context, opts ...option.ClientOption) (gtransport.ConnPool, error) {
		cloudDials.Add(1)
		return gtransport.DialPool(ctx, append(opts,
			option.WithEndpoint(cpLis.Addr().String()),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)...)
	}
	defer func() { dialFallbackCloudPathFn = origDial }()

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
		// Step 1: First query runs on DirectPath and establishes the transaction on a slot.
		iter := txn.Query(ctx, Statement{SQL: testutil.SelectFooFromBar})
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				return err
			}
		}
		iter.Stop()

		// Step 2: Simulate DirectPath failing and fallback window tripping mid-transaction.
		if !client.sc.fallbackState.inFallback() {
			client.sc.fallbackState.primarySuccesses.Store(0)
			client.sc.fallbackState.primaryFailures.Store(1)
			client.sc.fallbackState.rateCheck()
		}

		// Step 3: Second query + Commit run on the slot's lazily-dialed CloudPath connection.
		iter2 := txn.Query(ctx, Statement{SQL: testutil.SelectFooFromBar})
		for {
			_, err := iter2.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter2.Stop()
				return err
			}
		}
		iter2.Stop()
		return nil
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

// TestEEF_DynamicChannelPool_FallbackSlot verifies that Dynamic Channel Pooling
// wraps its connections in fallbackSlots sharing sc.fallbackState and keeps
// transactions on the same DCP entry across fallback.
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

	cpGrpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				mu.Lock()
				cpSeenCh = append(cpSeenCh, parseChannelIDFromHeader(md))
				mu.Unlock()
			}
			return handler(ctx, req)
		}),
	)
	sppb.RegisterSpannerServer(cpGrpcServer, dpServer.TestSpanner)
	cpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go cpGrpcServer.Serve(cpLis)
	defer cpGrpcServer.Stop()

	origDial := dialFallbackCloudPathFn
	dialFallbackCloudPathFn = func(ctx context.Context, opts ...option.ClientOption) (gtransport.ConnPool, error) {
		return gtransport.DialPool(ctx, append(opts,
			option.WithEndpoint(cpLis.Addr().String()),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)...)
	}
	defer func() { dialFallbackCloudPathFn = origDial }()

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

// TestEEF_PerSlotProbingAndRecovery verifies Phase 3 of the design doc:
// when GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY=true, slots enter
// fallback together, probe their DirectPath connection using GetSession on the
// client's multiplexed session, emit eef.probe_result / eef.fallback_count /
// eef.call_status metrics, and recover back to DirectPath after reaching the
// probe threshold.
func TestEEF_PerSlotProbingAndRecovery(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")
	t.Setenv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY", "true")

	origProbeInterval := recoveryProbeInterval
	origMinProbes := recoveryMinProbes
	origMinDuration := recoveryMinDuration
	recoveryProbeInterval = 5 * time.Millisecond
	recoveryMinProbes = 3
	recoveryMinDuration = 10 * time.Millisecond
	defer func() {
		recoveryProbeInterval = origProbeInterval
		recoveryMinProbes = origMinProbes
		recoveryMinDuration = origMinDuration
	}()

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

	cpGrpcServer := grpc.NewServer(
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if strings.HasSuffix(info.FullMethod, "ExecuteStreamingSql") {
				cpQueries.Add(1)
			}
			return handler(srv, ss)
		}),
	)
	sppb.RegisterSpannerServer(cpGrpcServer, dpServer.TestSpanner)
	cpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go cpGrpcServer.Serve(cpLis)
	defer cpGrpcServer.Stop()

	origDial := dialFallbackCloudPathFn
	dialFallbackCloudPathFn = func(ctx context.Context, opts ...option.ClientOption) (gtransport.ConnPool, error) {
		return gtransport.DialPool(ctx, append(opts,
			option.WithEndpoint(cpLis.Addr().String()),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)...)
	}
	defer func() { dialFallbackCloudPathFn = origDial }()

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
		iter := client.Single().Query(ctx, Statement{SQL: testutil.SelectFooFromBar})
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

	// 1. Healthy DirectPath query succeeds and ensures multiplexed session is ready.
	if err := runQuery(); err != nil {
		t.Fatalf("initial DirectPath query failed: %v", err)
	}
	if dpQueries.Load() != 1 || cpQueries.Load() != 0 {
		t.Fatalf("counts before fallback: dp=%d cp=%d, want dp=1 cp=0", dpQueries.Load(), cpQueries.Load())
	}

	// Wait for the background multiplexed session creation to populate probeSession.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := client.sc.fallbackState.probeSession.Load().(string); v != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 2. Break DirectPath, fail a query, and trigger rateCheck -> fallback to CloudPath.
	dpBroken.Store(true)
	failCtx, failCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	iter := client.Single().Query(failCtx, Statement{SQL: testutil.SelectFooFromBar})
	_, _ = iter.Next()
	iter.Stop()
	failCancel()
	client.sc.fallbackState.primarySuccesses.Store(0)
	client.sc.fallbackState.rateCheck()

	if err := runQuery(); err != nil {
		t.Fatalf("CloudPath query after fallback failed: %v", err)
	}
	if cpQueries.Load() == 0 {
		t.Fatal("expected query to route to CloudPath while DirectPath is down")
	}

	// 3. Heal DirectPath and wait for the slot's GetSession probes to recover it.
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

	// Verify eef.fallback_count, eef.call_status, and eef.probe_result metrics were recorded.
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

// TestEEF_DCP_ScaleUpWhileInFallback_RecoverAndScaleDown verifies the full
// interaction between Dynamic Channel Pooling and EEF fallback/recovery:
//  1. When fallback trips, scaling up a new DCP channel while DirectPath is
//     down succeeds because the new slot enters fallback immediately and primes
//     over CloudPath.
//  2. When DirectPath heals, both initial and scaled-up DCP slots recover to
//     DirectPath and reset fallbackState.inFallback() to false.
//  3. Subsequent DCP scale-up starts directly on DirectPath, and DCP scale-down
//     cleanly closes and unregisters drained fallbackSlots.
func TestEEF_DCP_ScaleUpWhileInFallback_RecoverAndScaleDown(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")
	t.Setenv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY", "true")

	origProbeInterval := recoveryProbeInterval
	origMinProbes := recoveryMinProbes
	origMinDuration := recoveryMinDuration
	recoveryProbeInterval = 5 * time.Millisecond
	recoveryMinProbes = 2
	recoveryMinDuration = 5 * time.Millisecond
	defer func() {
		recoveryProbeInterval = origProbeInterval
		recoveryMinProbes = origMinProbes
		recoveryMinDuration = origMinDuration
	}()

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

	cpGrpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			cpRPCs.Add(1)
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			cpRPCs.Add(1)
			return handler(srv, ss)
		}),
	)
	sppb.RegisterSpannerServer(cpGrpcServer, dpServer.TestSpanner)
	cpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go cpGrpcServer.Serve(cpLis)
	defer cpGrpcServer.Stop()

	origDial := dialFallbackCloudPathFn
	dialFallbackCloudPathFn = func(ctx context.Context, opts ...option.ClientOption) (gtransport.ConnPool, error) {
		return gtransport.DialPool(ctx, append(opts,
			option.WithEndpoint(cpLis.Addr().String()),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)...)
	}
	defer func() { dialFallbackCloudPathFn = origDial }()

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

	// Wait for multiplexed session to populate primeSession and probeSession.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client.sc.dynamicPool.hasPrimeSession() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 1. Break DirectPath and trip fallback on the initial DCP slot.
	dpBroken.Store(true)
	client.sc.fallbackState.primarySuccesses.Store(0)
	client.sc.fallbackState.primaryFailures.Store(1)
	client.sc.fallbackState.rateCheck()

	// 2. Simulate high load while DirectPath is down and trigger DCP scale-up.
	// The new DCP channel must enter fallback immediately and prime over CloudPath.
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

	// 3. Heal DirectPath and wait for all DCP slots to recover via GetSession probes.
	dpBroken.Store(false)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !client.sc.fallbackState.inFallback() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if client.sc.fallbackState.inFallback() {
		t.Fatal("expected fallbackState.inFallback() to reset to false after all DCP slots recovered")
	}

	// 4. Trigger DCP scale-down and verify the drained slot is unregistered from fallbackState.
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

// TestEEF_Stress_ConcurrentFlappingAndTransactions stress-tests concurrent
// ReadWriteTransactions and single-use streaming queries across 4 slots while
// DirectPath repeatedly flaps between healthy and broken with recovery enabled.
func TestEEF_Stress_ConcurrentFlappingAndTransactions(t *testing.T) {
	t.Setenv("GOOGLE_SPANNER_ENABLE_DIRECT_ACCESS", "true")
	t.Setenv("GOOGLE_SPANNER_ENABLE_GCP_FALLBACK", "true")
	t.Setenv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY", "true")

	origProbeInterval := recoveryProbeInterval
	origMinProbes := recoveryMinProbes
	origMinDuration := recoveryMinDuration
	recoveryProbeInterval = 3 * time.Millisecond
	recoveryMinProbes = 2
	recoveryMinDuration = 5 * time.Millisecond
	defer func() {
		recoveryProbeInterval = origProbeInterval
		recoveryMinProbes = origMinProbes
		recoveryMinDuration = origMinDuration
	}()

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

	cpGrpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			cpCount.Add(1)
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			cpCount.Add(1)
			return handler(srv, ss)
		}),
	)
	sppb.RegisterSpannerServer(cpGrpcServer, dpServer.TestSpanner)
	cpLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go cpGrpcServer.Serve(cpLis)
	defer cpGrpcServer.Stop()

	origDial := dialFallbackCloudPathFn
	dialFallbackCloudPathFn = func(ctx context.Context, opts ...option.ClientOption) (gtransport.ConnPool, error) {
		return gtransport.DialPool(ctx, append(opts,
			option.WithEndpoint(cpLis.Addr().String()),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)...)
	}
	defer func() { dialFallbackCloudPathFn = origDial }()

	ctx := context.Background()
	client, err := NewClientWithConfig(ctx, "projects/p/instances/i/databases/d", ClientConfig{
		DisableNativeMetrics: true,
		NumChannels:          4,
	}, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Warm up multiplexed session and all 4 slots.
	for i := 0; i < 4; i++ {
		iter := client.Single().Query(ctx, Statement{SQL: testutil.SelectFooFromBar})
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				iter.Stop()
				t.Fatalf("warmup query failed: %v", err)
			}
		}
		iter.Stop()
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
			// Break DirectPath and immediately trip fallback so workers switch to CloudPath.
			dpBroken.Store(true)
			client.sc.fallbackState.primarySuccesses.Store(0)
			client.sc.fallbackState.primaryFailures.Store(4)
			client.sc.fallbackState.rateCheck()
			time.Sleep(15 * time.Millisecond)

			// Heal DirectPath and let the background probe loops recover the slots.
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
				if (workerID+op)%2 == 0 {
					_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *ReadWriteTransaction) error {
						_, err := txn.Update(ctx, Statement{SQL: testutil.UpdateBarSetFoo})
						return err
					})
					if err != nil {
						errCh <- err
						return
					}
				} else {
					iter := client.Single().Query(ctx, Statement{SQL: testutil.SelectFooFromBar})
					for {
						_, err := iter.Next()
						if err == iterator.Done {
							break
						}
						if err != nil {
							iter.Stop()
							errCh <- err
							return
						}
					}
					iter.Stop()
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
