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
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"cloud.google.com/go/spanner/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/alts"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	defaultFallbackPeriod              = 3 * time.Minute
	defaultFallbackErrorRateThreshold  = float32(1.0)
	defaultFallbackMinFailedCalls      = 1
	defaultFallbackPrimaryChannelName  = "primary"
	defaultFallbackFallbackChannelName = "fallback"

	defaultRecoveryProbeInterval   = 1 * time.Minute
	defaultRecoveryMinProbes       = 10
	defaultRecoveryMinDuration     = 10 * time.Minute
	defaultRecoveryProbeRPCTimeout = 10 * time.Second
)

// Test hooks for overriding period/dialer/peer check in unit tests.
var (
	fallbackEvalPeriod      = defaultFallbackPeriod
	recoveryProbeInterval   = defaultRecoveryProbeInterval
	recoveryMinProbes       = defaultRecoveryMinProbes
	recoveryMinDuration     = defaultRecoveryMinDuration
	dialFallbackCloudPathFn = gtransport.DialPool
	isDirectPathPeerFn      = isDirectPathPeer
)

func isDirectPathPeer(p *peer.Peer) bool {
	if p == nil || p.AuthInfo == nil {
		return false
	}
	_, isALTS := p.AuthInfo.(alts.AuthInfo)
	return isALTS
}

func isFallbackRecoveryEnabled() bool {
	val, ok := os.LookupEnv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY")
	if !ok {
		return false
	}
	b, err := strconv.ParseBool(val)
	return err == nil && b
}

// fallbackState tracks client-wide DirectPath health and metrics across all fallbackSlots.
type fallbackState struct {
	ctx    context.Context
	cancel context.CancelFunc

	isInFallback     atomic.Bool
	primarySuccesses atomic.Uint64
	primaryFailures  atomic.Uint64

	erroneousCodes      map[codes.Code]struct{}
	errorRateThreshold  float32
	minFailedCalls      int
	period              time.Duration
	enableRecovery      bool
	probeInterval       time.Duration
	minRecoveryProbes   int
	minRecoveryDuration time.Duration
	database            string
	probeSession        atomic.Value // string

	slotsMu sync.Mutex
	slots   []*fallbackSlot

	fallbackCounter    metric.Int64Counter
	callCounter        metric.Int64Counter
	probeResultCounter metric.Int64Counter
}

func newFallbackState(database string, mp metric.MeterProvider) *fallbackState {
	ctx, cancel := context.WithCancel(context.Background())
	s := &fallbackState{
		ctx:    ctx,
		cancel: cancel,
		erroneousCodes: map[codes.Code]struct{}{
			codes.DeadlineExceeded: {},
			codes.Unavailable:      {},
			codes.Unauthenticated:  {},
		},
		errorRateThreshold:  defaultFallbackErrorRateThreshold,
		minFailedCalls:      defaultFallbackMinFailedCalls,
		period:              fallbackEvalPeriod,
		enableRecovery:      isFallbackRecoveryEnabled(),
		probeInterval:       recoveryProbeInterval,
		minRecoveryProbes:   recoveryMinProbes,
		minRecoveryDuration: recoveryMinDuration,
		database:            database,
	}
	if mp != nil {
		meter := mp.Meter(grpcGcpMetricMeterName, metric.WithInstrumentationVersion(internal.Version))
		s.fallbackCounter, _ = meter.Int64Counter(
			metricNameEEFFallbackCount,
			metric.WithDescription("Number of fallbacks occurred from one channel to another."),
			metric.WithUnit("{occurrence}"),
		)
		s.callCounter, _ = meter.Int64Counter(
			metricNameEEFCallStatus,
			metric.WithDescription("Number of calls with a status and channel."),
			metric.WithUnit("{call}"),
		)
		s.probeResultCounter, _ = meter.Int64Counter(
			metricNameEEFProbeResult,
			metric.WithDescription("Results of probing channels."),
			metric.WithUnit("{probe}"),
		)
	}
	go s.runRateCheck()
	return s
}

func (s *fallbackState) runRateCheck() {
	ticker := time.NewTicker(s.period)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.rateCheck()
		}
	}
}

func (s *fallbackState) inFallback() bool {
	return s.isInFallback.Load()
}

func (s *fallbackState) setProbeSession(sessionID string) {
	if s != nil && sessionID != "" {
		s.probeSession.Store(sessionID)
	}
}

func (s *fallbackState) registerSlot(slot *fallbackSlot) {
	s.slotsMu.Lock()
	s.slots = append(s.slots, slot)
	alreadyInFallback := s.isInFallback.Load()
	s.slotsMu.Unlock()
	if alreadyInFallback {
		slot.enterFallback()
	}
}

func (s *fallbackState) unregisterSlot(slot *fallbackSlot) {
	s.slotsMu.Lock()
	anyOnDirectPath := false
	for i := len(s.slots) - 1; i >= 0; i-- {
		if s.slots[i] == slot {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			continue
		}
		if !s.slots[i].slotInFallback.Load() {
			anyOnDirectPath = true
		}
	}
	if s.enableRecovery && (len(s.slots) == 0 || anyOnDirectPath) {
		s.resetFallback()
	}
	s.slotsMu.Unlock()
}

// onSlotRecovered resets the shared fallback flag when a slot recovers so
// rateCheck is re-armed for DirectPath traffic and new DCP channels start on DirectPath.
func (s *fallbackState) onSlotRecovered() {
	s.resetFallback()
}

func (s *fallbackState) resetFallback() {
	if s.isInFallback.Swap(false) {
		s.primaryFailures.Store(0)
		s.primarySuccesses.Store(0)
	}
}

func (s *fallbackState) isErroneousCode(code codes.Code) bool {
	_, ok := s.erroneousCodes[code]
	return ok
}

func (s *fallbackState) reportPrimaryStatus(slot *fallbackSlot, err error) {
	code := status.Code(err)
	s.recordCallMetric(defaultFallbackPrimaryChannelName, code)
	if slot.isInFallback() {
		return
	}
	if s.isErroneousCode(code) {
		s.primaryFailures.Add(1)
	} else {
		s.primarySuccesses.Add(1)
	}
}

func (s *fallbackState) reportFallbackStatus(err error) {
	s.recordCallMetric(defaultFallbackFallbackChannelName, status.Code(err))
}

func (s *fallbackState) recordCallMetric(channelName string, code codes.Code) {
	if s.callCounter != nil {
		s.callCounter.Add(s.ctx, 1, metric.WithAttributes(
			attribute.String(metricLabelKeyChannelName, channelName),
			attribute.String(metricLabelKeyStatusCode, code.String()),
		))
	}
}

func (s *fallbackState) recordFallbackSwitchMetric(from, to string) {
	if s.fallbackCounter != nil {
		s.fallbackCounter.Add(s.ctx, 1, metric.WithAttributes(
			attribute.String(metricLabelKeyFromChannelName, from),
			attribute.String(metricLabelKeyToChannelName, to),
		))
	}
}

func (s *fallbackState) recordProbeMetric(channelName, result string) {
	if s.probeResultCounter != nil {
		s.probeResultCounter.Add(s.ctx, 1, metric.WithAttributes(
			attribute.String(metricLabelKeyChannelName, channelName),
			attribute.Bool(metricLabelKeySuccess, result == ""),
			attribute.String(metricLabelKeyReason, result),
		))
	}
}

func (s *fallbackState) rateCheck() {
	failures := s.primaryFailures.Swap(0)
	successes := s.primarySuccesses.Swap(0)
	if s.isInFallback.Load() {
		return
	}
	total := failures + successes
	if total == 0 || int(failures) < s.minFailedCalls {
		return
	}
	if float32(failures)/float32(total) < s.errorRateThreshold {
		return
	}
	if s.isInFallback.CompareAndSwap(false, true) {
		s.recordFallbackSwitchMetric(defaultFallbackPrimaryChannelName, defaultFallbackFallbackChannelName)
		s.slotsMu.Lock()
		slots := append([]*fallbackSlot(nil), s.slots...)
		s.slotsMu.Unlock()
		for _, slot := range slots {
			slot.enterFallback()
		}
	}
}

func (s *fallbackState) close() {
	if s != nil {
		s.cancel()
	}
}

func (sc *sessionClient) initFallback(database, compression string, mp metric.MeterProvider, metricsTracerFactory *builtinMetricsTracerFactory, opts []option.ClientOption) {
	if mp == nil && metricsTracerFactory != nil {
		mp = metricsTracerFactory.meterProvider
	}
	sc.fallbackState = newFallbackState(database, mp)
	sc.dialFallback = newFallbackDialer(compression, opts)
}

func newFallbackDialer(compression string, opts []option.ClientOption) func(context.Context) (gtransport.ConnPool, error) {
	baseOpts := append([]option.ClientOption(nil), opts...)
	dialFn := dialFallbackCloudPathFn
	return func(ctx context.Context) (gtransport.ConnPool, error) {
		cloudOpts := buildClientOpts(1, compression, false, baseOpts...)
		cloudOpts = append(cloudOpts,
			internaloption.EnableDirectPath(false),
			option.WithGRPCConnectionPool(1),
		)
		return dialFn(ctx, cloudOpts[:len(cloudOpts):len(cloudOpts)]...)
	}
}

// fallbackSlot routes RPCs over DirectPath (primary) or CloudPath (fallback)
// for a single channel slot.
type fallbackSlot struct {
	primaryConn  grpc.ClientConnInterface
	primaryPool  gtransport.ConnPool
	fallbackConn gtransport.ConnPool
	dialFallback func(context.Context) (gtransport.ConnPool, error)
	state        *fallbackState

	mu     sync.Mutex
	closed bool

	slotInFallback    atomic.Bool
	consecutiveProbes int
	firstProbeSuccess time.Time
	probeCancel       context.CancelFunc
}

var _ gtransport.ConnPool = (*fallbackSlot)(nil)

func newFallbackSlot(primary grpc.ClientConnInterface, state *fallbackState, dialFallback func(context.Context) (gtransport.ConnPool, error)) *fallbackSlot {
	var primaryPool gtransport.ConnPool
	if p, ok := primary.(gtransport.ConnPool); ok {
		primaryPool = p
	}
	slot := &fallbackSlot{
		primaryConn:  primary,
		primaryPool:  primaryPool,
		dialFallback: dialFallback,
		state:        state,
	}
	state.registerSlot(slot)
	return slot
}

func (s *fallbackSlot) isInFallback() bool {
	if s.state.enableRecovery {
		return s.slotInFallback.Load()
	}
	return s.state.inFallback()
}

func (s *fallbackSlot) enterFallback() {
	if !s.slotInFallback.CompareAndSwap(false, true) {
		return
	}
	if s.state.enableRecovery {
		s.startProbing()
	}
}

func (s *fallbackSlot) startProbing() {
	s.mu.Lock()
	if s.closed || s.probeCancel != nil {
		s.mu.Unlock()
		return
	}
	s.consecutiveProbes = 0
	s.firstProbeSuccess = time.Time{}
	probeCtx, cancel := context.WithCancel(s.state.ctx)
	s.probeCancel = cancel
	s.mu.Unlock()

	go s.runProbeLoop(probeCtx)
}

func (s *fallbackSlot) runProbeLoop(ctx context.Context) {
	ticker := time.NewTicker(s.state.probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.probeOnce(ctx) {
				return
			}
		}
	}
}

// probeOnce executes a single GetSession probe on the slot's DirectPath channel
// using the client's multiplexed session, returning true if the slot recovered.
func (s *fallbackSlot) probeOnce(ctx context.Context) bool {
	if !s.slotInFallback.Load() {
		return true
	}
	reason := s.probeDirectPath(ctx)
	s.state.recordProbeMetric(defaultFallbackPrimaryChannelName, reason)

	s.mu.Lock()
	if s.closed || !s.slotInFallback.Load() {
		s.mu.Unlock()
		return true
	}
	if reason != "" {
		s.consecutiveProbes = 0
		s.firstProbeSuccess = time.Time{}
		s.mu.Unlock()
		return false
	}

	now := time.Now()
	if s.consecutiveProbes == 0 {
		s.firstProbeSuccess = now
	}
	s.consecutiveProbes++
	if s.consecutiveProbes >= s.state.minRecoveryProbes && now.Sub(s.firstProbeSuccess) >= s.state.minRecoveryDuration {
		s.consecutiveProbes = 0
		s.firstProbeSuccess = time.Time{}
		if s.probeCancel != nil {
			s.probeCancel()
			s.probeCancel = nil
		}
		recovered := s.slotInFallback.CompareAndSwap(true, false)
		s.mu.Unlock()
		if recovered {
			s.state.onSlotRecovered()
			s.state.recordFallbackSwitchMetric(defaultFallbackFallbackChannelName, defaultFallbackPrimaryChannelName)
		}
		return true
	}
	s.mu.Unlock()
	return false
}

func (s *fallbackSlot) probeDirectPath(ctx context.Context) string {
	v := s.state.probeSession.Load()
	sid, _ := v.(string)
	if sid == "" {
		return codes.FailedPrecondition.String()
	}
	probeCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(resourcePrefixHeader, s.state.database))
	probeCtx, cancel := context.WithTimeout(probeCtx, defaultRecoveryProbeRPCTimeout)
	defer cancel()

	var p peer.Peer
	client := spannerpb.NewSpannerClient(s.primaryConn)
	if _, err := client.GetSession(probeCtx, &spannerpb.GetSessionRequest{Name: sid}, grpc.Peer(&p)); err != nil {
		return status.Code(err).String()
	}
	if !isDirectPathPeerFn(&p) {
		return "NOT_DIRECTPATH"
	}
	return ""
}

func (s *fallbackSlot) getOrDialFallback() (gtransport.ConnPool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, spannerErrorf(codes.Canceled, "spanner: fallbackSlot is closed")
	}
	if s.fallbackConn != nil {
		return s.fallbackConn, nil
	}
	conn, err := s.dialFallback(s.state.ctx)
	if err != nil {
		return nil, err
	}
	s.fallbackConn = conn
	return s.fallbackConn, nil
}

func (s *fallbackSlot) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	if s.isInFallback() {
		fallback, err := s.getOrDialFallback()
		if err != nil {
			s.state.reportFallbackStatus(err)
			return err
		}
		err = fallback.Invoke(ctx, method, args, reply, opts...)
		s.state.reportFallbackStatus(err)
		return err
	}
	err := s.primaryConn.Invoke(ctx, method, args, reply, opts...)
	s.state.reportPrimaryStatus(s, err)
	return err
}

func (s *fallbackSlot) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if s.isInFallback() {
		fallback, err := s.getOrDialFallback()
		if err != nil {
			s.state.reportFallbackStatus(err)
			return nil, err
		}
		stream, err := fallback.NewStream(ctx, desc, method, opts...)
		if err != nil {
			s.state.reportFallbackStatus(err)
			return nil, err
		}
		return &fallbackMonitoredStream{
			ClientStream: stream,
			onFinish:     s.state.reportFallbackStatus,
		}, nil
	}
	stream, err := s.primaryConn.NewStream(ctx, desc, method, opts...)
	if err != nil {
		s.state.reportPrimaryStatus(s, err)
		return nil, err
	}
	return &fallbackMonitoredStream{
		ClientStream: stream,
		onFinish: func(err error) {
			s.state.reportPrimaryStatus(s, err)
		},
	}, nil
}

func (s *fallbackSlot) Conn() *grpc.ClientConn {
	if cc, ok := s.primaryConn.(*grpc.ClientConn); ok {
		return cc
	}
	if s.primaryPool != nil {
		return s.primaryPool.Conn()
	}
	return nil
}

func (s *fallbackSlot) Num() int {
	return 1
}

func (s *fallbackSlot) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.probeCancel != nil {
		s.probeCancel()
		s.probeCancel = nil
	}
	fb := s.fallbackConn
	s.fallbackConn = nil
	primaryPool := s.primaryPool
	s.mu.Unlock()

	s.state.unregisterSlot(s)

	var errs []error
	if fb != nil {
		errs = append(errs, fb.Close())
	}
	if primaryPool != nil {
		errs = append(errs, primaryPool.Close())
	}
	return errors.Join(errs...)
}

type fallbackMonitoredStream struct {
	grpc.ClientStream
	onFinish func(error)
	recorded atomic.Bool
}

func (ms *fallbackMonitoredStream) SendMsg(m any) error {
	err := ms.ClientStream.SendMsg(m)
	if err != nil && !errors.Is(err, io.EOF) && ms.recorded.CompareAndSwap(false, true) {
		ms.onFinish(err)
	}
	return err
}

func (ms *fallbackMonitoredStream) RecvMsg(m any) error {
	err := ms.ClientStream.RecvMsg(m)
	if err != nil && ms.recorded.CompareAndSwap(false, true) {
		if errors.Is(err, io.EOF) {
			ms.onFinish(nil)
		} else {
			ms.onFinish(err)
		}
	}
	return err
}
