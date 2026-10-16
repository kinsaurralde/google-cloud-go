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
	"google.golang.org/grpc/metadata"
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

// Test hooks for overriding period/dialer in unit tests.
var (
	fallbackEvalPeriod      = defaultFallbackPeriod
	recoveryProbeInterval   = defaultRecoveryProbeInterval
	recoveryMinProbes       = defaultRecoveryMinProbes
	recoveryMinDuration     = defaultRecoveryMinDuration
	dialFallbackCloudPathFn = gtransport.DialPool
)

func isFallbackRecoveryEnabled() bool {
	val, ok := os.LookupEnv("GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY")
	if !ok {
		return false
	}
	b, err := strconv.ParseBool(val)
	return err == nil && b
}

// fallbackState is shared by every fallbackSlot in a client. It replaces
// GCPFallback's decision and keeps its rule and metrics.
type fallbackState struct {
	ctx    context.Context
	cancel context.CancelFunc

	isInFallback     atomic.Bool
	primarySuccesses atomic.Uint64 // reset every period
	primaryFailures  atomic.Uint64

	// Same rule as GCPFallback options
	erroneousCodes     map[codes.Code]struct{} // DEADLINE_EXCEEDED, UNAVAILABLE, UNAUTHENTICATED
	errorRateThreshold float32                 // 1.0
	minFailedCalls     int                     // 1
	period             time.Duration           // 3 minutes

	// Recovery configuration (guarded by GOOGLE_SPANNER_EXPERIMENTAL_GCP_FALLBACK_RECOVERY)
	enableRecovery      bool
	probeInterval       time.Duration
	minRecoveryProbes   int
	minRecoveryDuration time.Duration
	database            string
	probeSession        atomic.Value // string (multiplexed session name)

	slotsMu sync.Mutex
	slots   []*fallbackSlot

	// Same metrics as GCPFallback, on the "grpc-gcp-go" meter
	fallbackCounter     metric.Int64Counter // eef.fallback_count
	callCounter         metric.Int64Counter // eef.call_status
	probeResultCounter  metric.Int64Counter // eef.probe_result
	primaryChannelName  string              // "primary"
	fallbackChannelName string              // "fallback"
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
		primaryChannelName:  defaultFallbackPrimaryChannelName,
		fallbackChannelName: defaultFallbackFallbackChannelName,
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
	if s == nil {
		return
	}
	s.slotsMu.Lock()
	defer s.slotsMu.Unlock()
	for i, candidate := range s.slots {
		if candidate == slot {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			return
		}
	}
}

func (s *fallbackState) isErroneousCode(code codes.Code) bool {
	_, ok := s.erroneousCodes[code]
	return ok
}

func (s *fallbackState) reportPrimaryStatus(slot *fallbackSlot, err error) {
	code := status.Code(err)
	s.recordCallMetric(s.primaryChannelName, code)
	if s.isErroneousCode(code) {
		s.primaryFailures.Add(1)
		if slot != nil && s.enableRecovery {
			slot.slotFailures.Add(1)
		}
	} else {
		s.primarySuccesses.Add(1)
		if slot != nil && s.enableRecovery {
			slot.slotSuccesses.Add(1)
		}
	}
}

func (s *fallbackState) reportFallbackStatus(err error) {
	code := status.Code(err)
	s.recordCallMetric(s.fallbackChannelName, code)
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

// rateCheck runs every period and switches slots to CloudPath if the error rate
// threshold is reached.
func (s *fallbackState) rateCheck() {
	failures := s.primaryFailures.Swap(0)
	successes := s.primarySuccesses.Swap(0)
	total := failures + successes

	if !s.isInFallback.Load() {
		if total > 0 && int(failures) >= s.minFailedCalls {
			errorRate := float32(failures) / float32(total)
			if errorRate >= s.errorRateThreshold {
				if s.isInFallback.CompareAndSwap(false, true) {
					s.recordFallbackSwitchMetric(s.primaryChannelName, s.fallbackChannelName)
					s.slotsMu.Lock()
					slots := append([]*fallbackSlot(nil), s.slots...)
					s.slotsMu.Unlock()
					for _, slot := range slots {
						slot.enterFallback()
					}
				}
			}
		}
		return
	}

	// If recovery is enabled, check any recovered slots for individual re-fallback.
	if s.enableRecovery {
		s.slotsMu.Lock()
		slots := append([]*fallbackSlot(nil), s.slots...)
		s.slotsMu.Unlock()
		for _, slot := range slots {
			slot.checkSlotRate()
		}
	}
}

func (s *fallbackState) close() {
	if s != nil {
		s.cancel()
	}
}

// newFallbackDialer returns a function that dials a single CloudPath connection
// from the client's options with DirectPath disabled.
func newFallbackDialer(compression string, opts []option.ClientOption) func(context.Context) (gtransport.ConnPool, error) {
	cloudOpts := append([]option.ClientOption{}, buildClientOpts(1, compression, false, opts...)...)
	cloudOpts = append(cloudOpts,
		internaloption.EnableDirectPath(false),
		option.WithGRPCConnectionPool(1),
	)
	dialFn := dialFallbackCloudPathFn
	return func(ctx context.Context) (gtransport.ConnPool, error) {
		return dialFn(ctx, cloudOpts...)
	}
}

// fallbackSlot sends each call over DirectPath (primary) or CloudPath (fallback).
// It implements gtransport.ConnPool, so it can be used in place of a connection.
type fallbackSlot struct {
	primaryConn  grpc.ClientConnInterface
	primaryPool  gtransport.ConnPool // non-nil when owned by this slot (e.g. DCP)
	fallbackConn gtransport.ConnPool // dialed on first use
	dialFallback func(context.Context) (gtransport.ConnPool, error)
	state        *fallbackState // shared by every slot in the client

	mu     sync.Mutex
	closed bool

	// Per-slot fallback and recovery state (Hybrid Option 2)
	slotInFallback    atomic.Bool
	slotSuccesses     atomic.Uint64
	slotFailures      atomic.Uint64
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
	if state != nil {
		state.registerSlot(slot)
	}
	return slot
}

func (s *fallbackSlot) isInFallback() bool {
	if s.state == nil {
		return false
	}
	if s.state.enableRecovery {
		return s.slotInFallback.Load()
	}
	return s.state.inFallback()
}

func (s *fallbackSlot) enterFallback() {
	s.slotSuccesses.Store(0)
	s.slotFailures.Store(0)
	if !s.slotInFallback.CompareAndSwap(false, true) {
		return
	}
	if s.state != nil && s.state.enableRecovery {
		s.startProbing()
	}
}

func (s *fallbackSlot) checkSlotRate() {
	failures := s.slotFailures.Swap(0)
	successes := s.slotSuccesses.Swap(0)
	if s.slotInFallback.Load() {
		return
	}
	total := failures + successes
	if total > 0 && int(failures) >= s.state.minFailedCalls {
		errorRate := float32(failures) / float32(total)
		if errorRate >= s.state.errorRateThreshold {
			if s.slotInFallback.CompareAndSwap(false, true) {
				s.state.recordFallbackSwitchMetric(s.state.primaryChannelName, s.state.fallbackChannelName)
				s.startProbing()
			}
		}
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
	s.state.recordProbeMetric(s.state.primaryChannelName, reason)

	s.mu.Lock()
	defer s.mu.Unlock()
	if reason != "" {
		s.consecutiveProbes = 0
		s.firstProbeSuccess = time.Time{}
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
		s.slotSuccesses.Store(0)
		s.slotFailures.Store(0)
		if s.slotInFallback.CompareAndSwap(true, false) {
			s.state.recordFallbackSwitchMetric(s.state.fallbackChannelName, s.state.primaryChannelName)
		}
		return true
	}
	return false
}

func (s *fallbackSlot) probeDirectPath(ctx context.Context) string {
	probeCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(resourcePrefixHeader, s.state.database))
	probeCtx, cancel := context.WithTimeout(probeCtx, defaultRecoveryProbeRPCTimeout)
	defer cancel()

	client := spannerpb.NewSpannerClient(s.primaryConn)
	v := s.state.probeSession.Load()
	sid, _ := v.(string)
	if sid == "" {
		sess, err := client.CreateSession(probeCtx, &spannerpb.CreateSessionRequest{
			Database: s.state.database,
			Session:  &spannerpb.Session{Multiplexed: true},
		})
		if err != nil {
			return status.Code(err).String()
		}
		s.state.setProbeSession(sess.GetName())
		return ""
	}

	if _, err := client.GetSession(probeCtx, &spannerpb.GetSessionRequest{Name: sid}); err != nil {
		return status.Code(err).String()
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
			return err
		}
		err = fallback.Invoke(ctx, method, args, reply, opts...)
		s.state.reportFallbackStatus(err)
		return err
	}
	err := s.primaryConn.Invoke(ctx, method, args, reply, opts...)
	if s.state != nil {
		s.state.reportPrimaryStatus(s, err)
	}
	return err
}

func (s *fallbackSlot) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if s.isInFallback() {
		fallback, err := s.getOrDialFallback()
		if err != nil {
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
		if s.state != nil {
			s.state.reportPrimaryStatus(s, err)
		}
		return nil, err
	}
	if s.state == nil {
		return stream, nil
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
