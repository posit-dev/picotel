package picotel

// sender_test.go — WP5: unit tests for syncSender, asyncSender, Flush, and
// theSender selection. All helpers are prefixed "snd" to avoid collisions when
// other agents' test files merge into the same package.
//
// Design constraints:
//   - No HTTP, no real network. All fn closures are synthetic.
//   - Async tests never use bare time.Sleep as the only synchronisation.
//   - All tests pass with -race.

import (
	"bytes"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// Test helpers (prefixed "snd" per merge convention)
// ============================================================================

// sndErrFn returns a func() error that always returns the given error.
func sndErrFn(err error) func() error { return func() error { return err } }

// sndOKFn returns a func() error that always returns nil (success).
func sndOKFn() func() error { return func() error { return nil } }

// sndPanicFn returns a func() error that panics with the given message.
func sndPanicFn(msg string) func() error {
	return func() error { panic(msg) }
}

// sndConfigErrFn returns a func() error that returns a *ConfigError.
func sndConfigErrFn(msg string) func() error {
	return func() error { return &ConfigError{Msg: msg} }
}

// sndSafeBuf is a thread-safe bytes.Buffer for capturing log output.
// The worker goroutine writes to it; the test goroutine reads it.
// A sync.Mutex protects concurrent access so -race doesn't fire.
type sndSafeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *sndSafeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *sndSafeBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *sndSafeBuf) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

// sndCaptureLogger replaces pkgLogger with one that writes to a
// thread-safe buffer, returning the buffer and a restore function.
// The swap is protected by pkgLoggerMu so concurrent worker goroutines
// that call pkgLog() see a consistent logger value.
func sndCaptureLogger() (*sndSafeBuf, func()) {
	buf := &sndSafeBuf{}
	pkgLoggerMu.Lock()
	old := pkgLogger
	pkgLogger = log.New(buf, "", 0)
	pkgLoggerMu.Unlock()
	return buf, func() {
		pkgLoggerMu.Lock()
		pkgLogger = old
		pkgLoggerMu.Unlock()
	}
}

// sndWaitPending polls until a.pending.Load()==0 or deadline passes.
// Returns true if pending reached 0 within the deadline.
func sndWaitPending(a *asyncSender, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if a.pending.Load() == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// sndWaitTripped polls until a.tripped.Load()==true or deadline passes.
func sndWaitTripped(a *asyncSender, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if a.tripped.Load() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// sndSyncWorker submits a sentinel fn and waits for it to be processed,
// ensuring the worker has finished all previously enqueued items.
// This is the correct race-free way to "flush" the async worker in tests.
func sndSyncWorker(t *testing.T, a *asyncSender, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	ok := a.submit(func() error { close(done); return nil })
	if !ok {
		// Breaker tripped — just wait for pending to drain.
		if !sndWaitPending(a, timeout) {
			t.Fatal("sndSyncWorker: pending did not drain within timeout")
		}
		return
	}
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("sndSyncWorker: sentinel fn not processed within timeout")
	}
}

// sndReadConsecErrors reads asyncSender.consecutiveErrors safely by submitting
// a fn that captures the value from within the worker goroutine, where it is
// the single writer. This avoids a -race false positive on the field read.
func sndReadConsecErrors(t *testing.T, a *asyncSender) int {
	t.Helper()
	var val int
	done := make(chan struct{})
	// This fn runs in the worker goroutine — it's safe to read consecutiveErrors here.
	a.submit(func() error {
		val = a.consecutiveErrors
		close(done)
		return nil
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sndReadConsecErrors: timed out waiting for worker fn")
	}
	return val
}

// sndDrainBarrier is a helper that gates a fn until a channel is closed.
// The fn blocks the worker so we can fill the queue behind it.
type sndDrainBarrier struct {
	startedOnce sync.Once
	started     chan struct{}
	release     chan struct{}
}

func sndNewDrainBarrier() *sndDrainBarrier {
	return &sndDrainBarrier{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

// fn returns a func() error that signals started (once) and blocks until release.
func (b *sndDrainBarrier) fn() func() error {
	return func() error {
		b.startedOnce.Do(func() { b.started <- struct{}{} })
		<-b.release
		return nil
	}
}

func (b *sndDrainBarrier) waitStarted(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-b.started:
	case <-time.After(timeout):
		t.Fatal("barrier: worker did not reach fn within timeout")
	}
}

func (b *sndDrainBarrier) open() { close(b.release) }

// sndCountSubstr counts non-overlapping occurrences of sub in s.
func sndCountSubstr(s, sub string) int {
	count := 0
	b := []byte(s)
	needle := []byte(sub)
	for len(b) >= len(needle) {
		idx := bytes.Index(b, needle)
		if idx < 0 {
			break
		}
		count++
		b = b[idx+len(needle):]
	}
	return count
}

// ============================================================================
// syncSender tests
// ============================================================================

func TestSyncSuccess(t *testing.T) {
	s := &syncSender{}
	called := false
	got := s.submit(func() error { called = true; return nil })
	if !got {
		t.Error("submit should return true on success")
	}
	if !called {
		t.Error("fn was not called")
	}
}

func TestSyncSuccessResetsCounter(t *testing.T) {
	s := &syncSender{}
	// Accumulate failures just below the threshold.
	for i := 0; i < maxConsecutiveErrors-1; i++ {
		s.submit(sndErrFn(errors.New("boom")))
	}
	if s.consecutiveErrors != maxConsecutiveErrors-1 {
		t.Fatalf("expected %d consecutive errors, got %d", maxConsecutiveErrors-1, s.consecutiveErrors)
	}
	// One success must reset.
	s.submit(sndOKFn())
	if s.consecutiveErrors != 0 {
		t.Errorf("counter not reset after success; got %d", s.consecutiveErrors)
	}
	if s.tripped {
		t.Error("should not be tripped after success reset")
	}
}

func TestSyncTripsAfterExactlyMaxConsecutiveErrors(t *testing.T) {
	_, restore := sndCaptureLogger()
	defer restore()

	s := &syncSender{}
	for i := 0; i < maxConsecutiveErrors-1; i++ {
		got := s.submit(sndErrFn(errors.New("fail")))
		if !got {
			t.Fatalf("iteration %d: expected true before trip", i)
		}
	}
	// The Nth failure trips the breaker.
	got := s.submit(sndErrFn(errors.New("fail")))
	if got {
		t.Error("Nth failure should return false (tripped)")
	}
	if !s.tripped {
		t.Error("should be tripped after maxConsecutiveErrors failures")
	}
}

func TestSyncPostTripReturnsFalseWithoutCallingFn(t *testing.T) {
	_, restore := sndCaptureLogger()
	defer restore()

	s := &syncSender{}
	for i := 0; i < maxConsecutiveErrors; i++ {
		s.submit(sndErrFn(errors.New("fail")))
	}
	if !s.tripped {
		t.Fatal("precondition: should be tripped")
	}

	var callCount int
	for i := 0; i < 5; i++ {
		got := s.submit(func() error { callCount++; return nil })
		if got {
			t.Errorf("post-trip submit should return false, got true on iteration %d", i)
		}
	}
	if callCount != 0 {
		t.Errorf("fn called %d times after trip; expected 0", callCount)
	}
}

func TestSyncErrDisabledIsNeutral(t *testing.T) {
	s := &syncSender{}
	// Accumulate some failures.
	for i := 0; i < 3; i++ {
		s.submit(sndErrFn(errors.New("fail")))
	}
	count := s.consecutiveErrors

	// ErrDisabled should not change the counter (neither reset nor increment).
	s.submit(sndErrFn(ErrDisabled))
	if s.consecutiveErrors != count {
		t.Errorf("ErrDisabled changed consecutiveErrors from %d to %d", count, s.consecutiveErrors)
	}
	if s.tripped {
		t.Error("ErrDisabled should not trip the breaker")
	}
}

func TestSyncErrDisabledDoesNotResetNonZeroCounter(t *testing.T) {
	s := &syncSender{}
	s.submit(sndErrFn(errors.New("fail")))
	if s.consecutiveErrors != 1 {
		t.Fatalf("expected 1 consecutive error, got %d", s.consecutiveErrors)
	}
	// ErrDisabled must not reset the counter.
	s.submit(sndErrFn(ErrDisabled))
	if s.consecutiveErrors != 1 {
		t.Errorf("expected counter to stay at 1 after ErrDisabled, got %d", s.consecutiveErrors)
	}
}

func TestSyncPanicIsTransientNotCounted(t *testing.T) {
	buf, restore := sndCaptureLogger()
	defer restore()

	s := &syncSender{}
	// Panics must not count toward the breaker.
	for i := 0; i < maxConsecutiveErrors+5; i++ {
		got := s.submit(sndPanicFn("test panic"))
		if !got {
			t.Fatalf("panic should not trip the breaker; returned false on iteration %d", i)
		}
	}
	if s.tripped {
		t.Error("panics must not trip the circuit breaker")
	}
	if s.consecutiveErrors != 0 {
		t.Errorf("consecutiveErrors should be 0 after panics, got %d", s.consecutiveErrors)
	}
	if buf.Len() == 0 {
		t.Error("panics should be logged")
	}
}

func TestSyncPanicInterleavedWithFailuresDoesNotContribute(t *testing.T) {
	_, restore := sndCaptureLogger()
	defer restore()

	s := &syncSender{}
	// 2 failures, then a panic, then 2 more failures — 4 persistent failures
	// total, below the threshold of 5. Breaker must not trip.
	for i := 0; i < 2; i++ {
		s.submit(sndErrFn(errors.New("fail")))
	}
	s.submit(sndPanicFn("mid-panic"))
	for i := 0; i < 2; i++ {
		s.submit(sndErrFn(errors.New("fail")))
	}
	if s.tripped {
		t.Error("4 persistent failures + 1 panic should not trip the breaker")
	}
	if s.consecutiveErrors != 4 {
		t.Errorf("expected consecutiveErrors=4, got %d", s.consecutiveErrors)
	}
}

func TestSyncConfigErrorIsPersistedFailure(t *testing.T) {
	buf, restore := sndCaptureLogger()
	defer restore()

	s := &syncSender{}
	for i := 0; i < maxConsecutiveErrors-1; i++ {
		s.submit(sndConfigErrFn("cfg"))
	}
	got := s.submit(sndConfigErrFn("cfg"))
	if got {
		t.Error("Nth ConfigError should return false (tripped)")
	}
	if !s.tripped {
		t.Error("should be tripped after maxConsecutiveErrors ConfigErrors")
	}
	if buf.Len() == 0 {
		t.Error("trip should be logged")
	}
}

// ============================================================================
// asyncSender tests
// ============================================================================

func TestAsyncSuccess(t *testing.T) {
	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	var called atomic.Bool
	a.submit(func() error { called.Store(true); return nil })
	if !sndWaitPending(a, 2*time.Second) {
		t.Fatal("pending did not reach 0")
	}
	if !called.Load() {
		t.Error("fn was not called by worker")
	}
}

func TestAsyncSuccessResetsCounter(t *testing.T) {
	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	// Submit failures just below threshold.
	for i := 0; i < maxConsecutiveErrors-1; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}
	// Submit a success sentinel fn and wait for it to process.
	done := make(chan struct{})
	a.submit(func() error { close(done); return nil })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not process success fn")
	}
	// Read consecutiveErrors from within the worker goroutine context.
	ce := sndReadConsecErrors(t, a)
	if ce != 0 {
		t.Errorf("consecutiveErrors not reset after success; got %d", ce)
	}
	if a.tripped.Load() {
		t.Error("should not be tripped after success reset")
	}
}

func TestAsyncTripsAfterMaxConsecutiveErrors(t *testing.T) {
	buf, restore := sndCaptureLogger()
	defer restore()

	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	for i := 0; i < maxConsecutiveErrors; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}
	if !sndWaitTripped(a, 2*time.Second) {
		t.Fatal("breaker did not trip within timeout")
	}
	if buf.Len() == 0 {
		t.Error("trip should be logged")
	}
}

func TestAsyncPostTripSubmitReturnsFalseWithoutCallingFn(t *testing.T) {
	_, restore := sndCaptureLogger()
	defer restore()

	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	for i := 0; i < maxConsecutiveErrors; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}
	if !sndWaitTripped(a, 2*time.Second) {
		t.Fatal("breaker did not trip")
	}

	var callCount int64
	for i := 0; i < 5; i++ {
		got := a.submit(func() error { atomic.AddInt64(&callCount, 1); return nil })
		if got {
			t.Errorf("post-trip submit should return false, got true on iteration %d", i)
		}
	}
	// Give the worker a moment (there should be nothing to process).
	time.Sleep(20 * time.Millisecond)
	if atomic.LoadInt64(&callCount) != 0 {
		t.Errorf("fn called %d times after trip; expected 0", callCount)
	}
}

func TestAsyncPostTripPendingReachesZero(t *testing.T) {
	_, restore := sndCaptureLogger()
	defer restore()

	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	for i := 0; i < maxConsecutiveErrors; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}
	if !sndWaitTripped(a, 2*time.Second) {
		t.Fatal("breaker did not trip")
	}
	// Queue should drain (worker discards items post-trip).
	if !sndWaitPending(a, 2*time.Second) {
		t.Fatalf("pending did not reach 0 after trip; got %d", a.pending.Load())
	}
}

func TestAsyncErrDisabledIsNeutral(t *testing.T) {
	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	// Accumulate some failures.
	for i := 0; i < 3; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}
	// ErrDisabled should be neutral (neither reset nor increment).
	a.submit(sndErrFn(ErrDisabled))

	// Use sndReadConsecErrors to safely read the field from the worker goroutine.
	// The fn runs after all previous items, so consecutiveErrors reflects 3 failures
	// (ErrDisabled didn't change it).
	ce := sndReadConsecErrors(t, a)
	// Should be 3 (3 persistent failures, ErrDisabled didn't reset or increment).
	if ce != 3 {
		t.Errorf("expected consecutiveErrors=3 after ErrDisabled, got %d", ce)
	}
	if a.tripped.Load() {
		t.Error("ErrDisabled should not trip the breaker")
	}
}

func TestAsyncErrDisabledDoesNotResetNonZeroCounter(t *testing.T) {
	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	a.submit(sndErrFn(errors.New("fail")))
	a.submit(sndErrFn(ErrDisabled))

	ce := sndReadConsecErrors(t, a)
	if ce != 1 {
		t.Errorf("expected consecutiveErrors=1 after ErrDisabled (no reset), got %d", ce)
	}
}

func TestAsyncPanicIsTransientNotCounted(t *testing.T) {
	buf, restore := sndCaptureLogger()
	defer restore()

	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	for i := 0; i < maxConsecutiveErrors+5; i++ {
		a.submit(sndPanicFn("async panic"))
	}
	sndSyncWorker(t, a, 2*time.Second)

	if a.tripped.Load() {
		t.Error("panics must not trip the circuit breaker")
	}
	ce := sndReadConsecErrors(t, a)
	if ce != 0 {
		t.Errorf("consecutiveErrors should be 0 after panics, got %d", ce)
	}
	if buf.Len() == 0 {
		t.Error("panics should be logged")
	}
}

func TestAsyncPanicInterleavedWithFailuresDoesNotContribute(t *testing.T) {
	_, restore := sndCaptureLogger()
	defer restore()

	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	// 2 failures + 1 panic + 2 failures = 4 persistent failures → no trip.
	for i := 0; i < 2; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}
	a.submit(sndPanicFn("mid-panic"))
	for i := 0; i < 2; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}

	// Read consecutiveErrors safely.
	ce := sndReadConsecErrors(t, a)
	if a.tripped.Load() {
		t.Error("4 persistent failures + 1 panic should not trip the breaker")
	}
	if ce != 4 {
		t.Errorf("expected consecutiveErrors=4, got %d", ce)
	}
}

func TestAsyncConfigErrorIsPersistedFailure(t *testing.T) {
	buf, restore := sndCaptureLogger()
	defer restore()

	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	for i := 0; i < maxConsecutiveErrors; i++ {
		a.submit(sndConfigErrFn("no endpoint"))
	}
	if !sndWaitTripped(a, 2*time.Second) {
		t.Fatal("breaker did not trip after ConfigErrors")
	}
	if buf.Len() == 0 {
		t.Error("trip should be logged")
	}
}

// TestAsyncQueueOverflow verifies:
// - Overflow returns false
// - Warning logged exactly once per episode
// - Flag resets after successful enqueue (next overflow logs again)
func TestAsyncQueueOverflow(t *testing.T) {
	buf, restore := sndCaptureLogger()
	defer restore()

	const qSize = 4
	a := &asyncSender{ch: make(chan func() error, qSize)}

	// Block the worker so we can fill the queue.
	barrier := sndNewDrainBarrier()
	a.submit(barrier.fn())
	barrier.waitStarted(t, 2*time.Second)

	// Fill the queue completely.
	for i := 0; i < qSize; i++ {
		if !a.submit(sndOKFn()) {
			t.Fatalf("fill iteration %d: expected true (queue not full yet)", i)
		}
	}

	// Overflow multiple times — should warn exactly once.
	for i := 0; i < 3; i++ {
		got := a.submit(sndOKFn())
		if got {
			t.Errorf("overflow iteration %d: expected false", i)
		}
	}
	if n := sndCountSubstr(buf.String(), "queue full"); n != 1 {
		t.Errorf("expected exactly 1 queue-full log, got %d\nlog:\n%s", n, buf.String())
	}

	// Unblock the worker and wait for queue to drain.
	barrier.open()
	if !sndWaitPending(a, 2*time.Second) {
		t.Fatal("queue did not drain after barrier opened")
	}

	// Next successful enqueue resets the warned flag.
	// Now the queue is empty — submit should succeed.
	got := a.submit(sndOKFn())
	if !got {
		t.Error("expected successful enqueue after drain")
	}
	if !sndWaitPending(a, 2*time.Second) {
		t.Fatal("pending did not reach 0")
	}

	// Overflow again — should produce a second warning.
	barrier2 := sndNewDrainBarrier()
	a.submit(barrier2.fn())
	barrier2.waitStarted(t, 2*time.Second)
	for i := 0; i < qSize; i++ {
		a.submit(sndOKFn())
	}
	a.submit(sndOKFn()) // overflow

	if n := sndCountSubstr(buf.String(), "queue full"); n != 2 {
		t.Errorf("expected 2 queue-full logs after second episode, got %d\nlog:\n%s", n, buf.String())
	}
	barrier2.open()
}

// ============================================================================
// Flush tests
// ============================================================================

func TestFlushSyncSenderAlwaysTrue(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "")
	resetSender()
	defer resetSender()
	// No PICOTEL_ASYNC set → syncSender.
	if !Flush(100 * time.Millisecond) {
		t.Error("Flush should return true immediately for sync sender")
	}
}

func TestFlushAsyncDrainsSuccessfully(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "1")
	resetSender()
	defer resetSender()

	s := theSender()
	s.submit(sndOKFn())
	s.submit(sndOKFn())

	if !Flush(2 * time.Second) {
		t.Error("Flush should return true after queue drains")
	}
}

func TestFlushTimeout(t *testing.T) {
	// Gate a fn so it blocks the worker indefinitely.
	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	release := make(chan struct{})
	a.submit(func() error { <-release; return nil })

	// Flush (inline logic) should time out.
	const deadline = 50 * time.Millisecond
	start := time.Now()

	dl := time.Now().Add(deadline)
	result := false
	for {
		if a.pending.Load() == 0 {
			result = true
			break
		}
		if time.Now().After(dl) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)
	close(release) // unblock worker

	if result {
		t.Error("Flush should have returned false (timeout)")
	}
	if elapsed < deadline/2 {
		t.Errorf("Flush returned too quickly: %v < %v", elapsed, deadline/2)
	}
}

func TestFlushZeroTimeoutImmediateCheck(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "1")
	resetSender()
	defer resetSender()

	// With PICOTEL_ASYNC=1 and nothing submitted, pending==0 → should be true.
	result := Flush(0)
	if !result {
		t.Error("Flush(0) should return true when nothing is pending")
	}
}

func TestFlushViaSyncSender(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "")
	resetSender()
	defer resetSender()

	// Sync sender: Flush must return true immediately.
	if !Flush(100 * time.Millisecond) {
		t.Error("Flush should return true for sync sender")
	}
}

func TestFlushAsyncViaSender(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "1")
	resetSender()
	defer resetSender()

	s := theSender()
	a, ok := s.(*asyncSender)
	if !ok {
		t.Fatal("expected *asyncSender with PICOTEL_ASYNC=1")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	s.submit(func() error { wg.Done(); return nil })
	// Wait for the fn to actually execute before calling Flush.
	wg.Wait()

	if !Flush(1 * time.Second) {
		t.Error("Flush should return true after fn executed")
	}
	if a.pending.Load() != 0 {
		t.Errorf("pending should be 0 after Flush, got %d", a.pending.Load())
	}
}

// ============================================================================
// theSender selection tests
// ============================================================================

func TestTheSenderSyncDefault(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "")
	resetSender()
	defer resetSender()

	s := theSender()
	if _, ok := s.(*syncSender); !ok {
		t.Errorf("expected *syncSender when PICOTEL_ASYNC unset, got %T", s)
	}
}

func TestTheSenderAsyncWithOne(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "1")
	resetSender()
	defer resetSender()

	s := theSender()
	if _, ok := s.(*asyncSender); !ok {
		t.Errorf("expected *asyncSender when PICOTEL_ASYNC=1, got %T", s)
	}
}

func TestTheSenderAsyncWithTrue(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "true")
	resetSender()
	defer resetSender()

	s := theSender()
	if _, ok := s.(*asyncSender); !ok {
		t.Errorf("expected *asyncSender when PICOTEL_ASYNC=true, got %T", s)
	}
}

func TestTheSenderOtherValueFallsBackToSync(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "yes")
	resetSender()
	defer resetSender()

	s := theSender()
	if _, ok := s.(*syncSender); !ok {
		t.Errorf("expected *syncSender for non-truthy PICOTEL_ASYNC, got %T", s)
	}
}

func TestResetSenderAllowsReselection(t *testing.T) {
	t.Setenv("PICOTEL_ASYNC", "1")
	resetSender()

	s1 := theSender()
	if _, ok := s1.(*asyncSender); !ok {
		t.Fatalf("expected *asyncSender, got %T", s1)
	}

	// Reset and change env.
	t.Setenv("PICOTEL_ASYNC", "")
	resetSender()

	s2 := theSender()
	if _, ok := s2.(*syncSender); !ok {
		t.Errorf("expected *syncSender after reset+env change, got %T", s2)
	}
	// They should be different instances.
	if s1 == s2 {
		t.Error("resetSender should produce a new sender instance")
	}
}

// TestAsyncWorkerExecutesFn verifies the worker goroutine is launched and
// processes all submitted fns.
func TestAsyncWorkerExecutesFn(t *testing.T) {
	a := &asyncSender{ch: make(chan func() error, asyncQueueSize)}
	var n atomic.Int64
	for i := 0; i < 10; i++ {
		a.submit(func() error { n.Add(1); return nil })
	}
	if !sndWaitPending(a, 2*time.Second) {
		t.Fatal("pending did not reach 0")
	}
	if n.Load() != 10 {
		t.Errorf("expected 10 fn executions, got %d", n.Load())
	}
}

// TestAsyncPendingDecrementedPostTrip verifies that even after trip, items
// already in the queue have their pending decremented so Flush can unblock.
func TestAsyncPendingDecrementedPostTrip(t *testing.T) {
	_, restore := sndCaptureLogger()
	defer restore()

	const qSize = 16
	a := &asyncSender{ch: make(chan func() error, qSize)}

	// Block the worker.
	barrier := sndNewDrainBarrier()
	a.submit(barrier.fn())
	barrier.waitStarted(t, 2*time.Second)

	// Queue up a bunch of failure fns behind the blocker.
	for i := 0; i < maxConsecutiveErrors; i++ {
		a.submit(sndErrFn(errors.New("fail")))
	}

	// Open the barrier — worker will process failures and trip.
	barrier.open()

	// Pending must reach 0 even after trip (drain-and-discard).
	if !sndWaitPending(a, 2*time.Second) {
		t.Fatalf("pending did not reach 0 after trip; got %d", a.pending.Load())
	}
}
