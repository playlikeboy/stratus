package sfn

import "time"

// block waits until ev fires or the clock reaches deadline (zero means no
// deadline), then returns nil; the caller re-checks what it waits for. It
// returns errCancelled when the execution is stopped and errExecTimeout when
// the machine's TimeoutSeconds runs out first.
//
// Time comes from the injected clock: block sleeps for the clock's distance
// to the deadline in real time, then asks the clock again, and Service.Wake
// cuts the sleep short so a test can move a fake clock.
func (e *execution) block(deadline time.Time, ev <-chan struct{}) error {
	for {
		wake := e.s.wakeChan()
		if e.ctx.Err() != nil {
			return errCancelled
		}
		limit, timedOut := deadline, false
		if !e.deadline.IsZero() && (limit.IsZero() || e.deadline.Before(limit)) {
			limit, timedOut = e.deadline, true
		}
		var (
			timer <-chan time.Time
			t     *time.Timer
		)
		if !limit.IsZero() {
			d := limit.Sub(e.s.clock.Now())
			if d <= 0 {
				if timedOut {
					return errExecTimeout
				}
				return nil
			}
			t = time.NewTimer(d)
			timer = t.C
		}
		select {
		case <-e.ctx.Done():
			if t != nil {
				t.Stop()
			}
			return errCancelled
		case <-ev:
			if t != nil {
				t.Stop()
			}
			return nil
		case <-wake:
		case <-timer:
		}
		if t != nil {
			t.Stop()
		}
	}
}

// sleepUntil blocks until the clock reaches until.
func (e *execution) sleepUntil(until time.Time) error {
	for e.s.clock.Now().Before(until) {
		if err := e.block(until, nil); err != nil {
			return err
		}
	}
	return nil
}
