package sfn

import "time"

// event is one entry of an execution's history.
type event struct {
	id      int64
	typ     string
	at      time.Time
	key     string // name of the details member, for example "stateEnteredEventDetails"
	details map[string]any
}

// addLocked appends an event; the caller holds Service.mu.
func (e *execution) addLocked(typ, key string, details map[string]any) {
	e.history = append(e.history, event{
		id: int64(len(e.history)) + 1, typ: typ, at: e.s.clock.Now(), key: key, details: details,
	})
}

// maxHistoryEvents is where Step Functions fails an execution.
const maxHistoryEvents = 25000

func (e *execution) historyLen() int {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	return len(e.history)
}

// add records an event of a running execution.
func (e *execution) add(typ, key string, details map[string]any) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	if e.status == statusRunning {
		e.addLocked(typ, key, details)
	}
}

// epoch renders a time the way the JSON protocol does: seconds with
// millisecond fractions.
func epoch(t time.Time) float64 { return float64(t.UnixMilli()) / 1000 }

// wire is the event as GetExecutionHistory returns it.
func (ev event) wire() map[string]any {
	out := map[string]any{
		"id":              ev.id,
		"previousEventId": ev.id - 1,
		"timestamp":       epoch(ev.at),
		"type":            ev.typ,
	}
	if ev.key != "" {
		out[ev.key] = ev.details
	}
	return out
}

// stateEntered records that a running execution entered st at entered, and
// remembers where it is, for a snapshot to resume it there.
func (e *execution) stateEntered(st *state, input any, entered time.Time) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	if e.status != statusRunning {
		return
	}
	e.at = position{state: st.name, input: input, entered: entered}
	e.addLocked(st.typ+"StateEntered", "stateEnteredEventDetails",
		map[string]any{"name": st.name, "input": encodeJSON(input), "inputDetails": map[string]any{"truncated": false}})
}

func (e *execution) stateExited(st *state, output any) {
	e.add(st.typ+"StateExited", "stateExitedEventDetails",
		map[string]any{"name": st.name, "output": encodeJSON(output), "outputDetails": map[string]any{"truncated": false}})
}
