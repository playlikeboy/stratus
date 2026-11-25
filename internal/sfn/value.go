package sfn

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// decodeJSON parses one JSON value. Numbers stay json.Number so that large
// integers survive a round trip through the interpreter unchanged.
func decodeJSON(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the JSON value")
	}
	return v, nil
}

// encodeJSON renders v as compact JSON without HTML escaping, the way the
// service returns state input and output.
func encodeJSON(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "null"
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

// toFloat reads a JSON number, however the value was produced.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// sameJSON tells whether two JSON documents hold the same value, whatever
// their formatting.
func sameJSON(a, b string) bool {
	x, errA := decodeJSON(a)
	y, errB := decodeJSON(b)
	return errA == nil && errB == nil && encodeJSON(x) == encodeJSON(y)
}
