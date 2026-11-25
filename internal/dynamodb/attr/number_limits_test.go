package attr

import (
	"strings"
	"testing"
	"time"
)

func TestHugeNumberLiteralIsCheapToParse(t *testing.T) {
	start := time.Now()
	for _, literal := range []string{"1" + strings.Repeat("0", 2_000_000), strings.Repeat("7", 2_000_000)} {
		if n, err := ParseNumber(literal); err == nil {
			t.Fatalf("a %d-digit number was accepted: %s", len(literal), n.String()[:20])
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("parsing took %v", elapsed)
	}
}

func TestTrailingZerosKeepTheValue(t *testing.T) {
	for in, want := range map[string]string{"1000": "1000", "0012.3400": "12.34", "0.000": "0", "5E+3": "5000", "120e-1": "12"} {
		n, err := ParseNumber(in)
		if err != nil || n.String() != want {
			t.Errorf("ParseNumber(%q) = %q, %v; want %q", in, n.String(), err, want)
		}
	}
}
