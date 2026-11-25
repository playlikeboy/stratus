package expr

import (
	"strings"
	"testing"
)

// A deeply nested expression must be refused, not parsed: the parser recurses.
func TestOversizedExpressionIsRejected(t *testing.T) {
	deep := strings.Repeat("(", 3000) + "a = :v" + strings.Repeat(")", 3000)
	_, err := ParseCondition("ConditionExpression", deep, NewContext(nil, nil))
	if err == nil || !strings.Contains(err.Error(), "maximum allowed size") {
		t.Fatalf("error = %v, want the expression-size error", err)
	}
}
