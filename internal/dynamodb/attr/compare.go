package attr

import (
	"bytes"
	"fmt"
	"strings"
)

// ValidationError is a request DynamoDB rejects as invalid; the service
// answers it with a ValidationException carrying Message.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// Errorf returns a ValidationError.
func Errorf(format string, args ...any) *ValidationError {
	return &ValidationError{Message: fmt.Sprintf(format, args...)}
}

// Equal reports whether a and b are the same value: numbers compare by value,
// sets ignore member order.
func Equal(a, b Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case S, N:
		return a.S == b.S // numbers are normalised, so equal numbers are equal strings
	case B:
		return bytes.Equal(a.B, b.B)
	case BOOL:
		return a.Bool == b.Bool
	case NULL:
		return true
	case L:
		if len(a.L) != len(b.L) {
			return false
		}
		for i := range a.L {
			if !Equal(a.L[i], b.L[i]) {
				return false
			}
		}
		return true
	case M:
		if len(a.M) != len(b.M) {
			return false
		}
		for k, av := range a.M {
			if bv, ok := b.M[k]; !ok || !Equal(av, bv) {
				return false
			}
		}
		return true
	default:
		return a.Len() == b.Len() && Difference(a, b).Len() == 0
	}
}

// Compare orders two scalars of the same type: strings by UTF-8 bytes,
// numbers numerically, binaries bytewise. ok is false when the values cannot
// be ordered (different types, or not S, N or B).
func Compare(a, b Value) (c int, ok bool) {
	if a.Type != b.Type {
		return 0, false
	}
	switch a.Type {
	case S:
		return strings.Compare(a.S, b.S), true
	case N:
		return a.Number().Cmp(b.Number()), true
	case B:
		return bytes.Compare(a.B, b.B), true
	}
	return 0, false
}

// HasPrefix reports whether v, an S or B, starts with prefix of the same type.
func HasPrefix(v, prefix Value) bool {
	switch {
	case v.Type == S && prefix.Type == S:
		return strings.HasPrefix(v.S, prefix.S)
	case v.Type == B && prefix.Type == B:
		return bytes.HasPrefix(v.B, prefix.B)
	}
	return false
}

// Contains implements the contains function: a substring of a string, a
// subsequence of bytes, a member of a set, or an element of a list.
func Contains(v, operand Value) bool {
	switch v.Type {
	case S:
		return operand.Type == S && strings.Contains(v.S, operand.S)
	case B:
		return operand.Type == B && bytes.Contains(v.B, operand.B)
	case SS, NS, BS:
		return HasMember(v, operand)
	case L:
		for _, e := range v.L {
			if Equal(e, operand) {
				return true
			}
		}
	}
	return false
}

// String renders v compactly for error messages, as {S:abc}.
func (v Value) String() string {
	switch v.Type {
	case S, N:
		return "{" + string(v.Type) + ":" + v.S + "}"
	case BOOL:
		return fmt.Sprintf("{BOOL:%t}", v.Bool)
	case NULL:
		return "{NULL:true}"
	case SS, NS:
		return "{" + string(v.Type) + ":[" + strings.Join(v.Set, ",") + "]}"
	default:
		return "{" + string(v.Type) + ":...}"
	}
}
