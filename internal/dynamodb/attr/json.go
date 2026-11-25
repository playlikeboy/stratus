package attr

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// MarshalJSON writes v in its wire form, for example {"S":"abc"}.
func (v Value) MarshalJSON() ([]byte, error) {
	var payload any
	switch v.Type {
	case S, N:
		payload = v.S
	case B:
		payload = nonNilBytes(v.B)
	case BOOL:
		payload = v.Bool
	case NULL:
		payload = true
	case L:
		payload = nonNil(v.L)
	case M:
		payload = nonNilMap(v.M)
	case SS, NS:
		payload = nonNil(v.Set)
	case BS:
		payload = nonNil(v.BSet)
	default:
		return nil, fmt.Errorf("attr: encoding a value without a type")
	}
	return json.Marshal(map[Type]any{v.Type: payload})
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func nonNilBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

func nonNilMap(m map[string]Value) map[string]Value {
	if m == nil {
		return map[string]Value{}
	}
	return m
}

// UnmarshalJSON reads v from its wire form, validating it the way DynamoDB
// does: exactly one type, NULL only true, sets non-empty and without
// duplicates, numbers valid and normalised. Invalid values give a
// *ValidationError; malformed JSON a JSON error.
func (v *Value) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch len(raw) {
	case 0:
		return Errorf("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
	case 1:
	default:
		return Errorf("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
	}
	for k, payload := range raw {
		decoded, err := decode(Type(k), payload)
		if err != nil {
			return err
		}
		*v = decoded
	}
	return nil
}

func decode(t Type, payload json.RawMessage) (Value, error) {
	v := Value{Type: t}
	var err error
	switch t {
	case S:
		err = json.Unmarshal(payload, &v.S)
	case N:
		var s string
		if err = json.Unmarshal(payload, &s); err == nil {
			v.S, err = normalNumber(s)
		}
	case B:
		err = json.Unmarshal(payload, &v.B)
	case BOOL:
		err = json.Unmarshal(payload, &v.Bool)
	case NULL:
		var b bool
		if err = json.Unmarshal(payload, &b); err == nil && !b {
			err = Errorf("One or more parameter values were invalid: Null attribute value types must have the value of true")
		}
	case L:
		err = json.Unmarshal(payload, &v.L)
		v.L = nonNil(v.L)
	case M:
		err = json.Unmarshal(payload, &v.M)
		v.M = nonNilMap(v.M)
	case SS, NS:
		err = decodeStringSet(&v, payload)
	case BS:
		err = decodeBinarySet(&v, payload)
	default:
		err = Errorf("Supplied AttributeValue has an unsupported datatype: %s", t)
	}
	return v, err
}

func normalNumber(s string) (string, error) {
	n, err := ParseNumber(s)
	if err != nil {
		return "", err
	}
	return n.String(), nil
}

func decodeStringSet(v *Value, payload json.RawMessage) error {
	var members []string
	if err := json.Unmarshal(payload, &members); err != nil {
		return err
	}
	if len(members) == 0 {
		kind := map[Type]string{SS: "string", NS: "number"}[v.Type]
		return Errorf("One or more parameter values were invalid: An %s set  may not be empty", kind)
	}
	out := make([]string, 0, len(members))
	for _, m := range members {
		if v.Type == NS {
			n, err := normalNumber(m)
			if err != nil {
				return err
			}
			m = n
		}
		if slices.Contains(out, m) {
			return Errorf("Input collection [%s] contains duplicates.", strings.Join(members, ", "))
		}
		out = append(out, m)
	}
	v.Set = out
	return nil
}

func decodeBinarySet(v *Value, payload json.RawMessage) error {
	var members [][]byte
	if err := json.Unmarshal(payload, &members); err != nil {
		return err
	}
	if len(members) == 0 {
		return Errorf("One or more parameter values were invalid: Binary sets should not be empty")
	}
	for i, m := range members {
		if containsBytes(members[:i], m) {
			return Errorf("Input collection contains duplicates.")
		}
	}
	v.BSet = members
	return nil
}
