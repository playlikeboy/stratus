// Package attr models DynamoDB attribute values: their wire form, numbers of
// arbitrary precision, comparison and the size DynamoDB accounts for them.
package attr

import (
	"fmt"
	"slices"
)

// Type is a DynamoDB data type descriptor, as written on the wire.
type Type string

const (
	S    Type = "S"
	N    Type = "N"
	B    Type = "B"
	BOOL Type = "BOOL"
	NULL Type = "NULL"
	L    Type = "L"
	M    Type = "M"
	SS   Type = "SS"
	NS   Type = "NS"
	BS   Type = "BS"
)

// Types lists every data type, for attribute_type.
var Types = []Type{S, N, B, BOOL, NULL, L, M, SS, NS, BS}

// Value is one attribute value. Values are never changed once built: an
// update builds new containers along the path it changes.
type Value struct {
	Type Type
	// S holds a string, or a number in its normalised form.
	S    string
	B    []byte
	Bool bool
	L    []Value
	M    map[string]Value
	// Set holds the members of SS and NS (numbers normalised), in the order
	// they were first added; BSet the members of BS.
	Set  []string
	BSet [][]byte
}

// Item is an item, or a key: attribute name to value.
type Item = map[string]Value

// Str returns an S value.
func Str(s string) Value { return Value{Type: S, S: s} }

// Num returns an N value.
func Num(n Number) Value { return Value{Type: N, S: n.String()} }

// Bin returns a B value.
func Bin(b []byte) Value { return Value{Type: B, B: b} }

// Bool returns a BOOL value.
func Bool(b bool) Value { return Value{Type: BOOL, Bool: b} }

// List returns an L value.
func List(l []Value) Value { return Value{Type: L, L: l} }

// Map returns an M value.
func Map(m map[string]Value) Value { return Value{Type: M, M: m} }

// Number returns the number of an N value; it panics on any other type,
// which is a programming error.
func (v Value) Number() Number {
	n, ok := parseDecimal(v.S)
	if v.Type != N || !ok {
		panic(fmt.Sprintf("attr: Number of a %s value %q", v.Type, v.S))
	}
	return n
}

// IsSet reports whether v is SS, NS or BS.
func (v Value) IsSet() bool { return v.Type == SS || v.Type == NS || v.Type == BS }

// Len is the member count of a set, list or map.
func (v Value) Len() int {
	switch v.Type {
	case L:
		return len(v.L)
	case M:
		return len(v.M)
	case BS:
		return len(v.BSet)
	default:
		return len(v.Set)
	}
}

// Union returns the members of a and b, two sets of the same type.
func Union(a, b Value) Value {
	out := Value{Type: a.Type, Set: slices.Clone(a.Set), BSet: slices.Clone(a.BSet)}
	for _, s := range b.Set {
		if !slices.Contains(out.Set, s) {
			out.Set = append(out.Set, s)
		}
	}
	for _, bs := range b.BSet {
		if !containsBytes(out.BSet, bs) {
			out.BSet = append(out.BSet, bs)
		}
	}
	return out
}

// Difference returns the members of a that are not in b.
func Difference(a, b Value) Value {
	out := Value{Type: a.Type}
	for _, s := range a.Set {
		if !slices.Contains(b.Set, s) {
			out.Set = append(out.Set, s)
		}
	}
	for _, bs := range a.BSet {
		if !containsBytes(b.BSet, bs) {
			out.BSet = append(out.BSet, bs)
		}
	}
	return out
}

// HasMember reports whether the set s holds the scalar v of the matching type.
func HasMember(s, v Value) bool {
	switch {
	case s.Type == SS && v.Type == S, s.Type == NS && v.Type == N:
		return slices.Contains(s.Set, v.S)
	case s.Type == BS && v.Type == B:
		return containsBytes(s.BSet, v.B)
	}
	return false
}

func containsBytes(set [][]byte, b []byte) bool {
	return slices.ContainsFunc(set, func(m []byte) bool { return string(m) == string(b) })
}

// Size is the number of bytes DynamoDB accounts for v.
func Size(v Value) int {
	switch v.Type {
	case S:
		return len(v.S)
	case N:
		return v.Number().size()
	case B:
		return len(v.B)
	case BOOL, NULL:
		return 1
	case L:
		n := 3
		for _, e := range v.L {
			n += 1 + Size(e)
		}
		return n
	case M:
		n := 3
		for k, e := range v.M {
			n += 1 + len(k) + Size(e)
		}
		return n
	case NS:
		n := 0
		for _, s := range v.Set {
			n += mustNumber(s).size()
		}
		return n
	case BS:
		n := 0
		for _, b := range v.BSet {
			n += len(b)
		}
		return n
	default:
		n := 0
		for _, s := range v.Set {
			n += len(s)
		}
		return n
	}
}

func mustNumber(s string) Number {
	n, _ := parseDecimal(s)
	return n
}

// ItemSize is the number of bytes DynamoDB accounts for an item: every
// attribute name and value.
func ItemSize(item Item) int {
	n := 0
	for k, v := range item {
		n += len(k) + Size(v)
	}
	return n
}
