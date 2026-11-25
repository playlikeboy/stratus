package attr

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestParseNumber(t *testing.T) {
	tests := []struct {
		in, want string
		wantErr  string
	}{
		{in: "0", want: "0"},
		{in: "-0.000", want: "0"},
		{in: "1.50", want: "1.5"},
		{in: "+12", want: "12"},
		{in: "1e3", want: "1000"},
		{in: "1.5E-3", want: "0.0015"},
		{in: "-120.0e-1", want: "-12"},
		{in: ".5", want: "0.5"},
		{in: "5.", want: "5"},
		{in: "00012300", want: "12300"},
		{in: "12345678901234567890123456789012345678", want: "12345678901234567890123456789012345678"},
		{in: "123456789012345678901234567890123456789", wantErr: "38 significant digits"},
		{in: "1e126", wantErr: "Number overflow"},
		{in: "9.9e125", want: "99" + strings.Repeat("0", 124)},
		{in: "1e-130", want: "0." + strings.Repeat("0", 129) + "1"},
		{in: "1e-131", wantErr: "Number underflow"},
		{in: "", wantErr: "cannot be converted"},
		{in: "abc", wantErr: "cannot be converted"},
		{in: "1.2.3", wantErr: "cannot be converted"},
		{in: "1e", wantErr: "cannot be converted"},
		{in: " 1", wantErr: "cannot be converted"},
		{in: "-", wantErr: "cannot be converted"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			n, err := ParseNumber(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseNumber(%q) error = %v, want one containing %q", tt.in, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseNumber(%q): %v", tt.in, err)
			}
			if got := n.String(); got != tt.want {
				t.Errorf("ParseNumber(%q) = %s, want %s", tt.in, got, tt.want)
			}
		})
	}
}

func mustParse(t *testing.T, s string) Number {
	t.Helper()
	n, err := ParseNumber(s)
	if err != nil {
		t.Fatalf("ParseNumber(%q): %v", s, err)
	}
	return n
}

func TestNumberArithmeticAndOrder(t *testing.T) {
	tests := []struct {
		a, b           string
		cmp            int
		sum, different string
	}{
		{a: "1", b: "2", cmp: -1, sum: "3", different: "-1"},
		{a: "10", b: "9", cmp: 1, sum: "19", different: "1"},
		{a: "0.1", b: "0.2", cmp: -1, sum: "0.3", different: "-0.1"},
		{a: "-5", b: "3", cmp: -1, sum: "-2", different: "-8"},
		{a: "1.0", b: "1", cmp: 0, sum: "2", different: "0"},
		{a: "99999999999999999999999999999999999999", b: "2", cmp: 1, different: "99999999999999999999999999999999999997"},
		{a: "-0.001", b: "-0.01", cmp: 1, sum: "-0.011", different: "0.009"},
	}
	for _, tt := range tests {
		t.Run(tt.a+" "+tt.b, func(t *testing.T) {
			a, b := mustParse(t, tt.a), mustParse(t, tt.b)
			if got := a.Cmp(b); got != tt.cmp {
				t.Errorf("Cmp = %d, want %d", got, tt.cmp)
			}
			sum, err := a.Add(b)
			if tt.sum == "" {
				if err == nil {
					t.Errorf("Add = %s, want an error (more than 38 digits)", sum)
				}
			} else if err != nil || sum.String() != tt.sum {
				t.Errorf("Add = %s, %v, want %s", sum, err, tt.sum)
			}
			if d, err := a.Sub(b); err != nil || d.String() != tt.different {
				t.Errorf("Sub = %s, %v, want %s", d, err, tt.different)
			}
		})
	}
}

func TestValueJSON(t *testing.T) {
	tests := []struct {
		in, want, wantErr string
	}{
		{in: `{"S":"a"}`, want: `{"S":"a"}`},
		{in: `{"N":"01.10"}`, want: `{"N":"1.1"}`},
		{in: `{"B":"AAE="}`, want: `{"B":"AAE="}`},
		{in: `{"BOOL":false}`, want: `{"BOOL":false}`},
		{in: `{"NULL":true}`, want: `{"NULL":true}`},
		{in: `{"L":[{"S":"a"},{"N":"1"}]}`, want: `{"L":[{"S":"a"},{"N":"1"}]}`},
		{in: `{"M":{"k":{"SS":["x","y"]}}}`, want: `{"M":{"k":{"SS":["x","y"]}}}`},
		{in: `{"NS":["1","2.50"]}`, want: `{"NS":["1","2.5"]}`},
		{in: `{"BS":["AA==","AQ=="]}`, want: `{"BS":["AA==","AQ=="]}`},
		{in: `{}`, wantErr: "is empty"},
		{in: `{"S":"a","N":"1"}`, wantErr: "more than one datatypes"},
		{in: `{"NULL":false}`, wantErr: "must have the value of true"},
		{in: `{"SS":[]}`, wantErr: "may not be empty"},
		{in: `{"SS":["a","a"]}`, wantErr: "contains duplicates"},
		{in: `{"NS":["1","1.0"]}`, wantErr: "contains duplicates"},
		{in: `{"N":"x"}`, wantErr: "cannot be converted"},
		{in: `{"L":[{}]}`, wantErr: "is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			var v Value
			err := json.Unmarshal([]byte(tt.in), &v)
			if tt.wantErr != "" {
				var invalid *ValidationError
				if !errors.As(err, &invalid) || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Unmarshal error = %v, want a ValidationError containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			out, err := json.Marshal(v)
			if err != nil || string(out) != tt.want {
				t.Errorf("Marshal = %s, %v, want %s", out, err, tt.want)
			}
		})
	}
}

func TestEqualAndCompare(t *testing.T) {
	ss1 := Value{Type: SS, Set: []string{"a", "b"}}
	ss2 := Value{Type: SS, Set: []string{"b", "a"}}
	if !Equal(ss1, ss2) {
		t.Error("sets with the same members in another order are not equal")
	}
	if Equal(Str("1"), Num(NumberFromInt(1))) {
		t.Error("the string 1 equals the number 1")
	}
	if c, ok := Compare(Num(mustParse(t, "9")), Num(mustParse(t, "10"))); !ok || c >= 0 {
		t.Errorf("9 vs 10 = %d, %v; numbers must compare numerically", c, ok)
	}
	if c, ok := Compare(Str("9"), Str("10")); !ok || c <= 0 {
		t.Errorf(`"9" vs "10" = %d, %v; strings compare bytewise`, c, ok)
	}
	if c, ok := Compare(Bin([]byte{0xff}), Bin([]byte{0x01, 0x00})); !ok || c <= 0 {
		t.Errorf("binary compare = %d, %v, want unsigned bytewise", c, ok)
	}
	if _, ok := Compare(Str("a"), Num(NumberFromInt(1))); ok {
		t.Error("values of different types are ordered")
	}
}

func TestItemSize(t *testing.T) {
	item := Item{"pk": Str("abc"), "n": Num(mustParse(t, "12345")), "b": Bool(true)}
	// pk: 2+3, n: 1+ (5+1)/2+1 = 1+4, b: 1+1
	if got := ItemSize(item); got != 12 {
		t.Errorf("ItemSize = %d, want 12", got)
	}
}
