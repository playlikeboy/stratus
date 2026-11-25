package attr

import (
	"math/big"
	"strconv"
	"strings"
)

// DynamoDB numbers hold up to 38 significant digits, with a magnitude from
// 1E-130 to 9.99…E+125.
const (
	maxDigits   = 38
	maxExponent = 125
	minExponent = -130
)

// Number is an exact decimal: coef × 10^exp. A normalised Number has no
// trailing zeros in coef, and zero is always {0, 0}, so two equal numbers
// have the same normalised form and the same String.
type Number struct {
	coef *big.Int
	exp  int
}

var bigTen = big.NewInt(10)

// ParseNumber reads a DynamoDB number ("12", "-1.50", "3e4") and normalises
// it; it fails on anything else and on numbers DynamoDB cannot store.
func ParseNumber(s string) (Number, error) {
	n, ok := parseDecimal(s)
	if !ok {
		return Number{}, Errorf("A value provided cannot be converted into a number")
	}
	return n, n.validate()
}

// parseDecimal reads [+-]digits[.digits][(e|E)[+-]digits], needing at least
// one digit in the mantissa.
func parseDecimal(s string) (Number, bool) {
	i, neg := 0, false
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	var digits strings.Builder
	frac, seenDot := 0, false
	for ; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			digits.WriteByte(c)
			if seenDot {
				frac++
			}
		case c == '.' && !seenDot:
			seenDot = true
		default:
			goto mantissaDone
		}
	}
mantissaDone:
	if digits.Len() == 0 {
		return Number{}, false
	}
	exp := 0
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		rest := s[i+1:]
		if len(rest) == 0 || len(rest) > 6 {
			return Number{}, false
		}
		e, err := strconv.Atoi(rest)
		if err != nil || rest[len(rest)-1] < '0' || rest[len(rest)-1] > '9' {
			return Number{}, false
		}
		exp = e
		i = len(s)
	}
	if i != len(s) {
		return Number{}, false
	}
	coef, _ := new(big.Int).SetString(digits.String(), 10)
	if neg {
		coef.Neg(coef)
	}
	return normalise(coef, exp-frac), true
}

// normalise strips the trailing zeros of coef; it owns coef.
func normalise(coef *big.Int, exp int) Number {
	if coef.Sign() == 0 {
		return Number{coef: new(big.Int), exp: 0}
	}
	q, r := new(big.Int), new(big.Int)
	for {
		q.QuoRem(coef, bigTen, r)
		if r.Sign() != 0 {
			return Number{coef: coef, exp: exp}
		}
		coef.Set(q)
		exp++
	}
}

func (n Number) digits() int {
	if n.coef.Sign() == 0 {
		return 1
	}
	return len(new(big.Int).Abs(n.coef).String())
}

// validate enforces DynamoDB's precision and range.
func (n Number) validate() error {
	if n.coef.Sign() == 0 {
		return nil
	}
	d := n.digits()
	if d > maxDigits {
		return Errorf("Attempting to store more than 38 significant digits in a Number")
	}
	switch adjusted := n.exp + d - 1; {
	case adjusted > maxExponent:
		return Errorf("Number overflow. Attempting to store a number with magnitude larger than supported range")
	case adjusted < minExponent:
		return Errorf("Number underflow. Attempting to store a number with magnitude smaller than supported range")
	}
	return nil
}

// String renders n the way DynamoDB returns numbers: plain decimal notation,
// no exponent, no trailing zeros.
func (n Number) String() string {
	if n.coef.Sign() == 0 {
		return "0"
	}
	ds := new(big.Int).Abs(n.coef).String()
	sign := ""
	if n.coef.Sign() < 0 {
		sign = "-"
	}
	if n.exp >= 0 {
		return sign + ds + strings.Repeat("0", n.exp)
	}
	point := len(ds) + n.exp
	if point > 0 {
		return sign + ds[:point] + "." + ds[point:]
	}
	return sign + "0." + strings.Repeat("0", -point) + ds
}

// align returns the coefficients of a and b scaled to their common exponent.
func align(a, b Number) (x, y *big.Int, exp int) {
	exp = min(a.exp, b.exp)
	scale := func(n Number) *big.Int {
		f := new(big.Int).Exp(bigTen, big.NewInt(int64(n.exp-exp)), nil)
		return f.Mul(f, n.coef)
	}
	return scale(a), scale(b), exp
}

// Cmp compares a and b numerically: -1, 0 or +1.
func (n Number) Cmp(o Number) int {
	if s1, s2 := n.coef.Sign(), o.coef.Sign(); s1 != s2 {
		if s1 < s2 {
			return -1
		}
		return 1
	}
	x, y, _ := align(n, o)
	return x.Cmp(y)
}

// Add returns n + o, failing when the exact result cannot be stored.
func (n Number) Add(o Number) (Number, error) {
	x, y, exp := align(n, o)
	sum := normalise(x.Add(x, y), exp)
	return sum, sum.validate()
}

// Sub returns n - o, failing when the exact result cannot be stored.
func (n Number) Sub(o Number) (Number, error) {
	return n.Add(Number{coef: new(big.Int).Neg(o.coef), exp: o.exp})
}

// NumberFromInt returns i as a Number.
func NumberFromInt(i int64) Number { return normalise(big.NewInt(i), 0) }

// size is the storage size DynamoDB accounts for a number: about one byte per
// two significant digits, plus one.
func (n Number) size() int { return (n.digits()+1)/2 + 1 }
