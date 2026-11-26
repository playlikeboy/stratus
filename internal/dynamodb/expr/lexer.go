// Package expr parses and evaluates DynamoDB expressions: condition, filter,
// key condition, projection and update expressions, with their
// ExpressionAttributeNames and ExpressionAttributeValues placeholders.
package expr

import (
	"strings"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

type tokenKind int

const (
	tEOF      tokenKind = iota
	tIdent              // a bare name or keyword: a, AND, size
	tName               // an attribute name placeholder: #n
	tValue              // an attribute value placeholder: :v
	tInt                // a list index: 3
	tLParen             // (
	tRParen             // )
	tLBracket           // [
	tRBracket           // ]
	tComma              // ,
	tDot                // .
	tCmp                // = <> < <= > >=
	tPlus               // +
	tMinus              // -
)

type token struct {
	kind tokenKind
	text string
}

// keyword reports whether t is the keyword kw, which DynamoDB matches
// without regard to case.
func (t token) keyword(kw string) bool {
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

var punctuation = map[byte]tokenKind{
	'(': tLParen, ')': tRParen, '[': tLBracket, ']': tRBracket,
	',': tComma, '.': tDot, '+': tPlus, '-': tMinus, '=': tCmp,
}

// lex splits an expression into tokens; kind names the expression in errors.
func lex(kind, s string) ([]token, error) {
	var toks []token
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '<' || c == '>':
			j := i + 1
			if j < len(s) && (s[j] == '=' || (c == '<' && s[j] == '>')) {
				j++
			}
			toks = append(toks, token{tCmp, s[i:j]})
			i = j
		case punctuation[c] != tEOF:
			toks = append(toks, token{punctuation[c], s[i : i+1]})
			i++
		case c == '#' || c == ':':
			j := scanWord(s, i+1)
			if j == i+1 {
				return nil, syntaxError(kind, s[i:i+1], toks)
			}
			k := tName
			if c == ':' {
				k = tValue
			}
			toks = append(toks, token{k, s[i:j]})
			i = j
		case isDigit(c):
			j := i
			for j < len(s) && isDigit(s[j]) {
				j++
			}
			toks = append(toks, token{tInt, s[i:j]})
			i = j
		case isWordStart(c):
			j := scanWord(s, i)
			toks = append(toks, token{tIdent, s[i:j]})
			i = j
		default:
			return nil, syntaxError(kind, s[i:i+1], toks)
		}
	}
	return toks, nil
}

func scanWord(s string, i int) int {
	for i < len(s) && (isWordStart(s[i]) || isDigit(s[i])) {
		i++
	}
	return i
}

func isDigit(c byte) bool     { return c >= '0' && c <= '9' }
func isWordStart(c byte) bool { return c == '_' || (c|0x20 >= 'a' && c|0x20 <= 'z') }

// syntaxError reports an unexpected token the way DynamoDB does, naming the
// token and what came just before it.
func syntaxError(kind, tok string, before []token) error {
	near := tok
	if n := len(before); n > 0 {
		near = before[n-1].text + " " + tok
	}
	if tok == "" {
		tok = "<EOF>"
	}
	return attr.Errorf("Invalid %s: Syntax error; token: \"%s\", near: \"%s\"", kind, tok, near)
}
