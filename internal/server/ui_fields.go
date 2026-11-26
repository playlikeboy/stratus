package server

import (
	"strconv"
	"strings"
)

// Every form control of the UI is one component, the "field" template: a
// label above the control, an optional hint below, the same look for text,
// numbers, choices, long text and files. Templates build one with field and
// adjust it with val and choose:
//
//	{{template "field" (field "q" "Search" "placeholder=Text in the body" | val .F.Q)}}
//	{{template "field" (field "shard" "Shard" | choose (choices "All shards" .Shards) | val .F.Shard)}}

type uiField struct {
	Name, Label, Value string
	Type               string // input type: text (default), file or checkbox
	Placeholder, Hint  string
	InputMode          string
	Rows               int // above 0: a textarea
	Required, Wide     bool
	Choices            []choice // set: a select
}

type choice struct{ Value, Label string }

// field builds a field. Each option is a flag ("required", "wide") or
// key=value: type, placeholder, hint, inputmode, rows.
func field(name, label string, options ...string) uiField {
	f := uiField{Name: name, Label: label, Type: "text"}
	for _, o := range options {
		key, value, _ := strings.Cut(o, "=")
		switch key {
		case "required":
			f.Required = true
		case "wide":
			f.Wide = true
		case "type":
			f.Type = value
		case "placeholder":
			f.Placeholder = value
		case "hint":
			f.Hint = value
		case "inputmode":
			f.InputMode = value
		case "rows":
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				f.Rows = n
			}
		}
	}
	return f
}

// val sets a field's value; it comes last so that templates can pipe into it.
func val(v string, f uiField) uiField {
	f.Value = v
	return f
}

// choose makes a field a select of the choices.
func choose(cs []choice, f uiField) uiField {
	f.Choices = cs
	return f
}

// choices are values offered as they are, after an optional empty choice
// labelled empty ("All shards", "No dead-letter queue").
func choices(empty string, values []string) []choice {
	var out []choice
	if empty != "" {
		out = append(out, choice{"", empty})
	}
	for _, v := range values {
		out = append(out, choice{v, v})
	}
	return out
}

// withEmpty puts an empty choice before labelled ones.
func withEmpty(empty string, cs []choice) []choice {
	return append([]choice{{"", empty}}, cs...)
}

var keyTypeChoices = []choice{{"S", "String"}, {"N", "Number"}, {"B", "Binary"}}
