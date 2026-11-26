package server

import (
	"regexp"
	"strings"
	"testing"
)

// The panel has one field and one dropdown: no template builds its own.
func TestTemplatesUseTheSharedComponents(t *testing.T) {
	details := regexp.MustCompile(`<details[^>]*>`)
	control := regexp.MustCompile(`<(select|textarea|input)\b[^>]*>`)
	for _, name := range []string{"ui/page.html", "ui/resources.html", "ui/detail.html"} {
		b, err := uiFiles.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range details.FindAllString(string(b), -1) {
			if !strings.Contains(d, `class="dropdown `) {
				t.Errorf("%s: %s is not the shared dropdown", name, d)
			}
		}
		for _, c := range control.FindAllString(string(b), -1) {
			if !strings.Contains(c, `type="hidden"`) {
				t.Errorf("%s: %s is not the shared field", name, c)
			}
		}
	}
}

// The create panel shows nothing to fill until a type is chosen, then only
// that type's settings.
func TestCreateFormFollowsTheChosenType(t *testing.T) {
	_, base := newOpsServer(t)
	_, page := get(t, base+"/_stratus/ui")
	if !strings.Contains(page, "Choose a type to set up the new resource.") || strings.Contains(page, "/_stratus/ui/create/queue") {
		t.Errorf("before a type is chosen the panel should show no settings")
	}
	for kind, want := range map[string]string{"queue": "Dead-letter queue", "stream": "Shards", "bucket": "Keep every version",
		"table": "Partition key", "machine": "Amazon States Language"} {
		_, form := get(t, base+"/_stratus/ui/create-form?type="+kind)
		if !strings.Contains(form, want) || !strings.Contains(form, "/_stratus/ui/create/"+kind) {
			t.Errorf("the %s form lacks %q", kind, want)
		}
		for other := range map[string]bool{"queue": true, "stream": true, "bucket": true, "table": true, "machine": true} {
			if other != kind && strings.Contains(form, "/_stratus/ui/create/"+other+`"`) {
				t.Errorf("the %s form also shows the %s form", kind, other)
			}
		}
	}
}

// A dropdown is the panel's own: a button and a menu, its value in a hidden input.
func TestDropdownIsTheCustomOne(t *testing.T) {
	_, base := newOpsServer(t)
	_, page := get(t, base+"/_stratus/ui/dynamodb/orders?index=by-status")
	for _, want := range []string{`data-select`, `role="combobox"`, `role="listbox"`, `<input type="hidden" name="index" value="by-status">`,
		`aria-selected="true">by-status</li>`, `/_stratus/ui/dropdown.js`} {
		if !strings.Contains(page, want) {
			t.Errorf("the table page's dropdown lacks %s", want)
		}
	}
	if strings.Contains(page, "<select") {
		t.Error("a native select is still rendered")
	}
}
