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
