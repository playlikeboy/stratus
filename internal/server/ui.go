package server

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ODudek/stratus/internal/dynamodb"
	"github.com/ODudek/stratus/internal/kinesis"
	"github.com/ODudek/stratus/internal/s3"
	"github.com/ODudek/stratus/internal/sfn"
)

// The web UI: one page that htmx refreshes with server-rendered fragments.
// It needs no build step and no network: htmx is embedded.

//go:embed ui/fields.html ui/page.html ui/resources.html ui/detail.html ui/style.css ui/htmx-2.0.4.min.js
var uiFiles embed.FS

var uiTemplates = template.Must(template.New("").Funcs(template.FuncMap{
	"bytes":     formatBytes,
	"time":      func(t time.Time) string { return t.Local().Format("2006-01-02 15:04:05") },
	"deref":     func(s *string) string { return *s },
	"decode":    func(s string) decodedRecord { return decodePayload([]byte(s), nil) },
	"hasPrefix": strings.HasPrefix,
	"hasSuffix": strings.HasSuffix,
	"len64":     func(b []byte) int64 { return int64(len(b)) },
	"field":     field,
	"val":       val,
	"choose":    choose,
	"choices":   choices,
	"queueChoices": func(qs []queueSummary) []choice {
		names := make([]string, len(qs))
		for i, q := range qs {
			names[i] = q.Name
		}
		return choices("None", names)
	},
	"keyTypes":     func() []choice { return keyTypeChoices },
	"statusFilter": func() []choice { return withEmpty("Any status", executionStatuses) },
	"keyType":      keyType,
	"projection":   projection,
	"status":       statusLabel,
	"retention":    retention,
	"enum":         humanEnum,
}).ParseFS(uiFiles, "ui/fields.html", "ui/page.html", "ui/resources.html", "ui/detail.html"))

func (s *Server) registerUI() {
	s.control.HandleFunc("GET /_stratus/{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/_stratus/ui", http.StatusFound)
	})
	s.control.HandleFunc("GET /_stratus/ui", s.handleUIPage)
	s.control.HandleFunc("GET /_stratus/ui/htmx.min.js", serveUIFile("ui/htmx-2.0.4.min.js", "text/javascript; charset=utf-8"))
	s.control.HandleFunc("GET /_stratus/ui/style.css", serveUIFile("ui/style.css", "text/css; charset=utf-8"))
	s.control.HandleFunc("GET /_stratus/ui/resources", s.handleUIResources)
	s.registerUIDetails()
	s.control.HandleFunc("POST /_stratus/ui/reset", s.uiAction(func(*http.Request) (string, error) {
		if err := s.Reset(); err != nil {
			return "", err
		}
		return "Reset to the manifest's resources.", nil
	}))
	s.control.HandleFunc("POST /_stratus/ui/expire-ttl", s.uiAction(func(*http.Request) (string, error) {
		return fmt.Sprintf("Deleted %d expired DynamoDB items.", s.ExpireTTL()), nil
	}))
	s.control.HandleFunc("POST /_stratus/ui/sqs/purge", s.uiAction(func(r *http.Request) (string, error) {
		name := r.FormValue("name")
		if err := s.targets.SQS.Purge(name); err != nil {
			return "", err
		}
		return "Purged queue " + name + ".", nil
	}))
	s.control.HandleFunc("POST /_stratus/ui/s3/empty", s.uiAction(func(r *http.Request) (string, error) {
		name := r.FormValue("name")
		n, err := s.targets.S3.EmptyBucket(name)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Emptied bucket %s: %d keys removed.", name, n), nil
	}))
}

func (s *Server) handleUIPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = uiTemplates.ExecuteTemplate(w, "page.html", s.overview(""))
}

func serveUIFile(name, contentType string) http.HandlerFunc {
	body, _ := uiFiles.ReadFile(name) // embedded: cannot fail
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(body)
	}
}

func (s *Server) handleUIResources(w http.ResponseWriter, _ *http.Request) {
	s.renderResources(w, "")
}

// uiAction runs a change asked for from the UI and answers with the
// refreshed resources and what happened. It takes only requests htmx made:
// a plain form on another site cannot set the HX-Request header, so it
// cannot reset or purge the emulator behind its user's back.
func (s *Server) uiAction(do func(*http.Request) (string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("HX-Request") != "true" {
			http.Error(w, "only the stratus UI may post here", http.StatusForbidden)
			return
		}
		notice, err := do(r)
		if err != nil {
			notice = "Failed: " + err.Error()
		}
		s.renderResources(w, notice)
	}
}

func (s *Server) renderResources(w http.ResponseWriter, notice string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = uiTemplates.ExecuteTemplate(w, "resources.html", s.overview(notice))
}

// overview is everything the UI shows.
type overview struct {
	Notice   string
	Region   string
	Account  string
	Services []string
	At       string
	Queues   []queueSummary
	Streams  []kinesis.StreamSummary
	Buckets  []s3.BucketSummary
	Tables   []dynamodb.TableSummary
	Machines []sfn.MachineSummary
}

type queueSummary struct {
	Name                       string
	Visible, InFlight, Delayed int
	DeadLetter                 bool
}

func (s *Server) overview(notice string) overview {
	t := s.targets
	return overview{
		Notice:   notice,
		Region:   s.identity.Region,
		Account:  s.identity.Account,
		Services: s.gateway.Services(),
		At:       time.Now().Format("15:04:05"),
		Queues:   s.queueSummaries(),
		Streams:  t.Kinesis.Streams(),
		Buckets:  t.S3.Buckets(),
		Tables:   t.DynamoDB.Tables(),
		Machines: t.SFN.StateMachines(),
	}
}

func (s *Server) queueSummaries() []queueSummary {
	var out []queueSummary
	for _, name := range s.targets.SQS.ListQueues("") {
		attrs, err := s.targets.SQS.Attributes(name)
		if err != nil {
			continue // deleted since it was listed
		}
		n := func(key string) int { v, _ := strconv.Atoi(attrs[key]); return v }
		out = append(out, queueSummary{
			Name:       name,
			Visible:    n("ApproximateNumberOfMessages"),
			InFlight:   n("ApproximateNumberOfMessagesNotVisible"),
			Delayed:    n("ApproximateNumberOfMessagesDelayed"),
			DeadLetter: attrs["RedrivePolicy"] != "",
		})
	}
	return out
}

// formatBytes is a size for people: 512 B, 3.4 KiB, 12.0 MiB.
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
