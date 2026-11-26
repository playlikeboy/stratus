package server

import (
	"encoding/base64"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ODudek/stratus/internal/kinesis"
	"github.com/ODudek/stratus/internal/s3"
	"github.com/ODudek/stratus/internal/sfn"
	"github.com/ODudek/stratus/internal/sqs"
)

// How much a detail page shows: enough to look at, little enough to render
// fast on a resource with millions of entries.
const (
	maxPeekMessages   = 100
	maxPeekRecords    = 100
	maxPeekObjects    = 500
	maxPeekItems      = 100
	maxPeekExecutions = 50
	maxObjectPreview  = 64 << 10
)

// detailPage is what a detail page shows; Kind picks the part that applies.
type detailPage struct {
	Kind, Service, Name string
	Region, Account     string
	At, Summary, Error  string
	Self                string // the page's own URL, which htmx polls

	Messages   []sqs.PeekedMessage
	Records    []kinesis.RecordView
	Objects    []s3.ObjectView
	Prefix     string
	Bucket     string
	Object     *objectPreview
	Items      []string
	Executions []sfn.ExecutionView
	Definition string
}

type objectPreview struct {
	View  s3.ObjectView
	Head  []byte
	Whole bool
}

func (s *Server) registerUIDetails() {
	s.control.HandleFunc("GET /_stratus/ui/sqs/{name}", s.uiDetail("queue", "SQS", func(p *detailPage, r *http.Request) error {
		msgs, err := s.targets.SQS.Peek(p.Name, maxPeekMessages)
		p.Messages = msgs
		p.Summary = limited(len(msgs), maxPeekMessages, "message", "messages")
		return err
	}))
	s.control.HandleFunc("GET /_stratus/ui/kinesis/{name}", s.uiDetail("stream", "Kinesis", func(p *detailPage, r *http.Request) error {
		recs, err := s.targets.Kinesis.RecentRecords(p.Name, maxPeekRecords)
		p.Records = recs
		p.Summary = "newest " + limited(len(recs), maxPeekRecords, "record", "records")
		return err
	}))
	s.control.HandleFunc("GET /_stratus/ui/s3/{name}", s.uiDetail("bucket", "S3", func(p *detailPage, r *http.Request) error {
		p.Prefix = r.URL.Query().Get("prefix")
		objs, more, err := s.targets.S3.ListObjectViews(p.Name, p.Prefix, maxPeekObjects)
		p.Objects = objs
		p.Summary = fmt.Sprintf("%d objects", len(objs))
		if more {
			p.Summary = fmt.Sprintf("the first %d objects; filter by prefix to see others", maxPeekObjects)
		}
		return err
	}))
	s.control.HandleFunc("GET /_stratus/ui/s3/{name}/object", s.uiDetail("object", "S3", func(p *detailPage, r *http.Request) error {
		key := r.URL.Query().Get("key")
		p.Bucket = p.Name
		head, view, whole, err := s.targets.S3.ObjectHead(p.Bucket, key, maxObjectPreview)
		p.Name = p.Bucket + "/" + key
		p.Object = &objectPreview{View: view, Head: head, Whole: whole}
		p.Summary = "object"
		return err
	}))
	s.control.HandleFunc("GET /_stratus/ui/s3/{name}/download", s.handleUIDownload)
	s.control.HandleFunc("GET /_stratus/ui/dynamodb/{name}", s.uiDetail("table", "DynamoDB", func(p *detailPage, r *http.Request) error {
		items, total, err := s.targets.DynamoDB.PeekItems(p.Name, maxPeekItems)
		p.Items = items
		p.Summary = fmt.Sprintf("%d of %d items, in key order", len(items), total)
		return err
	}))
	s.control.HandleFunc("GET /_stratus/ui/states/{name}", s.uiDetail("machine", "Step Functions", func(p *detailPage, r *http.Request) error {
		def, execs, err := s.targets.SFN.MachineDetail(p.Name, maxPeekExecutions)
		p.Definition, p.Executions = def, execs
		p.Summary = limited(len(execs), maxPeekExecutions, "execution", "executions")
		return err
	}))
}

// uiDetail serves a detail page: the whole document on a plain request, only
// its body when htmx polls it.
func (s *Server) uiDetail(kind, service string, load func(*detailPage, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := &detailPage{
			Kind: kind, Service: service, Name: r.PathValue("name"),
			Region: s.identity.Region, Account: s.identity.Account,
			At: time.Now().Format("15:04:05"), Self: r.URL.RequestURI(),
		}
		status := http.StatusOK
		if err := load(p, r); err != nil {
			p.Error = err.Error()
			status = http.StatusNotFound
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		name := "detail.html"
		if r.Header.Get("HX-Request") == "true" {
			name = "detail-body"
			status = http.StatusOK // htmx swaps only successful answers
		}
		w.WriteHeader(status)
		_ = uiTemplates.ExecuteTemplate(w, name, p)
	}
}

// handleUIDownload sends an object as a file to save. It is never shown
// inline: an uploaded page must not run as part of the UI.
func (s *Server) handleUIDownload(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.PathValue("name"), r.URL.Query().Get("key")
	err := s.targets.S3.WriteObject(w, bucket, key, func(v s3.ObjectView) {
		h := w.Header()
		h.Set("Content-Type", "application/octet-stream")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": path.Base(key)}))
		h.Set("Content-Length", fmt.Sprint(v.Size))
	})
	if err != nil && w.Header().Get("Content-Disposition") == "" {
		http.Error(w, err.Error(), http.StatusNotFound)
	}
}

// displayText shows bytes as text when they are printable UTF-8, else as
// base64, so that binary data neither garbles the page nor hides.
func displayText(b []byte) string {
	if utf8.Valid(b) && strings.IndexFunc(string(b), func(r rune) bool {
		return !unicode.IsPrint(r) && !unicode.IsSpace(r)
	}) < 0 {
		return string(b)
	}
	return "base64: " + base64.StdEncoding.EncodeToString(b)
}

func limited(n, max int, one, many string) string {
	word := many
	if n == 1 {
		word = one
	}
	if n == max {
		return fmt.Sprintf("the first %d %s", n, word)
	}
	return fmt.Sprintf("%d %s", n, word)
}
