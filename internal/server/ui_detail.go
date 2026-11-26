package server

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/dynamodb"
	"github.com/stratus-emulator/stratus/internal/kinesis"
	"github.com/stratus-emulator/stratus/internal/s3"
	"github.com/stratus-emulator/stratus/internal/sfn"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

// How much a detail page shows: enough to look at, little enough to render
// fast on a resource with millions of entries.
const (
	maxShown         = 100
	maxShownObjects  = 500
	maxScanRecords   = 2000 // the newest records a stream search looks through
	maxObjectPreview = 64 << 10
)

// detailPage is what a detail page shows: the resource's settings (Info),
// the filters of its search, its results and the notice of the last action.
// Kind picks the parts that apply.
type detailPage struct {
	Kind, Service, Name string
	Region, Account     string
	At, Summary, Error  string
	Notice              string
	Self                string // the page's path, which htmx polls with the filters
	F                   filters

	Queue    *queueInfo
	Stream   *kinesis.StreamInfo
	Bucket   *s3.BucketInfo
	Table    *dynamodb.TableInfo
	Machine  *sfn.MachineInfo
	Shards   []string // a stream's shard ids, for the shard filter
	Indexes  []string // a table's index names, for the index filter
	Messages []sqs.PeekedMessage
	Records  []recordRow
	Objects  []s3.ObjectView
	Object   *objectPreview
	Items    []string
	Runs     []sfn.ExecutionView

	// A table's key in DynamoDB JSON with empty values, to start the put and
	// delete forms from.
	ItemTemplate, KeyTemplate string
}

// filters are a detail page's search fields, read from the query of a look
// or the form of an action.
type filters struct {
	Q, Key, Shard, Prefix, Index, Status string
}

func filtersOf(r *http.Request) filters {
	v := func(k string) string { return strings.TrimSpace(r.FormValue(k)) }
	return filters{Q: v("q"), Key: v("key"), Shard: v("shard"), Prefix: v("prefix"), Index: v("index"), Status: v("status")}
}

type queueInfo struct {
	Attributes  []attribute
	DLQ         string   // the queue this one sends failed messages to
	MaxReceives string   // receives before a message moves to the DLQ
	Sources     []string // the queues that send their failed messages here
}

type attribute struct{ Name, Value string }

type recordRow struct {
	kinesis.RecordView
	Decoded []decodedRecord
}

type objectPreview struct {
	View  s3.ObjectView
	Head  decodedRecord
	Whole bool
}

func (s *Server) registerUIDetails() {
	s.detail("sqs", "queue", "SQS", s.loadQueue)
	s.detail("kinesis", "stream", "Kinesis", s.loadStream)
	s.detail("s3", "bucket", "S3", s.loadBucket)
	s.detail("dynamodb", "table", "DynamoDB", s.loadTable)
	s.detail("states", "machine", "Step Functions", s.loadMachine)
	s.control.HandleFunc("GET /_stratus/ui/s3/{name}/object", s.uiDetail("object", "S3", s.loadObject))
	s.control.HandleFunc("GET /_stratus/ui/s3/{name}/download", s.handleUIDownload)
	s.registerUIOperations()
	s.registerUIResources()
}

func (s *Server) detail(prefix, kind, service string, load func(*detailPage, *http.Request) error) {
	s.control.HandleFunc("GET /_stratus/ui/"+prefix+"/{name}", s.uiDetail(kind, service, load))
}

func (s *Server) newDetail(kind, service string, r *http.Request) *detailPage {
	return &detailPage{
		Kind: kind, Service: service, Name: r.PathValue("name"),
		Region: s.identity.Region, Account: s.identity.Account,
		At: time.Now().Format("15:04:05"), Self: r.URL.Path, F: filtersOf(r),
	}
}

// uiDetail serves a detail page: the whole document on a plain request, its
// polled part when htmx asks.
func (s *Server) uiDetail(kind, service string, load func(*detailPage, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := s.newDetail(kind, service, r)
		status := http.StatusOK
		if err := load(p, r); err != nil {
			p.Error, status = err.Error(), http.StatusNotFound
		}
		name := "detail.html"
		if r.Header.Get("HX-Request") == "true" {
			name, status = "detail-body", http.StatusOK // htmx swaps only successful answers
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_ = uiTemplates.ExecuteTemplate(w, name, p)
	}
}

func (s *Server) loadQueue(p *detailPage, _ *http.Request) error {
	attrs, err := s.targets.SQS.Attributes(p.Name)
	if err != nil {
		return err
	}
	info := &queueInfo{}
	info.Attributes = labelledAttributes(attrs)
	var redrive struct {
		DeadLetterTargetArn string
		MaxReceiveCount     json.Number
	}
	if json.Unmarshal([]byte(attrs["RedrivePolicy"]), &redrive) == nil && redrive.DeadLetterTargetArn != "" {
		arn := redrive.DeadLetterTargetArn
		info.DLQ = arn[strings.LastIndex(arn, ":")+1:]
		info.MaxReceives = redrive.MaxReceiveCount.String()
	}
	info.Sources, _, _ = s.targets.SQS.ListDeadLetterSourceQueues(p.Name, 1000, "")
	for i, u := range info.Sources {
		info.Sources[i] = path.Base(u)
	}
	p.Queue = info
	msgs, matched, err := s.targets.SQS.Peek(p.Name, p.F.Q, maxShown)
	p.Messages, p.Summary = msgs, shown(len(msgs), matched, "message", "messages")
	return err
}

func (s *Server) loadStream(p *detailPage, _ *http.Request) error {
	info, err := s.targets.Kinesis.Stream(p.Name)
	if err != nil {
		return err
	}
	p.Stream = &info
	for _, sh := range info.Shards {
		p.Shards = append(p.Shards, sh.ID)
	}
	recent, err := s.targets.Kinesis.RecentRecords(p.Name, maxScanRecords)
	if err != nil {
		return err
	}
	matched := 0
	for _, rec := range recent {
		if p.F.Shard != "" && rec.Shard != p.F.Shard {
			continue
		}
		row := recordRow{RecordView: rec, Decoded: decodeRecord(rec.Data)}
		if !row.matches(p.F.Key, strings.ToLower(p.F.Q)) {
			continue
		}
		matched++
		if len(p.Records) < maxShown {
			p.Records = append(p.Records, row)
		}
	}
	p.Summary = shown(len(p.Records), matched, "record", "records") + fmt.Sprintf(" among the newest %d", len(recent))
	return nil
}

// matches tells whether a record, or a user record inside it, has the
// partition key key and contains the text q (both optional).
func (row recordRow) matches(key, q string) bool {
	for _, d := range row.Decoded {
		pk := row.PartitionKey
		if d.PartitionKey != "" {
			pk = d.PartitionKey
		}
		if (key == "" || pk == key || row.PartitionKey == key) && (q == "" || strings.Contains(strings.ToLower(d.Text), q)) {
			return true
		}
	}
	return false
}

func (s *Server) loadBucket(p *detailPage, _ *http.Request) error {
	info, err := s.targets.S3.Bucket(p.Name)
	if err != nil {
		return err
	}
	p.Bucket = &info
	objs, more, err := s.targets.S3.ListObjectViews(p.Name, p.F.Prefix, p.F.Q, maxShownObjects)
	p.Objects = objs
	p.Summary = fmt.Sprintf("%d objects", len(objs))
	if more {
		p.Summary = fmt.Sprintf("the first %d matching objects; narrow the search to see others", maxShownObjects)
	}
	return err
}

func (s *Server) loadObject(p *detailPage, r *http.Request) error {
	key := r.FormValue("key")
	head, view, whole, err := s.targets.S3.ObjectHead(p.Name, key, maxObjectPreview)
	if err != nil {
		return err
	}
	p.Object = &objectPreview{View: view, Head: decodePayload(head, nil), Whole: whole}
	p.Summary = "object " + key
	p.Self = r.URL.Path + "?key=" + url.QueryEscape(key) // polled with its key
	return nil
}

// keyTemplate is a table's key in DynamoDB JSON with empty values.
func keyTemplate(spec dynamodb.TableSpec) string {
	example := func(typ string) string {
		if typ == "N" {
			return "0"
		}
		return ""
	}
	key := map[string]map[string]string{spec.PartitionKey.Name: {spec.PartitionKey.Type: example(spec.PartitionKey.Type)}}
	if spec.SortKey != nil {
		key[spec.SortKey.Name] = map[string]string{spec.SortKey.Type: example(spec.SortKey.Type)}
	}
	b, _ := json.MarshalIndent(key, "", "  ") // maps of strings always marshal
	return string(b)
}

func (s *Server) loadTable(p *detailPage, _ *http.Request) error {
	info, err := s.targets.DynamoDB.Table(p.Name)
	if err != nil {
		return err
	}
	p.Table = &info
	for _, idx := range append(slices.Clone(info.Spec.LocalIndexes), info.Spec.GlobalIndexes...) {
		p.Indexes = append(p.Indexes, idx.Name)
	}
	p.KeyTemplate = keyTemplate(info.Spec)
	p.ItemTemplate = p.KeyTemplate
	items, matched, err := s.targets.DynamoDB.SearchItems(p.Name,
		dynamodb.ItemSearch{Index: p.F.Index, PartitionKey: p.F.Key, Contains: p.F.Q}, maxShown)
	p.Items, p.Summary = items, shown(len(items), matched, "item", "items")
	return err
}

func (s *Server) loadMachine(p *detailPage, _ *http.Request) error {
	info, runs, matched, err := s.targets.SFN.MachineDetail(p.Name, p.F.Status, p.F.Q, maxShown)
	if err != nil {
		return err
	}
	p.Machine, p.Runs = &info, runs
	p.Summary = shown(len(runs), matched, "execution", "executions")
	return nil
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

func shown(n, matched int, one, many string) string {
	word := many
	if matched == 1 {
		word = one
	}
	if n < matched {
		return fmt.Sprintf("showing %d of %d %s", n, matched, word)
	}
	return fmt.Sprintf("%d %s", matched, word)
}
