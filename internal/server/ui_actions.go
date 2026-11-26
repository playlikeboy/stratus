package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ODudek/stratus/internal/sqs"
)

// maxUpload is how much of an uploaded file is held in memory while the
// upload is read; the rest goes to a temporary file until it is stored.
const maxUpload = 32 << 20

// An operation changes a resource from its detail page and answers with the
// page's refreshed part, the filters kept, and what happened.
type operation func(p *detailPage, r *http.Request) (string, error)

func (s *Server) registerUIOperations() {
	q := func(op string, do operation) { s.operation("sqs", "queue", "SQS", s.loadQueue, op, do) }
	q("send", s.opSendMessage)
	q("delete", func(p *detailPage, r *http.Request) (string, error) {
		id := r.FormValue("id")
		return "Deleted message " + id + ".", s.targets.SQS.DeleteByID(p.Name, id)
	})
	q("purge", func(p *detailPage, _ *http.Request) (string, error) {
		return "Purged the queue.", s.targets.SQS.Purge(p.Name)
	})
	q("redrive", func(p *detailPage, r *http.Request) (string, error) {
		source := r.FormValue("source")
		n, err := s.targets.SQS.Redrive(p.Name, source)
		return fmt.Sprintf("Moved %d messages back to %s.", n, source), err
	})

	s.operation("kinesis", "stream", "Kinesis", s.loadStream, "put", func(p *detailPage, r *http.Request) (string, error) {
		key := r.FormValue("partition_key")
		err := s.callJSON("Kinesis_20131202.PutRecord", "application/x-amz-json-1.1", map[string]string{
			"StreamName": p.Name, "PartitionKey": key, "Data": base64.StdEncoding.EncodeToString([]byte(r.FormValue("data"))),
		})
		return "Put a record with partition key " + key + ".", err
	})

	b := func(op string, do operation) { s.operation("s3", "bucket", "S3", s.loadBucket, op, do) }
	b("upload", s.opUpload)
	b("delete", func(p *detailPage, r *http.Request) (string, error) {
		key := r.FormValue("object")
		return "Deleted " + key + ".", s.callS3(http.MethodDelete, p.Name, key, nil, 0, "")
	})
	b("empty", func(p *detailPage, _ *http.Request) (string, error) {
		n, err := s.targets.S3.EmptyBucket(p.Name)
		return fmt.Sprintf("Emptied the bucket: %d keys removed.", n), err
	})

	t := func(op string, do operation) { s.operation("dynamodb", "table", "DynamoDB", s.loadTable, op, do) }
	t("put", func(p *detailPage, r *http.Request) (string, error) {
		item, err := jsonField(r, "item")
		if err != nil {
			return "", err
		}
		return "Put the item.", s.callJSON("DynamoDB_20120810.PutItem", "application/x-amz-json-1.0",
			map[string]any{"TableName": p.Name, "Item": item})
	})
	t("delete", func(p *detailPage, r *http.Request) (string, error) {
		key, err := jsonField(r, "item_key")
		if err != nil {
			return "", err
		}
		return "Deleted the item, if it existed.", s.callJSON("DynamoDB_20120810.DeleteItem", "application/x-amz-json-1.0",
			map[string]any{"TableName": p.Name, "Key": key})
	})
	t("expire-ttl", func(*detailPage, *http.Request) (string, error) {
		return fmt.Sprintf("Deleted %d expired items.", s.ExpireTTL()), nil
	})

	m := func(op string, do operation) {
		s.operation("states", "machine", "Step Functions", s.loadMachine, op, do)
	}
	m("start", func(p *detailPage, r *http.Request) (string, error) {
		in := map[string]any{"stateMachineArn": s.targets.SFN.StateMachineARN(p.Name), "input": orEmptyObject(r.FormValue("input"))}
		if name := strings.TrimSpace(r.FormValue("execution")); name != "" {
			in["name"] = name
		}
		return "Started an execution.", s.callJSON("AWSStepFunctions.StartExecution", "application/x-amz-json-1.0", in)
	})
	m("stop", func(p *detailPage, r *http.Request) (string, error) {
		name := r.FormValue("execution")
		return "Stopped " + name + ".", s.callJSON("AWSStepFunctions.StopExecution", "application/x-amz-json-1.0",
			map[string]string{"executionArn": s.identity.ARN("states", "execution:"+p.Name+":"+name), "cause": "Stopped from the stratus UI"})
	})
}

// operation registers POST /_stratus/ui/<prefix>/{name}/<op>. Like the
// overview's actions it takes only requests htmx made.
func (s *Server) operation(prefix, kind, service string, load func(*detailPage, *http.Request) error, op string, do operation) {
	s.control.HandleFunc("POST /_stratus/ui/"+prefix+"/{name}/"+op, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("HX-Request") != "true" {
			http.Error(w, "only the stratus UI may post here", http.StatusForbidden)
			return
		}
		p := s.newDetail(kind, service, r)
		p.Self = path.Dir(r.URL.Path)
		notice, err := do(p, r)
		if err != nil {
			notice = "Failed: " + err.Error()
		}
		if err := load(p, r); err != nil {
			p.Error = err.Error()
		}
		p.Notice = notice
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = uiTemplates.ExecuteTemplate(w, "detail-body", p)
	})
}

func (s *Server) opSendMessage(p *detailPage, r *http.Request) (string, error) {
	in := sqs.SendInput{Body: r.FormValue("body"), GroupID: r.FormValue("group"), DeduplicationID: r.FormValue("dedup")}
	if v := strings.TrimSpace(r.FormValue("delay")); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			return "", errors.New("the delay must be a whole number of seconds")
		}
		d := time.Duration(secs) * time.Second
		in.Delay = &d
	}
	out, err := s.targets.SQS.Send(p.Name, in)
	return "Sent message " + out.MessageID + ".", err
}

func (s *Server) opUpload(p *detailPage, r *http.Request) (string, error) {
	if err := r.ParseMultipartForm(maxUpload); err != nil {
		return "", errors.New("choose a file to upload")
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		return "", errors.New("choose a file to upload")
	}
	defer file.Close()
	key := strings.TrimSpace(r.FormValue("object"))
	if key == "" {
		key = header.Filename
	} else if strings.HasSuffix(key, "/") {
		key += header.Filename
	}
	if err := s.callS3(http.MethodPut, p.Name, key, file, header.Size, header.Header.Get("Content-Type")); err != nil {
		return "", err
	}
	return fmt.Sprintf("Uploaded %s (%s).", key, formatBytes(header.Size)), nil
}

// jsonField reads a form field that must hold a JSON object.
func jsonField(r *http.Request, name string) (json.RawMessage, error) {
	raw := strings.TrimSpace(r.FormValue(name))
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, errors.New("not a JSON object in DynamoDB JSON form, for example {\"id\": {\"S\": \"42\"}}")
	}
	return json.RawMessage(raw), nil
}

func orEmptyObject(s string) string {
	if strings.TrimSpace(s) == "" {
		return "{}"
	}
	return s
}
