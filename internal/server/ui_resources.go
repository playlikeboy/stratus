package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Creating and deleting resources from the UI. Like the other changes they
// go through the emulator's AWS API; Reset still returns to the manifest.

func (s *Server) registerUIResources() {
	create := func(kind string, do func(*http.Request) (string, error)) {
		s.control.HandleFunc("POST /_stratus/ui/create/"+kind, s.uiAction(do))
	}
	create("queue", s.createQueue)
	create("stream", func(r *http.Request) (string, error) {
		name := field(r, "name")
		shards := 1
		if v := field(r, "shards"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return "", errors.New("the shard count must be a whole number")
			}
			shards = n
		}
		return "Created stream " + name + ".", s.callJSON("Kinesis_20131202.CreateStream", "application/x-amz-json-1.1",
			map[string]any{"StreamName": name, "ShardCount": shards})
	})
	create("bucket", func(r *http.Request) (string, error) {
		name := field(r, "name")
		body := "<CreateBucketConfiguration><LocationConstraint>" + s.identity.Region + "</LocationConstraint></CreateBucketConfiguration>"
		if err := s.callS3Path(http.MethodPut, "/"+name, body); err != nil {
			return "", err
		}
		if r.FormValue("versioning") != "" {
			if err := s.callS3Path(http.MethodPut, "/"+name+"?versioning",
				"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"); err != nil {
				return "", err
			}
		}
		return "Created bucket " + name + ".", nil
	})
	create("table", s.createTable)
	create("machine", func(r *http.Request) (string, error) {
		name := field(r, "name")
		return "Created state machine " + name + ".", s.callJSON("AWSStepFunctions.CreateStateMachine", "application/x-amz-json-1.0",
			map[string]string{"name": name, "definition": r.FormValue("definition"), "roleArn": s.identity.ARN("iam", "role/stratus")})
	})

	deleteResource := func(prefix string, do func(name string) error) {
		s.control.HandleFunc("POST /_stratus/ui/"+prefix+"/{name}/delete-resource", func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("HX-Request") != "true" {
				http.Error(w, "only the stratus UI may post here", http.StatusForbidden)
				return
			}
			if err := do(r.PathValue("name")); err != nil {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				fmt.Fprintf(w, `<div class="notice error">Failed: %s</div>`, html.EscapeString(err.Error()))
				return
			}
			w.Header().Set("HX-Redirect", "/_stratus/ui")
		})
	}
	deleteResource("sqs", func(name string) error {
		return s.callJSON("AmazonSQS.DeleteQueue", "application/x-amz-json-1.0",
			map[string]string{"QueueUrl": "http://localhost/" + s.identity.Account + "/" + name})
	})
	deleteResource("kinesis", func(name string) error {
		return s.callJSON("Kinesis_20131202.DeleteStream", "application/x-amz-json-1.1", map[string]string{"StreamName": name})
	})
	deleteResource("s3", func(name string) error {
		if _, err := s.targets.S3.EmptyBucket(name); err != nil { // S3 deletes only an empty bucket
			return err
		}
		return s.callS3Path(http.MethodDelete, "/"+name, "")
	})
	deleteResource("dynamodb", func(name string) error {
		return s.callJSON("DynamoDB_20120810.DeleteTable", "application/x-amz-json-1.0", map[string]string{"TableName": name})
	})
	deleteResource("states", func(name string) error {
		return s.callJSON("AWSStepFunctions.DeleteStateMachine", "application/x-amz-json-1.0",
			map[string]string{"stateMachineArn": s.targets.SFN.StateMachineARN(name)})
	})

	// An object's own page saves its text in place, or deletes it and goes
	// back to its bucket.
	s.control.HandleFunc("POST /_stratus/ui/s3/{name}/object/save", s.objectChange(func(bucket, key string, r *http.Request) (string, error) {
		content := r.FormValue("content")
		ct := strings.TrimSpace(r.FormValue("content_type"))
		if ct == "" {
			ct = "text/plain; charset=utf-8"
		}
		return "", s.callS3(http.MethodPut, bucket, key, strings.NewReader(content), int64(len(content)), ct)
	}))
	s.control.HandleFunc("POST /_stratus/ui/s3/{name}/object/delete", s.objectChange(func(bucket, key string, _ *http.Request) (string, error) {
		return "/_stratus/ui/s3/" + url.PathEscape(bucket), s.callS3(http.MethodDelete, bucket, key, nil, 0, "")
	}))
}

// objectChange runs a change on the object named by the key field, then has
// htmx reload the page, or go to the page the change names.
func (s *Server) objectChange(do func(bucket, key string, r *http.Request) (string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("HX-Request") != "true" {
			http.Error(w, "only the stratus UI may post here", http.StatusForbidden)
			return
		}
		next, err := do(r.PathValue("name"), r.FormValue("key"), r)
		switch {
		case err != nil:
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, `<div class="notice error">Failed: %s</div>`, html.EscapeString(err.Error()))
		case next != "":
			w.Header().Set("HX-Redirect", next)
		default:
			w.Header().Set("HX-Refresh", "true")
		}
	}
}

func (s *Server) createQueue(r *http.Request) (string, error) {
	name := field(r, "name")
	attrs := map[string]string{}
	if strings.HasSuffix(name, ".fifo") {
		attrs["FifoQueue"] = "true"
	}
	if dlq := field(r, "dlq"); dlq != "" {
		count := field(r, "max_receives")
		if count == "" {
			count = "5"
		}
		policy, _ := json.Marshal(map[string]string{
			"deadLetterTargetArn": s.identity.ARN("sqs", dlq), "maxReceiveCount": count,
		})
		attrs["RedrivePolicy"] = string(policy)
	}
	return "Created queue " + name + ".", s.callJSON("AmazonSQS.CreateQueue", "application/x-amz-json-1.0",
		map[string]any{"QueueName": name, "Attributes": attrs})
}

func (s *Server) createTable(r *http.Request) (string, error) {
	name := field(r, "name")
	attrDefs := []map[string]string{{"AttributeName": field(r, "pk"), "AttributeType": field(r, "pk_type")}}
	keys := []map[string]string{{"AttributeName": field(r, "pk"), "KeyType": "HASH"}}
	if sk := field(r, "sk"); sk != "" {
		attrDefs = append(attrDefs, map[string]string{"AttributeName": sk, "AttributeType": field(r, "sk_type")})
		keys = append(keys, map[string]string{"AttributeName": sk, "KeyType": "RANGE"})
	}
	if err := s.callJSON("DynamoDB_20120810.CreateTable", "application/x-amz-json-1.0", map[string]any{
		"TableName": name, "KeySchema": keys, "AttributeDefinitions": attrDefs, "BillingMode": "PAY_PER_REQUEST",
	}); err != nil {
		return "", err
	}
	if ttl := field(r, "ttl"); ttl != "" {
		if err := s.callJSON("DynamoDB_20120810.UpdateTimeToLive", "application/x-amz-json-1.0", map[string]any{
			"TableName": name, "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": ttl},
		}); err != nil {
			return "", fmt.Errorf("created table %s but could not turn TTL on: %w", name, err)
		}
	}
	return "Created table " + name + ".", nil
}

// callS3Path makes an S3 REST call on a bucket-level path, such as
// "/bucket" or "/bucket?versioning", with an XML body or none.
func (s *Server) callS3Path(method, target, body string) error {
	req, err := http.NewRequest(method, target, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(body))
	return s.serve(req)
}

func field(r *http.Request, name string) string { return strings.TrimSpace(r.FormValue(name)) }
