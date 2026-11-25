package s3

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/ODudek/stratus/internal/awsapi"
)

// The event names of the notifications this emulator sends.
const (
	eventPut          = "ObjectCreated:Put"
	eventCopy         = "ObjectCreated:Copy"
	eventComplete     = "ObjectCreated:CompleteMultipartUpload"
	eventDelete       = "ObjectRemoved:Delete"
	eventDeleteMarker = "ObjectRemoved:DeleteMarkerCreated"
)

// notificationConfiguration is a bucket's notification configuration in its
// wire form; it is kept as sent so that a GET returns it unchanged. Only the
// queue configurations are acted on.
type notificationConfiguration struct {
	XMLName     xml.Name
	Topics      []notificationTarget `xml:"TopicConfiguration"`
	Queues      []notificationTarget `xml:"QueueConfiguration"`
	Functions   []notificationTarget `xml:"CloudFunctionConfiguration"`
	EventBridge *struct{}            `xml:"EventBridgeConfiguration"`
}

type notificationTarget struct {
	ID            string              `xml:"Id,omitempty"`
	Topic         string              `xml:"Topic,omitempty"`
	Queue         string              `xml:"Queue,omitempty"`
	CloudFunction string              `xml:"CloudFunction,omitempty"`
	Events        []string            `xml:"Event"`
	Filter        *notificationFilter `xml:"Filter,omitempty"`
}

type notificationFilter struct {
	Rules []filterRule `xml:"S3Key>FilterRule"`
}

type filterRule struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

// notificationsFromQueues turns the Go API's queue notifications into the
// wire form.
func notificationsFromQueues(queues []QueueNotification) (notificationConfiguration, error) {
	var cfg notificationConfiguration
	for _, q := range queues {
		t := notificationTarget{ID: q.ID, Queue: q.QueueARN, Events: q.Events}
		if len(t.Events) == 0 {
			t.Events = []string{"s3:ObjectCreated:*"}
		}
		var rules []filterRule
		if q.Prefix != "" {
			rules = append(rules, filterRule{Name: "prefix", Value: q.Prefix})
		}
		if q.Suffix != "" {
			rules = append(rules, filterRule{Name: "suffix", Value: q.Suffix})
		}
		if rules != nil {
			t.Filter = &notificationFilter{Rules: rules}
		}
		cfg.Queues = append(cfg.Queues, t)
	}
	return cfg, validateNotifications(&cfg)
}

// validateNotifications checks a configuration and gives every target an id,
// as S3 does for those sent without one.
func validateNotifications(cfg *notificationConfiguration) error {
	for _, targets := range [][]notificationTarget{cfg.Topics, cfg.Queues, cfg.Functions} {
		for i := range targets {
			if err := validateTarget(&targets[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTarget(t *notificationTarget) error {
	if t.Topic+t.Queue+t.CloudFunction == "" {
		return errInvalidArgument("A notification configuration names no destination ARN.")
	}
	if strings.HasSuffix(t.Queue, ".fifo") {
		return errInvalidArgument("FIFO SQS queues are not supported as S3 event destinations.")
	}
	if len(t.Events) == 0 {
		return errInvalidArgument("A notification configuration names no event.")
	}
	for _, e := range t.Events {
		if !strings.HasPrefix(e, "s3:") {
			return errInvalidArgument("The event " + e + " is not a valid S3 event type.")
		}
	}
	if t.Filter != nil {
		seen := map[string]bool{}
		for i, r := range t.Filter.Rules {
			name := strings.ToLower(r.Name)
			if (name != "prefix" && name != "suffix") || seen[name] {
				return errInvalidArgument("A filter rule name must be prefix or suffix, each at most once.")
			}
			seen[name] = true
			t.Filter.Rules[i].Name = name
		}
	}
	if t.ID == "" {
		t.ID = awsapi.NewID()
	}
	return nil
}

// matches tells whether a target wants an event about key.
func (t notificationTarget) matches(eventName, key string) bool {
	if !slices.ContainsFunc(t.Events, func(e string) bool {
		want := strings.TrimPrefix(e, "s3:")
		group, _, _ := strings.Cut(eventName, ":")
		return want == eventName || want == group+":*"
	}) {
		return false
	}
	if t.Filter == nil {
		return true
	}
	for _, r := range t.Filter.Rules {
		if (r.Name == "prefix" && !strings.HasPrefix(key, r.Value)) ||
			(r.Name == "suffix" && !strings.HasSuffix(key, r.Value)) {
			return false
		}
	}
	return true
}

// delivery is one message for one queue, sent once the lock is released.
type delivery struct {
	queueARN string
	body     string
	notifier func(queueARN, messageBody string) error
}

// eventsLocked builds the messages for the queues that want an event about
// o; the caller holds s.mu for writing.
func (s *Service) eventsLocked(b *bucket, o *object, eventName string, src eventSource) []delivery {
	if s.notifier == nil {
		return nil
	}
	var out []delivery
	for _, q := range b.notifications.Queues {
		if !q.matches(eventName, o.key) {
			continue
		}
		s.sequence++
		body, _ := json.Marshal(s.eventRecord(b, o, eventName, q.ID, src)) // plain structs always marshal
		out = append(out, delivery{queueARN: q.Queue, body: string(body), notifier: s.notifier})
	}
	return out
}

// deliver sends the messages; the service's lock must not be held, because
// the notifier calls into another service.
func (s *Service) deliver(deliveries []delivery) {
	for _, d := range deliveries {
		_ = d.notifier(d.queueARN, d.body) // S3 delivers asynchronously: a failure never fails the request
	}
}

// eventRecord is the standard S3 event message with a single record.
func (s *Service) eventRecord(b *bucket, o *object, eventName, configID string, src eventSource) map[string]any {
	obj := map[string]any{
		"key":       eventKey(o.key),
		"sequencer": fmt.Sprintf("%016X", s.sequence),
	}
	if strings.HasPrefix(eventName, "ObjectCreated:") {
		obj["size"] = o.size()
		obj["eTag"] = strings.Trim(o.etag, `"`)
	}
	if o.versionID != "" {
		obj["versionId"] = o.versionID
	}
	principal := map[string]string{"principalId": "AWS:" + s.identity.Account}
	record := map[string]any{
		"eventVersion":      "2.1",
		"eventSource":       "aws:s3",
		"awsRegion":         b.region,
		"eventTime":         s.clock.Now().UTC().Format(timeFormatXML),
		"eventName":         eventName,
		"userIdentity":      principal,
		"requestParameters": map[string]string{"sourceIPAddress": src.sourceIP},
		"responseElements":  map[string]string{"x-amz-request-id": src.requestID, "x-amz-id-2": src.requestID},
		"s3": map[string]any{
			"s3SchemaVersion": "1.0",
			"configurationId": configID,
			"bucket": map[string]any{
				"name":          b.name,
				"ownerIdentity": map[string]string{"principalId": s.identity.Account},
				"arn":           s.bucketARN(b.name),
			},
			"object": obj,
		},
	}
	return map[string]any{"Records": []any{record}}
}

// eventKey URL-encodes a key the way S3 event messages do: form encoding
// (a space becomes "+"), with "/" left as it is.
func eventKey(key string) string {
	return strings.ReplaceAll(url.QueryEscape(key), "%2F", "/")
}
