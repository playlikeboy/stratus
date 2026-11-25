package sqs

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
)

const (
	targetPrefix = "AmazonSQS."
	contentType  = "application/x-amz-json-1.0"
	namespace    = "com.amazonaws.sqs"

	// maxRequestBytes leaves room for a full-size message and its JSON framing.
	maxRequestBytes = 2 * maxMessageBytes
)

type operation func(s *Service, r *http.Request, body []byte) (any, error)

var operations = map[string]operation{
	"CreateQueue":             op((*Service).createQueue),
	"DeleteQueue":             op((*Service).deleteQueue),
	"GetQueueUrl":             op((*Service).getQueueURL),
	"ListQueues":              op((*Service).listQueues),
	"GetQueueAttributes":      op((*Service).getQueueAttributes),
	"SetQueueAttributes":      op((*Service).setQueueAttributes),
	"PurgeQueue":              op((*Service).purgeQueue),
	"SendMessage":             op((*Service).sendMessage),
	"ReceiveMessage":          op((*Service).receiveMessage),
	"DeleteMessage":           op((*Service).deleteMessage),
	"ChangeMessageVisibility": op((*Service).changeMessageVisibility),
}

// op adapts a typed operation: it decodes the request body into In.
func op[In any](fn func(*Service, *http.Request, In) (any, error)) operation {
	return func(s *Service, r *http.Request, body []byte) (any, error) {
		var in In
		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, &awsapi.Error{
					Status:  http.StatusBadRequest,
					Code:    "SerializationException",
					Message: "The request body is not valid JSON for this operation: " + err.Error(),
				}
			}
		}
		return fn(s, r, in)
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	out, err := s.serve(w, r)
	if err != nil {
		var apiErr *awsapi.Error
		if !errors.As(err, &apiErr) {
			apiErr = &awsapi.Error{Status: http.StatusInternalServerError, Code: "InternalFailure", Message: err.Error()}
		}
		awsapi.WriteJSONError(w, contentType, namespace, apiErr)
		return
	}
	awsapi.WriteJSON(w, contentType, http.StatusOK, out)
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) (any, error) {
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return nil, &awsapi.Error{
			Status:  http.StatusBadRequest,
			Code:    "UnsupportedProtocol",
			Message: "stratus speaks only the SQS JSON protocol: the request has no X-Amz-Target header.",
		}
	}
	name := strings.TrimPrefix(target, targetPrefix)
	handle, ok := operations[name]
	if !ok {
		return nil, &awsapi.Error{
			Status:  http.StatusBadRequest,
			Code:    "InvalidAction",
			Message: "stratus does not implement the SQS operation " + name + ".",
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		return nil, errInvalidParameter("The request body could not be read or is too large.")
	}
	return handle(s, r, body)
}

type empty struct{}

type createQueueInput struct {
	QueueName  string
	Attributes map[string]string
}

func (s *Service) createQueue(r *http.Request, in createQueueInput) (any, error) {
	if err := s.CreateQueue(in.QueueName, in.Attributes); err != nil {
		return nil, err
	}
	return map[string]string{"QueueUrl": queueURL(r, s.identity.Account, in.QueueName)}, nil
}

type queueInput struct {
	QueueUrl string //nolint:revive // the wire name
}

func (s *Service) deleteQueue(_ *http.Request, in queueInput) (any, error) {
	return empty{}, s.DeleteQueue(queueName(in.QueueUrl))
}

func (s *Service) purgeQueue(_ *http.Request, in queueInput) (any, error) {
	return empty{}, s.Purge(queueName(in.QueueUrl))
}

type getQueueURLInput struct {
	QueueName string
}

func (s *Service) getQueueURL(r *http.Request, in getQueueURLInput) (any, error) {
	if !s.Exists(in.QueueName) {
		return nil, errQueueNotFound()
	}
	return map[string]string{"QueueUrl": queueURL(r, s.identity.Account, in.QueueName)}, nil
}

type listQueuesInput struct {
	QueueNamePrefix string
}

func (s *Service) listQueues(r *http.Request, in listQueuesInput) (any, error) {
	names := s.ListQueues(in.QueueNamePrefix)
	urls := make([]string, 0, len(names))
	for _, name := range names {
		urls = append(urls, queueURL(r, s.identity.Account, name))
	}
	return map[string][]string{"QueueUrls": urls}, nil
}

type getQueueAttributesInput struct {
	QueueUrl       string //nolint:revive // the wire name
	AttributeNames []string
}

func (s *Service) getQueueAttributes(_ *http.Request, in getQueueAttributesInput) (any, error) {
	all, err := s.Attributes(queueName(in.QueueUrl))
	if err != nil {
		return nil, err
	}
	if len(in.AttributeNames) == 0 {
		return empty{}, nil
	}
	if !slices.Contains(in.AttributeNames, "All") {
		for name := range all {
			if !slices.Contains(in.AttributeNames, name) {
				delete(all, name)
			}
		}
	}
	return map[string]map[string]string{"Attributes": all}, nil
}

type setQueueAttributesInput struct {
	QueueUrl   string //nolint:revive // the wire name
	Attributes map[string]string
}

func (s *Service) setQueueAttributes(_ *http.Request, in setQueueAttributesInput) (any, error) {
	return empty{}, s.SetAttributes(queueName(in.QueueUrl), in.Attributes)
}

type sendMessageInput struct {
	QueueUrl          string //nolint:revive // the wire name
	MessageBody       string
	DelaySeconds      *int
	MessageAttributes map[string]MessageAttribute
}

type sendMessageOutput struct {
	MessageId              string //nolint:revive // the wire name
	MD5OfMessageBody       string
	MD5OfMessageAttributes string `json:",omitempty"`
}

func (s *Service) sendMessage(_ *http.Request, in sendMessageInput) (any, error) {
	delay, err := optionalSeconds("DelaySeconds", in.DelaySeconds, maxDelay)
	if err != nil {
		return nil, err
	}
	out, err := s.Send(queueName(in.QueueUrl), SendInput{
		Body:       in.MessageBody,
		Delay:      delay,
		Attributes: in.MessageAttributes,
	})
	if err != nil {
		return nil, err
	}
	return sendMessageOutput{
		MessageId:              out.MessageID,
		MD5OfMessageBody:       out.MD5OfBody,
		MD5OfMessageAttributes: out.MD5OfAttributes,
	}, nil
}

type receiveMessageInput struct {
	QueueUrl                    string //nolint:revive // the wire name
	MaxNumberOfMessages         *int
	VisibilityTimeout           *int
	WaitTimeSeconds             *int
	AttributeNames              []string // the deprecated spelling of the next field
	MessageSystemAttributeNames []string
	MessageAttributeNames       []string
}

type receivedMessage struct {
	MessageId              string //nolint:revive // the wire name
	ReceiptHandle          string
	MD5OfBody              string
	Body                   string
	Attributes             map[string]string           `json:",omitempty"`
	MD5OfMessageAttributes string                      `json:",omitempty"`
	MessageAttributes      map[string]MessageAttribute `json:",omitempty"`
}

func (s *Service) receiveMessage(r *http.Request, in receiveMessageInput) (any, error) {
	visibility, err := optionalSeconds("VisibilityTimeout", in.VisibilityTimeout, maxVisibility)
	if err != nil {
		return nil, err
	}
	wait, err := optionalSeconds("WaitTimeSeconds", in.WaitTimeSeconds, maxReceiveWait)
	if err != nil {
		return nil, err
	}
	limit := 0
	if in.MaxNumberOfMessages != nil {
		if limit = *in.MaxNumberOfMessages; limit == 0 {
			limit = -1 // an explicit 0 is invalid, unlike an absent value
		}
	}
	received, err := s.Receive(r.Context(), queueName(in.QueueUrl), ReceiveInput{
		Max:        limit,
		Visibility: visibility,
		Wait:       wait,
	})
	if err != nil {
		return nil, err
	}
	if len(received) == 0 {
		return empty{}, nil
	}
	systemNames := slices.Concat(in.MessageSystemAttributeNames, in.AttributeNames)
	messages := make([]receivedMessage, 0, len(received))
	for _, m := range received {
		attrs := selectAttributes(m.Attributes, in.MessageAttributeNames)
		messages = append(messages, receivedMessage{
			MessageId:              m.MessageID,
			ReceiptHandle:          m.Receipt,
			MD5OfBody:              m.MD5OfBody,
			Body:                   m.Body,
			Attributes:             s.systemAttributes(m, systemNames),
			MD5OfMessageAttributes: md5OfAttributes(attrs),
			MessageAttributes:      attrs,
		})
	}
	return map[string][]receivedMessage{"Messages": messages}, nil
}

func (s *Service) systemAttributes(m Received, names []string) map[string]string {
	if len(names) == 0 {
		return nil
	}
	all := map[string]string{
		"SenderId":                         s.identity.Account,
		"SentTimestamp":                    strconv.FormatInt(m.SentAt.UnixMilli(), 10),
		"ApproximateReceiveCount":          strconv.Itoa(m.ReceiveCount),
		"ApproximateFirstReceiveTimestamp": strconv.FormatInt(m.FirstReceivedAt.UnixMilli(), 10),
	}
	if slices.Contains(names, "All") {
		return all
	}
	out := map[string]string{}
	for _, name := range names {
		if value, ok := all[name]; ok {
			out[name] = value
		}
	}
	return out
}

// selectAttributes returns the attributes a receiver asked for: by name, by
// "prefix.*", or all of them with "All" or ".*".
func selectAttributes(attrs map[string]MessageAttribute, names []string) map[string]MessageAttribute {
	out := map[string]MessageAttribute{}
	for name, attr := range attrs {
		for _, want := range names {
			prefix, isPrefix := strings.CutSuffix(want, ".*")
			if want == "All" || want == name || (isPrefix && strings.HasPrefix(name, prefix)) {
				out[name] = attr
				break
			}
		}
	}
	return out
}

type deleteMessageInput struct {
	QueueUrl      string //nolint:revive // the wire name
	ReceiptHandle string
}

func (s *Service) deleteMessage(_ *http.Request, in deleteMessageInput) (any, error) {
	return empty{}, s.Delete(queueName(in.QueueUrl), in.ReceiptHandle)
}

type changeMessageVisibilityInput struct {
	QueueUrl          string //nolint:revive // the wire name
	ReceiptHandle     string
	VisibilityTimeout *int
}

func (s *Service) changeMessageVisibility(_ *http.Request, in changeMessageVisibilityInput) (any, error) {
	if in.VisibilityTimeout == nil {
		return nil, errMissingParameter("VisibilityTimeout")
	}
	timeout, err := optionalSeconds("VisibilityTimeout", in.VisibilityTimeout, maxVisibility)
	if err != nil {
		return nil, err
	}
	return empty{}, s.ChangeVisibility(queueName(in.QueueUrl), in.ReceiptHandle, *timeout)
}

func optionalSeconds(name string, value *int, limit time.Duration) (*time.Duration, error) {
	if value == nil {
		return nil, nil
	}
	d := time.Duration(*value) * time.Second
	if *value < 0 || d > limit {
		return nil, errInvalidParameter("Value " + strconv.Itoa(*value) + " for parameter " + name +
			" is invalid. Reason: Must be between 0 and " + seconds(limit) + ", if provided.")
	}
	return &d, nil
}

// queueURL builds a queue's URL on the host the client used, so that the same
// queue is reachable as localhost and under a container name.
func queueURL(r *http.Request, account, name string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/" + account + "/" + name
}

// queueName takes the queue's name out of its URL; any host is accepted.
func queueName(queueURL string) string {
	u, err := url.Parse(queueURL)
	if err != nil || u.Path == "" {
		return ""
	}
	return path.Base(u.Path)
}
