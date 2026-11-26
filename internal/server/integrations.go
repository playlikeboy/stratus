package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/sfn"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

const sqsSendMessageResource = "arn:aws:states:::sqs:sendMessage"

// sqsSendMessage is the Step Functions integration that sends a message to a
// queue of this emulator.
func sqsSendMessage(queues *sqs.Service) sfn.Integration {
	return func(_ context.Context, parameters json.RawMessage) (json.RawMessage, error) {
		var in struct {
			QueueUrl               string //nolint:revive // the wire name
			MessageBody            json.RawMessage
			DelaySeconds           *int
			MessageAttributes      map[string]sqs.MessageAttribute
			MessageGroupId         string //nolint:revive // the wire name
			MessageDeduplicationId string //nolint:revive // the wire name
		}
		if err := json.Unmarshal(parameters, &in); err != nil {
			return nil, &sfn.IntegrationError{Name: "SQS.InvalidParameterValue", Cause: err.Error()}
		}
		send := sqs.SendInput{
			Body:            messageBody(in.MessageBody),
			Attributes:      in.MessageAttributes,
			GroupID:         in.MessageGroupId,
			DeduplicationID: in.MessageDeduplicationId,
		}
		if in.DelaySeconds != nil {
			delay := time.Duration(*in.DelaySeconds) * time.Second
			send.Delay = &delay
		}
		out, err := queues.Send(queueNameOf(in.QueueUrl), send)
		if err != nil {
			var apiErr *awsapi.Error
			if errors.As(err, &apiErr) {
				return nil, &sfn.IntegrationError{Name: "SQS." + apiErr.Code, Cause: apiErr.Message}
			}
			return nil, err
		}
		return json.Marshal(map[string]string{
			"MessageId":        out.MessageID,
			"MD5OfMessageBody": out.MD5OfBody,
		})
	}
}

// messageBody is the text to send: a JSON string is sent as its value, any
// other JSON value as its JSON text, as Step Functions does.
func messageBody(raw json.RawMessage) string {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	return string(raw)
}

func queueNameOf(queueURL string) string {
	u, err := url.Parse(queueURL)
	if err != nil || u.Path == "" {
		return ""
	}
	return path.Base(u.Path)
}

// notifyQueue delivers an S3 event notification to the queue with the ARN.
func notifyQueue(queues *sqs.Service) func(queueARN, messageBody string) error {
	return func(queueARN, messageBody string) error {
		name := queueARN[strings.LastIndex(queueARN, ":")+1:]
		_, err := queues.Send(name, sqs.SendInput{Body: messageBody})
		return err
	}
}
