package sqs

import (
	"net/http"
	"time"
)

type failedEntry struct {
	Id          string //nolint:revive // the wire name
	SenderFault bool
	Code        string
	Message     string
}

func failedEntries(failed []BatchFailure) []failedEntry {
	out := make([]failedEntry, 0, len(failed))
	for _, f := range failed {
		out = append(out, failedEntry{Id: f.ID, SenderFault: f.SenderFault, Code: f.Code, Message: f.Message})
	}
	return out
}

type sendBatchEntry struct {
	Id string //nolint:revive // the wire name
	messageFields
}

type sendMessageBatchInput struct {
	QueueUrl string //nolint:revive // the wire name
	Entries  []sendBatchEntry
}

type sendBatchResultEntry struct {
	Id string //nolint:revive // the wire name
	sendResult
}

type sendMessageBatchOutput struct {
	Successful []sendBatchResultEntry
	Failed     []failedEntry
}

func (s *Service) sendMessageBatch(_ *http.Request, in sendMessageBatchInput) (any, error) {
	entries := make([]SendBatchEntry, len(in.Entries))
	for i, e := range in.Entries {
		entries[i] = SendBatchEntry{ID: e.Id, Input: e.sendInput()}
	}
	done, failed, err := s.SendBatch(queueName(in.QueueUrl), entries)
	if err != nil {
		return nil, err
	}
	out := sendMessageBatchOutput{Successful: make([]sendBatchResultEntry, 0, len(done)), Failed: failedEntries(failed)}
	for _, d := range done {
		out.Successful = append(out.Successful, sendBatchResultEntry{Id: d.ID, sendResult: newSendResult(d.Output)})
	}
	return out, nil
}

type deleteBatchEntry struct {
	Id            string //nolint:revive // the wire name
	ReceiptHandle string
}

type deleteMessageBatchInput struct {
	QueueUrl string //nolint:revive // the wire name
	Entries  []deleteBatchEntry
}

type succeededEntry struct {
	Id string //nolint:revive // the wire name
}

type batchOutput struct {
	Successful []succeededEntry
	Failed     []failedEntry
}

func newBatchOutput(done []string, failed []BatchFailure) batchOutput {
	out := batchOutput{Successful: make([]succeededEntry, 0, len(done)), Failed: failedEntries(failed)}
	for _, id := range done {
		out.Successful = append(out.Successful, succeededEntry{Id: id})
	}
	return out
}

func (s *Service) deleteMessageBatch(_ *http.Request, in deleteMessageBatchInput) (any, error) {
	entries := make([]DeleteBatchEntry, len(in.Entries))
	for i, e := range in.Entries {
		entries[i] = DeleteBatchEntry{ID: e.Id, Receipt: e.ReceiptHandle}
	}
	done, failed, err := s.DeleteBatch(queueName(in.QueueUrl), entries)
	if err != nil {
		return nil, err
	}
	return newBatchOutput(done, failed), nil
}

type visibilityBatchEntry struct {
	Id                string //nolint:revive // the wire name
	ReceiptHandle     string
	VisibilityTimeout *int
}

type changeMessageVisibilityBatchInput struct {
	QueueUrl string //nolint:revive // the wire name
	Entries  []visibilityBatchEntry
}

func (s *Service) changeMessageVisibilityBatch(_ *http.Request, in changeMessageVisibilityBatchInput) (any, error) {
	entries := make([]VisibilityBatchEntry, len(in.Entries))
	for i, e := range in.Entries {
		entries[i] = VisibilityBatchEntry{ID: e.Id, Receipt: e.ReceiptHandle}
		if e.VisibilityTimeout != nil {
			timeout := time.Duration(*e.VisibilityTimeout) * time.Second
			entries[i].Timeout = &timeout
		}
	}
	done, failed, err := s.ChangeVisibilityBatch(queueName(in.QueueUrl), entries)
	if err != nil {
		return nil, err
	}
	return newBatchOutput(done, failed), nil
}
