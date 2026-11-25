package sqs

import (
	"net/http"
	"strconv"
)

type listURLsOutput struct {
	QueueUrls []string //nolint:revive // the wire name
	NextToken string   `json:",omitempty"`
}

// queueURLs renders a page of queue names as the response of a list operation.
func (s *Service) queueURLs(r *http.Request, names []string, next string) listURLsOutput {
	urls := make([]string, 0, len(names))
	for _, name := range names {
		urls = append(urls, queueURL(r, s.identity.Account, name))
	}
	return listURLsOutput{QueueUrls: urls, NextToken: next}
}

// pageSize validates MaxResults: absent means the largest page, which is all a
// real SQS returns without it.
func pageSize(maxResults *int) (int, error) {
	if maxResults == nil {
		return maxListResults, nil
	}
	if *maxResults < 1 || *maxResults > maxListResults {
		return 0, errInvalidParameter("Value " + strconv.Itoa(*maxResults) + " for parameter MaxResults is invalid. " +
			"Reason: MaxResults must be an integer between 1 and 1000.")
	}
	return *maxResults, nil
}

type listDeadLetterSourceQueuesInput struct {
	QueueUrl   string //nolint:revive // the wire name
	MaxResults *int
	NextToken  string
}

func (s *Service) listDeadLetterSourceQueues(r *http.Request, in listDeadLetterSourceQueuesInput) (any, error) {
	limit, err := pageSize(in.MaxResults)
	if err != nil {
		return nil, err
	}
	names, next, err := s.ListDeadLetterSourceQueues(queueName(in.QueueUrl), limit, in.NextToken)
	if err != nil {
		return nil, err
	}
	// Unlike ListQueues, this response spells its list "queueUrls".
	page := s.queueURLs(r, names, next)
	return struct {
		QueueUrls []string `json:"queueUrls"` //nolint:revive // the wire name
		NextToken string   `json:",omitempty"`
	}{page.QueueUrls, page.NextToken}, nil
}

type tagQueueInput struct {
	QueueUrl string //nolint:revive // the wire name
	Tags     map[string]string
}

func (s *Service) tagQueue(_ *http.Request, in tagQueueInput) (any, error) {
	return empty{}, s.TagQueue(queueName(in.QueueUrl), in.Tags)
}

type untagQueueInput struct {
	QueueUrl string //nolint:revive // the wire name
	TagKeys  []string
}

func (s *Service) untagQueue(_ *http.Request, in untagQueueInput) (any, error) {
	return empty{}, s.UntagQueue(queueName(in.QueueUrl), in.TagKeys)
}

func (s *Service) listQueueTags(_ *http.Request, in queueInput) (any, error) {
	tags, err := s.ListQueueTags(queueName(in.QueueUrl))
	if err != nil {
		return nil, err
	}
	if len(tags) == 0 {
		return empty{}, nil
	}
	return map[string]map[string]string{"Tags": tags}, nil
}
