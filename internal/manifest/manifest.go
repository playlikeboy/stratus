// Package manifest reads the declarative list of resources an emulator starts
// with, and creates them. A key the emulator does not know is an error: a
// resource a test relies on must not be silently missing.
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"go.yaml.in/yaml/v3"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/sqs"
)

// Manifest is the resources to create; the zero value creates nothing.
type Manifest struct {
	SQS SQS `yaml:"sqs"`
}

type SQS struct {
	Queues []Queue `yaml:"queues"`
}

type Queue struct {
	Name                     string      `yaml:"name"`
	VisibilityTimeoutSeconds *int        `yaml:"visibilityTimeoutSeconds"`
	DelaySeconds             *int        `yaml:"delaySeconds"`
	ReceiveWaitTimeSeconds   *int        `yaml:"receiveWaitTimeSeconds"`
	DeadLetter               *DeadLetter `yaml:"deadLetter"`
}

type DeadLetter struct {
	Queue           string `yaml:"queue"`
	MaxReceiveCount int    `yaml:"maxReceiveCount"`
}

// Load reads the manifest file.
func Load(path string) (Manifest, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the operator names the file
	if err != nil {
		return Manifest{}, fmt.Errorf("reading the manifest: %w", err)
	}
	m, err := Parse(data)
	if err != nil {
		return Manifest{}, fmt.Errorf("manifest %s: %w", path, err)
	}
	return m, nil
}

// Parse decodes and validates a manifest.
func Parse(data []byte) (Manifest, error) {
	var m Manifest
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&m); err != nil && !errors.Is(err, io.EOF) {
		return Manifest{}, err
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func (m Manifest) validate() error {
	names := map[string]bool{}
	for i, q := range m.SQS.Queues {
		if q.Name == "" {
			return fmt.Errorf("sqs.queues[%d]: name is required", i)
		}
		if names[q.Name] {
			return fmt.Errorf("sqs.queues: %q is declared twice", q.Name)
		}
		names[q.Name] = true
	}
	for _, q := range m.SQS.Queues {
		if q.DeadLetter != nil && !names[q.DeadLetter.Queue] {
			return fmt.Errorf("sqs.queues: %q has the dead-letter queue %q, which is not declared",
				q.Name, q.DeadLetter.Queue)
		}
	}
	return nil
}

// Apply creates the manifest's resources. Queues may be declared in any order:
// dead-letter links are set once every queue exists.
func (m Manifest) Apply(identity awsapi.Identity, queues *sqs.Service) error {
	for _, q := range m.SQS.Queues {
		if err := queues.CreateQueue(q.Name, q.attributes()); err != nil {
			return fmt.Errorf("sqs queue %q: %w", q.Name, err)
		}
	}
	for _, q := range m.SQS.Queues {
		if q.DeadLetter == nil {
			continue
		}
		policy, err := json.Marshal(map[string]any{
			"deadLetterTargetArn": identity.ARN("sqs", q.DeadLetter.Queue),
			"maxReceiveCount":     q.DeadLetter.MaxReceiveCount,
		})
		if err != nil {
			return fmt.Errorf("sqs queue %q: %w", q.Name, err)
		}
		if err := queues.SetAttributes(q.Name, map[string]string{"RedrivePolicy": string(policy)}); err != nil {
			return fmt.Errorf("sqs queue %q: %w", q.Name, err)
		}
	}
	return nil
}

func (q Queue) attributes() map[string]string {
	attrs := map[string]string{}
	set := func(name string, value *int) {
		if value != nil {
			attrs[name] = strconv.Itoa(*value)
		}
	}
	set("VisibilityTimeout", q.VisibilityTimeoutSeconds)
	set("DelaySeconds", q.DelaySeconds)
	set("ReceiveMessageWaitTimeSeconds", q.ReceiveWaitTimeSeconds)
	return attrs
}
