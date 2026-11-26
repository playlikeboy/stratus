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
	"path/filepath"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/dynamodb"
	"github.com/stratus-emulator/stratus/internal/kinesis"
	"github.com/stratus-emulator/stratus/internal/s3"
	"github.com/stratus-emulator/stratus/internal/sfn"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

// Targets is the services a manifest creates its resources in.
type Targets struct {
	Identity awsapi.Identity
	SQS      *sqs.Service
	Kinesis  *kinesis.Service
	S3       *s3.Service
	DynamoDB *dynamodb.Service
	SFN      *sfn.Service
}

// Manifest is the resources to create; the zero value creates nothing.
type Manifest struct {
	SQS      SQS      `yaml:"sqs"`
	Kinesis  Kinesis  `yaml:"kinesis"`
	S3       S3       `yaml:"s3"`
	DynamoDB DynamoDB `yaml:"dynamodb"`
	// StepFunctions is applied last: a state machine may refer to the other resources.
	StepFunctions StepFunctions `yaml:"stepFunctions"`
}

type S3 struct {
	Buckets []Bucket `yaml:"buckets"`
}

type Bucket struct {
	Name          string               `yaml:"name"`
	Versioning    bool                 `yaml:"versioning"`
	Lifecycle     []BucketExpiration   `yaml:"lifecycle"`
	Notifications []BucketNotification `yaml:"notifications"`
}

// BucketExpiration expires the objects under Prefix after ExpirationDays.
type BucketExpiration struct {
	Prefix         string `yaml:"prefix"`
	ExpirationDays int    `yaml:"expirationDays"`
}

// BucketNotification sends the bucket's events to a queue of this manifest.
type BucketNotification struct {
	Queue string `yaml:"queue"`
	// Events defaults to "s3:ObjectCreated:*".
	Events []string `yaml:"events"`
	Prefix string   `yaml:"prefix"`
	Suffix string   `yaml:"suffix"`
}

type StepFunctions struct {
	StateMachines []StateMachine `yaml:"stateMachines"`
}

// StateMachine is declared with its Amazon States Language definition, either
// inline or in a file named relative to the manifest.
type StateMachine struct {
	Name           string `yaml:"name"`
	Definition     string `yaml:"definition"`
	DefinitionFile string `yaml:"definitionFile"`
	// Substitutions replaces each "${key}" in the definition with its value.
	Substitutions map[string]string `yaml:"substitutions"`
	// Type is STANDARD (the default) or EXPRESS.
	Type string `yaml:"type"`
}

type Kinesis struct {
	Streams []Stream `yaml:"streams"`
}

type Stream struct {
	Name string `yaml:"name"`
	// Shards defaults to 1.
	Shards int `yaml:"shards"`
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
	// ContentBasedDeduplication applies to a FIFO queue: one whose name ends in ".fifo".
	ContentBasedDeduplication bool `yaml:"contentBasedDeduplication"`
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
	if err := m.readDefinitionFiles(filepath.Dir(path)); err != nil {
		return Manifest{}, fmt.Errorf("manifest %s: %w", path, err)
	}
	return m, nil
}

// readDefinitionFiles loads the state machine definitions kept in files, which
// are named relative to dir.
func (m Manifest) readDefinitionFiles(dir string) error {
	for i, sm := range m.StepFunctions.StateMachines {
		if sm.DefinitionFile == "" {
			continue
		}
		file := sm.DefinitionFile
		if !filepath.IsAbs(file) {
			file = filepath.Join(dir, file)
		}
		data, err := os.ReadFile(file) //nolint:gosec // the operator names the file
		if err != nil {
			return fmt.Errorf("state machine %q: %w", sm.Name, err)
		}
		m.StepFunctions.StateMachines[i].Definition = string(data)
	}
	return nil
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
		if q.ContentBasedDeduplication && !strings.HasSuffix(q.Name, ".fifo") {
			return fmt.Errorf("sqs.queues: %q sets contentBasedDeduplication but is not a FIFO queue: its name must end in .fifo", q.Name)
		}
		names[q.Name] = true
	}
	for _, q := range m.SQS.Queues {
		if q.DeadLetter != nil && !names[q.DeadLetter.Queue] {
			return fmt.Errorf("sqs.queues: %q has the dead-letter queue %q, which is not declared",
				q.Name, q.DeadLetter.Queue)
		}
	}
	streams := map[string]bool{}
	for i, st := range m.Kinesis.Streams {
		if st.Name == "" {
			return fmt.Errorf("kinesis.streams[%d]: name is required", i)
		}
		if streams[st.Name] {
			return fmt.Errorf("kinesis.streams: %q is declared twice", st.Name)
		}
		if st.Shards < 0 {
			return fmt.Errorf("kinesis.streams: %q has a negative number of shards", st.Name)
		}
		streams[st.Name] = true
	}
	buckets := map[string]bool{}
	for i, b := range m.S3.Buckets {
		if b.Name == "" {
			return fmt.Errorf("s3.buckets[%d]: name is required", i)
		}
		if buckets[b.Name] {
			return fmt.Errorf("s3.buckets: %q is declared twice", b.Name)
		}
		buckets[b.Name] = true
		for _, rule := range b.Lifecycle {
			if rule.ExpirationDays < 1 {
				return fmt.Errorf("s3.buckets: %q has a lifecycle rule without expirationDays", b.Name)
			}
		}
		for _, n := range b.Notifications {
			if !names[n.Queue] {
				return fmt.Errorf("s3.buckets: %q notifies the queue %q, which is not declared", b.Name, n.Queue)
			}
			if strings.HasSuffix(n.Queue, ".fifo") {
				return fmt.Errorf("s3.buckets: %q notifies the FIFO queue %q; S3 delivers only to standard queues", b.Name, n.Queue)
			}
		}
	}
	if err := m.DynamoDB.validate(); err != nil {
		return err
	}
	machines := map[string]bool{}
	for i, sm := range m.StepFunctions.StateMachines {
		switch {
		case sm.Name == "":
			return fmt.Errorf("stepFunctions.stateMachines[%d]: name is required", i)
		case machines[sm.Name]:
			return fmt.Errorf("stepFunctions.stateMachines: %q is declared twice", sm.Name)
		case (sm.Definition == "") == (sm.DefinitionFile == ""):
			return fmt.Errorf("stepFunctions.stateMachines: %q needs exactly one of definition and definitionFile", sm.Name)
		}
		machines[sm.Name] = true
	}
	return nil
}

// Apply creates the manifest's resources.
func (m Manifest) Apply(t Targets) error {
	if err := m.applyQueues(t.Identity, t.SQS); err != nil {
		return err
	}
	for _, st := range m.Kinesis.Streams {
		if err := t.Kinesis.CreateStream(st.Name, max(st.Shards, 1)); err != nil {
			return fmt.Errorf("kinesis stream %q: %w", st.Name, err)
		}
	}
	for _, b := range m.S3.Buckets {
		if err := t.S3.CreateBucket(b.Name, b.options(t.Identity)); err != nil {
			return fmt.Errorf("s3 bucket %q: %w", b.Name, err)
		}
	}
	if err := m.DynamoDB.apply(t.DynamoDB); err != nil {
		return err
	}
	for _, sm := range m.StepFunctions.StateMachines {
		definition := sm.Definition
		for key, value := range sm.Substitutions {
			definition = strings.ReplaceAll(definition, "${"+key+"}", value)
		}
		var opts []sfn.CreateOption
		if sm.Type != "" {
			opts = append(opts, sfn.WithType(sm.Type))
		}
		if err := t.SFN.CreateStateMachine(sm.Name, definition, opts...); err != nil {
			return fmt.Errorf("state machine %q: %w", sm.Name, err)
		}
	}
	return nil
}

// applyQueues creates the queues. They may be declared in any order:
// dead-letter links are set once every queue exists.
func (m Manifest) applyQueues(identity awsapi.Identity, queues *sqs.Service) error {
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
	if strings.HasSuffix(q.Name, ".fifo") {
		attrs["FifoQueue"] = "true"
		if q.ContentBasedDeduplication {
			attrs["ContentBasedDeduplication"] = "true"
		}
	}
	return attrs
}

func (b Bucket) options(identity awsapi.Identity) s3.BucketOptions {
	opts := s3.BucketOptions{Versioning: b.Versioning}
	for _, rule := range b.Lifecycle {
		opts.Lifecycle = append(opts.Lifecycle, s3.LifecycleRule{Prefix: rule.Prefix, ExpirationDays: rule.ExpirationDays})
	}
	for _, n := range b.Notifications {
		opts.Notifications = append(opts.Notifications, s3.QueueNotification{
			QueueARN: identity.ARN("sqs", n.Queue),
			Events:   n.Events,
			Prefix:   n.Prefix,
			Suffix:   n.Suffix,
		})
	}
	return opts
}
