// Package s3 emulates Amazon S3 over its REST protocol with path-style
// addressing: buckets, versioned objects, multipart uploads, checksums,
// lifecycle expiration and queue notifications. Object bytes are kept in
// files of a data directory, everything else in memory.
package s3

import (
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
)

// validBucketName is the DNS-compatible bucket naming rule.
var validBucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// Service holds every bucket of one emulator. One RWMutex guards the state;
// it is only held while pointers are moved, never while a body is read or
// written: object bytes are immutable once stored.
type Service struct {
	identity awsapi.Identity
	clock    clock.Clock
	blobs    *blobStore

	mu       sync.RWMutex
	buckets  map[string]*bucket
	notifier func(queueARN, messageBody string) error
	sequence uint64 // orders notification events
}

// BucketOptions configures a bucket created through the Go API.
type BucketOptions struct {
	// Versioning enables versioning from the start.
	Versioning bool
	// Lifecycle is the bucket's lifecycle configuration; ExpireLifecycle applies it.
	Lifecycle []LifecycleRule
	// Notifications are the queues told about object events.
	Notifications []QueueNotification
}

// LifecycleRule is one lifecycle rule. A rule applies to the keys under
// Prefix whose size is within the optional bounds.
type LifecycleRule struct {
	ID       string
	Prefix   string
	Disabled bool
	// ObjectSizeGreaterThan and ObjectSizeLessThan bound the object size; 0 is no bound.
	ObjectSizeGreaterThan int64
	ObjectSizeLessThan    int64
	// ExpirationDays expires current versions that many days after their
	// creation, rounded up to the next midnight UTC as S3 does; 0 is none.
	ExpirationDays int
	// ExpirationDate expires current versions from that instant; zero is none.
	ExpirationDate time.Time
	// ExpiredObjectDeleteMarker removes delete markers that no longer hide any version.
	ExpiredObjectDeleteMarker bool
	// NoncurrentDays removes versions that many days after they became noncurrent.
	NoncurrentDays int
	// AbortIncompleteUploadDays aborts multipart uploads that many days after initiation.
	AbortIncompleteUploadDays int
}

// QueueNotification sends object events of a bucket to a queue.
type QueueNotification struct {
	ID       string
	QueueARN string
	// Events are S3 event types, for example "s3:ObjectCreated:*" or
	// "s3:ObjectCreated:Put"; empty means "s3:ObjectCreated:*".
	Events []string
	// Prefix and Suffix filter the object keys; empty matches every key.
	Prefix string
	Suffix string
}

// ObjectOptions describes an object stored through the Go API.
type ObjectOptions struct {
	ContentType string
	// Metadata is the user metadata, sent back as x-amz-meta-<name> headers.
	Metadata map[string]string
}

// New returns an S3 service that keeps object bytes in files of dataDir, an
// existing directory it owns; the metadata stays in memory.
func New(identity awsapi.Identity, c clock.Clock, dataDir string) *Service {
	return &Service{identity: identity, clock: c, blobs: newBlobStore(dataDir), buckets: map[string]*bucket{}}
}

func (s *Service) Name() string { return "s3" }

// TargetPrefix is empty: S3 requests carry no X-Amz-Target header.
func (s *Service) TargetPrefix() string { return "" }

// Reset drops every bucket. The queue notifier stays: it is wiring, not state.
func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buckets = map[string]*bucket{}
}

// SetQueueNotifier sets the function that delivers an S3 event message to a
// queue. It is called outside the service's lock, once per matching queue
// configuration; its error is ignored, as S3 delivers events asynchronously.
func (s *Service) SetQueueNotifier(fn func(queueARN, messageBody string) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notifier = fn
}

// CreateBucket creates a bucket in the emulator's region.
func (s *Service) CreateBucket(name string, opts BucketOptions) error {
	if err := validateBucketName(name); err != nil {
		return err
	}
	if err := validateLifecycle(opts.Lifecycle); err != nil {
		return err
	}
	notifications, err := notificationsFromQueues(opts.Notifications)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[name]; ok {
		return errBucketExists()
	}
	b := newBucket(name, s.identity.Region, s.clock.Now())
	if opts.Versioning {
		b.versioning = versioningEnabled
	}
	b.lifecycle = slices.Clone(opts.Lifecycle)
	b.notifications = notifications
	s.buckets[name] = b
	return nil
}

// PutObject stores an object as a plain PutObject would, firing the
// bucket's ObjectCreated:Put notifications.
func (s *Service) PutObject(bucketName, key string, body []byte, opts ObjectOptions) error {
	if key == "" {
		return errInvalidArgument("The object key must not be empty.")
	}
	stored, err := s.blobs.put(body)
	if err != nil {
		return err
	}
	obj := &object{
		key:      key,
		body:     stored,
		etag:     etagOf(body),
		meta:     objectMeta{contentType: opts.ContentType, user: lowerKeys(opts.Metadata)},
		checksum: defaultChecksum(body),
	}
	_, err = s.store(bucketName, obj, writeCondition{}, eventPut, eventSource{})
	return err
}

func validateBucketName(name string) error {
	if !validBucketName.MatchString(name) || strings.Contains(name, "..") {
		return errInvalidBucketName()
	}
	return nil
}

// bucketOf returns a bucket; the caller holds s.mu.
func (s *Service) bucketOf(name string) (*bucket, error) {
	b, ok := s.buckets[name]
	if !ok {
		return nil, errNoSuchBucket()
	}
	return b, nil
}

func (s *Service) bucketARN(name string) string { return "arn:aws:s3:::" + name }

func lowerKeys(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[strings.ToLower(k)] = v
	}
	return out
}
