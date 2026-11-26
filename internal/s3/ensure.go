package s3

import "slices"

// EnsureBucket creates the bucket, or, when one of that name exists, as after
// a restore, gives it the options' versioning, lifecycle and notifications.
// Its objects stay. A versioned bucket the options do not version is
// suspended: S3 cannot make a bucket unversioned again.
func (s *Service) EnsureBucket(name string, opts BucketOptions) error {
	if err := validateLifecycle(opts.Lifecycle); err != nil {
		return err
	}
	notifications, err := notificationsFromQueues(opts.Notifications)
	if err != nil {
		return err
	}
	s.mu.Lock()
	b, ok := s.buckets[name]
	if !ok {
		s.mu.Unlock()
		return s.CreateBucket(name, opts)
	}
	defer s.mu.Unlock()
	switch {
	case opts.Versioning:
		b.versioning = versioningEnabled
	case b.versioning == versioningEnabled:
		b.versioning = versioningSuspended
	}
	b.lifecycle = slices.Clone(opts.Lifecycle)
	b.notifications = notifications
	return nil
}
