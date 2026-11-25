package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

type createBucketConfiguration struct {
	XMLName            xml.Name
	LocationConstraint string `xml:"LocationConstraint"`
}

// createBucket accepts an optional CreateBucketConfiguration; its
// LocationConstraint must be the emulator's region.
func (s *Service) createBucket(w http.ResponseWriter, r *http.Request, name string) error {
	p, err := readPayload(r, maxXMLBody)
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(p.data))) > 0 {
		var cfg createBucketConfiguration
		if err := xml.Unmarshal(p.data, &cfg); err != nil {
			return errMalformedXML()
		}
		if cfg.LocationConstraint != "" && cfg.LocationConstraint != s.identity.Region {
			return errIllegalLocation(cfg.LocationConstraint)
		}
	}
	if err := s.CreateBucket(name, BucketOptions{}); err != nil {
		return err
	}
	w.Header().Set("Location", "/"+name)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Service) deleteBucket(w http.ResponseWriter, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucketOf(name)
	if err != nil {
		return err
	}
	if !b.isEmpty() {
		return errBucketNotEmpty()
	}
	delete(s.buckets, name)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) headBucket(w http.ResponseWriter, name string) error {
	region, err := withBucket(s, name, func(b *bucket) string { return b.region })
	if err != nil {
		return err
	}
	w.Header().Set("x-amz-bucket-region", region)
	w.WriteHeader(http.StatusOK)
	return nil
}

// withBucket reads something of a bucket under the read lock.
func withBucket[T any](s *Service, name string, read func(*bucket) T) (T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucketOf(name)
	if err != nil {
		var zero T
		return zero, err
	}
	return read(b), nil
}

// updateBucket changes a bucket under the write lock.
func (s *Service) updateBucket(name string, update func(*bucket) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.bucketOf(name)
	if err != nil {
		return err
	}
	return update(b)
}

type listAllMyBucketsResult struct {
	XMLName xml.Name
	Owner   owner         `xml:"Owner"`
	Buckets []bucketEntry `xml:"Buckets>Bucket"`
	Prefix  string        `xml:"Prefix,omitempty"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
	BucketRegion string `xml:"BucketRegion"`
}

func (s *Service) listBuckets(w http.ResponseWriter, q url.Values) error {
	prefix := q.Get("prefix")
	s.mu.RLock()
	out := listAllMyBucketsResult{XMLName: xmlName("ListAllMyBucketsResult"), Owner: s.owner(), Prefix: prefix}
	for _, b := range s.buckets {
		if strings.HasPrefix(b.name, prefix) {
			out.Buckets = append(out.Buckets, bucketEntry{Name: b.name, CreationDate: xmlTime(b.createdAt), BucketRegion: b.region})
		}
	}
	s.mu.RUnlock()
	slices.SortFunc(out.Buckets, func(a, b bucketEntry) int { return strings.Compare(a.Name, b.Name) })
	writeXML(w, out)
	return nil
}

type locationConstraint struct {
	XMLName xml.Name
	Region  string `xml:",chardata"`
}

// getBucketLocation reports the region; us-east-1 is the empty constraint.
func (s *Service) getBucketLocation(w http.ResponseWriter, name string) error {
	region, err := withBucket(s, name, func(b *bucket) string { return b.region })
	if err != nil {
		return err
	}
	if region == "us-east-1" {
		region = ""
	}
	writeXML(w, locationConstraint{XMLName: xmlName("LocationConstraint"), Region: region})
	return nil
}

type versioningConfiguration struct {
	XMLName xml.Name
	Status  string `xml:"Status,omitempty"`
}

func (s *Service) getBucketVersioning(w http.ResponseWriter, name string) error {
	status, err := withBucket(s, name, func(b *bucket) string { return b.versioning })
	if err != nil {
		return err
	}
	writeXML(w, versioningConfiguration{XMLName: xmlName("VersioningConfiguration"), Status: status})
	return nil
}

// putBucketVersioning enables or suspends versioning; a bucket never goes
// back to the unversioned state.
func (s *Service) putBucketVersioning(w http.ResponseWriter, r *http.Request, name string) error {
	var cfg versioningConfiguration
	if err := readXML(r, &cfg); err != nil {
		return err
	}
	if cfg.Status != versioningEnabled && cfg.Status != versioningSuspended {
		return errMalformedXML()
	}
	if err := s.updateBucket(name, func(b *bucket) error { b.versioning = cfg.Status; return nil }); err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Service) putBucketLifecycle(w http.ResponseWriter, r *http.Request, name string) error {
	var cfg lifecycleConfiguration
	if err := readXML(r, &cfg); err != nil {
		return err
	}
	rules, err := rulesFromXML(cfg)
	if err != nil {
		return err
	}
	if err := s.updateBucket(name, func(b *bucket) error { b.lifecycle = rules; return nil }); err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Service) getBucketLifecycle(w http.ResponseWriter, name string) error {
	rules, err := withBucket(s, name, func(b *bucket) []LifecycleRule { return b.lifecycle })
	if err != nil {
		return err
	}
	if rules == nil {
		return errNoSuchLifecycle()
	}
	cfg := rulesToXML(rules)
	cfg.XMLName = xmlName("LifecycleConfiguration")
	writeXML(w, cfg)
	return nil
}

func (s *Service) deleteBucketLifecycle(w http.ResponseWriter, name string) error {
	if err := s.updateBucket(name, func(b *bucket) error { b.lifecycle = nil; return nil }); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// putBucketNotification replaces the notification configuration; an empty
// one turns notifications off.
func (s *Service) putBucketNotification(w http.ResponseWriter, r *http.Request, name string) error {
	var cfg notificationConfiguration
	if err := readXML(r, &cfg); err != nil {
		return err
	}
	if err := validateNotifications(&cfg); err != nil {
		return err
	}
	if err := s.updateBucket(name, func(b *bucket) error { b.notifications = cfg; return nil }); err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Service) getBucketNotification(w http.ResponseWriter, name string) error {
	cfg, err := withBucket(s, name, func(b *bucket) notificationConfiguration { return b.notifications })
	if err != nil {
		return err
	}
	cfg.XMLName = xmlName("NotificationConfiguration")
	writeXML(w, cfg)
	return nil
}
