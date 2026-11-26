package s3

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
)

func TestASnapshotKeepsBucketsObjectsVersionsAndUploads(t *testing.T) {
	identity := awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}
	clk := clock.NewFake(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	dir := t.TempDir()
	s := New(identity, clk, dir)
	if err := s.CreateBucket("docs", BucketOptions{Versioning: true, Lifecycle: []LifecycleRule{{Prefix: "tmp/", ExpirationDays: 7}}}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"v1", "v2"} {
		if err := s.PutObject("docs", "a.txt", []byte(body), ObjectOptions{ContentType: "text/plain", Metadata: map[string]string{"Owner": "me"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutObject("docs", "empty", nil, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutObject("docs", "gone.txt", []byte("x"), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.buckets["docs"].deleteKey("gone.txt", clk.Now())
	s.mu.Unlock()
	u := &upload{key: "big.bin"}
	if err := s.createUpload("docs", u); err != nil {
		t.Fatal(err)
	}
	partBody, err := s.blobs.put([]byte("part one"))
	if err != nil {
		t.Fatal(err)
	}
	u.parts[1] = &part{number: 1, body: partBody, etag: `"p1"`, lastModified: clk.Now()}

	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	stray := filepath.Join(dir, "zzzz")
	if err := os.WriteFile(stray, []byte("written after the snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}

	next := New(identity, clk, dir)
	if err := next.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	b := next.buckets["docs"]
	if b == nil || b.versioning != versioningEnabled || len(b.lifecycle) != 1 {
		t.Fatalf("bucket after Restore: %+v", b)
	}
	if versions := b.objects["a.txt"]; len(versions) != 2 {
		t.Fatalf("a.txt has %d versions after Restore, want 2", len(versions))
	}
	cur := b.current("a.txt")
	got, err := cur.body.bytes()
	if err != nil || string(got) != "v2" || cur.meta.contentType != "text/plain" || cur.meta.user["owner"] != "me" {
		t.Fatalf("current a.txt after Restore: %q, %v, %+v", got, err, cur.meta)
	}
	if b.current("gone.txt") != nil || len(b.objects["gone.txt"]) != 2 {
		t.Fatalf("gone.txt after Restore: current %v, %d versions; want hidden by its delete marker", b.current("gone.txt"), len(b.objects["gone.txt"]))
	}
	if e := b.current("empty"); e == nil || e.size() != 0 {
		t.Fatalf("empty object after Restore: %+v", e)
	}
	ru := b.uploads[u.id]
	if ru == nil || ru.parts[1] == nil {
		t.Fatalf("upload after Restore: %+v", ru)
	}
	if p, _ := ru.parts[1].body.bytes(); string(p) != "part one" {
		t.Fatalf("part after Restore: %q", p)
	}
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Errorf("a file no version names is still there after Restore: %v", err)
	}
	// New objects never take the name of a restored one.
	if err := next.PutObject("docs", "new.txt", []byte("new"), ObjectOptions{}); err != nil {
		t.Fatalf("PutObject after Restore: %v", err)
	}
	if again, _ := next.buckets["docs"].current("a.txt").body.bytes(); string(again) != "v2" {
		t.Fatalf("a.txt after a new put: %q", again)
	}
}

func TestRestoreDropsAVersionWhoseBytesAreGone(t *testing.T) {
	identity := awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}
	clk := clock.NewFake(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	dir := t.TempDir()
	s := New(identity, clk, dir)
	if err := s.CreateBucket("docs", BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutObject("docs", "a.txt", []byte("bytes"), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	data, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(s.buckets["docs"].current("a.txt").body.path); err != nil {
		t.Fatal(err)
	}
	next := New(identity, clk, dir)
	if err := next.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if o := next.buckets["docs"].current("a.txt"); o != nil {
		t.Fatalf("a.txt whose bytes are gone is still listed: %+v", o)
	}
}
