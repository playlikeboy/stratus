package s3

import (
	"bytes"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
)

func newDiskService(t *testing.T) (*Service, string) {
	t.Helper()
	dir := t.TempDir()
	s := New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clock.System{}, dir)
	if err := s.CreateBucket("files", BucketOptions{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return s, dir
}

func filesIn(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	return len(entries)
}

func TestObjectBytesAreKeptInAFile(t *testing.T) {
	s, dir := newDiskService(t)
	body := bytes.Repeat([]byte("stratus "), 1000)
	if err := s.PutObject("files", "a.txt", body, ObjectOptions{}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if err := s.PutObject("files", "empty", nil, ObjectOptions{}); err != nil {
		t.Fatalf("PutObject empty: %v", err)
	}
	if n := filesIn(t, dir); n != 1 {
		t.Fatalf("%d files in the data directory, want 1: an empty object needs none", n)
	}

	o := s.buckets["files"].objects["a.txt"][0]
	if o.size() != int64(len(body)) {
		t.Fatalf("size = %d, want %d", o.size(), len(body))
	}
	got, err := o.body.read(8, 16)
	if err != nil || string(got) != "stratus stratus " {
		t.Fatalf("read(8, 16) = %q, %v", got, err)
	}
	if e := s.buckets["files"].objects["empty"][0]; e.size() != 0 {
		t.Fatalf("empty object size = %d", e.size())
	}
}

func TestFileIsRemovedOnceNothingRefersToTheObject(t *testing.T) {
	s, dir := newDiskService(t)
	if err := s.PutObject("files", "a.txt", []byte("hello"), ObjectOptions{}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	s.Reset()
	deadline := time.Now().Add(5 * time.Second)
	for filesIn(t, dir) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the object's file is still there after the object was dropped")
		}
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConcatHashesWhatItWrites(t *testing.T) {
	st := newBlobStore(t.TempDir())
	a, err := st.put([]byte("hello, "))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	b, err := st.put([]byte("world"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	h := newHash(algoCRC32)
	joined, err := st.concat([]*blob{a, emptyBlob, b}, h)
	if err != nil {
		t.Fatalf("concat: %v", err)
	}
	all, err := joined.bytes()
	if err != nil || string(all) != "hello, world" {
		t.Fatalf("bytes = %q, %v", all, err)
	}
	want := newHash(algoCRC32)
	_, _ = want.Write([]byte("hello, world"))
	if !bytes.Equal(h.Sum(nil), want.Sum(nil)) {
		t.Fatal("the hash does not match the concatenated bytes")
	}
}
