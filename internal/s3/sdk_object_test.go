package s3_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ODudek/stratus/internal/clock"
	"github.com/ODudek/stratus/internal/s3"
)

func TestSDKObjectRoundTrip(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()
	if _, err := e.client.CreateBucket(ctx, &s3sdk.CreateBucketInput{
		Bucket: aws.String("docs"),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(identity.Region),
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	key := "dir//sub/../my file é.txt"
	put, err := e.client.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: aws.String("docs"), Key: aws.String(key), Body: strings.NewReader("hello world"),
		ContentType: aws.String("text/plain"), ContentDisposition: aws.String(`attachment; filename="a.txt"`),
		CacheControl: aws.String("max-age=60"), ContentEncoding: aws.String("identity"),
		Metadata: map[string]string{"owner": "team-a", "Mixed-Case": "v"},
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if aws.ToString(put.ETag) != `"5eb63bbbe01eeed093cb22bb8f5acdc3"` {
		t.Fatalf("ETag = %s", aws.ToString(put.ETag))
	}

	got, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("docs"), Key: aws.String(key)})
	if body != "hello world" || aws.ToString(got.ContentType) != "text/plain" ||
		aws.ToString(got.ContentDisposition) != `attachment; filename="a.txt"` ||
		aws.ToString(got.CacheControl) != "max-age=60" || aws.ToString(got.ContentEncoding) != "identity" ||
		got.Metadata["owner"] != "team-a" || got.Metadata["mixed-case"] != "v" ||
		aws.ToInt64(got.ContentLength) != 11 || aws.ToString(got.AcceptRanges) != "bytes" || got.LastModified == nil {
		t.Fatalf("GetObject = %+v, body %q", got, body)
	}

	head, err := e.client.HeadObject(ctx, &s3sdk.HeadObjectInput{
		Bucket: aws.String("docs"), Key: aws.String(key), ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil || aws.ToInt64(head.ContentLength) != 11 || aws.ToString(head.ETag) != aws.ToString(put.ETag) ||
		aws.ToString(head.ChecksumCRC64NVME) == "" {
		t.Fatalf("HeadObject = %+v, %v", head, err)
	}

	list, err := e.client.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{Bucket: aws.String("docs")})
	if err != nil || len(list.Contents) != 1 || aws.ToString(list.Contents[0].Key) != key {
		t.Fatalf("ListObjectsV2 = %+v, %v", list, err)
	}

	if _, err := e.client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: aws.String("docs"), Key: aws.String(key)}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	_, err = e.client.HeadObject(ctx, &s3sdk.HeadObjectInput{Bucket: aws.String("docs"), Key: aws.String(key)})
	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("HeadObject after delete: %v, want NotFound", err)
	}
	if _, err := e.client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: aws.String("docs")}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
}

func TestSDKTypedErrors(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()
	_, err := e.client.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{Bucket: aws.String("missing")})
	var noBucket *types.NoSuchBucket
	if !errors.As(err, &noBucket) {
		t.Fatalf("ListObjectsV2 on a missing bucket: %v, want *types.NoSuchBucket", err)
	}
	e.mustCreateBucket(t, "bkt", s3.BucketOptions{})
	_, err = e.client.GetObject(ctx, &s3sdk.GetObjectInput{Bucket: aws.String("bkt"), Key: aws.String("k")})
	var noKey *types.NoSuchKey
	if !errors.As(err, &noKey) {
		t.Fatalf("GetObject on a missing key: %v, want *types.NoSuchKey", err)
	}
	_, err = e.client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("bkt")})
	var owned *types.BucketAlreadyOwnedByYou
	if !errors.As(err, &owned) {
		t.Fatalf("CreateBucket twice: %v, want BucketAlreadyOwnedByYou", err)
	}
	wantStatus(t, err, 409)
	e.mustPut(t, "bkt", "k", "x")
	_, err = e.client.DeleteBucket(ctx, &s3sdk.DeleteBucketInput{Bucket: aws.String("bkt")})
	wantAPIError(t, err, "BucketNotEmpty")
	_, err = e.client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: aws.String("missing")})
	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("HeadBucket on a missing bucket: %v, want NotFound", err)
	}
	_, err = e.client.GetBucketPolicy(ctx, &s3sdk.GetBucketPolicyInput{Bucket: aws.String("bkt")})
	wantAPIError(t, err, "NotImplemented")
	wantStatus(t, err, 501)
	if !strings.Contains(err.Error(), "GetBucketPolicy") {
		t.Fatalf("NotImplemented does not name the operation: %v", err)
	}
}

// Over HTTPS the SDK streams a body it cannot seek as aws-chunked with a
// trailing CRC32 (STREAMING-UNSIGNED-PAYLOAD-TRAILER), without a length.
func TestSDKStreamedPutUnseekable(t *testing.T) {
	e := startTLS(t)
	e.mustCreateBucket(t, "stream", s3.BucketOptions{})
	payload := bytes.Repeat([]byte("stream-me "), 300_000) // 3 MB, several chunks
	if _, err := e.client.PutObject(t.Context(), &s3sdk.PutObjectInput{
		Bucket: aws.String("stream"), Key: aws.String("big"), Body: onlyReader{bytes.NewReader(payload)},
	}); err != nil {
		t.Fatalf("PutObject from an unseekable reader: %v", err)
	}
	_, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("stream"), Key: aws.String("big")})
	if body != string(payload) {
		t.Fatalf("stored %d bytes, want %d", len(body), len(payload))
	}
}

func TestSDKRangeGet(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "bkt", s3.BucketOptions{})
	e.mustPut(t, "bkt", "digits", "0123456789")
	for rng, want := range map[string]string{
		"bytes=2-4": "234", "bytes=7-": "789", "bytes=-3": "789", "bytes=8-100": "89",
	} {
		out, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("bkt"), Key: aws.String("digits"), Range: aws.String(rng)})
		if body != want || aws.ToString(out.ContentRange) == "" {
			t.Errorf("%s: body %q range %q, want %q", rng, body, aws.ToString(out.ContentRange), want)
		}
	}
	_, err := e.client.GetObject(t.Context(), &s3sdk.GetObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("digits"), Range: aws.String("bytes=10-20"),
	})
	wantAPIError(t, err, "InvalidRange")
	wantStatus(t, err, 416)
}

func TestSDKCreateIfAbsent(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "bkt", s3.BucketOptions{})
	put := func(body string) (*s3sdk.PutObjectOutput, error) {
		return e.client.PutObject(t.Context(), &s3sdk.PutObjectInput{
			Bucket: aws.String("bkt"), Key: aws.String("lock"), Body: strings.NewReader(body), IfNoneMatch: aws.String("*"),
		})
	}
	first, err := put("one")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err = put("two")
	wantAPIError(t, err, "PreconditionFailed")
	wantStatus(t, err, 412)

	_, err = e.client.PutObject(t.Context(), &s3sdk.PutObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("lock"), Body: strings.NewReader("three"), IfMatch: aws.String(`"wrong"`),
	})
	wantStatus(t, err, 412)
	if _, err := e.client.PutObject(t.Context(), &s3sdk.PutObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("lock"), Body: strings.NewReader("four"), IfMatch: first.ETag,
	}); err != nil {
		t.Fatalf("PutObject with a matching If-Match: %v", err)
	}
	if _, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("bkt"), Key: aws.String("lock")}); body != "four" {
		t.Fatalf("body = %q", body)
	}
	_, err = e.client.GetObject(t.Context(), &s3sdk.GetObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("lock"), IfMatch: aws.String(`"nope"`),
	})
	wantStatus(t, err, 412)
}

func TestSDKCopyObject(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()
	e.mustCreateBucket(t, "src", s3.BucketOptions{})
	e.mustCreateBucket(t, "dst", s3.BucketOptions{})
	if _, err := e.client.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: aws.String("src"), Key: aws.String("a b"), Body: strings.NewReader("payload"),
		ContentType: aws.String("text/x-a"), Metadata: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if _, err := e.client.CopyObject(ctx, &s3sdk.CopyObjectInput{
		Bucket: aws.String("dst"), Key: aws.String("copied"), CopySource: aws.String("src/a%20b"),
	}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	got, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("dst"), Key: aws.String("copied")})
	if body != "payload" || aws.ToString(got.ContentType) != "text/x-a" || got.Metadata["k"] != "v" {
		t.Fatalf("copy (COPY) = %+v %q", got, body)
	}
	if _, err := e.client.CopyObject(ctx, &s3sdk.CopyObjectInput{
		Bucket: aws.String("dst"), Key: aws.String("replaced"), CopySource: aws.String("src/a%20b"),
		MetadataDirective: types.MetadataDirectiveReplace, ContentType: aws.String("text/x-b"),
		Metadata: map[string]string{"n": "w"},
	}); err != nil {
		t.Fatalf("CopyObject REPLACE: %v", err)
	}
	got, _ = e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("dst"), Key: aws.String("replaced")})
	if aws.ToString(got.ContentType) != "text/x-b" || got.Metadata["n"] != "w" || got.Metadata["k"] != "" {
		t.Fatalf("copy (REPLACE) = %+v", got)
	}
	_, err := e.client.CopyObject(ctx, &s3sdk.CopyObjectInput{
		Bucket: aws.String("src"), Key: aws.String("a b"), CopySource: aws.String("src/a%20b"),
	})
	wantAPIError(t, err, "InvalidRequest")
}

func TestSDKDeleteObjects(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "bkt", s3.BucketOptions{})
	for _, k := range []string{"a", "b", "c"} {
		e.mustPut(t, "bkt", k, k)
	}
	out, err := e.client.DeleteObjects(t.Context(), &s3sdk.DeleteObjectsInput{
		Bucket: aws.String("bkt"),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("a")}, {Key: aws.String("b")}, {Key: aws.String("nope")}}},
	})
	if err != nil || len(out.Deleted) != 3 || len(out.Errors) != 0 {
		t.Fatalf("DeleteObjects = %+v, %v", out, err)
	}
	out, err = e.client.DeleteObjects(t.Context(), &s3sdk.DeleteObjectsInput{
		Bucket: aws.String("bkt"),
		Delete: &types.Delete{Quiet: aws.Bool(true), Objects: []types.ObjectIdentifier{{Key: aws.String("c")}}},
	})
	if err != nil || len(out.Deleted) != 0 {
		t.Fatalf("quiet DeleteObjects = %+v, %v", out, err)
	}
	list, err := e.client.ListObjectsV2(t.Context(), &s3sdk.ListObjectsV2Input{Bucket: aws.String("bkt")})
	if err != nil || len(list.Contents) != 0 {
		t.Fatalf("after DeleteObjects: %+v, %v", list.Contents, err)
	}
}

func TestSDKListObjectsV2Pagination(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "bkt", s3.BucketOptions{})
	for _, k := range []string{"logs/2026/a", "logs/2026/b", "logs/2027/a", "logs/top", "readme", "src/main.go", "z"} {
		e.mustPut(t, "bkt", k, k)
	}
	var keys, prefixes []string
	paginator := s3sdk.NewListObjectsV2Paginator(e.client, &s3sdk.ListObjectsV2Input{
		Bucket: aws.String("bkt"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1),
	})
	pages := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(t.Context())
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		pages++
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		for _, p := range page.CommonPrefixes {
			prefixes = append(prefixes, aws.ToString(p.Prefix))
		}
	}
	if pages != 4 || strings.Join(keys, ",") != "readme,z" || strings.Join(prefixes, ",") != "logs/,src/" {
		t.Fatalf("pages %d keys %v prefixes %v", pages, keys, prefixes)
	}
	out, err := e.client.ListObjectsV2(t.Context(), &s3sdk.ListObjectsV2Input{
		Bucket: aws.String("bkt"), Prefix: aws.String("logs/"), Delimiter: aws.String("/"), StartAfter: aws.String("logs/2026/"),
	})
	if err != nil || len(out.CommonPrefixes) != 1 || aws.ToString(out.CommonPrefixes[0].Prefix) != "logs/2027/" ||
		len(out.Contents) != 1 || aws.ToInt32(out.KeyCount) != 2 {
		t.Fatalf("prefix + start-after = %+v, %v", out, err)
	}
}

func TestSDKVersioning(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()
	e.mustCreateBucket(t, "ver", s3.BucketOptions{})
	if put := e.mustPut(t, "ver", "k", "unversioned"); put.VersionId != nil {
		t.Fatalf("version id before versioning: %s", aws.ToString(put.VersionId))
	}
	if _, err := e.client.PutBucketVersioning(ctx, &s3sdk.PutBucketVersioningInput{
		Bucket: aws.String("ver"), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
	status, err := e.client.GetBucketVersioning(ctx, &s3sdk.GetBucketVersioningInput{Bucket: aws.String("ver")})
	if err != nil || status.Status != types.BucketVersioningStatusEnabled {
		t.Fatalf("GetBucketVersioning = %+v, %v", status, err)
	}
	v1 := aws.ToString(e.mustPut(t, "ver", "k", "one").VersionId)
	v2 := aws.ToString(e.mustPut(t, "ver", "k", "two").VersionId)
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatalf("version ids %q %q", v1, v2)
	}
	del, err := e.client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: aws.String("ver"), Key: aws.String("k")})
	if err != nil || !aws.ToBool(del.DeleteMarker) || del.VersionId == nil {
		t.Fatalf("DeleteObject = %+v, %v", del, err)
	}
	_, err = e.client.GetObject(ctx, &s3sdk.GetObjectInput{Bucket: aws.String("ver"), Key: aws.String("k")})
	var noKey *types.NoSuchKey
	if !errors.As(err, &noKey) {
		t.Fatalf("GetObject behind a delete marker: %v", err)
	}
	if _, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("ver"), Key: aws.String("k"), VersionId: aws.String(v1)}); body != "one" {
		t.Fatalf("version %s = %q", v1, body)
	}
	if _, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("ver"), Key: aws.String("k"), VersionId: aws.String("null")}); body != "unversioned" {
		t.Fatalf("null version = %q", body)
	}
	versions, err := e.client.ListObjectVersions(ctx, &s3sdk.ListObjectVersionsInput{Bucket: aws.String("ver")})
	if err != nil || len(versions.Versions) != 3 || len(versions.DeleteMarkers) != 1 || !aws.ToBool(versions.DeleteMarkers[0].IsLatest) {
		t.Fatalf("ListObjectVersions = %+v, %v", versions, err)
	}
	// Removing the delete marker by its version id brings "two" back.
	if _, err := e.client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: aws.String("ver"), Key: aws.String("k"), VersionId: del.VersionId}); err != nil {
		t.Fatalf("DeleteObject of the marker: %v", err)
	}
	if _, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("ver"), Key: aws.String("k")}); body != "two" {
		t.Fatalf("after removing the marker: %q", body)
	}
	if _, err := e.client.DeleteObject(ctx, &s3sdk.DeleteObjectInput{Bucket: aws.String("ver"), Key: aws.String("k"), VersionId: aws.String(v2)}); err != nil {
		t.Fatalf("DeleteObject by version: %v", err)
	}
	if _, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("ver"), Key: aws.String("k")}); body != "one" {
		t.Fatalf("after deleting %s: %q", v2, body)
	}
}

func TestSDKListBucketsAndLocation(t *testing.T) {
	e := start(t, clock.System{})
	for _, b := range []string{"zeta", "alpha"} {
		e.mustCreateBucket(t, b, s3.BucketOptions{})
	}
	out, err := e.client.ListBuckets(t.Context(), &s3sdk.ListBucketsInput{})
	if err != nil || len(out.Buckets) != 2 || aws.ToString(out.Buckets[0].Name) != "alpha" {
		t.Fatalf("ListBuckets = %+v, %v", out, err)
	}
	loc, err := e.client.GetBucketLocation(t.Context(), &s3sdk.GetBucketLocationInput{Bucket: aws.String("alpha")})
	if err != nil || string(loc.LocationConstraint) != identity.Region {
		t.Fatalf("GetBucketLocation = %+v, %v", loc, err)
	}
	_, err = e.client.CreateBucket(t.Context(), &s3sdk.CreateBucketInput{Bucket: aws.String("Bad_Name")})
	wantAPIError(t, err, "InvalidBucketName")
}
