package s3_test

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/s3"
)

const mib = 1 << 20

// crc64NVME is computed independently of the service: the CRC-64/NVME
// polynomial, reflected, with Go's standard pre and post inversion.
func crc64NVME(data []byte) string {
	sum := crc64.Checksum(data, crc64.MakeTable(0x9A6C9329AC4BC9B5))
	return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, sum))
}

// uploadParts uploads each part with a CRC64NVME checksum and returns the
// completion list.
func uploadParts(t *testing.T, e *env, bucket, key, uploadID string, parts ...[]byte) []types.CompletedPart {
	t.Helper()
	var done []types.CompletedPart
	for i, p := range parts {
		n := aws.Int32(int32(i + 1))
		out, err := e.client.UploadPart(t.Context(), &s3sdk.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID), PartNumber: n,
			Body: bytes.NewReader(p), ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		done = append(done, types.CompletedPart{PartNumber: n, ETag: out.ETag, ChecksumCRC64NVME: out.ChecksumCRC64NVME})
	}
	return done
}

func TestSDKMultipartCRC64NVME(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()
	e.mustCreateBucket(t, "mpu", s3.BucketOptions{})
	created, err := e.client.CreateMultipartUpload(ctx, &s3sdk.CreateMultipartUploadInput{
		Bucket: aws.String("mpu"), Key: aws.String("big"), ContentType: aws.String("application/x-big"),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme, ChecksumType: types.ChecksumTypeFullObject,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	p1, p2 := bytes.Repeat([]byte{'a'}, 5*mib), []byte("tail")
	parts := uploadParts(t, e, "mpu", "big", aws.ToString(created.UploadId), p1, p2)

	listed, err := e.client.ListParts(ctx, &s3sdk.ListPartsInput{Bucket: aws.String("mpu"), Key: aws.String("big"), UploadId: created.UploadId})
	if err != nil || len(listed.Parts) != 2 || aws.ToInt64(listed.Parts[1].Size) != 4 {
		t.Fatalf("ListParts = %+v, %v", listed, err)
	}
	uploads, err := e.client.ListMultipartUploads(ctx, &s3sdk.ListMultipartUploadsInput{Bucket: aws.String("mpu")})
	if err != nil || len(uploads.Uploads) != 1 {
		t.Fatalf("ListMultipartUploads = %+v, %v", uploads, err)
	}

	complete := func(size int64, parts []types.CompletedPart) (*s3sdk.CompleteMultipartUploadOutput, error) {
		return e.client.CompleteMultipartUpload(ctx, &s3sdk.CompleteMultipartUploadInput{
			Bucket: aws.String("mpu"), Key: aws.String("big"), UploadId: created.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
			MpuObjectSize:   aws.Int64(size), ChecksumType: types.ChecksumTypeFullObject,
		})
	}
	_, err = complete(1, parts)
	wantAPIError(t, err, "InvalidRequest")
	_, err = complete(int64(len(p1)+len(p2)), []types.CompletedPart{parts[1], parts[0]})
	wantAPIError(t, err, "InvalidPartOrder")
	_, err = complete(int64(len(p1)+len(p2)), []types.CompletedPart{{PartNumber: parts[0].PartNumber, ETag: aws.String(`"bad"`)}, parts[1]})
	wantAPIError(t, err, "InvalidPart")

	done, err := complete(int64(len(p1)+len(p2)), parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	whole := append(bytes.Clone(p1), p2...)
	if aws.ToString(done.ChecksumCRC64NVME) != crc64NVME(whole) || done.ChecksumType != types.ChecksumTypeFullObject {
		t.Fatalf("completion checksum %s/%s, want %s FULL_OBJECT", aws.ToString(done.ChecksumCRC64NVME), done.ChecksumType, crc64NVME(whole))
	}
	if etag := aws.ToString(done.ETag); len(etag) < 4 || etag[len(etag)-3:] != `-2"` {
		t.Fatalf("multipart ETag = %s", etag)
	}

	// The SDK validates the full-object checksum of the body it receives.
	got, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("mpu"), Key: aws.String("big"), ChecksumMode: types.ChecksumModeEnabled})
	if body != string(whole) || aws.ToString(got.ChecksumCRC64NVME) != crc64NVME(whole) ||
		aws.ToString(got.ContentType) != "application/x-big" || aws.ToInt32(got.PartsCount) != 2 {
		t.Fatalf("GetObject: %d bytes, checksum %s, type %s, parts %d", len(body), aws.ToString(got.ChecksumCRC64NVME),
			aws.ToString(got.ContentType), aws.ToInt32(got.PartsCount))
	}
	_, part2 := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("mpu"), Key: aws.String("big"), PartNumber: aws.Int32(2)})
	if part2 != "tail" {
		t.Fatalf("part 2 = %q", part2)
	}
	_, err = e.client.ListParts(ctx, &s3sdk.ListPartsInput{Bucket: aws.String("mpu"), Key: aws.String("big"), UploadId: created.UploadId})
	wantAPIError(t, err, "NoSuchUpload")
}

func TestSDKMultipartAbortAndTooSmall(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()
	e.mustCreateBucket(t, "mpu", s3.BucketOptions{})
	created, err := e.client.CreateMultipartUpload(ctx, &s3sdk.CreateMultipartUploadInput{Bucket: aws.String("mpu"), Key: aws.String("k")})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	var parts []types.CompletedPart
	for i, p := range [][]byte{[]byte("small"), []byte("last")} {
		out, err := e.client.UploadPart(ctx, &s3sdk.UploadPartInput{
			Bucket: aws.String("mpu"), Key: aws.String("k"), UploadId: created.UploadId,
			PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(p),
		})
		if err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: out.ETag})
	}
	_, err = e.client.CompleteMultipartUpload(ctx, &s3sdk.CompleteMultipartUploadInput{
		Bucket: aws.String("mpu"), Key: aws.String("k"), UploadId: created.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	wantAPIError(t, err, "EntityTooSmall")

	if _, err := e.client.AbortMultipartUpload(ctx, &s3sdk.AbortMultipartUploadInput{
		Bucket: aws.String("mpu"), Key: aws.String("k"), UploadId: created.UploadId,
	}); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	_, err = e.client.ListParts(ctx, &s3sdk.ListPartsInput{Bucket: aws.String("mpu"), Key: aws.String("k"), UploadId: created.UploadId})
	wantAPIError(t, err, "NoSuchUpload")
	_, err = e.client.AbortMultipartUpload(ctx, &s3sdk.AbortMultipartUploadInput{
		Bucket: aws.String("mpu"), Key: aws.String("k"), UploadId: created.UploadId,
	})
	var noUpload *types.NoSuchUpload
	if !errors.As(err, &noUpload) {
		t.Fatalf("AbortMultipartUpload twice: %v, want *types.NoSuchUpload", err)
	}
	wantStatus(t, err, 404)
}

// manager.Uploader splits a body larger than its part size into a multipart
// upload, reading it as a stream: the reader here cannot seek.
func TestSDKUploaderManager(t *testing.T) {
	e := start(t, clock.System{})
	e.mustCreateBucket(t, "upl", s3.BucketOptions{})
	payload := make([]byte, 12*mib+123)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	uploader := manager.NewUploader(e.client, func(u *manager.Uploader) { u.PartSize = 5 * mib })
	out, err := uploader.Upload(t.Context(), &s3sdk.PutObjectInput{
		Bucket: aws.String("upl"), Key: aws.String("blob"), Body: onlyReader{bytes.NewReader(payload)},
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if out.UploadID == "" {
		t.Fatal("the uploader did not use a multipart upload")
	}
	_, body := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("upl"), Key: aws.String("blob"), ChecksumMode: types.ChecksumModeEnabled})
	if body != string(payload) {
		t.Fatalf("stored %d bytes, want %d", len(body), len(payload))
	}
}
