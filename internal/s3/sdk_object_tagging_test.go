package s3_test

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/s3"
)

func TestSDKObjectTagging(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()

	_, err := e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("missing"), Key: aws.String("k"),
	})
	wantAPIError(t, err, "NoSuchBucket")

	e.mustCreateBucket(t, "objs", s3.BucketOptions{Versioning: true})

	_, err = e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("missing"),
	})
	wantAPIError(t, err, "NoSuchKey")

	p1, err := e.client.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket:  aws.String("objs"),
		Key:     aws.String("doc.txt"),
		Body:    strings.NewReader("v1"),
		Tagging: aws.String("stage=init&owner=alice"),
	})
	if err != nil {
		t.Fatalf("PutObject v1: %v", err)
	}
	v1 := aws.ToString(p1.VersionId)

	// GetObject returns x-amz-tagging-count when tags exist.
	get1, _ := e.mustGet(t, &s3sdk.GetObjectInput{Bucket: aws.String("objs"), Key: aws.String("doc.txt"), VersionId: aws.String(v1)})
	if aws.ToInt32(get1.TagCount) != 2 {
		t.Fatalf("GetObject TagCount = %d, want 2", aws.ToInt32(get1.TagCount))
	}

	// Put second version without tags; v1 keeps its tags and v2 starts empty.
	p2, err := e.client.PutObject(ctx, &s3sdk.PutObjectInput{
		Bucket: aws.String("objs"),
		Key:    aws.String("doc.txt"),
		Body:   strings.NewReader("v2"),
	})
	if err != nil {
		t.Fatalf("PutObject v2: %v", err)
	}
	v2 := aws.ToString(p2.VersionId)

	gotV2, err := e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("doc.txt"), VersionId: aws.String(v2),
	})
	if err != nil || len(gotV2.TagSet) != 0 {
		t.Fatalf("GetObjectTagging v2 = %+v, %v", gotV2, err)
	}

	gotV1, err := e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("doc.txt"), VersionId: aws.String(v1),
	})
	if err != nil {
		t.Fatalf("GetObjectTagging v1: %v", err)
	}
	wantV1 := map[string]string{"stage": "init", "owner": "alice"}
	haveV1 := map[string]string{}
	for _, tag := range gotV1.TagSet {
		haveV1[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if !maps.Equal(haveV1, wantV1) {
		t.Fatalf("v1 tags = %v, want %v", haveV1, wantV1)
	}

	// PutObjectTagging on latest (v2) updates only v2.
	ten := make([]types.Tag, 10)
	for i := range ten {
		ten[i] = types.Tag{Key: aws.String(fmt.Sprintf("k%d", i)), Value: aws.String("v")}
	}
	if _, err := e.client.PutObjectTagging(ctx, &s3sdk.PutObjectTaggingInput{
		Bucket:  aws.String("objs"),
		Key:     aws.String("doc.txt"),
		Tagging: &types.Tagging{TagSet: ten},
	}); err != nil {
		t.Fatalf("PutObjectTagging 10 tags: %v", err)
	}
	gotLatest, err := e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("doc.txt"),
	})
	if err != nil || len(gotLatest.TagSet) != 10 {
		t.Fatalf("GetObjectTagging latest = %+v, %v", gotLatest, err)
	}

	// Over 10 tags, duplicate keys, and key length > 128 are rejected.
	eleven := append(append([]types.Tag(nil), ten...), types.Tag{Key: aws.String("extra"), Value: aws.String("v")})
	_, err = e.client.PutObjectTagging(ctx, &s3sdk.PutObjectTaggingInput{
		Bucket:  aws.String("objs"),
		Key:     aws.String("doc.txt"),
		Tagging: &types.Tagging{TagSet: eleven},
	})
	wantAPIError(t, err, "InvalidTag")

	_, err = e.client.PutObjectTagging(ctx, &s3sdk.PutObjectTaggingInput{
		Bucket: aws.String("objs"),
		Key:    aws.String("doc.txt"),
		Tagging: &types.Tagging{TagSet: []types.Tag{
			{Key: aws.String(strings.Repeat("k", 129)), Value: aws.String("v")},
		}},
	})
	wantAPIError(t, err, "InvalidTag")

	// Missing versionId returns NoSuchVersion.
	_, err = e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("doc.txt"), VersionId: aws.String("nonexistent"),
	})
	wantAPIError(t, err, "NoSuchVersion")

	// DeleteObjectTagging removes only the targeted version's tags.
	if _, err := e.client.DeleteObjectTagging(ctx, &s3sdk.DeleteObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("doc.txt"), VersionId: aws.String(v1),
	}); err != nil {
		t.Fatalf("DeleteObjectTagging v1: %v", err)
	}
	gotV1After, err := e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("doc.txt"), VersionId: aws.String(v1),
	})
	if err != nil || len(gotV1After.TagSet) != 0 {
		t.Fatalf("v1 after DeleteObjectTagging = %+v, %v", gotV1After, err)
	}
	gotV2After, _ := e.client.GetObjectTagging(ctx, &s3sdk.GetObjectTaggingInput{
		Bucket: aws.String("objs"), Key: aws.String("doc.txt"), VersionId: aws.String(v2),
	})
	if len(gotV2After.TagSet) != 10 {
		t.Fatalf("v2 tags unexpectedly altered: %d", len(gotV2After.TagSet))
	}
}
