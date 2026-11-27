package kinesis_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"
)

func wantAPIErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != want {
		t.Fatalf("error = %v, want code %q", err, want)
	}
}

func TestSDKStreamEncryption(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.createStream(t, "secure", 1)

	desc, err := e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStream: %v", err)
	}
	if desc.StreamDescription.EncryptionType != types.EncryptionTypeNone || desc.StreamDescription.KeyId != nil {
		t.Fatalf("initial DescribeStream encryption = (%v, %v)", desc.StreamDescription.EncryptionType, desc.StreamDescription.KeyId)
	}

	// Validation errors.
	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeNone,
		KeyId:          aws.String("alias/test"),
	})
	wantAPIErrorCode(t, err, "InvalidArgumentException")

	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String(""),
	})
	wantAPIErrorCode(t, err, "ValidationException")

	_, err = e.client.StopStreamEncryption(ctx, &sdk.StopStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeNone,
		KeyId:          aws.String("alias/test"),
	})
	wantAPIErrorCode(t, err, "InvalidArgumentException")

	_, err = e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("missing"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String("alias/test"),
	})
	wantAPIErrorCode(t, err, "ResourceNotFoundException")

	const keyID = "arn:aws:kms:eu-central-1:000000000000:key/1234abcd"
	if _, err := e.client.StartStreamEncryption(ctx, &sdk.StartStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String(keyID),
	}); err != nil {
		t.Fatalf("StartStreamEncryption: %v", err)
	}

	desc, err = e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStream after Start: %v", err)
	}
	if desc.StreamDescription.EncryptionType != types.EncryptionTypeKms || aws.ToString(desc.StreamDescription.KeyId) != keyID {
		t.Fatalf("DescribeStream after Start = (%v, %v)", desc.StreamDescription.EncryptionType, aws.ToString(desc.StreamDescription.KeyId))
	}

	sum, err := e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{StreamName: aws.String("secure")})
	if err != nil {
		t.Fatalf("DescribeStreamSummary after Start: %v", err)
	}
	if sum.StreamDescriptionSummary.EncryptionType != types.EncryptionTypeKms || aws.ToString(sum.StreamDescriptionSummary.KeyId) != keyID {
		t.Fatalf("DescribeStreamSummary after Start = (%v, %v)", sum.StreamDescriptionSummary.EncryptionType, aws.ToString(sum.StreamDescriptionSummary.KeyId))
	}

	if _, err := e.client.StopStreamEncryption(ctx, &sdk.StopStreamEncryptionInput{
		StreamName:     aws.String("secure"),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String(keyID),
	}); err != nil {
		t.Fatalf("StopStreamEncryption: %v", err)
	}

	desc, _ = e.client.DescribeStream(ctx, &sdk.DescribeStreamInput{StreamName: aws.String("secure")})
	sum, _ = e.client.DescribeStreamSummary(ctx, &sdk.DescribeStreamSummaryInput{StreamName: aws.String("secure")})
	if desc.StreamDescription.EncryptionType != types.EncryptionTypeNone || desc.StreamDescription.KeyId != nil {
		t.Fatalf("DescribeStream after Stop = (%v, %v)", desc.StreamDescription.EncryptionType, desc.StreamDescription.KeyId)
	}
	if sum.StreamDescriptionSummary.EncryptionType != types.EncryptionTypeNone || sum.StreamDescriptionSummary.KeyId != nil {
		t.Fatalf("DescribeStreamSummary after Stop = (%v, %v)", sum.StreamDescriptionSummary.EncryptionType, sum.StreamDescriptionSummary.KeyId)
	}
}
