package stratus_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/testcontainers/testcontainers-go"

	stratus "github.com/stratus-emulator/stratus/testcontainers"
)

const manifest = `
sqs:
  queues:
    - name: jobs
dynamodb:
  tables:
    - name: locks
      partitionKey: {name: id}
      ttlAttribute: expires_at
`

// image is the stratus image under test: STRATUS_IMAGE, for example an image
// CI has just built, or the release this module pins.
func image() string {
	if img := os.Getenv("STRATUS_IMAGE"); img != "" {
		return img
	}
	return stratus.DefaultImage
}

func config() aws.Config {
	return aws.Config{
		Region: "eu-central-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	}
}

func run(t *testing.T, opts ...testcontainers.ContainerCustomizer) (*stratus.Container, string) {
	t.Helper()
	ctx := context.Background()
	ctr, err := stratus.Run(ctx, image(), opts...)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	endpoint, err := ctr.BaseEndpoint(ctx)
	if err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	return ctr, endpoint
}

func TestManifestResourcesExistOnceRunReturns(t *testing.T) {
	ctx := context.Background()
	ctr, endpoint := run(t, stratus.WithManifestYAML(manifest))
	client := sqs.NewFromConfig(config(), func(o *sqs.Options) { o.BaseEndpoint = &endpoint })

	url, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("jobs")})
	if err != nil {
		t.Fatalf("GetQueueUrl: %v", err)
	}
	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: url.QueueUrl, MessageBody: aws.String("hello")}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Reset empties the queue and keeps it, as the manifest declares it.
	if err := ctr.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url.QueueUrl, WaitTimeSeconds: 0})
	if err != nil {
		t.Fatalf("ReceiveMessage after Reset: %v", err)
	}
	if len(out.Messages) != 0 {
		t.Fatalf("after Reset the queue still holds %d messages", len(out.Messages))
	}
}

func TestManifestFileAndExpireTTL(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "manifest.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	ctr, endpoint := run(t, stratus.WithManifest(path))
	client := dynamodb.NewFromConfig(config(), func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint })

	past := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
	if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("locks"), Item: map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "stale"}, "expires_at": &ddbtypes.AttributeValueMemberN{Value: past},
	}}); err != nil {
		t.Fatalf("PutItem: %v", err)
	}
	if n, err := ctr.ExpireTTL(ctx); err != nil || n != 1 {
		t.Fatalf("ExpireTTL = %d, %v; want 1 deleted", n, err)
	}
}
