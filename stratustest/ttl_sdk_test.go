package stratustest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/stratus-emulator/stratus/stratustest"
)

const ttlManifest = `
dynamodb:
  tables:
    - name: locks
      partitionKey: {name: id}
      ttlAttribute: expires_at
`

func putLock(t *testing.T, client *dynamodb.Client, id string, expiresAt time.Time) {
	t.Helper()
	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("locks"),
		Item: map[string]types.AttributeValue{
			"id":         &types.AttributeValueMemberS{Value: id},
			"expires_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(expiresAt.Unix(), 10)},
		},
	})
	if err != nil {
		t.Fatalf("PutItem(%s): %v", id, err)
	}
}

func lockExists(t *testing.T, client *dynamodb.Client, id string) bool {
	t.Helper()
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("locks"),
		Key:       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}},
	})
	if err != nil {
		t.Fatalf("GetItem(%s): %v", id, err)
	}
	return out.Item != nil
}

// Without a TTL interval nothing expires by itself; POST /_stratus/expire-ttl
// deletes exactly the items past their TTL.
func TestExpireTTLEndpoint(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(ttlManifest))
	client := dynamodb.New(dynamodb.Options{Region: stratustest.Region, BaseEndpoint: aws.String(srv.URL), Credentials: credentials()})
	putLock(t, client, "stale", time.Now().Add(-time.Hour))
	putLock(t, client, "fresh", time.Now().Add(time.Hour))

	if !lockExists(t, client, "stale") {
		t.Fatal("an expired item was deleted with no TTL interval and no request to expire")
	}
	resp, err := http.Post(srv.URL+"/_stratus/expire-ttl", "", nil)
	if err != nil {
		t.Fatalf("POST expire-ttl: %v", err)
	}
	defer resp.Body.Close()
	var body struct{ Deleted int }
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK || body.Deleted != 1 {
		t.Fatalf("expire-ttl = %d %+v (%v), want 200 and 1 deleted", resp.StatusCode, body, err)
	}
	if lockExists(t, client, "stale") || !lockExists(t, client, "fresh") {
		t.Fatal("expire-ttl must delete the expired item and keep the other")
	}
}

func TestTTLIntervalExpiresInTheBackground(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(ttlManifest), stratustest.WithTTLInterval(10*time.Millisecond))
	client := dynamodb.New(dynamodb.Options{Region: stratustest.Region, BaseEndpoint: aws.String(srv.URL), Credentials: credentials()})
	putLock(t, client, "stale", time.Now().Add(-time.Hour))
	putLock(t, client, "fresh", time.Now().Add(time.Hour))

	deadline := time.Now().Add(5 * time.Second)
	for lockExists(t, client, "stale") {
		if time.Now().After(deadline) {
			t.Fatal("the expired item is still there after the TTL interval passed many times")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !lockExists(t, client, "fresh") {
		t.Fatal("an item whose TTL is in the future was deleted")
	}
	if n := srv.ExpireTTL(); n != 0 {
		t.Fatalf("ExpireTTL after the background run = %d, want 0", n)
	}
}
