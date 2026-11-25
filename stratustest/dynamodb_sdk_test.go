package stratustest_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ODudek/stratus/stratustest"
)

const tableManifest = `
dynamodb:
  tables:
    - name: threads
      partitionKey: {name: owner}
      sortKey: {name: id, type: N}
      ttlAttribute: ttl
      localIndexes:
        - name: by-folder
          sortKey: {name: folder}
          projection: INCLUDE
          nonKeyAttributes: [subject]
`

func TestTableFromTheManifest(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(tableManifest))
	ctx := t.Context()
	client := dynamodb.New(dynamodb.Options{Region: stratustest.Region, BaseEndpoint: aws.String(srv.URL), Credentials: credentials()})

	item := func(id, folder, subject string) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{
			"owner":   &types.AttributeValueMemberS{Value: "acme#u1"},
			"id":      &types.AttributeValueMemberN{Value: id},
			"folder":  &types.AttributeValueMemberS{Value: folder},
			"subject": &types.AttributeValueMemberS{Value: subject},
			"body":    &types.AttributeValueMemberS{Value: "not projected"},
		}
	}
	for _, it := range []map[string]types.AttributeValue{item("1", "inbox", "hello"), item("2", "sent", "re: hello"), item("3", "inbox", "again")} {
		if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("threads"), Item: it, ConditionExpression: aws.String("attribute_not_exists(#o)"),
			ExpressionAttributeNames: map[string]string{"#o": "owner"},
		}); err != nil {
			t.Fatalf("PutItem: %v", err)
		}
	}

	// The same create-if-absent a second time is a conditional failure.
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("threads"), Item: item("1", "inbox", "dup"), ConditionExpression: aws.String("attribute_not_exists(#o)"),
		ExpressionAttributeNames: map[string]string{"#o": "owner"},
	})
	var failed *types.ConditionalCheckFailedException
	if !errors.As(err, &failed) {
		t.Fatalf("second PutItem: %v, want ConditionalCheckFailedException", err)
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String("threads"),
		IndexName:                 aws.String("by-folder"),
		KeyConditionExpression:    aws.String("#o = :o AND folder = :f"),
		ExpressionAttributeNames:  map[string]string{"#o": "owner"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":o": &types.AttributeValueMemberS{Value: "acme#u1"}, ":f": &types.AttributeValueMemberS{Value: "inbox"}},
	})
	if err != nil {
		t.Fatalf("Query on the index: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("index query returned %d items, want the 2 in the inbox", out.Count)
	}
	if _, projected := out.Items[0]["body"]; projected {
		t.Error("the index returned an attribute outside its projection")
	}

	ttl, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String("threads")})
	if err != nil || aws.ToString(ttl.TimeToLiveDescription.AttributeName) != "ttl" {
		t.Fatalf("DescribeTimeToLive: %+v, error %v; want the attribute ttl", ttl, err)
	}

	srv.Reset()
	scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("threads")})
	if err != nil || scan.Count != 0 {
		t.Fatalf("after the reset: %d items, error %v; want an empty table", scan.Count, err)
	}
}
