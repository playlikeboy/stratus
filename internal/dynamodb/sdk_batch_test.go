package dynamodb

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSDKBatchWriteAndGet(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	ctx := t.Context()
	h.put(t, "orders", order("c", "gone", "", ""))
	var reqs []types.WriteRequest
	for i := range 24 {
		reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: order("c", fmt.Sprintf("b%02d", i), "", "")}})
	}
	reqs = append(reqs, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: orderKey("c", "gone")}})
	out, err := h.client.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"orders": reqs}})
	if err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}
	if out.UnprocessedItems == nil || len(out.UnprocessedItems) != 0 {
		t.Errorf("UnprocessedItems = %v, want present and empty", out.UnprocessedItems)
	}
	tooMany := append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: order("c", "x", "", "")}})
	_, err = h.client.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"orders": tooMany}})
	wantAPIError(t, err, "ValidationException", "less than or equal to 25")
	dup := []types.WriteRequest{reqs[0], reqs[0]}
	_, err = h.client.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"orders": dup}})
	wantAPIError(t, err, "ValidationException", "Provided list of item keys contains duplicates")

	got, err := h.client.BatchGetItem(ctx, &sdk.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		"orders": {
			Keys:                     []map[string]types.AttributeValue{orderKey("c", "b00"), orderKey("c", "b23"), orderKey("c", "gone")},
			ProjectionExpression:     aws.String("#o"),
			ExpressionAttributeNames: map[string]string{"#o": "order"},
		},
	}})
	if err != nil {
		t.Fatalf("BatchGetItem: %v", err)
	}
	if items := got.Responses["orders"]; len(items) != 2 || len(items[0]) != 1 {
		t.Errorf("Responses = %v, want the two existing items, projected", items)
	}
	if got.UnprocessedKeys == nil || len(got.UnprocessedKeys) != 0 {
		t.Errorf("UnprocessedKeys = %v, want present and empty", got.UnprocessedKeys)
	}
	var keys []map[string]types.AttributeValue
	for i := range 101 {
		keys = append(keys, orderKey("c", fmt.Sprint(i)))
	}
	_, err = h.client.BatchGetItem(ctx, &sdk.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{"orders": {Keys: keys}}})
	wantAPIError(t, err, "ValidationException", "Too many items requested for the BatchGetItem call")
}

func TestSDKTransactions(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	ctx := t.Context()
	h.put(t, "orders", order("c", "1", "10", "open"))
	h.put(t, "orders", order("c", "2", "20", "open"))

	_, err := h.client.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String("orders"), Item: order("c", "3", "30", "open"),
			ConditionExpression: aws.String("attribute_not_exists(customer)")}},
		{Update: &types.Update{TableName: aws.String("orders"), Key: orderKey("c", "1"),
			UpdateExpression:          aws.String("SET #t = #t + :n"),
			ExpressionAttributeNames:  map[string]string{"#t": "total"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":n": num("5")}}},
		{Delete: &types.Delete{TableName: aws.String("orders"), Key: orderKey("c", "2")}},
		{ConditionCheck: &types.ConditionCheck{TableName: aws.String("orders"), Key: orderKey("c", "9"),
			ConditionExpression: aws.String("attribute_not_exists(customer)")}},
	}})
	if err != nil {
		t.Fatalf("TransactWriteItems: %v", err)
	}

	_, err = h.client.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: aws.String("orders"), Key: orderKey("c", "3"),
			UpdateExpression:          aws.String("SET #t = :zero"),
			ExpressionAttributeNames:  map[string]string{"#t": "total"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":zero": num("0")}}},
		{ConditionCheck: &types.ConditionCheck{TableName: aws.String("orders"), Key: orderKey("c", "1"),
			ConditionExpression:                 aws.String("#t = :ten"),
			ExpressionAttributeNames:            map[string]string{"#t": "total"},
			ExpressionAttributeValues:           map[string]types.AttributeValue{":ten": num("10")},
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld}},
		{Update: &types.Update{TableName: aws.String("orders"), Key: orderKey("c", "4"),
			UpdateExpression:          aws.String("ADD note :one"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":one": num("1")}}},
	}})
	var cancelled *types.TransactionCanceledException
	if !errors.As(err, &cancelled) {
		t.Fatalf("TransactWriteItems with a false condition: %v, want TransactionCanceledException", err)
	}
	codes := make([]string, len(cancelled.CancellationReasons))
	for i, r := range cancelled.CancellationReasons {
		codes[i] = aws.ToString(r.Code)
	}
	if strings.Join(codes, ",") != "None,ConditionalCheckFailed,None" {
		t.Errorf("reasons = %v", codes)
	}
	if item := cancelled.CancellationReasons[1].Item; getN(t, item, "total") != "15" {
		t.Errorf("the failed check's Item = %v, want the item with total 15", item)
	}

	got, err := h.client.TransactGetItems(ctx, &sdk.TransactGetItemsInput{TransactItems: []types.TransactGetItem{
		{Get: &types.Get{TableName: aws.String("orders"), Key: orderKey("c", "3")}},
		{Get: &types.Get{TableName: aws.String("orders"), Key: orderKey("c", "2")}},
		{Get: &types.Get{TableName: aws.String("orders"), Key: orderKey("c", "1"), ProjectionExpression: aws.String("#t"), ExpressionAttributeNames: map[string]string{"#t": "total"}}},
	}})
	if err != nil {
		t.Fatalf("TransactGetItems: %v", err)
	}
	r := got.Responses
	if len(r) != 3 || getN(t, r[0].Item, "total") != "30" || r[1].Item != nil || len(r[2].Item) != 1 {
		t.Errorf("responses = %+v, want the cancelled transaction to have changed nothing", r)
	}

	_, err = h.client.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Delete: &types.Delete{TableName: aws.String("orders"), Key: orderKey("c", "1")}},
		{ConditionCheck: &types.ConditionCheck{TableName: aws.String("orders"), Key: orderKey("c", "1"),
			ConditionExpression: aws.String("attribute_exists(customer)")}},
	}})
	wantAPIError(t, err, "ValidationException", "multiple operations on one item")
	_, err = h.client.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: aws.String("orders"), Key: orderKey("c", "1"),
			UpdateExpression:          aws.String("ADD #s :one"),
			ExpressionAttributeNames:  map[string]string{"#s": "status"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":one": num("1")}}},
	}})
	if !errors.As(err, &cancelled) || aws.ToString(cancelled.CancellationReasons[0].Code) != "ValidationError" {
		t.Errorf("an invalid change in a transaction: %v, want a ValidationError reason", err)
	}
}

// TestRawHTTP drives the service the way a non-SDK client does, with a
// placeholder signature that must be accepted as it is.
func TestRawHTTP(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	send := func(target, body string) (int, string) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, h.url+"/", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+target)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/dynamodb/aws4_request, "+
			"SignedHeaders=host, Signature=e2e")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		if ct := resp.Header.Get("Content-Type"); ct != "application/x-amz-json-1.0" {
			t.Errorf("Content-Type = %q", ct)
		}
		return resp.StatusCode, string(b)
	}
	status, body := send("PutItem", `{"TableName":"orders","Item":{"customer":{"S":"c"},"order":{"S":"1"},"n":{"N":"1.10"}}}`)
	if status != http.StatusOK || body != "{}" {
		t.Fatalf("PutItem = %d %s", status, body)
	}
	status, body = send("GetItem", `{"TableName":"orders","Key":{"customer":{"S":"c"},"order":{"S":"1"}}}`)
	if status != http.StatusOK || !strings.Contains(body, `"n":{"N":"1.1"}`) {
		t.Fatalf("GetItem = %d %s", status, body)
	}
	status, body = send("PutItem", `{"TableName":"orders","Item":{"customer":{"S":"c"},"order":{"S":"1"}},`+
		`"ConditionExpression":"attribute_not_exists(customer)","ReturnValuesOnConditionCheckFailure":"ALL_OLD"}`)
	if status != http.StatusBadRequest ||
		!strings.Contains(body, `"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"`) ||
		!strings.Contains(body, `"Item":{`) {
		t.Fatalf("conditional failure = %d %s", status, body)
	}
	status, body = send("GetItem", `{"TableName":"orders","Key":{"customer":{"S":"c"}}}`)
	if status != http.StatusBadRequest || !strings.Contains(body, `"__type":"com.amazon.coral.validate#ValidationException"`) {
		t.Fatalf("invalid key = %d %s", status, body)
	}
	status, body = send("CreateBackup", `{}`)
	if status != http.StatusBadRequest || !strings.Contains(body, "CreateBackup") {
		t.Fatalf("unsupported operation = %d %s", status, body)
	}
	status, body = send("GetItem", `{"TableName":`)
	if status != http.StatusBadRequest || !strings.Contains(body, "SerializationException") {
		t.Fatalf("malformed JSON = %d %s", status, body)
	}
	status, body = send("Query", `{"TableName":"orders","KeyConditions":{}}`)
	if status != http.StatusBadRequest || !strings.Contains(body, "legacy parameter KeyConditions") {
		t.Fatalf("legacy parameter = %d %s", status, body)
	}
}
