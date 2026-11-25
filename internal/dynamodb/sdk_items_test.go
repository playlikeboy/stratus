package dynamodb

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSDKItemTypesRoundTrip(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	item := order("c", "all-types", "", "")
	item["s"] = str("ünïcode")
	item["n"] = num("001.500")
	item["big"] = num("12345678901234567890123456789012345678")
	item["exp"] = num("1E+3")
	item["b"] = &types.AttributeValueMemberB{Value: []byte{0, 1, 255}}
	item["t"] = &types.AttributeValueMemberBOOL{Value: true}
	item["null"] = &types.AttributeValueMemberNULL{Value: true}
	item["l"] = &types.AttributeValueMemberL{Value: []types.AttributeValue{str("x"), num("2")}}
	item["m"] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"k": str("v")}}
	item["ss"] = &types.AttributeValueMemberSS{Value: []string{"a", "b"}}
	item["ns"] = &types.AttributeValueMemberNS{Value: []string{"1", "2.50"}}
	item["bs"] = &types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}}
	h.put(t, "orders", item)

	out, err := h.client.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String("orders"), Key: orderKey("c", "all-types"), ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	got := out.Item
	if getS(t, got, "s") != "ünïcode" || getN(t, got, "n") != "1.5" || getN(t, got, "exp") != "1000" ||
		getN(t, got, "big") != "12345678901234567890123456789012345678" {
		t.Errorf("scalars did not round-trip, normalised: %v", got)
	}
	if b := got["b"].(*types.AttributeValueMemberB).Value; string(b) != "\x00\x01\xff" {
		t.Errorf("B = %v", b)
	}
	if ns := got["ns"].(*types.AttributeValueMemberNS).Value; len(ns) != 2 || ns[1] != "2.5" {
		t.Errorf("NS = %v, want normalised members", ns)
	}
	if m := got["m"].(*types.AttributeValueMemberM).Value; getS(t, m, "k") != "v" {
		t.Errorf("M = %v", m)
	}
	if len(got) != len(item) {
		t.Errorf("got %d attributes, want %d", len(got), len(item))
	}

	missing, err := h.client.GetItem(t.Context(), &sdk.GetItemInput{TableName: aws.String("orders"), Key: orderKey("c", "none")})
	if err != nil || missing.Item != nil {
		t.Errorf("GetItem of a missing item = %v, %v, want no item", missing.Item, err)
	}
}

func TestSDKPutGetDeleteReturnValues(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	ctx := t.Context()
	h.put(t, "orders", order("c", "1", "10", "open"))
	put, err := h.client.PutItem(ctx, &sdk.PutItemInput{
		TableName: aws.String("orders"), Item: order("c", "1", "20", "open"), ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil || getN(t, put.Attributes, "total") != "10" {
		t.Fatalf("PutItem ALL_OLD = %v, %v, want the replaced item", put.Attributes, err)
	}
	get, err := h.client.GetItem(ctx, &sdk.GetItemInput{
		TableName: aws.String("orders"), Key: orderKey("c", "1"),
		ProjectionExpression:     aws.String("#t, note"),
		ExpressionAttributeNames: map[string]string{"#t": "total"},
	})
	if err != nil || len(get.Item) != 2 || getN(t, get.Item, "total") != "20" {
		t.Fatalf("GetItem with projection = %v, %v", get.Item, err)
	}
	del, err := h.client.DeleteItem(ctx, &sdk.DeleteItemInput{
		TableName: aws.String("orders"), Key: orderKey("c", "1"), ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil || getS(t, del.Attributes, "status") != "open" {
		t.Fatalf("DeleteItem ALL_OLD = %v, %v", del.Attributes, err)
	}
	del, err = h.client.DeleteItem(ctx, &sdk.DeleteItemInput{
		TableName: aws.String("orders"), Key: orderKey("c", "1"), ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil || del.Attributes != nil {
		t.Fatalf("deleting a missing item = %v, %v, want no attributes", del.Attributes, err)
	}
	_, err = h.client.PutItem(ctx, &sdk.PutItemInput{
		TableName: aws.String("orders"), Item: order("c", "1", "", ""), ReturnValues: types.ReturnValueAllNew,
	})
	wantAPIError(t, err, "ValidationException", "Return values set to invalid value")
}

func TestSDKUpdateItem(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	ctx := t.Context()
	update := func(expr string, rv types.ReturnValue, vals map[string]types.AttributeValue) (*sdk.UpdateItemOutput, error) {
		var names map[string]string
		if strings.Contains(expr, "#s") {
			names = map[string]string{"#s": "status"}
		}
		return h.client.UpdateItem(ctx, &sdk.UpdateItemInput{
			TableName: aws.String("orders"), Key: orderKey("c", "1"),
			UpdateExpression: aws.String(expr), ReturnValues: rv, ExpressionAttributeValues: vals,
			ExpressionAttributeNames: names,
		})
	}
	out, err := update("SET #s = :s, total = if_not_exists(total, :zero) + :inc, tags = :tags",
		types.ReturnValueAllNew, map[string]types.AttributeValue{
			":s": str("open"), ":zero": num("0"), ":inc": num("2.5"),
			":tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{str("a")}},
		})
	if err != nil {
		t.Fatalf("UpdateItem creating the item: %v", err)
	}
	if getN(t, out.Attributes, "total") != "2.5" || getS(t, out.Attributes, "customer") != "c" {
		t.Errorf("ALL_NEW = %v", out.Attributes)
	}
	out, err = update("SET tags = list_append(tags, :more), total = total - :one ADD seen :set REMOVE #s",
		types.ReturnValueUpdatedOld, map[string]types.AttributeValue{
			":more": &types.AttributeValueMemberL{Value: []types.AttributeValue{str("b")}},
			":one":  num("1"), ":set": &types.AttributeValueMemberSS{Value: []string{"x"}},
		})
	if err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}
	if len(out.Attributes) != 3 || getN(t, out.Attributes, "total") != "2.5" || getS(t, out.Attributes, "status") != "open" {
		t.Errorf("UPDATED_OLD = %v, want the old tags, total and status", out.Attributes)
	}
	out, err = update("SET tags[0] = :z", types.ReturnValueUpdatedNew, map[string]types.AttributeValue{":z": str("z")})
	if err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}
	tags := out.Attributes["tags"].(*types.AttributeValueMemberL).Value
	if len(out.Attributes) != 1 || len(tags) != 1 || getS(t, map[string]types.AttributeValue{"v": tags[0]}, "v") != "z" {
		t.Errorf("UPDATED_NEW = %v, want only tags[0]", out.Attributes)
	}

	_, err = update("SET customer = :s", types.ReturnValueNone, map[string]types.AttributeValue{":s": str("x")})
	wantAPIError(t, err, "ValidationException", "Cannot update attribute customer. This attribute is part of the key")
	_, err = update("SET a = :s", types.ReturnValueNone, map[string]types.AttributeValue{":s": str("x"), ":extra": str("y")})
	wantAPIError(t, err, "ValidationException", "ExpressionAttributeValues unused in expressions: keys: {:extra}")
	_, err = update("SET #s = :s, total = :s", types.ReturnValueNone, map[string]types.AttributeValue{":s": str("x")})
	wantAPIError(t, err, "ValidationException", "Type mismatch for Index Key total Expected: N Actual: S")
	_, err = update("SET #s = #s + :s", types.ReturnValueNone, map[string]types.AttributeValue{":s": num("1")})
	wantAPIError(t, err, "ValidationException", "refers to an attribute that does not exist in the item")
}

func TestSDKConditionalWrites(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	ctx := t.Context()
	h.put(t, "orders", order("c", "1", "10", "open"))

	_, err := h.client.PutItem(ctx, &sdk.PutItemInput{
		TableName: aws.String("orders"), Item: order("c", "1", "99", "open"),
		ConditionExpression:                 aws.String("attribute_not_exists(customer)"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	var failed *types.ConditionalCheckFailedException
	if !errors.As(err, &failed) {
		t.Fatalf("PutItem over an existing item: %v, want ConditionalCheckFailedException", err)
	}
	if getN(t, failed.Item, "total") != "10" {
		t.Errorf("the exception's Item = %v, want the item as it was", failed.Item)
	}

	_, err = h.client.UpdateItem(ctx, &sdk.UpdateItemInput{
		TableName: aws.String("orders"), Key: orderKey("c", "1"),
		UpdateExpression:          aws.String("SET total = total + :one"),
		ConditionExpression:       aws.String("#s = :closed"),
		ExpressionAttributeNames:  map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": num("1"), ":closed": str("closed")},
	})
	if !errors.As(err, &failed) || failed.Item != nil {
		t.Fatalf("UpdateItem with a false condition: %v (Item %v), want the exception without Item", err, failed)
	}

	_, err = h.client.DeleteItem(ctx, &sdk.DeleteItemInput{
		TableName: aws.String("orders"), Key: orderKey("c", "1"),
		ConditionExpression:       aws.String("total BETWEEN :lo AND :hi AND begins_with(note, :n)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":lo": num("5"), ":hi": num("15"), ":n": str("n-")},
	})
	if err != nil {
		t.Fatalf("DeleteItem with a true condition: %v", err)
	}
}

func TestSDKKeyAndItemValidation(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	ctx := t.Context()
	tests := []struct {
		name string
		item map[string]types.AttributeValue
		want string
	}{
		{"missing sort key", map[string]types.AttributeValue{"customer": str("c")}, "Missing the key order in the item"},
		{"wrong key type", map[string]types.AttributeValue{"customer": num("1"), "order": str("o")},
			"Type mismatch for key customer expected: S actual: N"},
		{"empty key", map[string]types.AttributeValue{"customer": str(""), "order": str("o")},
			"cannot contain an empty string value. Key: customer"},
		{"too big", map[string]types.AttributeValue{"customer": str("c"), "order": str("o"),
			"blob": str(strings.Repeat("x", 400*1024))}, "Item size has exceeded the maximum allowed size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := h.client.PutItem(ctx, &sdk.PutItemInput{TableName: aws.String("orders"), Item: tt.item})
			wantAPIError(t, err, "ValidationException", tt.want)
		})
	}
	_, err := h.client.GetItem(ctx, &sdk.GetItemInput{
		TableName: aws.String("orders"), Key: map[string]types.AttributeValue{"customer": str("c")},
	})
	wantAPIError(t, err, "ValidationException", "The provided key element does not match the schema")
	_, err = h.client.GetItem(ctx, &sdk.GetItemInput{TableName: aws.String("nope"), Key: orderKey("c", "1")})
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		t.Errorf("GetItem on a missing table: %v, want ResourceNotFoundException", err)
	}
	_, err = h.client.PutItem(ctx, &sdk.PutItemInput{
		TableName: aws.String("orders"), Item: order("c", "1", "", ""),
		ConditionExpression: aws.String("attribute_exists(#x) AND"),
	})
	wantAPIError(t, err, "ValidationException", "Invalid ConditionExpression: Syntax error")
}
