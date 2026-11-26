package dynamodb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// seedOrders stores ten orders of customer c (o00..o09, total 0..9, status
// open for even totals) and one of customer d.
func seedOrders(t *testing.T, h *harness) {
	t.Helper()
	h.createOrders(t)
	for i := range 10 {
		status := "closed"
		if i%2 == 0 {
			status = "open"
		}
		h.put(t, "orders", order("c", fmt.Sprintf("o%02d", i), fmt.Sprint(i), status))
	}
	h.put(t, "orders", order("d", "o00", "100", "open"))
}

func orderIDs(t *testing.T, items []map[string]types.AttributeValue) string {
	t.Helper()
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = getS(t, item, "order")
	}
	return strings.Join(ids, ",")
}

func TestSDKQueryPaginationAndOrder(t *testing.T) {
	h := start(t)
	seedOrders(t, h)
	in := &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		KeyConditionExpression:    aws.String("customer = :c AND #o BETWEEN :from AND :to"),
		ExpressionAttributeNames:  map[string]string{"#o": "order"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c"), ":from": str("o02"), ":to": str("o08")},
		ScanIndexForward:          aws.Bool(false),
		Limit:                     aws.Int32(3),
	}
	var pages []string
	for p := sdk.NewQueryPaginator(h.client, in); p.HasMorePages(); {
		out, err := p.NextPage(t.Context())
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		pages = append(pages, orderIDs(t, out.Items))
	}
	// Limit 3 over 7 items: 3, 3, 1; descending.
	if got := strings.Join(pages, " | "); got != "o08,o07,o06 | o05,o04,o03 | o02" {
		t.Errorf("pages = %s", got)
	}

	out, err := h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		KeyConditionExpression:    aws.String("customer = :c AND begins_with(#o, :p)"),
		FilterExpression:          aws.String("#s = :open"),
		ExpressionAttributeNames:  map[string]string{"#o": "order", "#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c"), ":p": str("o0"), ":open": str("open")},
		Limit:                     aws.Int32(4),
	})
	if err != nil {
		t.Fatalf("Query with a filter: %v", err)
	}
	// Limit applies before the filter: 4 read, 2 of them open.
	if out.Count != 2 || out.ScannedCount != 4 || out.LastEvaluatedKey == nil {
		t.Errorf("Count %d ScannedCount %d LastEvaluatedKey %v, want 2, 4 and a key", out.Count, out.ScannedCount, out.LastEvaluatedKey)
	}

	count, err := h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		KeyConditionExpression:    aws.String("customer = :c"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c")},
		Select:                    types.SelectCount,
	})
	if err != nil || count.Count != 10 || count.Items != nil {
		t.Errorf("Select COUNT = %d items %v, %v, want a count of 10 and no items", count.Count, count.Items, err)
	}

	_, err = h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		KeyConditionExpression:    aws.String("customer = :c"),
		FilterExpression:          aws.String("#o = :c"),
		ExpressionAttributeNames:  map[string]string{"#o": "order"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c")},
	})
	wantAPIError(t, err, "ValidationException", "Filter Expression can only contain non-primary key attributes")
	_, err = h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		KeyConditionExpression:    aws.String("customer = :c"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": num("1")},
	})
	wantAPIError(t, err, "ValidationException", "Condition parameter type does not match schema type")
}

func TestSDKQueryLocalIndex(t *testing.T) {
	h := start(t)
	seedOrders(t, h)
	h.put(t, "orders", order("c", "no-total", "", "open")) // absent from the sparse index
	out, err := h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		IndexName:                 aws.String("by-total"),
		KeyConditionExpression:    aws.String("customer = :c AND #t >= :min"),
		ExpressionAttributeNames:  map[string]string{"#t": "total"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c"), ":min": num("7")},
		ScanIndexForward:          aws.Bool(false),
	})
	if err != nil {
		t.Fatalf("Query on the local index: %v", err)
	}
	if got := orderIDs(t, out.Items); got != "o09,o08,o07" {
		t.Errorf("index query = %s, want o09,o08,o07", got)
	}
	// INCLUDE status: table keys, index key and status, not note.
	if first := out.Items[0]; len(first) != 4 || first["note"] != nil || getS(t, first, "status") != "closed" {
		t.Errorf("projected item = %v, want customer, order, total and status", first)
	}
	if out.LastEvaluatedKey != nil {
		t.Errorf("LastEvaluatedKey = %v on the last page", out.LastEvaluatedKey)
	}

	page, err := h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		IndexName:                 aws.String("by-total"),
		KeyConditionExpression:    aws.String("customer = :c"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c")},
		Select:                    types.SelectAllAttributes,
		Limit:                     aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("Query ALL_ATTRIBUTES on the local index: %v", err)
	}
	lek := page.LastEvaluatedKey
	if len(lek) != 3 || getN(t, lek, "total") != "1" || page.Items[0]["note"] == nil {
		t.Errorf("LastEvaluatedKey = %v, want the table keys and total; items %v with every attribute", lek, page.Items)
	}
	next, err := h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		IndexName:                 aws.String("by-total"),
		KeyConditionExpression:    aws.String("customer = :c"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c")},
		ExclusiveStartKey:         lek,
		Limit:                     aws.Int32(2),
	})
	if err != nil || orderIDs(t, next.Items) != "o02,o03" {
		t.Errorf("next page = %v, %v, want o02,o03", next.Items, err)
	}
}

func TestSDKQueryGlobalIndex(t *testing.T) {
	h := start(t)
	seedOrders(t, h)
	out, err := h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		IndexName:                 aws.String("by-status"),
		KeyConditionExpression:    aws.String("#s = :open AND #t > :n"),
		ExpressionAttributeNames:  map[string]string{"#s": "status", "#t": "total"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":open": str("open"), ":n": num("5")},
	})
	if err != nil {
		t.Fatalf("Query on the global index: %v", err)
	}
	if got := orderIDs(t, out.Items); got != "o06,o08,o00" || out.Items[2]["note"] != nil {
		t.Errorf("global index query = %s (%v), want o06,o08,o00 keys only", got, out.Items)
	}
	_, err = h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		IndexName:                 aws.String("by-status"),
		KeyConditionExpression:    aws.String("#s = :open"),
		ExpressionAttributeNames:  map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":open": str("open")},
		ConsistentRead:            aws.Bool(true),
	})
	wantAPIError(t, err, "ValidationException", "Consistent reads are not supported on global secondary indexes")
}

func TestSDKScan(t *testing.T) {
	h := start(t)
	seedOrders(t, h)
	var all []map[string]types.AttributeValue
	in := &sdk.ScanInput{
		TableName:                 aws.String("orders"),
		FilterExpression:          aws.String("#t < :five"),
		ProjectionExpression:      aws.String("customer, #o"),
		ExpressionAttributeNames:  map[string]string{"#o": "order", "#t": "total"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":five": num("5")},
		Limit:                     aws.Int32(4),
	}
	for p := sdk.NewScanPaginator(h.client, in); p.HasMorePages(); {
		out, err := p.NextPage(t.Context())
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		all = append(all, out.Items...)
	}
	if got := orderIDs(t, all); got != "o00,o01,o02,o03,o04" || len(all[0]) != 2 {
		t.Errorf("scan = %s (%v), want o00..o04 with two attributes", got, all[0])
	}

	seen := 0
	for seg := range 3 {
		out, err := h.client.Scan(t.Context(), &sdk.ScanInput{
			TableName: aws.String("orders"), Segment: aws.Int32(int32(seg)), TotalSegments: aws.Int32(3),
		})
		if err != nil {
			t.Fatalf("Scan segment %d: %v", seg, err)
		}
		seen += int(out.Count)
	}
	if seen != 11 {
		t.Errorf("the segments hold %d items together, want 11", seen)
	}
}

func TestSDKPageSizeLimit(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	big := strings.Repeat("x", 300*1024)
	for i := range 5 {
		item := order("c", fmt.Sprint(i), "", "")
		item["blob"] = str(big)
		h.put(t, "orders", item)
	}
	out, err := h.client.Query(t.Context(), &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		KeyConditionExpression:    aws.String("customer = :c"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": str("c")},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	// 300 KB items: the page stops once it has read 1 MB, after the fourth.
	if out.Count != 4 || out.LastEvaluatedKey == nil {
		t.Errorf("first page = %d items, LastEvaluatedKey %v, want 4 and a key", out.Count, out.LastEvaluatedKey)
	}
}
