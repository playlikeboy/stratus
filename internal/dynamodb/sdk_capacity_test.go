package dynamodb

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSDKConsumedCapacitySingleItem(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	ctx := t.Context()

	// Empty ReturnConsumedCapacity / NONE omits ConsumedCapacity.
	putNone, err := h.client.PutItem(ctx, &sdk.PutItemInput{
		TableName:              aws.String("orders"),
		Item:                   order("c", "1", "10", "open"),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityNone,
	})
	if err != nil {
		t.Fatalf("PutItem NONE: %v", err)
	}
	if putNone.ConsumedCapacity != nil {
		t.Fatalf("NONE ConsumedCapacity = %+v, want nil", putNone.ConsumedCapacity)
	}

	// Put >1 KB item with INDEXES updates base table, LSI (by-total), and GSI (by-status).
	bigItem := order("c", "big", "99", "open")
	bigItem["payload"] = str(strings.Repeat("x", 1500))
	putIdx, err := h.client.PutItem(ctx, &sdk.PutItemInput{
		TableName:              aws.String("orders"),
		Item:                   bigItem,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
	})
	if err != nil {
		t.Fatalf("PutItem INDEXES: %v", err)
	}
	cc := putIdx.ConsumedCapacity
	if cc == nil || aws.ToString(cc.TableName) != "orders" {
		t.Fatalf("ConsumedCapacity = %+v", cc)
	}
	if got := aws.ToFloat64(cc.Table.WriteCapacityUnits); got != 2 {
		t.Fatalf("Table WCU = %v, want 2", got)
	}
	if got := aws.ToFloat64(cc.LocalSecondaryIndexes["by-total"].WriteCapacityUnits); got != 1 {
		t.Fatalf("LSI WCU = %v, want 1", got)
	}
	if got := aws.ToFloat64(cc.GlobalSecondaryIndexes["by-status"].WriteCapacityUnits); got != 1 {
		t.Fatalf("GSI WCU = %v, want 1", got)
	}
	if got := aws.ToFloat64(cc.WriteCapacityUnits); got != 4 {
		t.Fatalf("total WCU = %v, want 4", got)
	}

	// GetItem: eventually consistent (0.5 RCU/4KB) vs strongly consistent (1.0 RCU/4KB),
	// and >4 KB charges multiple 4 KB blocks even when projected down.
	largeItem := order("c", "read-big", "10", "open")
	largeItem["blob"] = str(strings.Repeat("a", 5000))
	h.put(t, "orders", largeItem)

	evRead, err := h.client.GetItem(ctx, &sdk.GetItemInput{
		TableName:              aws.String("orders"),
		Key:                    orderKey("c", "read-big"),
		ProjectionExpression:   aws.String("customer"),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("GetItem TOTAL: %v", err)
	}
	if got := aws.ToFloat64(evRead.ConsumedCapacity.ReadCapacityUnits); got != 1.0 {
		t.Fatalf("eventually consistent >4KB RCU = %v, want 1.0", got)
	}
	if evRead.ConsumedCapacity.Table != nil {
		t.Fatalf("TOTAL must omit Table breakdown, got %+v", evRead.ConsumedCapacity.Table)
	}

	strongRead, err := h.client.GetItem(ctx, &sdk.GetItemInput{
		TableName:              aws.String("orders"),
		Key:                    orderKey("c", "read-big"),
		ConsistentRead:         aws.Bool(true),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("GetItem ConsistentRead: %v", err)
	}
	if got := aws.ToFloat64(strongRead.ConsumedCapacity.ReadCapacityUnits); got != 2.0 {
		t.Fatalf("strongly consistent >4KB RCU = %v, want 2.0", got)
	}

	// Missing item still reports 0.5 / 1.0 minimum RCU.
	miss, err := h.client.GetItem(ctx, &sdk.GetItemInput{
		TableName:              aws.String("orders"),
		Key:                    orderKey("c", "missing"),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("GetItem missing: %v", err)
	}
	if got := aws.ToFloat64(miss.ConsumedCapacity.CapacityUnits); got != 0.5 {
		t.Fatalf("missing GetItem RCU = %v, want 0.5", got)
	}

	// UpdateItem changing unindexed attribute skips unchanged index write units.
	upd, err := h.client.UpdateItem(ctx, &sdk.UpdateItemInput{
		TableName:                 aws.String("orders"),
		Key:                       orderKey("c", "1"),
		UpdateExpression:          aws.String("SET note = :n"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":n": str("updated")},
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
	})
	if err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}
	if got := aws.ToFloat64(upd.ConsumedCapacity.WriteCapacityUnits); got != 1 {
		t.Fatalf("UpdateItem unindexed WCU = %v, want 1", got)
	}
	if len(upd.ConsumedCapacity.LocalSecondaryIndexes) != 0 || len(upd.ConsumedCapacity.GlobalSecondaryIndexes) != 0 {
		t.Fatalf("expected 0 index capacity when only non-projected attribute changed, got LSI=%+v GSI=%+v",
			upd.ConsumedCapacity.LocalSecondaryIndexes, upd.ConsumedCapacity.GlobalSecondaryIndexes)
	}

	del, err := h.client.DeleteItem(ctx, &sdk.DeleteItemInput{
		TableName:              aws.String("orders"),
		Key:                    orderKey("c", "1"),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("DeleteItem: %v", err)
	}
	if got := aws.ToFloat64(del.ConsumedCapacity.WriteCapacityUnits); got != 3 {
		t.Fatalf("DeleteItem WCU = %v, want 3 (table + LSI + GSI)", got)
	}

	delMiss, err := h.client.DeleteItem(ctx, &sdk.DeleteItemInput{
		TableName:              aws.String("orders"),
		Key:                    orderKey("c", "missing"),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("DeleteItem missing: %v", err)
	}
	if got := aws.ToFloat64(delMiss.ConsumedCapacity.WriteCapacityUnits); got != 1 {
		t.Fatalf("DeleteItem missing WCU = %v, want 1", got)
	}
}

func TestSDKConsumedCapacityQueryScanBatchAndTransact(t *testing.T) {
	h := start(t)
	h.createOrders(t)
	if err := h.svc.CreateTable(TableSpec{
		Name:         "extra",
		PartitionKey: KeyAttribute{Name: "id", Type: TypeString},
	}); err != nil {
		t.Fatalf("CreateTable extra: %v", err)
	}
	ctx := t.Context()

	// BatchWriteItem across two tables.
	bw, err := h.client.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"orders": {
				{PutRequest: &types.PutRequest{Item: order("c", "1", "10", "open")}},
				{PutRequest: &types.PutRequest{Item: order("c", "2", "20", "closed")}},
			},
			"extra": {
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"id": str("e1")}}},
			},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}
	if len(bw.ConsumedCapacity) != 2 {
		t.Fatalf("BatchWriteItem ConsumedCapacity len = %d, want 2", len(bw.ConsumedCapacity))
	}
	if aws.ToString(bw.ConsumedCapacity[0].TableName) != "extra" || aws.ToFloat64(bw.ConsumedCapacity[0].WriteCapacityUnits) != 1 {
		t.Fatalf("extra WCU = %+v", bw.ConsumedCapacity[0])
	}
	if aws.ToString(bw.ConsumedCapacity[1].TableName) != "orders" || aws.ToFloat64(bw.ConsumedCapacity[1].WriteCapacityUnits) != 6 {
		t.Fatalf("orders WCU = %+v, want 6", bw.ConsumedCapacity[1])
	}

	// Query GSI with INDEXES attributes capacity to GlobalSecondaryIndexes.
	qGSI, err := h.client.Query(ctx, &sdk.QueryInput{
		TableName:                 aws.String("orders"),
		IndexName:                 aws.String("by-status"),
		KeyConditionExpression:    aws.String("#s = :open"),
		ExpressionAttributeNames:  map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":open": str("open")},
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
	})
	if err != nil {
		t.Fatalf("Query GSI: %v", err)
	}
	if got := aws.ToFloat64(qGSI.ConsumedCapacity.GlobalSecondaryIndexes["by-status"].ReadCapacityUnits); got != 0.5 {
		t.Fatalf("GSI Query RCU = %v, want 0.5", got)
	}
	if got := aws.ToFloat64(qGSI.ConsumedCapacity.Table.ReadCapacityUnits); got != 0 {
		t.Fatalf("GSI Query Table RCU = %v, want 0", got)
	}

	// Scan base table with empty result still consumes min 0.5 RCU (eventually consistent).
	scanRes, err := h.client.Scan(ctx, &sdk.ScanInput{
		TableName:                 aws.String("orders"),
		FilterExpression:          aws.String("#s = :none"),
		ExpressionAttributeNames:  map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":none": str("missing")},
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got := aws.ToFloat64(scanRes.ConsumedCapacity.ReadCapacityUnits); got != 0.5 {
		t.Fatalf("Scan RCU = %v, want 0.5", got)
	}

	// BatchGetItem charges 1.0 per key with ConsistentRead=true even for missing keys.
	bg, err := h.client.BatchGetItem(ctx, &sdk.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"orders": {
				Keys:           []map[string]types.AttributeValue{orderKey("c", "1"), orderKey("c", "missing")},
				ConsistentRead: aws.Bool(true),
			},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("BatchGetItem: %v", err)
	}
	if len(bg.ConsumedCapacity) != 1 || aws.ToFloat64(bg.ConsumedCapacity[0].ReadCapacityUnits) != 2.0 {
		t.Fatalf("BatchGetItem ConsumedCapacity = %+v, want 2.0 RCU", bg.ConsumedCapacity)
	}

	// TransactWriteItems doubles write units (2x per table item + indexes).
	tw, err := h.client.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{
				TableName: aws.String("extra"),
				Item:      map[string]types.AttributeValue{"id": str("e2")},
			}},
			{ConditionCheck: &types.ConditionCheck{
				TableName:           aws.String("orders"),
				Key:                 orderKey("c", "1"),
				ConditionExpression: aws.String("attribute_exists(customer)"),
			}},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("TransactWriteItems: %v", err)
	}
	if len(tw.ConsumedCapacity) != 2 ||
		aws.ToFloat64(tw.ConsumedCapacity[0].WriteCapacityUnits) != 2 ||
		aws.ToFloat64(tw.ConsumedCapacity[1].WriteCapacityUnits) != 2 {
		t.Fatalf("TransactWriteItems ConsumedCapacity = %+v, want 2 WCU each", tw.ConsumedCapacity)
	}

	// TransactGetItems doubles strongly consistent reads (2.0 RCU per item).
	tg, err := h.client.TransactGetItems(ctx, &sdk.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{TableName: aws.String("extra"), Key: map[string]types.AttributeValue{"id": str("e1")}}},
			{Get: &types.Get{TableName: aws.String("orders"), Key: orderKey("c", "1")}},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatalf("TransactGetItems: %v", err)
	}
	if len(tg.ConsumedCapacity) != 2 ||
		aws.ToFloat64(tg.ConsumedCapacity[0].ReadCapacityUnits) != 2 ||
		aws.ToFloat64(tg.ConsumedCapacity[1].ReadCapacityUnits) != 2 {
		t.Fatalf("TransactGetItems ConsumedCapacity = %+v, want 2 RCU each", tg.ConsumedCapacity)
	}

	// Invalid ReturnConsumedCapacity returns ValidationException.
	_, err = h.client.GetItem(ctx, &sdk.GetItemInput{
		TableName:              aws.String("extra"),
		Key:                    map[string]types.AttributeValue{"id": str("e1")},
		ReturnConsumedCapacity: types.ReturnConsumedCapacity("BOGUS"),
	})
	wantAPIError(t, err, "ValidationException", "returnConsumedCapacity")
}
