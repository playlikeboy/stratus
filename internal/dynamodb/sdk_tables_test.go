package dynamodb

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestSDKTableLifecycle(t *testing.T) {
	h := start(t)
	ctx := t.Context()
	created, err := h.client.CreateTable(ctx, &sdk.CreateTableInput{
		TableName:   aws.String("events"),
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5),
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("kind"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName: aws.String("by-kind"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("kind"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if created.TableDescription.TableStatus != types.TableStatusActive {
		t.Errorf("status = %s, want ACTIVE at once", created.TableDescription.TableStatus)
	}

	h.put(t, "events", map[string]types.AttributeValue{"pk": str("a"), "sk": num("1"), "data": str("xyz")})
	desc, err := h.client.DescribeTable(ctx, &sdk.DescribeTableInput{TableName: aws.String("events")})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	tbl := desc.Table
	if aws.ToInt64(tbl.ItemCount) != 1 || aws.ToInt64(tbl.TableSizeBytes) == 0 {
		t.Errorf("ItemCount %d, TableSizeBytes %d, want 1 and > 0", aws.ToInt64(tbl.ItemCount), aws.ToInt64(tbl.TableSizeBytes))
	}
	if want := "arn:aws:dynamodb:eu-central-1:000000000000:table/events"; aws.ToString(tbl.TableArn) != want {
		t.Errorf("TableArn = %s, want %s", aws.ToString(tbl.TableArn), want)
	}
	if len(tbl.KeySchema) != 2 || len(tbl.AttributeDefinitions) != 3 || len(tbl.LocalSecondaryIndexes) != 1 {
		t.Errorf("description = %d keys, %d definitions, %d local indexes, want 2, 3, 1",
			len(tbl.KeySchema), len(tbl.AttributeDefinitions), len(tbl.LocalSecondaryIndexes))
	}
	if tbl.BillingModeSummary == nil || tbl.BillingModeSummary.BillingMode != types.BillingModeProvisioned {
		t.Errorf("BillingModeSummary = %+v, want PROVISIONED", tbl.BillingModeSummary)
	}
	if lsi := tbl.LocalSecondaryIndexes[0]; lsi.Projection.ProjectionType != types.ProjectionTypeKeysOnly ||
		aws.ToInt64(lsi.ItemCount) != 0 {
		t.Errorf("local index = %+v, want KEYS_ONLY with no items (the item has no kind)", lsi)
	}
	if !tbl.CreationDateTime.Equal(testStart) {
		t.Errorf("CreationDateTime = %v, want %v", tbl.CreationDateTime, testStart)
	}

	_, err = h.client.CreateTable(ctx, &sdk.CreateTableInput{
		TableName:            aws.String("events"),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	var inUse *types.ResourceInUseException
	if !errors.As(err, &inUse) {
		t.Errorf("creating an existing table: %v, want ResourceInUseException", err)
	}

	deleted, err := h.client.DeleteTable(ctx, &sdk.DeleteTableInput{TableName: aws.String("events")})
	if err != nil || deleted.TableDescription.TableStatus != types.TableStatusDeleting {
		t.Fatalf("DeleteTable = %v, want a DELETING description", err)
	}
	_, err = h.client.DescribeTable(ctx, &sdk.DescribeTableInput{TableName: aws.String("events")})
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		t.Errorf("describing a deleted table: %v, want ResourceNotFoundException", err)
	}
}

func TestSDKCreateTableValidation(t *testing.T) {
	h := start(t)
	defs := []types.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("unused"), AttributeType: types.ScalarAttributeTypeS},
	}
	_, err := h.client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName:            aws.String("bad"),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: defs,
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	wantAPIError(t, err, "ValidationException", "Some AttributeDefinitions are not used")

	_, err = h.client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName:            aws.String("bad"),
		AttributeDefinitions: defs[:1],
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	wantAPIError(t, err, "ValidationException", "must both be specified when BillingMode is PROVISIONED")

	err = h.svc.CreateTable(TableSpec{
		Name:         "bad",
		PartitionKey: KeyAttribute{Name: "pk", Type: TypeString},
		LocalIndexes: []IndexSpec{{Name: "lsi", SortKey: &KeyAttribute{Name: "x", Type: TypeString}}},
	})
	if err == nil || !strings.Contains(err.Error(), "does not have a range key") {
		t.Errorf("a local index on a table without a sort key: %v", err)
	}
}

func TestSDKListTablesPages(t *testing.T) {
	h := start(t)
	for i := range 5 {
		spec := TableSpec{Name: fmt.Sprintf("table-%d", i), PartitionKey: KeyAttribute{Name: "pk", Type: TypeString}}
		if err := h.svc.CreateTable(spec); err != nil {
			t.Fatal(err)
		}
	}
	var names []string
	in := &sdk.ListTablesInput{Limit: aws.Int32(2)}
	pages := 0
	for p := sdk.NewListTablesPaginator(h.client, in); p.HasMorePages(); pages++ {
		out, err := p.NextPage(t.Context())
		if err != nil {
			t.Fatalf("ListTables: %v", err)
		}
		names = append(names, out.TableNames...)
	}
	if len(names) != 5 || names[0] != "table-0" || names[4] != "table-4" || pages != 3 {
		t.Errorf("listed %v in %d pages, want 5 sorted names in 3 pages", names, pages)
	}
}

func TestSDKTimeToLive(t *testing.T) {
	h := start(t)
	ctx := t.Context()
	spec := ordersSpec()
	spec.TTLAttribute = ""
	if err := h.svc.CreateTable(spec); err != nil {
		t.Fatal(err)
	}
	desc, err := h.client.DescribeTimeToLive(ctx, &sdk.DescribeTimeToLiveInput{TableName: aws.String("orders")})
	if err != nil || desc.TimeToLiveDescription.TimeToLiveStatus != types.TimeToLiveStatusDisabled {
		t.Fatalf("DescribeTimeToLive = %+v, %v, want DISABLED", desc, err)
	}
	_, err = h.client.UpdateTimeToLive(ctx, &sdk.UpdateTimeToLiveInput{
		TableName:               aws.String("orders"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("expires"), Enabled: aws.Bool(true)},
	})
	if err != nil {
		t.Fatalf("UpdateTimeToLive: %v", err)
	}
	desc, err = h.client.DescribeTimeToLive(ctx, &sdk.DescribeTimeToLiveInput{TableName: aws.String("orders")})
	if err != nil || desc.TimeToLiveDescription.TimeToLiveStatus != types.TimeToLiveStatusEnabled ||
		aws.ToString(desc.TimeToLiveDescription.AttributeName) != "expires" {
		t.Fatalf("DescribeTimeToLive = %+v, %v, want ENABLED on expires", desc.TimeToLiveDescription, err)
	}

	now := testStart.Unix()
	past, future := fmt.Sprint(now-60), fmt.Sprint(now+60)
	ancient := fmt.Sprint(now - 6*365*24*3600)
	for id, exp := range map[string]types.AttributeValue{"1": num(past), "2": num(future), "3": num(ancient), "4": str(past)} {
		item := order("c", id, "", "")
		item["expires"] = exp
		h.put(t, "orders", item)
	}
	if n := h.svc.ExpireTTL(); n != 1 {
		t.Fatalf("ExpireTTL = %d, want 1 (past only; not future, too old or not a number)", n)
	}
	h.clock.Advance(2 * time.Minute)
	if n := h.svc.ExpireTTL(); n != 1 {
		t.Fatalf("ExpireTTL after the clock moved = %d, want 1", n)
	}
	scan, err := h.client.Scan(ctx, &sdk.ScanInput{TableName: aws.String("orders")})
	if err != nil || scan.Count != 2 {
		t.Fatalf("Scan after expiry = %v items, %v, want the 2 items TTL ignores", scan.Count, err)
	}
}
