package dynamodb

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/gateway"
)

var testStart = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// harness is a service behind the gateway on a test server, with an SDK
// client pointed at it.
type harness struct {
	svc    *Service
	clock  *clock.Fake
	url    string
	client *sdk.Client
}

func start(t *testing.T) *harness {
	t.Helper()
	clk := clock.NewFake(testStart)
	svc := New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clk)
	srv := httptest.NewServer(gateway.New(svc))
	t.Cleanup(srv.Close)
	client := sdk.New(sdk.Options{
		Region:           awsapi.DefaultRegion,
		BaseEndpoint:     aws.String(srv.URL),
		RetryMaxAttempts: 1,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})
	return &harness{svc: svc, clock: clk, url: srv.URL, client: client}
}

// ordersSpec is the table most tests use: customer/order keys, a local index
// on total that also holds status, and a global index on status.
func ordersSpec() TableSpec {
	total := &KeyAttribute{Name: "total", Type: TypeNumber}
	return TableSpec{
		Name:         "orders",
		PartitionKey: KeyAttribute{Name: "customer", Type: TypeString},
		SortKey:      &KeyAttribute{Name: "order", Type: TypeString},
		LocalIndexes: []IndexSpec{{
			Name:       "by-total",
			SortKey:    total,
			Projection: Projection{Type: ProjectInclude, NonKeyAttributes: []string{"status"}},
		}},
		GlobalIndexes: []IndexSpec{{
			Name:         "by-status",
			PartitionKey: KeyAttribute{Name: "status", Type: TypeString},
			SortKey:      total,
			Projection:   Projection{Type: ProjectKeysOnly},
		}},
		TTLAttribute: "expires",
	}
}

func (h *harness) createOrders(t *testing.T) {
	t.Helper()
	if err := h.svc.CreateTable(ordersSpec()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
}

func (h *harness) put(t *testing.T, table string, item map[string]types.AttributeValue) {
	t.Helper()
	if _, err := h.client.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String(table), Item: item}); err != nil {
		t.Fatalf("PutItem: %v", err)
	}
}

func str(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }
func num(n string) types.AttributeValue { return &types.AttributeValueMemberN{Value: n} }

func order(customer, id, total, status string) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{"customer": str(customer), "order": str(id), "note": str("n-" + id)}
	if total != "" {
		item["total"] = num(total)
	}
	if status != "" {
		item["status"] = str(status)
	}
	return item
}

func orderKey(customer, id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"customer": str(customer), "order": str(id)}
}

func getS(t *testing.T, item map[string]types.AttributeValue, name string) string {
	t.Helper()
	v, ok := item[name].(*types.AttributeValueMemberS)
	if !ok {
		t.Fatalf("attribute %s is %T, want S", name, item[name])
	}
	return v.Value
}

func getN(t *testing.T, item map[string]types.AttributeValue, name string) string {
	t.Helper()
	v, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("attribute %s is %T, want N", name, item[name])
	}
	return v.Value
}

// wantAPIError asserts err is an API error of the given code whose message
// contains msg.
func wantAPIError(t *testing.T, err error, code, msg string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %v, want %s", err, code)
	}
	if apiErr.ErrorCode() != code || !strings.Contains(apiErr.ErrorMessage(), msg) {
		t.Fatalf("error = %s: %s, want %s containing %q", apiErr.ErrorCode(), apiErr.ErrorMessage(), code, msg)
	}
}
