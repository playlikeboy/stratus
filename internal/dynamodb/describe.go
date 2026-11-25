package dynamodb

import (
	"slices"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
)

// The wire shapes of table descriptions, shared by requests and responses.

type keySchemaElement struct {
	AttributeName string
	KeyType       string
}

type attributeDefinition struct {
	AttributeName string
	AttributeType string
}

type projectionWire struct {
	ProjectionType   string
	NonKeyAttributes []string `json:",omitempty"`
}

type throughputWire struct {
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

type throughputDescription struct {
	ReadCapacityUnits      int64
	WriteCapacityUnits     int64
	NumberOfDecreasesToday int64
}

type indexDescription struct {
	IndexName             string
	IndexArn              string
	IndexStatus           string `json:",omitempty"` // global indexes only
	KeySchema             []keySchemaElement
	Projection            projectionWire
	IndexSizeBytes        int64
	ItemCount             int64
	ProvisionedThroughput *throughputDescription `json:",omitempty"`
}

type billingModeSummary struct {
	BillingMode                       string
	LastUpdateToPayPerRequestDateTime float64 `json:",omitempty"`
}

type tableDescription struct {
	TableName                 string
	TableStatus               string
	TableArn                  string
	TableId                   string //nolint:revive // the wire name
	CreationDateTime          float64
	KeySchema                 []keySchemaElement
	AttributeDefinitions      []attributeDefinition
	ItemCount                 int64
	TableSizeBytes            int64
	BillingModeSummary        billingModeSummary
	ProvisionedThroughput     throughputDescription
	LocalSecondaryIndexes     []indexDescription `json:",omitempty"`
	GlobalSecondaryIndexes    []indexDescription `json:",omitempty"`
	DeletionProtectionEnabled bool
}

func keySchemaOf(pk KeyAttribute, sk *KeyAttribute) []keySchemaElement {
	ks := []keySchemaElement{{AttributeName: pk.Name, KeyType: "HASH"}}
	if sk != nil {
		ks = append(ks, keySchemaElement{AttributeName: sk.Name, KeyType: "RANGE"})
	}
	return ks
}

// describe returns the table's description with the given status.
func (t *table) describe(status string) tableDescription {
	created := float64(t.createdAt.UnixMilli()) / 1000
	d := tableDescription{
		TableName:          t.spec.Name,
		TableStatus:        status,
		TableArn:           t.arn,
		TableId:            t.id,
		CreationDateTime:   created,
		KeySchema:          keySchemaOf(t.spec.PartitionKey, t.spec.SortKey),
		ItemCount:          int64(t.count),
		TableSizeBytes:     t.sizeBytes(),
		BillingModeSummary: billingModeSummary{BillingMode: t.spec.BillingMode},
		ProvisionedThroughput: throughputDescription{
			ReadCapacityUnits: t.spec.ReadCapacity, WriteCapacityUnits: t.spec.WriteCapacity,
		},
	}
	if t.spec.BillingMode == PayPerRequest {
		d.BillingModeSummary.LastUpdateToPayPerRequestDateTime = created
	}
	var seen []string
	for _, k := range t.spec.keyAttributes() {
		if !slices.Contains(seen, k.Name) {
			seen = append(seen, k.Name)
			d.AttributeDefinitions = append(d.AttributeDefinitions,
				attributeDefinition{AttributeName: k.Name, AttributeType: k.Type})
		}
	}
	for i := range t.spec.allIndexes() {
		spec := t.spec.indexAt(i)
		desc := t.describeIndex(spec)
		if i < len(t.spec.LocalIndexes) {
			d.LocalSecondaryIndexes = append(d.LocalSecondaryIndexes, desc)
			continue
		}
		desc.IndexStatus = "ACTIVE"
		desc.ProvisionedThroughput = &throughputDescription{
			ReadCapacityUnits: t.spec.ReadCapacity, WriteCapacityUnits: t.spec.WriteCapacity,
		}
		d.GlobalSecondaryIndexes = append(d.GlobalSecondaryIndexes, desc)
	}
	return d
}

func (t *table) describeIndex(spec *IndexSpec) indexDescription {
	ix := t.index(spec.Name)
	desc := indexDescription{
		IndexName: spec.Name,
		IndexArn:  t.arn + "/index/" + spec.Name,
		KeySchema: keySchemaOf(spec.PartitionKey, spec.SortKey),
		Projection: projectionWire{
			ProjectionType: spec.Projection.Type, NonKeyAttributes: spec.Projection.NonKeyAttributes,
		},
	}
	for _, item := range t.all() {
		if t.inIndex(ix, item) {
			desc.ItemCount++
			desc.IndexSizeBytes += int64(attr.ItemSize(t.projectIndex(ix, item)))
		}
	}
	return desc
}
