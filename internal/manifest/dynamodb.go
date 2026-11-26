package manifest

import (
	"errors"
	"fmt"

	"github.com/stratus-emulator/stratus/internal/dynamodb"
)

type DynamoDB struct {
	Tables []Table `yaml:"tables"`
}

type Table struct {
	Name         string `yaml:"name"`
	PartitionKey Key    `yaml:"partitionKey"`
	SortKey      *Key   `yaml:"sortKey"`
	// TTLAttribute names the attribute that holds an item's expiry time.
	TTLAttribute  string  `yaml:"ttlAttribute"`
	LocalIndexes  []Index `yaml:"localIndexes"`
	GlobalIndexes []Index `yaml:"globalIndexes"`
}

// Key is a key attribute; Type is S (the default), N or B.
type Key struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// Index is a secondary index. A local index takes the table's partition key
// and needs only a sort key.
type Index struct {
	Name         string `yaml:"name"`
	PartitionKey *Key   `yaml:"partitionKey"`
	SortKey      *Key   `yaml:"sortKey"`
	// Projection is ALL (the default), KEYS_ONLY or INCLUDE.
	Projection       string   `yaml:"projection"`
	NonKeyAttributes []string `yaml:"nonKeyAttributes"`
}

func (d DynamoDB) validate() error {
	tables := map[string]bool{}
	for i, t := range d.Tables {
		switch {
		case t.Name == "":
			return fmt.Errorf("dynamodb.tables[%d]: name is required", i)
		case tables[t.Name]:
			return fmt.Errorf("dynamodb.tables: %q is declared twice", t.Name)
		case t.PartitionKey.Name == "":
			return fmt.Errorf("dynamodb.tables: %q needs a partitionKey", t.Name)
		}
		tables[t.Name] = true
	}
	return nil
}

func (d DynamoDB) apply(tables *dynamodb.Service) error {
	for _, t := range d.Tables {
		if err := tables.EnsureTable(t.spec()); err != nil {
			if errors.Is(err, dynamodb.ErrSpecMismatch) {
				return conflict("dynamodb table", t.Name, err)
			}
			return fmt.Errorf("dynamodb table %q: %w", t.Name, err)
		}
	}
	return nil
}

func (t Table) spec() dynamodb.TableSpec {
	spec := dynamodb.TableSpec{
		Name:         t.Name,
		PartitionKey: t.PartitionKey.attribute(),
		SortKey:      optionalKey(t.SortKey),
		TTLAttribute: t.TTLAttribute,
	}
	for _, index := range t.LocalIndexes {
		spec.LocalIndexes = append(spec.LocalIndexes, index.spec())
	}
	for _, index := range t.GlobalIndexes {
		spec.GlobalIndexes = append(spec.GlobalIndexes, index.spec())
	}
	return spec
}

func (i Index) spec() dynamodb.IndexSpec {
	spec := dynamodb.IndexSpec{
		Name:    i.Name,
		SortKey: optionalKey(i.SortKey),
		Projection: dynamodb.Projection{
			Type:             i.Projection,
			NonKeyAttributes: i.NonKeyAttributes,
		},
	}
	if i.PartitionKey != nil {
		spec.PartitionKey = i.PartitionKey.attribute()
	}
	return spec
}

func (k Key) attribute() dynamodb.KeyAttribute {
	kind := k.Type
	if kind == "" {
		kind = dynamodb.TypeString
	}
	return dynamodb.KeyAttribute{Name: k.Name, Type: kind}
}

func optionalKey(k *Key) *dynamodb.KeyAttribute {
	if k == nil {
		return nil
	}
	attribute := k.attribute()
	return &attribute
}
