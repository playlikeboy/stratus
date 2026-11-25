package dynamodb

import (
	"slices"
	"strings"
)

type indexWire struct {
	IndexName             string
	KeySchema             []keySchemaElement
	Projection            projectionWire
	ProvisionedThroughput *throughputWire
}

type createTableInput struct {
	TableName              string
	KeySchema              []keySchemaElement
	AttributeDefinitions   []attributeDefinition
	LocalSecondaryIndexes  []indexWire
	GlobalSecondaryIndexes []indexWire
	BillingMode            string
	ProvisionedThroughput  *throughputWire
}

type tableOutput struct {
	TableDescription tableDescription
}

func (s *Service) createTableOp(in createTableInput) (any, error) {
	spec, err := in.spec()
	if err != nil {
		return nil, err
	}
	t, err := s.createTable(spec)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return tableOutput{t.describe("ACTIVE")}, nil
}

// spec turns the wire form, key schemas referring to attribute definitions,
// into a TableSpec.
func (in createTableInput) spec() (TableSpec, error) {
	defs := map[string]string{}
	for _, d := range in.AttributeDefinitions {
		if _, dup := defs[d.AttributeName]; dup {
			return TableSpec{}, errValidation("Cannot have two attributes with the same name")
		}
		defs[d.AttributeName] = d.AttributeType
	}
	r := keyResolver{defs: defs, used: map[string]bool{}}
	spec := TableSpec{Name: in.TableName, BillingMode: in.BillingMode}
	if err := r.resolve(in.KeySchema, &spec.PartitionKey, &spec.SortKey); err != nil {
		return spec, err
	}
	for _, list := range []struct {
		wire []indexWire
		into *[]IndexSpec
	}{{in.LocalSecondaryIndexes, &spec.LocalIndexes}, {in.GlobalSecondaryIndexes, &spec.GlobalIndexes}} {
		for _, w := range list.wire {
			ix := IndexSpec{Name: w.IndexName, Projection: Projection{
				Type: w.Projection.ProjectionType, NonKeyAttributes: w.Projection.NonKeyAttributes,
			}}
			if err := r.resolve(w.KeySchema, &ix.PartitionKey, &ix.SortKey); err != nil {
				return spec, err
			}
			*list.into = append(*list.into, ix)
		}
	}
	if err := r.checkAllUsed(); err != nil {
		return spec, err
	}
	return spec, in.billing(&spec)
}

// billing applies DynamoDB's default, PROVISIONED, which needs throughput.
func (in createTableInput) billing(spec *TableSpec) error {
	if in.ProvisionedThroughput != nil {
		if in.BillingMode == PayPerRequest {
			return errValidation("One or more parameter values were invalid: Neither ReadCapacityUnits nor " +
				"WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST")
		}
		spec.ReadCapacity = in.ProvisionedThroughput.ReadCapacityUnits
		spec.WriteCapacity = in.ProvisionedThroughput.WriteCapacityUnits
	}
	if spec.BillingMode == "" {
		spec.BillingMode = Provisioned
	}
	return nil
}

// keyResolver looks key schema attributes up in the attribute definitions
// and records which definitions were used.
type keyResolver struct {
	defs map[string]string
	used map[string]bool
}

func (r keyResolver) resolve(ks []keySchemaElement, pk *KeyAttribute, sk **KeyAttribute) error {
	if len(ks) < 1 || len(ks) > 2 {
		return errValidation("1 validation error detected: Value '%v' at 'keySchema' failed to satisfy "+
			"constraint: Member must have length less than or equal to 2 and greater than or equal to 1", ks)
	}
	if ks[0].KeyType != "HASH" {
		return errValidation("Invalid KeySchema: The first KeySchemaElement is not a HASH key type")
	}
	if len(ks) == 2 && ks[1].KeyType != "RANGE" {
		return errValidation("Invalid KeySchema: The second KeySchemaElement is not a RANGE key type")
	}
	for i, k := range ks {
		t, ok := r.defs[k.AttributeName]
		if !ok {
			return errValidation("One or more parameter values were invalid: Some index key attributes are "+
				"not defined in AttributeDefinitions. Keys: [%s], AttributeDefinitions: [%s]",
				k.AttributeName, strings.Join(sortedKeys(r.defs), ", "))
		}
		r.used[k.AttributeName] = true
		if i == 0 {
			*pk = KeyAttribute{Name: k.AttributeName, Type: t}
		} else {
			*sk = &KeyAttribute{Name: k.AttributeName, Type: t}
		}
	}
	return nil
}

func (r keyResolver) checkAllUsed() error {
	if len(r.used) == len(r.defs) {
		return nil
	}
	return errValidation("One or more parameter values were invalid: Some AttributeDefinitions are not "+
		"used. AttributeDefinitions: [%s], keys used: [%s]",
		strings.Join(sortedKeys(r.defs), ", "), strings.Join(sortedKeys(r.used), ", "))
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

type tableNameInput struct {
	TableName string
}

func (s *Service) deleteTableOp(in tableNameInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	delete(s.tables, in.TableName)
	return tableOutput{t.describe("DELETING")}, nil
}

func (s *Service) describeTableOp(in tableNameInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	return map[string]tableDescription{"Table": t.describe("ACTIVE")}, nil
}

type listTablesInput struct {
	ExclusiveStartTableName string
	Limit                   *int
}

type listTablesOutput struct {
	TableNames             []string
	LastEvaluatedTableName string `json:",omitempty"`
}

func (s *Service) listTablesOp(in listTablesInput) (any, error) {
	limit := 100
	if in.Limit != nil {
		if *in.Limit < 1 || *in.Limit > 100 {
			return nil, errValidation("1 validation error detected: Value '%d' at 'limit' failed to satisfy "+
				"constraint: Member must have value less than or equal to 100 and greater than or equal to 1",
				*in.Limit)
		}
		limit = *in.Limit
	}
	s.mu.Lock()
	names := s.tableNames()
	s.mu.Unlock()
	if in.ExclusiveStartTableName != "" {
		at, found := slices.BinarySearch(names, in.ExclusiveStartTableName)
		if found {
			at++
		}
		names = names[at:]
	}
	out := listTablesOutput{TableNames: names}
	if len(names) > limit {
		out.TableNames = names[:limit]
		out.LastEvaluatedTableName = names[limit-1]
	}
	return out, nil
}
