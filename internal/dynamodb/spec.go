package dynamodb

import (
	"regexp"
	"slices"
)

// Key attribute types.
const (
	TypeString = "S"
	TypeNumber = "N"
	TypeBinary = "B"
)

// Index projection types.
const (
	ProjectAll      = "ALL"
	ProjectKeysOnly = "KEYS_ONLY"
	ProjectInclude  = "INCLUDE"
)

// Billing modes. Neither is ever throttled.
const (
	PayPerRequest = "PAY_PER_REQUEST"
	Provisioned   = "PROVISIONED"
)

const (
	maxLocalIndexes  = 5
	maxGlobalIndexes = 20
)

var validTableName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,255}$`)

// TableSpec describes a table to create.
type TableSpec struct {
	Name         string
	PartitionKey KeyAttribute
	SortKey      *KeyAttribute // nil: the table has a partition key only
	// LocalIndexes share the table's partition key and need a SortKey of
	// their own; their PartitionKey may be left zero.
	LocalIndexes  []IndexSpec
	GlobalIndexes []IndexSpec
	// TTLAttribute names the attribute holding each item's expiry time, in
	// epoch seconds; empty leaves TTL disabled.
	TTLAttribute string
	// BillingMode is PayPerRequest (the default when empty) or Provisioned,
	// which needs ReadCapacity and WriteCapacity.
	BillingMode   string
	ReadCapacity  int64
	WriteCapacity int64
}

// KeyAttribute is a key attribute: its name and its type, TypeString,
// TypeNumber or TypeBinary.
type KeyAttribute struct {
	Name string
	Type string
}

// IndexSpec describes a secondary index.
type IndexSpec struct {
	Name         string
	PartitionKey KeyAttribute
	SortKey      *KeyAttribute
	Projection   Projection
}

// Projection is the set of attributes an index holds besides the keys.
type Projection struct {
	// Type is ProjectAll (the default when empty), ProjectKeysOnly or
	// ProjectInclude.
	Type string
	// NonKeyAttributes are the attributes an INCLUDE projection adds.
	NonKeyAttributes []string
}

// normalised validates spec and returns a copy with its defaults filled in;
// the copy shares nothing with the caller's spec.
func (spec TableSpec) normalised() (TableSpec, error) {
	out := spec
	if !validTableName.MatchString(spec.Name) {
		return out, errValidation("1 validation error detected: Value '%s' at 'tableName' failed to satisfy "+
			"constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+ and have length "+
			"between 3 and 255", spec.Name)
	}
	if err := out.normaliseBilling(); err != nil {
		return out, err
	}
	out.SortKey = cloneKey(spec.SortKey)
	out.LocalIndexes = cloneIndexes(spec.LocalIndexes)
	out.GlobalIndexes = cloneIndexes(spec.GlobalIndexes)
	if err := out.checkKeys(); err != nil {
		return out, err
	}
	if err := out.normaliseLocalIndexes(); err != nil {
		return out, err
	}
	if len(out.GlobalIndexes) > maxGlobalIndexes {
		return out, errValidation("One or more parameter values were invalid: Number of GlobalSecondaryIndexes "+
			"exceeds per-table limit of %d", maxGlobalIndexes)
	}
	for _, g := range out.GlobalIndexes {
		if g.PartitionKey.Name == "" {
			return out, errValidation("One or more parameter values were invalid: Index KeySchema of the "+
				"global secondary index %s has no hash key", g.Name)
		}
	}
	seen := map[string]bool{}
	for i := range out.allIndexes() {
		ix := out.indexAt(i)
		if err := ix.normalise(seen); err != nil {
			return out, err
		}
	}
	return out, out.checkKeys()
}

func (spec *TableSpec) normaliseBilling() error {
	switch spec.BillingMode {
	case "", PayPerRequest:
		if spec.ReadCapacity != 0 || spec.WriteCapacity != 0 {
			return errValidation("One or more parameter values were invalid: Neither ReadCapacityUnits nor " +
				"WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST")
		}
		spec.BillingMode = PayPerRequest
	case Provisioned:
		if spec.ReadCapacity < 1 || spec.WriteCapacity < 1 {
			return errValidation("One or more parameter values were invalid: ReadCapacityUnits and " +
				"WriteCapacityUnits must both be specified when BillingMode is PROVISIONED")
		}
	default:
		return errValidation("1 validation error detected: Value '%s' at 'billingMode' failed to satisfy "+
			"constraint: Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]", spec.BillingMode)
	}
	return nil
}

func (spec *TableSpec) normaliseLocalIndexes() error {
	if len(spec.LocalIndexes) == 0 {
		return nil
	}
	if spec.SortKey == nil {
		return errValidation("One or more parameter values were invalid: Table KeySchema does not have a " +
			"range key, which is required when specifying a LocalSecondaryIndex")
	}
	if len(spec.LocalIndexes) > maxLocalIndexes {
		return errValidation("One or more parameter values were invalid: Number of LocalSecondaryIndexes "+
			"exceeds per-table limit of %d", maxLocalIndexes)
	}
	for i := range spec.LocalIndexes {
		ix := &spec.LocalIndexes[i]
		if ix.PartitionKey.Name == "" {
			ix.PartitionKey = spec.PartitionKey
		}
		if ix.PartitionKey != spec.PartitionKey {
			return errValidation("One or more parameter values were invalid: Index KeySchema does not have "+
				"the same leading hash key as table KeySchema for index: %s. index hash key: %s, table hash "+
				"key: %s", ix.Name, ix.PartitionKey.Name, spec.PartitionKey.Name)
		}
		if ix.SortKey == nil {
			return errValidation("One or more parameter values were invalid: Index KeySchema must have a "+
				"range key for a local secondary index: %s", ix.Name)
		}
	}
	return nil
}

// allIndexes counts the local and then the global indexes, for indexAt.
func (spec *TableSpec) allIndexes() int { return len(spec.LocalIndexes) + len(spec.GlobalIndexes) }

func (spec *TableSpec) indexAt(i int) *IndexSpec {
	if i < len(spec.LocalIndexes) {
		return &spec.LocalIndexes[i]
	}
	return &spec.GlobalIndexes[i-len(spec.LocalIndexes)]
}

func (ix *IndexSpec) normalise(seen map[string]bool) error {
	if !validTableName.MatchString(ix.Name) {
		return errValidation("1 validation error detected: Value '%s' at 'indexName' failed to satisfy "+
			"constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", ix.Name)
	}
	if seen[ix.Name] {
		return errValidation("One or more parameter values were invalid: Duplicate index name: %s", ix.Name)
	}
	seen[ix.Name] = true
	switch ix.Projection.Type {
	case "", ProjectAll, ProjectKeysOnly:
		if ix.Projection.Type == "" {
			ix.Projection.Type = ProjectAll
		}
		if len(ix.Projection.NonKeyAttributes) > 0 {
			return errValidation("One or more parameter values were invalid: ProjectionType is %s, but "+
				"NonKeyAttributes is specified", ix.Projection.Type)
		}
	case ProjectInclude:
		if len(ix.Projection.NonKeyAttributes) == 0 {
			return errValidation("One or more parameter values were invalid: ProjectionType is INCLUDE, " +
				"but NonKeyAttributes is not specified")
		}
	default:
		return errValidation("1 validation error detected: Value '%s' at 'projection.projectionType' "+
			"failed to satisfy constraint: Member must satisfy enum value set: [ALL, INCLUDE, KEYS_ONLY]",
			ix.Projection.Type)
	}
	return nil
}

// checkKeys checks every key attribute: a name, a valid type, and one type
// per name across the table and its indexes.
func (spec *TableSpec) checkKeys() error {
	types := map[string]string{}
	for _, k := range spec.keyAttributes() {
		if k.Name == "" {
			return errValidation("One or more parameter values were invalid: A key attribute has no name")
		}
		if !slices.Contains([]string{TypeString, TypeNumber, TypeBinary}, k.Type) {
			return errValidation("1 validation error detected: Value '%s' at 'attributeDefinitions.member."+
				"attributeType' failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]", k.Type)
		}
		if t, ok := types[k.Name]; ok && t != k.Type {
			return errValidation("Cannot have two attributes with the same name")
		}
		types[k.Name] = k.Type
	}
	return nil
}

// keyAttributes lists the key attributes of the table and of its indexes.
func (spec *TableSpec) keyAttributes() []KeyAttribute {
	keys := []KeyAttribute{spec.PartitionKey}
	if spec.SortKey != nil {
		keys = append(keys, *spec.SortKey)
	}
	for i := range spec.allIndexes() {
		ix := spec.indexAt(i)
		if ix.PartitionKey.Name != "" {
			keys = append(keys, ix.PartitionKey)
		}
		if ix.SortKey != nil {
			keys = append(keys, *ix.SortKey)
		}
	}
	return keys
}

func cloneKey(k *KeyAttribute) *KeyAttribute {
	if k == nil {
		return nil
	}
	c := *k
	return &c
}

func cloneIndexes(in []IndexSpec) []IndexSpec {
	out := slices.Clone(in)
	for i := range out {
		out[i].SortKey = cloneKey(out[i].SortKey)
		out[i].Projection.NonKeyAttributes = slices.Clone(out[i].Projection.NonKeyAttributes)
	}
	return out
}
