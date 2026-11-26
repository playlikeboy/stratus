package dynamodb

import (
	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// maxItemBytes is DynamoDB's item size limit, 400 KB.
const maxItemBytes = 400 * 1024

// checkKey checks a key given to GetItem, DeleteItem, UpdateItem and the
// like: exactly the table's key attributes, of the right types.
func (t *table) checkKey(key attr.Item) error {
	want := 1
	if t.spec.SortKey != nil {
		want = 2
	}
	if len(key) != want {
		return errValidation("The provided key element does not match the schema")
	}
	for name := range t.keyOf(key) {
		v, ok := key[name]
		if !ok || v.Type != t.types[name] {
			return errValidation("The provided key element does not match the schema")
		}
		if err := checkKeyValue(name, v); err != nil {
			return err
		}
	}
	return nil
}

func checkKeyValue(name string, v attr.Value) error {
	switch {
	case v.Type == attr.S && v.S == "":
		return errValidation("One or more parameter values are not valid. The AttributeValue for a key "+
			"attribute cannot contain an empty string value. Key: %s", name)
	case v.Type == attr.B && len(v.B) == 0:
		return errValidation("One or more parameter values are not valid. The AttributeValue for a key "+
			"attribute cannot contain an empty binary value. Key: %s", name)
	}
	return nil
}

// checkItem checks an item about to be stored: its key attributes, the
// index key attributes it has, and its size.
func (t *table) checkItem(item attr.Item) error {
	for name := range t.keyOf(item) {
		v, ok := item[name]
		if !ok {
			return errValidation("One or more parameter values were invalid: Missing the key %s in the item", name)
		}
		if v.Type != t.types[name] {
			return errValidation("One or more parameter values were invalid: Type mismatch for key %s "+
				"expected: %s actual: %s", name, t.types[name], v.Type)
		}
		if err := checkKeyValue(name, v); err != nil {
			return err
		}
	}
	for i := range t.spec.allIndexes() {
		if err := t.checkIndexKeys(t.spec.indexAt(i), item); err != nil {
			return err
		}
	}
	if attr.ItemSize(item) > maxItemBytes {
		return errValidation("Item size has exceeded the maximum allowed size")
	}
	return nil
}

// checkIndexKeys checks the index key attributes item has; an item without
// them is simply absent from the index.
func (t *table) checkIndexKeys(ix *IndexSpec, item attr.Item) error {
	for _, k := range []*KeyAttribute{&ix.PartitionKey, ix.SortKey} {
		if k == nil {
			continue
		}
		v, ok := item[k.Name]
		if !ok {
			continue
		}
		if v.Type != attr.Type(k.Type) {
			return errValidation("One or more parameter values were invalid: Type mismatch for Index Key %s "+
				"Expected: %s Actual: %s IndexName: %s", k.Name, k.Type, v.Type, ix.Name)
		}
		if (v.Type == attr.S && v.S == "") || (v.Type == attr.B && len(v.B) == 0) {
			return errValidation("One or more parameter values are not valid. A value specified for a "+
				"secondary index key is not supported. The AttributeValue for a key attribute cannot contain "+
				"an empty string value. IndexName: %s, IndexKey: %s", ix.Name, k.Name)
		}
	}
	return nil
}

// checkUpdatedKey rejects an update that writes a key attribute.
func (t *table) checkUpdatedKey(paths []string) error {
	for _, top := range paths {
		if _, isKey := t.keyOf(attr.Item{})[top]; isKey {
			return errValidation("One or more parameter values were invalid: Cannot update attribute %s. "+
				"This attribute is part of the key", top)
		}
	}
	return nil
}
