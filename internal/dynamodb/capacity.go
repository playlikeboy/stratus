package dynamodb

import (
	"slices"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

const (
	returnCapacityNone    = "NONE"
	returnCapacityTotal   = "TOTAL"
	returnCapacityIndexes = "INDEXES"

	readBlockBytes  = 4096 // 4 KB per read capacity unit
	writeBlockBytes = 1024 // 1 KB per write capacity unit
)

// capacityInput holds ReturnConsumedCapacity on DynamoDB data-plane requests.
type capacityInput struct {
	ReturnConsumedCapacity string
}

func (in capacityInput) wantCapacity() (string, error) {
	switch in.ReturnConsumedCapacity {
	case "", returnCapacityNone:
		return "", nil
	case returnCapacityTotal, returnCapacityIndexes:
		return in.ReturnConsumedCapacity, nil
	default:
		return "", errValidation("1 validation error detected: Value '%s' at 'returnConsumedCapacity' "+
			"failed to satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
			in.ReturnConsumedCapacity)
	}
}

type capacity struct {
	ReadCapacityUnits  *float64 `json:",omitempty"`
	WriteCapacityUnits *float64 `json:",omitempty"`
	CapacityUnits      *float64 `json:",omitempty"`
}

type consumedCapacity struct {
	TableName              string              `json:"TableName"`
	CapacityUnits          float64             `json:"CapacityUnits"`
	ReadCapacityUnits      *float64            `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits     *float64            `json:"WriteCapacityUnits,omitempty"`
	Table                  *capacity           `json:"Table,omitempty"`
	LocalSecondaryIndexes  map[string]capacity `json:"LocalSecondaryIndexes,omitempty"`
	GlobalSecondaryIndexes map[string]capacity `json:"GlobalSecondaryIndexes,omitempty"`
}

func readUnits(sizeBytes int, consistent bool, factor float64) float64 {
	blocks := max(1, (sizeBytes+readBlockBytes-1)/readBlockBytes)
	unit := 0.5
	if consistent {
		unit = 1.0
	}
	return float64(blocks) * unit * factor
}

func writeUnits(sizeBytes int, factor float64) float64 {
	blocks := max(1, (sizeBytes+writeBlockBytes-1)/writeBlockBytes)
	return float64(blocks) * factor
}

func floatPtr(v float64) *float64 { return &v }

func makeCapacity(read, write float64, hasRead, hasWrite bool) capacity {
	c := capacity{CapacityUnits: floatPtr(read + write)}
	if hasRead {
		c.ReadCapacityUnits = floatPtr(read)
	}
	if hasWrite {
		c.WriteCapacityUnits = floatPtr(write)
	}
	return c
}

type tableCapacity struct {
	name      string
	hasBase   bool
	baseRead  float64
	baseWrite float64
	hasRead   bool
	hasWrite  bool
	lsiRead   map[string]float64
	lsiWrite  map[string]float64
	gsiRead   map[string]float64
	gsiWrite  map[string]float64
}

type capacityTracker struct {
	order  []string
	tables map[string]*tableCapacity
}

func newCapacityTracker() *capacityTracker {
	return &capacityTracker{tables: map[string]*tableCapacity{}}
}

func (ct *capacityTracker) entry(name string) *tableCapacity {
	tc, ok := ct.tables[name]
	if !ok {
		tc = &tableCapacity{
			name:     name,
			lsiRead:  map[string]float64{},
			lsiWrite: map[string]float64{},
			gsiRead:  map[string]float64{},
			gsiWrite: map[string]float64{},
		}
		ct.tables[name] = tc
		ct.order = append(ct.order, name)
	}
	return tc
}

func (ct *capacityTracker) addRead(t *table, ix *index, sizeBytes int, consistent bool, factor float64) {
	tc := ct.entry(t.spec.Name)
	tc.hasRead = true
	units := readUnits(sizeBytes, consistent, factor)
	if ix == nil || ix.name == "" {
		tc.hasBase = true
		tc.baseRead += units
		return
	}
	if ix.global {
		tc.gsiRead[ix.name] += units
		return
	}
	tc.lsiRead[ix.name] += units
}

func (ct *capacityTracker) addWrite(t *table, w write, factor float64) {
	tc := ct.entry(t.spec.Name)
	tc.hasWrite = true
	tc.hasBase = true
	baseBytes := attr.ItemSize(w.old)
	if !w.checkOnly {
		baseBytes = max(baseBytes, attr.ItemSize(w.new))
	}
	tc.baseWrite += writeUnits(baseBytes, factor)
	if w.checkOnly {
		return
	}
	for i := range t.spec.allIndexes() {
		spec := t.spec.indexAt(i)
		ix := t.index(spec.Name)
		oldIn := w.old != nil && t.inIndex(ix, w.old)
		newIn := w.new != nil && t.inIndex(ix, w.new)
		if !oldIn && !newIn {
			continue
		}
		var oldProj, newProj attr.Item
		idxBytes := 0
		if oldIn {
			oldProj = t.projectIndex(ix, w.old)
			idxBytes = max(idxBytes, attr.ItemSize(oldProj))
		}
		if newIn {
			newProj = t.projectIndex(ix, w.new)
			idxBytes = max(idxBytes, attr.ItemSize(newProj))
		}
		if oldIn && newIn && itemsEqual(oldProj, newProj) {
			continue
		}
		units := writeUnits(idxBytes, factor)
		if ix.global {
			tc.gsiWrite[ix.name] += units
		} else {
			tc.lsiWrite[ix.name] += units
		}
	}
}

func itemsEqual(a, b attr.Item) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || !attr.Equal(va, vb) {
			return false
		}
	}
	return true
}

func (ct *capacityTracker) single(mode string) *consumedCapacity {
	all := ct.list(mode)
	if len(all) == 0 {
		return nil
	}
	return &all[0]
}

func (ct *capacityTracker) list(mode string) []consumedCapacity {
	if mode == "" || len(ct.order) == 0 {
		return nil
	}
	names := slices.Clone(ct.order)
	slices.Sort(names)
	out := make([]consumedCapacity, 0, len(names))
	for _, name := range names {
		out = append(out, ct.tables[name].format(mode))
	}
	return out
}

func (tc *tableCapacity) format(mode string) consumedCapacity {
	totalRead := tc.baseRead + sumValues(tc.lsiRead) + sumValues(tc.gsiRead)
	totalWrite := tc.baseWrite + sumValues(tc.lsiWrite) + sumValues(tc.gsiWrite)
	cc := consumedCapacity{
		TableName:     tc.name,
		CapacityUnits: totalRead + totalWrite,
	}
	if tc.hasRead {
		cc.ReadCapacityUnits = floatPtr(totalRead)
	}
	if tc.hasWrite {
		cc.WriteCapacityUnits = floatPtr(totalWrite)
	}
	if mode != returnCapacityIndexes {
		return cc
	}
	if tc.hasBase {
		c := makeCapacity(tc.baseRead, tc.baseWrite, tc.hasRead, tc.hasWrite)
		cc.Table = &c
	} else {
		// Reading strictly through a GSI charges 0 on the base table.
		c := makeCapacity(0, 0, tc.hasRead, tc.hasWrite)
		cc.Table = &c
	}
	if lsi := buildIndexMap(tc.lsiRead, tc.lsiWrite, tc.hasRead, tc.hasWrite); len(lsi) > 0 {
		cc.LocalSecondaryIndexes = lsi
	}
	if gsi := buildIndexMap(tc.gsiRead, tc.gsiWrite, tc.hasRead, tc.hasWrite); len(gsi) > 0 {
		cc.GlobalSecondaryIndexes = gsi
	}
	return cc
}

func sumValues(m map[string]float64) float64 {
	total := 0.0
	for _, v := range m {
		total += v
	}
	return total
}

func buildIndexMap(readMap, writeMap map[string]float64, hasRead, hasWrite bool) map[string]capacity {
	out := map[string]capacity{}
	for name, r := range readMap {
		w := writeMap[name]
		out[name] = makeCapacity(r, w, hasRead, hasWrite)
	}
	for name, w := range writeMap {
		if _, ok := out[name]; ok {
			continue
		}
		out[name] = makeCapacity(readMap[name], w, hasRead, hasWrite)
	}
	return out
}
