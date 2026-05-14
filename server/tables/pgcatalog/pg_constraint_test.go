package pgcatalog

import (
	"testing"

	"github.com/dolthub/doltgresql/core/id"

	assert "github.com/stretchr/testify/require"
)

func TestIndexes(t *testing.T) {
	// Only the fields that impact indexing are set here
	con1 := &pgConstraint{
		oidNative:       100,
		name:            "abc",
		tableOidNative:  300,
		typeOidNative:   0, // this means the constraint is on a table, not a type
		schemaOidNative: 500,
	}

	relidTypNameIdx := NewUniqueInMemIndexStorage[*pgConstraint](lessConstraintRelidTypeName)

	relidTypNameIdx.Add(con1)

	var foundElements []*pgConstraint
	cb := func(c *pgConstraint) bool {
		foundElements = append(foundElements, c)
		return true
	}

	// lookup by relid
	relidTypNameIdx.uniqTree.AscendRange(
		&pgConstraint{tableOidNative: 300},
		&pgConstraint{tableOidNative: 301},
		cb,
	)

	assert.Equal(t, []*pgConstraint{con1}, foundElements)

	foundElements = nil
	// lookup by relid, typeid
	relidTypNameIdx.uniqTree.AscendRange(
		&pgConstraint{tableOidNative: 300, typeOidNative: 0},
		&pgConstraint{tableOidNative: 300, typeOidNative: 1},
		cb,
	)

	assert.Equal(t, []*pgConstraint{con1}, foundElements)
}

func TestPgConstraintOptionalOidFieldsUseZeroOid(t *testing.T) {
	row := pgConstraintToRow(&pgConstraint{
		oid:       id.NewCheck("public", "items", "items_label_not_null").AsId(),
		name:      "items_label_not_null",
		schemaOid: id.NewNamespace("public").AsId(),
		conType:   "n",
	})

	zeroOid := id.NewOID(0).AsId()
	assert.Equal(t, zeroOid, row[7])  // conrelid
	assert.Equal(t, zeroOid, row[8])  // contypid
	assert.Equal(t, zeroOid, row[9])  // conindid
	assert.Equal(t, zeroOid, row[10]) // conparentid
	assert.Equal(t, zeroOid, row[11]) // confrelid
}
