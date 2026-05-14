// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fkmetadata

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	pgmerge "github.com/dolthub/doltgresql/core/merge"
	"github.com/dolthub/doltgresql/core/rootobject/objinterface"
	"github.com/dolthub/doltgresql/flatbuffers/gen/serial"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// Timing stores PostgreSQL DEFERRABLE timing metadata for a foreign key.
type Timing struct {
	Deferrable        bool
	InitiallyDeferred bool
}

// ActionColumns stores PostgreSQL SET NULL / SET DEFAULT column lists for
// referential actions. Dolt's FK collection only models the action kind, not
// the optional subset of child columns affected by that action.
type ActionColumns struct {
	OnUpdate []string
	OnDelete []string
}

func (columns ActionColumns) IsEmpty() bool {
	return len(columns.OnUpdate) == 0 && len(columns.OnDelete) == 0
}

// Metadata stores Doltgres-only foreign-key metadata that Dolt's FK
// collection does not yet model directly.
type Metadata struct {
	ID            id.ForeignKey
	Columns       []string
	ParentSchema  string
	ParentTable   string
	ParentColumns []string
	Timing        Timing
	MatchFull     bool
	ActionColumns ActionColumns
}

// Collection contains foreign-key metadata in the current database root.
type Collection struct {
	accessCache   map[id.ForeignKey]Metadata
	idCache       []id.ForeignKey
	mapHash       hash.Hash
	underlyingMap prolly.AddressMap
	ns            tree.NodeStore
}

var _ objinterface.Collection = (*Collection)(nil)
var _ objinterface.RootObject = Metadata{}

var storage = objinterface.RootObjectSerializer{
	Bytes:        (*serial.RootValue).ForeignKeyMetadataBytes,
	RootValueAdd: serial.RootValueAddForeignKeyMetadata,
}

// NewCollection returns a new Collection.
func NewCollection(ctx context.Context, underlyingMap prolly.AddressMap, ns tree.NodeStore) (*Collection, error) {
	collection := &Collection{
		accessCache:   make(map[id.ForeignKey]Metadata),
		underlyingMap: underlyingMap,
		ns:            ns,
	}
	return collection, collection.reloadCaches(ctx)
}

// MetadataFromForeignKey returns persisted metadata for fk using the given resolved schema name.
func MetadataFromForeignKey(schemaName string, fk sql.ForeignKeyConstraint, timing Timing, matchFull bool, actionColumns ActionColumns) Metadata {
	if schemaName == "" {
		schemaName = fk.SchemaName
	}
	return Metadata{
		ID:            id.NewForeignKey(schemaName, fk.Table, fk.Name),
		Columns:       slices.Clone(fk.Columns),
		ParentSchema:  fk.ParentSchema,
		ParentTable:   fk.ParentTable,
		ParentColumns: slices.Clone(fk.ParentColumns),
		Timing:        timing,
		MatchFull:     matchFull,
		ActionColumns: actionColumns.clone(),
	}
}

// SetTiming persists timing metadata for a deferrable FK and removes metadata for a non-deferrable FK.
func (pgf *Collection) SetTiming(ctx context.Context, metadata Metadata) error {
	metadata.normalize()
	if !metadata.ID.IsValid() {
		return nil
	}
	if existing, ok := pgf.accessCache[metadata.ID]; ok {
		metadata.MatchFull = existing.MatchFull
		metadata.ActionColumns = existing.ActionColumns.clone()
	}
	return pgf.setMetadata(ctx, metadata)
}

// SetMatchFull persists MATCH FULL metadata for a FK.
func (pgf *Collection) SetMatchFull(ctx context.Context, metadata Metadata) error {
	metadata.normalize()
	if !metadata.ID.IsValid() {
		return nil
	}
	if existing, ok := pgf.accessCache[metadata.ID]; ok {
		metadata.Timing = existing.Timing
		metadata.ActionColumns = existing.ActionColumns.clone()
	}
	return pgf.setMetadata(ctx, metadata)
}

// SetMetadata persists the complete metadata record for a FK.
func (pgf *Collection) SetMetadata(ctx context.Context, metadata Metadata) error {
	metadata.normalize()
	if !metadata.ID.IsValid() {
		return nil
	}
	return pgf.setMetadata(ctx, metadata)
}

func (pgf *Collection) setMetadata(ctx context.Context, metadata Metadata) error {
	if !metadata.Timing.Deferrable && !metadata.MatchFull && metadata.ActionColumns.IsEmpty() {
		return pgf.DeleteMetadata(ctx, metadata.ID)
	}
	return pgf.putMetadata(ctx, metadata)
}

// DeleteMetadata removes metadata for the given FK if it exists.
func (pgf *Collection) DeleteMetadata(ctx context.Context, fkID id.ForeignKey) error {
	if !fkID.IsValid() {
		return nil
	}
	if _, ok := pgf.accessCache[fkID]; !ok {
		return nil
	}
	mapEditor := pgf.underlyingMap.Editor()
	if err := mapEditor.Delete(ctx, string(fkID)); err != nil {
		return err
	}
	newMap, err := mapEditor.Flush(ctx)
	if err != nil {
		return err
	}
	pgf.underlyingMap = newMap
	return pgf.reloadCaches(ctx)
}

// TimingForForeignKey returns the persisted timing metadata for fk.
func (pgf *Collection) TimingForForeignKey(ctx context.Context, fkID id.ForeignKey, fk sql.ForeignKeyConstraint) (Timing, bool, error) {
	if fkID.IsValid() {
		if metadata, ok := pgf.accessCache[fkID]; ok && metadata.matchesForeignKey(fkID.SchemaName(), fk) {
			return metadata.Timing, true, nil
		}
	}
	schemaName := fk.SchemaName
	if fkID.IsValid() {
		schemaName = fkID.SchemaName()
	}
	for _, cachedID := range pgf.idCache {
		metadata := pgf.accessCache[cachedID]
		if metadata.matchesForeignKey(schemaName, fk) {
			return metadata.Timing, true, nil
		}
	}
	return Timing{}, false, nil
}

// MatchFullForForeignKey returns whether fk was declared with MATCH FULL.
func (pgf *Collection) MatchFullForForeignKey(ctx context.Context, fkID id.ForeignKey, fk sql.ForeignKeyConstraint) (bool, bool, error) {
	if fkID.IsValid() {
		if metadata, ok := pgf.accessCache[fkID]; ok && metadata.matchesForeignKey(fkID.SchemaName(), fk) {
			return metadata.MatchFull, true, nil
		}
	}
	schemaName := fk.SchemaName
	if fkID.IsValid() {
		schemaName = fkID.SchemaName()
	}
	for _, cachedID := range pgf.idCache {
		metadata := pgf.accessCache[cachedID]
		if metadata.matchesForeignKey(schemaName, fk) {
			return metadata.MatchFull, true, nil
		}
	}
	return false, false, nil
}

// ActionColumnsForForeignKey returns the persisted SET NULL / SET DEFAULT
// column-list metadata for fk.
func (pgf *Collection) ActionColumnsForForeignKey(ctx context.Context, fkID id.ForeignKey, fk sql.ForeignKeyConstraint) (ActionColumns, bool, error) {
	if fkID.IsValid() {
		if metadata, ok := pgf.accessCache[fkID]; ok && metadata.matchesForeignKey(fkID.SchemaName(), fk) {
			return metadata.ActionColumns.clone(), true, nil
		}
	}
	schemaName := fk.SchemaName
	if fkID.IsValid() {
		schemaName = fkID.SchemaName()
	}
	for _, cachedID := range pgf.idCache {
		metadata := pgf.accessCache[cachedID]
		if metadata.matchesForeignKey(schemaName, fk) {
			return metadata.ActionColumns.clone(), true, nil
		}
	}
	return ActionColumns{}, false, nil
}

func (pgf *Collection) putMetadata(ctx context.Context, metadata Metadata) error {
	metadata.normalize()
	data, err := metadata.Serialize(ctx)
	if err != nil {
		return err
	}
	h, err := pgf.ns.WriteBytes(ctx, data)
	if err != nil {
		return err
	}
	mapEditor := pgf.underlyingMap.Editor()
	if err := mapEditor.Add(ctx, string(metadata.ID), h); err != nil {
		return err
	}
	newMap, err := mapEditor.Flush(ctx)
	if err != nil {
		return err
	}
	pgf.underlyingMap = newMap
	return pgf.reloadCaches(ctx)
}

// Map returns the underlying map.
func (pgf *Collection) Map(_ context.Context) (prolly.AddressMap, error) {
	return pgf.underlyingMap, nil
}

// DiffersFrom returns true when this collection differs from the collection in the given root.
func (pgf *Collection) DiffersFrom(ctx context.Context, root objinterface.RootValue) bool {
	hashOnGivenRoot, err := pgf.LoadCollectionHash(ctx, root)
	if err != nil {
		return true
	}
	if pgf.mapHash.Equal(hashOnGivenRoot) {
		return false
	}
	count, err := pgf.underlyingMap.Count()
	return err != nil || count != 0 || !hashOnGivenRoot.IsEmpty()
}

func (pgf *Collection) reloadCaches(ctx context.Context) error {
	count, err := pgf.underlyingMap.Count()
	if err != nil {
		return err
	}
	clear(pgf.accessCache)
	pgf.mapHash = pgf.underlyingMap.HashOf()
	pgf.idCache = make([]id.ForeignKey, 0, count)
	return pgf.underlyingMap.IterAll(ctx, func(_ string, h hash.Hash) error {
		if h.IsEmpty() {
			return nil
		}
		data, err := pgf.ns.ReadBytes(ctx, h)
		if err != nil {
			return err
		}
		metadata, err := DeserializeMetadata(ctx, data)
		if err != nil {
			return err
		}
		metadata.normalize()
		pgf.accessCache[metadata.ID] = metadata
		pgf.idCache = append(pgf.idCache, metadata.ID)
		return nil
	})
}

// Serialize returns the metadata as a byte slice.
func (metadata Metadata) Serialize(ctx context.Context) ([]byte, error) {
	if !metadata.ID.IsValid() {
		return nil, nil
	}
	metadata.normalize()
	writer := utils.NewWriter(256)
	writer.VariableUint(0)
	writer.Id(metadata.ID.AsId())
	writer.StringSlice(metadata.Columns)
	writer.String(metadata.ParentSchema)
	writer.String(metadata.ParentTable)
	writer.StringSlice(metadata.ParentColumns)
	writer.Bool(metadata.Timing.Deferrable)
	writer.Bool(metadata.Timing.InitiallyDeferred)
	writer.Bool(metadata.MatchFull)
	writer.StringSlice(metadata.ActionColumns.OnUpdate)
	writer.StringSlice(metadata.ActionColumns.OnDelete)
	return writer.Data(), nil
}

// DeserializeMetadata returns the metadata serialized in data.
func DeserializeMetadata(ctx context.Context, data []byte) (Metadata, error) {
	if len(data) == 0 {
		return Metadata{}, nil
	}
	reader := utils.NewReader(data)
	version := reader.VariableUint()
	if version != 0 {
		return Metadata{}, errors.Errorf("version %d of foreign-key metadata is not supported, please upgrade the server", version)
	}
	metadata := Metadata{}
	metadata.ID = id.ForeignKey(reader.Id())
	metadata.Columns = reader.StringSlice()
	metadata.ParentSchema = reader.String()
	metadata.ParentTable = reader.String()
	metadata.ParentColumns = reader.StringSlice()
	metadata.Timing.Deferrable = reader.Bool()
	metadata.Timing.InitiallyDeferred = reader.Bool()
	if !reader.IsEmpty() {
		metadata.MatchFull = reader.Bool()
	}
	if !reader.IsEmpty() {
		metadata.ActionColumns.OnUpdate = reader.StringSlice()
	}
	if !reader.IsEmpty() {
		metadata.ActionColumns.OnDelete = reader.StringSlice()
	}
	if !reader.IsEmpty() {
		return Metadata{}, errors.New("extra data found while deserializing foreign-key metadata")
	}
	metadata.normalize()
	return metadata, nil
}

func (metadata *Metadata) normalize() {
	metadata.Columns = compactStringsPreservingOrder(metadata.Columns)
	metadata.ParentColumns = compactStringsPreservingOrder(metadata.ParentColumns)
	metadata.ActionColumns.OnUpdate = compactStringsPreservingOrder(metadata.ActionColumns.OnUpdate)
	metadata.ActionColumns.OnDelete = compactStringsPreservingOrder(metadata.ActionColumns.OnDelete)
}

func (columns ActionColumns) clone() ActionColumns {
	return ActionColumns{
		OnUpdate: slices.Clone(columns.OnUpdate),
		OnDelete: slices.Clone(columns.OnDelete),
	}
}

func compactStringsPreservingOrder(values []string) []string {
	if len(values) < 2 {
		return values
	}
	seen := make(map[string]struct{}, len(values))
	ret := values[:0]
	for _, value := range values {
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, value)
	}
	return ret
}

func (metadata Metadata) matchesForeignKey(schemaName string, fk sql.ForeignKeyConstraint) bool {
	if !strings.EqualFold(metadata.ID.ForeignKeyName(), fk.Name) ||
		!strings.EqualFold(metadata.ID.TableName(), fk.Table) ||
		!equalWhenBothSet(metadata.ID.SchemaName(), schemaName) ||
		!equalWhenBothSet(metadata.ParentSchema, fk.ParentSchema) ||
		!strings.EqualFold(metadata.ParentTable, fk.ParentTable) ||
		!equalStringSlices(metadata.Columns, fk.Columns) {
		return false
	}
	return len(metadata.ParentColumns) == 0 || equalStringSlices(metadata.ParentColumns, fk.ParentColumns)
}

func equalWhenBothSet(left string, right string) bool {
	return left == "" || right == "" || strings.EqualFold(left, right)
}

func equalStringSlices(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !strings.EqualFold(left[i], right[i]) {
			return false
		}
	}
	return true
}

func (metadata Metadata) summary() string {
	metadata.normalize()
	return fmt.Sprintf("%s columns=%v parent=%s.%s(%v) deferrable=%t initiallyDeferred=%t matchFull=%t onUpdateColumns=%v onDeleteColumns=%v",
		metadata.ID.AsId().String(), metadata.Columns, metadata.ParentSchema, metadata.ParentTable, metadata.ParentColumns,
		metadata.Timing.Deferrable, metadata.Timing.InitiallyDeferred, metadata.MatchFull,
		metadata.ActionColumns.OnUpdate, metadata.ActionColumns.OnDelete)
}

// DeserializeRootObject implements the interface objinterface.Collection.
func (pgf *Collection) DeserializeRootObject(ctx context.Context, data []byte) (objinterface.RootObject, error) {
	return DeserializeMetadata(ctx, data)
}

// DiffRootObjects implements the interface objinterface.Collection.
func (pgf *Collection) DiffRootObjects(ctx context.Context, fromHash string, o objinterface.RootObject, t objinterface.RootObject, a objinterface.RootObject) ([]objinterface.RootObjectDiff, objinterface.RootObject, error) {
	ours := o.(Metadata)
	theirs := t.(Metadata)
	if ours.summary() == theirs.summary() {
		return nil, ours, nil
	}
	var ancestor any
	if anc, ok := a.(Metadata); ok {
		ancestor = anc.summary()
	}
	return []objinterface.RootObjectDiff{{
		Type:          pgtypes.Text,
		FromHash:      fromHash,
		FieldName:     objinterface.FIELD_NAME_ROOT_OBJECT,
		AncestorValue: ancestor,
		OurValue:      ours.summary(),
		TheirValue:    theirs.summary(),
		OurChange:     objinterface.RootObjectDiffChange_Modified,
		TheirChange:   objinterface.RootObjectDiffChange_Modified,
	}}, ours, nil
}

// DropRootObject implements the interface objinterface.Collection.
func (pgf *Collection) DropRootObject(ctx context.Context, identifier id.Id) error {
	if identifier.Section() != id.Section_ForeignKey {
		return errors.Errorf(`foreign key metadata %s does not exist`, identifier.String())
	}
	return pgf.DeleteMetadata(ctx, id.ForeignKey(identifier))
}

// GetFieldType implements the interface objinterface.Collection.
func (pgf *Collection) GetFieldType(ctx context.Context, fieldName string) *pgtypes.DoltgresType {
	return nil
}

// GetID implements the interface objinterface.Collection.
func (pgf *Collection) GetID() objinterface.RootObjectID {
	return objinterface.RootObjectID_ForeignKeyMetadata
}

// GetRootObject implements the interface objinterface.Collection.
func (pgf *Collection) GetRootObject(ctx context.Context, identifier id.Id) (objinterface.RootObject, bool, error) {
	if identifier.Section() != id.Section_ForeignKey {
		return nil, false, nil
	}
	metadata, ok := pgf.accessCache[id.ForeignKey(identifier)]
	return metadata, ok, nil
}

// HasRootObject implements the interface objinterface.Collection.
func (pgf *Collection) HasRootObject(ctx context.Context, identifier id.Id) (bool, error) {
	if identifier.Section() != id.Section_ForeignKey {
		return false, nil
	}
	_, ok := pgf.accessCache[id.ForeignKey(identifier)]
	return ok, nil
}

// IDToTableName implements the interface objinterface.Collection.
func (pgf *Collection) IDToTableName(identifier id.Id) doltdb.TableName {
	if identifier.Section() != id.Section_ForeignKey {
		return doltdb.TableName{}
	}
	fkID := id.ForeignKey(identifier)
	return doltdb.TableName{
		Name:   fmt.Sprintf("%s.%s", fkID.TableName(), fkID.ForeignKeyName()),
		Schema: fkID.SchemaName(),
	}
}

// IterAll implements the interface objinterface.Collection.
func (pgf *Collection) IterAll(ctx context.Context, callback func(rootObj objinterface.RootObject) (stop bool, err error)) error {
	for _, fkID := range pgf.idCache {
		stop, err := callback(pgf.accessCache[fkID])
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// IterIDs implements the interface objinterface.Collection.
func (pgf *Collection) IterIDs(ctx context.Context, callback func(identifier id.Id) (stop bool, err error)) error {
	for _, fkID := range pgf.idCache {
		stop, err := callback(fkID.AsId())
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// PutRootObject implements the interface objinterface.Collection.
func (pgf *Collection) PutRootObject(ctx context.Context, rootObj objinterface.RootObject) error {
	metadata, ok := rootObj.(Metadata)
	if !ok {
		return errors.Newf("invalid foreign-key metadata root object: %T", rootObj)
	}
	return pgf.setMetadata(ctx, metadata)
}

// RenameRootObject implements the interface objinterface.Collection.
func (pgf *Collection) RenameRootObject(ctx context.Context, oldName id.Id, newName id.Id) error {
	if oldName.Section() != id.Section_ForeignKey || newName.Section() != id.Section_ForeignKey {
		return errors.New("cannot rename foreign-key metadata due to invalid name")
	}
	metadata, ok := pgf.accessCache[id.ForeignKey(oldName)]
	if !ok {
		return errors.Errorf(`foreign-key metadata "%s" does not exist`, id.ForeignKey(oldName).ForeignKeyName())
	}
	if err := pgf.DeleteMetadata(ctx, id.ForeignKey(oldName)); err != nil {
		return err
	}
	metadata.ID = id.ForeignKey(newName)
	return pgf.setMetadata(ctx, metadata)
}

// ResolveName implements the interface objinterface.Collection.
func (pgf *Collection) ResolveName(ctx context.Context, name doltdb.TableName) (doltdb.TableName, id.Id, error) {
	target := pgf.TableNameToID(name)
	if !target.IsValid() {
		return doltdb.TableName{}, id.Null, nil
	}
	for _, fkID := range pgf.idCache {
		if strings.EqualFold(string(fkID), string(target)) {
			return pgf.IDToTableName(fkID.AsId()), fkID.AsId(), nil
		}
	}
	return doltdb.TableName{}, id.Null, nil
}

// TableNameToID implements the interface objinterface.Collection.
func (pgf *Collection) TableNameToID(name doltdb.TableName) id.Id {
	parts := strings.Split(name.Name, ".")
	if len(parts) != 2 {
		return id.Null
	}
	return id.NewForeignKey(name.Schema, parts[0], parts[1]).AsId()
}

// UpdateField implements the interface objinterface.Collection.
func (pgf *Collection) UpdateField(ctx context.Context, rootObject objinterface.RootObject, fieldName string, newValue any) (objinterface.RootObject, error) {
	return nil, errors.Newf("unknown field name: `%s`", fieldName)
}

// HandleMerge implements the interface objinterface.Collection.
func (*Collection) HandleMerge(ctx context.Context, mro merge.MergeRootObject) (doltdb.RootObject, *merge.MergeStats, error) {
	ours := mro.OurRootObj.(Metadata)
	theirs := mro.TheirRootObj.(Metadata)
	if ours.ID != theirs.ID {
		return nil, nil, errors.Newf("attempted to merge different foreign-key metadata: `%s` and `%s`", ours.Name().String(), theirs.Name().String())
	}
	ourHash, err := ours.HashOf(ctx)
	if err != nil {
		return nil, nil, err
	}
	theirHash, err := theirs.HashOf(ctx)
	if err != nil {
		return nil, nil, err
	}
	if ourHash.Equal(theirHash) {
		return mro.OurRootObj, &merge.MergeStats{Operation: merge.TableUnmodified}, nil
	}
	return pgmerge.CreateConflict(ctx, mro.RightSrc, ours, theirs, mro.AncestorRootObj)
}

// LoadCollection implements the interface objinterface.Collection.
func (*Collection) LoadCollection(ctx context.Context, root objinterface.RootValue) (objinterface.Collection, error) {
	return LoadMetadata(ctx, root)
}

// LoadCollectionHash implements the interface objinterface.Collection.
func (*Collection) LoadCollectionHash(ctx context.Context, root objinterface.RootValue) (hash.Hash, error) {
	m, ok, err := storage.GetProllyMap(ctx, root)
	if err != nil || !ok {
		return hash.Hash{}, err
	}
	return m.HashOf(), nil
}

// LoadMetadata loads the foreign-key metadata collection from the given root.
func LoadMetadata(ctx context.Context, root objinterface.RootValue) (*Collection, error) {
	m, ok, err := storage.GetProllyMap(ctx, root)
	if err != nil {
		return nil, err
	}
	if !ok {
		m, err = prolly.NewEmptyAddressMap(root.NodeStore())
		if err != nil {
			return nil, err
		}
	}
	return NewCollection(ctx, m, root.NodeStore())
}

// ResolveNameFromObjects implements the interface objinterface.Collection.
func (*Collection) ResolveNameFromObjects(ctx context.Context, name doltdb.TableName, rootObjects []objinterface.RootObject) (doltdb.TableName, id.Id, error) {
	target := id.NewForeignKey(name.Schema, "", "").AsId()
	if !target.IsValid() {
		return doltdb.TableName{}, id.Null, nil
	}
	for _, rootObject := range rootObjects {
		if metadata, ok := rootObject.(Metadata); ok && metadata.ID.AsId() == target {
			return metadata.Name(), metadata.ID.AsId(), nil
		}
	}
	return doltdb.TableName{}, id.Null, nil
}

// Serializer implements the interface objinterface.Collection.
func (*Collection) Serializer() objinterface.RootObjectSerializer {
	return storage
}

// UpdateRoot implements the interface objinterface.Collection.
func (pgf *Collection) UpdateRoot(ctx context.Context, root objinterface.RootValue) (objinterface.RootValue, error) {
	m, err := pgf.Map(ctx)
	if err != nil {
		return nil, err
	}
	return storage.WriteProllyMap(ctx, root, m)
}

// GetID implements the interface objinterface.RootObject.
func (metadata Metadata) GetID() id.Id {
	return metadata.ID.AsId()
}

// GetRootObjectID implements the interface objinterface.RootObject.
func (metadata Metadata) GetRootObjectID() objinterface.RootObjectID {
	return objinterface.RootObjectID_ForeignKeyMetadata
}

// HashOf implements the interface objinterface.RootObject.
func (metadata Metadata) HashOf(ctx context.Context) (hash.Hash, error) {
	data, err := metadata.Serialize(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return hash.Of(data), nil
}

// Name implements the interface objinterface.RootObject.
func (metadata Metadata) Name() doltdb.TableName {
	return doltdb.TableName{
		Name:   fmt.Sprintf("%s.%s", metadata.ID.TableName(), metadata.ID.ForeignKeyName()),
		Schema: metadata.ID.SchemaName(),
	}
}

// Clone returns a new *Collection with the same contents as the original.
func (pgf *Collection) Clone(_ context.Context) *Collection {
	return &Collection{
		accessCache:   maps.Clone(pgf.accessCache),
		idCache:       slices.Clone(pgf.idCache),
		mapHash:       pgf.mapHash,
		underlyingMap: pgf.underlyingMap,
		ns:            pgf.ns,
	}
}

// RootValueAddForeignKeyMetadata is referenced through the serializer signature.
var _ func(*flatbuffers.Builder, flatbuffers.UOffsetT) = serial.RootValueAddForeignKeyMetadata
