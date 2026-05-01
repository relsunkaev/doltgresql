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

package publications

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

	"github.com/dolthub/doltgresql/core/id"
	pgmerge "github.com/dolthub/doltgresql/core/merge"
	"github.com/dolthub/doltgresql/core/rootobject/objinterface"
	"github.com/dolthub/doltgresql/flatbuffers/gen/serial"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// PublicationRelation stores a table membership in a publication.
type PublicationRelation struct {
	Table     id.Table
	Columns   []string
	RowFilter string
}

// Publication represents a logical replication publication.
type Publication struct {
	ID                  id.Publication
	AllTables           bool
	Tables              []PublicationRelation
	Schemas             []string
	PublishInsert       bool
	PublishUpdate       bool
	PublishDelete       bool
	PublishTruncate     bool
	PublishViaPartition bool
}

// Collection contains publications in the current database root.
type Collection struct {
	accessCache   map[id.Publication]Publication
	idCache       []id.Publication
	mapHash       hash.Hash
	underlyingMap prolly.AddressMap
	ns            tree.NodeStore
}

var _ objinterface.Collection = (*Collection)(nil)
var _ objinterface.RootObject = Publication{}

var storage = objinterface.RootObjectSerializer{
	Bytes:        (*serial.RootValue).PublicationsBytes,
	RootValueAdd: serial.RootValueAddPublications,
}

// NewCollection returns a new Collection.
func NewCollection(ctx context.Context, underlyingMap prolly.AddressMap, ns tree.NodeStore) (*Collection, error) {
	collection := &Collection{
		accessCache:   make(map[id.Publication]Publication),
		underlyingMap: underlyingMap,
		ns:            ns,
	}
	return collection, collection.reloadCaches(ctx)
}

// NewPublication returns a publication with PostgreSQL-compatible default publish actions.
func NewPublication(name string) Publication {
	return Publication{
		ID:              id.NewPublication(name),
		PublishInsert:   true,
		PublishUpdate:   true,
		PublishDelete:   true,
		PublishTruncate: true,
	}
}

// GetPublication returns the publication with the given ID.
func (pgp *Collection) GetPublication(_ context.Context, pubID id.Publication) (Publication, error) {
	if f, ok := pgp.accessCache[pubID]; ok {
		return f, nil
	}
	return Publication{}, nil
}

// HasPublication returns whether the publication is present.
func (pgp *Collection) HasPublication(_ context.Context, pubID id.Publication) bool {
	_, ok := pgp.accessCache[pubID]
	return ok
}

// AddPublication adds a new publication.
func (pgp *Collection) AddPublication(ctx context.Context, pub Publication) error {
	if _, ok := pgp.accessCache[pub.ID]; ok {
		return errors.Errorf(`publication "%s" already exists`, pub.ID.PublicationName())
	}
	return pgp.putPublication(ctx, pub)
}

// UpdatePublication replaces an existing publication.
func (pgp *Collection) UpdatePublication(ctx context.Context, pub Publication) error {
	if _, ok := pgp.accessCache[pub.ID]; !ok {
		return errors.Errorf(`publication "%s" does not exist`, pub.ID.PublicationName())
	}
	return pgp.putPublication(ctx, pub)
}

func (pgp *Collection) putPublication(ctx context.Context, pub Publication) error {
	pub.normalize()
	data, err := pub.Serialize(ctx)
	if err != nil {
		return err
	}
	h, err := pgp.ns.WriteBytes(ctx, data)
	if err != nil {
		return err
	}
	mapEditor := pgp.underlyingMap.Editor()
	if err = mapEditor.Update(ctx, string(pub.ID), h); err != nil {
		return err
	}
	newMap, err := mapEditor.Flush(ctx)
	if err != nil {
		return err
	}
	pgp.underlyingMap = newMap
	return pgp.reloadCaches(ctx)
}

// DropPublication drops existing publications.
func (pgp *Collection) DropPublication(ctx context.Context, pubIDs ...id.Publication) error {
	for _, pubID := range pubIDs {
		if _, ok := pgp.accessCache[pubID]; !ok {
			return errors.Errorf(`publication "%s" does not exist`, pubID.PublicationName())
		}
	}
	mapEditor := pgp.underlyingMap.Editor()
	for _, pubID := range pubIDs {
		if err := mapEditor.Delete(ctx, string(pubID)); err != nil {
			return err
		}
	}
	newMap, err := mapEditor.Flush(ctx)
	if err != nil {
		return err
	}
	pgp.underlyingMap = newMap
	return pgp.reloadCaches(ctx)
}

// IteratePublications iterates over all publications.
func (pgp *Collection) IteratePublications(_ context.Context, callback func(f Publication) (stop bool, err error)) error {
	for _, pubID := range pgp.idCache {
		stop, err := callback(pgp.accessCache[pubID])
		if err != nil {
			return err
		} else if stop {
			return nil
		}
	}
	return nil
}

// Clone returns a new *Collection with the same contents as the original.
func (pgp *Collection) Clone(_ context.Context) *Collection {
	return &Collection{
		accessCache:   maps.Clone(pgp.accessCache),
		idCache:       slices.Clone(pgp.idCache),
		mapHash:       pgp.mapHash,
		underlyingMap: pgp.underlyingMap,
		ns:            pgp.ns,
	}
}

// Map returns the underlying map.
func (pgp *Collection) Map(_ context.Context) (prolly.AddressMap, error) {
	return pgp.underlyingMap, nil
}

// DiffersFrom returns true when this collection differs from the collection in the given root.
func (pgp *Collection) DiffersFrom(ctx context.Context, root objinterface.RootValue) bool {
	hashOnGivenRoot, err := pgp.LoadCollectionHash(ctx, root)
	if err != nil {
		return true
	}
	if pgp.mapHash.Equal(hashOnGivenRoot) {
		return false
	}
	count, err := pgp.underlyingMap.Count()
	return err != nil || count != 0 || !hashOnGivenRoot.IsEmpty()
}

func (pgp *Collection) reloadCaches(ctx context.Context) error {
	count, err := pgp.underlyingMap.Count()
	if err != nil {
		return err
	}
	clear(pgp.accessCache)
	pgp.mapHash = pgp.underlyingMap.HashOf()
	pgp.idCache = make([]id.Publication, 0, count)
	return pgp.underlyingMap.IterAll(ctx, func(_ string, h hash.Hash) error {
		if h.IsEmpty() {
			return nil
		}
		data, err := pgp.ns.ReadBytes(ctx, h)
		if err != nil {
			return err
		}
		pub, err := DeserializePublication(ctx, data)
		if err != nil {
			return err
		}
		pgp.accessCache[pub.ID] = pub
		pgp.idCache = append(pgp.idCache, pub.ID)
		return nil
	})
}

// Serialize returns the Publication as a byte slice.
func (publication Publication) Serialize(ctx context.Context) ([]byte, error) {
	if !publication.ID.IsValid() {
		return nil, nil
	}
	publication.normalize()
	writer := utils.NewWriter(256)
	writer.VariableUint(0)
	writer.Id(publication.ID.AsId())
	writer.Bool(publication.AllTables)
	writer.Bool(publication.PublishInsert)
	writer.Bool(publication.PublishUpdate)
	writer.Bool(publication.PublishDelete)
	writer.Bool(publication.PublishTruncate)
	writer.Bool(publication.PublishViaPartition)
	writer.VariableUint(uint64(len(publication.Tables)))
	for _, table := range publication.Tables {
		writer.Id(table.Table.AsId())
		writer.StringSlice(table.Columns)
		writer.String(table.RowFilter)
	}
	writer.StringSlice(publication.Schemas)
	return writer.Data(), nil
}

// DeserializePublication returns the Publication serialized in the byte slice.
func DeserializePublication(ctx context.Context, data []byte) (Publication, error) {
	if len(data) == 0 {
		return Publication{}, nil
	}
	reader := utils.NewReader(data)
	version := reader.VariableUint()
	if version > 0 {
		return Publication{}, errors.Errorf("version %d of publications is not supported, please upgrade the server", version)
	}
	pub := Publication{}
	pub.ID = id.Publication(reader.Id())
	pub.AllTables = reader.Bool()
	pub.PublishInsert = reader.Bool()
	pub.PublishUpdate = reader.Bool()
	pub.PublishDelete = reader.Bool()
	pub.PublishTruncate = reader.Bool()
	pub.PublishViaPartition = reader.Bool()
	tableCount := reader.VariableUint()
	pub.Tables = make([]PublicationRelation, tableCount)
	for i := uint64(0); i < tableCount; i++ {
		pub.Tables[i].Table = id.Table(reader.Id())
		pub.Tables[i].Columns = reader.StringSlice()
		pub.Tables[i].RowFilter = reader.String()
	}
	pub.Schemas = reader.StringSlice()
	if !reader.IsEmpty() {
		return Publication{}, errors.New("extra data found while deserializing a publication")
	}
	pub.normalize()
	return pub, nil
}

func (publication *Publication) normalize() {
	slices.SortFunc(publication.Tables, func(a, b PublicationRelation) int {
		return strings.Compare(string(a.Table), string(b.Table))
	})
	for i := range publication.Tables {
		publication.Tables[i].Columns = compactStringsPreservingOrder(publication.Tables[i].Columns)
	}
	slices.Sort(publication.Schemas)
	publication.Schemas = slices.Compact(publication.Schemas)
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

func (publication Publication) summary() string {
	publication.normalize()
	return fmt.Sprintf("%s all=%t tables=%v schemas=%v publish=%t/%t/%t/%t viaRoot=%t",
		publication.ID.PublicationName(), publication.AllTables, publication.Tables, publication.Schemas,
		publication.PublishInsert, publication.PublishUpdate, publication.PublishDelete, publication.PublishTruncate,
		publication.PublishViaPartition)
}

// DeserializeRootObject implements the interface objinterface.Collection.
func (pgp *Collection) DeserializeRootObject(ctx context.Context, data []byte) (objinterface.RootObject, error) {
	return DeserializePublication(ctx, data)
}

// DiffRootObjects implements the interface objinterface.Collection.
func (pgp *Collection) DiffRootObjects(ctx context.Context, fromHash string, o objinterface.RootObject, t objinterface.RootObject, a objinterface.RootObject) ([]objinterface.RootObjectDiff, objinterface.RootObject, error) {
	ours := o.(Publication)
	theirs := t.(Publication)
	if ours.summary() == theirs.summary() {
		return nil, ours, nil
	}
	var ancestor any
	if anc, ok := a.(Publication); ok {
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
func (pgp *Collection) DropRootObject(ctx context.Context, identifier id.Id) error {
	if identifier.Section() != id.Section_Publication {
		return errors.Errorf(`publication %s does not exist`, identifier.String())
	}
	return pgp.DropPublication(ctx, id.Publication(identifier))
}

// GetFieldType implements the interface objinterface.Collection.
func (pgp *Collection) GetFieldType(ctx context.Context, fieldName string) *pgtypes.DoltgresType {
	return nil
}

// GetID implements the interface objinterface.Collection.
func (pgp *Collection) GetID() objinterface.RootObjectID {
	return objinterface.RootObjectID_Publications
}

// GetRootObject implements the interface objinterface.Collection.
func (pgp *Collection) GetRootObject(ctx context.Context, identifier id.Id) (objinterface.RootObject, bool, error) {
	if identifier.Section() != id.Section_Publication {
		return nil, false, nil
	}
	f, err := pgp.GetPublication(ctx, id.Publication(identifier))
	return f, err == nil && f.ID.IsValid(), err
}

// HasRootObject implements the interface objinterface.Collection.
func (pgp *Collection) HasRootObject(ctx context.Context, identifier id.Id) (bool, error) {
	if identifier.Section() != id.Section_Publication {
		return false, nil
	}
	return pgp.HasPublication(ctx, id.Publication(identifier)), nil
}

// IDToTableName implements the interface objinterface.Collection.
func (pgp *Collection) IDToTableName(identifier id.Id) doltdb.TableName {
	if identifier.Section() != id.Section_Publication {
		return doltdb.TableName{}
	}
	return doltdb.TableName{Name: id.Publication(identifier).PublicationName()}
}

// IterAll implements the interface objinterface.Collection.
func (pgp *Collection) IterAll(ctx context.Context, callback func(rootObj objinterface.RootObject) (stop bool, err error)) error {
	return pgp.IteratePublications(ctx, func(f Publication) (stop bool, err error) {
		return callback(f)
	})
}

// IterIDs implements the interface objinterface.Collection.
func (pgp *Collection) IterIDs(ctx context.Context, callback func(identifier id.Id) (stop bool, err error)) error {
	for _, pubID := range pgp.idCache {
		stop, err := callback(pubID.AsId())
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// PutRootObject implements the interface objinterface.Collection.
func (pgp *Collection) PutRootObject(ctx context.Context, rootObj objinterface.RootObject) error {
	f, ok := rootObj.(Publication)
	if !ok {
		return errors.Newf("invalid publication root object: %T", rootObj)
	}
	return pgp.AddPublication(ctx, f)
}

// RenameRootObject implements the interface objinterface.Collection.
func (pgp *Collection) RenameRootObject(ctx context.Context, oldName id.Id, newName id.Id) error {
	if oldName.Section() != id.Section_Publication || newName.Section() != id.Section_Publication {
		return errors.New("cannot rename publication due to invalid name")
	}
	pub, err := pgp.GetPublication(ctx, id.Publication(oldName))
	if err != nil {
		return err
	}
	if !pub.ID.IsValid() {
		return errors.Errorf(`publication "%s" does not exist`, id.Publication(oldName).PublicationName())
	}
	if err = pgp.DropPublication(ctx, id.Publication(oldName)); err != nil {
		return err
	}
	pub.ID = id.Publication(newName)
	return pgp.AddPublication(ctx, pub)
}

// ResolveName implements the interface objinterface.Collection.
func (pgp *Collection) ResolveName(ctx context.Context, name doltdb.TableName) (doltdb.TableName, id.Id, error) {
	for _, pubID := range pgp.idCache {
		if strings.EqualFold(pubID.PublicationName(), name.Name) {
			return doltdb.TableName{Name: pubID.PublicationName()}, pubID.AsId(), nil
		}
	}
	return doltdb.TableName{}, id.Null, nil
}

// TableNameToID implements the interface objinterface.Collection.
func (pgp *Collection) TableNameToID(name doltdb.TableName) id.Id {
	return id.NewPublication(name.Name).AsId()
}

// UpdateField implements the interface objinterface.Collection.
func (pgp *Collection) UpdateField(ctx context.Context, rootObject objinterface.RootObject, fieldName string, newValue any) (objinterface.RootObject, error) {
	return nil, errors.Newf("unknown field name: `%s`", fieldName)
}

// HandleMerge implements the interface objinterface.Collection.
func (*Collection) HandleMerge(ctx context.Context, mro merge.MergeRootObject) (doltdb.RootObject, *merge.MergeStats, error) {
	ourPub := mro.OurRootObj.(Publication)
	theirPub := mro.TheirRootObj.(Publication)
	if ourPub.ID != theirPub.ID {
		return nil, nil, errors.Newf("attempted to merge different publications: `%s` and `%s`", ourPub.Name().String(), theirPub.Name().String())
	}
	ourHash, err := ourPub.HashOf(ctx)
	if err != nil {
		return nil, nil, err
	}
	theirHash, err := theirPub.HashOf(ctx)
	if err != nil {
		return nil, nil, err
	}
	if ourHash.Equal(theirHash) {
		return mro.OurRootObj, &merge.MergeStats{Operation: merge.TableUnmodified}, nil
	}
	return pgmerge.CreateConflict(ctx, mro.RightSrc, ourPub, theirPub, mro.AncestorRootObj)
}

// LoadCollection implements the interface objinterface.Collection.
func (*Collection) LoadCollection(ctx context.Context, root objinterface.RootValue) (objinterface.Collection, error) {
	return LoadPublications(ctx, root)
}

// LoadCollectionHash implements the interface objinterface.Collection.
func (*Collection) LoadCollectionHash(ctx context.Context, root objinterface.RootValue) (hash.Hash, error) {
	m, ok, err := storage.GetProllyMap(ctx, root)
	if err != nil || !ok {
		return hash.Hash{}, err
	}
	return m.HashOf(), nil
}

// LoadPublications loads the publications collection from the given root.
func LoadPublications(ctx context.Context, root objinterface.RootValue) (*Collection, error) {
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
	for _, rootObject := range rootObjects {
		if obj, ok := rootObject.(Publication); ok && strings.EqualFold(obj.ID.PublicationName(), name.Name) {
			return obj.Name(), obj.ID.AsId(), nil
		}
	}
	return doltdb.TableName{}, id.Null, nil
}

// Serializer implements the interface objinterface.Collection.
func (*Collection) Serializer() objinterface.RootObjectSerializer {
	return storage
}

// UpdateRoot implements the interface objinterface.Collection.
func (pgp *Collection) UpdateRoot(ctx context.Context, root objinterface.RootValue) (objinterface.RootValue, error) {
	m, err := pgp.Map(ctx)
	if err != nil {
		return nil, err
	}
	return storage.WriteProllyMap(ctx, root, m)
}

// GetID implements the interface objinterface.RootObject.
func (publication Publication) GetID() id.Id {
	return publication.ID.AsId()
}

// GetRootObjectID implements the interface objinterface.RootObject.
func (publication Publication) GetRootObjectID() objinterface.RootObjectID {
	return objinterface.RootObjectID_Publications
}

// HashOf implements the interface objinterface.RootObject.
func (publication Publication) HashOf(ctx context.Context) (hash.Hash, error) {
	data, err := publication.Serialize(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return hash.Of(data), nil
}

// Name implements the interface objinterface.RootObject.
func (publication Publication) Name() doltdb.TableName {
	return doltdb.TableName{Name: publication.ID.PublicationName()}
}

func (publication Publication) String() string {
	return publication.Name().String()
}

// RootValueAddPublications is referenced through the serializer signature.
var _ func(*flatbuffers.Builder, flatbuffers.UOffsetT) = serial.RootValueAddPublications
