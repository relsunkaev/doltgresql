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

package subscriptions

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

// Subscription represents logical replication subscription metadata.
type Subscription struct {
	ID             id.Subscription
	Owner          id.Id
	SkipLSN        string
	Enabled        bool
	Binary         bool
	Stream         bool
	TwoPhaseState  string
	DisableOnError bool
	ConnInfo       string
	SlotName       string
	SyncCommit     string
	Publications   []string
}

// Collection contains subscriptions in the current database root.
type Collection struct {
	accessCache   map[id.Subscription]Subscription
	idCache       []id.Subscription
	mapHash       hash.Hash
	underlyingMap prolly.AddressMap
	ns            tree.NodeStore
}

var _ objinterface.Collection = (*Collection)(nil)
var _ objinterface.RootObject = Subscription{}

var storage = objinterface.RootObjectSerializer{
	Bytes:        (*serial.RootValue).SubscriptionsBytes,
	RootValueAdd: serial.RootValueAddSubscriptions,
}

// NewCollection returns a new Collection.
func NewCollection(ctx context.Context, underlyingMap prolly.AddressMap, ns tree.NodeStore) (*Collection, error) {
	collection := &Collection{
		accessCache:   make(map[id.Subscription]Subscription),
		underlyingMap: underlyingMap,
		ns:            ns,
	}
	return collection, collection.reloadCaches(ctx)
}

// NewSubscription returns a subscription with PostgreSQL-compatible metadata defaults.
func NewSubscription(name string) Subscription {
	return Subscription{
		ID:            id.NewSubscription(name),
		Owner:         defaultSubscriptionOwner(),
		SkipLSN:       "0/0",
		TwoPhaseState: "d",
		SyncCommit:    "off",
	}
}

func defaultSubscriptionOwner() id.Id {
	return id.NewOID(10).AsId()
}

// GetSubscription returns the subscription with the given ID.
func (pgs *Collection) GetSubscription(_ context.Context, subID id.Subscription) (Subscription, error) {
	if f, ok := pgs.accessCache[subID]; ok {
		return f, nil
	}
	return Subscription{}, nil
}

// HasSubscription returns whether the subscription is present.
func (pgs *Collection) HasSubscription(_ context.Context, subID id.Subscription) bool {
	_, ok := pgs.accessCache[subID]
	return ok
}

// AddSubscription adds a new subscription.
func (pgs *Collection) AddSubscription(ctx context.Context, sub Subscription) error {
	if _, ok := pgs.accessCache[sub.ID]; ok {
		return errors.Errorf(`subscription "%s" already exists`, sub.ID.SubscriptionName())
	}
	return pgs.putSubscription(ctx, sub)
}

// UpdateSubscription replaces an existing subscription.
func (pgs *Collection) UpdateSubscription(ctx context.Context, sub Subscription) error {
	if _, ok := pgs.accessCache[sub.ID]; !ok {
		return errors.Errorf(`subscription "%s" does not exist`, sub.ID.SubscriptionName())
	}
	return pgs.putSubscription(ctx, sub)
}

func (pgs *Collection) putSubscription(ctx context.Context, sub Subscription) error {
	sub.normalize()
	data, err := sub.Serialize(ctx)
	if err != nil {
		return err
	}
	h, err := pgs.ns.WriteBytes(ctx, data)
	if err != nil {
		return err
	}
	mapEditor := pgs.underlyingMap.Editor()
	if err = mapEditor.Update(ctx, string(sub.ID), h); err != nil {
		return err
	}
	newMap, err := mapEditor.Flush(ctx)
	if err != nil {
		return err
	}
	pgs.underlyingMap = newMap
	return pgs.reloadCaches(ctx)
}

// DropSubscription drops existing subscriptions.
func (pgs *Collection) DropSubscription(ctx context.Context, subIDs ...id.Subscription) error {
	for _, subID := range subIDs {
		if _, ok := pgs.accessCache[subID]; !ok {
			return errors.Errorf(`subscription "%s" does not exist`, subID.SubscriptionName())
		}
	}
	mapEditor := pgs.underlyingMap.Editor()
	for _, subID := range subIDs {
		if err := mapEditor.Delete(ctx, string(subID)); err != nil {
			return err
		}
	}
	newMap, err := mapEditor.Flush(ctx)
	if err != nil {
		return err
	}
	pgs.underlyingMap = newMap
	return pgs.reloadCaches(ctx)
}

// IterateSubscriptions iterates over all subscriptions.
func (pgs *Collection) IterateSubscriptions(_ context.Context, callback func(f Subscription) (stop bool, err error)) error {
	for _, subID := range pgs.idCache {
		stop, err := callback(pgs.accessCache[subID])
		if err != nil {
			return err
		} else if stop {
			return nil
		}
	}
	return nil
}

// Clone returns a new *Collection with the same contents as the original.
func (pgs *Collection) Clone(_ context.Context) *Collection {
	return &Collection{
		accessCache:   maps.Clone(pgs.accessCache),
		idCache:       slices.Clone(pgs.idCache),
		mapHash:       pgs.mapHash,
		underlyingMap: pgs.underlyingMap,
		ns:            pgs.ns,
	}
}

// Map returns the underlying map.
func (pgs *Collection) Map(_ context.Context) (prolly.AddressMap, error) {
	return pgs.underlyingMap, nil
}

// DiffersFrom returns true when this collection differs from the collection in the given root.
func (pgs *Collection) DiffersFrom(ctx context.Context, root objinterface.RootValue) bool {
	hashOnGivenRoot, err := pgs.LoadCollectionHash(ctx, root)
	if err != nil {
		return true
	}
	if pgs.mapHash.Equal(hashOnGivenRoot) {
		return false
	}
	count, err := pgs.underlyingMap.Count()
	return err != nil || count != 0 || !hashOnGivenRoot.IsEmpty()
}

func (pgs *Collection) reloadCaches(ctx context.Context) error {
	count, err := pgs.underlyingMap.Count()
	if err != nil {
		return err
	}
	clear(pgs.accessCache)
	pgs.mapHash = pgs.underlyingMap.HashOf()
	pgs.idCache = make([]id.Subscription, 0, count)
	return pgs.underlyingMap.IterAll(ctx, func(_ string, h hash.Hash) error {
		if h.IsEmpty() {
			return nil
		}
		data, err := pgs.ns.ReadBytes(ctx, h)
		if err != nil {
			return err
		}
		sub, err := DeserializeSubscription(ctx, data)
		if err != nil {
			return err
		}
		pgs.accessCache[sub.ID] = sub
		pgs.idCache = append(pgs.idCache, sub.ID)
		return nil
	})
}

// Serialize returns the Subscription as a byte slice.
func (subscription Subscription) Serialize(ctx context.Context) ([]byte, error) {
	if !subscription.ID.IsValid() {
		return nil, nil
	}
	subscription.normalize()
	writer := utils.NewWriter(256)
	writer.VariableUint(1)
	writer.Id(subscription.ID.AsId())
	writer.Id(subscription.Owner)
	writer.String(subscription.SkipLSN)
	writer.Bool(subscription.Enabled)
	writer.Bool(subscription.Binary)
	writer.Bool(subscription.Stream)
	writer.String(subscription.TwoPhaseState)
	writer.Bool(subscription.DisableOnError)
	writer.String(subscription.ConnInfo)
	writer.String(subscription.SlotName)
	writer.String(subscription.SyncCommit)
	writer.StringSlice(subscription.Publications)
	return writer.Data(), nil
}

// DeserializeSubscription returns the Subscription serialized in the byte slice.
func DeserializeSubscription(ctx context.Context, data []byte) (Subscription, error) {
	if len(data) == 0 {
		return Subscription{}, nil
	}
	reader := utils.NewReader(data)
	version := reader.VariableUint()
	if version > 1 {
		return Subscription{}, errors.Errorf("version %d of subscriptions is not supported, please upgrade the server", version)
	}
	sub := Subscription{}
	sub.ID = id.Subscription(reader.Id())
	if version >= 1 {
		sub.Owner = reader.Id()
	} else {
		sub.Owner = defaultSubscriptionOwner()
	}
	sub.SkipLSN = reader.String()
	sub.Enabled = reader.Bool()
	sub.Binary = reader.Bool()
	sub.Stream = reader.Bool()
	sub.TwoPhaseState = reader.String()
	sub.DisableOnError = reader.Bool()
	sub.ConnInfo = reader.String()
	sub.SlotName = reader.String()
	sub.SyncCommit = reader.String()
	sub.Publications = reader.StringSlice()
	if !reader.IsEmpty() {
		return Subscription{}, errors.New("extra data found while deserializing a subscription")
	}
	sub.normalize()
	return sub, nil
}

func (subscription *Subscription) normalize() {
	if !subscription.Owner.IsValid() {
		subscription.Owner = defaultSubscriptionOwner()
	}
	if subscription.SkipLSN == "" {
		subscription.SkipLSN = "0/0"
	}
	if subscription.TwoPhaseState == "" {
		subscription.TwoPhaseState = "d"
	}
	if subscription.SyncCommit == "" {
		subscription.SyncCommit = "off"
	}
	subscription.Publications = compactStringsPreservingOrder(subscription.Publications)
}

func compactStringsPreservingOrder(values []string) []string {
	if len(values) < 2 {
		return values
	}
	seen := make(map[string]struct{}, len(values))
	out := values[:0]
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (subscription Subscription) summary() string {
	subscription.normalize()
	return fmt.Sprintf("%s owner=%s enabled=%t binary=%t stream=%t twophase=%s conn=%s slot=%s pubs=%v",
		subscription.ID.SubscriptionName(), subscription.Owner.String(), subscription.Enabled, subscription.Binary, subscription.Stream,
		subscription.TwoPhaseState, subscription.ConnInfo, subscription.SlotName, subscription.Publications)
}

// DeserializeRootObject implements the interface objinterface.Collection.
func (pgs *Collection) DeserializeRootObject(ctx context.Context, data []byte) (objinterface.RootObject, error) {
	return DeserializeSubscription(ctx, data)
}

// DiffRootObjects implements the interface objinterface.Collection.
func (pgs *Collection) DiffRootObjects(ctx context.Context, fromHash string, o objinterface.RootObject, t objinterface.RootObject, a objinterface.RootObject) ([]objinterface.RootObjectDiff, objinterface.RootObject, error) {
	ours := o.(Subscription)
	theirs := t.(Subscription)
	if ours.summary() == theirs.summary() {
		return nil, ours, nil
	}
	var ancestor any
	if anc, ok := a.(Subscription); ok {
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
func (pgs *Collection) DropRootObject(ctx context.Context, identifier id.Id) error {
	if identifier.Section() != id.Section_Subscription {
		return errors.Errorf(`subscription %s does not exist`, identifier.String())
	}
	return pgs.DropSubscription(ctx, id.Subscription(identifier))
}

// GetFieldType implements the interface objinterface.Collection.
func (pgs *Collection) GetFieldType(ctx context.Context, fieldName string) *pgtypes.DoltgresType {
	return nil
}

// GetID implements the interface objinterface.Collection.
func (pgs *Collection) GetID() objinterface.RootObjectID {
	return objinterface.RootObjectID_Subscriptions
}

// GetRootObject implements the interface objinterface.Collection.
func (pgs *Collection) GetRootObject(ctx context.Context, identifier id.Id) (objinterface.RootObject, bool, error) {
	if identifier.Section() != id.Section_Subscription {
		return nil, false, nil
	}
	f, err := pgs.GetSubscription(ctx, id.Subscription(identifier))
	return f, err == nil && f.ID.IsValid(), err
}

// HasRootObject implements the interface objinterface.Collection.
func (pgs *Collection) HasRootObject(ctx context.Context, identifier id.Id) (bool, error) {
	if identifier.Section() != id.Section_Subscription {
		return false, nil
	}
	return pgs.HasSubscription(ctx, id.Subscription(identifier)), nil
}

// IDToTableName implements the interface objinterface.Collection.
func (pgs *Collection) IDToTableName(identifier id.Id) doltdb.TableName {
	if identifier.Section() != id.Section_Subscription {
		return doltdb.TableName{}
	}
	return doltdb.TableName{Name: id.Subscription(identifier).SubscriptionName()}
}

// IterAll implements the interface objinterface.Collection.
func (pgs *Collection) IterAll(ctx context.Context, callback func(rootObj objinterface.RootObject) (stop bool, err error)) error {
	return pgs.IterateSubscriptions(ctx, func(f Subscription) (stop bool, err error) {
		return callback(f)
	})
}

// IterIDs implements the interface objinterface.Collection.
func (pgs *Collection) IterIDs(ctx context.Context, callback func(identifier id.Id) (stop bool, err error)) error {
	for _, subID := range pgs.idCache {
		stop, err := callback(subID.AsId())
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// PutRootObject implements the interface objinterface.Collection.
func (pgs *Collection) PutRootObject(ctx context.Context, rootObj objinterface.RootObject) error {
	f, ok := rootObj.(Subscription)
	if !ok {
		return errors.Newf("invalid subscription root object: %T", rootObj)
	}
	return pgs.AddSubscription(ctx, f)
}

// RenameRootObject implements the interface objinterface.Collection.
func (pgs *Collection) RenameRootObject(ctx context.Context, oldName id.Id, newName id.Id) error {
	if oldName.Section() != id.Section_Subscription || newName.Section() != id.Section_Subscription {
		return errors.New("cannot rename subscription due to invalid name")
	}
	sub, err := pgs.GetSubscription(ctx, id.Subscription(oldName))
	if err != nil {
		return err
	}
	if !sub.ID.IsValid() {
		return errors.Errorf(`subscription "%s" does not exist`, id.Subscription(oldName).SubscriptionName())
	}
	if err = pgs.DropSubscription(ctx, id.Subscription(oldName)); err != nil {
		return err
	}
	sub.ID = id.Subscription(newName)
	return pgs.AddSubscription(ctx, sub)
}

// ResolveName implements the interface objinterface.Collection.
func (pgs *Collection) ResolveName(ctx context.Context, name doltdb.TableName) (doltdb.TableName, id.Id, error) {
	for _, subID := range pgs.idCache {
		if strings.EqualFold(subID.SubscriptionName(), name.Name) {
			return doltdb.TableName{Name: subID.SubscriptionName()}, subID.AsId(), nil
		}
	}
	return doltdb.TableName{}, id.Null, nil
}

// TableNameToID implements the interface objinterface.Collection.
func (pgs *Collection) TableNameToID(name doltdb.TableName) id.Id {
	return id.NewSubscription(name.Name).AsId()
}

// UpdateField implements the interface objinterface.Collection.
func (pgs *Collection) UpdateField(ctx context.Context, rootObject objinterface.RootObject, fieldName string, newValue any) (objinterface.RootObject, error) {
	return nil, errors.Newf("unknown field name: `%s`", fieldName)
}

// HandleMerge implements the interface objinterface.Collection.
func (*Collection) HandleMerge(ctx context.Context, mro merge.MergeRootObject) (doltdb.RootObject, *merge.MergeStats, error) {
	ourSub := mro.OurRootObj.(Subscription)
	theirSub := mro.TheirRootObj.(Subscription)
	if ourSub.ID != theirSub.ID {
		return nil, nil, errors.Newf("attempted to merge different subscriptions: `%s` and `%s`", ourSub.Name().String(), theirSub.Name().String())
	}
	ourHash, err := ourSub.HashOf(ctx)
	if err != nil {
		return nil, nil, err
	}
	theirHash, err := theirSub.HashOf(ctx)
	if err != nil {
		return nil, nil, err
	}
	if ourHash.Equal(theirHash) {
		return mro.OurRootObj, &merge.MergeStats{Operation: merge.TableUnmodified}, nil
	}
	return pgmerge.CreateConflict(ctx, mro.RightSrc, ourSub, theirSub, mro.AncestorRootObj)
}

// LoadCollection implements the interface objinterface.Collection.
func (*Collection) LoadCollection(ctx context.Context, root objinterface.RootValue) (objinterface.Collection, error) {
	return LoadSubscriptions(ctx, root)
}

// LoadCollectionHash implements the interface objinterface.Collection.
func (*Collection) LoadCollectionHash(ctx context.Context, root objinterface.RootValue) (hash.Hash, error) {
	m, ok, err := storage.GetProllyMap(ctx, root)
	if err != nil || !ok {
		return hash.Hash{}, err
	}
	return m.HashOf(), nil
}

// LoadSubscriptions loads the subscriptions collection from the given root.
func LoadSubscriptions(ctx context.Context, root objinterface.RootValue) (*Collection, error) {
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
		if obj, ok := rootObject.(Subscription); ok && strings.EqualFold(obj.ID.SubscriptionName(), name.Name) {
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
func (pgs *Collection) UpdateRoot(ctx context.Context, root objinterface.RootValue) (objinterface.RootValue, error) {
	m, err := pgs.Map(ctx)
	if err != nil {
		return nil, err
	}
	return storage.WriteProllyMap(ctx, root, m)
}

// GetID implements the interface objinterface.RootObject.
func (subscription Subscription) GetID() id.Id {
	return subscription.ID.AsId()
}

// GetRootObjectID implements the interface objinterface.RootObject.
func (subscription Subscription) GetRootObjectID() objinterface.RootObjectID {
	return objinterface.RootObjectID_Subscriptions
}

// HashOf implements the interface objinterface.RootObject.
func (subscription Subscription) HashOf(ctx context.Context) (hash.Hash, error) {
	data, err := subscription.Serialize(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return hash.Of(data), nil
}

// Name implements the interface objinterface.RootObject.
func (subscription Subscription) Name() doltdb.TableName {
	return doltdb.TableName{Name: subscription.ID.SubscriptionName()}
}

func (subscription Subscription) String() string {
	return subscription.Name().String()
}

// RootValueAddSubscriptions is referenced through the serializer signature.
var _ func(*flatbuffers.Builder, flatbuffers.UOffsetT) = serial.RootValueAddSubscriptions
