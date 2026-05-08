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

package deltameta

import (
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/store/hash"
)

// wireDelta is the on-the-wire shape of a Delta. Hashes are hex strings so
// the encoding is human-inspectable and easy to diff. Field tags drive JSON
// key naming and presence-elision for omittable fields.
type wireDelta struct {
	Format       uint16            `json:"format"`
	BaseRoot     string            `json:"baseRoot"`
	TargetRef    string            `json:"targetRef"`
	TargetCommit string            `json:"targetCommit"`
	Tables       []wireTableDelta  `json:"tables"`
	Provenance   map[string]string `json:"provenance,omitempty"`
}

type wireTableDelta struct {
	Name string           `json:"name"`
	Rows []wireRowChange  `json:"rows"`
}

type wireRowChange struct {
	PrimaryKey     string   `json:"primaryKey"`
	OldRowHash     string   `json:"oldRowHash,omitempty"`
	NewRowHash     string   `json:"newRowHash,omitempty"`
	ChangedScalars []string `json:"changedScalars,omitempty"`
	TouchedComplex []string `json:"touchedComplex,omitempty"`
}

// Encode returns the canonical byte form of a Delta. Encoding is deterministic:
// tables are sorted by name, rows by primary-key bytes, and column-name
// slices by lex order. Provenance and other map fields rely on Go's
// json.Marshal sorting map keys.
//
// The caller may pass an unsorted Delta; canonicalization happens here.
func Encode(d Delta) ([]byte, error) {
	if err := Validate(d); err != nil {
		return nil, err
	}
	w, err := toWire(d)
	if err != nil {
		return nil, err
	}
	return json.Marshal(w)
}

// Decode parses canonical bytes back into a Delta. It rejects unknown format
// versions, malformed JSON, malformed hash strings, and any payload that
// would fail Validate.
func Decode(data []byte) (Delta, error) {
	if len(data) == 0 {
		return Delta{}, errors.New("deltameta: empty payload")
	}
	var w wireDelta
	if err := json.Unmarshal(data, &w); err != nil {
		return Delta{}, errors.Wrap(err, "deltameta: malformed payload")
	}
	d, err := fromWire(w)
	if err != nil {
		return Delta{}, err
	}
	if err := Validate(d); err != nil {
		return Delta{}, err
	}
	return d, nil
}

// IsBoundTo reports whether the encoded delta declares the given base root
// and target commit. The fast-path driver calls this with the values it
// independently expects from the branch ref state; a mismatch is the signal
// that the delta is stale and the merge falls back.
//
// Returns false on any decode error so callers can safely use this as a
// predicate (the merge will then decline with declined_missing_delta_metadata
// per architecture).
func IsBoundTo(encoded []byte, baseRoot, targetCommit hash.Hash) bool {
	d, err := Decode(encoded)
	if err != nil {
		return false
	}
	return d.BaseRoot == baseRoot && d.TargetCommit == targetCommit
}

// Validate runs structural checks that must hold for any well-formed delta.
// Producers should call this before persisting a delta; consumers (the
// merge driver) call Decode which calls Validate.
func Validate(d Delta) error {
	if d.Format != FormatVersion1 {
		return errors.Newf("deltameta: unknown format version %d", d.Format)
	}
	if d.BaseRoot.IsEmpty() {
		return errors.New("deltameta: base root is required")
	}
	if d.TargetCommit.IsEmpty() {
		return errors.New("deltameta: target commit is required")
	}
	if strings.TrimSpace(d.TargetRef) == "" {
		return errors.New("deltameta: target ref is required")
	}
	if len(d.Tables) == 0 {
		return errors.New("deltameta: at least one affected table is required")
	}
	for ti, t := range d.Tables {
		if strings.TrimSpace(t.Name) == "" {
			return errors.Newf("deltameta: table[%d] has empty name", ti)
		}
		seenPK := make(map[string]struct{}, len(t.Rows))
		for ri, r := range t.Rows {
			if len(r.PrimaryKey) == 0 {
				return errors.Newf("deltameta: %s row[%d] has empty primary key", t.Name, ri)
			}
			pkKey := string(r.PrimaryKey)
			if _, ok := seenPK[pkKey]; ok {
				return errors.Newf("deltameta: %s row[%d] duplicates primary key", t.Name, ri)
			}
			seenPK[pkKey] = struct{}{}

			kind := r.Kind()
			if kind == RowKindInvalid {
				return errors.Newf("deltameta: %s row[%d] missing both old and new row hash", t.Name, ri)
			}
			if kind == RowKindUpdate && r.OldRowHash != nil && r.NewRowHash != nil && *r.OldRowHash == *r.NewRowHash {
				return errors.Newf("deltameta: %s row[%d] has identical old and new row hash", t.Name, ri)
			}
			if (kind == RowKindInsert || kind == RowKindDelete) && len(r.ChangedScalars) > 0 {
				return errors.Newf("deltameta: %s row[%d] is %s but lists changed scalars", t.Name, ri, kind)
			}
			if err := validateColumnLists(t.Name, ri, r); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateColumnLists(table string, rowIdx int, r RowChange) error {
	scalarSet := make(map[string]struct{}, len(r.ChangedScalars))
	for _, c := range r.ChangedScalars {
		c = strings.TrimSpace(c)
		if c == "" {
			return errors.Newf("deltameta: %s row[%d] has empty changed scalar name", table, rowIdx)
		}
		if _, ok := scalarSet[c]; ok {
			return errors.Newf("deltameta: %s row[%d] duplicates changed scalar %q", table, rowIdx, c)
		}
		scalarSet[c] = struct{}{}
	}
	complexSet := make(map[string]struct{}, len(r.TouchedComplex))
	for _, c := range r.TouchedComplex {
		c = strings.TrimSpace(c)
		if c == "" {
			return errors.Newf("deltameta: %s row[%d] has empty touched complex name", table, rowIdx)
		}
		if _, ok := complexSet[c]; ok {
			return errors.Newf("deltameta: %s row[%d] duplicates touched complex %q", table, rowIdx, c)
		}
		if _, ok := scalarSet[c]; ok {
			return errors.Newf("deltameta: %s row[%d] column %q listed as both scalar and complex", table, rowIdx, c)
		}
		complexSet[c] = struct{}{}
	}
	return nil
}

func toWire(d Delta) (wireDelta, error) {
	tables := make([]wireTableDelta, 0, len(d.Tables))
	for _, t := range d.Tables {
		rows := make([]wireRowChange, 0, len(t.Rows))
		for _, r := range t.Rows {
			rows = append(rows, wireRowChange{
				PrimaryKey:     hex.EncodeToString(r.PrimaryKey),
				OldRowHash:     hashHex(r.OldRowHash),
				NewRowHash:     hashHex(r.NewRowHash),
				ChangedScalars: sortedCopy(r.ChangedScalars),
				TouchedComplex: sortedCopy(r.TouchedComplex),
			})
		}
		// Sort rows by their hex-encoded primary key, which is byte-order
		// equivalent to sorting by the raw bytes (hex is stable, lex-sortable).
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].PrimaryKey < rows[j].PrimaryKey })
		tables = append(tables, wireTableDelta{Name: t.Name, Rows: rows})
	}
	sort.SliceStable(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return wireDelta{
		Format:       d.Format,
		BaseRoot:     d.BaseRoot.String(),
		TargetRef:    d.TargetRef,
		TargetCommit: d.TargetCommit.String(),
		Tables:       tables,
		Provenance:   sortedCopyMap(d.Provenance),
	}, nil
}

func fromWire(w wireDelta) (Delta, error) {
	baseRoot, ok := hash.MaybeParse(w.BaseRoot)
	if !ok {
		return Delta{}, errors.Newf("deltameta: invalid base root %q", w.BaseRoot)
	}
	targetCommit, ok := hash.MaybeParse(w.TargetCommit)
	if !ok {
		return Delta{}, errors.Newf("deltameta: invalid target commit %q", w.TargetCommit)
	}
	tables := make([]TableDelta, 0, len(w.Tables))
	for _, wt := range w.Tables {
		rows := make([]RowChange, 0, len(wt.Rows))
		for _, wr := range wt.Rows {
			pk, err := hex.DecodeString(wr.PrimaryKey)
			if err != nil {
				return Delta{}, errors.Wrapf(err, "deltameta: %s row primary key %q", wt.Name, wr.PrimaryKey)
			}
			oldHash, err := optHash(wr.OldRowHash)
			if err != nil {
				return Delta{}, errors.Wrapf(err, "deltameta: %s row old hash", wt.Name)
			}
			newHash, err := optHash(wr.NewRowHash)
			if err != nil {
				return Delta{}, errors.Wrapf(err, "deltameta: %s row new hash", wt.Name)
			}
			rows = append(rows, RowChange{
				PrimaryKey:     pk,
				OldRowHash:     oldHash,
				NewRowHash:     newHash,
				ChangedScalars: sortedCopy(wr.ChangedScalars),
				TouchedComplex: sortedCopy(wr.TouchedComplex),
			})
		}
		tables = append(tables, TableDelta{Name: wt.Name, Rows: rows})
	}
	return Delta{
		Format:       w.Format,
		BaseRoot:     baseRoot,
		TargetRef:    w.TargetRef,
		TargetCommit: targetCommit,
		Tables:       tables,
		Provenance:   sortedCopyMap(w.Provenance),
	}, nil
}

func hashHex(h *hash.Hash) string {
	if h == nil {
		return ""
	}
	return h.String()
}

func optHash(s string) (*hash.Hash, error) {
	if s == "" {
		return nil, nil
	}
	h, ok := hash.MaybeParse(s)
	if !ok {
		return nil, errors.Newf("invalid hash %q", s)
	}
	return &h, nil
}

func sortedCopy(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	sortStrings(out)
	return out
}

func sortedCopyMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sortStrings(s []string) {
	sort.Strings(s)
}

