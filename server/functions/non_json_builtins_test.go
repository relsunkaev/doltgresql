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

package functions

import (
	"hash/crc32"
	"math"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/uuid"
	"github.com/dolthub/doltgresql/server/largeobject"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestFormatIntegerBase(t *testing.T) {
	if got, want := formatIntegerBase(int32(10), 2), "1010"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := formatIntegerBase(int64(10), 8), "12"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatDynamicWidth(t *testing.T) {
	ctx := sql.NewEmptyContext()
	argTypes := []*pgtypes.DoltgresType{pgtypes.Int64, pgtypes.Text, pgtypes.Int64, pgtypes.Text}
	args := []any{int64(5), "x", int64(-5), "y"}
	got, err := pgFormat(ctx, "%*s|%*s", argTypes, args)
	if err != nil {
		t.Fatal(err)
	}
	if want := "    x|y    "; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	argTypes = []*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Int64}
	args = []any{"foo", int64(6)}
	got, err = pgFormat(ctx, "%*2$s", argTypes, args)
	if err != nil {
		t.Fatal(err)
	}
	if want := "   foo"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	if _, err = pgFormat(ctx, "%*s", []*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text}, []any{"wide", "x"}); err == nil {
		t.Fatal("expected non-integer dynamic width to error")
	}
	if _, err = pgFormat(ctx, "%*s", []*pgtypes.DoltgresType{pgtypes.Int64}, []any{int64(3)}); err == nil {
		t.Fatal("expected missing value argument to error")
	}
}

func TestUuidExtractHelpers(t *testing.T) {
	v4 := uuid.Must(uuid.FromString("41db1265-8bc1-4ab3-992f-885799a4af1d"))
	if got, want := uuidExtractVersion(v4), uuid.V4; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if got, err := uuidExtractTimestamp(v4); err != nil || got != nil {
		t.Fatalf("got %#v, err %v; want nil timestamp without error", got, err)
	}

	v1 := uuid.NamespaceDNS
	got, err := uuidExtractTimestamp(v1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(time.Time); !ok {
		t.Fatalf("got %#v, want time.Time", got)
	}
}

func TestTypeMetadataHelpers(t *testing.T) {
	if got, want := parseTypeModifier("varchar(32)"), int32(36); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if got, want := parseTypeModifier("text"), int32(-1); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestCatalogLookupHelpers(t *testing.T) {
	ctx := sql.NewEmptyContext()
	formatted, err := format_type.Callable(ctx, [3]*pgtypes.DoltgresType{}, id.NewOID(0).AsId(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := formatted.(string), "-"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	sqlASCII, err := pg_encoding_to_char_int.Callable(ctx, [2]*pgtypes.DoltgresType{}, int32(0))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sqlASCII.(string), "SQL_ASCII"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	invalidEncoding, err := pg_encoding_to_char_int.Callable(ctx, [2]*pgtypes.DoltgresType{}, int32(-1))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := invalidEncoding.(string), ""; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestUnicodeInformationHelpers(t *testing.T) {
	if !unicodeAssigned("abc") {
		t.Fatal("expected valid UTF-8 text to be assigned")
	}
	if unicodeAssigned(string([]byte{0xff})) {
		t.Fatal("expected invalid UTF-8 text to be unassigned")
	}
}

func TestPostgres18MathHelpers(t *testing.T) {
	if got, want := math.Gamma(6), float64(120); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	lgamma, _ := math.Lgamma(6)
	if lgamma <= 4.7 || lgamma >= 4.8 {
		t.Fatalf("got %v, want value between 4.7 and 4.8", lgamma)
	}
}

func TestLargeObjectPrivilegeInquiry(t *testing.T) {
	largeobject.ResetForTests()
	t.Cleanup(largeobject.ResetForTests)

	oid, err := largeobject.Create(12345, "owner_role", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = largeobject.AddACLItem(oid, "reader_role=r/owner_role"); err != nil {
		t.Fatal(err)
	}

	hasPrivilege, err := hasLargeObjectPrivilege("owner_role", oid, "SELECT")
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrivilege {
		t.Fatal("expected owner to have SELECT")
	}
	hasPrivilege, err = hasLargeObjectPrivilege("reader_role", oid, "SELECT")
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrivilege {
		t.Fatal("expected explicit ACL to grant SELECT")
	}
	hasPrivilege, err = hasLargeObjectPrivilege("reader_role", oid, "UPDATE")
	if err != nil {
		t.Fatal(err)
	}
	if hasPrivilege {
		t.Fatal("expected SELECT-only ACL not to grant UPDATE")
	}
	if _, err = hasLargeObjectPrivilege("reader_role", oid, "INSERT"); err == nil {
		t.Fatal("expected unsupported large-object privilege error")
	}
}

func TestPostgres18ByteaHelpers(t *testing.T) {
	ctx := sql.NewEmptyContext()
	if got, want := int64(crc32.ChecksumIEEE([]byte("abc"))), int64(891568578); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if got, want := int64(crc32.Checksum([]byte("abc"), crc32.MakeTable(crc32.Castagnoli))), int64(910901175); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	encoded, err := int32ToBytea(ctx, int32(1234), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := encoded.([]byte), []byte{0, 0, 4, 210}; string(got) != string(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	decoded, err := byteaToInt16(ctx, []byte{0x80, 0x00}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := decoded.(int16), int16(-32768); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}
