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

package largeobject

import (
	"bytes"
	"testing"
)

func TestPutAndGetSlice(t *testing.T) {
	ResetForTests()
	oid, err := Create("postgres", 1234, "postgres", []byte{0x00, 0x11, 0x22, 0x33})
	if err != nil {
		t.Fatal(err)
	}
	if err := Put("postgres", oid, 2, []byte{0xaa, 0xbb}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := GetSlice("postgres", oid, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected large object to exist")
	}
	if want := []byte{0x00, 0x11, 0xaa, 0xbb}; !bytes.Equal(want, got) {
		t.Fatalf("unexpected slice: got %x, want %x", got, want)
	}
	got, ok, err = GetSlice("postgres", oid, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected large object to exist")
	}
	if want := []byte{0xaa, 0xbb}; !bytes.Equal(want, got) {
		t.Fatalf("unexpected slice: got %x, want %x", got, want)
	}
}

func TestPutExtendsWithZeroBytes(t *testing.T) {
	ResetForTests()
	oid, err := Create("postgres", 1234, "postgres", []byte{0x01})
	if err != nil {
		t.Fatal(err)
	}
	if err := Put("postgres", oid, 3, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	got, ok := Get("postgres", oid)
	if !ok {
		t.Fatal("expected large object to exist")
	}
	if want := []byte{0x01, 0x00, 0x00, 0xff}; !bytes.Equal(want, got) {
		t.Fatalf("unexpected object data: got %x, want %x", got, want)
	}
}

func TestGetSliceBounds(t *testing.T) {
	ResetForTests()
	oid, err := Create("postgres", 1234, "postgres", []byte{0x01, 0x02})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := GetSlice("postgres", oid, -1, 1); err == nil {
		t.Fatal("expected negative offset to fail")
	}
	if _, _, err := GetSlice("postgres", oid, 0, -1); err == nil {
		t.Fatal("expected negative length to fail")
	}
	got, ok, err := GetSlice("postgres", oid, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected large object to exist")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice past end, got %x", got)
	}
}

func TestAddACLItemDeduplicates(t *testing.T) {
	ResetForTests()
	oid, err := Create("postgres", 1234, "postgres", nil)
	if err != nil {
		t.Fatal(err)
	}
	const item = "reader=r/postgres"
	if err := AddACLItem("postgres", oid, item); err != nil {
		t.Fatal(err)
	}
	if err := AddACLItem("postgres", oid, item); err != nil {
		t.Fatal(err)
	}
	objects := Objects("postgres")
	if len(objects) != 1 {
		t.Fatalf("expected one large object, got %d", len(objects))
	}
	if got := objects[0].ACL; len(got) != 1 || got[0] != item {
		t.Fatalf("unexpected ACL items: %#v", got)
	}
}

func TestDatabaseLocalObjects(t *testing.T) {
	ResetForTests()
	oid, err := Create("one", 1234, "postgres", []byte{0x01})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Create("two", oid, "postgres", []byte{0x02}); err != nil {
		t.Fatal(err)
	}
	one, ok := Get("one", oid)
	if !ok {
		t.Fatal("expected object in database one")
	}
	two, ok := Get("two", oid)
	if !ok {
		t.Fatal("expected object in database two")
	}
	if bytes.Equal(one, two) {
		t.Fatalf("expected database-local object data, got %x and %x", one, two)
	}
}

func TestRollbackToSavepoint(t *testing.T) {
	ResetForTests()
	BeginTransaction(1)
	oid, err := Create("postgres", 1234, "postgres", []byte{0x01})
	if err != nil {
		t.Fatal(err)
	}
	PushSavepoint(1, "s")
	if err = Put("postgres", oid, 0, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err = RollbackToSavepoint(1, "s"); err != nil {
		t.Fatal(err)
	}
	got, ok := Get("postgres", oid)
	if !ok {
		t.Fatal("expected object to exist after savepoint rollback")
	}
	if want := []byte{0x01}; !bytes.Equal(want, got) {
		t.Fatalf("unexpected object data: got %x, want %x", got, want)
	}
}
