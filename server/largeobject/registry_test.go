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
	oid, err := Create(1234, "postgres", []byte{0x00, 0x11, 0x22, 0x33})
	if err != nil {
		t.Fatal(err)
	}
	if err := Put(oid, 2, []byte{0xaa, 0xbb}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := GetSlice(oid, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected large object to exist")
	}
	if want := []byte{0x00, 0x11, 0xaa, 0xbb}; !bytes.Equal(want, got) {
		t.Fatalf("unexpected slice: got %x, want %x", got, want)
	}
	got, ok, err = GetSlice(oid, 2, 2)
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
	oid, err := Create(1234, "postgres", []byte{0x01})
	if err != nil {
		t.Fatal(err)
	}
	if err := Put(oid, 3, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	got, ok := Get(oid)
	if !ok {
		t.Fatal("expected large object to exist")
	}
	if want := []byte{0x01, 0x00, 0x00, 0xff}; !bytes.Equal(want, got) {
		t.Fatalf("unexpected object data: got %x, want %x", got, want)
	}
}

func TestGetSliceBounds(t *testing.T) {
	ResetForTests()
	oid, err := Create(1234, "postgres", []byte{0x01, 0x02})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := GetSlice(oid, -1, 1); err == nil {
		t.Fatal("expected negative offset to fail")
	}
	if _, _, err := GetSlice(oid, 0, -1); err == nil {
		t.Fatal("expected negative length to fail")
	}
	got, ok, err := GetSlice(oid, 10, 2)
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
	oid, err := Create(1234, "postgres", nil)
	if err != nil {
		t.Fatal(err)
	}
	const item = "reader=r/postgres"
	if err := AddACLItem(oid, item); err != nil {
		t.Fatal(err)
	}
	if err := AddACLItem(oid, item); err != nil {
		t.Fatal(err)
	}
	objects := Objects()
	if len(objects) != 1 {
		t.Fatalf("expected one large object, got %d", len(objects))
	}
	if got := objects[0].ACL; len(got) != 1 || got[0] != item {
		t.Fatalf("unexpected ACL items: %#v", got)
	}
}
