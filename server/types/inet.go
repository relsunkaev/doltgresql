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

package types

import (
	"net/netip"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
)

const (
	pgInetFamilyIPv4   byte = 2
	pgInetAddressLenV4 byte = 4
)

// InetValue is the in-memory representation of PostgreSQL's inet type.
type InetValue struct {
	Addr netip.Addr
	Bits uint8
}

// Inet is PostgreSQL's network address type. This currently implements the IPv4 subset needed by the test suite.
var Inet = &DoltgresType{
	ID:                  toInternal("inet"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_NetworkAddressTypes,
	IsPreferred:         true,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_inet"),
	InputFunc:           toFuncID("inet_in", toInternal("cstring")),
	OutputFunc:          toFuncID("inet_out", toInternal("inet")),
	ReceiveFunc:         toFuncID("inet_recv", toInternal("internal")),
	SendFunc:            toFuncID("inet_send", toInternal("inet")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_Plain,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("-"),
	SerializationFunc:   serializeTypeInet,
	DeserializationFunc: deserializeTypeInet,
}

// Cidr is PostgreSQL's network block type. This currently implements the IPv4 subset needed by the test suite.
var Cidr = &DoltgresType{
	ID:                  toInternal("cidr"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_NetworkAddressTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_cidr"),
	InputFunc:           toFuncID("cidr_in", toInternal("cstring")),
	OutputFunc:          toFuncID("cidr_out", toInternal("cidr")),
	ReceiveFunc:         toFuncID("cidr_recv", toInternal("internal")),
	SendFunc:            toFuncID("cidr_send", toInternal("cidr")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_Plain,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("-"),
	SerializationFunc:   serializeTypeInet,
	DeserializationFunc: deserializeTypeInet,
}

// ParseInet converts PostgreSQL inet text input into an InetValue.
func ParseInet(input string) (InetValue, error) {
	trimmed := strings.TrimSpace(input)
	if strings.Contains(trimmed, "/") {
		prefix, err := netip.ParsePrefix(trimmed)
		if err != nil {
			return InetValue{}, ErrInvalidSyntaxForType.New("inet", input)
		}
		addr := prefix.Addr().Unmap()
		if !addr.Is4() {
			return InetValue{}, ErrInvalidSyntaxForType.New("inet", input)
		}
		return InetValue{Addr: addr, Bits: uint8(prefix.Bits())}, nil
	}

	addr, err := netip.ParseAddr(trimmed)
	if err != nil {
		return InetValue{}, ErrInvalidSyntaxForType.New("inet", input)
	}
	addr = addr.Unmap()
	if !addr.Is4() {
		return InetValue{}, ErrInvalidSyntaxForType.New("inet", input)
	}
	return InetValue{Addr: addr, Bits: 32}, nil
}

// ParseCidr converts PostgreSQL cidr text input into an InetValue.
func ParseCidr(input string) (InetValue, error) {
	trimmed := strings.TrimSpace(input)
	if strings.Contains(trimmed, "/") {
		prefix, err := netip.ParsePrefix(trimmed)
		if err != nil {
			return InetValue{}, ErrInvalidSyntaxForType.New("cidr", input)
		}
		addr := prefix.Addr().Unmap()
		if !addr.Is4() {
			return InetValue{}, ErrInvalidSyntaxForType.New("cidr", input)
		}
		masked := netip.PrefixFrom(addr, prefix.Bits()).Masked()
		if masked.Addr() != addr {
			return InetValue{}, ErrInvalidSyntaxForType.New("cidr", input)
		}
		return InetValue{Addr: masked.Addr(), Bits: uint8(masked.Bits())}, nil
	}

	addr, err := netip.ParseAddr(trimmed)
	if err != nil {
		return InetValue{}, ErrInvalidSyntaxForType.New("cidr", input)
	}
	addr = addr.Unmap()
	if !addr.Is4() {
		return InetValue{}, ErrInvalidSyntaxForType.New("cidr", input)
	}
	return InetValue{Addr: addr, Bits: 32}, nil
}

// FormatInet converts an InetValue to PostgreSQL's canonical text form.
func FormatInet(value InetValue) string {
	if value.Bits == 32 {
		return value.Addr.String()
	}
	return value.Addr.String() + "/" + strconv.Itoa(int(value.Bits))
}

// FormatCidr converts an InetValue to PostgreSQL's canonical cidr text form.
func FormatCidr(value InetValue) string {
	return value.prefix().String()
}

// Host returns the address without the network mask.
func (v InetValue) Host() string {
	return v.Addr.String()
}

// StrictlyContainedBy returns whether v is a strict subnet of container.
func (v InetValue) StrictlyContainedBy(container InetValue) bool {
	return v.Bits > container.Bits && container.prefix().Contains(v.Addr)
}

func (v InetValue) prefix() netip.Prefix {
	return netip.PrefixFrom(v.Addr, int(v.Bits)).Masked()
}

func serializeTypeInet(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	inet := val.(InetValue)
	addr := inet.Addr.As4()
	return []byte{pgInetFamilyIPv4, inet.Bits, 0, pgInetAddressLenV4, addr[0], addr[1], addr[2], addr[3]}, nil
}

func deserializeTypeInet(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) == 5 {
		return InetValue{
			Addr: netip.AddrFrom4([4]byte{data[0], data[1], data[2], data[3]}),
			Bits: data[4],
		}, nil
	}
	if len(data) != 8 {
		return nil, errors.Errorf("invalid inet data length: %d", len(data))
	}
	if data[0] != pgInetFamilyIPv4 || data[3] != pgInetAddressLenV4 {
		return nil, errors.Errorf("unsupported inet data family %d length %d", data[0], data[3])
	}
	return InetValue{
		Addr: netip.AddrFrom4([4]byte{data[4], data[5], data[6], data[7]}),
		Bits: data[1],
	}, nil
}
