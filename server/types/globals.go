// Copyright 2024 Dolthub, Inc.
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
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/types"
)

// TypeAlignment represents the alignment required when storing a value of this type.
type TypeAlignment string

const (
	TypeAlignment_Char   TypeAlignment = "c"
	TypeAlignment_Short  TypeAlignment = "s"
	TypeAlignment_Int    TypeAlignment = "i"
	TypeAlignment_Double TypeAlignment = "d"
)

// TypeCategory represents the type category that a type belongs to. These are used by Postgres to group similar types
// for parameter resolution, operator resolution, etc.
type TypeCategory string

const (
	TypeCategory_ArrayTypes          TypeCategory = "A"
	TypeCategory_BooleanTypes        TypeCategory = "B"
	TypeCategory_CompositeTypes      TypeCategory = "C"
	TypeCategory_DateTimeTypes       TypeCategory = "D"
	TypeCategory_EnumTypes           TypeCategory = "E"
	TypeCategory_GeometricTypes      TypeCategory = "G"
	TypeCategory_NetworkAddressTypes TypeCategory = "I"
	TypeCategory_NumericTypes        TypeCategory = "N"
	TypeCategory_PseudoTypes         TypeCategory = "P"
	TypeCategory_RangeTypes          TypeCategory = "R"
	TypeCategory_StringTypes         TypeCategory = "S"
	TypeCategory_TimespanTypes       TypeCategory = "T"
	TypeCategory_UserDefinedTypes    TypeCategory = "U"
	TypeCategory_BitStringTypes      TypeCategory = "V"
	TypeCategory_UnknownTypes        TypeCategory = "X"
	TypeCategory_InternalUseTypes    TypeCategory = "Z"
)

// TypeStorage represents the storage strategy for storing `varlena` (typlen = -1) types.
type TypeStorage string

const (
	TypeStorage_Plain    TypeStorage = "p"
	TypeStorage_External TypeStorage = "e"
	TypeStorage_Main     TypeStorage = "m"
	TypeStorage_Extended TypeStorage = "x"
)

// TypeType represents the type of types that can be created/used.
// This includes 'base', 'composite', 'domain', 'enum', 'shell', 'range' and  'multirange'.
type TypeType string

const (
	TypeType_Base       TypeType = "b"
	TypeType_Composite  TypeType = "c"
	TypeType_Domain     TypeType = "d"
	TypeType_Enum       TypeType = "e"
	TypeType_Pseudo     TypeType = "p"
	TypeType_Range      TypeType = "r"
	TypeType_MultiRange TypeType = "m"
)

// GetTypeByID returns the DoltgresType matching the given Internal ID.
// If the Internal ID does not match a type, then nil is returned.
func GetTypeByID(internalID id.Type) *DoltgresType {
	t, ok := IDToBuiltInDoltgresType[internalID]
	if !ok {
		// TODO: return UNKNOWN?
		return nil
	}
	return t
}

// GetAllBuitInTypes returns a slice containing all registered types.
// The slice is sorted by each type's ID.
func GetAllBuitInTypes() []*DoltgresType {
	pgTypes := make([]*DoltgresType, 0, len(IDToBuiltInDoltgresType))
	for internalID, typ := range IDToBuiltInDoltgresType {
		if typ.ID == Unknown.ID && internalID.TypeName() != "unknown" {
			continue
		}
		pgTypes = append(pgTypes, typ)
	}
	sort.Slice(pgTypes, func(i, j int) bool {
		return pgTypes[i].ID < pgTypes[j].ID
	})
	return pgTypes
}

// IDToBuiltInDoltgresType is a map of id.Id to Doltgres' built-in type.
var IDToBuiltInDoltgresType map[id.Type]*DoltgresType

// NameToInternalID is a mapping from a given name to its respective Internal ID.
var NameToInternalID = map[string]id.Type{}

// idToInternalSerializationFunc is a map from the `send` function ID to the internal serialization function.
var idToInternalSerializationFunc = map[id.Function]internalSerializationFunc{}

// idToInternalDeserializationFunc is a map from the `receive` function ID to the internal deserialization function.
var idToInternalDeserializationFunc = map[id.Function]internalDeserializationFunc{}

// init, for now, fills the contents of IDToBuiltInDoltgresType and NameToInternalID, so that we may search for types
// using regtype. This should be replaced with a better abstraction at some point.
func init() {
	// For some reason, Go refuses to compile if this map is declared globally.
	// Serialization references functions that eventually reference this map, and this map references all types such
	// that Go thinks there's a reference cycle. It's not a real cycle though since the functions aren't evaluated at
	// compile time, but Go finds an issue with it, so this is a workaround.
	IDToBuiltInDoltgresType = map[id.Type]*DoltgresType{
		toInternal("_abstime"):         Unknown,
		toInternal("_aclitem"):         Unknown,
		toInternal("_bit"):             BitArray,
		toInternal("_bool"):            BoolArray,
		toInternal("_box"):             BoxArray,
		toInternal("_bpchar"):          BpCharArray,
		toInternal("_bytea"):           ByteaArray,
		toInternal("_char"):            InternalCharArray,
		toInternal("_cid"):             Unknown,
		toInternal("_cidr"):            CidrArray,
		toInternal("_circle"):          CircleArray,
		toInternal("_cstring"):         CstringArray,
		toInternal("_date"):            DateArray,
		toInternal("_datemultirange"):  DateMultiRangeArray,
		toInternal("_daterange"):       DateRangeArray,
		toInternal("_float4"):          Float32Array,
		toInternal("_float8"):          Float64Array,
		toInternal("_gtsvector"):       Unknown,
		toInternal("_halfvec"):         HalfvecArray,
		toInternal("_inet"):            InetArray,
		toInternal("_int2"):            Int16Array,
		toInternal("_int2vector"):      Int16vectorArray,
		toInternal("_int4"):            Int32Array,
		toInternal("_int4multirange"):  Int4MultiRangeArray,
		toInternal("_int4range"):       Int4RangeArray,
		toInternal("_int8"):            Int64Array,
		toInternal("_int8multirange"):  Int8MultiRangeArray,
		toInternal("_int8range"):       Int8RangeArray,
		toInternal("_interval"):        IntervalArray,
		toInternal("_json"):            JsonArray,
		toInternal("_jsonb"):           JsonBArray,
		toInternal("_line"):            LineArray,
		toInternal("_lseg"):            LsegArray,
		toInternal("_macaddr"):         MacaddrArray,
		toInternal("_money"):           MoneyArray,
		toInternal("_name"):            NameArray,
		toInternal("_numeric"):         NumericArray,
		toInternal("_nummultirange"):   NumMultiRangeArray,
		toInternal("_numrange"):        NumRangeArray,
		toInternal("_oid"):             OidArray,
		toInternal("_oidvector"):       OidvectorArray,
		toInternal("_path"):            PathArray,
		toInternal("_pg_lsn"):          PgLsnArray,
		toInternal("_pg_snapshot"):     PgSnapshotArray,
		toInternal("_point"):           PointArray,
		toInternal("_polygon"):         PolygonArray,
		toInternal("_record"):          RecordArray,
		toInternal("_refcursor"):       Unknown,
		toInternal("_regclass"):        RegclassArray,
		toInternal("_regconfig"):       RegconfigArray,
		toInternal("_regdictionary"):   RegdictionaryArray,
		toInternal("_regnamespace"):    RegnamespaceArray,
		toInternal("_regoper"):         Unknown,
		toInternal("_regoperator"):     RegoperatorArray,
		toInternal("_regproc"):         RegprocArray,
		toInternal("_regprocedure"):    RegprocedureArray,
		toInternal("_regrole"):         RegroleArray,
		toInternal("_regtype"):         RegtypeArray,
		toInternal("_reltime"):         Unknown,
		toInternal("_sparsevec"):       SparsevecArray,
		toInternal("_text"):            TextArray,
		toInternal("_tid"):             Unknown,
		toInternal("_time"):            TimeArray,
		toInternal("_timestamp"):       TimestampArray,
		toInternal("_timestamptz"):     TimestampTZArray,
		toInternal("_timetz"):          TimeTZArray,
		toInternal("_tinterval"):       Unknown,
		toInternal("_tsmultirange"):    TsMultiRangeArray,
		toInternal("_tsquery"):         TsQueryArray,
		toInternal("_tsrange"):         TsRangeArray,
		toInternal("_tstzmultirange"):  TstzMultiRangeArray,
		toInternal("_tstzrange"):       TstzRangeArray,
		toInternal("_tsvector"):        TsVectorArray,
		toInternal("_txid_snapshot"):   Unknown,
		toInternal("_uuid"):            UuidArray,
		toInternal("_varbit"):          VarBitArray,
		toInternal("_varchar"):         VarCharArray,
		toInternal("_vector"):          VectorArray,
		toInternal("_xid"):             XidArray,
		toInternal("_xid8"):            Xid8Array,
		toInternal("_xml"):             XmlArray,
		toInternal("abstime"):          Unknown,
		toInternal("aclitem"):          Unknown,
		toInternal("any"):              Any,
		toInternal("anyarray"):         AnyArray,
		toInternal("anyelement"):       AnyElement,
		toInternal("anyenum"):          AnyEnum,
		toInternal("anynonarray"):      AnyNonArray,
		toInternal("anyrange"):         Unknown,
		toInternal("bit"):              Bit,
		toInternal("bool"):             Bool,
		toInternal("box"):              Box,
		toInternal("bpchar"):           BpChar,
		toInternal("bytea"):            Bytea,
		toInternal("char"):             InternalChar,
		toInternal("cid"):              Unknown,
		toInternal("cidr"):             Cidr,
		toInternal("circle"):           Circle,
		toInternal("cstring"):          Cstring,
		toInternal("date"):             Date,
		toInternal("datemultirange"):   DateMultiRange,
		toInternal("daterange"):        DateRange,
		toInternal("event_trigger"):    EventTrigger,
		toInternal("fdw_handler"):      Unknown,
		toInternal("float4"):           Float32,
		toInternal("float8"):           Float64,
		toInternal("gtsvector"):        Unknown,
		toInternal("halfvec"):          Halfvec,
		toInternal("index_am_handler"): Unknown,
		toInternal("inet"):             Inet,
		toInternal("int2"):             Int16,
		toInternal("int2vector"):       Int16vector,
		toInternal("int4"):             Int32,
		toInternal("int4multirange"):   Int4MultiRange,
		toInternal("int4range"):        Int4Range,
		toInternal("int8"):             Int64,
		toInternal("int8multirange"):   Int8MultiRange,
		toInternal("int8range"):        Int8Range,
		toInternal("internal"):         Internal,
		toInternal("interval"):         Interval,
		toInternal("json"):             Json,
		toInternal("jsonb"):            JsonB,
		toInternal("language_handler"): Unknown,
		toInternal("line"):             Line,
		toInternal("lseg"):             Lseg,
		toInternal("macaddr"):          Macaddr,
		toInternal("money"):            Money,
		toInternal("name"):             Name,
		toInternal("numeric"):          Numeric,
		toInternal("nummultirange"):    NumMultiRange,
		toInternal("numrange"):         NumRange,
		toInternal("oid"):              Oid,
		toInternal("oidvector"):        Oidvector,
		toInternal("opaque"):           Unknown,
		toInternal("path"):             Path,
		toInternal("pg_attribute"):     Unknown,
		toInternal("pg_auth_members"):  Unknown,
		toInternal("pg_authid"):        Unknown,
		toInternal("pg_class"):         Unknown,
		toInternal("pg_database"):      Unknown,
		toInternal("pg_ddl_command"):   Unknown,
		toInternal("pg_lsn"):           PgLsn,
		toInternal("pg_node_tree"):     Unknown,
		toInternal("pg_proc"):          Unknown,
		toInternal("pg_snapshot"):      PgSnapshot,
		toInternal("pg_shseclabel"):    Unknown,
		toInternal("pg_type"):          Unknown,
		toInternal("point"):            Point,
		toInternal("polygon"):          Polygon,
		toInternal("record"):           Record,
		toInternal("refcursor"):        Unknown,
		toInternal("regclass"):         Regclass,
		toInternal("regconfig"):        Regconfig,
		toInternal("regdictionary"):    Regdictionary,
		toInternal("regnamespace"):     Regnamespace,
		toInternal("regoper"):          Unknown,
		toInternal("regoperator"):      Regoperator,
		toInternal("regproc"):          Regproc,
		toInternal("regprocedure"):     Regprocedure,
		toInternal("regrole"):          Regrole,
		toInternal("regtype"):          Regtype,
		toInternal("reltime"):          Unknown,
		toInternal("smgr"):             Unknown,
		toInternal("sparsevec"):        Sparsevec,
		toInternal("text"):             Text,
		toInternal("tid"):              Unknown,
		toInternal("time"):             Time,
		toInternal("timestamp"):        Timestamp,
		toInternal("timestamptz"):      TimestampTZ,
		toInternal("timetz"):           TimeTZ,
		toInternal("tinterval"):        Unknown,
		toInternal("trigger"):          Trigger,
		toInternal("tsm_handler"):      Unknown,
		toInternal("tsmultirange"):     TsMultiRange,
		toInternal("tsquery"):          TsQuery,
		toInternal("tsrange"):          TsRange,
		toInternal("tstzmultirange"):   TstzMultiRange,
		toInternal("tstzrange"):        TstzRange,
		toInternal("tsvector"):         TsVector,
		toInternal("txid_snapshot"):    Unknown,
		toInternal("unknown"):          Unknown,
		toInternal("uuid"):             Uuid,
		toInternal("varbit"):           VarBit,
		toInternal("varchar"):          VarChar,
		toInternal("vector"):           Vector,
		toInternal("void"):             Void,
		toInternal("xid"):              Xid,
		toInternal("xid8"):             Xid8,
		toInternal("xml"):              Xml,
	}
	for _, t := range GetAllBuitInTypes() {
		NameToInternalID[t.Name()] = t.ID
		pt, ok := types.OidToType[oid.Oid(id.Cache().ToOID(t.ID.AsId()))]
		if ok {
			NameToInternalID[pt.SQLStandardName()] = t.ID
		}
		if t.SendFunc != 0 && t.SerializationFunc != nil {
			idToInternalSerializationFunc[globalFunctionRegistry.GetInternalID(t.SendFunc)] = t.SerializationFunc
		}
		if t.ReceiveFunc != 0 && t.DeserializationFunc != nil {
			idToInternalDeserializationFunc[globalFunctionRegistry.GetInternalID(t.ReceiveFunc)] = t.DeserializationFunc
		}
	}
	// XMLPARSE(DOCUMENT '...') is currently parsed as a typed string literal.
	NameToInternalID["document"] = Xml.ID
	NameToInternalID["content"] = Xml.ID
	// Add the created types to the deserialization map
	idToInternalDeserializationFunc[globalFunctionRegistry.GetInternalID(toFuncID("domain_recv", toInternal("internal"), toInternal("oid"), toInternal("int4")))] = deserializeTypeDomain
	idToInternalDeserializationFunc[globalFunctionRegistry.GetInternalID(toFuncID("enum_recv", toInternal("internal"), toInternal("oid")))] = deserializeTypeEnum
	// Add the created types to the serialization map
	idToInternalSerializationFunc[globalFunctionRegistry.GetInternalID(toFuncID("enum_send", toInternal("anyenum")))] = serializeTypeEnum
}

// ErrCastOutOfRange is returned when a value is out of range for a given cast function.
// We use this error type as a sentinel error when a type conversion function fails only due to the range of the
// input in order to conform to expectations in go-mysql-server.
var ErrCastOutOfRange = errors.New("out of range")
