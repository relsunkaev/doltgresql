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

// BoxArray is the array variant of Box.
var BoxArray = CreateArrayTypeFromBaseType(Box)

// CidrArray is the array variant of Cidr.
var CidrArray = CreateArrayTypeFromBaseType(Cidr)

// CircleArray is the array variant of Circle.
var CircleArray = CreateArrayTypeFromBaseType(Circle)

// InetArray is the array variant of Inet.
var InetArray = CreateArrayTypeFromBaseType(Inet)

// LineArray is the array variant of Line.
var LineArray = CreateArrayTypeFromBaseType(Line)

// LsegArray is the array variant of Lseg.
var LsegArray = CreateArrayTypeFromBaseType(Lseg)

// MacaddrArray is the array variant of Macaddr.
var MacaddrArray = CreateArrayTypeFromBaseType(Macaddr)

// MoneyArray is the array variant of Money.
var MoneyArray = CreateArrayTypeFromBaseType(Money)

// PathArray is the array variant of Path.
var PathArray = CreateArrayTypeFromBaseType(Path)

// PointArray is the array variant of Point.
var PointArray = CreateArrayTypeFromBaseType(Point)

// PolygonArray is the array variant of Polygon.
var PolygonArray = CreateArrayTypeFromBaseType(Polygon)

// TsQueryArray is the array variant of TsQuery.
var TsQueryArray = CreateArrayTypeFromBaseType(TsQuery)

// TsVectorArray is the array variant of TsVector.
var TsVectorArray = CreateArrayTypeFromBaseType(TsVector)
