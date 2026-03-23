////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "pg/pg_catalog/pg_type.h"

#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr auto kSampleData = std::to_array<PgType>({
  // bool (OID 16)
  {
    .oid = 16,
    .typname = "bool",
    .typnamespace = 11,  // pg_catalog
    .typowner = 10,
    .typlen = 1,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Boolean,
    .typispreferred = true,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1000,    // _bool
    .typinput = 1242,    // boolin
    .typoutput = 1243,   // boolout
    .typreceive = 2436,  // boolrecv
    .typsend = 2437,     // boolsend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Char,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // int2/smallint (OID 21)
  {
    .oid = 21,
    .typname = "int2",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 2,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1005,    // _int2
    .typinput = 38,      // int2in
    .typoutput = 39,     // int2out
    .typreceive = 2410,  // int2recv
    .typsend = 2411,     // int2send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Short,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // int4/integer (OID 23)
  {
    .oid = 23,
    .typname = "int4",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1007,    // _int4
    .typinput = 42,      // int4in
    .typoutput = 43,     // int4out
    .typreceive = 2406,  // int4recv
    .typsend = 2407,     // int4send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // int8/bigint (OID 20)
  {
    .oid = 20,
    .typname = "int8",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 8,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1016,    // _int8
    .typinput = 460,     // int8in
    .typoutput = 461,    // int8out
    .typreceive = 2408,  // int8recv
    .typsend = 2409,     // int8send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Double,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // float4/real (OID 700)
  {
    .oid = 700,
    .typname = "float4",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1021,    // _float4
    .typinput = 200,     // float4in
    .typoutput = 201,    // float4out
    .typreceive = 2424,  // float4recv
    .typsend = 2425,     // float4send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // float8/double precision (OID 701)
  {
    .oid = 701,
    .typname = "float8",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 8,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = true,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1022,    // _float8
    .typinput = 214,     // float8in
    .typoutput = 215,    // float8out
    .typreceive = 2426,  // float8recv
    .typsend = 2427,     // float8send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Double,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // text (OID 25)
  {
    .oid = 25,
    .typname = "text",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::String,
    .typispreferred = true,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1009,    // _text
    .typinput = 2275,    // textin
    .typoutput = 2276,   // textout
    .typreceive = 2434,  // textrecv
    .typsend = 2435,     // textsend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Extended,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // char (OID 18)
  {
    .oid = 18,
    .typname = "char",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 1,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::String,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1002,    // _char
    .typinput = 1245,    // charin
    .typoutput = 33,     // charout
    .typreceive = 2434,  // charrecv
    .typsend = 2435,     // charsend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Char,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // varchar (OID 1043)
  {
    .oid = 1043,
    .typname = "varchar",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::String,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1015,    // _varchar
    .typinput = 1046,    // varcharin
    .typoutput = 1047,   // varcharout
    .typreceive = 2432,  // varcharrecv
    .typsend = 2433,     // varcharsend
    .typmodin = 1048,    // varchartypmodin
    .typmodout = 1049,   // varchartypmodout
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Extended,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 100,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // bytea (OID 17)
  {
    .oid = 17,
    .typname = "bytea",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::UserDefined,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1001,    // _bytea
    .typinput = 1244,    // byteain
    .typoutput = 31,     // byteaout
    .typreceive = 2412,  // bytearecv
    .typsend = 2413,     // byteasend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Extended,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // json (OID 114)
  {
    .oid = 114,
    .typname = "json",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::UserDefined,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 199,    // _json
    .typinput = 321,    // json_in
    .typoutput = 322,   // json_out
    .typreceive = 323,  // json_recv
    .typsend = 324,     // json_send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Extended,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // uuid (OID 2950)
  {
    .oid = 2950,
    .typname = "uuid",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 16,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::UserDefined,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 2951,    // _uuid
    .typinput = 2952,    // uuid_in
    .typoutput = 2953,   // uuid_out
    .typreceive = 2954,  // uuid_recv
    .typsend = 2955,     // uuid_send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Char,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // numeric (OID 1700)
  {
    .oid = 1700,
    .typname = "numeric",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1231,    // _numeric
    .typinput = 1701,    // numeric_in
    .typoutput = 1702,   // numeric_out
    .typreceive = 2460,  // numeric_recv
    .typsend = 2461,     // numeric_send
    .typmodin = 1703,    // numerictypmodin
    .typmodout = 1704,   // numerictypmodout
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Main,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // date (OID 1082)
  {
    .oid = 1082,
    .typname = "date",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::DateTime,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1182,    // _date
    .typinput = 1084,    // date_in
    .typoutput = 1085,   // date_out
    .typreceive = 2468,  // date_recv
    .typsend = 2469,     // date_send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // timestamp (OID 1114)
  {
    .oid = 1114,
    .typname = "timestamp",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 8,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::DateTime,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1115,    // _timestamp
    .typinput = 1312,    // timestamp_in
    .typoutput = 1313,   // timestamp_out
    .typreceive = 2474,  // timestamp_recv
    .typsend = 2475,     // timestamp_send
    .typmodin = 1316,    // timestamptypmodin
    .typmodout = 1317,   // timestamptypmodout
    .typanalyze = 0,
    .typalign = PgType::Typalign::Double,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // timestamptz (OID 1184)
  {
    .oid = 1184,
    .typname = "timestamptz",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 8,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::DateTime,
    .typispreferred = true,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1185,    // _timestamptz
    .typinput = 1150,    // timestamptz_in
    .typoutput = 1151,   // timestamptz_out
    .typreceive = 2476,  // timestamptz_recv
    .typsend = 2477,     // timestamptz_send
    .typmodin = 1316,    // timestamptztypmodin
    .typmodout = 1317,   // timestamptztypmodout
    .typanalyze = 0,
    .typalign = PgType::Typalign::Double,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // regtype (OID 2206)
  {
    .oid = 2206,
    .typname = "regtype",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 2211,    // _regtype
    .typinput = 2220,    // regtypein
    .typoutput = 2221,   // regtypeout
    .typreceive = 2454,  // regtyperecv
    .typsend = 2455,     // regtypesend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // regclass (OID 2205)
  {
    .oid = 2205,
    .typname = "regclass",
    .typnamespace = 11,
    .typowner = 10,
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 2210,    // _regclass
    .typinput = 2218,    // regclassin
    .typoutput = 2219,   // regclassout
    .typreceive = 2452,  // regclassrecv
    .typsend = 2453,     // regclasssend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
});

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgType::typdefaultbin),
  GetIndex(&PgType::typdefault),
  GetIndex(&PgType::typacl),
});

}  // namespace

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgType>::GetTableData(
  velox::memory::MemoryPool& pool) {
  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<PgType>);
  boost::pfr::for_each_field(PgType{}, [&]<typename Field>(const Field& field) {
    auto column = CreateColumn<Field>(kSampleData.size(), &pool);
    result.push_back(std::move(column));
  });
  for (size_t row = 0; row < kSampleData.size(); ++row) {
    WriteData(result, kSampleData[row], kNullMask, row, &pool);
  }
  return result;
}

}  // namespace sdb::pg
