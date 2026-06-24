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

#include <deque>
#include <string>

#include "basics/containers/flat_hash_set.h"
#include "catalog/catalog.h"
#include "catalog/user_type.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr auto kSampleData = std::to_array<PgType>({
  // bool (OID 16)
  {
    .oid = 16,
    .typname = "bool",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
  {
    .oid = 26,
    .typname = "oid",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = true,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1028,    // oid
    .typinput = 1798,    // oidin
    .typoutput = 1799,   // oidout
    .typreceive = 2418,  // oidrecv
    .typsend = 2419,     // oidsend
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
  {
    .oid = 27,
    .typname = "tid",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 6,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::UserDefined,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1010,    // tid
    .typinput = 48,      // tidin
    .typoutput = 49,     // tidout
    .typreceive = 2438,  // tidrecv
    .typsend = 2439,     // tidsend
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
  {
    .oid = 28,
    .typname = "xid",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::UserDefined,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1011,    // xid
    .typinput = 50,      // xidin
    .typoutput = 51,     // xidout
    .typreceive = 2440,  // xidrecv
    .typsend = 2441,     // xidsend
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
  {
    .oid = 29,
    .typname = "cid",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::UserDefined,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1012,    // cid
    .typinput = 52,      // cidin
    .typoutput = 53,     // cidout
    .typreceive = 2442,  // cidrecv
    .typsend = 2443,     // cidsend
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
  {
    .oid = 5069,
    .typname = "xid8",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 8,
    .typbyval = true,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::UserDefined,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 271,     // xid8
    .typinput = 5070,    // xid8in
    .typoutput = 5081,   // xid8out
    .typreceive = 5082,  // xid8recv
    .typsend = 5083,     // xid8send
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
  // inet (OID 869) -- registered by the `inet` DuckDB extension
  {
    .oid = 869,
    .typname = "inet",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::Network,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 1041,    // _inet
    .typinput = 910,     // inet_in
    .typoutput = 911,    // inet_out
    .typreceive = 2429,  // inet_recv
    .typsend = 2430,     // inet_send
    .typmodin = 0,
    .typmodout = 0,
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
  // numeric (OID 1700)
  {
    .oid = 1700,
    .typname = "numeric",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
  // regnamespace (OID 4089)
  {
    .oid = 4089,
    .typname = "regnamespace",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 4090,    // _regnamespace
    .typinput = 4084,    // regnamespacein
    .typoutput = 4085,   // regnamespaceout
    .typreceive = 4086,  // regnamespacerecv
    .typsend = 4087,     // regnamespacesend
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
  // regproc (OID 24)
  {
    .oid = 24,
    .typname = "regproc",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 1008,    // _regproc
    .typinput = 44,      // regprocin
    .typoutput = 45,     // regprocout
    .typreceive = 2444,  // regprocrecv
    .typsend = 2445,     // regprocsend
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
  // regprocedure (OID 2202)
  {
    .oid = 2202,
    .typname = "regprocedure",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 2207,    // _regprocedure
    .typinput = 2212,    // regprocedurein
    .typoutput = 2213,   // regprocedureout
    .typreceive = 2446,  // regprocedurerecv
    .typsend = 2447,     // regproceduresend
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
  // regoper (OID 2203)
  {
    .oid = 2203,
    .typname = "regoper",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 2208,    // _regoper
    .typinput = 2214,    // regoperin
    .typoutput = 2215,   // regoperout
    .typreceive = 2448,  // regoperrecv
    .typsend = 2449,     // regopersend
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
  // regoperator (OID 2204)
  {
    .oid = 2204,
    .typname = "regoperator",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 2209,    // _regoperator
    .typinput = 2216,    // regoperatorin
    .typoutput = 2217,   // regoperatorout
    .typreceive = 2450,  // regoperatorrecv
    .typsend = 2451,     // regoperatorsend
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
  // regrole (OID 4096)
  {
    .oid = 4096,
    .typname = "regrole",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 4097,    // _regrole
    .typinput = 4098,    // regrolein
    .typoutput = 4092,   // regroleout
    .typreceive = 4094,  // regrolerecv
    .typsend = 4095,     // regrolesend
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
  // regconfig (OID 3734)
  {
    .oid = 3734,
    .typname = "regconfig",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 3735,    // _regconfig
    .typinput = 3736,    // regconfigin
    .typoutput = 3737,   // regconfigout
    .typreceive = 3738,  // regconfigrecv
    .typsend = 3739,     // regconfigsend
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
  // regdictionary (OID 3769)
  {
    .oid = 3769,
    .typname = "regdictionary",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 3770,    // _regdictionary
    .typinput = 3771,    // regdictionaryin
    .typoutput = 3772,   // regdictionaryout
    .typreceive = 3773,  // regdictionaryrecv
    .typsend = 3774,     // regdictionarysend
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
  // regcollation (OID 4191)
  {
    .oid = 4191,
    .typname = "regcollation",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
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
    .typarray = 4192,    // _regcollation
    .typinput = 4193,    // regcollationin
    .typoutput = 4194,   // regcollationout
    .typreceive = 4196,  // regcollationrecv
    .typsend = 4197,     // regcollationsend
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
  // internal (OID 2281) -- PG pseudo-type for arguments/results of internal
  // C-level functions. Not user-visible, but PG tooling (sqlsmith, ORMs,
  // catalog readers) expects this to exist in pg_type.
  {
    .oid = 2281,
    .typname = "internal",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 8,
    .typbyval = true,
    .typtype = PgType::Typetype::Pseudo,
    .typcategory = PgType::Typcategory::Pseudo,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 0,
    .typinput = 2304,   // internal_in
    .typoutput = 2305,  // internal_out
    .typreceive = 0,
    .typsend = 0,
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
  // anyarray (OID 2277) -- PG pseudo-type used to declare polymorphic
  // array-of-anything arguments and results (e.g. array_agg, unnest).
  {
    .oid = 2277,
    .typname = "anyarray",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Pseudo,
    .typcategory = PgType::Typcategory::Pseudo,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 0,
    .typinput = 2296,    // anyarray_in
    .typoutput = 2297,   // anyarray_out
    .typreceive = 2502,  // anyarray_recv
    .typsend = 2503,     // anyarray_send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Double,
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
  // unknown (OID 705) -- PG's catch-all for types serened doesn't yet map to
  // a concrete PG OID. Returned by Type2Oid() as a fallback so that
  // pg_attribute.atttypid always resolves to a row in pg_type.
  {
    .oid = 705,
    .typname = "unknown",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = -2,
    .typbyval = false,
    .typtype = PgType::Typetype::Pseudo,
    .typcategory = PgType::Typcategory::Unknown,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 0,
    .typinput = 109,     // unknownin
    .typoutput = 110,    // unknownout
    .typreceive = 2402,  // unknownrecv
    .typsend = 2403,     // unknownsend
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
  // name (OID 19)
  {
    .oid = 19,
    .typname = "name",
    .typnamespace = id::kPgCatalogSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 64,  // NAMEDATALEN
    .typbyval = false,
    .typtype = PgType::Typetype::Base,
    .typcategory = PgType::Typcategory::String,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 18,       // char
    .typarray = 1003,    // _name
    .typinput = 34,      // namein
    .typoutput = 35,     // nameout
    .typreceive = 2422,  // namerecv
    .typsend = 2423,     // namesend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Char,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 0,
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 950,  // C_COLLATION_OID
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // information_schema.cardinal_number (domain over int4)
  {
    .oid = 13873,
    .typname = "cardinal_number",
    .typnamespace = id::kPgInformationSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 4,
    .typbyval = true,
    .typtype = PgType::Typetype::Domain,
    .typcategory = PgType::Typcategory::Numeric,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 13872,   // _cardinal_number
    .typinput = 2597,    // domain_in
    .typoutput = 43,     // int4out
    .typreceive = 2598,  // domain_recv
    .typsend = 2407,     // int4send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 23,  // int4
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // information_schema.character_data (domain over varchar)
  {
    .oid = 13876,
    .typname = "character_data",
    .typnamespace = id::kPgInformationSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Domain,
    .typcategory = PgType::Typcategory::String,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 13875,   // _character_data
    .typinput = 2597,    // domain_in
    .typoutput = 1047,   // varcharout
    .typreceive = 2598,  // domain_recv
    .typsend = 2433,     // varcharsend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Extended,
    .typnotnull = false,
    .typbasetype = 1043,  // varchar
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 950,  // C_COLLATION_OID
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // information_schema.sql_identifier (domain over name)
  {
    .oid = 13878,
    .typname = "sql_identifier",
    .typnamespace = id::kPgInformationSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 64,  // NAMEDATALEN
    .typbyval = false,
    .typtype = PgType::Typetype::Domain,
    .typcategory = PgType::Typcategory::String,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 13877,   // _sql_identifier
    .typinput = 2597,    // domain_in
    .typoutput = 35,     // nameout
    .typreceive = 2598,  // domain_recv
    .typsend = 2423,     // namesend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Char,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 19,  // name
    .typtypmod = -1,
    .typndims = 0,
    .typcollation = 950,  // C_COLLATION_OID
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // information_schema.time_stamp (domain over timestamptz(2))
  {
    .oid = 13884,
    .typname = "time_stamp",
    .typnamespace = id::kPgInformationSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = 8,
    .typbyval = true,
    .typtype = PgType::Typetype::Domain,
    .typcategory = PgType::Typcategory::DateTime,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 13883,   // _time_stamp
    .typinput = 2597,    // domain_in
    .typoutput = 1151,   // timestamptz_out
    .typreceive = 2598,  // domain_recv
    .typsend = 2477,     // timestamptz_send
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Double,
    .typstorage = PgType::Typstorage::Plain,
    .typnotnull = false,
    .typbasetype = 1184,  // timestamptz
    .typtypmod = 2,       // timestamptz(2)
    .typndims = 0,
    .typcollation = 0,
    .typdefaultbin = {},
    .typdefault = {},
    .typacl = {},
  },
  // information_schema.yes_or_no (domain over varchar(3))
  {
    .oid = 13886,
    .typname = "yes_or_no",
    .typnamespace = id::kPgInformationSchema.id(),
    .typowner = id::kRootUser.id(),
    .typlen = -1,
    .typbyval = false,
    .typtype = PgType::Typetype::Domain,
    .typcategory = PgType::Typcategory::String,
    .typispreferred = false,
    .typisdefined = true,
    .typdelim = ',',
    .typrelid = 0,
    .typsubscript = 0,
    .typelem = 0,
    .typarray = 13885,   // _yes_or_no
    .typinput = 2597,    // domain_in
    .typoutput = 1047,   // varcharout
    .typreceive = 2598,  // domain_recv
    .typsend = 2433,     // varcharsend
    .typmodin = 0,
    .typmodout = 0,
    .typanalyze = 0,
    .typalign = PgType::Typalign::Int,
    .typstorage = PgType::Typstorage::Extended,
    .typnotnull = false,
    .typbasetype = 1043,  // varchar
    .typtypmod = 7,       // varchar(3) -- typmod is len + VARHDRSZ(4)
    .typndims = 0,
    .typcollation = 950,  // C_COLLATION_OID
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
catalog::MaterializedData SystemTableSnapshot<PgType>::GetTableData() {
  auto snapshot = _config.EnsureCatalogSnapshot();
  auto database_id = GetDatabaseId();

  std::vector<PgType> rows;
  rows.reserve(kSampleData.size() * 2);
  for (const auto& row : kSampleData) {
    rows.push_back(row);
  }

  containers::FlatHashSet<std::string_view> taken;
  for (const auto& schema : snapshot->GetSchemas(database_id)) {
    auto types = snapshot->GetTypes(database_id, schema->GetName());
    taken.reserve(taken.size() + types.size() * 2);
    for (const auto& type : types) {
      taken.insert(type->GetName());
    }
  }
  std::deque<std::string> array_names;
  auto make_array_name = [&](std::string_view scalar) -> std::string_view {
    std::string name = "_" + std::string{scalar};
    while (taken.contains(name)) {
      name.insert(0, "_");
    }
    array_names.push_back(std::move(name));
    taken.insert(array_names.back());
    return array_names.back();
  };

  // Synthesize an array-type row for every scalar entry above that declares a
  // non-zero typarray. PG clients (and sqlsmith) resolve column types via
  // pg_attribute.atttypid -> pg_type.oid; without these rows, columns whose
  // type is an array (text[], oid[], int2[], ...) fail to resolve.
  for (const auto& scalar : kSampleData) {
    if (scalar.typarray == 0 ||
        scalar.typcategory == PgType::Typcategory::Array) {
      continue;
    }
    rows.push_back(PgType{
      .oid = scalar.typarray,
      .typname = make_array_name(scalar.typname.v),
      .typnamespace = scalar.typnamespace,
      .typowner = scalar.typowner,
      .typlen = -1,
      .typbyval = false,
      .typtype = PgType::Typetype::Base,
      .typcategory = PgType::Typcategory::Array,
      .typispreferred = false,
      .typisdefined = true,
      .typdelim = scalar.typdelim,
      .typrelid = 0,
      .typsubscript = 0,
      .typelem = scalar.oid,
      .typarray = 0,
      .typinput = 750,     // array_in
      .typoutput = 751,    // array_out
      .typreceive = 2400,  // array_recv
      .typsend = 2401,     // array_send
      .typmodin = 0,
      .typmodout = 0,
      .typanalyze = 0,
      .typalign = scalar.typalign,
      .typstorage = PgType::Typstorage::Extended,
      .typnotnull = false,
      .typbasetype = 0,
      .typtypmod = -1,
      .typndims = 0,
      .typcollation = scalar.typcollation,
      .typdefaultbin = {},
      .typdefault = {},
      .typacl = {},
    });
  }

  for (const auto& schema : snapshot->GetSchemas(database_id)) {
    for (const auto& type :
         snapshot->GetTypes(database_id, schema->GetName())) {
      const auto& info = type->GetInfo();
      const auto kind = info.type.id();
      const bool is_enum = kind == duckdb::LogicalTypeId::ENUM;
      const bool is_composite = kind == duckdb::LogicalTypeId::STRUCT;

      const auto type_oid = type->GetId().id();
      const auto namespace_oid = schema->GetId().id();
      const auto array_oid = type->GetArrayOid().id();
      const auto array_name = make_array_name(type->GetName());

      auto make_row = [&](bool as_array) {
        return PgType{
          .oid = as_array ? array_oid : type_oid,
          .typname = as_array ? array_name : std::string_view{type->GetName()},
          .typnamespace = namespace_oid,
          .typowner = id::kRootUser.id(),
          .typlen = (!as_array && is_enum) ? int16_t{4} : int16_t{-1},
          .typbyval = !as_array && is_enum,
          .typtype = as_array       ? PgType::Typetype::Base
                     : is_enum      ? PgType::Typetype::Enum
                     : is_composite ? PgType::Typetype::Composite
                                    : PgType::Typetype::Base,
          .typcategory = as_array       ? PgType::Typcategory::Array
                         : is_enum      ? PgType::Typcategory::Enum
                         : is_composite ? PgType::Typcategory::Composite
                                        : PgType::Typcategory::UserDefined,
          .typispreferred = false,
          .typisdefined = true,
          .typdelim = ',',
          .typrelid = (!as_array && is_composite) ? type_oid : 0,
          .typsubscript = 0,
          .typelem = as_array ? type_oid : 0,
          .typarray = as_array ? 0 : array_oid,
          .typinput = 0,
          .typoutput = 0,
          .typreceive = 0,
          .typsend = 0,
          .typmodin = 0,
          .typmodout = 0,
          .typanalyze = 0,
          .typalign = PgType::Typalign::Int,
          .typstorage =
            as_array ? PgType::Typstorage::Extended : PgType::Typstorage::Plain,
          .typnotnull = false,
          .typbasetype = 0,
          .typtypmod = -1,
          .typndims = 0,
          .typcollation = 0,
          .typdefaultbin = {},
          .typdefault = {},
          .typacl = {},
        };
      };

      rows.emplace_back(make_row(false));
      rows.emplace_back(make_row(true));
    }
  }

  auto result = CreateColumns<PgType>(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    WriteData(result, rows[i], kNullMask, i);
  }
  return {std::move(result), rows.size()};
}

}  // namespace sdb::pg
