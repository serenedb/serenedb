////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <duckdb/common/types.hpp>
#include <string_view>

namespace sdb::pg {

// PG logical types for DuckDB -- base type + alias, following the same
// pattern as DuckDB's LogicalType::JSON() (VARCHAR + alias "JSON").
//
// DECLARE_PG_TYPE(REGTYPE, Regtype, "regtype", BIGINT) expands to:
//   inline constexpr std::string_view kRegtypeAlias = "regtype";
//   inline LogicalType REGTYPE() { ... with BIGINT + alias "regtype" ... }
//   inline bool IsRegtype(const LogicalType&) { ... }

// Alias constant + Is* check (safe to include alongside Velox types)
#define DECLARE_PG_TYPE_CHECK(Name, alias_str, BaseTypeId)      \
  inline constexpr std::string_view k##Name##Alias = alias_str; \
  inline bool Is##Name(const duckdb::LogicalType& type) {       \
    return type.id() == duckdb::LogicalTypeId::BaseTypeId &&    \
           type.HasAlias() && type.GetAlias() == alias_str;     \
  }

// Factory function (only safe where Velox types.h is NOT included)
#define DECLARE_PG_TYPE_FACTORY(UPPER, alias_str, BaseTypeId)           \
  inline duckdb::LogicalType UPPER() {                                  \
    auto type = duckdb::LogicalType(duckdb::LogicalTypeId::BaseTypeId); \
    type.SetAlias(alias_str);                                           \
    return type;                                                        \
  }

#ifdef SDB_PG_LOGICAL_TYPES_NO_FACTORY
#define DECLARE_PG_TYPE(UPPER, Name, alias_str, BaseTypeId) \
  DECLARE_PG_TYPE_CHECK(Name, alias_str, BaseTypeId)
#else
#define DECLARE_PG_TYPE(UPPER, Name, alias_str, BaseTypeId) \
  DECLARE_PG_TYPE_CHECK(Name, alias_str, BaseTypeId)        \
  DECLARE_PG_TYPE_FACTORY(UPPER, alias_str, BaseTypeId)
#endif

// clang-format off
DECLARE_PG_TYPE(REGTYPE,        Regtype,        "regtype",        BIGINT)
DECLARE_PG_TYPE(REGCLASS,       Regclass,       "regclass",       BIGINT)
DECLARE_PG_TYPE(REGPROC,        Regproc,        "regproc",        BIGINT)
DECLARE_PG_TYPE(REGNAMESPACE,   Regnamespace,   "regnamespace",   BIGINT)
DECLARE_PG_TYPE(OID,            Oid,            "oid",            BIGINT)
DECLARE_PG_TYPE(REGOPER,        Regoper,        "regoper",        BIGINT)
DECLARE_PG_TYPE(REGOPERATOR,    Regoperator,    "regoperator",    BIGINT)
DECLARE_PG_TYPE(REGPROCEDURE,   Regprocedure,   "regprocedure",   BIGINT)
DECLARE_PG_TYPE(REGROLE,        Regrole,        "regrole",        BIGINT)
DECLARE_PG_TYPE(REGCONFIG,      Regconfig,      "regconfig",      BIGINT)
DECLARE_PG_TYPE(REGDICTIONARY,  Regdictionary,  "regdictionary",  BIGINT)
DECLARE_PG_TYPE(REGCOLLATION,   Regcollation,   "regcollation",   BIGINT)
DECLARE_PG_TYPE(TID,            Tid,            "tid",            BIGINT)
DECLARE_PG_TYPE(CID,            Cid,            "cid",            BIGINT)
DECLARE_PG_TYPE(XID,            Xid,            "xid",            BIGINT)
DECLARE_PG_TYPE(XID8,           Xid8,           "xid8",           BIGINT)
DECLARE_PG_TYPE(NAME,           Name,           "name",           VARCHAR)
// clang-format on

#undef DECLARE_PG_TYPE

}  // namespace sdb::pg
