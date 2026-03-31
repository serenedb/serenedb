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

#pragma once

#include <velox/type/HugeInt.h>
#include <velox/type/SimpleFunctionApi.h>
#include <velox/type/Type.h>

#include "basics/fwd.h"

namespace sdb::aql {

// TODO: move aql to a separate header

velox::TypePtr COLLECTION();

bool IsCollection(const velox::TypePtr& type);

}  // namespace sdb::aql
namespace sdb::pg {

velox::TypePtr VOID();
bool IsVoid(const velox::TypePtr& type);

velox::TypePtr PROCEDURE();
bool IsProcedure(const velox::TypePtr& type);

velox::TypePtr INTERVAL();
bool IsInterval(const velox::TypePtr& type);
bool IsInterval(const velox::Type& type);

struct IntervalTrait {
  using type = velox::int128_t;                           // NOLINT
  static constexpr const char* typeName = "PG_INTERVAL";  // NOLINT
};
using Interval = velox::CustomType<IntervalTrait>;

#define SDB_DECLARE_PG_TYPE(base, func, name, lateral) \
  velox::TypePtr func();                               \
  bool Is##name(const velox::TypePtr& type);           \
  bool Is##name(const velox::Type& type);              \
  struct name##Trait {                                 \
    using type = base;                                 \
    static constexpr const char* typeName = lateral;   \
  };                                                   \
  using name##CustomType = velox::CustomType<name##Trait>

SDB_DECLARE_PG_TYPE(velox::Varchar, PGUNKNOWN, Unknown, "PG_UNKNOWN");
SDB_DECLARE_PG_TYPE(velox::Varchar, PGNAME, Name, "PG_NAME");

SDB_DECLARE_PG_TYPE(int64_t, PGOID, Oid, "PG_OID");

SDB_DECLARE_PG_TYPE(int64_t, REGPROC, Regproc, "PG_REGPROC");
SDB_DECLARE_PG_TYPE(int64_t, REGCLASS, Regclass, "PG_REGCLASS");
SDB_DECLARE_PG_TYPE(int64_t, REGTYPE, Regtype, "PG_REGTYPE");
SDB_DECLARE_PG_TYPE(int64_t, REGNAMESPACE, Regnamespace, "PG_REGNAMESPACE");
SDB_DECLARE_PG_TYPE(int64_t, REGOPER, Regoper, "PG_REGOPER");
SDB_DECLARE_PG_TYPE(int64_t, REGOPERATOR, Regoperator, "PG_REGOPERATOR");
SDB_DECLARE_PG_TYPE(int64_t, REGPROCEDURE, Regprocedure, "PG_REGPROCEDURE");
SDB_DECLARE_PG_TYPE(int64_t, REGROLE, Regrole, "PG_REGROLE");
SDB_DECLARE_PG_TYPE(int64_t, REGCONFIG, Regconfig, "PG_REGCONFIG");
SDB_DECLARE_PG_TYPE(int64_t, REGDICTIONARY, Regdictionary, "PG_REGDICTIONARY");
SDB_DECLARE_PG_TYPE(int64_t, REGCOLLATION, Regcollation, "PG_REGCOLLATION");

SDB_DECLARE_PG_TYPE(int64_t, PGTID, Tid, "PG_TID");
SDB_DECLARE_PG_TYPE(int64_t, PGCID, Cid, "PG_CID");
SDB_DECLARE_PG_TYPE(int64_t, PGXID, Xid, "PG_XID");
SDB_DECLARE_PG_TYPE(int64_t, PGXID8, Xid8, "PG_XID8");

#undef SDB_DECLARE_PG_TYPE

void RegisterTypes();

}  // namespace sdb::pg
