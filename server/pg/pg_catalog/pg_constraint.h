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

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/catalog-pg-constraint.html
// NOLINTBEGIN
struct PgConstraint {
  static constexpr uint64_t kId = 112;
  static constexpr std::string_view kName = "pg_constraint";

  enum class Contype : char {
    Check = 'c',
    ForeignKey = 'f',
    NotNull = 'n',
    PrimaryKey = 'p',
    Unique = 'u',
    Trigger = 't',
    Exclusion = 'x',
  };
  enum class Confchgtype : char {
    NoAction = 'a',
    Restrict = 'r',
    Cascade = 'c',
    SetNull = 'n',
    SetDefault = 'd',
  };
  enum class Confmatchtype : char {
    Full = 'f',
    Partial = 'p',
    Simple = 's',
  };

  Oid oid;
  Name conname;
  Oid connamespace;
  Contype contype;
  bool condeferrable;
  bool condeferred;
  bool conenforced;
  bool convalidated;
  Oid conrelid;
  Oid contypid;
  Oid conindid;
  Oid conparentid;
  Oid confrelid;
  Confchgtype confupdtype;
  Confchgtype confdeltype;
  Confmatchtype confmatchtype;
  bool conislocal;
  int16_t coninhcount;
  bool connoinherit;
  bool conperiod;
  Array<int16_t> conkey;
  Array<int16_t> confkey;
  Array<Oid> conpfeqop;
  Array<Oid> conppeqop;
  Array<Oid> conffeqop;
  Array<int16_t> confdelsetcols;
  Array<Oid> conexclop;
  PgNodeTree conbin;
};
// NOLINTEND

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgConstraint>::GetTableData(
  velox::memory::MemoryPool& pool);

}  // namespace sdb::pg
