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

// https://www.postgresql.org/docs/18/catalog-pg-class.html
// NOLINTBEGIN
struct PgClass {
  static constexpr uint64_t kId = 110;
  static constexpr std::string_view kName = "pg_class";

  enum class Relpersistence : char {
    Permanent = 'p',
    Unlogged = 'u',
    Temporary = 't',
  };

  enum class Relkind : char {
    OrdinaryTable = 'r',
    Index = 'i',
    Sequence = 'S',
    TOASTTable = 't',
    View = 'v',
    MaterializedView = 'm',
    CompositeType = 'c',
    ForeignTable = 'f',
    PartitionedTable = 'p',
    PartitionedIndex = 'I',
  };

  enum class Relreplident : char {
    Default = 'd',
    Nothing = 'n',
    AllColumns = 'f',
    Index = 'i',
  };

  Oid oid;
  Name relname;
  Oid relnamespace;
  Oid reltype;
  Oid reloftype;
  Oid relowner;
  Oid relam;
  Oid relfilenode;
  Oid reltablespace;
  int32_t relpages;
  float reltuples;
  int32_t relallvisible;
  int32_t relallfrozen;
  Oid reltoastrelid;
  bool relhasindex;
  bool relisshared;
  Relpersistence relpersistence;
  Relkind relkind;
  int16_t relnatts;
  int16_t relchecks;
  bool relhasrules;
  bool relhastriggers;
  bool relhassubclass;
  bool relrowsecurity;
  bool relforcerowsecurity;
  bool relispopulated;
  Relreplident relreplident;
  bool relispartition;
  Oid relrewrite;
  Xid relfrozenxid;
  Xid relminmxid;
  AclColumn relacl;
  Array<Text> reloptions;
  PgNodeTree relpartbound;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<PgClass>::GetTableData();

}  // namespace sdb::pg
