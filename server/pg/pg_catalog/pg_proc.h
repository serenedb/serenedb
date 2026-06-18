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

// https://www.postgresql.org/docs/18/catalog-pg-proc.html
// NOLINTBEGIN
struct PgProc {
  static constexpr uint64_t kId = 138;
  static constexpr std::string_view kName = "pg_proc";

  enum class Prokind : char {
    Function = 'f',
    Procedure = 'p',
    Aggregate = 'a',
    Window = 'w',
  };
  enum class Proargmode : char {
    In = 'i',
    Out = 'o',
    InOut = 'b',
    Variadic = 'v',
    Table = 't',
  };

  Oid oid;
  Name proname;
  Oid pronamespace;
  Oid proowner;
  Oid prolang;
  float procost;
  float prorows;
  Oid provariadic;
  Regproc prosupport;
  Prokind prokind;
  bool prosecdef;
  bool proleakproof;
  bool proisstrict;
  bool proretset;
  enum class Provolatile : char {
    Immutable = 'i',
    Stable = 's',
    Volatile = 'v',
  };
  enum class Proparallel : char {
    Safe = 's',
    Restricted = 'r',
    Unsafe = 'u',
  };
  Provolatile provolatile;
  Proparallel proparallel;
  int16_t pronargs;
  int16_t pronargdefaults;
  Oid prorettype;
  Vector<Oid> proargtypes;
  Array<Oid> proallargtypes;
  Array<Proargmode> proargmodes;
  Array<Text> proargnames;
  PgNodeTree proargdefaults;
  Array<Oid> protrftypes;
  Text prosrc;
  Text probin;
  PgNodeTree prosqlbody;
  Array<Text> proconfig;
  AclColumn proacl;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<PgProc>::GetTableData();

}  // namespace sdb::pg
