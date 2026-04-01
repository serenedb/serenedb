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

// https://www.postgresql.org/docs/18/catalog-pg-aggregate.html
// NOLINTBEGIN
struct PgAggregate {
  static constexpr uint64_t kId = 101;
  static constexpr std::string_view kName = "pg_aggregate";

  enum class Aggkind : char {
    Normal = 'n',
    OrderedSet = 'o',
    HypotheticalSet = 'h',
  };

  enum class Aggfinalmodify : char {
    ReadOnly = 'r',
    Special = 's',
    Writes = 'w',
  };

  Regproc aggfnoid;
  Aggkind aggkind;
  int16_t aggnumdirectargs;

  Regproc aggtransfn;
  Regproc aggfinalfn;
  Regproc aggcombinefn;
  Regproc aggserialfn;
  Regproc aggdeserialfn;

  Regproc aggmtransfn;
  Regproc aggminvtransfn;
  Regproc aggmfinalfn;

  bool aggfinalextra;
  bool aggmfinalextra;

  Aggfinalmodify aggfinalmodify;
  Aggfinalmodify aggmfinalmodify;

  Oid aggsortop;
  Oid aggtranstype;
  int32_t aggtransspace;
  Oid aggmtranstype;
  Oid aggmtransspace;

  Text agginitval;
  Text aggminitval;
};
// NOLINTEND

}  // namespace sdb::pg
