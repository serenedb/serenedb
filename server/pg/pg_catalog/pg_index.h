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

// https://www.postgresql.org/docs/18/catalog-pg-index.html
// NOLINTBEGIN
struct PgIndex {
  static constexpr uint64_t kId = 125;
  static constexpr std::string_view kName = "pg_index";

  enum class Indisunique : char {
    Unique = 't',
    NonUnique = 'f',
  };

  enum class Indisprimary : char {
    Primary = 't',
    NonPrimary = 'f',
  };

  enum class Indisclustered : char {
    Clustered = 't',
    NonClustered = 'f',
  };

  enum class Indisvalid : char {
    Valid = 't',
    Invalid = 'f',
  };

  enum class Indisready : char {
    Ready = 't',
    NotReady = 'f',
  };

  enum class Indisreplident : char {
    Default = 'd',
    Nothing = 'n',
    AllColumns = 'f',
    Index = 'i',
  };

  Oid indexrelid;
  Oid indrelid;
  int16_t indnatts;
  int16_t indnkeyatts;
  bool indisunique;
  bool indnullsnotdistinct;
  bool indisprimary;
  bool indisexclusion;
  bool indimmediate;
  bool indisclustered;
  bool indisvalid;
  bool indcheckxmin;
  bool indisready;
  bool indislive;
  bool indisreplident;
  Vector<int16_t> indkey;
  Vector<Oid> indcollation;
  Vector<Oid> indclass;
  Vector<int16_t> indoption;
  PgNodeTree indexprs;
  PgNodeTree indpred;
};
// NOLINTEND

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgIndex>::GetTableData(
  velox::memory::MemoryPool& pool);

}  // namespace sdb::pg
