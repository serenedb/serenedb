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

// https://www.postgresql.org/docs/18/catalog-pg-rewrite.html
// NOLINTBEGIN
struct PgRewrite {
  static constexpr uint64_t kId = 144;
  static constexpr std::string_view kName = "pg_rewrite";

  enum class EvType : char {
    Select = '1',
    Update = '2',
    Insert = '3',
    Delete = '4',
  };

  enum class EvEnabled : char {
    Origin = 'O',
    Disabled = 'D',
    Replica = 'R',
    Always = 'A',
  };

  Oid oid;
  Name rulename;
  Oid ev_class;
  EvType ev_type;
  EvEnabled ev_enabled;
  bool is_instead;
  PgNodeTree ev_qual;
  PgNodeTree ev_action;
};
// NOLINTEND

inline constexpr uint64_t kViewRuleBit = uint64_t{1} << 60;

inline constexpr Oid ViewRuleOid(uint64_t view_oid) {
  return Oid{view_oid | kViewRuleBit};
}

template<>
catalog::MaterializedData SystemTableSnapshot<PgRewrite>::GetTableData();

}  // namespace sdb::pg
