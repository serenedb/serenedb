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

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/view-pg-hba-file-rules.html
// Backed by the live HBA ruleset (network::pg::hba::RenderHbaRules), not a
// file. file_name / line_number / error are always NULL: rules come from the
// `hba` GUC, not a file, and a ruleset that fails to parse never becomes live.
// NOLINTBEGIN
struct PgHbaFileRule {
  static constexpr uint64_t kId = 165;
  static constexpr std::string_view kName = "pg_hba_file_rules";
  static constexpr bool kSuperuserOnly = true;

  int32_t rule_number;
  Empty file_name;
  Empty line_number;
  Text type;
  Array<Text> database;
  Array<Text> user_name;
  Text address;
  Text netmask;
  Text auth_method;
  Array<Text> options;
  Empty error;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<PgHbaFileRule>::GetTableData();

}  // namespace sdb::pg
