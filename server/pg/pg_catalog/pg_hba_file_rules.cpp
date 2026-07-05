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

#include "pg/pg_catalog/pg_hba_file_rules.h"

#include <vector>

#include "network/pg/hba.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgHbaFileRule>::GetTableData() {
  // Keep the rendered rules alive: the row structs hold Text (string_view) and
  // Array<Text> spans that point into these strings and into the per-row Text
  // vectors below, so both must outlive WriteData.
  auto rendered = network::pg::hba::RenderHbaRules();
  std::vector<std::vector<Text>> db_storage;
  std::vector<std::vector<Text>> role_storage;
  std::vector<std::vector<Text>> opt_storage;
  db_storage.reserve(rendered.size());
  role_storage.reserve(rendered.size());
  opt_storage.reserve(rendered.size());

  std::vector<PgHbaFileRule> values;
  values.reserve(rendered.size());
  for (const auto& r : rendered) {
    db_storage.emplace_back(r.databases.begin(), r.databases.end());
    role_storage.emplace_back(r.roles.begin(), r.roles.end());
    opt_storage.emplace_back(r.options.begin(), r.options.end());
    values.push_back(PgHbaFileRule{
      .rule_number = static_cast<int32_t>(r.rule_number),
      .file_name = r.file_name,
      .type = r.type,
      .database = db_storage.back(),
      .user_name = role_storage.back(),
      .address = r.address,
      .netmask = r.netmask,
      .auth_method = r.auth_method,
      .options = opt_storage.back(),
    });
  }

  auto catalog = _config.CatalogSnapshot();
  auto result = CreateColumns<PgHbaFileRule>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], /*null_mask=*/0, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
