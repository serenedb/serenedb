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

// https://www.postgresql.org/docs/current/view-pg-settings.html
// We use the following construction:
// pg_show_all_settings() -> select * from sdb_show_all_settings
// NOLINTBEGIN
struct SdbShowAllSettings {
  static constexpr uint64_t kId =
    999998;  // TODO(mkornaukhov): assign proper OID
  static constexpr std::string_view kName = "sdb_show_all_settings";

  std::string name;
  std::string value;
  std::string description;
};
// NOLINTEND

template<>
std::vector<velox::VectorPtr>
SystemTableSnapshot<SdbShowAllSettings>::GetTableData(
  velox::memory::MemoryPool& pool) {
  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<SdbShowAllSettings>);
  std::vector<SdbShowAllSettings> values;

  _config.VisitFullDescription([&](std::string_view name,
                                   std::string_view value,
                                   std::string_view description) {
    values.emplace_back(std::string{name}, std::string{value},
                        std::string{description});
  });

  boost::pfr::for_each_field(
    SdbShowAllSettings{}, [&]<typename Field>(const Field& field) {
      auto column = CreateColumn<Field>(values.size(), &pool);
      result.push_back(std::move(column));
    });

  static constexpr uint64_t kNullMask = MaskFromNonNulls({
    GetIndex(&SdbShowAllSettings::name),
    GetIndex(&SdbShowAllSettings::value),
    GetIndex(&SdbShowAllSettings::description),
  });
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
