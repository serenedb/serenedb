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

#include <absl/algorithm/container.h>
#include <absl/flags/commandlineflag.h>
#include <absl/flags/reflection.h>

#include <ranges>
#include <string_view>
#include <vector>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemSqlColumn kSdbSettingsColumns[] = {
  {"name", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"setting", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"unit", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"category", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"short_desc", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"context", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"vartype", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"source", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"min_val", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"max_val", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"boot_val", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"reset_val", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"pending_restart", kBool, 0, false, false, 0, 0, SystemKey::None},
};

constexpr SystemSql kSdbSettingsSql{.oid = kSdbSettingsTable,
                                    .schema = "pg_catalog",
                                    .name = "sdb_settings",
                                    .relkind = 'r',
                                    .replident = 'n',
                                    .columns = kSdbSettingsColumns};

std::string_view VarType(const absl::CommandLineFlag& flag) {
  if (flag.IsOfType<bool>()) {
    return "bool";
  }
  if (flag.IsOfType<int32_t>() || flag.IsOfType<int64_t>() ||
      flag.IsOfType<uint32_t>() || flag.IsOfType<uint64_t>()) {
    return "integer";
  }
  if (flag.IsOfType<float>() || flag.IsOfType<double>()) {
    return "real";
  }
  return "string";
}

constexpr std::string_view kSecretFlags[] = {"auth_password", "auth_api_key",
                                             "auth_bearer_token"};

SystemRows<const absl::CommandLineFlag*> LoadFlags(SystemScan&) {
  static const auto kFlags = [] {
    auto flags = std::views::values(absl::GetAllFlags()) |
                 std::ranges::to<std::vector<const absl::CommandLineFlag*>>();
    absl::c_sort(flags, [](const absl::CommandLineFlag* lhs,
                           const absl::CommandLineFlag* rhs) {
      return lhs->Name() < rhs->Name();
    });
    return flags;
  }();
  return std::span<const absl::CommandLineFlag* const>{kFlags};
}

struct Flag {
  const absl::CommandLineFlag& flag;
  mutable std::optional<std::string> current;
  mutable std::optional<std::string> boot;

  std::string_view Current() const {
    if (!current) {
      current = flag.CurrentValue();
    }
    return *current;
  }

  std::string_view Boot() const {
    if (!boot) {
      boot = flag.DefaultValue();
    }
    return *boot;
  }
};

class SdbSettings final : public SystemTableScan<kSdbSettingsSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<const absl::CommandLineFlag*>{&LoadFlags, {}, {}}};

  static constexpr auto kFlag = Shape<kSql, const Flag>(
    Col<"name">(
      [](const auto& row) { return std::string_view{row.flag.Name()}; }),
    Col<"setting">([](const auto& row) -> std::string_view {
      if (absl::c_linear_search(kSecretFlags, row.flag.Name()) &&
          !row.Current().empty()) {
        return "***";
      }
      return row.Current();
    }),
    Col<"short_desc">([](const auto& row) { return row.flag.Help(); }),
    Col<"context">([](const auto&) { return std::string_view{"postmaster"}; }),
    Col<"vartype">([](const auto& row) { return VarType(row.flag); }),
    Col<"source">([](const auto& row) {
      return std::string_view{row.Current() == row.Boot() ? "default"
                                                          : "command line"};
    }),
    Col<"boot_val">([](const auto& row) { return row.Boot(); }),
    Col<"reset_val">([](const auto& row) { return row.Boot(); }),
    Col<"pending_restart">([](const auto&) { return false; }));

  void Row(const absl::CommandLineFlag* flag) { Emit<kFlag>({*flag, {}, {}}); }
};

}  // namespace

SystemTable gSdbSettings = SystemTableOf<SdbSettings>();

}  // namespace sdb::pg
