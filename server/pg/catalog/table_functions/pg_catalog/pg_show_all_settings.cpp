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
#include <absl/synchronization/mutex.h>

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "query/config.h"

namespace sdb::pg {
namespace {

std::string_view VarType(const duckdb::LogicalType& type) {
  if (type.id() == duckdb::LogicalTypeId::BOOLEAN) {
    return "bool";
  }
  if (type.IsIntegral()) {
    return "integer";
  }
  if (type.IsNumeric()) {
    return "real";
  }
  return "string";
}

std::optional<std::string> Display(duckdb::ClientContext& context,
                                   const duckdb::Value& value) {
  if (value.IsNull()) {
    return std::nullopt;
  }
  return duckdb::Settings::FormatDisplayValue(context, value).ToString();
}

struct SettingNames {
  const duckdb::DBConfig* config;
  duckdb::idx_t version;
  std::vector<duckdb::Identifier> names;
};

absl::Mutex gSettingNamesLock;
std::shared_ptr<const SettingNames> gSettingNames
  ABSL_GUARDED_BY(gSettingNamesLock);

SystemRows<duckdb::Identifier> LoadSettings(SystemScan& scan) {
  auto& config = duckdb::DBConfig::GetConfig(scan.Context());
  const auto version = config.user_settings.GetSettings().version;
  std::shared_ptr<const SettingNames> cached;
  {
    absl::ReaderMutexLock guard{&gSettingNamesLock};
    cached = gSettingNames;
  }
  if (!cached || cached->config != &config || cached->version != version) {
    auto built = std::make_shared<SettingNames>(
      SettingNames{&config, version, duckdb::DBConfig::GetOptionNames()});
    built->names.append_range(std::views::keys(config.GetExtensionSettings()));
    absl::c_sort(built->names);
    absl::MutexLock guard{&gSettingNamesLock};
    gSettingNames = built;
    cached = std::move(built);
  }
  return {cached->names, cached};
}

constexpr ArrayKey<duckdb::Identifier> kKeys[] = {
  {kPgShowAllSettingsSql["name"],
   SortedBy<duckdb::Identifier, std::identity{}>},
};

struct Setting {
  duckdb::ClientContext& context;
  const duckdb::Identifier& name;
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  const duckdb::ExtensionOption& extension;
  mutable std::optional<std::optional<std::string>> boot;
  mutable std::optional<duckdb::Value> current;
  mutable std::optional<std::optional<std::string>> displayed;
  mutable bool session = false;

  const std::optional<std::string>& Boot() const {
    if (!boot) {
      auto initial = extension.default_value;
      duckdb::DBConfig::TryGetDefaultValue(option, initial);
      boot.emplace(Display(context, initial));
    }
    return *boot;
  }

  const duckdb::Value& Current() const {
    if (!current) {
      auto& value = current.emplace();
      if (option && option->get_setting) {
        value = option->get_setting(context);
      } else if (auto lookup = context.TryGetCurrentSetting(name, value)) {
        session = lookup.GetScope() == duckdb::SettingScope::LOCAL;
      }
    }
    return *current;
  }

  const std::optional<std::string>& Displayed() const {
    if (!displayed) {
      displayed.emplace(Display(context, Current()));
    }
    return *displayed;
  }

  bool Session() const {
    Current();
    return session;
  }
};

class PgShowAllSettings final : public SystemTableScan<kPgShowAllSettingsSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr SystemColumnKey kColumnKeys[] = {
    {kSql["name"], SystemKey::Unique},
  };

  static constexpr std::tuple kSources{
    ArraySource<duckdb::Identifier>{&LoadSettings, kKeys, {}}};

  static constexpr auto kSetting = Shape<kSql, const Setting>(
    Col<"name">([](const auto& row) -> const auto& { return row.name; }),
    Col<"short_desc">([](const auto& row) {
      return row.option ? std::string_view{row.option->description}
                        : std::string_view{row.extension.description};
    }),
    Col<"source">([](const auto& row) {
      return std::string_view{row.Session() ? "session" : "default"};
    }),
    Col<"pending_restart">([](const auto&) { return false; }),
    Col<"setting">(
      [](const auto& row) -> const auto& { return row.Displayed(); }),
    Col<"boot_val">([](const auto& row) -> const auto& { return row.Boot(); }),
    Col<"reset_val">([](const auto& row) -> const auto& { return row.Boot(); }),
    Col<"category">(
      [](const auto&) { return std::string_view{"Customized Options"}; }),
    Col<"context">([](const auto& row) {
      return std::string_view{
        IsUnchangeableSetting(row.name.GetIdentifierName()) ? "internal"
                                                            : "user"};
    }),
    Col<"vartype">([](const auto& row) {
      return VarType(row.option ? duckdb::DBConfig::ParseLogicalType(
                                    row.option->parameter_type)
                                : row.extension.type);
    }));

  void Row(const duckdb::Identifier& name) {
    auto& context = Context();
    const auto option = duckdb::DBConfig::GetOptionByName(name);
    duckdb::ExtensionOption extension;
    if (!option) {
      duckdb::DBConfig::GetConfig(context).TryGetExtensionOption(name,
                                                                 extension);
    }
    if ((option ? option->is_debug || option->is_deprecated
                : extension.is_debug || extension.is_deprecated) ||
        (context.setting_visibility &&
         !context.setting_visibility(context, name.GetIdentifierName()))) {
      return;
    }
    Emit<kSetting>({.context = context,
                    .name = name,
                    .option = option,
                    .extension = extension});
  }
};

}  // namespace

SystemTable gPgShowAllSettings = SystemTableOf<PgShowAllSettings>();

}  // namespace sdb::pg
