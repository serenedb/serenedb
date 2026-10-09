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
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <cmath>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/settings.h"
#include "pg/catalog/tables/tables.h"
#include "query/config.h"

namespace sdb::pg {

const Guc* FindGuc(std::string_view name) {
  const auto it = absl::c_find_if(kGucs, [&](const Guc& guc) {
    return absl::EqualsIgnoreCase(guc.name, name);
  });
  return it == std::end(kGucs) ? nullptr : &*it;
}

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

struct GucUnit {
  std::string_view name;
  double factor;
  bool memory;
};

constexpr GucUnit kGucUnits[] = {
  {"B", 1, true},
  {"kB", 1024, true},
  {"8kB", 8 * 1024, true},
  {"MB", 1024.0 * 1024, true},
  {"GB", 1024.0 * 1024 * 1024, true},
  {"TB", 1024.0 * 1024 * 1024 * 1024, true},
  {"us", 1, false},
  {"ms", 1000, false},
  {"s", 1000.0 * 1000, false},
  {"min", 60.0 * 1000 * 1000, false},
  {"h", 3600.0 * 1000 * 1000, false},
  {"d", 86400.0 * 1000 * 1000, false},
};

std::optional<std::string> InBaseUnit(std::optional<std::string> text,
                                      const Guc& guc) {
  const auto find = [](std::string_view name) {
    return absl::c_find_if(
      kGucUnits, [&](const GucUnit& unit) { return unit.name == name; });
  };
  const auto base = find(guc.unit);
  if (!text || base == std::end(kGucUnits)) {
    return text;
  }
  const std::string_view value = absl::StripAsciiWhitespace(*text);
  const auto split = static_cast<size_t>(
    absl::c_find_if(value, absl::ascii_isalpha) - value.begin());
  const auto unit = find(absl::StripAsciiWhitespace(value.substr(split)));
  double number = 0;
  if (unit == std::end(kGucUnits) || unit->memory != base->memory ||
      !absl::SimpleAtod(value.substr(0, split), &number)) {
    return text;
  }
  const auto converted = number * unit->factor / base->factor;
  if (guc.vartype == "integer") {
    return absl::StrCat(std::llround(converted));
  }
  return absl::StrCat(converted);
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
  {kPgSettingsSql["name"], SortedBy<duckdb::Identifier, std::identity{}>},
};

struct Setting {
  duckdb::ClientContext& context;
  const duckdb::Identifier& name;
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  const duckdb::ExtensionOption& extension;
  const Guc* guc;
  mutable std::optional<std::optional<std::string>> boot;
  mutable std::optional<duckdb::Value> current;
  mutable bool session;

  const std::optional<std::string>& Boot() const {
    if (!boot) {
      auto initial = extension.default_value;
      duckdb::DBConfig::TryGetDefaultValue(option, initial);
      auto display = Display(context, initial);
      if (!display && guc) {
        display = std::string{guc->setting};
      }
      boot.emplace(std::move(display));
    }
    return *boot;
  }

  const duckdb::Value& Current() const {
    if (!current) {
      auto& value = current.emplace();
      if (option && option->get_setting) {
        value = option->get_setting(context);
        session = guc && Display(context, value) != Boot();
      } else if (auto lookup = context.TryGetCurrentSetting(name, value)) {
        session = lookup.GetScope() == duckdb::SettingScope::LOCAL;
      }
    }
    return *current;
  }

  bool Session() const {
    Current();
    return session;
  }
};

constexpr std::tuple kSettingColumns{
  Col<"name">([](const auto& row) -> const auto& { return row.name; }),
  Col<"source">([](const auto& row) {
    return std::string_view{row.Session() ? "session" : "default"};
  }),
  Col<"boot_val">([](const auto& row) -> const auto& { return row.Boot(); }),
  Col<"reset_val">([](const auto& row) -> const auto& { return row.Boot(); }),
  Col<"pending_restart">([](const auto&) { return false; })};

class PgSettings final : public SystemTableScan<kPgSettingsSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<duckdb::Identifier>{&LoadSettings, kKeys}};

  static constexpr auto kGuc = Shape<kSql, const Setting>(
    kSettingColumns, Col<"setting">([](const auto& row) {
      return InBaseUnit(Display(row.context, row.Current()), *row.guc);
    }),
    Col<"short_desc">([](const auto& row) { return row.guc->short_desc; }),
    Col<"unit">([](const auto& row) { return NonEmpty(row.guc->unit); }),
    Col<"category">([](const auto& row) { return row.guc->category; }),
    Col<"extra_desc">(
      [](const auto& row) { return NonEmpty(row.guc->extra_desc); }),
    Col<"context">([](const auto& row) { return row.guc->context; }),
    Col<"vartype">([](const auto& row) { return row.guc->vartype; }),
    Col<"min_val">([](const auto& row) { return NonEmpty(row.guc->min_val); }),
    Col<"max_val">([](const auto& row) { return NonEmpty(row.guc->max_val); }),
    Col<"enumvals">(
      [](const auto& row) { return NonEmpty(row.guc->enumvals); }));

  static constexpr auto kCustom = Shape<kSql, const Setting>(
    kSettingColumns, Col<"setting">([](const auto& row) {
      return Display(row.context, row.Current());
    }),
    Col<"short_desc">([](const auto& row) {
      return row.option ? std::string_view{row.option->description}
                        : std::string_view{row.extension.description};
    }),
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
    const Setting setting{
      context, name, option, extension, FindGuc(name.GetIdentifierName()),
      {},      {},   false};
    if (setting.guc) {
      Emit<kGuc>(setting);
    } else {
      Emit<kCustom>(setting);
    }
  }
};

}  // namespace

SystemTable gPgSettings = SystemTableOf<PgSettings>();

}  // namespace sdb::pg
