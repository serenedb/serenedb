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
#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>
#include <fast_float/fast_float.h>

#include <cmath>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/settings.h"
#include "pg/catalog/tables/tables.h"
#include "query/config.h"

ABSL_DECLARE_FLAG(uint64_t, max_connections);

namespace sdb::pg {
namespace {

struct GucUnit {
  std::string_view name;
  double factor;
  bool memory;
};

constexpr GucUnit kGucUnits[] = {
  {"B", 1, true},
  {"kB", 1024, true},
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

constexpr std::string_view kMemoryUnitsHint =
  "Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and "
  "\"TB\".";
constexpr std::string_view kTimeUnitsHint =
  "Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", "
  "and \"d\".";

const GucUnit* FindUnit(std::string_view name) {
  const auto* it = absl::c_find_if(
    kGucUnits, [&](const GucUnit& unit) { return unit.name == name; });
  return it == std::end(kGucUnits) ? nullptr : it;
}

std::optional<double> GucBaseValue(const Guc& guc, std::string_view text) {
  const auto value = absl::StripAsciiWhitespace(text);
  double number = 0;
  const auto [end, error] =
    fast_float::from_chars(value.data(), value.data() + value.size(), number);
  if (error != std::errc{}) {
    return std::nullopt;
  }
  const auto suffix = absl::StripLeadingAsciiWhitespace(
    value.substr(static_cast<size_t>(end - value.data())));
  if (suffix.empty()) {
    return number;
  }
  const auto* base = FindUnit(guc.unit);
  const auto* unit = FindUnit(suffix);
  if (!base || !unit || unit->memory != base->memory) {
    return std::nullopt;
  }
  return number * unit->factor / base->factor;
}

std::optional<bool> GucBool(std::string_view text) {
  const auto value = absl::AsciiStrToLower(text);
  const auto abbreviates = [&](std::string_view word, size_t shortest) {
    return value.size() >= shortest && word.starts_with(value);
  };
  if (abbreviates("true", 1) || abbreviates("yes", 1) || abbreviates("on", 2) ||
      value == "1") {
    return true;
  }
  if (abbreviates("false", 1) || abbreviates("no", 1) ||
      abbreviates("off", 2) || value == "0") {
    return false;
  }
  return std::nullopt;
}

}  // namespace

const Guc* FindGuc(std::string_view name) {
  const auto it = absl::c_find_if(kGucs, [&](const Guc& guc) {
    return absl::EqualsIgnoreCase(guc.name, name);
  });
  return it == std::end(kGucs) ? nullptr : &*it;
}

std::string MaxConnectionsSetting() {
  return absl::StrCat(absl::GetFlag(FLAGS_max_connections));
}

int64_t CheckGucInteger(const Guc& guc, std::string_view text) {
  const auto value = GucBaseValue(guc, text);
  if (!value) {
    const auto* base = FindUnit(guc.unit);
    std::string_view hint;
    if (base) {
      hint = base->memory ? kMemoryUnitsHint : kTimeUnitsHint;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", guc.name, "\": \"", text, "\""),
      ERR_HINT(hint));
  }
  const auto rounded = std::round(*value);
  double min = 0;
  double max = 0;
  if (!absl::SimpleAtod(guc.min_val, &min) ||
      !absl::SimpleAtod(guc.max_val, &max) || rounded < min || rounded > max) {
    const auto unit =
      guc.unit.empty() ? std::string{} : absl::StrCat(" ", guc.unit);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(absl::StrFormat("%.0f", rounded), unit,
              " is outside the valid range for parameter \"", guc.name, "\" (",
              guc.min_val, unit, " .. ", guc.max_val, unit, ")"));
  }
  return static_cast<int64_t>(rounded);
}

std::string GucIntegerText(const Guc& guc, int64_t value) {
  const auto* base = FindUnit(guc.unit);
  if (!base || value == 0) {
    return absl::StrCat(value);
  }
  for (const auto& unit : std::views::reverse(kGucUnits)) {
    const auto ratio = static_cast<int64_t>(unit.factor / base->factor);
    if (unit.memory == base->memory && ratio > 1 && value % ratio == 0) {
      return absl::StrCat(value / ratio, unit.name);
    }
  }
  return absl::StrCat(value, base->name);
}

std::string NormalizeGucValue(const Guc& guc, std::string_view text) {
  if (guc.vartype == "integer") {
    return GucIntegerText(guc, CheckGucInteger(guc, text));
  }
  const auto value = absl::StripAsciiWhitespace(text);
  if (guc.vartype == "bool") {
    if (const auto flag = GucBool(value)) {
      return *flag ? "on" : "off";
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("parameter \"", guc.name, "\" requires a Boolean value"));
  }
  if (guc.vartype == "enum") {
    const auto it = absl::c_find_if(guc.enumvals, [&](std::string_view option) {
      return absl::EqualsIgnoreCase(option, value);
    });
    if (it != guc.enumvals.end()) {
      return std::string{*it};
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", guc.name, "\": \"", text, "\""),
      ERR_HINT("Available values: ", absl::StrJoin(guc.enumvals, ", "), "."));
  }
  return std::string{text};
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

std::optional<std::string> InBaseUnit(std::optional<std::string> text,
                                      const Guc& guc) {
  if (!text || guc.unit.empty()) {
    return text;
  }
  if (const auto value = GucBaseValue(guc, *text)) {
    return absl::StrCat(std::llround(*value));
  }
  return text;
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
    Col<"boot_val">(
      [](const auto& row) { return InBaseUnit(row.Boot(), *row.guc); }),
    Col<"reset_val">(
      [](const auto& row) { return InBaseUnit(row.Boot(), *row.guc); }),
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
    Col<"boot_val">([](const auto& row) -> const auto& { return row.Boot(); }),
    Col<"reset_val">([](const auto& row) -> const auto& { return row.Boot(); }),
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
