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

#include <absl/algorithm/container.h>
#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <cmath>
#include <duckdb/common/assert.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/types/string.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/search/detail/pattern_cache.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <iterator>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/role.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "query/config.h"
#include "query/config_variable_names.h"
#include "scheduler/job_scheduler.h"

ABSL_DECLARE_FLAG(uint64_t, max_connections);

namespace sdb {
namespace {

struct PgUnit {
  std::string_view name;
  double factor;
  bool memory;
};

constexpr PgUnit kPgUnits[] = {
  {"B", 1, true},
  {"kB", 1024, true},
  {"MB", 1024.0 * 1024, true},
  {"GB", 1024.0 * 1024 * 1024, true},
  {"TB", 1024.0 * 1024 * 1024 * 1024, true},
  {"us", 0.001, false},
  {"ms", 1, false},
  {"s", 1000, false},
  {"min", 60000, false},
  {"h", 3600000, false},
  {"d", 86400000, false},
};

constexpr std::string_view kMemoryUnitsHint =
  "Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and "
  "\"TB\".";
constexpr std::string_view kTimeUnitsHint =
  "Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", "
  "and \"d\".";

const PgUnit* FindPgUnit(std::string_view name) {
  const auto* it = absl::c_find_if(
    kPgUnits, [&](const PgUnit& unit) { return unit.name == name; });
  return it == std::end(kPgUnits) ? nullptr : it;
}

}  // namespace

int64_t ParseDurationMillis(std::string_view name, std::string_view text,
                            double unitless_ms) {
  const auto invalid = [&] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", name, "\": \"", text, "\""),
      ERR_HINT(kTimeUnitsHint));
  };
  const auto trimmed = absl::StripAsciiWhitespace(text);
  double number = 0;
  const auto [ptr, ec] = fast_float::from_chars(
    trimmed.data(), trimmed.data() + trimmed.size(), number);
  if (ec != std::errc{}) {
    invalid();
  }
  const auto unit = absl::StripLeadingAsciiWhitespace(
    trimmed.substr(static_cast<size_t>(ptr - trimmed.data())));
  double scale = unitless_ms;
  if (!unit.empty()) {
    const auto* found = FindPgUnit(unit);
    if (!found || found->memory) {
      invalid();
    }
    scale = found->factor;
  }
  const auto ms = std::round(number * scale);
  if (ms < 0 || ms > std::numeric_limits<int32_t>::max()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(absl::StrFormat("%g", ms),
                            " ms is outside the valid range for parameter \"",
                            name, "\" (0 ms .. 2147483647 ms)"));
  }
  return static_cast<int64_t>(ms);
}

std::optional<double> PgBaseValue(std::string_view unit,
                                  std::string_view text) {
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
  const auto* base = FindPgUnit(unit);
  const auto* given = FindPgUnit(suffix);
  if (!base || !given || given->memory != base->memory) {
    return std::nullopt;
  }
  return number * given->factor / base->factor;
}

duckdb::Value SettingRef::Global(const duckdb::DBConfig& config) const {
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  const auto index =
    config.TryGetSettingIndex(duckdb::Identifier{_name}, option);
  SDB_ASSERT(index.IsValid());
  duckdb::Value value;
  if (config.user_settings.TryGetSetting(index.GetIndex(), value)) {
    return value;
  }
  duckdb::ExtensionOption extension;
  const auto found = config.user_settings.TryGetExtensionOption(
    duckdb::Identifier{_name}, extension);
  SDB_ASSERT(found);
  return extension.default_value;
}

duckdb::Value SettingRef::Read(duckdb::ClientContext& context) const {
  auto& config = duckdb::DBConfig::GetConfig(context);
  auto slot = _slot.load(std::memory_order_relaxed);
  if (slot.config != &config) [[unlikely]] {
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    const auto index =
      config.TryGetSettingIndex(duckdb::Identifier{_name}, option);
    SDB_ASSERT(index.IsValid());
    slot = {.config = &config, .index = index.GetIndex()};
    _slot.store(slot, std::memory_order_relaxed);
  }
  duckdb::Value value;
  auto found = context.config.user_settings.TryGetSetting(config.user_settings,
                                                          slot.index, value);
  if (!found) [[unlikely]] {
    auto res = context.TryGetCurrentSetting(duckdb::Identifier{_name}, value);
    SDB_ASSERT(res);
  }
  SDB_ASSERT(!value.IsNull());
  return value;
}

uint32_t SettingRef::Int(duckdb::ClientContext& context) const {
  return Read(context).GetValue<uint32_t>();
}

double SettingRef::Double(duckdb::ClientContext& context) const {
  return Read(context).GetValue<double>();
}

bool SettingRef::Bool(duckdb::ClientContext& context) const {
  return Read(context).GetValue<bool>();
}

uint32_t SettingRef::Enum(duckdb::ClientContext& context,
                          std::span<const std::string_view> options) const {
  const auto v = Read(context);
  const std::string_view value = duckdb::StringValue::Get(v);
  for (uint32_t i = 0; i != options.size(); ++i) {
    if (absl::EqualsIgnoreCase(value, options[i])) {
      return i;
    }
  }
  return static_cast<uint32_t>(options.size());
}

using duckdb::LogicalTypeId;

// Defined in network/pg/hba.cpp; declared here to drive the `hba` GUC without
// pulling in the heavy hba/session headers (resolved at link time). Applies the
// ruleset and returns a formatted "line N: <msg>" error string on a parse
// failure (leaving the live ruleset untouched), or nullopt on success.
namespace network::pg::hba {

std::optional<std::string> SetHbaFromTextString(std::string_view text);

}  // namespace network::pg::hba
namespace {

template<irs::utils::detail::FixedString Name>
void RejectZero(duckdb::ClientContext&, duckdb::SetScope,
                duckdb::Value& value) {
  if (value.GetValue<uint64_t>() == 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", std::string_view{Name},
              "\": \"", value.ToString(), "\""));
  }
}

void NoticeIfChanged(duckdb::ClientContext& ctx, std::string_view name,
                     const duckdb::Value& value) {
  duckdb::Value current;
  if (!ctx.TryGetCurrentSetting(duckdb::Identifier{name}, current)) {
    return;
  }
  bool equal = false;
  if (current.type().id() == duckdb::LogicalTypeId::VARCHAR &&
      value.type().id() == duckdb::LogicalTypeId::VARCHAR &&
      !current.IsNull() && !value.IsNull()) {
    equal = absl::EqualsIgnoreCase(current.ToString(), value.ToString());
  } else {
    equal = duckdb::Value::NotDistinctFrom(current, value);
  }
  if (equal) {
    return;
  }
  connector::GetSereneDBContext(ctx).AddNotice(SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_WARNING),
    ERR_MSG(
      "parameter \"", name,
      "\" is accepted for compatibility but is not enforced by serened")));
}

std::string PgIntegerText(std::string_view unit, int64_t value) {
  const auto* base = FindPgUnit(unit);
  if (!base || value == 0) {
    return absl::StrCat(value);
  }
  for (const auto& larger : std::views::reverse(kPgUnits)) {
    const auto ratio = static_cast<int64_t>(larger.factor / base->factor);
    if (larger.memory == base->memory && ratio > 1 && value % ratio == 0) {
      return absl::StrCat(value / ratio, larger.name);
    }
  }
  return absl::StrCat(value, base->name);
}

int64_t CheckPgInteger(std::string_view name, const PgSetting& pg,
                       std::string_view text) {
  const auto value = PgBaseValue(pg.unit, text);
  if (!value) {
    const auto* base = FindPgUnit(pg.unit);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", name, "\": \"", text, "\""),
      ERR_HINT(!base          ? std::string_view{}
               : base->memory ? kMemoryUnitsHint
                              : kTimeUnitsHint));
  }
  const auto rounded = std::round(*value);
  double min = 0;
  double max = 0;
  if (!absl::SimpleAtod(pg.min_val, &min) ||
      !absl::SimpleAtod(pg.max_val, &max) || rounded < min || rounded > max) {
    const auto unit =
      pg.unit.empty() ? std::string{} : absl::StrCat(" ", pg.unit);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(absl::StrFormat("%.0f", rounded), unit,
              " is outside the valid range for parameter \"", name, "\" (",
              pg.min_val, unit, " .. ", pg.max_val, unit, ")"));
  }
  return static_cast<int64_t>(rounded);
}

std::string NormalizePgValue(std::string_view name, const PgSetting& pg,
                             std::string_view text) {
  if (pg.vartype == "integer") {
    return PgIntegerText(pg.unit, CheckPgInteger(name, pg, text));
  }
  const auto value = absl::StripAsciiWhitespace(text);
  if (pg.vartype == "enum") {
    const auto it = absl::c_find_if(pg.enumvals, [&](std::string_view option) {
      return absl::EqualsIgnoreCase(option, value);
    });
    if (it != pg.enumvals.end()) {
      return std::string{*it};
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid value for parameter \"", name, "\": \"", text, "\""),
      ERR_HINT("Available values: ", absl::StrJoin(pg.enumvals, ", "), "."));
  }
  return std::string{text};
}

template<irs::utils::detail::FixedString Name>
void NoOverwrite(duckdb::ClientContext& ctx, duckdb::SetScope,
                 duckdb::Value& value) {
  constexpr std::string_view kName{Name};
  if (!value.IsNull() && value.type().id() == duckdb::LogicalTypeId::VARCHAR) {
    value = duckdb::Value{
      NormalizePgValue(kName, FindVariable(kName)->pg, value.ToString())};
  }
  NoticeIfChanged(ctx, kName, value);
}

void SetStatementTimeout(duckdb::ClientContext& ctx, duckdb::SetScope scope,
                         duckdb::Value& value) {
  const auto ms = value.IsNull() ? 0
                                 : ParseDurationMillis("statement_timeout",
                                                       value.ToString(), 1);
  value = duckdb::Value{PgIntegerText("ms", ms)};
  duckdb::Settings::Set<duckdb::MaxExecutionTimeSetting>(
    ctx,
    scope == duckdb::SetScope::AUTOMATIC ? duckdb::SetScope::SESSION : scope,
    duckdb::Value::BIGINT(ms));
}

void ResetStatementTimeout(duckdb::ClientContext& ctx, duckdb::SetScope scope) {
  duckdb::Settings::Set<duckdb::MaxExecutionTimeSetting>(
    ctx,
    scope == duckdb::SetScope::AUTOMATIC ? duckdb::SetScope::SESSION : scope,
    duckdb::Value::BIGINT(0));
}

// PG's rule for a GUC whose effect reaches past the session that set it is
// PGC_SUSET: still readable by everyone, settable only by a superuser, 42501
// otherwise. Every developer knob that can damage or kill the backend is in
// that class -- zero_damaged_pages, ignore_checksum_failure,
// allow_system_table_mods, debug_discard_caches, wal_consistency_checking.
void RequireSuperuser(duckdb::ClientContext& ctx, std::string_view name,
                      std::string_view detail) {
  // No connection context means an internal connection (boot, background
  // work): the server acting on its own behalf, with no role to check.
  auto* conn = connector::GetSereneDBContextPtr(ctx);
  if (!conn || auth::ClosureFor(&ctx, conn->GetRoleId())->is_superuser) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied to set parameter \"", name, "\""),
                  ERR_DETAIL(detail));
}

template<irs::utils::detail::FixedString Name, int64_t UnitlessMs>
void SetServerDuration(duckdb::ClientContext& ctx, duckdb::SetScope,
                       duckdb::Value& value) {
  constexpr std::string_view kName{Name};
  RequireSuperuser(ctx, kName,
                   "Only roles with the SUPERUSER attribute may change "
                   "server-wide replication settings.");
  value = duckdb::Value{
    PgIntegerText("ms", ParseDurationMillis(kName, value.ToString(),
                                            static_cast<double>(UnitlessMs)))};
}

template<irs::utils::detail::FixedString Name>
void SetServerCount(duckdb::ClientContext& ctx, duckdb::SetScope,
                    duckdb::Value&) {
  RequireSuperuser(ctx, std::string_view{Name},
                   "Only roles with the SUPERUSER attribute may change "
                   "server-wide replication settings.");
}

void SetReplicationRole(duckdb::ClientContext& ctx, duckdb::SetScope,
                        duckdb::Value& value) {
  auto* conn = connector::GetSereneDBContextPtr(ctx);
  if (conn && !auth::ClosureFor(&ctx, conn->GetRoleId())->is_superuser) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG(
        "permission denied to set parameter \"session_replication_role\""));
  }
  const auto role = absl::AsciiStrToLower(value.ToString());
  if (role != "origin" && role != "replica" && role != "local") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value for parameter "
                            "\"session_replication_role\": \"",
                            value.ToString(), "\""),
                    ERR_HINT("Available values: origin, replica, local."));
  }
  value = duckdb::Value{role};
}

void RequireHbaSuperuser(duckdb::ClientContext& ctx) {
  RequireSuperuser(ctx, "hba",
                   "Only roles with the SUPERUSER attribute may change the "
                   "host-based authentication ruleset.");
}

#ifdef SDB_FAULT_INJECTION
void RequireFaultSuperuser(duckdb::ClientContext& ctx) {
  RequireSuperuser(ctx, "sdb_faults",
                   "Only roles with the SUPERUSER attribute may arm failure "
                   "points: the registry is process-global, so a fault armed "
                   "by one session fires in all of them.");
}
#endif

const catalog::RoleCatalogEntry& RoleNamed(duckdb::ClientContext& ctx,
                                           std::string_view name) {
  auto& cluster = catalog::ClusterOf(ctx);
  auto entry =
    cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
      .GetEntry(cluster.GetCatalogTransaction(ctx), duckdb::Identifier{name});
  if (!entry) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  return entry->Cast<catalog::RoleCatalogEntry>();
}

void RefreshSuperuser(ConnectionContext& conn) {
  const bool superuser =
    auth::ClosureFor(&conn.GetClientContext(), conn.GetRoleId())->is_superuser;
  conn.SetSetting("is_superuser", superuser ? "on" : "off", false);
}

void ResetRoleCallback(duckdb::ClientContext& ctx, duckdb::SetScope) {
  auto& conn = connector::GetSereneDBContext(ctx);
  conn.SetEffectiveRole(conn.GetSessionRoleId());
  RefreshSuperuser(conn);
}

void SetRoleCallback(duckdb::ClientContext& ctx, duckdb::SetScope scope,
                     duckdb::Value& value) {
  const auto name = value.ToString();
  if (name.empty() || absl::EqualsIgnoreCase(name, "none")) {
    ResetRoleCallback(ctx, scope);
    value = duckdb::Value{"none"};
    return;
  }
  auto& conn = connector::GetSereneDBContext(ctx);
  const auto& role = RoleNamed(ctx, name);
  const auto session_role = conn.GetSessionRoleId();
  const auto session =
    auth::ComputeRoleClosure(*auth::RolesOf(&ctx), session_role);
  if (role.oid != session_role && !session.is_superuser &&
      !session.CanSet(role.oid)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to set role \"", name, "\""));
  }
  conn.SetEffectiveRole(role.oid);
  RefreshSuperuser(conn);
}

void SetSessionAuthCallback(duckdb::ClientContext& ctx, duckdb::SetScope,
                            duckdb::Value& value) {
  const auto name = value.ToString();
  auto& conn = connector::GetSereneDBContext(ctx);
  const auto& role = RoleNamed(ctx, name);
  const auto login = conn.GetLoginRoleId();
  if (role.oid != login && !auth::ClosureFor(&ctx, login)->is_superuser) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to set session authorization \"", name, "\""));
  }
  conn.SetSessionRole(role.oid);
  RefreshSuperuser(conn);
}

void ResetSessionAuthCallback(duckdb::ClientContext& ctx, duckdb::SetScope) {
  auto& conn = connector::GetSereneDBContext(ctx);
  conn.ResetIdentity();
  RefreshSuperuser(conn);
}

void NoOverwriteClientEncoding(duckdb::ClientContext& ctx, duckdb::SetScope,
                               duckdb::Value& value) {
  auto canonicalize = [](std::string_view name) {
    if (name.size() >= 2 && (name.front() == '\'' || name.front() == '"') &&
        name.front() == name.back()) {
      name.remove_prefix(1);
      name.remove_suffix(1);
    }
    auto cleaned_str =
      absl::StrReplaceAll(name, {{"-", ""}, {"_", ""}, {" ", ""}});
    absl::AsciiStrToUpper(&cleaned_str);
    return cleaned_str;
  };

  duckdb::Value current;
  bool got_current =
    ctx.TryGetCurrentSetting("client_encoding", current) && !current.IsNull();
  std::string new_str = value.IsNull() ? std::string{} : value.ToString();
  std::string new_canonical = canonicalize(new_str);
  if (got_current && canonicalize(current.ToString()) == new_canonical) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
                  ERR_MSG("parameter \"client_encoding\" cannot be changed "
                          "from \"",
                          got_current ? current.ToString() : std::string{},
                          "\" to \"", new_str, "\""));
}

struct DateStyle {
  std::string_view style = "ISO";
  std::string_view order = "MDY";
};

std::optional<bool> ApplyDateStyle(DateStyle& date_style,
                                   std::string_view field) {
  const auto token = absl::AsciiStrToUpper(absl::StripAsciiWhitespace(field));
  if (token == "ISO" || token == "SQL") {
    date_style.style = token == "ISO" ? "ISO" : "SQL";
    return false;
  }
  if (token == "POSTGRES" || token == "GERMAN") {
    date_style.style = token == "POSTGRES" ? "Postgres" : "German";
    return false;
  }
  if (token == "YMD") {
    date_style.order = "YMD";
    return true;
  }
  if (token == "DMY" || token.starts_with("EURO")) {
    date_style.order = "DMY";
    return true;
  }
  if (token == "MDY" || token == "US" || token.starts_with("NONEURO")) {
    date_style.order = "MDY";
    return true;
  }
  if (token == "DEFAULT") {
    date_style = {};
    return true;
  }
  return std::nullopt;
}

void CheckDateStyle(duckdb::ClientContext& ctx, duckdb::SetScope,
                    duckdb::Value& value) {
  if (value.IsNull()) {
    return;
  }
  DateStyle date_style;
  duckdb::Value current;
  if (ctx.TryGetCurrentSetting("DateStyle", current) && !current.IsNull()) {
    for (std::string_view field : absl::StrSplit(current.ToString(), ',')) {
      ApplyDateStyle(date_style, field);
    }
  }
  const std::string raw = value.ToString();
  bool ordered = false;
  for (std::string_view field : absl::StrSplit(raw, ',', absl::SkipEmpty())) {
    const auto order = ApplyDateStyle(date_style, field);
    if (!order) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid value for parameter \"DateStyle\": \"", raw, "\""));
    }
    ordered |= *order;
  }
  if (date_style.style == "German" && !ordered) {
    date_style.order = "DMY";
  }
  value = duckdb::Value{absl::StrCat(date_style.style, ", ", date_style.order)};
  NoticeIfChanged(ctx, "DateStyle", value);
}

void CheckApplicationName(duckdb::ClientContext&, duckdb::SetScope,
                          duckdb::Value& value) {
  if (value.IsNull()) {
    return;
  }
  const std::string raw = value.ToString();
  std::string clean;
  clean.reserve(raw.size());
  bool dirty = false;
  for (const unsigned char c : raw) {
    if (c < 32 || c > 126) {
      absl::StrAppend(&clean, "\\x", absl::Hex(c, absl::kZeroPad2));
      dirty = true;
    } else {
      clean.push_back(static_cast<char>(c));
    }
  }
  if (dirty) {
    value = duckdb::Value{clean};
  }
}

constexpr std::string_view kStatementBehavior =
  "Client Connection Defaults / Statement Behavior";
constexpr std::string_view kLocaleAndFormatting =
  "Client Connection Defaults / Locale and Formatting";
constexpr std::string_view kAuthentication =
  "Connections and Authentication / Authentication";
constexpr std::string_view kPresetOptions = "Preset Options";
constexpr std::string_view kMemory = "Resource Usage / Memory";

constexpr std::string_view kByteaOutputs[] = {"escape", "hex"};
constexpr std::string_view kIsolationLevels[] = {"repeatable read",
                                                 "read committed"};
constexpr std::string_view kIntervalStyles[] = {"postgres", "postgres_verbose",
                                                "sql_standard", "iso_8601"};
constexpr std::string_view kMessageLevels[] = {"debug5", "debug4",  "debug3",
                                               "debug2", "debug1",  "log",
                                               "notice", "warning", "error"};
constexpr std::string_view kPasswordEncryptions[] = {"md5", "scram-sha-256"};
constexpr std::string_view kSynchronousCommits[] = {
  "local", "remote_write", "remote_apply", "on", "off"};
constexpr std::string_view kXmlOptions[] = {"content", "document"};

constexpr std::pair<std::string_view, VariableDescription>
  kVariableDescription[] = {
    // serenedb specific variables
    {
      "hba",
      {
        LogicalTypeId::VARCHAR,
        "Host-based authentication ruleset (pg_hba.conf syntax) as a single "
        "string: one rule per line, first match wins, applied to connections "
        "opened after the SET. An un-removable local + loopback trust rule is "
        "force-prepended so a bad ruleset cannot lock you out; an empty value "
        "restores the default (unix trust + SCRAM for TCP). Server-global.",
        [] { return duckdb::Value{""}; },
        [](duckdb::ClientContext& ctx, duckdb::SetScope, duckdb::Value& value) {
          RequireHbaSuperuser(ctx);
          if (auto err =
                network::pg::hba::SetHbaFromTextString(value.ToString())) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid hba ruleset: ", *err));
          }
        },
        [](duckdb::ClientContext& ctx, duckdb::SetScope) {
          RequireHbaSuperuser(ctx);
          network::pg::hba::SetHbaFromTextString("");  // RESET => default
        },
        duckdb::SetScope::GLOBAL,
      },
    },
#ifdef SDB_FAULT_INJECTION
    {
      "sdb_faults",
      {
        LogicalTypeId::VARCHAR,
        "Fault injection control. SET sdb_faults = 'name' to add a failure "
        "point, SET sdb_faults = '-name' to remove one, RESET sdb_faults to "
        "clear all. Superuser only: the failure-point registry is "
        "process-global, so an armed point fires for every session.",
        [] { return duckdb::Value{""}; },
        [](duckdb::ClientContext& ctx, duckdb::SetScope, duckdb::Value& value) {
          RequireFaultSuperuser(ctx);
          auto s = value.ToString();
          if (s.starts_with('-')) {
            if (!irs::RemoveFailurePointDebugging(
                  std::string_view{s}.substr(1))) {
              THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                              ERR_MSG("failure point '", s, "' not set"));
            }
          } else {
            if (!irs::AddFailurePointDebugging(s)) {
              THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                              ERR_MSG("failure point '", s, "' already set"));
            }
          }
          auto points = irs::GetFailurePointsDebugging();
          value = duckdb::Value(absl::StrJoin(points, ","));
        },
        [](duckdb::ClientContext& ctx, duckdb::SetScope) {
          RequireFaultSuperuser(ctx);
          irs::ClearFailurePointsDebugging();
        },
        // SESSION scope, though the registry behind it is process-global: it
        // is the only scope that works. GLOBAL is refused inside a
        // transaction, and every crash-recovery test arms its fault inside an
        // uncommitted txn; LOCAL would promise a rollback the registry cannot
        // give. PG's PGC_SUSET developer knobs are declared the same way --
        // session-scoped and superuser-gated -- and the gate is what makes the
        // mismatch safe: only a superuser can reach the global registry.
        duckdb::SetScope::SESSION,
      },
    },
#endif
#ifdef D_ASSERT_IS_ENABLED
    {
      "debug_verification",
      {
        LogicalTypeId::BOOLEAN,
        "Toggle DuckDB's debug Verify() calls. SET debug_verification = "
        "false to disable verification projections in EXPLAIN and speed "
        "up tests in debug builds. Default: false.",
        [] { return duckdb::Value::BOOLEAN(false); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          duckdb::g_debug_verify_enabled.store(value.GetValue<bool>(),
                                               std::memory_order_relaxed);
        },
        [](duckdb::ClientContext&, duckdb::SetScope) {
          duckdb::g_debug_verify_enabled.store(false,
                                               std::memory_order_relaxed);
        },
        duckdb::SetScope::GLOBAL,
      },
    },
#endif
    {
      "sdb_ai_text_default_secret",
      {
        LogicalTypeId::VARCHAR,
        "Name of the openai secret used by ai_generate, ai_classify, "
        "ai_classify_labels, ai_extract, ai_filter, ai_translate, ai_redact, "
        "ai_score, ai_rerank, ai_agg and ai_summarize_agg when the call does "
        "not pass secret_name. Default: '' (no default).",
        [] { return duckdb::Value{""}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_embedding_default_secret",
      {
        LogicalTypeId::VARCHAR,
        "Name of the openai secret used by ai_embed and ai_similarity when "
        "the call does not pass secret_name. Default: '' (no default).",
        [] { return duckdb::Value{""}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_system_one_default_secret",
      {
        LogicalTypeId::VARCHAR,
        "Name of the typesafe secret used by ai_system_one when the call does "
        "not pass secret_name. Default: '' (no default).",
        [] { return duckdb::Value{""}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_throw_on_error",
      {
        LogicalTypeId::BOOLEAN,
        "When true, a row whose AI function request fails fails the query; "
        "when false, that row returns NULL. Authentication, not-found, "
        "validation (422) and exhausted-quota (429 insufficient_quota) errors "
        "always fail the query. Default: true.",
        [] { return duckdb::Value::BOOLEAN(true); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_throw_on_quota_exceeded",
      {
        LogicalTypeId::BOOLEAN,
        "When true, exceeding sdb_ai_max_api_calls_per_query or "
        "sdb_ai_max_output_tokens_per_query fails the query; when false, the "
        "remaining rows return NULL. Default: true.",
        [] { return duckdb::Value::BOOLEAN(true); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_max_api_calls_per_query",
      {
        LogicalTypeId::UINTEGER,
        "Maximum number of AI provider requests a single query may send. "
        "0 = unlimited. Default: 0.",
        [] { return duckdb::Value::UINTEGER(0); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_max_output_tokens_per_query",
      {
        LogicalTypeId::UINTEGER,
        "Maximum number of output tokens, as reported by the provider, a "
        "single query may consume; checked before each request, so requests "
        "already in flight may exceed it. 0 = unlimited. Default: 0.",
        [] { return duckdb::Value::UINTEGER(0); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_max_retries",
      {
        LogicalTypeId::UINTEGER,
        "How many times an AI provider request is retried after a connection "
        "error or HTTP 408, 429, 5xx or 529. Default: 3.",
        [] { return duckdb::Value::UINTEGER(3); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_retry_initial_delay_ms",
      {
        LogicalTypeId::UINTEGER,
        "Delay before the first AI provider retry, in milliseconds; each "
        "further retry doubles it, up to 60 seconds. A Retry-After response "
        "header overrides it, up to 60 "
        "seconds. Default: 500.",
        [] { return duckdb::Value::UINTEGER(500); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_request_timeout",
      {
        LogicalTypeId::UINTEGER,
        "Timeout of a single AI provider request, in seconds. Default: 120.",
        [] { return duckdb::Value::UINTEGER(120); },
        RejectZero<"sdb_ai_request_timeout">,
      },
    },
    {
      "sdb_ai_max_concurrent_requests",
      {
        LogicalTypeId::UINTEGER,
        "Maximum number of AI provider requests a query has in flight, across "
        "all of its AI calls and threads. Requests run on DuckDB's async I/O "
        "threads (async_threads) and on the threads that evaluate the calls. "
        "Default: 16.",
        [] { return duckdb::Value::UINTEGER(16); },
        RejectZero<"sdb_ai_max_concurrent_requests">,
      },
    },
    {
      "sdb_ai_embedding_max_batch_size",
      {
        LogicalTypeId::UINTEGER,
        "Maximum number of texts ai_embed and ai_similarity send in one "
        "embeddings request. Default: 100.",
        [] { return duckdb::Value::UINTEGER(100); },
        RejectZero<"sdb_ai_embedding_max_batch_size">,
      },
    },
    {
      "sdb_ai_allow_insecure_endpoint",
      {
        LogicalTypeId::BOOLEAN,
        "When false, AI functions refuse a secret whose base_url sends "
        "requests over plain http:// to a host other than localhost, "
        "127.0.0.0/8 or ::1, because the prompts and the API key would "
        "travel unencrypted. Default: false.",
        [] { return duckdb::Value::BOOLEAN(false); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    // Logging knobs (level, type filters, storage, on/off) live in duckdb's
    // built-in settings: logging_level / enable_logging / enabled_log_types
    // / disabled_log_types / logging_storage / logging_mode. The previous
    // sdb_log_level extension option was dropped in favour of those.
    {
      "sdb_ivf_search_nprobe",
      {
        LogicalTypeId::INTEGER,
        "Number of IVF cluster lists scanned per vector-similarity query. "
        "Higher values improve recall at the cost of latency. Default 8.",
        [] { return duckdb::Value::INTEGER(8); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n < 1) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"sdb_ivf_search_nprobe\": \"",
                                    value.ToString(), "\""));
          }
        },
      },
    },
    {
      "sdb_ivf_max_search_fanout",
      {
        LogicalTypeId::INTEGER,
        "Maximum number of IVF centroid-tree children expanded per node while "
        "descending to the probed clusters. Decouples the descent width from "
        "sdb_ivf_search_nprobe: lower values cut centroid work on deep "
        "(multi-level) "
        "trees at some recall cost. The width applies per node and so "
        "compounds "
        "over the tree's levels; it is raised when smaller than the width "
        "whose "
        "compounded value reaches sdb_ivf_search_nprobe, so the descent can "
        "always supply "
        "the requested number of clusters. Default 16.",
        [] { return duckdb::Value::INTEGER(16); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n < 1) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"sdb_ivf_max_search_fanout\": \"",
                                    value.ToString(), "\""));
          }
        },
      },
    },
    {
      "sdb_hnsw_ef_search",
      {
        LogicalTypeId::INTEGER,
        "Search-time beam width (ef) for HNSW vector indexes. Higher values "
        "improve recall at the cost of latency. The beam is also the result "
        "ceiling: a value below the query's LIMIT returns fewer rows than "
        "asked for. Default 64.",
        [] { return duckdb::Value::INTEGER(64); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n <= 0) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"sdb_hnsw_ef_search\": \"",
                                    value.ToString(), "\""));
          }
        },
      },
    },
    {
      "sdb_ivf_sample_factor",
      {
        LogicalTypeId::DOUBLE,
        "Fraction of rows used to train the IVF centroid tree "
        "(sample_size = sample_factor * N), captured into the index config at "
        "CREATE INDEX. Any value in (0, 1]. Default 0.2.",
        [] { return duckdb::Value::DOUBLE(0.2); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto f = value.GetValue<double>();
          if (f <= 0.0 || f > 1.0) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"sdb_ivf_sample_factor\": \"",
                                    value.ToString(), "\""));
          }
        },
      },
    },
    {
      "sdb_ivf_posting_size",
      {
        LogicalTypeId::INTEGER,
        "Target IVF posting-list size (leaf cap t), captured into the index "
        "config at CREATE INDEX. Smaller values force deeper multi-level "
        "centroid trees (useful for testing). Default 1024.",
        [] { return duckdb::Value::INTEGER(1024); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n < 1) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"sdb_ivf_posting_size\": \"",
                                    value.ToString(), "\""));
          }
        },
      },
    },
    {
      "sdb_rerank_factor",
      {
        LogicalTypeId::DOUBLE,
        "Multiplier applied to LIMIT k to size the candidate pool re-scored "
        "with exact distances for a quantized IVF vector-similarity query "
        "(pool = ceil(sdb_rerank_factor * k)). Higher values improve recall "
        "at the cost of latency; 0 disables reranking (top-k picked by the "
        "approximate quantized distance). Fractional values are allowed, but "
        "a nonzero factor below 1 is rejected because the pool must cover k. "
        "Default 4. Unquantized (quant = 'none') indexes never rerank, "
        "regardless of this setting.",
        [] { return duckdb::Value::DOUBLE(4); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<double>();
          if (n < 0.0 || (n > 0.0 && n < 1.0)) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"sdb_rerank_factor\": \"",
                                    value.ToString(), "\""));
          }
        },
      },
    },
    {
      "sdb_levenshtein_max_terms",
      {
        LogicalTypeId::INTEGER,
        "The maximum number of dictionary terms a fuzzy predicate "
        "(`ts_levenshtein`) expands to per index segment. Terms closest to the "
        "query survive; the rest neither match nor contribute to scoring. "
        "Higher values improve recall on wide expansions at the cost of "
        "per-query work. 0 removes the cap, so every term within the edit "
        "distance matches. Default 50.",
        [] { return duckdb::Value::INTEGER(50); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n < 0) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"sdb_levenshtein_max_terms\": \"",
                                    value.ToString(), "\""));
          }
        },
      },
    },
    {
      "sdb_pattern_cache_size",
      {
        LogicalTypeId::UBIGINT,
        "Bytes of compiled search patterns (`ts_regexp`, `ts_like`, LIKE and "
        "fused alternations over an inverted index) kept across queries, so "
        "a pattern is compiled once per server rather than once per query. "
        "The least recently used patterns are dropped first; a pattern in use "
        "by a running query stays alive until it finishes. 0, the default, "
        "keeps nothing: each query compiles its own patterns. Server-global.",
        [] {
          return duckdb::Value::UBIGINT(irs::PatternCache::kDefaultCapacity);
        },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          irs::PatternCache::Instance().SetCapacity(value.GetValue<uint64_t>());
        },
        [](duckdb::ClientContext&, duckdb::SetScope) {
          irs::PatternCache::Instance().SetCapacity(
            irs::PatternCache::kDefaultCapacity);
        },
        duckdb::SetScope::GLOBAL,
      },
    },
    {
      "sdb_job_history_size",
      {
        LogicalTypeId::UBIGINT,
        "Number of recent job runs, across all jobs, that sdb_job_runs() "
        "returns; the oldest run is dropped first. 0 keeps none. Default "
        "1024. Server-global.",
        [] {
          return duckdb::Value::UBIGINT(JobScheduler::kDefaultHistorySize);
        },
        nullptr,
        nullptr,
        duckdb::SetScope::GLOBAL,
      },
    },
    {
      "sdb_job_max_depth",
      {
        LogicalTypeId::UINTEGER,
        "Maximum number of job runs in one chain of EXECUTE JOB calls, where "
        "a job's body executes another job; a run that would go deeper fails. "
        "Default 16. Server-global.",
        [] { return duckdb::Value::UINTEGER(16); },
        nullptr,
        nullptr,
        duckdb::SetScope::GLOBAL,
      },
    },
    {
      "sdb_disable_top_k_optimization",
      {
        LogicalTypeId::BOOLEAN,
        "When true, the optimizer skips pulling `ORDER BY <scorer>(...) DESC "
        "LIMIT k` into the inverted-index scan, so block-max score pruning "
        "never engages. Default: false (optimization on).",
        [] { return duckdb::Value::BOOLEAN(false); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_scan_split",
      {
        LogicalTypeId::VARCHAR,
        "How an inverted-index scan shares a segment between workers: 'tail' "
        "gives each worker its own segment and lets an idle worker join the "
        "segment with the most row groups left only once no segment is "
        "unclaimed; 'always' puts every worker on the same segment until it "
        "is claimed; 'never' scans each segment whole on one worker; 'auto' "
        "(default) is 'always' under an ORDER BY scan order and 'tail' "
        "otherwise.",
        [] { return duckdb::Value{"auto"}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          const auto mode = value.ToString();
          if (!absl::EqualsIgnoreCase(mode, "auto") &&
              !absl::EqualsIgnoreCase(mode, "tail") &&
              !absl::EqualsIgnoreCase(mode, "always") &&
              !absl::EqualsIgnoreCase(mode, "never")) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
              ERR_MSG("invalid value for parameter \"sdb_scan_split\": \"",
                      mode, "\" (auto, tail, always or never)"));
          }
        },
      },
    },
    {
      "sdb_scan_order",
      {
        LogicalTypeId::VARCHAR,
        "The order an inverted-index scan claims its segments in: "
        "'smallest_first' and 'largest_first' order them by live document "
        "count; 'order' is best-first by the ORDER BY column's row-group "
        "statistics when the query has a scan order (and 'largest_first' "
        "otherwise); 'auto' (default) is 'order' under a scan order and "
        "'largest_first' otherwise.",
        [] { return duckdb::Value{"auto"}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          const auto mode = value.ToString();
          if (!absl::EqualsIgnoreCase(mode, "auto") &&
              !absl::EqualsIgnoreCase(mode, "smallest_first") &&
              !absl::EqualsIgnoreCase(mode, "largest_first") &&
              !absl::EqualsIgnoreCase(mode, "order")) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
              ERR_MSG("invalid value for parameter \"sdb_scan_order\": \"",
                      mode,
                      "\" (auto, smallest_first, largest_first or order)"));
          }
        },
      },
    },
    {
      "sdb_scan_no_split_row_groups",
      {
        LogicalTypeId::INTEGER,
        "A segment with at most this many row groups is always one unit of "
        "an inverted-index scan and is never split across workers. "
        "Default 1.",
        [] { return duckdb::Value::INTEGER(1); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          if (value.GetValue<int32_t>() < 1) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
              ERR_MSG("sdb_scan_no_split_row_groups must be at least 1"));
          }
        },
      },
    },
    {
      "sdb_compact_target_segments",
      {
        LogicalTypeId::UINTEGER,
        "How many segments VACUUM (COMPACT_*) leaves in a search table. 0 or "
        "1 (default) merges every segment into one; a larger N splits the "
        "segments into N disjoint groups, merges each group into one segment "
        "and stops there, so no single merge holds the whole table. A table "
        "with N segments or fewer is left as it is. Default 1.",
        [] { return duckdb::Value::UINTEGER(1); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kRowGroupSizeSetting,
      {
        LogicalTypeId::UINTEGER,
        "Default columnstore row-group size for newly created inverted "
        "indexes. Per-index WITH (row_group_size = ...) overrides it. Reads "
        "from existing indexes are unaffected. Default: 122'880.",
        [] { return duckdb::Value::UINTEGER(DEFAULT_ROW_GROUP_SIZE); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          const auto n = value.GetValue<uint32_t>();
          if (n == 0 || n % STANDARD_VECTOR_SIZE != 0) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"row_group_size\": \"",
                                    value.ToString(),
                                    "\" (must be a positive multiple of the "
                                    "vector size ",
                                    STANDARD_VECTOR_SIZE, ")"));
          }
        },
      },
    },
    {
      kRefreshIntervalSetting,
      {
        LogicalTypeId::UINTEGER,
        "Background refresh interval (ms) for newly created inverted indexes "
        "and search tables. WITH (refresh_interval = ...) overrides it per "
        "index or table, ALTER INDEX / ALTER TABLE ... SET changes it later. "
        "0 disables the refresh task. Default: 1000.",
        [] { return duckdb::Value::UINTEGER(1000); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kReindexIntervalSetting,
      {
        LogicalTypeId::UINTEGER,
        "Periodic reindex interval (ms) for newly created VIEW-backed "
        "inverted indexes: a non-zero value creates a job named after the "
        "index that runs the ordinary REINDEX road (up_to_date / delta / "
        "rebuild) as the index owner, AFTER the interval. Per-index "
        "WITH (reindex_interval = ...) overrides; ALTER INDEX SET "
        "retunes, creates or drops the job. 0 disables it. Hint-less iceberg "
        "sources refresh from the background only when the GLOBAL "
        "unsafe_enable_version_guessing is set. Default: 0.",
        [] { return duckdb::Value::UINTEGER(0); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kCompactionIntervalSetting,
      {
        LogicalTypeId::UINTEGER,
        "Background compaction interval (ms) for newly created inverted "
        "indexes and search tables. WITH (compaction_interval = ...) "
        "overrides it per index or table, ALTER INDEX / ALTER TABLE ... SET "
        "changes it later. 0 disables background compaction; VACUUM "
        "(COMPACT_*) still merges on demand. Default: 1000.",
        [] { return duckdb::Value::UINTEGER(1000); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kCleanupIntervalStepSetting,
      {
        LogicalTypeId::UINTEGER,
        "Number of commit ticks between background unreferenced-file cleanup "
        "passes for newly created inverted indexes and search tables. WITH "
        "(cleanup_interval_step = ...) overrides it per index or table. 0 "
        "disables cleanup. Default: 1.",
        [] { return duckdb::Value::UINTEGER(1); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kSearchBackfillGroupBytesSetting,
      {
        LogicalTypeId::UBIGINT,
        "Bytes of existing segments a search-table CREATE INDEX rewrites "
        "before swapping them in and moving on. Bounds the build's peak disk "
        "(~2x one group) and the length of each publish; a value above the "
        "table's size means a single swap. 0 = no limit (one swap for the "
        "whole table). Default 1073741824 (1GB).",
        [] { return duckdb::Value::UBIGINT(uint64_t{1} << 30); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kSegmentMemoryMaxSetting,
      {
        LogicalTypeId::UBIGINT,
        "In-memory bytes an inverted-index or search-table segment writer "
        "fills before rolling over to a new on-disk segment (also the CREATE "
        "INDEX backfill commit cadence, and half of it is the write buffer a "
        "serial search-table statement fills before it starts feeding the "
        "index as it goes). Per-object WITH (segment_memory_max = ...) "
        "overrides. Default 268435456 (256MB).",
        [] {
          return duckdb::Value::UBIGINT(
            catalog::InvertedIndexSettings{}.segment_memory_max);
        },
        RejectZero<"segment_memory_max">,
      },
    },
    {
      kSegmentDocsMaxSetting,
      {
        LogicalTypeId::UINTEGER,
        "Document count at which an inverted-index segment writer rolls over "
        "to a new on-disk segment. Per-index WITH (segment_docs_max = ...) "
        "overrides. 0 = unlimited (memory limit governs).",
        [] { return duckdb::Value::UINTEGER(0); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kRecoveryReplayDepthSetting,
      {
        LogicalTypeId::UINTEGER,
        "Maximum WAL chunks in flight per inverted index during recovery "
        "replay (the prefetch window; bounds replay memory). 0 = auto "
        "(4 x cpu threads).",
        [] { return duckdb::Value::UINTEGER(0); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      kCompactionMaxSegmentsSetting,
      {
        LogicalTypeId::UINTEGER,
        "Maximum segments of an inverted index or search table merged by one "
        "background compaction. WITH (compaction_max_segments = ...) "
        "overrides it per index or table. Default 10.",
        [] {
          return duckdb::Value::UINTEGER(
            catalog::InvertedIndexSettings{}.compaction_max_segments);
        },
        RejectZero<"compaction_max_segments">,
      },
    },
    {
      kCompactionMaxSegmentsBytesSetting,
      {
        LogicalTypeId::UBIGINT,
        "Byte budget of one background compaction of an inverted index or "
        "search table (the tier target size). WITH "
        "(compaction_max_segments_bytes = ...) overrides it per index or "
        "table. Default 5368709120 (5GB).",
        [] {
          return duckdb::Value::UBIGINT(
            catalog::InvertedIndexSettings{}.compaction_max_segments_bytes);
        },
        RejectZero<"compaction_max_segments_bytes">,
      },
    },
    {
      kCompactionFloorSegmentBytesSetting,
      {
        LogicalTypeId::UBIGINT,
        "Segments of an inverted index or search table below this size count "
        "as equal-sized for compaction candidate selection. WITH "
        "(compaction_floor_segment_bytes = ...) overrides it per index or "
        "table. Default 2097152 (2MB).",
        [] {
          return duckdb::Value::UBIGINT(
            catalog::InvertedIndexSettings{}.compaction_floor_segment_bytes);
        },
        RejectZero<"compaction_floor_segment_bytes">,
      },
    },
    {
      "extra_float_digits",
      {
        .type = LogicalTypeId::INTEGER,
        .description =
          "Sets the number of digits displayed for floating-point values.",
        .default_value = [] { return duckdb::Value{"1"}; },
        .set_callback =
          [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
            auto n = value.GetValue<int32_t>();
            if (!(-15 <= n && n <= 3)) {
              THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                              ERR_MSG("invalid value for parameter "
                                      "\"extra_float_digits\": \"",
                                      value.ToString(), "\""));
            }
          },
        .pg = {.vartype = "integer",
               .category = kLocaleAndFormatting,
               .min_val = "-15",
               .max_val = "3"},
      },
    },
    {
      "bytea_output",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Sets the output format for bytea.",
        .default_value = [] { return duckdb::Value{"hex"}; },
        .set_callback =
          [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
            if (!magic_enum::enum_cast<ByteaOutput>(
                   value.ToString(), magic_enum::case_insensitive)
                   .has_value()) {
              THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                              ERR_MSG("invalid value for parameter "
                                      "\"bytea_output\": \"",
                                      value.ToString(), "\""));
            }
          },
        .pg = {.vartype = "enum",
               .category = kStatementBehavior,
               .enumvals = kByteaOutputs},
      },
    },
    {
      "client_encoding",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Sets the client's character set encoding.",
        .default_value = [] { return duckdb::Value{"UTF8"}; },
        .set_callback = NoOverwriteClientEncoding,
        .pg = {.vartype = "string", .category = kLocaleAndFormatting},
      },
    },
    {
      "application_name",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the application name to be reported in statistics and logs.",
        .default_value = [] { return duckdb::Value{""}; },
        .set_callback = CheckApplicationName,
        .pg = {.vartype = "string",
               .category = "Reporting and Logging / What to Log"},
      },
    },
    {
      "in_hot_standby",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description = "Shows whether hot standby is currently active.",
        .default_value = [] { return duckdb::Value{false}; },
        .pg = {.vartype = "bool",
               .category = kPresetOptions,
               .context = "internal"},
      },
    },
    {
      "integer_datetimes",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description = "Shows whether datetimes are integer based.",
        .default_value = [] { return duckdb::Value{true}; },
        .set_callback = NoOverwrite<"integer_datetimes">,
        .pg = {.vartype = "bool",
               .category = kPresetOptions,
               .context = "internal"},
      },
    },
    {
      "scram_iterations",
      {
        .type = LogicalTypeId::INTEGER,
        .description = "Sets the iteration count for SCRAM secret generation.",
        .default_value = [] { return duckdb::Value{"4096"}; },
        .set_callback = NoOverwrite<"scram_iterations">,
        .pg = {.vartype = "integer",
               .category = kAuthentication,
               .min_val = "1",
               .max_val = "2147483647"},
      },
    },
    {
      "server_encoding",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Shows the server (database) character set encoding.",
        .default_value = [] { return duckdb::Value{"UTF8"}; },
        .pg = {.vartype = "string",
               .category = kPresetOptions,
               .context = "internal"},
      },
    },
    {
      "server_version",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Shows the server version.",
        .default_value = [] { return duckdb::Value{"18.6"}; },
        .pg = {.vartype = "string",
               .category = kPresetOptions,
               .context = "internal"},
      },
    },
    {
      "server_version_num",
      {
        .type = LogicalTypeId::INTEGER,
        .description = "Shows the server version as an integer.",
        .default_value = [] { return duckdb::Value::INTEGER(180006); },
        .pg = {.vartype = "integer",
               .category = kPresetOptions,
               .context = "internal",
               .min_val = "180006",
               .max_val = "180006"},
      },
    },
    {
      "standard_conforming_strings",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description = "Causes '...' strings to treat backslashes literally.",
        .default_value = [] { return duckdb::Value{true}; },
        .set_callback = NoOverwrite<"standard_conforming_strings">,
        .pg = {.vartype = "bool",
               .category = "Version and Platform Compatibility / Previous "
                           "PostgreSQL Versions"},
      },
    },
    {
      "client_min_messages",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Sets the message levels that are sent to the client.",
        .default_value = [] { return duckdb::Value{"notice"}; },
        .set_callback = NoOverwrite<"client_min_messages">,
        .pg = {.vartype = "enum",
               .category = kStatementBehavior,
               .enumvals = kMessageLevels},
      },
    },
    {
      "statement_timeout",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Aborts any statement that takes more than the specified amount of "
          "time (milliseconds without a unit). 0 disables the timeout.",
        .default_value = [] { return duckdb::Value{"0"}; },
        .set_callback = SetStatementTimeout,
        .reset_callback = ResetStatementTimeout,
        .pg = {.vartype = "integer",
               .category = kStatementBehavior,
               .unit = "ms",
               .min_val = "0",
               .max_val = "2147483647"},
      },
    },
    {
      kWalReceiverTimeoutSetting,
      {
        LogicalTypeId::VARCHAR,
        "Terminates a logical replication worker whose publisher sent "
        "nothing for this long (milliseconds without a unit); 0 disables the "
        "timeout. Server-wide, superuser only.",
        [] { return duckdb::Value{"1min"}; },
        SetServerDuration<"wal_receiver_timeout", 1>,
        nullptr,
        duckdb::SetScope::GLOBAL,
      },
    },
    {
      kWalReceiverStatusIntervalSetting,
      {
        LogicalTypeId::VARCHAR,
        "How often a logical replication worker reports its position to the "
        "publisher (seconds without a unit); 0 disables the periodic "
        "reports. Server-wide, superuser only.",
        [] { return duckdb::Value{"10s"}; },
        SetServerDuration<"wal_receiver_status_interval", 1000>,
        nullptr,
        duckdb::SetScope::GLOBAL,
      },
    },
    {
      kWalRetrieveRetryIntervalSetting,
      {
        LogicalTypeId::VARCHAR,
        "How long a failed logical replication worker waits before it "
        "reconnects (milliseconds without a unit). Server-wide, superuser "
        "only.",
        [] { return duckdb::Value{"5s"}; },
        SetServerDuration<"wal_retrieve_retry_interval", 1>,
        nullptr,
        duckdb::SetScope::GLOBAL,
      },
    },
    {
      kMaxSyncWorkersPerSubscriptionSetting,
      {
        LogicalTypeId::UINTEGER,
        "How many tables one subscription copies at the same time during "
        "its initial synchronization. Server-wide, superuser only.",
        [] { return duckdb::Value::UINTEGER(2); },
        SetServerCount<"max_sync_workers_per_subscription">,
        nullptr,
        duckdb::SetScope::GLOBAL,
      },
    },
    {
      "session_replication_role",
      {
        LogicalTypeId::VARCHAR,
        "Sets the session's behavior for triggers and rewrite rules.",
        [] { return duckdb::Value{"origin"}; },
        SetReplicationRole,
      },
    },
    {
      "session_authorization",
      {
        LogicalTypeId::VARCHAR,
        "Sets the current session's user name.",
        [] { return duckdb::Value{irs::StaticStrings::kDefaultUser}; },
        SetSessionAuthCallback,
        ResetSessionAuthCallback,
      },
    },
    {
      "role",
      {
        LogicalTypeId::VARCHAR,
        "Sets the current role.",
        [] { return duckdb::Value{"none"}; },
        SetRoleCallback,
        ResetRoleCallback,
      },
    },
    {
      "is_superuser",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether the current session's user is a superuser.",
        [] { return duckdb::Value{true}; },
        nullptr,  // refused via kUnchangeableSettings
      },
    },
    {
      "DateStyle",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Sets the display format for date and time values.",
        .default_value = [] { return duckdb::Value{"ISO, MDY"}; },
        .set_callback = CheckDateStyle,
        .pg = {.vartype = "string", .category = kLocaleAndFormatting},
      },
    },
    {
      "IntervalStyle",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Sets the display format for interval values.",
        .default_value = [] { return duckdb::Value{"postgres"}; },
        .set_callback = NoOverwrite<"IntervalStyle">,
        .pg = {.vartype = "enum",
               .category = kLocaleAndFormatting,
               .enumvals = kIntervalStyles},
      },
    },
    {
      "TimeZone",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the time zone for displaying and interpreting time stamps.",
        .default_value = [] { return duckdb::Value{"Etc/UTC"}; },
        .pg = {.vartype = "string", .category = kLocaleAndFormatting},
      },
    },
    {
      "search_path",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Sets the schema search order for names that are not "
                       "schema-qualified.",
        .default_value = [] { return duckdb::Value{"\"$user\", public"}; },
        .pg = {.vartype = "string", .category = kStatementBehavior},
      },
    },
    {
      "transaction_isolation",
      {
        .type = LogicalTypeId::VARCHAR,
        .description = "Sets the current transaction's isolation level.",
        .default_value = [] { return duckdb::Value{"repeatable read"}; },
        .pg = {.vartype = "enum",
               .category = kStatementBehavior,
               .enumvals = kIsolationLevels},
      },
    },
    {
      "default_transaction_isolation",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the transaction isolation level of each new transaction.",
        .default_value = [] { return duckdb::Value{"repeatable read"}; },
        .pg = {.vartype = "enum",
               .category = kStatementBehavior,
               .enumvals = kIsolationLevels},
      },
    },
    {
      "default_transaction_read_only",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description = "Sets the default read-only status of new transactions.",
        .default_value = [] { return duckdb::Value{false}; },
        .pg = {.vartype = "bool", .category = kStatementBehavior},
      },
    },
    {
      "check_function_bodies",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description =
          "Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE. "
          "SereneDB always checks them; turning this off has no effect.",
        .default_value = [] { return duckdb::Value{true}; },
        .set_callback = NoOverwrite<"check_function_bodies">,
        .pg = {.vartype = "bool", .category = kStatementBehavior},
      },
    },
    {
      "default_table_access_method",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the default table access method for new tables. Accepted for "
          "compatibility; it has no effect in SereneDB.",
        .default_value = [] { return duckdb::Value{"heap"}; },
        .set_callback = NoOverwrite<"default_table_access_method">,
        .pg = {.vartype = "string", .category = kStatementBehavior},
      },
    },
    {
      "default_tablespace",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the default tablespace to create tables and indexes in. "
          "Accepted for compatibility; SereneDB has no tablespaces.",
        .default_value = [] { return duckdb::Value{""}; },
        .set_callback = NoOverwrite<"default_tablespace">,
        .pg = {.vartype = "string", .category = kStatementBehavior},
      },
    },
    {
      "idle_in_transaction_session_timeout",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the maximum allowed idle time between queries, when in a "
          "transaction. Accepted for compatibility; SereneDB does not end idle "
          "transactions.",
        .default_value = [] { return duckdb::Value{"0"}; },
        .set_callback = NoOverwrite<"idle_in_transaction_session_timeout">,
        .pg = {.vartype = "integer",
               .category = kStatementBehavior,
               .unit = "ms",
               .min_val = "0",
               .max_val = "2147483647"},
      },
    },
    {
      "lock_timeout",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the maximum allowed duration of any wait for a lock. Accepted "
          "for compatibility; SereneDB never waits for a lock, a conflicting "
          "write fails at once.",
        .default_value = [] { return duckdb::Value{"0"}; },
        .set_callback = NoOverwrite<"lock_timeout">,
        .pg = {.vartype = "integer",
               .category = kStatementBehavior,
               .unit = "ms",
               .min_val = "0",
               .max_val = "2147483647"},
      },
    },
    {
      "maintenance_work_mem",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the maximum memory to be used for maintenance operations. "
          "Accepted for compatibility; memory_limit bounds the memory of every "
          "operation.",
        .default_value = [] { return duckdb::Value{"64MB"}; },
        .set_callback = NoOverwrite<"maintenance_work_mem">,
        .pg = {.vartype = "integer",
               .category = kMemory,
               .unit = "kB",
               .min_val = "1024",
               .max_val = "2147483647"},
      },
    },
    {
      "max_connections",
      {
        .type = LogicalTypeId::UBIGINT,
        .description =
          "Sets the maximum number of concurrent client connections. Set with "
          "--max_connections; 0 means no limit. A listener may override it "
          "with ?max_connections=.",
        .default_value =
          [] {
            return duckdb::Value::UBIGINT(absl::GetFlag(FLAGS_max_connections));
          },
        .pg = {.vartype = "integer",
               .category =
                 "Connections and Authentication / Connection Settings",
               .context = "postmaster",
               .min_val = "0",
               .max_val = "4294967295"},
      },
    },
    {
      "max_identifier_length",
      {
        .type = LogicalTypeId::INTEGER,
        .description = "Shows the maximum identifier length. SereneDB keeps "
                       "longer identifiers in full.",
        .default_value = [] { return duckdb::Value::INTEGER(63); },
        .pg = {.vartype = "integer",
               .category = kPresetOptions,
               .context = "internal",
               .min_val = "63",
               .max_val = "63"},
      },
    },
    {
      "max_index_keys",
      {
        .type = LogicalTypeId::INTEGER,
        .description =
          "Shows the maximum number of index keys. Clients read it as the "
          "bound of index and constraint key arrays, such as pg_index.indkey "
          "and pg_constraint.conkey.",
        .default_value = [] { return duckdb::Value::INTEGER(32); },
        .pg = {.vartype = "integer",
               .category = kPresetOptions,
               .context = "internal",
               .min_val = "32",
               .max_val = "32"},
      },
    },
    {
      "password_encryption",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Chooses the algorithm for encrypting passwords. SereneDB stores a "
          "password given in plain text as SCRAM-SHA-256; clients read this "
          "setting to choose how they encrypt one themselves.",
        .default_value = [] { return duckdb::Value{"scram-sha-256"}; },
        .set_callback = NoOverwrite<"password_encryption">,
        .pg = {.vartype = "enum",
               .category = kAuthentication,
               .enumvals = kPasswordEncryptions},
      },
    },
    {
      "row_security",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description = "Enables row security. Accepted for compatibility; "
                       "SereneDB has no row security policies.",
        .default_value = [] { return duckdb::Value{true}; },
        .set_callback = NoOverwrite<"row_security">,
        .pg = {.vartype = "bool", .category = kStatementBehavior},
      },
    },
    {
      "synchronous_commit",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the current transaction's synchronization level. Accepted for "
          "compatibility; it has no effect in SereneDB.",
        .default_value = [] { return duckdb::Value{"on"}; },
        .set_callback = NoOverwrite<"synchronous_commit">,
        .pg = {.vartype = "enum",
               .category = "Write-Ahead Log / Settings",
               .enumvals = kSynchronousCommits},
      },
    },
    {
      "transaction_deferrable",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description =
          "Whether to defer a read-only serializable transaction until it can "
          "be executed with no possible serialization failures. Accepted for "
          "compatibility; it has no effect in SereneDB.",
        .default_value = [] { return duckdb::Value{false}; },
        .set_callback = NoOverwrite<"transaction_deferrable">,
        .pg = {.vartype = "bool", .category = kStatementBehavior},
      },
    },
    {
      "transaction_read_only",
      {
        .type = LogicalTypeId::BOOLEAN,
        .description = "Sets the current transaction's read-only status. "
                       "Accepted for compatibility; it has no effect in "
                       "SereneDB.",
        .default_value = [] { return duckdb::Value{false}; },
        .set_callback = NoOverwrite<"transaction_read_only">,
        .pg = {.vartype = "bool", .category = kStatementBehavior},
      },
    },
    {
      "transaction_timeout",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the maximum allowed duration of any transaction within a "
          "session (not a prepared transaction). Accepted for compatibility; "
          "SereneDB does not time transactions out.",
        .default_value = [] { return duckdb::Value{"0"}; },
        .set_callback = NoOverwrite<"transaction_timeout">,
        .pg = {.vartype = "integer",
               .category = kStatementBehavior,
               .unit = "ms",
               .min_val = "0",
               .max_val = "2147483647"},
      },
    },
    {
      "work_mem",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets the maximum memory to be used for query workspaces. Accepted "
          "for compatibility; memory_limit bounds the memory of every "
          "operation.",
        .default_value = [] { return duckdb::Value{"4MB"}; },
        .set_callback = NoOverwrite<"work_mem">,
        .pg = {.vartype = "integer",
               .category = kMemory,
               .unit = "kB",
               .min_val = "64",
               .max_val = "2147483647"},
      },
    },
    {
      "xmloption",
      {
        .type = LogicalTypeId::VARCHAR,
        .description =
          "Sets whether XML data in implicit parsing and serialization "
          "operations is to be considered as documents or content fragments. "
          "Accepted for compatibility; SereneDB has no xml type.",
        .default_value = [] { return duckdb::Value{"content"}; },
        .set_callback = NoOverwrite<"xmloption">,
        .pg = {.vartype = "enum",
               .category = kStatementBehavior,
               .enumvals = kXmlOptions},
      },
    },
};

const duckdb::case_insensitive_map_view_t<const VariableDescription*>
  kVariableIndex = [] {
    duckdb::case_insensitive_map_view_t<const VariableDescription*> m;
    m.reserve(std::size(kVariableDescription));
    for (const auto& [name, desc] : kVariableDescription) {
      m.emplace(name, &desc);
    }
    return m;
  }();

}  // namespace

std::string_view GetOriginalName(std::string_view name) {
  auto it = kVariableIndex.find(name);
  if (it == kVariableIndex.end()) {
    return {};
  }
  return it->first;
}

const VariableDescription* FindVariable(std::string_view name) {
  const auto it = kVariableIndex.find(name);
  return it == kVariableIndex.end() ? nullptr : it->second;
}

// Settings a client may never change, refused for the lifetime of the process
// by the setting_change_handler.
const duckdb::case_insensitive_set_view_t kUnchangeableSettings = {
  // Describes how this process is wired rather than a preference, and is pinned
  // at startup in
  // ConfigureServerDBConfig.
  "external_threads",
  // Read-only in PostgreSQL, where reporting the value is the whole point of
  // the GUC.
  "in_hot_standby",
  "is_superuser",
  "max_connections",
  "max_identifier_length",
  "max_index_keys",
  "server_encoding",
  "server_version",
  "server_version_num",
};

bool IsUnchangeableSetting(std::string_view name) {
  return kUnchangeableSettings.contains(name);
}

namespace {

void TryRegister(duckdb::DBConfig& config, std::string_view name,
                 const VariableDescription& desc) {
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  const duckdb::Identifier setting{name};
  if (duckdb::DBConfig::GetOptionByName(setting) ||
      config.TryGetSettingIndex(setting, option).IsValid()) {
    return;  // already registered or built-in
  }
  config.AddExtensionOption(
    setting, std::string{desc.description}, duckdb::LogicalType{desc.type},
    desc.default_value ? desc.default_value() : duckdb::Value{},
    desc.set_callback, desc.reset_callback, desc.scope,
    IsUnchangeableSetting(name));
}

}  // namespace
namespace connector {

void RegisterConfigVariables(duckdb::DBConfig& config) {
  for (const auto& [name, desc] : kVariableDescription) {
    TryRegister(config, name, desc);
  }
}

duckdb::Value ValidateSetting(duckdb::ClientContext& context,
                              std::string_view name,
                              const duckdb::Value& value) {
  duckdb::ExtensionOption option;
  duckdb::DBConfig::GetConfig(context).TryGetExtensionOption(
    duckdb::Identifier{name}, option);
  auto result = value.CastAs(context, option.type);
  option.set_function(context, duckdb::SetScope::AUTOMATIC, result);
  return result;
}

}  // namespace connector
}  // namespace sdb
