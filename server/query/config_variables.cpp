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

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>

#include <duckdb/common/types/string.hpp>
#include <duckdb/main/config.hpp>

#include "basics/assert.h"
#include "basics/containers/trivial_map.h"
#include "basics/debugging.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/isolation_level.h"
#include "query/config.h"

namespace sdb {

using duckdb::LogicalTypeId;

constexpr std::pair<std::string_view, VariableDescription>
  kVariableDescription[] = {
// serenedb specific variables
#ifdef SDB_FAULT_INJECTION
    {
      "sdb_faults",
      {
        LogicalTypeId::VARCHAR,
        "Fault injection control. SET sdb_faults = 'name' to add a failure "
        "point, SET sdb_faults = '-name' to remove one, RESET sdb_faults to "
        "clear all.",
        "",
        nullptr,
        [](duckdb::ClientContext&, duckdb::SetScope scope, const std::string&,
           duckdb::Value& value, bool is_reset) {
          if (scope == duckdb::SetScope::LOCAL) {
            throw duckdb::InvalidInputException(
              "sdb_faults cannot be set locally");
          }
          if (is_reset) {
            ClearFailurePointsDebugging();
          } else {
            auto s = value.ToString();
            if (s.starts_with('-')) {
              if (!RemoveFailurePointDebugging(
                    std::string_view{s}.substr(1))) {
                throw duckdb::InvalidInputException(
                  "failure point '%s' not set", s);
              }
            } else {
              if (!AddFailurePointDebugging(s)) {
                throw duckdb::InvalidInputException(
                  "failure point '%s' already set", s);
              }
            }
          }
          // Sync stored value to reflect current state
          auto points = GetFailurePointsDebugging();
          value = duckdb::Value(absl::StrJoin(points, ","));
        },
      },
    },
#endif
    {
      log::kLogLevelVariable,
      {
        LogicalTypeId::VARCHAR,
        "Sets the server log level. "
        "Use 'topic=level' format, e.g. 'all=trace', 'requests=debug'. "
        "Valid levels: fatal, error, warning, info, debug, trace. "
        "Valid topics: all, authentication, authorization, communication, "
        "config, crash, engines, flush, fuerte, general, httpclient, "
        "iresearch, memory, replication, requests, rocksdb, search, ssl, "
        "startup, statistics, syscall, threads.",
        "info",
        nullptr,
        [](duckdb::ClientContext&, duckdb::SetScope scope, const std::string&,
           duckdb::Value& value, bool is_reset) {
          if (scope == duckdb::SetScope::LOCAL) {
            throw duckdb::InvalidInputException(
              "parameter \"%s\" is global and cannot be set locally",
              std::string{log::kLogLevelVariable});
          }
          if (is_reset) {
            log::ResetLogLevels();
          } else {
            log::SetLogLevel(value.ToString());
          }
          // Sync stored value to reflect current state
          value = duckdb::Value(log::LogLevelString());
        },
      },
    },
    {
      "sdb_write_conflict_policy",
      {
        LogicalTypeId::VARCHAR,
        "Sets the write conflict policy. Valid values are "
        "'emit_error' "
        "(the default), 'do_nothing' (skip conflicted rows) and 'replace'.",
        "emit_error",
        [](const duckdb::Value& v) {
          auto s = v.ToString();
          return absl::EqualsIgnoreCase("emit_error", s) ||
                 absl::EqualsIgnoreCase("do_nothing", s) ||
                 absl::EqualsIgnoreCase("replace", s);
        },
      },
    },
    {
      "sdb_read_your_own_writes",
      {
        LogicalTypeId::BOOLEAN,
        "Controls whether queries can see uncommitted writes from the current "
        "transaction.",
        "true",
      },
    },
    // pg specific variables
    {
      "search_path",
      {
        LogicalTypeId::VARCHAR,
        "Sets the schema search order for names that are not schema-qualified.",
        "\"$user\", public",
      },
    },
    {
      "extra_float_digits",
      {
        LogicalTypeId::INTEGER,
        "Sets the number of digits displayed for floating-point values.",
        "1",
        [](const duckdb::Value& v) {
          auto n = v.GetValue<int32_t>();
          return -15 <= n && n <= 3;
        },
      },
    },
    {
      "bytea_output",
      {
        LogicalTypeId::VARCHAR,
        "Sets the output format for bytea.",
        "hex",
        [](const duckdb::Value& v) {
          auto s = v.ToString();
          return absl::EqualsIgnoreCase("hex", s) ||
                 absl::EqualsIgnoreCase("escape", s);
        },
      },
    },
    {
      "client_encoding",
      {
        LogicalTypeId::VARCHAR,
        "Sets the client's character set encoding.",
        "UTF8",
      },
    },
    {
      "application_name",
      {
        LogicalTypeId::VARCHAR,
        "Sets the application name to be reported in statistics and logs.",
        "",
      },
    },
    {
      pg::kDefaultTransactionIsolation,
      {
        LogicalTypeId::VARCHAR,
        "Sets the transaction isolation level of each new transaction.",
        "repeatable read",
        [](const duckdb::Value& v) {
          return pg::IsSupportedIsolationLevel(v.ToString());
        },
        [](duckdb::ClientContext& ctx, duckdb::SetScope, const std::string&,
           duckdb::Value& value, bool) {
          // When set outside a transaction, also update transaction_isolation
          auto& conn_ctx = connector::GetSereneDBContext(ctx);
          if (conn_ctx.IsAutoCommit()) {
            conn_ctx.SetSetting(pg::kTransactionIsolation, value.ToString(),
                                false);
          }
        },
      },
    },
    {
      pg::kTransactionIsolation,
      {
        LogicalTypeId::VARCHAR,
        "Sets the current transaction's isolation level.",
        "repeatable read",
        [](const duckdb::Value& v) {
          return pg::IsSupportedIsolationLevel(v.ToString());
        },
        [](duckdb::ClientContext& ctx, duckdb::SetScope, const std::string&,
           duckdb::Value&, bool is_reset) {
          if (is_reset) {
            throw duckdb::InvalidInputException(
              "parameter \"transaction_isolation\" cannot be reset");
          }
          auto& conn_ctx = connector::GetSereneDBContext(ctx);
          if (conn_ctx.IsAutoCommit()) {
            // Silently ignore SET transaction_isolation outside a transaction
            return;
          }
          if (conn_ctx.HasRocksDBRead() || conn_ctx.HasRocksDBWrite()) {
            throw duckdb::InvalidInputException(
              "SET TRANSACTION ISOLATION LEVEL must be called before "
              "any query");
          }
        },
      },
    },
    {
      "default_transaction_read_only",
      {
        LogicalTypeId::BOOLEAN,
        "Sets the default read-only status of new transactions.",
        "off",
      },
    },
    {
      "in_hot_standby",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether hot standby is currently active.",
        "off",
      },
    },
    {
      "integer_datetimes",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether datetimes are integer based.",
        "on",
      },
    },
    {
      "scram_iterations",
      {
        LogicalTypeId::INTEGER,
        "Sets the iteration count for SCRAM secret generation.",
        "4096",
      },
    },
    {
      "server_encoding",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server (database) character set encoding.",
        "UTF8",
      },
    },
    {
      "server_version",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server version.",
        "18.3",
      },
    },
    {
      "standard_conforming_strings",
      {
        LogicalTypeId::BOOLEAN,
        "Causes '...' strings to treat backslashes literally.",
        "on",
      },
    },
    {
      "client_min_messages",
      {
        LogicalTypeId::VARCHAR,
        "Sets the message levels that are sent to the client.",
        "notice",
      },
    },
    {
      "session_authorization",
      {
        LogicalTypeId::VARCHAR,
        "Sets the current session's user name.",
        StaticStrings::kDefaultUser,
      },
    },
    {
      "is_superuser",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether the current session's user is a superuser.",
        "on",
      },
    },
};

constexpr std::pair<std::string_view,
                    std::pair<std::string_view, VariableDescription>>
  kVariableDescriptionCanonical[] = {
    {
      "datestyle",
      {
        "DateStyle",
        {
          LogicalTypeId::VARCHAR,
          "Sets the display format for date and time values.",
          "ISO, MDY",
        },
      },
    },
    {
      "intervalstyle",
      {
        "IntervalStyle",
        {
          LogicalTypeId::VARCHAR,
          "Sets the display format for interval values.",
          "postgres",
        },
      },
    },
    {
      "timezone",
      {
        "TimeZone",
        {
          LogicalTypeId::VARCHAR,
          "Sets the time zone for displaying and interpreting time stamps.",
          "Etc/UTC",
        },
      },
    },
};

// Curated allowlist of DuckDB native settings exposed through Config.
// Only settings listed here are accessible via SET/SHOW/current_setting.
// DuckDB manages the actual values; we just provide metadata and gating.
// NOTE: default_value here is for documentation only -- the real default
// comes from DuckDB's own setting definition.
constexpr std::pair<std::string_view, VariableDescription>
  kDuckDBVariableDescription[] = {
    {
      "threads",
      {
        LogicalTypeId::INTEGER,
        "The number of threads used by the query executor.",
        "1",
      },
    },
    {
      "memory_limit",
      {
        LogicalTypeId::VARCHAR,
        "The maximum amount of memory the system can use.",
        "",
      },
    },
    {
      "temp_directory",
      {
        LogicalTypeId::VARCHAR,
        "The directory to which temporary files are written.",
        "",
      },
    },
};

constexpr auto kVarIndex =
  containers::MakeTrivialBiMapFirstToIndex<kVariableDescription>();
constexpr auto kVarCanonicalIndex =
  containers::MakeTrivialBiMapFirstToIndex<kVariableDescriptionCanonical>();
constexpr auto kDuckDBIndex =
  containers::MakeTrivialBiMapFirstToIndex<kDuckDBVariableDescription>();

std::optional<std::pair<std::string_view, VariableDescription>> GetDefault(
  std::string_view name) {
  if (auto idx = kVarIndex.TryFindICaseByFirst(name)) {
    return kVariableDescription[*idx];
  }
  if (auto idx = kVarCanonicalIndex.TryFindICaseByFirst(name)) {
    return kVariableDescriptionCanonical[*idx].second;
  }
  if (auto idx = kDuckDBIndex.TryFindICaseByFirst(name)) {
    return kDuckDBVariableDescription[*idx];
  }
  return std::nullopt;
}

std::optional<VariableDescription> GetDefaultDescription(
  std::string_view name) {
  return GetDefault(name).and_then(
    [](auto info) { return std::optional{info.second}; });
}

bool HasDefault(std::string_view name) {
  return static_cast<bool>(GetDefault(name));
}

std::string_view GetOriginalName(std::string_view name) {
  auto info = GetDefault(name);
  if (!info) {
    return {};
  }
  return info->first;
}

void Config::VisitFullDescription(
  absl::FunctionRef<void(std::string_view, std::string_view, std::string_view)>
    f) const {
  auto visit = [&](const auto& name, const auto& description) {
    auto value = Get(name);
    f(name, value.value_or(std::string{description.default_value}),
      description.description);
  };
  for (const auto& [name, description] : kVariableDescription) {
    visit(name, description);
  }

  for (const auto& [_, pair] : kVariableDescriptionCanonical) {
    const auto& [name, description] = pair;
    visit(name, description);
  }

  for (const auto& [name, description] : kDuckDBVariableDescription) {
    visit(name, description);
  }
}

namespace {

void OnSetCallback(duckdb::ClientContext& context, duckdb::SetScope scope,
                   const std::string& name, duckdb::Value& value,
                   bool is_reset) {
  auto desc = GetDefaultDescription(name);
  if (desc) {
    if (!is_reset && desc->validate && !desc->validate(value)) {
      throw duckdb::InvalidInputException(
        "invalid value for parameter \"%s\": \"%s\"", name, value.ToString());
    }
    if (desc->on_set) {
      desc->on_set(context, scope, name, value, is_reset);
    }
  }
  auto& conn_ctx = connector::GetSereneDBContext(context);
  conn_ctx.OnSet(name, scope == duckdb::SetScope::LOCAL);
}

void TryRegister(duckdb::DBConfig& config, std::string_view name,
                 const VariableDescription& desc) {
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  if (config
        .TryGetSettingIndex(duckdb::String::Reference(name.data(), name.size()),
                            option)
        .IsValid()) {
    return;  // already registered or built-in
  }
  config.AddExtensionOption(std::string{name}, std::string{desc.description},
                            duckdb::LogicalType{desc.type},
                            desc.default_value.data()
                              ? duckdb::Value{std::string{desc.default_value}}
                              : duckdb::Value{},
                            OnSetCallback);
}

}  // namespace
namespace connector {

void RegisterConfigVariables(duckdb::DBConfig& config) {
  for (const auto& [name, desc] : kVariableDescription) {
    TryRegister(config, name, desc);
  }
  for (const auto& [_, pair] : kVariableDescriptionCanonical) {
    const auto& [name, desc] = pair;
    TryRegister(config, name, desc);
  }

}

}  // namespace connector
}  // namespace sdb
