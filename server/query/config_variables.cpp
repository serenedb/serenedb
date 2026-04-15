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
#include <magic_enum/magic_enum.hpp>
#include <string>

#include "basics/assert.h"
#include "basics/containers/trivial_map.h"
#include "basics/debugging.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/isolation_level.h"
#include "query/config.h"
#include "vpack/serializer.h"

namespace sdb {

using duckdb::LogicalTypeId;

namespace {

template<vpack::detail::FixedString Name>
void Readonly(duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {
  throw duckdb::InvalidInputException{"parameter \"%s\" cannot be changed",
                                      std::string_view{Name}.data()};
}

void EnsureNotLocal(std::string_view name, duckdb::SetScope scope) {
  if (scope == duckdb::SetScope::LOCAL) {
    throw duckdb::InvalidInputException{
      "parameter \"%s\" cannot be set locally", name};
  }
}

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
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope,
           duckdb::Value& value) {
          static constexpr std::string_view kName = "sdb_faults";
          EnsureNotLocal(kName, scope);

          auto s = value.ToString();
          if (s.starts_with('-')) {
            if (!RemoveFailurePointDebugging(std::string_view{s}.substr(1))) {
              throw duckdb::InvalidInputException("failure point '%s' not set",
                                                  s);
            }
          } else {
            if (!AddFailurePointDebugging(s)) {
              throw duckdb::InvalidInputException(
                "failure point '%s' already set", s);
            }
          }
          auto points = GetFailurePointsDebugging();
          value = duckdb::Value(absl::StrJoin(points, ","));

          connector::GetSereneDBContext(ctx).OnSet(kName, false);
        },
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope) {
          static constexpr std::string_view kName = "sdb_faults";
          EnsureNotLocal(kName, scope);

          ClearFailurePointsDebugging();
          connector::GetSereneDBContext(ctx).OnSet(kName, false);
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
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope,
           duckdb::Value& value) {
          EnsureNotLocal(log::kLogLevelVariable, scope);
          log::SetLogLevel(value.ToString());
          value = duckdb::Value(log::LogLevelString());

          connector::GetSereneDBContext(ctx).OnSet(log::kLogLevelVariable,
                                                   false);
        },
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope) {
          EnsureNotLocal(log::kLogLevelVariable, scope);
          log::ResetLogLevels();

          connector::GetSereneDBContext(ctx).OnSet(log::kLogLevelVariable,
                                                   false);
        },
      },
    },
    {
      "sdb_write_conflict_policy",
      {
        LogicalTypeId::VARCHAR,
        "Sets the write conflict policy. Valid values are "
        "'emit_error' (the default), 'do_nothing' (skip conflicted rows) and "
        "'replace'.",
        "emit_error",
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope,
           duckdb::Value& value) {
          static constexpr std::string_view kName = "sdb_write_conflict_policy";

          if (!magic_enum::enum_cast<WriteConflictPolicy>(
                 value.ToString(), magic_enum::case_insensitive)
                 .has_value()) {
            throw duckdb::InvalidInputException(
              "invalid value for parameter \"%s\": "
              "\"%s\"",
              kName.data(), value.ToString());
          }

          connector::GetSereneDBContext(ctx).OnSet(
            kName, scope == duckdb::SetScope::LOCAL);
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
    {
      "extra_float_digits",
      {
        LogicalTypeId::INTEGER,
        "Sets the number of digits displayed for floating-point values.",
        "1",
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope,
           duckdb::Value& value) {
          static constexpr std::string_view kName = "extra_float_digits";

          auto n = value.GetValue<int32_t>();
          if (!(-15 <= n && n <= 3)) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"%s\": \"%s\"", kName.data(),
              value.ToString()};
          }

          connector::GetSereneDBContext(ctx).OnSet(
            kName, scope == duckdb::SetScope::LOCAL);
        },
      },
    },
    {
      "bytea_output",
      {
        LogicalTypeId::VARCHAR,
        "Sets the output format for bytea.",
        "hex",
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope,
           duckdb::Value& value) {
          static constexpr std::string_view kName = "bytea_output";

          if (!magic_enum::enum_cast<ByteaOutput>(value.ToString(),
                                                  magic_enum::case_insensitive)
                 .has_value()) {
            throw duckdb::InvalidInputException(
              "invalid value for parameter \"%s\": \"%s\"", kName.data(),
              value.ToString());
          }

          connector::GetSereneDBContext(ctx).OnSet(
            kName, scope == duckdb::SetScope::LOCAL);
        },
      },
    },
    {
      "client_encoding",
      {
        LogicalTypeId::VARCHAR,
        "Sets the client's character set encoding.",
        "UTF8",
        Readonly<"client_encoding">,
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
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope,
           duckdb::Value& value) {
          if (!pg::IsSupportedIsolationLevel(value.ToString())) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"%s\": \"%s\"",
              pg::kDefaultTransactionIsolation.data(), value.ToString()};
          }
          auto& conn_ctx = connector::GetSereneDBContext(ctx);
          if (conn_ctx.IsAutoCommit()) {
            conn_ctx.SetSetting(pg::kTransactionIsolation, value.ToString(),
                                false);
          }
          conn_ctx.OnSet(pg::kDefaultTransactionIsolation,
                         scope == duckdb::SetScope::LOCAL);
        },
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope) {
          connector::GetSereneDBContext(ctx).OnSet(
            pg::kDefaultTransactionIsolation, scope == duckdb::SetScope::LOCAL);
        },
      },
    },
    {
      pg::kTransactionIsolation,
      {
        LogicalTypeId::VARCHAR,
        "Sets the current transaction's isolation level.",
        "repeatable read",
        [](duckdb::ClientContext& ctx, duckdb::SetScope scope,
           duckdb::Value& val) {
          if (!pg::IsSupportedIsolationLevel(val.ToString())) {
            throw duckdb::InvalidInputException(
              "invalid value for parameter \"%s\": \"%s\"",
              pg::kTransactionIsolation.data(), val.ToString());
          }
          auto& conn_ctx = connector::GetSereneDBContext(ctx);
          if (conn_ctx.IsAutoCommit()) {
            return;
          }
          if (conn_ctx.HasRocksDBRead() || conn_ctx.HasRocksDBWrite()) {
            throw duckdb::InvalidInputException(
              "SET TRANSACTION ISOLATION LEVEL must be called before "
              "any query");
          }
          conn_ctx.OnSet(pg::kTransactionIsolation,
                         scope == duckdb::SetScope::LOCAL);
        },
        [](duckdb::ClientContext&, duckdb::SetScope) {
          throw duckdb::InvalidInputException{
            "parameter \"%s\" cannot be reset",
            pg::kTransactionIsolation.data()};
        },
      },
    },
    {
      "default_transaction_read_only",
      {
        LogicalTypeId::BOOLEAN,
        "Sets the default read-only status of new transactions.",
        "off",
        Readonly<"default_transaction_read_only">,
      },
    },
    {
      "in_hot_standby",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether hot standby is currently active.",
        "off",
        Readonly<"in_hot_standby">,
      },
    },
    {
      "integer_datetimes",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether datetimes are integer based.",
        "on",
        Readonly<"integer_datetimes">,
      },
    },
    {
      "scram_iterations",
      {
        LogicalTypeId::INTEGER,
        "Sets the iteration count for SCRAM secret generation.",
        "4096",
        Readonly<"scram_iterations">,
      },
    },
    {
      "server_encoding",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server (database) character set encoding.",
        "UTF8",
        Readonly<"server_encoding">,
      },
    },
    {
      "server_version",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server version.",
        "18.3",
        Readonly<"server_version">,
      },
    },
    {
      "standard_conforming_strings",
      {
        LogicalTypeId::BOOLEAN,
        "Causes '...' strings to treat backslashes literally.",
        "on",
        Readonly<"standard_conforming_strings">,
      },
    },
    {
      "client_min_messages",
      {
        LogicalTypeId::VARCHAR,
        "Sets the message levels that are sent to the client.",
        "notice",
        Readonly<"client_min_messages">,
      },
    },
    {
      "session_authorization",
      {
        LogicalTypeId::VARCHAR,
        "Sets the current session's user name.",
        StaticStrings::kDefaultUser,
        Readonly<"session_authorization">,
      },
    },
    {
      "is_superuser",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether the current session's user is a superuser.",
        "on",
        Readonly<"is_superuser">,
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
          Readonly<"DateStyle">,
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
          Readonly<"IntervalStyle">,
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
          Readonly<"TimeZone">,
        },
      },
    },
};

constexpr auto kVarIndex =
  containers::MakeTrivialBiMapFirstToIndex<kVariableDescription>();
constexpr auto kVarCanonicalIndex =
  containers::MakeTrivialBiMapFirstToIndex<kVariableDescriptionCanonical>();

}  // namespace

std::optional<std::pair<std::string_view, VariableDescription>> GetDefault(
  std::string_view name) {
  if (auto idx = kVarIndex.TryFindICaseByFirst(name)) {
    return kVariableDescription[*idx];
  }
  if (auto idx = kVarCanonicalIndex.TryFindICaseByFirst(name)) {
    return kVariableDescriptionCanonical[*idx].second;
  }
  return std::nullopt;
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
}

namespace {

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
                            desc.set_callback, desc.reset_callback);
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
