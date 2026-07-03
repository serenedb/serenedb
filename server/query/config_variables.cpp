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

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>

#include <duckdb/common/assert.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/types/string.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <iterator>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <string>

#include "basics/debugging.h"
#include "basics/serializer.h"
#include "basics/static_strings.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/config.h"

namespace sdb {

using duckdb::LogicalTypeId;

namespace {

template<basics::detail::FixedString Name>
void Readonly(duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {
  throw duckdb::InvalidInputException{"parameter \"%s\" cannot be changed",
                                      std::string_view{Name}.data()};
}

// Settings that are writable in principle but where serened doesn't yet honor
// the change. Emit a NOTICE so clients that care can see the SET is a no-op
// on the server side; the value still flows into DuckDB session state so
// SHOW round-trips what the client set.
template<basics::detail::FixedString Name>
void NoOverwrite(duckdb::ClientContext& ctx, duckdb::SetScope,
                 duckdb::Value& value) {
  constexpr std::string_view kName{Name};
  duckdb::Value current;
  if (!ctx.TryGetCurrentSetting(std::string{kName}, current)) {
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
      "parameter \"", kName,
      "\" is accepted for compatibility but is not enforced by serened")));
}

// PostgreSQL drivers supply client_encoding in many forms (UTF8, UTF-8, utf_8,
// utf8, 'utf-8', "utf-8", ...).
// To tackle this we have a special overload for encoding.
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
  throw duckdb::InvalidInputException{
    "parameter \"client_encoding\" cannot be changed from \"%s\" to \"%s\"",
    got_current ? current.ToString() : std::string{}, new_str};
}

// PG validates DateStyle as an output format (ISO / SQL / Postgres / German)
// and/or a field order (YMD / DMY / MDY, plus the EURO*, NONEURO* and US
// aliases), comma-separated in either order; an unrecognized token is
// invalid_parameter_value. serenedb only renders ISO, but it still rejects
// garbage so a bad value is a user error, not silently stored.
void CheckDateStyle(duckdb::ClientContext&, duckdb::SetScope,
                    duckdb::Value& value) {
  if (value.IsNull()) {
    return;
  }
  const std::string raw = value.ToString();
  for (std::string_view field : absl::StrSplit(raw, ',', absl::SkipEmpty())) {
    const auto token = absl::AsciiStrToUpper(absl::StripAsciiWhitespace(field));
    static constexpr std::string_view kKnown[] = {
      "ISO", "SQL", "POSTGRES", "GERMAN", "YMD", "DMY", "MDY", "US", "DEFAULT"};
    bool ok =
      absl::StartsWith(token, "EURO") || absl::StartsWith(token, "NONEURO");
    for (const auto known : kKnown) {
      ok = ok || token == known;
    }
    if (!ok) {
      throw duckdb::InvalidInputException(
        "invalid value for parameter \"DateStyle\": \"%s\"", raw);
    }
  }
}

// PG's IntervalStyle is an enum GUC: postgres / postgres_verbose /
// sql_standard / iso_8601. Anything else is invalid_parameter_value.
void CheckIntervalStyle(duckdb::ClientContext&, duckdb::SetScope,
                        duckdb::Value& value) {
  if (value.IsNull()) {
    return;
  }
  const auto token = absl::AsciiStrToUpper(value.ToString());
  if (token != "POSTGRES" && token != "POSTGRES_VERBOSE" &&
      token != "SQL_STANDARD" && token != "ISO_8601") {
    throw duckdb::InvalidInputException(
      "invalid value for parameter \"IntervalStyle\": \"%s\"",
      value.ToString());
  }
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
        [] { return duckdb::Value{""}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
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
        },
        [](duckdb::ClientContext&, duckdb::SetScope) {
          ClearFailurePointsDebugging();
        },
        // SESSION scope: fault points are a test-only hook, not persistent
        // config. GLOBAL would block `SET sdb_faults` inside transactions,
        // but recovery tests need to arm a fault inside an uncommitted txn
        // before triggering the crash.
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
    // Logging knobs (level, type filters, storage, on/off) live in duckdb's
    // built-in settings: logging_level / enable_logging / enabled_log_types
    // / disabled_log_types / logging_storage / logging_mode. The previous
    // sdb_log_level extension option was dropped in favour of those.
    {
      "sdb_strict_ddl",
      {
        LogicalTypeId::BOOLEAN,
        "When enabled, DDL inside a transaction block fails instead of "
        "committing immediately (DDL is not transactional).",
        [] { return duckdb::Value::BOOLEAN(false); },
      },
    },
    {
      "sdb_nprobe",
      {
        LogicalTypeId::INTEGER,
        "Number of IVF cluster lists scanned per vector-similarity query. "
        "Higher values improve recall at the cost of latency. Default 8.",
        [] { return duckdb::Value::INTEGER(8); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n < 1) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"sdb_nprobe\": \"%s\"",
              value.ToString()};
          }
        },
      },
    },
    {
      "sdb_rerank_factor",
      {
        LogicalTypeId::INTEGER,
        "Multiplier applied to LIMIT k to size the candidate pool re-scored "
        "with exact distances for a quantized IVF vector-similarity query "
        "(pool = sdb_rerank_factor * k). Higher values improve recall at the "
        "cost of latency; 0 disables reranking (top-k picked by the "
        "approximate quantized distance). Default 4. Unquantized (quant = "
        "'none') indexes never rerank, regardless of this setting.",
        [] { return duckdb::Value::INTEGER(4); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n < 0) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"sdb_rerank_factor\": \"%s\"",
              value.ToString()};
          }
        },
      },
    },
    {
      "sdb_scored_terms_limit",
      {
        LogicalTypeId::INTEGER,
        "The maximum number of terms to consider for scoring in multi-term "
        "filters. Higher values give more accurate IDF-style scoring at the "
        "cost of memory and per-query work. 0 disables scored-term collection "
        "entirely.",
        [] { return duckdb::Value::INTEGER(1024); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (n < 0) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"sdb_scored_terms_limit\": "
              "\"%s\"",
              value.ToString()};
          }
        },
      },
    },
    {
      "sdb_disable_top_k_optimization",
      {
        LogicalTypeId::BOOLEAN,
        "When true, the optimizer skips pulling `ORDER BY <scorer>(...) "
        "DESC LIMIT k` into the inverted-index scan, so WAND (Block-Max "
        "top-K) pruning never engages. Default: false (optimization on).",
        [] { return duckdb::Value::BOOLEAN(false); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "row_group_size",
      {
        LogicalTypeId::UINTEGER,
        "Default column row-group size for INCLUDEd in newly created inverted "
        "indexes. "
        "Per-column (row_group_size = ...) and per-index WITH (row_group_size "
        "= ...) override. Reads from existing indexes are unaffected. "
        "Default: 122'880.",
        [] { return duckdb::Value::UINTEGER(DEFAULT_ROW_GROUP_SIZE); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          const auto n = value.GetValue<uint32_t>();
          if (n == 0) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"row_group_size\": \"%s\"",
              value.ToString()};
          }
        },
      },
    },
    {
      "norm_row_group_size",
      {
        LogicalTypeId::UINTEGER,
        "Default column row-group size for norm columns of text-indexed fields "
        "(Norm feature) in newly created inverted indexes. "
        "Per-column (row_group_size = ...) and per-index WITH (row_group_size "
        "= ...) override. Reads from existing indexes are unaffected. "
        "Default: 122'880.",
        [] { return duckdb::Value::UINTEGER(122'880); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          const auto n = value.GetValue<uint32_t>();
          if (n == 0) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"norm_row_group_size\": \"%s\"",
              value.ToString()};
          }
        },
      },
    },
    {
      "refresh_interval",
      {
        LogicalTypeId::UINTEGER,
        "Background refresh interval (ms) for newly created inverted indexes. "
        "Per-index WITH (refresh_interval = ...) overrides. 0 disables the "
        "refresh task. Default: 1000.",
        [] { return duckdb::Value::UINTEGER(1000); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "compaction_interval",
      {
        LogicalTypeId::UINTEGER,
        "Background compaction interval (ms) for newly created inverted "
        "indexes. Per-index WITH (compaction_interval = ...) overrides. "
        "0 disables the compaction task. Default: 1000.",
        [] { return duckdb::Value::UINTEGER(1000); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "cleanup_interval_step",
      {
        LogicalTypeId::UINTEGER,
        "Number of commit ticks between background unreferenced-file cleanup "
        "passes for newly created inverted indexes. Per-index WITH "
        "(cleanup_interval_step = ...) overrides. 0 disables cleanup. "
        "Default: 1.",
        [] { return duckdb::Value::UINTEGER(1); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "extra_float_digits",
      {
        LogicalTypeId::INTEGER,
        "Sets the number of digits displayed for floating-point values.",
        [] { return duckdb::Value{"1"}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (!(-15 <= n && n <= 3)) {
            throw duckdb::InvalidInputException{
              "invalid value for parameter \"extra_float_digits\": \"%s\"",
              value.ToString()};
          }
        },
      },
    },
    {
      "bytea_output",
      {
        LogicalTypeId::VARCHAR,
        "Sets the output format for bytea.",
        [] { return duckdb::Value{"hex"}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          if (!magic_enum::enum_cast<ByteaOutput>(value.ToString(),
                                                  magic_enum::case_insensitive)
                 .has_value()) {
            throw duckdb::InvalidInputException(
              "invalid value for parameter \"bytea_output\": \"%s\"",
              value.ToString());
          }
        },
      },
    },
    {
      "client_encoding",
      {
        LogicalTypeId::VARCHAR,
        "Sets the client's character set encoding.",
        [] { return duckdb::Value{"UTF8"}; },
        NoOverwriteClientEncoding,
      },
    },
    {
      "application_name",
      {
        LogicalTypeId::VARCHAR,
        "Sets the application name to be reported in statistics and logs.",
        [] { return duckdb::Value{""}; },
        CheckApplicationName,
      },
    },
    {
      "in_hot_standby",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether hot standby is currently active.",
        [] { return duckdb::Value{false}; },
        Readonly<"in_hot_standby">,
      },
    },
    {
      "integer_datetimes",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether datetimes are integer based.",
        [] { return duckdb::Value{true}; },
        NoOverwrite<"integer_datetimes">,
      },
    },
    {
      "scram_iterations",
      {
        LogicalTypeId::INTEGER,
        "Sets the iteration count for SCRAM secret generation.",
        [] { return duckdb::Value{"4096"}; },
        Readonly<"scram_iterations">,
      },
    },
    {
      "server_encoding",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server (database) character set encoding.",
        [] { return duckdb::Value{"UTF8"}; },
        Readonly<"server_encoding">,
      },
    },
    {
      "server_version",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server version.",
        [] { return duckdb::Value{"18.3"}; },
        Readonly<"server_version">,
      },
    },
    {
      "server_version_num",
      {
        LogicalTypeId::INTEGER,
        "Shows the server version as an integer.",
        [] { return duckdb::Value::INTEGER(180003); },
        Readonly<"server_version_num">,
      },
    },
    {
      "standard_conforming_strings",
      {
        LogicalTypeId::BOOLEAN,
        "Causes '...' strings to treat backslashes literally.",
        [] { return duckdb::Value{true}; },
        NoOverwrite<"standard_conforming_strings">,
      },
    },
    {
      "client_min_messages",
      {
        LogicalTypeId::VARCHAR,
        "Sets the message levels that are sent to the client.",
        [] { return duckdb::Value{"notice"}; },
        NoOverwrite<"client_min_messages">,
      },
    },
    {
      "statement_timeout",
      {
        LogicalTypeId::VARCHAR,
        "Aborts any statement that takes more than the specified number of "
        "milliseconds. Accepted for compatibility but not currently enforced.",
        [] { return duckdb::Value{"0"}; },
        NoOverwrite<"statement_timeout">,
      },
    },
    {
      "session_authorization",
      {
        LogicalTypeId::VARCHAR,
        "Sets the current session's user name.",
        [] { return duckdb::Value{std::string{StaticStrings::kDefaultUser}}; },
        NoOverwrite<"session_authorization">,
      },
    },
    {
      "role",
      {
        LogicalTypeId::VARCHAR,
        "Sets the current role.",
        [] { return duckdb::Value{std::string{StaticStrings::kDefaultUser}}; },
        NoOverwrite<"role">,
      },
    },
    {
      "is_superuser",
      {
        LogicalTypeId::BOOLEAN,
        "Shows whether the current session's user is a superuser.",
        [] { return duckdb::Value{true}; },
        Readonly<"is_superuser">,
      },
    },
    {
      "DateStyle",
      {
        LogicalTypeId::VARCHAR,
        "Sets the display format for date and time values.",
        [] { return duckdb::Value{"ISO, MDY"}; },
        CheckDateStyle,
      },
    },
    {
      "IntervalStyle",
      {
        LogicalTypeId::VARCHAR,
        "Sets the display format for interval values.",
        [] { return duckdb::Value{"postgres"}; },
        CheckIntervalStyle,
      },
    },
    {
      "TimeZone",
      {
        LogicalTypeId::VARCHAR,
        "Sets the time zone for displaying and interpreting time stamps.",
        [] { return duckdb::Value{"Etc/UTC"}; },
      },
    },
};

const duckdb::case_insensitive_set_view_t kVariableIndex = [] {
  duckdb::case_insensitive_set_view_t m;
  m.reserve(std::size(kVariableDescription));
  for (const auto& entry : kVariableDescription) {
    m.emplace(entry.first);
  }
  return m;
}();

}  // namespace

std::string_view GetOriginalName(std::string_view name) {
  auto it = kVariableIndex.find(name);
  if (it == kVariableIndex.end()) {
    return {};
  }
  return *it;
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
  config.AddExtensionOption(
    std::string{name}, std::string{desc.description},
    duckdb::LogicalType{desc.type},
    desc.default_value ? desc.default_value() : duckdb::Value{},
    desc.set_callback, desc.reset_callback, desc.scope);
}

}  // namespace
namespace connector {

void RegisterConfigVariables(duckdb::DBConfig& config) {
  for (const auto& [name, desc] : kVariableDescription) {
    TryRegister(config, name, desc);
  }
}

}  // namespace connector
}  // namespace sdb
