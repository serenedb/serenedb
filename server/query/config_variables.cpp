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

#include <algorithm>
#include <duckdb/common/assert.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/types/string.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <iterator>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <string>
#include <string_view>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "pg/commands/rbac.h"
#include "pg/connection_context.h"
#include "query/config.h"
#include "query/config_variable_names.h"

namespace sdb {

duckdb::Value SettingRef::Read(duckdb::ClientContext& context) const {
  auto& config = duckdb::DBConfig::GetConfig(context);
  auto slot = _slot.load(std::memory_order_relaxed);
  if (slot.config != &config) [[unlikely]] {
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    const auto index = config.TryGetSettingIndex(
      duckdb::String{_name.data(), static_cast<uint32_t>(_name.size())},
      option);
    SDB_ASSERT(index.IsValid());
    slot = {.config = &config, .index = index.GetIndex()};
    _slot.store(slot, std::memory_order_relaxed);
  }
  duckdb::Value value;
  auto found = context.config.user_settings.TryGetSetting(config.user_settings,
                                                          slot.index, value);
  if (!found) [[unlikely]] {
    auto res = context.TryGetCurrentSetting(std::string{_name}, value);
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

template<irs::utils::detail::FixedString Name>
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

// The `role` / `session_authorization` GUCs are thin shims over the identity
// switches in pg/commands/rbac (which own the catalog/authz logic, next to
// CREATE/ALTER ROLE); they only bridge the DuckDB Value <-> the string API.
void SetRoleCallback(duckdb::ClientContext& ctx, duckdb::SetScope,
                     duckdb::Value& value) {
  value = duckdb::Value{
    pg::SetRole(connector::GetSereneDBContext(ctx), value.ToString())};
}

void ResetRoleCallback(duckdb::ClientContext& ctx, duckdb::SetScope) {
  pg::ResetRole(connector::GetSereneDBContext(ctx));
}

void SetSessionAuthCallback(duckdb::ClientContext& ctx, duckdb::SetScope,
                            duckdb::Value& value) {
  value = duckdb::Value{pg::SetSessionAuthorization(
    connector::GetSereneDBContext(ctx), value.ToString())};
}

void ResetSessionAuthCallback(duckdb::ClientContext& ctx, duckdb::SetScope) {
  pg::ResetSessionAuthorization(connector::GetSereneDBContext(ctx));
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
    const bool ok = absl::StartsWith(token, "EURO") ||
                    absl::StartsWith(token, "NONEURO") ||
                    std::ranges::any_of(
                      kKnown, [&](std::string_view k) { return k == token; });
    if (!ok) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid value for parameter \"DateStyle\": \"", raw, "\""));
    }
  }
}

void CheckIntervalStyle(duckdb::ClientContext&, duckdb::SetScope,
                        duckdb::Value& value) {
  if (value.IsNull()) {
    return;
  }
  const auto token = absl::AsciiStrToUpper(value.ToString());
  if (token != "POSTGRES" && token != "POSTGRES_VERBOSE" &&
      token != "SQL_STANDARD" && token != "ISO_8601") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value for parameter \"IntervalStyle\": "
                            "\"",
                            value.ToString(), "\""));
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
      "sdb_ai_system1_default_secret",
      {
        LogicalTypeId::VARCHAR,
        "Name of the typesafe secret used by ai_system1 when the call does "
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
        "when false, that row returns NULL. Authentication, not-found and "
        "validation (422) errors always fail the query. Default: true.",
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
        LogicalTypeId::UBIGINT,
        "Maximum number of AI provider requests a single query may send. "
        "0 = unlimited. Default: 0.",
        [] { return duckdb::Value::UBIGINT(0); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_max_output_tokens_per_query",
      {
        LogicalTypeId::UBIGINT,
        "Maximum number of output tokens, as reported by the provider, a "
        "single query may consume; checked before each request, so requests "
        "already in flight may exceed it. 0 = unlimited. Default: 0.",
        [] { return duckdb::Value::UBIGINT(0); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_max_retries",
      {
        LogicalTypeId::UBIGINT,
        "How many times an AI provider request is retried after a connection "
        "error or HTTP 408, 429, 5xx or 529. Default: 3.",
        [] { return duckdb::Value::UBIGINT(3); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_retry_initial_delay_ms",
      {
        LogicalTypeId::UBIGINT,
        "Delay before the first AI provider retry, in milliseconds; each "
        "further retry doubles it, up to 60 seconds. A Retry-After response "
        "header overrides it, up to 60 "
        "seconds. Default: 500.",
        [] { return duckdb::Value::UBIGINT(500); },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value&) {},
      },
    },
    {
      "sdb_ai_request_timeout",
      {
        LogicalTypeId::UBIGINT,
        "Timeout of a single AI provider request, in seconds. Default: 120.",
        [] { return duckdb::Value::UBIGINT(120); },
        RejectZero<"sdb_ai_request_timeout">,
      },
    },
    {
      "sdb_ai_max_concurrent_requests",
      {
        LogicalTypeId::UBIGINT,
        "Maximum number of AI provider requests an AI_EVALUATE step, or one "
        "call outside it, has in flight. Requests run on DuckDB's async I/O "
        "threads (async_threads). Default: 16.",
        [] { return duckdb::Value::UBIGINT(16); },
        RejectZero<"sdb_ai_max_concurrent_requests">,
      },
    },
    {
      "sdb_ai_embedding_max_batch_size",
      {
        LogicalTypeId::UBIGINT,
        "Maximum number of texts ai_embed and ai_similarity send in one "
        "embeddings request. Default: 100.",
        [] { return duckdb::Value::UBIGINT(100); },
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
        "inverted indexes: every tick runs the ordinary REINDEX road "
        "(up_to_date / delta / rebuild) as the index owner. Per-index "
        "WITH (reindex_interval = ...) overrides; ALTER INDEX SET "
        "retunes a live loop. 0 disables the task. Hint-less iceberg "
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
        "INDEX backfill commit cadence). Per-object WITH (segment_memory_max = "
        "...) overrides. Default 268435456 (256MB).",
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
        LogicalTypeId::INTEGER,
        "Sets the number of digits displayed for floating-point values.",
        [] { return duckdb::Value{"1"}; },
        [](duckdb::ClientContext&, duckdb::SetScope, duckdb::Value& value) {
          auto n = value.GetValue<int32_t>();
          if (!(-15 <= n && n <= 3)) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"extra_float_digits\": \"",
                                    value.ToString(), "\""));
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
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                            ERR_MSG("invalid value for parameter "
                                    "\"bytea_output\": \"",
                                    value.ToString(), "\""));
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
        nullptr,  // refused via kUnchangeableSettings
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
        NoOverwrite<"scram_iterations">,
      },
    },
    {
      "server_encoding",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server (database) character set encoding.",
        [] { return duckdb::Value{"UTF8"}; },
        nullptr,  // refused via kUnchangeableSettings
      },
    },
    {
      "server_version",
      {
        LogicalTypeId::VARCHAR,
        "Shows the server version.",
        [] { return duckdb::Value{"18.3"}; },
        nullptr,  // refused via kUnchangeableSettings
      },
    },
    {
      "server_version_num",
      {
        LogicalTypeId::INTEGER,
        "Shows the server version as an integer.",
        [] { return duckdb::Value::INTEGER(180003); },
        nullptr,  // refused via kUnchangeableSettings
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
        [] {
          return duckdb::Value{std::string{irs::StaticStrings::kDefaultUser}};
        },
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

duckdb::Value ValidateSetting(duckdb::ClientContext& context,
                              std::string_view name,
                              const duckdb::Value& value) {
  duckdb::ExtensionOption option;
  duckdb::DBConfig::GetConfig(context).TryGetExtensionOption(std::string{name},
                                                             option);
  auto result = value.CastAs(context, option.type);
  option.set_function(context, duckdb::SetScope::AUTOMATIC, result);
  return result;
}

}  // namespace connector
}  // namespace sdb
