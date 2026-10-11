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
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "network/cancel_registry.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "pg/types.h"
#include "search/search_table.h"
#include "server/utils/build.h"

namespace sdb::pg {
namespace {

using connector::AsView;
using connector::GetSereneDBContext;
using duckdb::CatalogType;
using duckdb::Identifier;

template<std::optional<bool> (*Visible)(const Session&, uint64_t)>
void IsVisibleFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, bool>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<bool> {
      if (oid <= 0) {
        return duckdb::nullopt;
      }
      return Visible(session, static_cast<uint64_t>(oid));
    });
}

void PgGetUserByIdFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  const auto roles = auth::RolesOf(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](int64_t oid) {
      const auto name = oid >= 0
                          ? roles->NameOf(static_cast<duckdb::idx_t>(oid))
                          : std::string_view{};
      if (!name.empty()) {
        return duckdb::StringVector::AddString(result, name);
      }
      return duckdb::StringVector::AddString(
        result, absl::StrCat("unknown (OID=", oid, ")"));
    });
}

duckdb::optional<duckdb::string_t> Comment(duckdb::Vector& result,
                                           const duckdb::Value& comment) {
  if (comment.IsNull()) {
    return duckdb::nullopt;
  }
  const auto& text = duckdb::StringValue::Get(comment);
  if (text.empty()) {
    return duckdb::nullopt;
  }
  return duckdb::StringVector::AddString(result, text);
}

void ObjDescription2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t oid,
        duckdb::string_t catalog) -> duckdb::optional<duckdb::string_t> {
      const auto* table =
        GetSystemTable(irs::StaticStrings::kPgCatalogSchema, AsView(catalog));
      auto entry = EntryByOid(session, oid);
      if (!table || !entry ||
          CatalogClassOid(entry->type) != table->Sql().oid) {
        return duckdb::nullopt;
      }
      return Comment(result, entry->comment);
    });
}

void ObjDescription1Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<duckdb::string_t> {
      auto entry = EntryByOid(session, oid);
      if (!entry || CatalogClassOid(entry->type) == kInvalidOid) {
        return duckdb::nullopt;
      }
      return Comment(result, entry->comment);
    });
}

void ColDescriptionFunction(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t, int32_t>(
    args, result,
    [&](int64_t oid, int32_t column) -> duckdb::optional<duckdb::string_t> {
      auto entry = EntryByOid(session, oid);
      if (!entry || column <= 0) {
        return duckdb::nullopt;
      }
      const auto index = static_cast<duckdb::idx_t>(column - 1);
      if (entry->type == CatalogType::TABLE_ENTRY) {
        const auto& columns =
          entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
        if (index >= columns.LogicalColumnCount()) {
          return duckdb::nullopt;
        }
        return Comment(
          result, columns.GetColumn(duckdb::LogicalIndex(index)).Comment());
      }
      if (entry->type == CatalogType::VIEW_ENTRY) {
        auto& view = entry->Cast<duckdb::ViewCatalogEntry>();
        const auto info = view.GetColumnInfo();
        if (!info || index >= info->names.size()) {
          return duckdb::nullopt;
        }
        return Comment(result, view.GetColumnComment(index));
      }
      return duckdb::nullopt;
    });
}

void PgGetSerialSequenceFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::VariadicExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t table_name,
        duckdb::string_t column_name) -> duckdb::optional<duckdb::string_t> {
      const auto name = duckdb::QualifiedName::Parse(table_name.GetString());
      auto relation = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_ENTRY, name},
        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!relation) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", AsView(table_name), "\" does not exist"));
      }
      const Identifier column_key{AsView(column_name)};
      if (relation->type == duckdb::CatalogType::VIEW_ENTRY) {
        if (ViewAttnum(context, relation->Cast<duckdb::ViewCatalogEntry>(),
                       column_key) == 0) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
            ERR_MSG("column \"", AsView(column_name), "\" of relation \"",
                    relation->name.GetIdentifierName(), "\" does not exist"));
        }
        return duckdb::nullopt;
      }
      if (relation->type != duckdb::CatalogType::TABLE_ENTRY) {
        return duckdb::nullopt;
      }
      const auto* table = &relation->Cast<duckdb::TableCatalogEntry>();
      const auto& columns = table->GetColumns();
      if (!columns.ColumnExists(column_key)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("column \"", AsView(column_name), "\" of relation \"",
                  table->name.GetIdentifierName(), "\" does not exist"));
      }
      const auto& column = columns.GetColumn(column_key);
      if (!column.HasDefaultValue()) {
        return duckdb::nullopt;
      }
      const auto argument = NextvalArgument(column.DefaultValue());
      auto database = SessionCatalog(context);
      if (!argument || !database) {
        return duckdb::nullopt;
      }
      const auto sequence_name = duckdb::QualifiedName::Parse(*argument);
      const auto transaction = database->GetCatalogTransaction(context);
      const auto schema_name = sequence_name.Schema().empty()
                                 ? table->ParentSchemaName()
                                 : sequence_name.Schema();
      auto schema = database->GetSchema(transaction, schema_name,
                                        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!schema) {
        return duckdb::nullopt;
      }
      auto sequence = schema->LookupEntry(
        transaction, duckdb::EntryLookupInfo{CatalogType::SEQUENCE_ENTRY,
                                             sequence_name.Name()});
      if (!sequence) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(
        result, QualifiedOutName(schema->name.GetIdentifierName(),
                                 sequence->name.GetIdentifierName()));
    });
}

constexpr int32_t kAllUpdateEvents = (1 << 2) | (1 << 3) | (1 << 4);

int32_t UpdatableEvents(const Session& session, int64_t oid) {
  auto entry = EntryByOid(session, oid);
  return entry && entry->type == CatalogType::TABLE_ENTRY ? kAllUpdateEvents
                                                          : 0;
}

void PgRelationIsUpdatableFunction(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<int32_t, int64_t, bool>(
    args, result,
    [&](int64_t oid, bool) { return UpdatableEvents(session, oid); });
}

void PgColumnIsUpdatableFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int16_t, bool>(
    args, result, [&](int64_t oid, int16_t attnum, bool) {
      return attnum > 0 && UpdatableEvents(session, oid) == kAllUpdateEvents;
    });
}

void PgSequenceLastValueFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<int64_t> {
      auto entry = EntryByOid(session, oid);
      if (!entry) {
        return duckdb::nullopt;
      }
      if (entry->type != CatalogType::SEQUENCE_ENTRY) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                        ERR_MSG("\"", entry->name.GetIdentifierName(),
                                "\" is not a sequence"));
      }
      const auto data = entry->Cast<duckdb::SequenceCatalogEntry>().GetData();
      if (!data.last_value) {
        return duckdb::nullopt;
      }
      return *data.last_value;
    });
}

void ClockTimestampFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  const duckdb::timestamp_tz_t now{duckdb::Timestamp::GetCurrentTimestamp()};
  result.Reference(duckdb::Value::TIMESTAMPTZ(now),
                   duckdb::count_t(args.size()));
}

constexpr std::string_view kEncodings[] = {
  "SQL_ASCII",    "EUC_JP",         "EUC_CN",        "EUC_KR",     "EUC_TW",
  "EUC_JIS_2004", "UTF8",           "MULE_INTERNAL", "LATIN1",     "LATIN2",
  "LATIN3",       "LATIN4",         "LATIN5",        "LATIN6",     "LATIN7",
  "LATIN8",       "LATIN9",         "LATIN10",       "WIN1256",    "WIN1258",
  "WIN866",       "WIN874",         "KOI8R",         "WIN1251",    "WIN1252",
  "ISO_8859_5",   "ISO_8859_6",     "ISO_8859_7",    "ISO_8859_8", "WIN1250",
  "WIN1253",      "WIN1254",        "WIN1255",       "WIN1257",    "KOI8U",
  "SJIS",         "BIG5",           "GBK",           "UHC",        "GB18030",
  "JOHAB",        "SHIFT_JIS_2004",
};

struct EncodingAlias {
  std::string_view alias;
  int32_t encoding;
};

constexpr EncodingAlias kEncodingAliases[] = {
  {"abc", 19},         {"alt", 20},         {"big5", 36},
  {"euccn", 2},        {"eucjis2004", 5},   {"eucjp", 1},
  {"euckr", 3},        {"euctw", 4},        {"gb18030", 39},
  {"gbk", 37},         {"iso88591", 8},     {"iso885910", 13},
  {"iso885913", 14},   {"iso885914", 15},   {"iso885915", 16},
  {"iso885916", 17},   {"iso88592", 9},     {"iso88593", 10},
  {"iso88594", 11},    {"iso88595", 25},    {"iso88596", 26},
  {"iso88597", 27},    {"iso88598", 28},    {"iso88599", 12},
  {"johab", 40},       {"koi8", 22},        {"koi8r", 22},
  {"koi8u", 34},       {"latin1", 8},       {"latin10", 17},
  {"latin2", 9},       {"latin3", 10},      {"latin4", 11},
  {"latin5", 12},      {"latin6", 13},      {"latin7", 14},
  {"latin8", 15},      {"latin9", 16},      {"mskanji", 35},
  {"muleinternal", 7}, {"shiftjis", 35},    {"shiftjis2004", 41},
  {"sjis", 35},        {"sqlascii", 0},     {"tcvn", 19},
  {"tcvn5712", 19},    {"uhc", 38},         {"unicode", 6},
  {"utf8", 6},         {"vscii", 19},       {"win", 23},
  {"win1250", 29},     {"win1251", 23},     {"win1252", 24},
  {"win1253", 30},     {"win1254", 31},     {"win1255", 32},
  {"win1256", 18},     {"win1257", 33},     {"win1258", 19},
  {"win866", 20},      {"win874", 21},      {"win932", 35},
  {"win936", 37},      {"win949", 38},      {"win950", 36},
  {"windows1250", 29}, {"windows1251", 23}, {"windows1252", 24},
  {"windows1253", 30}, {"windows1254", 31}, {"windows1255", 32},
  {"windows1256", 18}, {"windows1257", 33}, {"windows1258", 19},
  {"windows866", 20},  {"windows874", 21},  {"windows932", 35},
  {"windows936", 37},  {"windows949", 38},  {"windows950", 36},
};

void PgEncodingToCharFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int32_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](int32_t encoding) {
      if (encoding < 0 ||
          static_cast<size_t>(encoding) >= std::size(kEncodings)) {
        return duckdb::StringVector::AddString(result, std::string_view{});
      }
      return duckdb::StringVector::AddString(result, kEncodings[encoding]);
    });
}

void PgCharToEncodingFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int32_t>(
    args.data[0], result, args.size(), [](duckdb::string_t name) {
      std::string clean;
      for (const auto c : AsView(name)) {
        if (absl::ascii_isalnum(c)) {
          clean.push_back(absl::ascii_tolower(c));
        }
      }
      const auto it = absl::c_find_if(
        kEncodingAliases, [&](const auto& a) { return a.alias == clean; });
      return it == std::end(kEncodingAliases) ? -1 : it->encoding;
    });
}

duckdb::unique_ptr<duckdb::Expression> BindZero(
  duckdb::FunctionBindExpressionInput& input) {
  const auto& type = input.bound_function.GetReturnType();
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::INTEGER(0).DefaultCastAs(type));
}

duckdb::unique_ptr<duckdb::Expression> BindNull(
  duckdb::FunctionBindExpressionInput& input) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value{input.bound_function.GetReturnType()});
}

template<typename T>
void ZeroFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int64_t, T>(args.data[0], result, args.size(),
                                             [](int64_t) { return T{0}; });
}

enum class StatKind : uint8_t {
  Count,
  Time,
  Timestamp,
  UntrackedCount,
  UntrackedTime,
};

struct StatFunction {
  std::string_view name;
  bool takes_oid;
  StatKind kind;
};

constexpr StatFunction kStatFunctions[] = {
  {"pg_stat_get_analyze_count", true, StatKind::Count},
  {"pg_stat_get_autoanalyze_count", true, StatKind::Count},
  {"pg_stat_get_autovacuum_count", true, StatKind::Count},
  {"pg_stat_get_bgwriter_buf_written_clean", false, StatKind::Count},
  {"pg_stat_get_bgwriter_maxwritten_clean", false, StatKind::Count},
  {"pg_stat_get_bgwriter_stat_reset_time", false, StatKind::Timestamp},
  {"pg_stat_get_blocks_fetched", true, StatKind::Count},
  {"pg_stat_get_blocks_hit", true, StatKind::Count},
  {"pg_stat_get_buf_alloc", false, StatKind::Count},
  {"pg_stat_get_checkpointer_buffers_written", false, StatKind::Count},
  {"pg_stat_get_checkpointer_num_performed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_num_requested", false, StatKind::Count},
  {"pg_stat_get_checkpointer_num_timed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_restartpoints_performed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_restartpoints_requested", false, StatKind::Count},
  {"pg_stat_get_checkpointer_restartpoints_timed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_slru_written", false, StatKind::Count},
  {"pg_stat_get_checkpointer_stat_reset_time", false, StatKind::Timestamp},
  {"pg_stat_get_checkpointer_sync_time", false, StatKind::Time},
  {"pg_stat_get_checkpointer_write_time", false, StatKind::Time},
  {"pg_stat_get_db_active_time", true, StatKind::Time},
  {"pg_stat_get_db_blk_read_time", true, StatKind::Time},
  {"pg_stat_get_db_blk_write_time", true, StatKind::Time},
  {"pg_stat_get_db_blocks_fetched", true, StatKind::Count},
  {"pg_stat_get_db_blocks_hit", true, StatKind::Count},
  {"pg_stat_get_db_checksum_failures", true, StatKind::Count},
  {"pg_stat_get_db_checksum_last_failure", true, StatKind::Timestamp},
  {"pg_stat_get_db_conflict_all", true, StatKind::Count},
  {"pg_stat_get_db_conflict_bufferpin", true, StatKind::Count},
  {"pg_stat_get_db_conflict_lock", true, StatKind::Count},
  {"pg_stat_get_db_conflict_logicalslot", true, StatKind::Count},
  {"pg_stat_get_db_conflict_snapshot", true, StatKind::Count},
  {"pg_stat_get_db_conflict_startup_deadlock", true, StatKind::Count},
  {"pg_stat_get_db_conflict_tablespace", true, StatKind::Count},
  {"pg_stat_get_db_deadlocks", true, StatKind::Count},
  {"pg_stat_get_db_idle_in_transaction_time", true, StatKind::Time},
  {"pg_stat_get_db_parallel_workers_launched", true, StatKind::Count},
  {"pg_stat_get_db_parallel_workers_to_launch", true, StatKind::Count},
  {"pg_stat_get_db_session_time", true, StatKind::Time},
  {"pg_stat_get_db_sessions", true, StatKind::Count},
  {"pg_stat_get_db_sessions_abandoned", true, StatKind::Count},
  {"pg_stat_get_db_sessions_fatal", true, StatKind::Count},
  {"pg_stat_get_db_sessions_killed", true, StatKind::Count},
  {"pg_stat_get_db_stat_reset_time", true, StatKind::Timestamp},
  {"pg_stat_get_db_temp_bytes", true, StatKind::Count},
  {"pg_stat_get_db_temp_files", true, StatKind::Count},
  {"pg_stat_get_db_tuples_deleted", true, StatKind::Count},
  {"pg_stat_get_db_tuples_fetched", true, StatKind::Count},
  {"pg_stat_get_db_tuples_inserted", true, StatKind::Count},
  {"pg_stat_get_db_tuples_returned", true, StatKind::Count},
  {"pg_stat_get_db_tuples_updated", true, StatKind::Count},
  {"pg_stat_get_db_xact_commit", true, StatKind::Count},
  {"pg_stat_get_db_xact_rollback", true, StatKind::Count},
  {"pg_stat_get_dead_tuples", true, StatKind::Count},
  {"pg_stat_get_function_calls", true, StatKind::UntrackedCount},
  {"pg_stat_get_function_self_time", true, StatKind::UntrackedTime},
  {"pg_stat_get_function_total_time", true, StatKind::UntrackedTime},
  {"pg_stat_get_ins_since_vacuum", true, StatKind::Count},
  {"pg_stat_get_last_analyze_time", true, StatKind::Timestamp},
  {"pg_stat_get_last_autoanalyze_time", true, StatKind::Timestamp},
  {"pg_stat_get_last_autovacuum_time", true, StatKind::Timestamp},
  {"pg_stat_get_last_vacuum_time", true, StatKind::Timestamp},
  {"pg_stat_get_lastscan", true, StatKind::Timestamp},
  {"pg_stat_get_mod_since_analyze", true, StatKind::Count},
  {"pg_stat_get_numscans", true, StatKind::Count},
  {"pg_stat_get_total_analyze_time", true, StatKind::Time},
  {"pg_stat_get_total_autoanalyze_time", true, StatKind::Time},
  {"pg_stat_get_total_autovacuum_time", true, StatKind::Time},
  {"pg_stat_get_total_vacuum_time", true, StatKind::Time},
  {"pg_stat_get_tuples_deleted", true, StatKind::Count},
  {"pg_stat_get_tuples_fetched", true, StatKind::Count},
  {"pg_stat_get_tuples_hot_updated", true, StatKind::Count},
  {"pg_stat_get_tuples_inserted", true, StatKind::Count},
  {"pg_stat_get_tuples_newpage_updated", true, StatKind::Count},
  {"pg_stat_get_tuples_returned", true, StatKind::Count},
  {"pg_stat_get_tuples_updated", true, StatKind::Count},
  {"pg_stat_get_vacuum_count", true, StatKind::Count},
  {"pg_stat_get_xact_function_calls", true, StatKind::UntrackedCount},
  {"pg_stat_get_xact_function_self_time", true, StatKind::UntrackedTime},
  {"pg_stat_get_xact_function_total_time", true, StatKind::UntrackedTime},
  {"pg_stat_get_xact_numscans", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_deleted", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_fetched", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_hot_updated", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_inserted", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_newpage_updated", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_returned", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_updated", true, StatKind::Count},
};

duckdb::LogicalType StatType(const StatFunction& stat) {
  switch (stat.kind) {
    case StatKind::Count:
    case StatKind::UntrackedCount:
      return duckdb::LogicalType::BIGINT;
    case StatKind::Time:
    case StatKind::UntrackedTime:
      return duckdb::LogicalType::DOUBLE;
    case StatKind::Timestamp:
      return duckdb::LogicalType::TIMESTAMP_TZ;
  }
  SDB_UNREACHABLE();
}

void RegisterStatFunctions(duckdb::ExtensionLoader& loader) {
  for (const auto& stat : kStatFunctions) {
    const bool zero =
      stat.kind == StatKind::Count || stat.kind == StatKind::Time;
    const bool per_row = zero && stat.takes_oid;
    duckdb::ScalarFunction function{
      stat.takes_oid ? duckdb::vector<duckdb::LogicalType>{OID()}
                     : duckdb::vector<duckdb::LogicalType>{},
      StatType(stat),
      per_row ? (stat.kind == StatKind::Count ? ZeroFunction<int64_t>
                                              : ZeroFunction<double>)
              : nullptr};
    if (!per_row) {
      function.SetBindExpressionCallback(zero ? BindZero : BindNull);
    }
    duckdb::ScalarFunctionSet set{Identifier{stat.name}};
    set.AddFunction(std::move(function));
    Register(loader, irs::StaticStrings::kPgCatalogSchema, std::move(set));
  }
}

int64_t LiveTuples(const Session& session, int64_t oid) {
  auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
    return 0;
  }
  if (const auto* search =
        dynamic_cast<const catalog::SearchTableEntry*>(&*entry)) {
    return static_cast<int64_t>(search->Storage()->GetStats().numLiveDocs);
  }
  if (auto* duck = dynamic_cast<duckdb::DuckTableEntry*>(&*entry)) {
    return static_cast<int64_t>(duck->GetStorage().GetTotalRows());
  }
  return 0;
}

void LiveTuplesFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto session = MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) { return LiveTuples(session, oid); });
}

void NumBackendsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  const auto sessions = ProgressRegistry::Instance().GetSnapshots();
  duckdb::UnaryExecutor::Execute<int64_t, int32_t>(
    args.data[0], result, args.size(), [&](int64_t oid) {
      return static_cast<int32_t>(absl::c_count_if(
        sessions, [&](const auto& session) { return session.datid == oid; }));
    });
}

// current_setting(name, missing_ok) -> text
void CurrentSetting2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto count = args.size();
  auto names = args.data[0].Values<duckdb::string_t>();
  auto oks = args.data[1].Values<bool>();
  auto* result_ptr =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t row = 0; row < count; row++) {
    auto name_value = names[row];
    auto ok_value = oks[row];
    if (!name_value.IsValid() || !ok_value.IsValid()) {
      result_validity.SetInvalid(row);
      continue;
    }
    const bool missing_ok = ok_value.GetValue();
    const auto key = name_value.GetValue().GetString();
    duckdb::Value value;
    if (context.TryGetCurrentSetting(duckdb::Identifier{key}, value)) {
      result_ptr[row] =
        duckdb::StringVector::AddString(result, value.ToString());
      continue;
    }
    if (missing_ok) {
      result_validity.SetInvalid(row);
      continue;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("unrecognized configuration parameter \"", key, "\""));
  }
}

// current_user / current_role -> the effective role (follows SET ROLE).
void CurrentUserFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& conn_ctx = GetSereneDBContext(context);
  auto value = duckdb::Value(conn_ctx.EffectiveUserName());
  result.Reference(value, duckdb::count_t(args.size()));
}

// session_user -> the session role (follows SET SESSION AUTHORIZATION, not
// SET ROLE).
void SessionUserFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& conn_ctx = GetSereneDBContext(context);
  auto value = duckdb::Value(conn_ctx.SessionUserName());
  result.Reference(value, duckdb::count_t(args.size()));
}

// pg_backend_pid() -> int4: this connection's backend PID, the same value sent
// in BackendKeyData (the high 32 bits of the random cancel key) so cancellation
// and pg_backend_pid() agree.
void PgBackendPidFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  const auto& conn_ctx = GetSereneDBContext(state.GetContext());
  result.Reference(duckdb::Value::INTEGER(conn_ctx.GetBackendPid()),
                   duckdb::count_t(args.size()));
}

// Cancel (or, with `terminate`, terminate) each requested backend by pid ->
// BOOLEAN per row (true if the pid matched a live backend). Shared by
// pg_cancel_backend and pg_terminate_backend. Authorised by being a session
// (single superuser), so it matches on the pid half of the cancel key alone,
// no secret.
void CancelBackendsByPid(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state, duckdb::Vector& result,
                         bool terminate) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto* registry = conn_ctx.GetCancelRegistry();
  auto pids = args.data[0].Values<int32_t>();
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  using CancelResult = network::CancelRegistry::CancelResult;
  for (duckdb::idx_t i = 0; i < args.size(); ++i) {
    auto pid = pids[i];
    if (!pid.IsValid()) {
      validity.SetInvalid(i);
      continue;
    }
    const auto target = static_cast<uint32_t>(pid.GetValue());
    const auto outcome = registry ? registry->CancelByPid(target, terminate)
                                  : CancelResult::NotFound;
    out[i] = outcome == CancelResult::Cancelled;
    if (outcome == CancelResult::Ambiguous) {
      // The pid is the high half of a random key, not a unique OS pid; two
      // backends collided on it, so cancelling either could hit the wrong one.
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_WARNING),
        ERR_MSG("PID ", target,
                " matches more than one backend; not cancelling any")));
    } else if (outcome == CancelResult::NotFound) {
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_WARNING),
        ERR_MSG("PID ", target, " is not a PostgreSQL backend process")));
    }
  }
}

// pg_cancel_backend(pid) -> bool: interrupt the target backend's current query.
void PgCancelBackendFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  CancelBackendsByPid(args, state, result, /*terminate=*/false);
}

// pg_terminate_backend(pid) -> bool: interrupt the target backend's current
// query and stop its session (the connection closes).
void PgTerminateBackendFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  CancelBackendsByPid(args, state, result, /*terminate=*/true);
}

// set_config(name, value, is_local) -> text
void SetConfigFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, duckdb::string_t value,
        bool is_local) -> duckdb::string_t {
      duckdb::Value val{value};
      const duckdb::Identifier setting{name.GetString()};
      duckdb::PhysicalSet::SetVariable(
        context, setting,
        is_local ? duckdb::SetScope::LOCAL : duckdb::SetScope::AUTOMATIC, val);

      // Return actual stored value (callbacks may have modified it).
      duckdb::Value current;
      const bool ok = context.TryGetCurrentSetting(setting, current);
      SDB_ASSERT(ok);
      return duckdb::StringVector::AddString(result, current.ToString());
    });
}

// PG-style version string. Overrides DuckDB's built-in version()
void VersionFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  auto value = duckdb::Value(
    absl::StrCat("PostgreSQL 18.3 (SereneDB ", SERENEDB_VERSION, ")"));
  result.Reference(value, duckdb::count_t(args.size()));
}

// search_path_canonical() -> text
// Returns the full catalog-qualified search path (catalog.schema,...).
// The PG-compliant SHOW search_path only lists schemas in the current database
// and keeps the literal "$user" placeholder; this function exposes the
// effective, resolved form (with "$user" expanded to the session user).
void SearchPathCanonicalFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto entries =
    duckdb::ClientData::Get(context).catalog_search_path->GetResolvedSetPaths();
  auto str = duckdb::CatalogSearchEntry::ListToString(entries);
  result.Reference(duckdb::Value{std::move(str)}, duckdb::count_t(args.size()));
}

template<bool kCountNulls>
void CountNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  const auto count = args.size();
  const auto ncols = args.ColumnCount();
  std::vector<duckdb::UnifiedVectorFormat> vdata(ncols);
  for (duckdb::idx_t col = 0; col < ncols; col++) {
    args.data[col].ToUnifiedFormat(count, vdata[col]);
  }
  auto out = duckdb::FlatVector::Writer<int32_t>(result, count);

  for (duckdb::idx_t row = 0; row < count; row++) {
    int32_t matched = 0;
    for (duckdb::idx_t col = 0; col < ncols; col++) {
      const auto idx = vdata[col].sel->get_index(row);
      if (vdata[col].validity.RowIsValid(idx) != kCountNulls) {
        matched++;
      }
    }
    out.WriteValue(matched);
  }
}

// num_nonnulls(...) -> int
// Ported from PG: counts non-null arguments.
void NumNonNullsFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  CountNullsFunction<false>(args, state, result);
}

// num_nulls(...) -> int
// Ported from PG: counts null arguments.
void NumNullsFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  CountNullsFunction<true>(args, state, result);
}

// --- pg_typeof ---
// Returns regtype OID. The serializer formats regtype as PG type name.
void PgTypeofFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  const auto oid = static_cast<int64_t>(pg::Type2Oid(args.data[0].GetType()));
  result.Reference(duckdb::Value::BIGINT(oid), duckdb::count_t(args.size()));
}

duckdb::unique_ptr<duckdb::Expression> BindPgTypeof(
  duckdb::FunctionBindExpressionInput& input) {
  const auto oid =
    static_cast<int64_t>(pg::Type2Oid(input.children[0]->GetReturnType()));
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BIGINT(oid).WithType(pg::REGTYPE()));
}

}  // namespace

void RegisterMiscFunctions(duckdb::ExtensionLoader& loader) {
  const auto oid = OID();
  const auto regclass = REGCLASS();
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  const auto int2 = duckdb::LogicalType::SMALLINT;
  const auto int4 = duckdb::LogicalType::INTEGER;
  const auto int8 = duckdb::LogicalType::BIGINT;
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;

  RegisterScalar(loader, pg_catalog, "pg_table_is_visible",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<RelationIsVisible>})});
  RegisterScalar(loader, pg_catalog, "pg_type_is_visible",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<TypeIsVisible>})});
  RegisterScalar(loader, pg_catalog, "pg_function_is_visible",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<FunctionIsVisible>})});
  RegisterScalar(loader, pg_catalog, "pg_get_userbyid",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, NAME(), PgGetUserByIdFunction})});
  RegisterScalar(loader, pg_catalog, "obj_description",
                 {WithinQuery(duckdb::ScalarFunction{
                    {oid, text}, text, ObjDescription2Function}),
                  WithinQuery(duckdb::ScalarFunction{
                    {oid}, text, ObjDescription1Function})});
  RegisterScalar(loader, pg_catalog, "col_description",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid, int4}, text, ColDescriptionFunction})});
  RegisterScalar(loader, pg_catalog, "pg_get_serial_sequence",
                 {WithinQuery(duckdb::ScalarFunction{
                   {text, text}, text, PgGetSerialSequenceFunction})});
  RegisterScalar(loader, pg_catalog, "pg_relation_is_updatable",
                 {WithinQuery(duckdb::ScalarFunction{
                   {regclass, boolean}, int4, PgRelationIsUpdatableFunction})});
  RegisterScalar(
    loader, pg_catalog, "pg_column_is_updatable",
    {WithinQuery(duckdb::ScalarFunction{
      {regclass, int2, boolean}, boolean, PgColumnIsUpdatableFunction})});
  {
    duckdb::ScalarFunction function{
      {regclass}, int8, PgSequenceLastValueFunction};
    function.SetVolatile();
    RegisterScalar(loader, pg_catalog, "pg_sequence_last_value", {function});
  }
  {
    duckdb::ScalarFunction function{
      {}, duckdb::LogicalType::TIMESTAMP_TZ, ClockTimestampFunction};
    function.SetVolatile();
    RegisterScalar(loader, pg_catalog, "clock_timestamp", {function});
  }
  RegisterScalar(
    loader, pg_catalog, "pg_encoding_to_char",
    {duckdb::ScalarFunction{{int4}, NAME(), PgEncodingToCharFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_char_to_encoding",
    {duckdb::ScalarFunction{{text}, int4, PgCharToEncodingFunction}});
  RegisterStatFunctions(loader);
  RegisterScalar(
    loader, pg_catalog, "pg_stat_get_live_tuples",
    {WithinQuery(duckdb::ScalarFunction{{oid}, int8, LiveTuplesFunction})});
  duckdb::ScalarFunction backends{{oid}, int4, NumBackendsFunction};
  backends.SetVolatile();
  RegisterScalar(loader, pg_catalog, "pg_stat_get_db_numbackends", {backends});

  // pg_typeof(any) -> regtype
  // current_setting(name, missing_ok) -> text
  // num_nonnulls(...) -> int
  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction func{
      "pg_typeof", {duckdb::LogicalType::ANY}, pg::REGTYPE(), PgTypeofFunction};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    func.SetBindExpressionCallback(BindPgTypeof);
    loader.RegisterFunction(func);
  }

  // current_setting(name, missing_ok) -> text
  {
    duckdb::ScalarFunction func{
      "current_setting",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::VARCHAR,
      CurrentSetting2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // set_config(name, value, is_local) -> text
  loader.RegisterFunction(duckdb::ScalarFunction{
    "set_config",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::BOOLEAN},
    duckdb::LogicalType::VARCHAR,
    SetConfigFunction});

  // search_path_canonical() -> text
  loader.RegisterFunction(duckdb::ScalarFunction{"search_path_canonical",
                                                 {},
                                                 duckdb::LogicalType::VARCHAR,
                                                 SearchPathCanonicalFunction});

  // version() -> text (overrides DuckDB's built-in)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "version", {}, duckdb::LogicalType::VARCHAR, VersionFunction});

  // pg_backend_pid() -> int4 (this connection's backend PID)
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_backend_pid", {}, duckdb::LogicalType::INTEGER, PgBackendPidFunction});

  // pg_cancel_backend(pid) / pg_terminate_backend(pid) -> bool: cancel another
  // backend's query by pid (terminate degrades to cancel + a warning).
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_cancel_backend",
                                                 {duckdb::LogicalType::INTEGER},
                                                 duckdb::LogicalType::BOOLEAN,
                                                 PgCancelBackendFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_terminate_backend",
                                                 {duckdb::LogicalType::INTEGER},
                                                 duckdb::LogicalType::BOOLEAN,
                                                 PgTerminateBackendFunction});

  // num_nonnulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nonnulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNonNullsFunction};
    func.GetSignature()
      .AddArgs("args", duckdb::LogicalType::ANY)
      .AddKwargs("kwargs", duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction func{"num_nulls",
                                {duckdb::LogicalType::ANY},
                                duckdb::LogicalType::INTEGER,
                                NumNullsFunction};
    func.GetSignature()
      .AddArgs("args", duckdb::LogicalType::ANY)
      .AddKwargs("kwargs", duckdb::LogicalType::ANY);
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  // width_bucket(operand, low, high, count) -> int
  loader.RegisterFunction(duckdb::ScalarFunction{
    "width_bucket",
    {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE,
     duckdb::LogicalType::DOUBLE, duckdb::LogicalType::INTEGER},
    duckdb::LogicalType::INTEGER,
    [](duckdb::DataChunk& args, duckdb::ExpressionState&,
       duckdb::Vector& result) {
      duckdb::GenericExecutor::ExecuteQuaternary<
        duckdb::PrimitiveType<double>, duckdb::PrimitiveType<double>,
        duckdb::PrimitiveType<double>, duckdb::PrimitiveType<int32_t>,
        duckdb::PrimitiveType<int32_t>>(
        args.data[0], args.data[1], args.data[2], args.data[3], result,
        args.size(),
        [](duckdb::PrimitiveType<double> operand,
           duckdb::PrimitiveType<double> low,
           duckdb::PrimitiveType<double> high,
           duckdb::PrimitiveType<int32_t> count)
          -> duckdb::PrimitiveType<int32_t> {
          if (count.val <= 0) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
              ERR_MSG("count must be greater than 0"));
          }
          if (low.val >= high.val) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
              ERR_MSG("lower bound must be less than upper bound"));
          }
          if (operand.val < low.val) {
            return {0};
          }
          if (operand.val >= high.val) {
            return {count.val + 1};
          }
          return {static_cast<int32_t>(
            (operand.val - low.val) / (high.val - low.val) * count.val + 1)};
        });
    }});

  // Stub functions that throw "not supported"
  auto not_supported = [](duckdb::DataChunk&, duckdb::ExpressionState&,
                          duckdb::Vector&) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Function is not supported in SereneDB"));
  };
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_current_xact_id", {}, pg::XID8(), not_supported});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_xact_status",
                                                 {pg::XID8()},
                                                 duckdb::LogicalType::VARCHAR,
                                                 not_supported});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "current_user", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

  // current_role is same as current_user in postgres
  loader.RegisterFunction(duckdb::ScalarFunction{
    "current_role", {}, duckdb::LogicalType::VARCHAR, CurrentUserFunction});

  loader.RegisterFunction(duckdb::ScalarFunction{
    "session_user", {}, duckdb::LogicalType::VARCHAR, SessionUserFunction});
}

}  // namespace sdb::pg
