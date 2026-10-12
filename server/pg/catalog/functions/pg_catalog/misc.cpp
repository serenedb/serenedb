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
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/tables/settings.h"
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
  const auto& session = SessionOf(state);
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
      const auto name = roles->NameOf(static_cast<duckdb::idx_t>(oid));
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
  const auto& session = SessionOf(state);
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
  const auto& session = SessionOf(state);
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
  const auto& session = SessionOf(state);
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
  duckdb::CatalogEntryRetriever retriever{context};
  auto database = SessionCatalog(context);
  std::optional<duckdb::CatalogTransaction> transaction;
  if (database) {
    transaction.emplace(database->GetCatalogTransaction(context));
  }
  duckdb::VariadicExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t table_name,
        duckdb::string_t column_name) -> duckdb::optional<duckdb::string_t> {
      const auto name = duckdb::QualifiedName::Parse(AsView(table_name));
      auto relation = retriever.GetEntry(
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
      const auto& table = relation->Cast<duckdb::TableCatalogEntry>();
      const auto& columns = table.GetColumns();
      if (!columns.ColumnExists(column_key)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("column \"", AsView(column_name), "\" of relation \"",
                  table.name.GetIdentifierName(), "\" does not exist"));
      }
      const auto& column = columns.GetColumn(column_key);
      if (!column.HasDefaultValue()) {
        return duckdb::nullopt;
      }
      const auto argument = NextvalArgument(column.DefaultValue());
      if (!argument || !database) {
        return duckdb::nullopt;
      }
      const auto sequence_name = duckdb::QualifiedName::Parse(*argument);
      const auto& schema_name = sequence_name.Schema().empty()
                                  ? table.ParentSchema(context).name
                                  : sequence_name.Schema();
      auto schema = database->GetSchema(*transaction, schema_name,
                                        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!schema) {
        return duckdb::nullopt;
      }
      auto sequence = schema->LookupEntry(
        *transaction, duckdb::EntryLookupInfo{CatalogType::SEQUENCE_ENTRY,
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
  const auto& session = SessionOf(state);
  duckdb::VariadicExecutor::Execute<int32_t, int64_t, bool>(
    args, result,
    [&](int64_t oid, bool) { return UpdatableEvents(session, oid); });
}

void PgColumnIsUpdatableFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  duckdb::VariadicExecutor::Execute<bool, int64_t, int16_t, bool>(
    args, result, [&](int64_t oid, int16_t attnum, bool) {
      return attnum > 0 && UpdatableEvents(session, oid) == kAllUpdateEvents;
    });
}

void PgSequenceLastValueFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  const auto& session = SessionOf(state);
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

struct StubFunction {
  std::string_view name;
  int32_t argument;
  int32_t result;
  bool zero;
  duckdb::FunctionStability stability;
};

constexpr StubFunction kStubFunctions[] = {
#include "pg/catalog/generated/scalar_stubs.gen.inc"
};

void RegisterStubFunctions(duckdb::ExtensionLoader& loader) {
  for (const auto& stub : kStubFunctions) {
    auto result = SystemColumnType(stub.result);
    const bool per_row = stub.zero && stub.argument != 0;
    const auto zero = result.id() == duckdb::LogicalTypeId::DOUBLE
                        ? ZeroFunction<double>
                        : ZeroFunction<int64_t>;
    duckdb::ScalarFunction function{
      stub.argument != 0
        ? duckdb::vector<duckdb::LogicalType>{SystemColumnType(stub.argument)}
        : duckdb::vector<duckdb::LogicalType>{},
      std::move(result), per_row ? zero : nullptr};
    if (!per_row) {
      function.SetBindExpressionCallback(stub.zero ? BindZero : BindNull);
    }
    RegisterScalar(loader, irs::StaticStrings::kPgCatalogSchema, stub.name,
                   stub.stability, {function});
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
  const auto& session = SessionOf(state);
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
  const auto names = args.data[0].Values<duckdb::string_t>();
  const auto oks = args.data[1].Values<bool>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); row++) {
    const auto name = names[row];
    const auto ok = oks[row];
    if (!name.IsValid() || !ok.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto key = AsView(name.GetValue());
    duckdb::Value value;
    if (context.TryGetCurrentSetting(duckdb::Identifier{key}, value)) {
      writer.WriteValue(
        duckdb::StringVector::AddString(result, value.ToString()));
      continue;
    }
    if (ok.GetValue()) {
      writer.WriteNull();
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

// Cancel (or, with `kTerminate`, terminate) each requested backend by pid ->
// BOOLEAN per row (true if the pid matched a live backend). Shared by
// pg_cancel_backend and pg_terminate_backend. Authorised by being a session
// (single superuser), so it matches on the pid half of the cancel key alone,
// no secret.
template<bool kTerminate>
void CancelBackendsByPid(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto* registry = conn_ctx.GetCancelRegistry();
  const auto pids = args.data[0].Values<int32_t>();
  auto out = duckdb::FlatVector::Writer<bool>(result, args.size());
  using CancelResult = network::CancelRegistry::CancelResult;
  for (duckdb::idx_t i = 0; i < args.size(); ++i) {
    const auto pid = pids[i];
    if (!pid.IsValid()) {
      out.WriteNull();
      continue;
    }
    const auto target = static_cast<uint32_t>(pid.GetValue());
    const auto outcome = registry ? registry->CancelByPid(target, kTerminate)
                                  : CancelResult::NotFound;
    out.WriteValue(outcome == CancelResult::Cancelled);
    if (outcome == CancelResult::Ambiguous) {
      // The pid is the high half of a random key, not a unique OS pid; two
      // backends collided on it, so cancelling either could hit the wrong one.
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_WARNING),
        ERR_MSG("PID ", pid.GetValue(),
                " matches more than one backend; not cancelling any")));
    } else if (outcome == CancelResult::NotFound) {
      conn_ctx.AddNotice(
        SQL_ERROR_DATA(ERR_CODE(ERRCODE_WARNING),
                       ERR_MSG("PID ", pid.GetValue(),
                               " is not a PostgreSQL backend process")));
    }
  }
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
      const duckdb::Identifier setting{AsView(name)};
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
  auto value = duckdb::Value(absl::StrCat(
    "PostgreSQL ", kServerVersion, " (SereneDB ", SERENEDB_VERSION, ")"));
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
  std::vector<duckdb::VectorValidityIterator> columns;
  columns.reserve(args.ColumnCount());
  for (const auto& vector : args.data) {
    columns.emplace_back(vector);
  }
  auto out = duckdb::FlatVector::Writer<int32_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); row++) {
    out.WriteValue(static_cast<int32_t>(
      absl::c_count_if(columns, [&](const duckdb::VectorValidityIterator& v) {
        return v.IsValid(row) != kCountNulls;
      })));
  }
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
  using enum duckdb::FunctionStability;
  const auto oid = OID();
  const auto regclass = REGCLASS();
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  const auto int2 = duckdb::LogicalType::SMALLINT;
  const auto int4 = duckdb::LogicalType::INTEGER;
  const auto int8 = duckdb::LogicalType::BIGINT;
  const auto float8 = duckdb::LogicalType::DOUBLE;
  const auto any = duckdb::LogicalType::ANY;
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  const std::string_view main = DEFAULT_SCHEMA;

  RegisterScalar(loader, pg_catalog, "pg_table_is_visible",
                 CONSISTENT_WITHIN_QUERY, InitSession,
                 {duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<RelationIsVisible>}});
  RegisterScalar(
    loader, pg_catalog, "pg_type_is_visible", CONSISTENT_WITHIN_QUERY,
    InitSession,
    {duckdb::ScalarFunction{{oid}, boolean, IsVisibleFunction<TypeIsVisible>}});
  RegisterScalar(loader, pg_catalog, "pg_function_is_visible",
                 CONSISTENT_WITHIN_QUERY, InitSession,
                 {duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<FunctionIsVisible>}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_userbyid", CONSISTENT_WITHIN_QUERY,
    {duckdb::ScalarFunction{{oid}, NAME(), PgGetUserByIdFunction}});
  RegisterScalar(
    loader, pg_catalog, "obj_description", CONSISTENT_WITHIN_QUERY, InitSession,
    {duckdb::ScalarFunction{{oid, text}, text, ObjDescription2Function},
     duckdb::ScalarFunction{{oid}, text, ObjDescription1Function}});
  RegisterScalar(
    loader, pg_catalog, "col_description", CONSISTENT_WITHIN_QUERY, InitSession,
    {duckdb::ScalarFunction{{oid, int4}, text, ColDescriptionFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_get_serial_sequence", CONSISTENT_WITHIN_QUERY,
    {duckdb::ScalarFunction{{text, text}, text, PgGetSerialSequenceFunction}});
  RegisterScalar(loader, pg_catalog, "pg_relation_is_updatable",
                 CONSISTENT_WITHIN_QUERY, InitSession,
                 {duckdb::ScalarFunction{
                   {regclass, boolean}, int4, PgRelationIsUpdatableFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_column_is_updatable", CONSISTENT_WITHIN_QUERY,
    InitSession,
    {duckdb::ScalarFunction{
      {regclass, int2, boolean}, boolean, PgColumnIsUpdatableFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_sequence_last_value", VOLATILE, InitSession,
    {duckdb::ScalarFunction{{regclass}, int8, PgSequenceLastValueFunction}});
  RegisterScalar(
    loader, pg_catalog, "clock_timestamp", VOLATILE,
    {duckdb::ScalarFunction{
      {}, duckdb::LogicalType::TIMESTAMP_TZ, ClockTimestampFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_encoding_to_char", CONSISTENT_WITHIN_QUERY,
    {duckdb::ScalarFunction{{int4}, NAME(), PgEncodingToCharFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_char_to_encoding", CONSISTENT_WITHIN_QUERY,
    {duckdb::ScalarFunction{{text}, int4, PgCharToEncodingFunction}});
  RegisterStubFunctions(loader);
  RegisterScalar(loader, pg_catalog, "pg_stat_get_live_tuples",
                 CONSISTENT_WITHIN_QUERY, InitSession,
                 {duckdb::ScalarFunction{{oid}, int8, LiveTuplesFunction}});
  RegisterScalar(loader, pg_catalog, "pg_stat_get_db_numbackends",
                 CONSISTENT_WITHIN_QUERY,
                 {duckdb::ScalarFunction{{oid}, int4, NumBackendsFunction}});

  {
    duckdb::ScalarFunction function{{any}, pg::REGTYPE(), nullptr};
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    function.SetBindExpressionCallback(BindPgTypeof);
    RegisterScalar(loader, main, "pg_typeof", CONSISTENT_WITHIN_QUERY,
                   {function});
  }

  // current_setting(name, missing_ok) -> text
  {
    duckdb::ScalarFunction function{
      {text, boolean}, text, CurrentSetting2Function};
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    RegisterScalar(loader, main, "current_setting", CONSISTENT_WITHIN_QUERY,
                   {function});
  }

  // set_config(name, value, is_local) -> text
  RegisterScalar(
    loader, main, "set_config", VOLATILE,
    {duckdb::ScalarFunction{{text, text, boolean}, text, SetConfigFunction}});

  // search_path_canonical() -> text
  RegisterScalar(
    loader, main, "search_path_canonical", CONSISTENT_WITHIN_QUERY,
    {duckdb::ScalarFunction{{}, text, SearchPathCanonicalFunction}});

  // version() -> text (overrides DuckDB's built-in)
  RegisterScalar(loader, main, "version", CONSISTENT_WITHIN_QUERY,
                 {duckdb::ScalarFunction{{}, text, VersionFunction}});

  // pg_backend_pid() -> int4 (this connection's backend PID)
  RegisterScalar(loader, main, "pg_backend_pid", CONSISTENT_WITHIN_QUERY,
                 {duckdb::ScalarFunction{{}, int4, PgBackendPidFunction}});

  // pg_cancel_backend(pid) / pg_terminate_backend(pid) -> bool: cancel another
  // backend's query by pid (terminate degrades to cancel + a warning).
  RegisterScalar(
    loader, main, "pg_cancel_backend", VOLATILE,
    {duckdb::ScalarFunction{{int4}, boolean, CancelBackendsByPid<false>}});
  RegisterScalar(
    loader, main, "pg_terminate_backend", VOLATILE,
    {duckdb::ScalarFunction{{int4}, boolean, CancelBackendsByPid<true>}});

  // num_nonnulls(...) -> int
  {
    duckdb::ScalarFunction function{{any}, int4, CountNullsFunction<false>};
    function.GetSignature().AddArgs("args", any).AddKwargs("kwargs", any);
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    RegisterScalar(loader, main, "num_nonnulls", CONSISTENT, {function});
  }

  // num_nulls(...) -> int
  {
    duckdb::ScalarFunction function{{any}, int4, CountNullsFunction<true>};
    function.GetSignature().AddArgs("args", any).AddKwargs("kwargs", any);
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    RegisterScalar(loader, main, "num_nulls", CONSISTENT, {function});
  }

  // width_bucket(operand, low, high, count) -> int
  RegisterScalar(
    loader, main, "width_bucket", CONSISTENT,
    {duckdb::ScalarFunction{
      {float8, float8, float8, int4},
      int4,
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
      }}});

  // Stub functions that throw "not supported"
  auto not_supported = [](duckdb::DataChunk&, duckdb::ExpressionState&,
                          duckdb::Vector&) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Function is not supported in SereneDB"));
  };
  RegisterScalar(loader, main, "pg_current_xact_id", CONSISTENT_WITHIN_QUERY,
                 {duckdb::ScalarFunction{{}, pg::XID8(), not_supported}});
  RegisterScalar(loader, main, "pg_xact_status", VOLATILE,
                 {duckdb::ScalarFunction{{pg::XID8()}, text, not_supported}});

  RegisterScalar(loader, main, "current_user", CONSISTENT_WITHIN_QUERY,
                 {duckdb::ScalarFunction{{}, text, CurrentUserFunction}});

  // current_role is same as current_user in postgres
  RegisterScalar(loader, main, "current_role", CONSISTENT_WITHIN_QUERY,
                 {duckdb::ScalarFunction{{}, text, CurrentUserFunction}});

  RegisterScalar(loader, main, "session_user", CONSISTENT_WITHIN_QUERY,
                 {duckdb::ScalarFunction{{}, text, SessionUserFunction}});
}

}  // namespace sdb::pg
