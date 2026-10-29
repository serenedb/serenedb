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
#include <absl/strings/str_cat.h>
#include <libpq-fe.h>

#include <cmath>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/operator/comparison_operators.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/bit.hpp>
#include <duckdb/common/types/cast_helpers.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/inet/inet_ipaddress.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/planner_extension.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <text_data.hpp>
#include <utf8proc.hpp>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "network/cancel_registry.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "pg/sql_utils.h"
#include "pg/types/cast.h"
#include "pg/types/logical.h"
#include "pg/types/mapping.h"
#include "search/search_table.h"
#include "server/utils/build.h"

extern "C" int pg_encoding_max_length(int encoding);

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
      if (!entry || column < 0 ||
          (column == 0 && CatalogClassOid(entry->type) != kPgClassTable)) {
        return duckdb::nullopt;
      }
      if (column == 0) {
        return Comment(result, entry->comment);
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
        return Comment(
          result,
          entry->Cast<duckdb::ViewCatalogEntry>().GetColumnComment(index));
      }
      return duckdb::nullopt;
    });
}

void PgGetSerialSequenceFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& session = SessionOf(state);
  duckdb::VariadicExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t table_name,
        duckdb::string_t column_name) -> duckdb::optional<duckdb::string_t> {
      const auto oid =
        *RegIn(session, RegKind::Class, AsView(table_name), nullptr);
      auto relation = EntryByOid(session, oid);
      if (!relation) {
        relation = duckdb::Catalog::GetCatalog(
                     context, duckdb::Identifier::TempCatalog())
                     .Cast<duckdb::DuckCatalog>()
                     .FindEntryById(&context, CatalogType::TABLE_ENTRY, oid);
      }
      if (!relation) {
        return duckdb::nullopt;
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
      auto& catalog = relation->ParentCatalog();
      auto& dependency_manager = *catalog.GetDependencyManager();
      const auto transaction = catalog.GetCatalogTransaction(context);
      const duckdb::IdentifierEquality equals{catalog.IsCaseSensitive()};
      for (const auto& owned :
           dependency_manager.OwnedEntries(transaction, *relation)) {
        const auto& sequence = owned.get();
        SDB_ASSERT(sequence.type == CatalogType::SEQUENCE_ENTRY);
        const auto& schema = sequence.ParentSchema(transaction);
        const auto& owner =
          sequence.Cast<duckdb::SequenceCatalogEntry>().owner_column;
        if (owner.IsValid()
              ? owner.GetIndex() != column.CatalogOid()
              : !duckdb::Binder::DefaultNamesSequence(
                  column, schema.GetQualifiedName(sequence.name), equals)) {
          continue;
        }
        return duckdb::StringVector::AddString(
          result, QualifiedOutName(schema.name.GetIdentifierName(),
                                   sequence.name.GetIdentifierName()));
      }
      return duckdb::nullopt;
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

void StatementTimestampFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  const auto client =
    state.GetContext().registered_state->Get<connector::SereneDBClientState>(
      connector::kSereneDBClientStateKey);
  SDB_ASSERT(client);
  const auto start =
    client->progress_source->query_start_us.load(std::memory_order_relaxed);
  result.Reference(duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t{start}),
                   duckdb::count_t(args.size()));
}

void PgEncodingToCharFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int32_t, duckdb::string_t>(
    args.data[0], result, args.size(), [](int32_t encoding) {
      return duckdb::string_t{pg_encoding_to_char(encoding)};
    });
}

void PgCharToEncodingFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int32_t>(
    args.data[0], result, args.size(), [](duckdb::string_t name) {
      return pg_char_to_encoding(std::string{AsView(name)}.c_str());
    });
}

void PgEncodingMaxLengthFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState&,
                                 duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int32_t, int32_t>(
    args.data[0], result, args.size(),
    [](int32_t encoding) -> std::optional<int32_t> {
      if (*pg_encoding_to_char(encoding) == '\0') {
        return std::nullopt;
      }
      return pg_encoding_max_length(encoding);
    });
}

int64_t LiveTuples(const Session& session, int64_t oid) {
  auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
    return 0;
  }
  auto& table = entry->Cast<duckdb::TableCatalogEntry>();
  if (table.GetLogStorage()) {
    return static_cast<int64_t>(table.Cast<catalog::SearchTableEntry>()
                                  .Storage()
                                  ->GetStats()
                                  .numLiveDocs);
  }
  SDB_ASSERT(table.IsDuckTable());
  return static_cast<int64_t>(table.GetStorage().GetTotalRows());
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

template<int64_t DatabaseStats::* kCounter>
void DatabaseCounterFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                             duckdb::Vector& result) {
  const auto& registry = ProgressRegistry::Instance();
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) { return registry.GetDatabaseStats(oid).*kCounter; });
}

template<int64_t DatabaseStats::* kMicros>
void DatabaseMillisFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  const auto& registry = ProgressRegistry::Instance();
  duckdb::UnaryExecutor::Execute<int64_t, double>(
    args.data[0], result, args.size(), [&](int64_t oid) {
      return static_cast<double>(registry.GetDatabaseStats(oid).*kMicros) /
             1000.0;
    });
}

void CurrentSetting2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto names = args.data[0].Values<duckdb::string_t>();
  const auto oks = args.data[1].Values<bool>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  std::optional<duckdb::idx_t> last_found;
  std::optional<duckdb::string_t> last_value;
  const auto write = [&](const std::optional<duckdb::string_t>& value) {
    if (value) {
      writer.WriteValue(*value);
    } else {
      writer.WriteNull();
    }
  };
  for (duckdb::idx_t row = 0; row < args.size(); row++) {
    const auto name = names[row];
    const auto ok = oks[row];
    if (!name.IsValid() || !ok.IsValid()) {
      writer.WriteNull();
      continue;
    }
    if (last_found && names[*last_found].GetValue() == name.GetValue()) {
      write(last_value);
      continue;
    }
    const auto key = AsView(name.GetValue());
    duckdb::Value value;
    if (context.TryGetCurrentSetting(duckdb::Identifier{key}, value)) {
      value = duckdb::Settings::FormatDisplayValue(context, value);
      last_found = row;
      last_value = value.IsNull()
                     ? std::nullopt
                     : std::optional{duckdb::StringVector::AddString(
                         result, value.ToString())};
      write(last_value);
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

void CurrentUserFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& conn_ctx = GetSereneDBContext(context);
  auto value = duckdb::Value(conn_ctx.EffectiveUserName());
  result.Reference(value, duckdb::count_t(args.size()));
}

void SessionUserFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto& conn_ctx = GetSereneDBContext(context);
  auto value = duckdb::Value(conn_ctx.SessionUserName());
  result.Reference(value, duckdb::count_t(args.size()));
}

void SystemUserFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto& system_user = GetSereneDBContext(state.GetContext()).SystemUser();
  result.Reference(system_user.empty()
                     ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                     : duckdb::Value{system_user},
                   duckdb::count_t(args.size()));
}

void PgBackendPidFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  const auto& conn_ctx = GetSereneDBContext(state.GetContext());
  result.Reference(duckdb::Value::INTEGER(conn_ctx.GetBackendPid()),
                   duckdb::count_t(args.size()));
}

template<bool kTerminate>
[[noreturn]] void ThrowSignalDenied(std::string_view detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG(kTerminate ? "permission denied to terminate process"
                                     : "permission denied to cancel query"),
                  ERR_DETAIL(detail));
}

template<bool kTerminate>
void SignalBackendFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);
  auto* registry = conn_ctx.GetCancelRegistry();
  const auto roles = auth::RolesOf(&context);
  const auto caller = auth::ClosureFor(&context, conn_ctx.GetRoleId());
  const auto pids = args.data[0].Values<int32_t>();
  auto out = duckdb::FlatVector::Writer<bool>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto pid = pids[row];
    if (!pid.IsValid()) {
      out.WriteNull();
      continue;
    }
    const auto match =
      registry ? registry->FindByPid(static_cast<uint32_t>(pid.GetValue()))
               : network::CancelRegistry::PidMatch{nullptr, false};
    if (!match.token) {
      conn_ctx.AddNotice(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_WARNING),
        ERR_MSG("PID ", pid.GetValue(),
                match.ambiguous
                  ? " matches more than one backend; not cancelling any"
                  : " is not a PostgreSQL backend process")));
      out.WriteValue(false);
      continue;
    }
    const auto role = match.token->role.load(std::memory_order_relaxed);
    const auto* node = roles->Find(role);
    if (role == kInvalidOid || (node && node->is_superuser)) {
      if (!caller->is_superuser) {
        ThrowSignalDenied<kTerminate>(
          kTerminate ? "Only roles with the SUPERUSER attribute may terminate "
                       "processes of roles with the SUPERUSER attribute."
                     : "Only roles with the SUPERUSER attribute may cancel "
                       "queries of roles with the SUPERUSER attribute.");
      }
    } else if (!caller->MemberOf(role)) {
      ThrowSignalDenied<kTerminate>(
        kTerminate
          ? "Only roles with privileges of the role whose process is being "
            "terminated or with privileges of the \"pg_signal_backend\" role "
            "may terminate this process."
          : "Only roles with privileges of the role whose query is being "
            "canceled or with privileges of the \"pg_signal_backend\" role may "
            "cancel this query.");
    }
    if constexpr (kTerminate) {
      match.token->Terminate();
    } else {
      match.token->Cancel();
    }
    out.WriteValue(true);
  }
}

void SetConfigFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool,
                                   duckdb::string_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t name, duckdb::string_t value,
        bool is_local) -> duckdb::string_t {
      const duckdb::Identifier setting{AsView(name)};
      duckdb::PhysicalSet::SetVariable(
        context, setting,
        is_local ? duckdb::SetScope::LOCAL : duckdb::SetScope::AUTOMATIC,
        duckdb::Value{value});
      duckdb::Value current;
      const bool ok = context.TryGetCurrentSetting(setting, current);
      SDB_ASSERT(ok);
      return duckdb::StringVector::AddString(
        result,
        duckdb::Settings::FormatDisplayValue(context, current).ToString());
    });
}

void VersionFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  auto value = duckdb::Value(absl::StrCat(
    "PostgreSQL ", kPgServerVersion, " (SereneDB ", SERENEDB_VERSION, ")"));
  result.Reference(value, duckdb::count_t(args.size()));
}

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
    static_cast<int64_t>(CatalogTypeOid(input.children[0]->GetReturnType()));
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BIGINT(oid).WithType(REGTYPE()));
}

int32_t AscendingWidthBucket(double operand, double low, double high,
                             int32_t count) {
  if (operand < low) {
    return 0;
  }
  if (operand >= high) {
    int32_t bucket = 0;
    if (__builtin_add_overflow(count, 1, &bucket)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                      ERR_MSG("integer out of range"));
    }
    return bucket;
  }
  const auto quotient = std::isinf(high - low)
                          ? (operand / 2 - low / 2) / (high / 2 - low / 2)
                          : (operand - low) / (high - low);
  return std::min(static_cast<int32_t>(count * quotient), count - 1) + 1;
}

void WidthBucketFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  duckdb::GenericExecutor::ExecuteQuaternary<
    duckdb::PrimitiveType<double>, duckdb::PrimitiveType<double>,
    duckdb::PrimitiveType<double>, duckdb::PrimitiveType<int32_t>,
    duckdb::PrimitiveType<int32_t>>(
    args.data[0], args.data[1], args.data[2], args.data[3], result, args.size(),
    [](duckdb::PrimitiveType<double> operand, duckdb::PrimitiveType<double> low,
       duckdb::PrimitiveType<double> high,
       duckdb::PrimitiveType<int32_t> count) -> duckdb::PrimitiveType<int32_t> {
      if (count.val <= 0) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
          ERR_MSG("count must be greater than zero"));
      }
      if (std::isnan(operand.val) || std::isnan(low.val) ||
          std::isnan(high.val)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
          ERR_MSG("operand, lower bound, and upper bound cannot be NaN"));
      }
      if (std::isinf(low.val) || std::isinf(high.val)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
          ERR_MSG("lower and upper bounds must be finite"));
      }
      if (low.val == high.val) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
          ERR_MSG("lower bound cannot equal upper bound"));
      }
      return {
        low.val < high.val
          ? AscendingWidthBucket(operand.val, low.val, high.val, count.val)
          : AscendingWidthBucket(-operand.val, -low.val, -high.val, count.val)};
    });
}

duckdb::unique_ptr<duckdb::FunctionData> WidthBucketThresholdsBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto& operand = input.GetArguments()[0]->GetReturnType();
  const auto& element =
    duckdb::ListType::GetChildType(input.GetArguments()[1]->GetReturnType());
  if (element.id() == duckdb::LogicalTypeId::LIST) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                    ERR_MSG("thresholds must be one-dimensional array"));
  }
  duckdb::LogicalType common;
  if (!duckdb::LogicalType::TryGetMaxLogicalType(input.GetClientContext(),
                                                 operand, element, common)) {
    const auto session = MakeSession(&input.GetClientContext());
    const auto operand_name =
      FormatTypeOut(session, CatalogTypeOid(operand), std::nullopt);
    const auto thresholds_name = FormatTypeOut(
      session, CatalogTypeOid(input.GetArguments()[1]->GetReturnType()),
      std::nullopt);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
      ERR_MSG("function width_bucket(", pg::AsView(operand_name), ", ",
              pg::AsView(thresholds_name), ") does not exist"),
      ERR_HINT("No function matches the given name and argument types. You "
               "might need to add explicit type casts."));
  }
  auto& function = input.GetBoundFunction();
  function.GetArguments()[0] = common;
  function.GetArguments()[1] = duckdb::LogicalType::LIST(common);
  return nullptr;
}

void WidthBucketThresholdsFunction(duckdb::DataChunk& args,
                                   duckdb::ExpressionState&,
                                   duckdb::Vector& result) {
  const auto count = args.size();
  const auto& lists = args.data[1];
  const auto threshold_count = duckdb::ListVector::GetListSize(lists);
  const duckdb::OrderModifiers modifiers{duckdb::OrderType::ASCENDING,
                                         duckdb::OrderByNullType::NULLS_LAST};
  duckdb::Vector operand_keys{duckdb::LogicalType::BLOB, count};
  duckdb::Vector threshold_keys{duckdb::LogicalType::BLOB, threshold_count};
  duckdb::CreateSortKeyHelpers::CreateSortKeyWithValidity(
    args.data[0], operand_keys, modifiers, count);
  duckdb::CreateSortKeyHelpers::CreateSortKeyWithValidity(
    duckdb::ListVector::GetEntry(lists), threshold_keys, modifiers,
    threshold_count);
  duckdb::UnifiedVectorFormat list_format;
  lists.ToUnifiedFormat(count, list_format);
  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(list_format);
  const auto operands = operand_keys.Values<duckdb::string_t>();
  const auto thresholds = threshold_keys.Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<int32_t>(result, count);
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto list = list_format.sel->get_index(row);
    const auto operand = operands[row];
    if (!list_format.validity.RowIsValid(list) || !operand.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto& entry = entries[list];
    for (auto i = entry.offset; i < entry.offset + entry.length; ++i) {
      if (!thresholds[i].IsValid()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        ERR_MSG("thresholds array must not contain NULLs"));
      }
    }
    const auto* first =
      duckdb::FlatVector::GetData<duckdb::string_t>(threshold_keys) +
      entry.offset;
    const auto* bucket = std::upper_bound(
      first, first + entry.length, operand.GetValue(),
      [](const duckdb::string_t& left, const duckdb::string_t& right) {
        return duckdb::LessThan::Operation(left, right);
      });
    writer.WriteValue(static_cast<int32_t>(bucket - first));
  }
}

void NotSupportedFunction(duckdb::DataChunk&, duckdb::ExpressionState&,
                          duckdb::Vector&) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Function is not supported in SereneDB"));
}

void PgBasetypeFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<int64_t> {
      if (oid < 0 || !TypeExists(session, static_cast<uint64_t>(oid))) {
        return duckdb::nullopt;
      }
      for (const auto* builtin = FindBuiltinType(oid);
           builtin && builtin->type == 'd';
           builtin = FindBuiltinType(builtin->basetype)) {
        oid = builtin->basetype;
      }
      return oid;
    });
}

duckdb::unique_ptr<duckdb::Expression> BindPgCollationFor(
  duckdb::FunctionBindExpressionInput& input) {
  const auto& type = input.children[0]->GetReturnType();
  duckdb::Value collation{duckdb::LogicalType::VARCHAR};
  if (type.id() != duckdb::LogicalTypeId::SQLNULL) {
    const auto session = MakeSession(&input.context);
    const auto oid = DescribeColumnType(type).collation;
    if (oid == kInvalidOid) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                      ERR_MSG("collations are not supported by type ",
                              pg::AsView(FormatTypeOut(
                                session, CatalogTypeOid(type), std::nullopt))));
    }
    collation =
      duckdb::Value{std::string{RegOut(session, RegKind::Collation, oid)}};
  }
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    std::move(collation));
}

void ReplicaIdentityIndexFunction(duckdb::DataChunk& args,
                                  duckdb::ExpressionState& state,
                                  duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<int64_t> {
      const auto relation = FindRelation(session, static_cast<uint64_t>(oid));
      if (!relation) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                        ERR_MSG("could not open relation with OID ", oid));
      }
      const auto type =
        relation->entry ? relation->entry->type : CatalogType::INVALID;
      if (relation->key || type == CatalogType::INDEX_ENTRY ||
          type == CatalogType::TYPE_ENTRY) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
          ERR_MSG("cannot open relation \"",
                  NameOf(*session.transaction, *relation).name, "\""),
          ERR_DETAIL(type == CatalogType::TYPE_ENTRY
                       ? "This operation is not supported for composite types."
                       : "This operation is not supported for indexes."));
      }
      if (type != CatalogType::TABLE_ENTRY) {
        return duckdb::nullopt;
      }
      for (const auto& key :
           KeyIndexes(relation->entry->Cast<duckdb::TableCatalogEntry>())) {
        if (key.IsPrimaryKey()) {
          return static_cast<int64_t>(key.index_oid);
        }
      }
      return duckdb::nullopt;
    });
}

void SettingsGetFlagsFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto& context = state.GetContext();
  const auto names = args.data[0].Values<duckdb::string_t>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(
      result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto name = names[row];
    duckdb::Value value;
    if (!name.IsValid() || !context.TryGetCurrentSetting(
                             Identifier{AsView(name.GetValue())}, value)) {
      writer.WriteNull();
      continue;
    }
    writer.WriteList(0);
  }
}

duckdb::unique_ptr<duckdb::FunctionData> TablespaceDatabasesBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<Identifier>& names) {
  names.emplace_back("pg_tablespace_databases");
  types.emplace_back(OID());
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

void LoadTablespaceDatabases(duckdb::ClientContext& context,
                             duckdb::DataChunk& input,
                             std::vector<int64_t>& rows) {
  const auto oids = input.data[0].Values<int64_t>();
  for (duckdb::idx_t row = 0; row < input.size(); ++row) {
    const auto oid = oids[row];
    if (!oid.IsValid()) {
      continue;
    }
    if (oid.GetValue() == static_cast<int64_t>(kPgDefaultTablespace)) {
      for (const auto& database : Databases(context)) {
        rows.emplace_back(static_cast<int64_t>(database.get().oid));
      }
      continue;
    }
    GetSereneDBContext(context).AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_WARNING),
      ERR_MSG(oid.GetValue() == static_cast<int64_t>(kPgGlobalTablespace)
                ? std::string{"global tablespace never has databases"}
                : absl::StrCat(oid.GetValue(), " is not a tablespace OID"))));
  }
  absl::c_sort(rows);
}

void WriteTablespaceDatabases(std::span<const int64_t> rows,
                              duckdb::DataChunk& output) {
  auto writer =
    duckdb::FlatVector::Writer<int64_t>(output.data[0], rows.size());
  for (const auto oid : rows) {
    writer.WriteValue(oid);
  }
}

void ReferenceVersion(duckdb::Vector& result, duckdb::idx_t count,
                      std::string_view version) {
  result.Reference(duckdb::Value{std::string{version.substr(
                     0, version.find('.', version.find('.') + 1))}},
                   duckdb::count_t(count));
}

void UnicodeVersionFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  ReferenceVersion(result, args.size(), duckdb::utf8proc_unicode_version());
}

void IcuUnicodeVersionFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState&,
                               duckdb::Vector& result) {
  ReferenceVersion(result, args.size(), duckdb::text::text_unicode_version);
}

bool UnicodeAssigned(std::string_view text) {
  const auto* bytes = reinterpret_cast<const utf8proc_uint8_t*>(text.data());
  const auto size = static_cast<utf8proc_ssize_t>(text.size());
  for (utf8proc_ssize_t pos = 0; pos < size;) {
    utf8proc_int32_t codepoint = 0;
    const auto length =
      duckdb::utf8proc_iterate(bytes + pos, size - pos, &codepoint);
    if (length <= 0 ||
        duckdb::utf8proc_category(codepoint) == duckdb::UTF8PROC_CATEGORY_CN) {
      return false;
    }
    pos += length;
  }
  return true;
}

void UnicodeAssignedFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                             duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
    args.data[0], result, args.size(),
    [](duckdb::string_t text) { return UnicodeAssigned(AsView(text)); });
}

constexpr int64_t kVarHdrSz = 4;
constexpr int64_t kShortVarlenaMax = 127;
constexpr int64_t kMaxAlign = 8;
constexpr int64_t kArrayHeaderSize = 16;
constexpr int64_t kArrayDimensionSize = 8;
constexpr int64_t kTupleHeaderSize = 23;
constexpr int64_t kInetHeaderSize = 2;

int64_t AlignUp(int64_t offset, int64_t alignment) {
  return (offset + alignment - 1) / alignment * alignment;
}

int64_t Alignment(char align) {
  if (align == 'd') {
    return 8;
  }
  if (align == 'i') {
    return 4;
  }
  return align == 's' ? 2 : 1;
}

int64_t BitmapSize(int64_t items) { return (items + 7) / 8; }

int64_t NumericDatumSize(std::string_view text) {
  if (text.starts_with('-')) {
    text.remove_prefix(1);
  }
  const auto point = std::min(text.find('.'), text.size());
  auto integer = text.substr(0, point);
  auto fraction = text.substr(std::min(point + 1, text.size()));
  integer.remove_prefix(
    std::min(integer.find_first_not_of('0'), integer.size()));
  fraction = fraction.substr(0, fraction.find_last_not_of('0') + 1);
  const auto groups = [](std::string_view digits) {
    return static_cast<int64_t>((digits.size() + 3) / 4);
  };
  int64_t digits = 0;
  if (!integer.empty() && !fraction.empty()) {
    digits = groups(integer) + groups(fraction);
  } else if (!integer.empty()) {
    const auto zeros = integer.size() - 1 - integer.find_last_not_of('0');
    digits = groups(integer) - static_cast<int64_t>(zeros / 4);
  } else if (!fraction.empty()) {
    digits = groups(fraction) -
             static_cast<int64_t>(fraction.find_first_not_of('0') / 4);
  }
  return kVarHdrSz + 2 + 2 * digits;
}

int64_t DatumSize(const duckdb::Value& value);

int64_t ArrayDatumSize(const duckdb::Value& value) {
  int64_t dimensions = 0;
  const auto* element = &value.type();
  while (element->id() == duckdb::LogicalTypeId::LIST ||
         element->id() == duckdb::LogicalTypeId::ARRAY) {
    ++dimensions;
    element = element->id() == duckdb::LogicalTypeId::LIST
                ? &duckdb::ListType::GetChildType(*element)
                : &duckdb::ArrayType::GetChildType(*element);
  }
  const auto alignment = Alignment(DescribeColumnType(*element).align);
  int64_t items = 0;
  int64_t data = 0;
  bool nulls = false;
  const auto visit = [&](this const auto& self, const duckdb::Value& list,
                         int64_t depth) -> void {
    const auto& children = list.type().id() == duckdb::LogicalTypeId::LIST
                             ? duckdb::ListValue::GetChildren(list)
                             : duckdb::ArrayValue::GetChildren(list);
    for (const auto& child : children) {
      if (depth + 1 < dimensions) {
        if (!child.IsNull()) {
          self(child, depth + 1);
        }
        continue;
      }
      ++items;
      if (child.IsNull()) {
        nulls = true;
        continue;
      }
      data = AlignUp(data + DatumSize(child), alignment);
    }
  };
  visit(value, 0);
  if (items == 0) {
    return kArrayHeaderSize;
  }
  return AlignUp(kArrayHeaderSize + kArrayDimensionSize * dimensions +
                   (nulls ? BitmapSize(items) : 0),
                 kMaxAlign) +
         data;
}

int64_t TupleDatumSize(const duckdb::Value& value) {
  const auto& fields = duckdb::StructValue::GetChildren(value);
  const bool nulls = absl::c_any_of(
    fields, [](const duckdb::Value& field) { return field.IsNull(); });
  auto size =
    AlignUp(kTupleHeaderSize +
              (nulls ? BitmapSize(static_cast<int64_t>(fields.size())) : 0),
            kMaxAlign);
  for (const auto& field : fields) {
    if (field.IsNull()) {
      continue;
    }
    const auto column = DescribeColumnType(field.type());
    const auto datum = DatumSize(field);
    if (column.len == -1 && column.storage != 'p' &&
        datum - kVarHdrSz + 1 <= kShortVarlenaMax) {
      size += datum - kVarHdrSz + 1;
    } else {
      size = AlignUp(size, Alignment(column.align)) + datum;
    }
  }
  return size;
}

bool IsText(const duckdb::LogicalType& type) {
  return type.id() == duckdb::LogicalTypeId::VARCHAR ||
         type.id() == duckdb::LogicalTypeId::BLOB;
}

int64_t DatumSize(const duckdb::Value& value) {
  const auto& type = value.type();
  if (const auto column = DescribeColumnType(type); column.len > 0) {
    return column.len;
  }
  if (IsText(type)) {
    return kVarHdrSz +
           static_cast<int64_t>(duckdb::StringValue::Get(value).size());
  }
  if (type.id() == duckdb::LogicalTypeId::BIT) {
    const auto bits =
      duckdb::Bit::BitLength(duckdb::string_t{duckdb::StringValue::Get(value)});
    return kVarHdrSz + 4 + static_cast<int64_t>((bits + 7) / 8);
  }
  if (type.id() == duckdb::LogicalTypeId::LIST ||
      type.id() == duckdb::LogicalTypeId::ARRAY) {
    return ArrayDatumSize(value);
  }
  if (IsInetOrCidr(type)) {
    const auto& fields = duckdb::StructValue::GetChildren(value);
    return kVarHdrSz + kInetHeaderSize +
           (fields[0].GetValue<uint8_t>() == INET_IP_ADDRESS_V4 ? 4 : 16);
  }
  if (type.id() == duckdb::LogicalTypeId::STRUCT ||
      type.id() == duckdb::LogicalTypeId::TUPLE) {
    return TupleDatumSize(value);
  }
  if (Type2Oid(type) == kNumeric) {
    return NumericDatumSize(value.ToString());
  }
  return kVarHdrSz + static_cast<int64_t>(value.ToString().size());
}

template<typename T>
int64_t DecimalDatumSize(const duckdb::UnifiedVectorFormat& format,
                         duckdb::idx_t index, uint8_t width, uint8_t scale) {
  const auto value = duckdb::UnifiedVectorFormat::GetData<T>(format)[index];
  char buffer[64];
  const auto length =
    duckdb::DecimalToString::DecimalLength<T>(value, width, scale);
  duckdb::DecimalToString::FormatDecimal<T>(value, width, scale, buffer,
                                            static_cast<duckdb::idx_t>(length));
  return NumericDatumSize({buffer, static_cast<size_t>(length)});
}

int64_t DecimalDatumSize(const duckdb::LogicalType& type,
                         const duckdb::UnifiedVectorFormat& format,
                         duckdb::idx_t index) {
  const auto width = duckdb::DecimalType::GetWidth(type);
  const auto scale = duckdb::DecimalType::GetScale(type);
  const auto physical = type.InternalType();
  if (physical == duckdb::PhysicalType::INT16) {
    return DecimalDatumSize<int16_t>(format, index, width, scale);
  }
  if (physical == duckdb::PhysicalType::INT32) {
    return DecimalDatumSize<int32_t>(format, index, width, scale);
  }
  if (physical == duckdb::PhysicalType::INT64) {
    return DecimalDatumSize<int64_t>(format, index, width, scale);
  }
  return DecimalDatumSize<duckdb::hugeint_t>(format, index, width, scale);
}

int64_t RowDatumSize(duckdb::Vector& input,
                     const duckdb::UnifiedVectorFormat& format,
                     duckdb::idx_t row, duckdb::idx_t index) {
  const auto& type = input.GetType();
  if (IsText(type)) {
    return kVarHdrSz + static_cast<int64_t>(
                         duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
                           format)[index]
                           .GetSize());
  }
  if (type.id() == duckdb::LogicalTypeId::DECIMAL) {
    return DecimalDatumSize(type, format, index);
  }
  return DatumSize(input.GetValue(row));
}

void ColumnSizeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  auto& input = args.data[0];
  const auto fixed = DescribeColumnType(input.GetType()).len;
  duckdb::UnifiedVectorFormat format;
  input.ToUnifiedFormat(args.size(), format);
  auto writer = duckdb::FlatVector::Writer<int32_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto index = format.sel->get_index(row);
    if (!format.validity.RowIsValid(index)) {
      writer.WriteNull();
      continue;
    }
    writer.WriteValue(static_cast<int32_t>(
      fixed > 0 ? fixed : RowDatumSize(input, format, row, index)));
  }
}

duckdb::LogicalType InputType(duckdb::ClientContext& context,
                              std::string_view name) {
  RegIn(MakeSession(&context), RegKind::Type, name, nullptr);
  return duckdb::TransformStringToLogicalType(std::string{name}, context);
}

class InputCheck final : public duckdb::FunctionLocalState {
 public:
  explicit InputCheck(duckdb::ClientContext& context) : _context{context} {}

  TextInput& Input(duckdb::string_t type) {
    const auto name = AsView(type);
    if (!_input || name != _type) {
      _input.emplace(_context, InputType(_context, name));
      _type = name;
    }
    return *_input;
  }

 private:
  duckdb::ClientContext& _context;
  std::string _type;
  std::optional<TextInput> _input;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitInputCheck(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<InputCheck>(state.GetContext());
}

InputCheck& InputCheckOf(duckdb::ExpressionState& state) {
  return duckdb::ExecuteFunctionState::GetFunctionState(state)
    ->Cast<InputCheck>();
}

void InputIsValidFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  auto& check = InputCheckOf(state);
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t text, duckdb::string_t type) {
      return check.Input(type).Valid(text);
    });
}

void WriteNonEmpty(duckdb::VectorWriter<duckdb::string_t>& writer,
                   const std::string& text) {
  if (text.empty()) {
    writer.WriteNull();
  } else {
    writer.WriteValue(duckdb::string_t{text});
  }
}

void InputErrorInfoFunction(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  auto& check = InputCheckOf(state);
  const auto texts = args.data[0].Values<duckdb::string_t>();
  const auto types = args.data[1].Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<duckdb::VectorStructType<
    duckdb::string_t, duckdb::string_t, duckdb::string_t, duckdb::string_t>>(
    result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto text = texts[row];
    const auto type = types[row];
    if (!text.IsValid() || !type.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto error = check.Input(type.GetValue()).Error(text.GetValue());
    writer.WriteValue(
      [&](auto& message, auto& detail, auto& hint, auto& sql_error_code) {
        if (!error) {
          message.WriteNull();
          detail.WriteNull();
          hint.WriteNull();
          sql_error_code.WriteNull();
          return;
        }
        message.WriteValue(duckdb::string_t{error->errmsg});
        WriteNonEmpty(detail, error->errdetail);
        WriteNonEmpty(hint, error->errhint);
        char code[kSqlStateSize];
        UnpackSqlState(code, error->errcode);
        sql_error_code.WriteValue(duckdb::string_t{code, kSqlStateSize});
      });
  }
}

constexpr std::string_view kLogFormats[] = {"stderr", "csvlog", "jsonlog"};

void CurrentLogfileFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  if (args.ColumnCount() != 0) {
    for (const auto format : args.data[0].ValidValues<duckdb::string_t>()) {
      if (!absl::c_linear_search(kLogFormats, AsView(format.GetValue()))) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("log format \"", AsView(format.GetValue()),
                  "\" is not supported"),
          ERR_HINT("The supported log formats are \"stderr\", \"csvlog\", "
                   "and \"jsonlog\"."));
      }
    }
  }
  result.Reference(duckdb::Value{duckdb::LogicalType::VARCHAR},
                   duckdb::count_t(args.size()));
}

template<const std::string_view& kKind,
         bool (*Exists)(duckdb::ClientContext&, int64_t)>
void ActualVersionFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<duckdb::string_t> {
      if (!Exists(context, oid)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                        ERR_MSG(kKind, " with OID ", oid, " does not exist"));
      }
      return duckdb::nullopt;
    });
}

bool CollationExists(duckdb::ClientContext&, int64_t oid) {
  return static_cast<bool>(FindBuiltinCollation(oid));
}

bool DatabaseExists(duckdb::ClientContext& context, int64_t oid) {
  return oid >= 0 && static_cast<bool>(FindDatabaseById(
                       context, static_cast<duckdb::idx_t>(oid)));
}

constexpr std::string_view kCollationKind = "collation";
constexpr std::string_view kDatabaseKind = "database";

[[noreturn]] void ThrowInvalidExtensionName(std::string_view name,
                                            std::string_view detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("invalid extension name: \"", name, "\""),
                  ERR_DETAIL(detail));
}

duckdb::unique_ptr<duckdb::FunctionData> ExtensionUpdatePathsBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<Identifier>& names) {
  for (const auto name : {"source", "target", "path"}) {
    names.emplace_back(name);
    types.emplace_back(duckdb::LogicalType::VARCHAR);
  }
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::OperatorResultType ExtensionUpdatePathsInOut(
  duckdb::ExecutionContext&, duckdb::TableFunctionInput&,
  duckdb::DataChunk& input, duckdb::DataChunk& output) {
  const auto names = input.data[0].Values<duckdb::string_t>();
  for (duckdb::idx_t row = 0; row < input.size(); ++row) {
    const auto value = names[row];
    if (!value.IsValid()) {
      continue;
    }
    const auto name = AsView(value.GetValue());
    if (name.empty()) {
      ThrowInvalidExtensionName(name, "Extension names must not be empty.");
    }
    if (absl::StrContains(name, "--")) {
      ThrowInvalidExtensionName(name,
                                "Extension names must not contain \"--\".");
    }
    if (name.front() == '-' || name.back() == '-') {
      ThrowInvalidExtensionName(
        name, "Extension names must not begin or end with \"-\".");
    }
    if (absl::StrContains(name, '/')) {
      ThrowInvalidExtensionName(
        name,
        "Extension names must not contain directory separator characters.");
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("extension \"", name, "\" is not available"),
                    ERR_HINT("The extension must first be installed on the "
                             "system where PostgreSQL is running."));
  }
  output.SetCardinality(0);
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

struct StatScalar {
  std::string_view name;
  duckdb::LogicalType type;
  duckdb::scalar_function_t function;
};

struct NullFunction {
  std::string_view name;
  duckdb::vector<duckdb::LogicalType> arguments;
  duckdb::LogicalType result;
  duckdb::FunctionStability stability;
};

duckdb::unique_ptr<duckdb::Expression> BindStatisticsNotRestored(
  duckdb::FunctionBindExpressionInput&) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BOOLEAN(false));
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

  for (const auto& [name, function] : {
         NamedScalar{.name = "pg_table_is_visible",
                     .function = IsVisibleFunction<RelationIsVisible>},
         NamedScalar{.name = "pg_type_is_visible",
                     .function = IsVisibleFunction<TypeIsVisible>},
         NamedScalar{.name = "pg_function_is_visible",
                     .function = IsVisibleFunction<FunctionIsVisible>},
       }) {
    RegisterScalar(loader, pg_catalog, name, CONSISTENT_WITHIN_QUERY,
                   InitSession,
                   {duckdb::ScalarFunction{{oid}, boolean, function}});
  }
  RegisterScalar(
    loader, pg_catalog, "pg_get_userbyid", CONSISTENT_WITHIN_QUERY, nullptr,
    {duckdb::ScalarFunction{{oid}, NAME(), PgGetUserByIdFunction}});
  RegisterScalar(
    loader, pg_catalog, "obj_description", CONSISTENT_WITHIN_QUERY, InitSession,
    {duckdb::ScalarFunction{{oid, text}, text, ObjDescription2Function},
     duckdb::ScalarFunction{{oid}, text, ObjDescription1Function}});
  RegisterScalar(
    loader, pg_catalog, "col_description", CONSISTENT_WITHIN_QUERY, InitSession,
    {duckdb::ScalarFunction{{oid, int4}, text, ColDescriptionFunction}});
  {
    duckdb::ScalarFunction function{
      {text, text}, text, PgGetSerialSequenceFunction};
    function.SetFallible();
    RegisterScalar(loader, pg_catalog, "pg_get_serial_sequence",
                   CONSISTENT_WITHIN_QUERY, InitSession, {function});
  }
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
    loader, pg_catalog, "statement_timestamp", CONSISTENT_WITHIN_QUERY, nullptr,
    {duckdb::ScalarFunction{
      {}, duckdb::LogicalType::TIMESTAMP_TZ, StatementTimestampFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_encoding_to_char", CONSISTENT_WITHIN_QUERY, nullptr,
    {duckdb::ScalarFunction{{int4}, NAME(), PgEncodingToCharFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_char_to_encoding", CONSISTENT_WITHIN_QUERY, nullptr,
    {duckdb::ScalarFunction{{text}, int4, PgCharToEncodingFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_encoding_max_length", CONSISTENT, nullptr,
    {duckdb::ScalarFunction{{int4}, int4, PgEncodingMaxLengthFunction}});
  RegisterScalar(loader, pg_catalog, "pg_stat_get_live_tuples",
                 CONSISTENT_WITHIN_QUERY, InitSession,
                 {duckdb::ScalarFunction{{oid}, int8, LiveTuplesFunction}});
  for (const auto& [name, type, function] : {
         StatScalar{.name = "pg_stat_get_db_numbackends",
                    .type = int4,
                    .function = NumBackendsFunction},
         StatScalar{
           .name = "pg_stat_get_db_xact_commit",
           .type = int8,
           .function = DatabaseCounterFunction<&DatabaseStats::xact_commit>},
         StatScalar{
           .name = "pg_stat_get_db_xact_rollback",
           .type = int8,
           .function = DatabaseCounterFunction<&DatabaseStats::xact_rollback>},
         StatScalar{
           .name = "pg_stat_get_db_sessions",
           .type = int8,
           .function = DatabaseCounterFunction<&DatabaseStats::sessions>},
         StatScalar{
           .name = "pg_stat_get_db_session_time",
           .type = float8,
           .function = DatabaseMillisFunction<&DatabaseStats::session_time_us>},
       }) {
    RegisterScalar(loader, pg_catalog, name, CONSISTENT_WITHIN_QUERY, nullptr,
                   {duckdb::ScalarFunction{{oid}, type, function}});
  }

  {
    duckdb::ScalarFunction function{{any}, REGTYPE(), nullptr};
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    function.SetBindExpressionCallback(BindPgTypeof);
    RegisterScalar(loader, main, "pg_typeof", CONSISTENT_WITHIN_QUERY, nullptr,
                   {function});
  }

  {
    duckdb::ScalarFunction function{
      {text, boolean}, text, CurrentSetting2Function};
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    RegisterScalar(loader, main, "current_setting", CONSISTENT_WITHIN_QUERY,
                   nullptr, {function});
  }

  {
    duckdb::ScalarFunction function{
      {text, text, boolean}, text, SetConfigFunction};
    function.SetFallible();
    function.SetRequiresOrderedExecution(true);
    RegisterScalar(loader, main, "set_config", VOLATILE, nullptr, {function});
  }

  RegisterScalar(
    loader, main, "search_path_canonical", CONSISTENT_WITHIN_QUERY, nullptr,
    {duckdb::ScalarFunction{{}, text, SearchPathCanonicalFunction}});

  RegisterScalar(loader, main, "version", CONSISTENT_WITHIN_QUERY, nullptr,
                 {duckdb::ScalarFunction{{}, text, VersionFunction}});

  RegisterScalar(loader, main, "pg_backend_pid", CONSISTENT_WITHIN_QUERY,
                 nullptr,
                 {duckdb::ScalarFunction{{}, int4, PgBackendPidFunction}});

  RegisterScalar(
    loader, main, "pg_cancel_backend", VOLATILE, nullptr,
    {duckdb::ScalarFunction{{int4}, boolean, SignalBackendFunction<false>}});
  RegisterScalar(
    loader, main, "pg_terminate_backend", VOLATILE, nullptr,
    {duckdb::ScalarFunction{{int4}, boolean, SignalBackendFunction<true>}});

  {
    duckdb::ScalarFunction function{{any}, int4, CountNullsFunction<false>};
    function.GetSignature().AddArgs("args", any).AddKwargs("kwargs", any);
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    RegisterScalar(loader, main, "num_nonnulls", CONSISTENT, nullptr,
                   {function});
  }

  {
    duckdb::ScalarFunction function{{any}, int4, CountNullsFunction<true>};
    function.GetSignature().AddArgs("args", any).AddKwargs("kwargs", any);
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    RegisterScalar(loader, main, "num_nulls", CONSISTENT, nullptr, {function});
  }

  {
    duckdb::ScalarFunction function{
      {float8, float8, float8, int4}, int4, WidthBucketFunction};
    function.SetFallible();
    duckdb::ScalarFunction thresholds{{any, duckdb::LogicalType::LIST(any)},
                                      int4,
                                      WidthBucketThresholdsFunction,
                                      WidthBucketThresholdsBind};
    thresholds.SetFallible();
    RegisterScalar(loader, main, "width_bucket", CONSISTENT, nullptr,
                   {function, thresholds});
  }

  RegisterScalar(loader, main, "pg_current_xact_id", CONSISTENT_WITHIN_QUERY,
                 nullptr,
                 {duckdb::ScalarFunction{{}, XID8(), NotSupportedFunction}});
  RegisterScalar(
    loader, main, "pg_xact_status", VOLATILE, nullptr,
    {duckdb::ScalarFunction{{XID8()}, text, NotSupportedFunction}});

  RegisterScalar(loader, main, "current_user", CONSISTENT_WITHIN_QUERY, nullptr,
                 {duckdb::ScalarFunction{{}, text, CurrentUserFunction}});
  RegisterScalar(loader, main, "current_role", CONSISTENT_WITHIN_QUERY, nullptr,
                 {duckdb::ScalarFunction{{}, text, CurrentUserFunction}});
  RegisterScalar(loader, main, "session_user", CONSISTENT_WITHIN_QUERY, nullptr,
                 {duckdb::ScalarFunction{{}, text, SessionUserFunction}});

  RegisterScalar(
    loader, pg_catalog, "pg_basetype", CONSISTENT_WITHIN_QUERY, InitSession,
    {duckdb::ScalarFunction{{REGTYPE()}, REGTYPE(), PgBasetypeFunction}});

  duckdb::ScalarFunction input_is_valid{
    {text, text}, boolean, InputIsValidFunction};
  input_is_valid.SetFallible();
  RegisterScalar(loader, pg_catalog, "pg_input_is_valid",
                 CONSISTENT_WITHIN_QUERY, InitInputCheck, {input_is_valid});
  duckdb::ScalarFunction input_error_info{{text, text},
                                          duckdb::LogicalType::STRUCT({
                                            {"message", text},
                                            {"detail", text},
                                            {"hint", text},
                                            {"sql_error_code", text},
                                          }),
                                          InputErrorInfoFunction};
  input_error_info.SetFallible();
  input_error_info.GetSignature().SetRecordResult();
  RegisterScalar(loader, pg_catalog, "pg_input_error_info",
                 CONSISTENT_WITHIN_QUERY, InitInputCheck, {input_error_info});

  {
    duckdb::ScalarFunction function{{any}, text, nullptr};
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    function.SetBindExpressionCallback(BindPgCollationFor);
    RegisterScalar(loader, pg_catalog, "pg_collation_for",
                   CONSISTENT_WITHIN_QUERY, nullptr, {function});
  }

  duckdb::ScalarFunction replica_identity_index{
    {regclass}, regclass, ReplicaIdentityIndexFunction};
  replica_identity_index.SetFallible();
  RegisterScalar(loader, pg_catalog, "pg_get_replica_identity_index",
                 CONSISTENT_WITHIN_QUERY, InitSession,
                 {replica_identity_index});

  RegisterScalar(
    loader, pg_catalog, "pg_settings_get_flags", CONSISTENT_WITHIN_QUERY,
    nullptr,
    {duckdb::ScalarFunction{
      {text}, duckdb::LogicalType::LIST(text), SettingsGetFlagsFunction}});

  {
    duckdb::TableFunction function{
      "pg_tablespace_databases", {oid},   nullptr,
      TablespaceDatabasesBind,   nullptr, InitRows<int64_t>};
    function.in_out_function =
      RowsInOut<int64_t, &LoadTablespaceDatabases, &WriteTablespaceDatabases>;
    RegisterTable(loader, pg_catalog, std::move(function));
  }

  {
    duckdb::TableFunction function{
      "pg_extension_update_paths", {text}, nullptr, ExtensionUpdatePathsBind};
    function.in_out_function = ExtensionUpdatePathsInOut;
    RegisterTable(loader, pg_catalog, std::move(function));
  }

  RegisterScalar(loader, pg_catalog, "unicode_version", CONSISTENT, nullptr,
                 {duckdb::ScalarFunction{{}, text, UnicodeVersionFunction}});
  RegisterScalar(loader, pg_catalog, "icu_unicode_version", CONSISTENT, nullptr,
                 {duckdb::ScalarFunction{{}, text, IcuUnicodeVersionFunction}});
  RegisterScalar(
    loader, pg_catalog, "unicode_assigned", CONSISTENT, nullptr,
    {duckdb::ScalarFunction{{text}, boolean, UnicodeAssignedFunction}});

  RegisterScalar(loader, pg_catalog, "pg_column_size", CONSISTENT_WITHIN_QUERY,
                 nullptr,
                 {duckdb::ScalarFunction{{any}, int4, ColumnSizeFunction}});
  for (const auto& [name, arguments, result, stability] : {
         NullFunction{
           "pg_column_compression", {any}, text, CONSISTENT_WITHIN_QUERY},
         NullFunction{
           "pg_column_toast_chunk_id", {any}, oid, CONSISTENT_WITHIN_QUERY},
         NullFunction{
           "pg_clear_relation_stats", {text, text}, VOID(), VOLATILE},
         NullFunction{"pg_clear_attribute_stats",
                      {text, text, text, boolean},
                      VOID(),
                      VOLATILE},
       }) {
    duckdb::ScalarFunction function{arguments, result, nullptr};
    function.SetBindExpressionCallback(BindNull);
    RegisterScalar(loader, pg_catalog, name, stability, nullptr, {function});
  }

  for (const auto name :
       {"pg_restore_relation_stats", "pg_restore_attribute_stats"}) {
    duckdb::ScalarFunction function{{any}, boolean, nullptr};
    function.GetSignature().AddArgs("kwargs", any);
    function.SetBindExpressionCallback(BindStatisticsNotRestored);
    RegisterScalar(loader, pg_catalog, name, VOLATILE, nullptr, {function});
  }

  {
    duckdb::ScalarFunction function{{text}, text, CurrentLogfileFunction};
    function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    function.SetFallible();
    RegisterScalar(
      loader, pg_catalog, "pg_current_logfile", VOLATILE, nullptr,
      {duckdb::ScalarFunction{{}, text, CurrentLogfileFunction}, function});
  }

  duckdb::ScalarFunction collation_version{
    {oid}, text, ActualVersionFunction<kCollationKind, &CollationExists>};
  collation_version.SetFallible();
  RegisterScalar(loader, pg_catalog, "pg_collation_actual_version", VOLATILE,
                 nullptr, {collation_version});
  duckdb::ScalarFunction database_collation_version{
    {oid}, text, ActualVersionFunction<kDatabaseKind, &DatabaseExists>};
  database_collation_version.SetFallible();
  RegisterScalar(loader, pg_catalog, "pg_database_collation_actual_version",
                 VOLATILE, nullptr, {database_collation_version});

  RegisterScalar(loader, pg_catalog, "system_user", CONSISTENT_WITHIN_QUERY,
                 nullptr,
                 {duckdb::ScalarFunction{{}, text, SystemUserFunction}});
}

duckdb::GetSQLValueFunctionReturnType SystemUserValueFunction(
  duckdb::PlannerExtensionInput&, const std::string& column_name,
  duckdb::unique_ptr<duckdb::ParsedExpression>& result) {
  if (duckdb::StringUtil::CIEquals(column_name, "system_user")) {
    result = duckdb::make_uniq<duckdb::FunctionExpression>(
      duckdb::Identifier{"system_user"},
      duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>{});
  }
  return duckdb::GetSQLValueFunctionReturnType::CONTINUE_BINDING;
}

}  // namespace sdb::pg
