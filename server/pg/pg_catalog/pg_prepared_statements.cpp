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

#include "pg/pg_catalog/pg_prepared_statements.h"

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <string>
#include <vector>

#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "network/pg/protocol_state.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

struct PreparedStatementRow {
  std::string name;
  duckdb::shared_ptr<duckdb::PreparedStatementData> data;
  bool from_sql = false;
};

struct PreparedStatementsState final : duckdb::GlobalTableFunctionState {
  std::vector<PreparedStatementRow> rows;
  size_t offset = 0;
};

duckdb::Value TypeArray(const std::vector<uint64_t>& oids) {
  duckdb::vector<duckdb::Value> values;
  values.reserve(oids.size());
  for (const auto oid : oids) {
    values.push_back(duckdb::Value::BIGINT(OidToSql(oid)).WithType(REGTYPE()));
  }
  return duckdb::Value::LIST(REGTYPE(), std::move(values));
}

duckdb::Value ParameterTypes(duckdb::PreparedStatementData& data) {
  const auto& parameters = data.unbound_statement->named_param_map;
  std::vector<uint64_t> oids(parameters.size(),
                             static_cast<uint64_t>(PgTypeOID::kUnknown));
  for (const auto& [identifier, index] : parameters) {
    if (index < 1 || index > oids.size()) {
      continue;
    }
    duckdb::LogicalType type;
    if (data.TryGetType(identifier, type)) {
      oids[index - 1] = Type2Oid(type);
    } else if (const auto hint =
                 data.parameter_type_hints.find(identifier.GetIdentifierName());
               hint != data.parameter_type_hints.end()) {
      oids[index - 1] = Type2Oid(hint->second);
    }
  }
  return TypeArray(oids);
}

duckdb::Value ResultTypes(const duckdb::PreparedStatementData& data) {
  if (data.properties.return_type !=
      duckdb::StatementReturnType::QUERY_RESULT) {
    return duckdb::Value{duckdb::LogicalType::LIST(REGTYPE())};
  }
  std::vector<uint64_t> oids;
  oids.reserve(data.types.size());
  for (const auto& type : data.types) {
    oids.push_back(Type2Oid(type));
  }
  return TypeArray(oids);
}

duckdb::unique_ptr<duckdb::FunctionData> PreparedStatementsBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  const auto add = [&](std::string_view name, duckdb::LogicalType type) {
    names.emplace_back(name);
    return_types.emplace_back(std::move(type));
  };
  add("name", duckdb::LogicalType::VARCHAR);
  add("statement", duckdb::LogicalType::VARCHAR);
  add("prepare_time", duckdb::LogicalType::TIMESTAMP_TZ);
  add("parameter_types", duckdb::LogicalType::LIST(REGTYPE()));
  add("result_types", duckdb::LogicalType::LIST(REGTYPE()));
  add("from_sql", duckdb::LogicalType::BOOLEAN);
  add("generic_plans", duckdb::LogicalType::BIGINT);
  add("custom_plans", duckdb::LogicalType::BIGINT);
  return nullptr;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PreparedStatementsInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto state = duckdb::make_uniq<PreparedStatementsState>();
  const auto client =
    context.registered_state->Get<connector::SereneDBClientState>(
      connector::kSereneDBClientStateKey);
  const auto* protocol = client ? client->protocol_state : nullptr;
  for (const auto& [name, data] :
       duckdb::ClientData::Get(context).prepared_statements) {
    if (duckdb::ClientContext::IsPreparedStatementHandle(name)) {
      continue;
    }
    state->rows.push_back({
      .name = name.GetIdentifierName(),
      .data = data,
      .from_sql = !protocol || !protocol->statements.Named().contains(
                                 name.GetIdentifierName()),
    });
  }
  return state;
}

void PreparedStatementsExecute(duckdb::ClientContext& context,
                               duckdb::TableFunctionInput& input,
                               duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<PreparedStatementsState>();
  duckdb::idx_t count = 0;
  for (; state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE;
       ++state.offset, ++count) {
    auto& row = state.rows[state.offset];
    auto& data = *row.data;
    duckdb::idx_t col = 0;
    output.SetValue(col++, count, duckdb::Value{row.name});
    output.SetValue(col++, count, duckdb::Value{data.unbound_statement->query});
    output.SetValue(col++, count,
                    duckdb::Value::TIMESTAMPTZ(data.prepare_time));
    output.SetValue(col++, count, ParameterTypes(data));
    output.SetValue(col++, count, ResultTypes(data));
    output.SetValue(col++, count, duckdb::Value::BOOLEAN(row.from_sql));
    output.SetValue(col++, count, duckdb::Value{duckdb::LogicalType::BIGINT});
    output.SetValue(col++, count, duckdb::Value{duckdb::LogicalType::BIGINT});
  }
  output.SetChildCardinality(count);
}

}  // namespace

void RegisterPreparedStatements(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(duckdb::TableFunction{"pg_prepared_statement",
                                                {},
                                                PreparedStatementsExecute,
                                                PreparedStatementsBind,
                                                PreparedStatementsInit});
}

}  // namespace sdb::pg
