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

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <optional>
#include <string>
#include <vector>

#include "connector/duckdb_client_state.h"
#include "network/pg/protocol_state.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types/mapping.h"

namespace sdb::pg {
namespace {

struct PreparedStatement {
  std::string name;
  duckdb::shared_ptr<duckdb::PreparedStatementData> data;
  std::vector<uint32_t> parameter_types;
  std::optional<std::vector<uint32_t>> result_types;
  bool from_sql;
};

std::vector<uint32_t> ParameterTypes(duckdb::PreparedStatementData& data) {
  const auto& parameters = data.unbound_statement->named_param_map;
  std::vector<uint32_t> oids(parameters.size(), kUnknown);
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
  return oids;
}

std::optional<std::vector<uint32_t>> ResultTypes(
  const duckdb::PreparedStatementData& data) {
  if (data.properties.return_type !=
      duckdb::StatementReturnType::QUERY_RESULT) {
    return std::nullopt;
  }
  std::vector<uint32_t> oids;
  oids.reserve(data.types.size());
  for (const auto& type : data.types) {
    oids.push_back(Type2Oid(type));
  }
  return oids;
}

SystemRows<PreparedStatement> LoadPreparedStatements(SystemScan& scan) {
  auto& context = scan.Context();
  const auto client =
    context.registered_state->Get<connector::SereneDBClientState>(
      connector::kSereneDBClientStateKey);
  const auto* protocol = client ? client->protocol_state : nullptr;
  std::vector<PreparedStatement> rows;
  for (const auto& [name, data] :
       duckdb::ClientData::Get(context).prepared_statements) {
    if (duckdb::ClientContext::IsPreparedStatementHandle(name)) {
      continue;
    }
    const auto& text = name.GetIdentifierName();
    rows.push_back(
      {.name = text,
       .data = data,
       .parameter_types = ParameterTypes(*data),
       .result_types = ResultTypes(*data),
       .from_sql = !protocol || !protocol->statements.Named().contains(text)});
  }
  return rows;
}

class PgPreparedStatement final
  : public SystemTableScan<kPgPreparedStatementSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<PreparedStatement>{&LoadPreparedStatements, {}, {}}};

  static constexpr auto kStatement = Shape<kSql, const PreparedStatement>(
    Col<"name">([](const auto& row) -> const auto& { return row.name; }),
    Col<"statement">([](const auto& row) -> const auto& {
      return row.data->unbound_statement->query;
    }),
    Col<"prepare_time">([](const auto& row) { return row.data->prepare_time; }),
    Col<"parameter_types">(
      [](const auto& row) -> const auto& { return row.parameter_types; }),
    Col<"result_types">(
      [](const auto& row) -> const auto& { return row.result_types; }),
    Col<"from_sql">(&PreparedStatement::from_sql));

  void Row(const PreparedStatement& statement) { Emit<kStatement>(statement); }
};

}  // namespace

SystemTable gPgPreparedStatement = SystemTableOf<PgPreparedStatement>();

}  // namespace sdb::pg
