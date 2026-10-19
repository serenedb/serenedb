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

#include "network/pg/protocol_state.h"

#include <duckdb/main/client_data.hpp>

namespace sdb::network::pg {

duckdb::shared_ptr<duckdb::PreparedStatementData> PreparedData(
  duckdb::ClientContext& context, const Statement& statement) {
  if (statement.GetKind() != Statement::Kind::Prepared || !statement.Plan()) {
    return nullptr;
  }
  const auto& statements = duckdb::ClientData::Get(context).prepared_statements;
  const auto it =
    statements.find(duckdb::Identifier{statement.Plan()->GetName()});
  return it == statements.end() ? nullptr : it->second;
}

duckdb::shared_ptr<duckdb::PreparedStatementData>
ProtocolStatements::GetPreparedStatement(duckdb::ClientContext& context,
                                         const duckdb::Identifier& name) {
  const auto it = _statements.Named().find(name.GetIdentifierName());
  if (it == _statements.Named().end()) {
    return nullptr;
  }
  return PreparedData(context, *it->second);
}

bool ProtocolStatements::DeallocatePreparedStatement(
  duckdb::ClientContext& context, const duckdb::Identifier& name) {
  auto& named = _statements.Named();
  const auto it = named.find(name.GetIdentifierName());
  if (it == named.end()) {
    return false;
  }
  _deallocated.push_back(std::move(it->second));
  named.erase(it);
  return true;
}

void ProtocolStatements::DeallocatePreparedStatements(
  duckdb::ClientContext& context) {
  auto& named = _statements.Named();
  for (auto& [name, statement] : named) {
    _deallocated.push_back(std::move(statement));
  }
  named.clear();
}

}  // namespace sdb::network::pg
