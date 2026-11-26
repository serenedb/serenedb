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

#pragma once

#include <cstddef>
#include <duckdb/common/enums/statement_type.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <string_view>

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb {

class ConnectionContext;

}  // namespace sdb
namespace sdb::network::pg {

class ImplicitTxnState;

class SimpleQuery {
 public:
  struct Step {
    duckdb::unique_ptr<duckdb::SQLStatement> statement;
    duckdb::StatementType type = duckdb::StatementType::INVALID_STATEMENT;
    bool ends_raw = false;
    bool last = false;
  };

  SimpleQuery(duckdb::ClientContext& context, ConnectionContext& connection,
              ImplicitTxnState& txn)
    : _context{context}, _connection{connection}, _txn{txn} {}

  void Parse(std::string_view query);

  bool Empty() const noexcept { return _parsed.empty(); }

  size_t Statement() const noexcept { return _statement; }

  bool Next(Step& step);

  bool Commits(const Step& step) const;

 private:
  duckdb::ClientContext& _context;
  ConnectionContext& _connection;
  ImplicitTxnState& _txn;
  duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> _parsed;
  duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> _peel;
  size_t _raw = 0;
  size_t _peeled = 0;
  size_t _statement = 0;
  bool _implicit = false;
};

}  // namespace sdb::network::pg
