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

#include "network/pg/simple_query.h"

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/parse_iterator.hpp>
#include <utility>

#include "network/pg/protocol_state.h"
#include "pg/connection_context.h"

namespace sdb::network::pg {

void SimpleQuery::Parse(std::string_view query) {
  if (!query.empty()) {
    for (duckdb::ParseIterator parser{_context, query}; parser.Peek();) {
      _parsed.push_back(parser.GetStatement());
    }
  }
  _implicit = _parsed.size() > 1;
}

bool SimpleQuery::Next(Step& step) {
  while (_peeled == _peel.size()) {
    if (_raw == _parsed.size()) {
      return false;
    }
    auto& raw = _parsed[_raw++];
    _number = _raw;
    if (!_txn.GuardNotAborted(*raw)) {
      ThrowAbortedTransaction();
    }
    _peel.clear();
    _peeled = 0;
    _peel.push_back(std::move(raw));
    _context.PreprocessStatements(_peel, nullptr, false);
    if (_peel.empty()) {
      step = {};
      return true;
    }
    _implicit = _implicit || _peel.size() > 1;
  }
  auto& statement = _peel[_peeled++];
  step.type = statement->type;
  step.ends_raw = _peeled == _peel.size();
  step.last = step.ends_raw && _raw == _parsed.size();
  _connection.SetInTransactionBlock(_implicit ||
                                    !_context.transaction.IsAutoCommit());
  if (_implicit && step.type != duckdb::StatementType::TRANSACTION_STATEMENT) {
    _txn.Arm();
  }
  step.statement = std::move(statement);
  return true;
}

bool SimpleQuery::Commits(const Step& step) const {
  return step.last && _implicit &&
         step.type != duckdb::StatementType::TRANSACTION_STATEMENT &&
         _txn.ShouldCommitAtSync();
}

}  // namespace sdb::network::pg
