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
    duckdb::ParseIterator parser{_context, query};
    for (_number = 1; parser.Peek(); ++_number) {
      _parsed.push_back(parser.GetStatement());
    }
  }
  _number = 0;
  // A multi-statement simple query is an implicit transaction block (postgres
  // exec_simple_query: use_implicit_block = list_length > 1): every statement
  // commits or rolls back together. A single statement stays autocommit -- the
  // common fast path. Arming happens lazily per statement (postgres
  // re-evaluates each iteration), so a leading or mid-string
  // BEGIN/COMMIT/ROLLBACK reaches the transaction operator itself rather than
  // running wrapped in our block; a following statement then arms the block
  // (or joins the user's BEGIN).
  _implicit = _parsed.size() > 1;
}

bool SimpleQuery::Next(Step& step) {
  while (_peeled == _peel.size()) {
    if (_raw == _parsed.size()) {
      return false;
    }
    auto& raw = _parsed[_raw++];
    _number = _raw;
    // In an aborted transaction block PG rejects every statement except
    // COMMIT/ROLLBACK with 25P02 before parse-analysis. Mirror the extended
    // path (HandleParse): without this guard PREPARE/DEALLOCATE -- whose binder
    // permits an invalid transaction -- would execute here where PG rejects.
    if (!_txn.GuardNotAborted(*raw)) {
      ThrowAbortedTransaction();
    }
    _peel.clear();
    _peeled = 0;
    _peel.push_back(std::move(raw));
    _context.PreprocessStatements(_peel, nullptr, /*wrap_multi=*/false);
    if (_peel.empty()) {
      step = {.number = _number};
      return true;
    }
    _implicit = _implicit || _peel.size() > 1;
  }
  auto& statement = _peel[_peeled++];
  step.type = statement->type;
  step.number = _number;
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

// PG commits the implicit block in finish_xact_command BEFORE EndCommand for
// the last statement (postgres.c:1304-1314), so a commit-time failure is
// reported as an ErrorResponse in place of that statement's CommandComplete
// -- never one then the other. Commit just before the final tag; the trailing
// Sync then finds the block already resolved. A TRANSACTION_STATEMENT commits
// itself, and a trailing COPY completes before reaching the tag, so both keep
// the plain Sync-time commit.
bool SimpleQuery::Commits(const Step& step) const {
  return step.last && _implicit &&
         step.type != duckdb::StatementType::TRANSACTION_STATEMENT &&
         _txn.ShouldCommitAtSync();
}

bool SimpleQuery::Closes(const Step& step) {
  if (step.type != duckdb::StatementType::TRANSACTION_STATEMENT) {
    return false;
  }
  _txn.TrackTxnStatement();
  return _txn.Closed();
}

}  // namespace sdb::network::pg
