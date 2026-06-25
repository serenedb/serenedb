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

#include <absl/container/node_hash_map.h>

#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <duckdb/parser/statement/transaction_statement.hpp>
#include <duckdb/transaction/transaction_context.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "basics/debugging.h"
#include "network/pg/wire_frames.h"
#include "pg/errcodes.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"

namespace sdb::network::pg {

// Per-execution wire-collector contract (wire_collector.h); a paged portal
// retains it across Execute messages. shared_ptr over a forward declaration is
// safe -- the deleter is captured at construction.
class WireSinkContext;

// What a successful Parse produced -- a closed sum, classified once. A bound
// portal co-owns it (StatementPtr) so it outlives a Close / re-Parse of the
// statement entry; the heavy DuckDB plan inside PreparedStatement is already a
// shared_ptr<PreparedStatementData>, so this adds the one serenedb-level
// refcount at the whole-plan granularity.
//   Unbound  -- a fresh slot, or a deferred COPY already consumed at Execute
//   Empty    -- the empty query string (Execute -> EmptyQueryResponse)
//   Copy     -- COPY ... FROM STDIN / TO STDOUT, run (and consumed) at Execute
//   Prepared -- a normal prepared statement
//   Compound -- one user command the parser expanded into several statements,
//               prepared + run in sequence at Execute (a later sub-statement
//               binds against the catalog the previous one left)
class Statement {
 public:
  enum class Kind : uint8_t {
    Unbound,
    Empty,
    Copy,
    Prepared,
    Compound,
  };

  Kind GetKind() const { return _kind; }
  bool Bound() const { return _kind != Kind::Unbound; }

  void MakeEmpty() { _kind = Kind::Empty; }
  void SetCopy(duckdb::unique_ptr<duckdb::SQLStatement> copy) {
    _statements.clear();
    _statements.push_back(std::move(copy));
    _kind = Kind::Copy;
  }
  void SetPrepared(duckdb::unique_ptr<duckdb::PreparedStatement> prepared) {
    _prepared = std::move(prepared);
    _kind = Kind::Prepared;
  }
  // One user command the parser expanded into several statements (ALTER TABLE
  // ADD COLUMN ... DEFAULT <volatile> -> [ADD; UPDATE; SET DEFAULT]). The body
  // is kept UNPREPARED: a later sub-statement binds against the catalog an
  // earlier one leaves (the backfill UPDATE references the column the ADD
  // COLUMN adds), so preparing it before the ADD COLUMN runs fails. Each is
  // prepared and run in turn at Execute (ExecuteCompound). Bind/Describe see no
  // params and no rows (the expansion's last sub-statement is DDL/DML, never a
  // result set); Execute reports that last one's single CommandComplete. (A
  // PIVOT's leading temp DDL is run + stripped at Parse, so a PIVOT reduces to
  // a plain Prepared.)
  void SetCompound(
    duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> body) {
    _statements = std::move(body);
    _kind = Kind::Compound;
  }

  bool IsCompound() const { return _kind == Kind::Compound; }

  // The unprepared expanded body, consumed statement-by-statement at Execute.
  duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>>& GetCompound() {
    return _statements;
  }

  // Install the prepared last sub-statement once the leading ones have run, so
  // ExecutePrepared drives it (command tag, paging, re-Execute) like any plan.
  void SetCompoundResult(duckdb::unique_ptr<duckdb::PreparedStatement> last) {
    _prepared = std::move(last);
  }

  // The plan that owns the param/result metadata (for a Compound, the last
  // sub-statement, installed by SetCompoundResult). Bind/Describe go through
  // here. Deducing-this: one definition serves const and non-const callers.
  auto& GetPrepared(this auto& self) { return *self._prepared; }

  // Consume the deferred COPY at Execute (run-once): the plan reverts to
  // Unbound so a re-Execute, or a second portal bound to it, gets a clean "not
  // bound".
  duckdb::unique_ptr<duckdb::SQLStatement> TakeCopy() {
    _kind = Kind::Unbound;
    auto copy = std::move(_statements.front());
    _statements.clear();
    return copy;
  }

 private:
  Kind _kind = Kind::Unbound;
  duckdb::unique_ptr<duckdb::PreparedStatement> _prepared;
  duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> _statements;
};

using StatementPtr = std::shared_ptr<Statement>;

// Parameter values bound at Bind + the result column formats chosen there.
struct BindInfo {
  std::vector<sdb::pg::VarFormat> output_formats;
  duckdb::vector<duckdb::Value> param_values;
};

// A portal's execution lifecycle. The old started/exhausted bool pair only had
// three valid states (exhausted-but-not-started never happened).
//   Unstarted -> planned + executed exactly once
//   Paging    -> executing, fetched in batches (the paged wire ctx is retained)
//   Exhausted -> result fully drained; must never be fetched again
enum class PortalState : uint8_t { Unstarted, Paging, Exhausted };

// A portal's execution progress: filled in when Execute first drives the plan,
// then advanced across re-Executes. Kept apart from the portal's identity (the
// plan + bound parameters) so the two lifecycles don't tangle. A paged portal
// retains `wire` (the Direct collector ctx) + `pending` (the parked pipeline)
// across re-Executes; each re-Execute raises wire->row_budget and resumes.
struct PortalExecution {
  duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
  std::shared_ptr<WireSinkContext> wire;
  PortalState state = PortalState::Unstarted;
};

// A bound portal: a co-owned reference to the plan (so Close / re-Parse of the
// statement cannot pull it out from under the portal), the parameters bound at
// Bind, and its execution progress. stmt is null until the portal is bound.
struct Portal {
  StatementPtr stmt;
  BindInfo bind_info;
  PortalExecution exec;
};

// COMMIT/ROLLBACK are the only commands PG still accepts while a transaction
// block is aborted -- they end the block. Everything else is rejected.
inline bool IsTransactionExit(const duckdb::SQLStatement& statement) {
  if (statement.type != duckdb::StatementType::TRANSACTION_STATEMENT) {
    return false;
  }
  const auto type = statement.Cast<duckdb::TransactionStatement>().info->type;
  return type == duckdb::TransactionType::COMMIT ||
         type == duckdb::TransactionType::ROLLBACK;
}

// The implicit-transaction block layered over the connection's autocommit
// state. Borrows the TransactionContext& -- the real state lives there
// (IsAutoCommit / commit / rollback); the only thing we track is the bit it
// cannot derive: did WE open this block (a deferred commit-at-Sync) versus a
// user BEGIN that the client owns. Free of the session, so it is unit-testable.
// It also owns the wire-visible transaction status and the abort, so the
// session never reaches into duckdb::ValidChecker / the TransactionContext.
class ImplicitTxnState {
 public:
  explicit ImplicitTxnState(duckdb::TransactionContext& txn) : _txn{txn} {}

  bool OpenedByUs() const { return _open; }
  bool Explicit() const { return _explicit; }

  // The ReadyForQuery status byte: 'I'dle (autocommit), in a 'T'ransaction
  // block, or 'E'rror (an aborted block, rejecting all but COMMIT/ROLLBACK).
  char StatusByte() const {
    if (_txn.IsAutoCommit()) {
      return 'I';
    }
    if (_txn.HasActiveTransaction() &&
        duckdb::ValidChecker::IsInvalidated(_txn.ActiveTransaction())) {
      return 'E';
    }
    return 'T';
  }

  // May this statement run? In an aborted block PG rejects everything except
  // the COMMIT/ROLLBACK that ends it.
  bool GuardNotAborted(const duckdb::SQLStatement& statement) const {
    return StatusByte() != 'E' || IsTransactionExit(statement);
  }

  // Mark the active (explicit) block aborted so it reads 'E' and later
  // statements surface 25P02. DuckDB invalidates on its own execution errors;
  // a wire-layer error (e.g. parameter decode) never reaches it, so do it here.
  void AbortBlock() {
    if (_txn.HasActiveTransaction() &&
        !duckdb::ValidChecker::IsInvalidated(_txn.ActiveTransaction())) {
      duckdb::ValidChecker::Invalidate(
        _txn.ActiveTransaction(),
        "current transaction is aborted, commands ignored until end of "
        "transaction block");
    }
  }

  // Open the implicit block at the first command of a Sync-delimited batch:
  // SetAutoCommit(false) keeps one MetaTransaction open across the batch.
  // Idempotent; if a user BEGIN is already active the client owns it
  // (_explicit) and the Sync must not resolve it.
  void Arm() {
    if (_open || _explicit) {
      return;
    }
    if (!_txn.IsAutoCommit()) {
      _explicit = true;
      return;
    }
    _txn.SetAutoCommit(false);
    _open = true;
  }

  // After a TRANSACTION_STATEMENT: a still-open transaction is a user BEGIN
  // (hand ownership over), otherwise COMMIT/ROLLBACK closed it.
  void TrackTxnStatement() {
    _open = false;
    _explicit = !_txn.IsAutoCommit();
  }

  bool ShouldCommitAtSync() const { return _open && !_explicit; }

  // Roll back inline at the error site. No stream clause: an abort discards
  // every result regardless of cursor state.
  bool ShouldRollback() const { return _open && !_explicit; }

  // No transaction is open (autocommit): after a COMMIT/ROLLBACK closes the
  // block, portals created in it are dropped at this boundary.
  bool Closed() const { return !_open && !_explicit; }

  // Commit the deferred block. Clears the open bit FIRST so a commit failure
  // still leaves the block resolved, then drives TransactionContext::Commit
  // (the SereneDB commit hook + autocommit restore). Returns the converted
  // error rather than writing it, so the caller's single write site handles it
  // and this stays socket-free / unit-testable.
  std::optional<sdb::pg::SqlErrorData> Commit() {
    _open = false;
    SDB_IF_FAILURE("implicit_block_commit") {
      // Simulate a commit-time failure (deferred-constraint / write-conflict
      // class) deterministically: a real failed Commit() has already torn the
      // transaction down (ClearTransaction runs before the engine commit), so
      // roll back here too and report the error.
      _txn.Rollback(nullptr);
      return sdb::pg::SqlErrorData{
        .errcode = ERRCODE_T_R_SERIALIZATION_FAILURE,
        .errmsg = "injected implicit-block commit failure",
      };
    }
    try {
      _txn.Commit();
      return std::nullopt;
    } catch (const std::exception& exception) {
      return ToSqlError(exception);
    }
  }

  // Roll back the deferred block (non-blocking, no fsync; frees the undo /
  // write-batch / iresearch buffers now, like postgres
  // AbortCurrentTransaction). A rollback failure is catastrophic-only and
  // propagates to teardown.
  void Rollback() {
    _open = false;
    _txn.Rollback(nullptr);
  }

 private:
  duckdb::TransactionContext& _txn;
  bool _open = false;
  bool _explicit = false;
};

// Make a store slot fresh: a statement slot gets a new (empty) plan -- a
// previously installed one survives via any portal that copied the
// StatementPtr; a portal is cleared by whole-struct assign.
inline void ResetSlot(StatementPtr& statement) {
  statement = std::make_shared<Statement>();
}
inline void ResetSlot(Portal& portal) { portal = Portal{}; }

// One store kind: the named map plus the anonymous ("") slot, with the
// find / create / drop vocabulary that five handlers reimplemented. The errcode
// pair + noun are a property of the store kind, set once at construction
// (statements: INVALID_SQL_STATEMENT_NAME / DUPLICATE_PSTATEMENT / "prepared
// statement"; portals: INVALID_CURSOR_NAME / DUPLICATE_CURSOR / "portal").
//
// Element addresses must be stable across insert/erase: HandleExecute holds a
// Portal& across the co_await that drives the query, so the map stays
// node_hash_map; a flat map's rehash would dangle it.
template<typename T>
class NamedStore {
 public:
  NamedStore(int missing_code, int duplicate_code, std::string_view noun)
    : _missing_code{missing_code},
      _duplicate_code{duplicate_code},
      _noun{noun} {
    // The anon slot always exists; for a StatementPtr store that means a valid
    // (empty) plan rather than a null shared_ptr.
    ResetSlot(_anon);
  }

  // Empty name -> the anon slot. A named miss throws the store's missing code.
  T& FindOrThrow(std::string_view name) {
    if (name.empty()) {
      return _anon;
    }
    const auto it = _named.find(name);
    if (it == _named.end()) {
      THROW_SQL_ERROR(ERR_CODE(_missing_code),
                      ERR_MSG(_noun, " \"", name, "\" does not exist"));
    }
    return it->second;
  }

  // Empty name -> the anon slot; a named miss returns nullptr (silent paths:
  // Close of a missing entry, Describe's unbound-anon guard).
  T* Find(std::string_view name) {
    if (name.empty()) {
      return &_anon;
    }
    const auto it = _named.find(name);
    return it == _named.end() ? nullptr : &it->second;
  }

  // Empty name -> the (reset) anon slot. A named collision throws the store's
  // duplicate code; otherwise the fresh entry is returned.
  T& Create(std::string_view name) {
    if (name.empty()) {
      ResetSlot(_anon);
      return _anon;
    }
    auto [it, emplaced] = _named.try_emplace(name);
    if (!emplaced) {
      THROW_SQL_ERROR(ERR_CODE(_duplicate_code),
                      ERR_MSG(_noun, " \"", name, "\" already exists"));
    }
    ResetSlot(it->second);
    return it->second;
  }

  // Undo a name a failed create just defined (Parse prepare failure). No-op for
  // the anon slot and for a name that is not present.
  void Undefine(std::string_view name) {
    if (!name.empty()) {
      _named.erase(name);
    }
  }

  // Anon -> reset the slot (it persists); named -> erase. Silent no-op on a
  // missing named entry (PG Close semantics).
  void Drop(std::string_view name) {
    if (name.empty()) {
      ResetSlot(_anon);
      return;
    }
    _named.erase(name);
  }

  T& Anon() { return _anon; }
  const T& Anon() const { return _anon; }
  containers::NodeHashMap<std::string, T>& Named() { return _named; }
  const containers::NodeHashMap<std::string, T>& Named() const {
    return _named;
  }

  void Clear() {
    _named.clear();
    ResetSlot(_anon);
  }

 private:
  containers::NodeHashMap<std::string, T> _named;
  T _anon{};
  int _missing_code;
  int _duplicate_code;
  std::string_view _noun;
};

// The per-connection prepared-statement and portal stores plus the
// suspended-stream scan that no single store can own. Portals co-own their
// plan (StatementPtr), so closing a statement no longer cascades into portals.
class ProtocolState {
 public:
  ProtocolState()
    : statements(ERRCODE_INVALID_SQL_STATEMENT_NAME,
                 ERRCODE_DUPLICATE_PSTATEMENT, "prepared statement"),
      portals(ERRCODE_INVALID_CURSOR_NAME, ERRCODE_DUPLICATE_CURSOR, "portal") {
  }

  NamedStore<StatementPtr> statements;
  NamedStore<Portal> portals;

  // Drop every portal at a transaction boundary (commit/abort). Wire portals
  // are never holdable -- WITH HOLD is a separate SQL-cursor feature -- so,
  // like CockroachDB, all of them are scoped to the transaction that created
  // them (PG PreCommit_Portals / AtCleanup_Portals zap the non-holdable ones).
  void DropTxnPortals() { portals.Clear(); }

  // Teardown drops portals before statements (portals reference statements).
  void Clear() {
    portals.Clear();
    statements.Clear();
  }
};

}  // namespace sdb::network::pg
