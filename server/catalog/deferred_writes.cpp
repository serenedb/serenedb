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

#include "catalog/deferred_writes.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <ranges>
#include <utility>

#include "basics/log.h"
#include "catalog/store/store.h"
#include "connector/duckdb_catalog_sets.h"

namespace sdb::catalog {
namespace {

constexpr const char* kDeferredWritesKey = "sdb_deferred_catalog_writes";

}  // namespace

class DeferredCatalogWrites final : public duckdb::ClientContextState {
 public:
  std::span<const wal::Entry> Queue(std::vector<wal::Entry> entries) {
    for (const auto& entry : entries) {
      if (const auto* op = std::get_if<store_op::Targeted>(&entry)) {
        // At most one per transaction in practice -- a transaction writes one
        // attached database -- but the position has to reach every database a
        // frame named, so it is a set rather than a field.
        if (!absl::c_linear_search(_store_databases, op->database_id)) {
          _store_databases.push_back(op->database_id);
        }
      }
    }
    return _frames.emplace_back(std::move(entries));
  }

  void QueueDrop(const std::shared_ptr<DropTask>& task) {
    _drop_tasks.push_back(task);
  }

  // One mutation. Its entries went into the CatalogSets as the mutators wrote
  // them, on this statement's own transaction -- which is what lets the
  // transaction read its own DDL back out of the sets, and what makes a second
  // transaction writing the same object fail with a write-write conflict
  // rather than silently compose with this one. What is left here is the read
  // view the transaction plans against.
  void Record(duckdb::ClientContext& context) {
    if (_deltas == 0) {
      // The cluster-global sets are a separate attachment, so they are pinned
      // at the first write: this is the view the transaction plans against.
      connector::PinClusterGlobalReadView(context);
    }
    ++_deltas;
  }

  size_t DeltaCount() const noexcept { return _deltas; }

  void TransactionBegin(duckdb::MetaTransaction& /*transaction*/,
                        duckdb::ClientContext& /*context*/) override {
    // Everything here belongs to one transaction. A frame queue that outlived
    // its own would be appended and performed a second time, onto a catalog
    // that already holds its effect.
    SDB_ASSERT(_deltas == 0 && _frames.empty());
  }

  // The commit point, and where the catalog log is written from. The entries
  // went into the sets statement by statement, and duckdb refused this
  // transaction at the first one that clashed with another, so nothing is
  // decided here: the records go out and the catalog takes their effect in as
  // one step, the catalog decides ahead of the data, and an append that fails
  // is an ordinary throw with nothing durable behind it -- duckdb turns a
  // pre-commit hook's exception into a rolled-back commit. That is the point of
  // committing this way round: once the log is a consensus log, a failed append
  // is a lost leadership, not a reason to abort the process.
  //
  // The position the records landed at then rides the data commit, so the
  // database's file records in one atomic commit both the store change and the
  // log position it is in step with. The artifact cleanup starts in the same
  // breath as the records, never before.
  //
  void TransactionPreCommit(duckdb::MetaTransaction& /*transaction*/,
                            duckdb::ClientContext& context) override {
    Commit(context);
  }

  void TransactionCommit(duckdb::MetaTransaction& /*transaction*/,
                         duckdb::ClientContext& /*context*/) override {
    if (!_store_databases.empty() && CatalogStore::Available()) {
      // The data half is durable now, so the log no longer has to hold the
      // batch for a replay -- and the log can fold again.
      for (const auto database_id : _store_databases) {
        GetCatalogStore().AckDatabasePosition(database_id, _position);
      }
      GetCatalogStore().TryCompact();
    }
    Forget();
  }

  void TransactionRollback(duckdb::MetaTransaction& /*transaction*/,
                           duckdb::ClientContext& /*context*/) override {
    if (_appended) {
      // duckdb refused the commit after the catalog had already decided. The
      // records are durable and the rows are not, which is precisely the state
      // the position exists to describe: the catalog half stays so the server
      // is consistent with what is on disk, and the database stays behind the
      // log tail until boot replays the gap.
      SDB_WARN(GENERAL,
               "catalog: the data half of a committed catalog batch was "
               "refused; the database replays it at the next boot (position ",
               _position, ")");
    } else {
      // Nothing to undo. The entries this transaction wrote are its own
      // uncommitted versions, which duckdb discards with it, and the frame
      // queue names work that rolled back with it too.
      _frames.clear();
      _drop_tasks.clear();
    }
    Forget();
  }

 private:
  // The transaction's records go into the log and their effect into the
  // committed snapshot, in one step, and the position they landed at goes to
  // the databases whose commit carries it. Throws on failure, which is what
  // refuses the commit; nothing is durable and nothing is published behind such
  // a throw.
  void Commit(duckdb::ClientContext& context) {
    auto frames = std::exchange(_frames, {});
    auto tasks = std::exchange(_drop_tasks, {});
    // The state lives on the connection, so every later transaction of a
    // connection that once did DDL runs this hook. One that did none has
    // nothing to commit and must not take the catalog mutex for it.
    if (frames.empty() && tasks.empty()) {
      return;
    }
    // A connection closing during teardown still runs its hooks, and by then
    // the store and the background scheduler may be gone.
    if (!CatalogStore::Available()) {
      return;
    }
    _position = GetCatalog().CommitTransaction(&context, frames);
    _appended = !frames.empty();
    for (const auto database_id : _store_databases) {
      RecordCatalogPositionOnCommit(context, database_id, _position);
    }
    for (auto& task : tasks) {
      DropTask::Schedule(std::move(task)).Detach();
    }
  }

  void Forget() noexcept {
    _deltas = 0;
    _position = 0;
    _appended = false;
    _store_databases.clear();
  }

  std::vector<std::shared_ptr<DropTask>> _drop_tasks;
  std::vector<std::vector<wal::Entry>> _frames;
  // The databases whose rows this transaction changed, and the log position its
  // records landed at -- the pair the data commit records.
  std::vector<ObjectId> _store_databases;
  uint64_t _position = 0;
  // How many mutations this transaction has recorded. Only the count is kept:
  // what a commit replays is the frames.
  size_t _deltas = 0;
  // Whether the records are already durable. A rollback after that keeps them
  // rather than undoing them.
  bool _appended = false;
};

namespace {

// Lookup without creating, for the read side. Every statement of every
// transaction asks for its catalog view, and one that never touches the catalog
// must not be given a state -- registering one costs it the commit hooks too.
// The state is owned by the context, so the raw pointer outlives the temporary.
DeferredCatalogWrites* FindDeferredCatalogWrites(
  duckdb::ClientContext& context) {
  if (!context.transaction.HasActiveTransaction()) {
    return nullptr;
  }
  return context.registered_state
    ->Get<DeferredCatalogWrites>(kDeferredWritesKey)
    .get();
}

}  // namespace

DeferredCatalogWrites* TryGetDeferredCatalogWrites(
  duckdb::ClientContext& context) {
  if (!context.transaction.HasActiveTransaction()) {
    return nullptr;
  }
  return context.registered_state
    ->GetOrCreate<DeferredCatalogWrites>(kDeferredWritesKey)
    .get();
}

std::span<const wal::Entry> QueueDeferredFrame(
  duckdb::ClientContext& context, std::vector<wal::Entry> entries) {
  if (entries.empty()) {
    return {};
  }
  auto* state = TryGetDeferredCatalogWrites(context);
  SDB_ASSERT(state != nullptr);
  return state->Queue(std::move(entries));
}

bool QueueDropTask(duckdb::ClientContext& context,
                   const std::shared_ptr<DropTask>& task) {
  auto* state = TryGetDeferredCatalogWrites(context);
  if (state == nullptr) {
    return false;
  }
  state->QueueDrop(task);
  return true;
}

bool RecordCatalogDelta(duckdb::ClientContext& context) {
  auto* state = TryGetDeferredCatalogWrites(context);
  if (state == nullptr) {
    return false;
  }
  state->Record(context);
  return true;
}

size_t CatalogWriteCount(duckdb::ClientContext& context) {
  auto* state = FindDeferredCatalogWrites(context);
  return state == nullptr ? 0 : state->DeltaCount();
}

}  // namespace sdb::catalog
