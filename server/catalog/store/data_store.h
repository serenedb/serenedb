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

#include <absl/functional/function_ref.h>
#include <absl/status/status.h>

#include <atomic>
#include <duckdb/main/connection.hpp>
#include <span>
#include <string_view>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/store/store.h"

namespace sdb {

class ConnectionContext;

}  // namespace sdb
namespace sdb::catalog {

// The data side of the catalog/data split: reshapes the rows of the table an op
// names, in the attachment of the database that owns them. Every serenedb
// database is a duckdb file of its own (<datadir>/engine_duckdb/<id>.db), so
// the target is a property of the op rather than of this class. Store DDL is
// ordered around the catalog-WAL append by CatalogStore::Write; every call runs
// under CatalogStore's write mutex (plus the single-threaded boot), so the
// connection needs no lock of its own.
//
// An op names its table by catalog id, which is what makes it replayable. The
// reshape runs on the statement's own transaction -- the row versions it moves
// are that transaction's -- and the DataTable it produces is parked for the
// write, which is the only thing that makes an entry version.
class DataStore {
 public:
  // Gate for work that touches a database attachment from background tasks
  // (DropTask): false until the boot attach loop has run.
  static bool IsReady() noexcept {
    return gInstance && gInstance->_ready.load(std::memory_order_acquire);
  }

  // True while a store-op batch that drops this column is executing. The
  // catalog half of such a batch lands only after the store ops, so the
  // catalog an injection reads still lists the indexes the same statement
  // cascade-drops for covering the column -- see InjectExternalIndexes.
  static bool IsColumnDropInFlight(ObjectId column_id) noexcept {
    return gInstance != nullptr &&
           gInstance->_dropping_columns.contains(column_id);
  }

  DataStore();
  ~DataStore();

  // Opens the connection store DDL runs on. Must happen before the databases
  // are attached: their WAL replay feeds inverted indexes, which are built
  // through the bind contexts this owns.
  void Initialize();
  // Releases the bind context of a database that is going away, so its
  // connection and its catalog objects do not outlive the attachment.
  static void ForgetDatabase(ObjectId database_id);

  // Opens the gate, once every database is attached. No reconcile pass is
  // needed first: store objects are named by catalog ids, and ids come from a
  // monotonic counter that never reuses them, so a crash between the data
  // commit and the catalog append leaves a store object no catalog object will
  // ever name again -- unreachable, not inconsistent. Nothing is interrupted
  // either: the catalog frame for a create is appended only once its data is
  // durable, so a frame in the file is a decision that was reached.
  void MarkReady();
  void Shutdown();

  // Executes the batch's store ops in order. `context` is the statement that
  // asked for them: the ops run in its transaction. Null for the callers that
  // have no statement -- boot recovery and background drop tasks -- which get
  // a transaction of their own, and hand that transaction the catalog log
  // position so it commits the store change and the position together, the way
  // a statement's does. Zero when the caller records the position itself.
  absl::Status ApplyStoreOps(duckdb::ClientContext* context,
                             std::span<const store_op::Targeted> ops,
                             uint64_t catalog_position = 0);

  // Builds back the ARTs `database_id`'s rows are missing. An ART has no
  // duckdb entry, so the only thing that writes one down is the checkpoint
  // that captures the rows it covers; one built after the last checkpoint is
  // gone after a crash while the definition calling for it survives in the
  // catalog log. Boot rebuilds exactly those, which is why the cost is bounded
  // by what has happened since the last checkpoint rather than by the database.
  void RebuildMissingIndexes(ObjectId database_id);

 private:
  friend void WithStoreBindContext(
    duckdb::AttachedDatabase& db,
    absl::FunctionRef<void(duckdb::ClientContext&)> fn);
  friend DataStore& GetDataStore();

  class StatementTransaction;

  // One per database: the connection that resolves the catalog the way a
  // statement in that database does, so store-side index objects can be built
  // complete while its attach replays into them.
  struct BindContext {
    duckdb::unique_ptr<duckdb::Connection> conn;
    std::shared_ptr<ConnectionContext> ctx;
    // The one a store batch's index builds run on, kept apart from the
    // injection connection above: the two hold transactions on different
    // schedules and would otherwise close each other's.
    duckdb::unique_ptr<duckdb::Connection> exec_conn;
    std::shared_ptr<ConnectionContext> exec_ctx;
  };

  inline static DataStore* gInstance = nullptr;

  // Runs `fn` with a normal serenedb client context for `db`, creating one on
  // first use. Private: callers go through WithStoreBindContext, which gates
  // on the store being up.
  // The connection carrying `db`'s own client state, created on first use.
  // Statements issued on it resolve relations the way a session in that
  // database would, and it is marked as the store's -- which is what sends them
  // to duckdb's native catalog paths instead of back into the mutators.
  duckdb::Connection* BindConnection(duckdb::AttachedDatabase& db);
  void WithBindContext(duckdb::AttachedDatabase& db,
                       absl::FunctionRef<void(duckdb::ClientContext&)> fn);

  absl::Status ExecuteStoreOps(duckdb::ClientContext* context,
                               std::span<const store_op::Targeted> ops);
  absl::Status ExecuteStoreOp(duckdb::ClientContext* context,
                              const store_op::Op& op);
  // The arms with real bodies, so the visit in ExecuteStoreOp reads as the
  // dispatch table it is.
  absl::Status ExecuteAddStoreColumn(duckdb::ClientContext* context,
                                     const store_op::AddColumn& op);
  absl::Status ExecuteCreateStoreIndex(duckdb::ClientContext* context,
                                       const store_op::CreateIndex& op);
  absl::Status ExecuteDropStoreIndex(duckdb::ClientContext* context,
                                     const StoreIndexDef& def);
  absl::Status ExecuteRenameStoreIndex(duckdb::ClientContext* context,
                                       const store_op::RenameIndex& op);
  // Runs one store DDL statement, turning duckdb's error into a Status. What is
  // left of it is the index builds -- CREATE INDEX, ADD PRIMARY KEY, ADD UNIQUE
  // -- because an ART over existing rows is built by a physical plan, and a
  // plan is reached by running a statement.
  absl::Status Exec(const std::string& sql);
  // Reshapes the rows of `table_id` and parks the result for the write.
  // `context` is the statement, or null for the paths with none, which put the
  // rows on the entry themselves.
  absl::Status Alter(duckdb::ClientContext* context, ObjectId table_id,
                     duckdb::AlterInfo& info);
  absl::Status ExecuteAddKeyConstraint(duckdb::ClientContext* context,
                                       ObjectId table_id,
                                       const std::string& constraint,
                                       std::span<const std::string> columns,
                                       bool primary_key);
  absl::Status ExecuteCreateStoreTable(duckdb::ClientContext* context,
                                       ObjectId table_id);
  // The relation an op names. Null when no version of it holds rows -- a drop
  // of the table rides ahead of the ops emitted for its constraints.
  duckdb::optional_ptr<duckdb::DuckTableEntry> ResolveTable(
    duckdb::ClientContext& context, ObjectId table_id);
  static duckdb::AlterEntryData OpTarget(bool missing_ok = false);

  duckdb::unique_ptr<duckdb::Connection> _conn;
  // The connection the batch's index builds run on: the current target's own,
  // routed onto the statement's transaction. Null outside a batch.
  duckdb::Connection* _exec_conn = nullptr;
  // The attachment the ops being executed name. Follows the batch as it moves
  // between databases; null outside one.
  duckdb::AttachedDatabase* _target = nullptr;
  // Kept apart from _conn: the connection that issues store DDL must stay
  // plain, or the statements resolve differently.
  // Reached from two directions now -- a store batch resolving the connection
  // its index builds run on, and an inverted-index injection asking for a bind
  // context -- and those do not share a lock, so the map needs its own.
  absl::Mutex _bind_mutex;
  containers::FlatHashMap<ObjectId, BindContext> _bind_contexts
    ABSL_GUARDED_BY(_bind_mutex);
  // The routing of _conn onto the statement's transaction, while a statement's
  // batch runs. Null on the boot / background path.
  StatementTransaction* _statement = nullptr;
  // The columns the running batch drops, for IsColumnDropInFlight. Filled for
  // the length of ExecuteStoreOps; empty outside one.
  containers::FlatHashSet<ObjectId> _dropping_columns;
  std::atomic<bool> _ready = false;
};

// Opens the statement's transaction on the database it runs in and takes its
// shared checkpoint lock. Called before the catalog mutex: that lock waits for
// a running checkpoint, and a checkpoint waits -- through the inverted-index
// refresh -- for work that needs the catalog mutex. Does nothing before the
// databases are attached or outside a transaction.
void JoinStoreTransaction(duckdb::ClientContext& context,
                          ObjectId fallback_database_id);
// The same for a mutation that only knows the statement it belongs to: the
// rows live in the attachment of the database the statement runs in. A
// statement reaching into another database is not joined on purpose -- opening
// a second attachment's transaction under the catalog mutex is the lock-order
// inversion this exists to avoid, and writing two databases at once is refused
// anyway. Null (boot, background tasks, teardown) is a no-op.
void JoinStoreTransaction(duckdb::ClientContext* context);

// Runs `fn` with a normal serenedb client context for `db`, for building
// store-side index objects. Does nothing before the store exists.
void WithStoreBindContext(duckdb::AttachedDatabase& db,
                          absl::FunctionRef<void(duckdb::ClientContext&)> fn);

DataStore& GetDataStore();

}  // namespace sdb::catalog
