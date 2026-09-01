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

#include <duckdb/execution/index/bound_index.hpp>
#include <duckdb/execution/index/index_type.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "connector/duckdb_index_utils.h"
#include "search/inverted_index_storage.h"

namespace duckdb {

class DataTable;
class ClientContext;
class DuckTransaction;
class TableIndexList;
class RowGroupCollection;
struct StorageIndex;

}  // namespace duckdb
namespace sdb::catalog {

class Index;

}  // namespace sdb::catalog
namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb::connector {

// Per-index parallel feed: WAL replay and the live commit window share its
// worker pool and retirement ordering. Defined in the .cpp; query::Transaction
// holds a pointer and drives it through the free functions below.
struct InvertedFeedSession;

// The iresearch field an indexed expression feeds. `is_geojson` marks a JSON
// expression that indexes into a synthetic geo column, where JSON object/array
// leaves are meaningful instead of an error.
struct ExpressionField {
  irs::field_id field_id;
  bool is_geojson;
};

// The inverted index as a first-class index on store tables: postings live
// in the iresearch storage keyed by AppendSigned(rowid) PK bytes, fed at
// COMMIT time with final row ids through the committing connection's
// tokenizer/transaction machinery (see CurrentCommittingContext). The
// catalog definition/storage linkage rides the injected ids.
class InvertedStoreIndex final : public duckdb::BoundIndex {
 public:
  static constexpr const char* kTypeName = "inverted";
  // The two ids `create_instance` resolves its definitions by. An index entry
  // carries them in its options, so building the bound index needs nothing but
  // what duckdb hands the registry.
  static constexpr const char* kTableIdOption = "sdb_table_id";
  static constexpr const char* kIndexIdOption = "sdb_index_id";

  // `attached_index`/`attached_storage` are the definition and the open
  // directory this index was injected with. They are the fallback for a writer
  // whose catalog view does not name the index: an online CREATE INDEX attaches
  // its stub to the shared store table before its own transaction commits, so a
  // concurrent DML feeding that stub can legitimately have no catalog entry to
  // resolve either of them through.
  InvertedStoreIndex(
    const std::string& name, duckdb::TableIOManager& io,
    const duckdb::vector<duckdb::column_t>& column_ids,
    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
    duckdb::AttachedDatabase& db,
    std::shared_ptr<const catalog::Index> attached_index,
    std::shared_ptr<search::InvertedIndexStorage> attached_storage,
    std::vector<ExpressionField> expr_fields, bool has_predicate,
    std::vector<FeedColumn> ref_columns);
  ~InvertedStoreIndex() override;

  duckdb::ErrorData Append(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) override;
  duckdb::ErrorData Append(
    duckdb::IndexLock& l,
    const duckdb::shared_ptr<duckdb::ExternalIndexBatch>& batch,
    duckdb::IndexAppendInfo& info) override;
  duckdb::ErrorData Insert(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) override;
  void Delete(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
              duckdb::Vector& row_ids) override;
  duckdb::idx_t TryDelete(
    duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
    duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
    duckdb::optional_ptr<duckdb::SelectionVector> non_deleted_sel) override;

  // Payload lives in the iresearch storage: the checkpoint writes no storage
  // info (the index is re-injected from the catalog at attach) but still runs
  // CheckpointBarrier, which forces the storage durable -- or vetoes -- before
  // the store WAL truncates.
  bool IsExternal() const override { return true; }
  // Postings are keyed by AppendSigned(rowid), so a removal needs the row ids
  // and nothing else -- see Delete, which never reads the chunk.
  bool RemovalNeedsColumnValues() const override { return false; }
  void CheckpointBarrier() override;

  // Called by duckdb after every buffered WAL-replay insert/delete for this
  // bind has been delivered (via Append/Delete with no committing context).
  // Commits the accumulated replay transaction into the iresearch storage.
  void FinishReplay() override;

  // DBConfig::external_range_replay target: replay one merged ROW_GROUP_DATA
  // range into every inverted index of `table` with a single scan of the range
  // over the replay transaction, partitioned across workers the replay thread
  // help-executes -- no second scan, no copy, no side connection.
  static void ReplayExternalRange(duckdb::ClientContext& context,
                                  duckdb::DataTable& table,
                                  duckdb::row_t row_start, duckdb::idx_t count);

  // DBConfig::external_local_append target: feed every inverted index of the
  // table with the rows this commit appends, scanning the local row groups once
  // partitioned across workers. Only called when every index is external.
  static duckdb::ErrorData AppendLocalRange(
    duckdb::DuckTransaction& transaction, duckdb::TableIndexList& index_list,
    duckdb::RowGroupCollection& source,
    const duckdb::vector<duckdb::StorageIndex>& mapped_column_ids,
    duckdb::row_t row_start);

  void ResetStorage(duckdb::IndexLock&) override {}
  bool MergeIndexes(duckdb::IndexLock&, duckdb::BoundIndex&) override {
    return true;
  }
  void Vacuum(duckdb::IndexLock&) override {}
  duckdb::idx_t GetInMemorySize(duckdb::IndexLock&) override { return 0; }
  void Verify(duckdb::IndexLock&) override {}
  std::string ToString(duckdb::IndexLock&, bool) override;
  void VerifyAllocations(duckdb::IndexLock&) override {}
  void VerifyBuffers(duckdb::IndexLock&) override {}
  std::string GetConstraintViolationMessage(duckdb::VerifyExistenceType,
                                            duckdb::idx_t,
                                            duckdb::DataChunk&) override;

 public:
  // duckdb bound these at construction and rewrote their column references
  // into chunk offsets (BoundIndex::BindExpression), exactly as ART's are.
  // Shared read-only with the feed workers, which each run their own
  // ExpressionExecutor over them -- no rebinding, no context, no locking.
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& Expressions()
    const noexcept {
    return bound_expressions;
  }
  std::span<const ExpressionField> ExpressionFields() const noexcept {
    return _expr_fields;
  }
  // The chunk slot each indexed column is read from, derived once where both
  // sides are known: the catalog's referenced-column list and the store table's
  // own column order. Deriving it again from catalog order would agree only for
  // as long as the store mirror is built in that order.
  std::span<const FeedColumn> RefColumns() const noexcept {
    return _ref_columns;
  }
  bool HasPredicate() const noexcept { return _has_predicate; }

  // Lets recovery pair a bound index with the storage it replays into, so each
  // index's refresh can follow its own FinishReplay instead of a global one.
  ObjectId IndexId() const noexcept { return _index_id; }

 private:
  duckdb::ErrorData AppendImpl(duckdb::DataChunk& chunk,
                               duckdb::Vector& row_ids);

  // Replay path: a single iresearch batch held open across one
  // ApplyBufferedReplays pass. Each buffered WAL op is streamed straight into
  // the batch in WAL order on its own strictly-ascending sub-tick (insert ->
  // DuckDBSearchSinkInsertWriter, delete -> a tick-bound Remove), then
  // committed once in FinishReplay with last_tick placing every op above the
  // durable recovery tick. Tick-bound removes give last-op-wins for free (incl.
  // TRUNCATE
  // + rowid reuse), so no dedup is needed. Built lazily on the first replayed
  // operation.
  std::shared_ptr<InvertedFeedSession> EnsureInvertedFeedSession();

  duckdb::idx_t ReplayCommitOffset() const;
  void ReplayAppend(
    const duckdb::shared_ptr<duckdb::ExternalIndexBatch>& batch);
  void ReplayDelete(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);

  ObjectId _index_id;
  // The index definition this one was injected with; see the constructor.
  std::shared_ptr<const catalog::Index> _attached_index;
  // The storage it was injected with, for the same reason: an online build
  // publishes its stub before the entry carrying the handle is committed, so a
  // concurrent writer's own catalog view cannot resolve it.
  std::shared_ptr<search::InvertedIndexStorage> _attached_storage;
  // The iresearch field each of BoundIndex::bound_expressions feeds, in the
  // same order. When _has_predicate is set the last bound expression is the
  // partial-index predicate, which feeds no field -- it selects rows.
  std::vector<ExpressionField> _expr_fields;
  std::vector<FeedColumn> _ref_columns;
  bool _has_predicate = false;
  // Shared, not owned outright: a transaction that has engaged this feed
  // holds a reference for the length of its commit, and DROP INDEX destroys
  // the index (TableIndexList::RemoveIndex) without waiting for it.
  std::shared_ptr<InvertedFeedSession> _feed;
};

// Commit-time driver for one index's parallel feed, called by
// query::Transaction at CommitSearch.
//
// Drain + pin the segments; returns the max per-segment query count. Called
// before the commit tick is allocated.
uint64_t PrepareInvertedFeed(InvertedFeedSession& feed);
// Record the cursor and commit every segment at the tick.
void FinishInvertedFeed(InvertedFeedSession& feed, uint64_t last_tick,
                        std::optional<search::WalCursor> cursor);
// Drop the segments (rollback / teardown).
void AbortInvertedFeed(InvertedFeedSession& feed);

// duckdb's IndexType::create_instance for an inverted index: builds the bound
// index from the entry duckdb already read back, the way it builds an ART.
duckdb::unique_ptr<duckdb::BoundIndex> CreateInvertedInstance(
  duckdb::CreateIndexInput& input);

// Builds a bound inverted index over store table `storage` for the catalog
// index `inverted`, ready for TableIndexList injection. The indexed
// expressions are bound once, up front (like ART).
duckdb::unique_ptr<InvertedStoreIndex> MakeInjectedInvertedIndex(
  duckdb::ClientContext& context, duckdb::DataTable& storage,
  const duckdb::CreateTableInfo& table,
  std::shared_ptr<const catalog::Index> inverted);

// Puts an injected index into `list`, replacing the one already registered
// under its store name. An injected index has no duckdb catalog entry keeping
// that name unique, and a DROP COLUMN batch injects twice -- once from the
// table rebuild (DataTable::RefreshExternalIndexes), once from the store
// CreateIndex op. Two objects over one storage each build their own feed
// session, a commit settles only the last one engaged, and the other's
// registered segment then blocks every later refresh of the index forever.
void AddInjectedInvertedIndex(duckdb::TableIndexList& list,
                              duckdb::unique_ptr<InvertedStoreIndex> index);

// DBConfig::external_index_provider target: whenever a fresh store DataTable
// comes alive (attach checkpoint load, WAL-replay CREATE TABLE, reconciler
// recreate), injects every inverted index the catalog records for it --
// before any of the table's WAL operations replay.
void InjectExternalIndexes(duckdb::DataTable& storage);

}  // namespace sdb::connector
