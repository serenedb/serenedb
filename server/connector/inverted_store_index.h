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

#include <cstdint>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/execution/index/bound_index.hpp>
#include <duckdb/execution/index/index_type.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "connector/duckdb_index_utils.h"
#include "search/inverted_index_storage.h"

namespace duckdb {

class DataTable;
class ClientContext;
class DuckTransaction;
class TableCatalogEntry;
class TableIndexList;
class RowGroupCollection;
struct StorageIndex;

}  // namespace duckdb
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
  static constexpr const char* kTypeName = catalog::kInvertedIndexTypeName;

  // IndexType::create_instance, as ART has it.
  static duckdb::unique_ptr<duckdb::BoundIndex> Create(
    duckdb::CreateIndexInput& input);
  // IndexType registration, as ART has it. Named like ART's
  // GetARTIndexType() for the same reason: Index::GetIndexType() is a virtual
  // of the base returning the type NAME, so this one cannot share it.
  static duckdb::IndexType GetInvertedIndexType();

  // `storage` may be null only while a CREATE INDEX is still opening it; every
  // feed asserts it. `config` is never null.
  InvertedStoreIndex(duckdb::CreateIndexInput& input,
                     duckdb::SchemaCatalogEntry& schema, duckdb::idx_t index_id,
                     duckdb::idx_t table_id,
                     std::shared_ptr<search::InvertedIndexStorage> storage,
                     std::shared_ptr<const InvertedIndexConfig> config);
  ~InvertedStoreIndex() override;

  duckdb::ErrorData Append(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) override;
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
  // over the replay transaction. Not registered yet -- the catalog-WAL phase
  // supplies the call site.
  static void ReplayExternalRange(duckdb::ClientContext& context,
                                  duckdb::DataTable& table,
                                  duckdb::row_t row_start, duckdb::idx_t count);

  // DBConfig::external_local_append target: feed every inverted index of the
  // table with the rows this commit appends, scanning the local row groups once
  // partitioned across workers. Not registered yet -- duckdb's serial append
  // through Append() is the standing path.
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
  duckdb::idx_t IndexId() const noexcept { return _index_id; }

 private:
  duckdb::ErrorData AppendImpl(duckdb::DataChunk& chunk,
                               duckdb::Vector& row_ids);

  std::shared_ptr<InvertedFeedSession> EnsureInvertedFeedSession();

  duckdb::idx_t ReplayCommitOffset() const;
  // How far the WAL replayed cleanly. The catalog-WAL phase supplies the real
  // bound; until then replay retires everything it buffered.
  duckdb::idx_t ReplaySuccessOffset() const;
  void ReplayAppend(const std::shared_ptr<ReplayBatch>& batch);
  void ReplayDelete(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);

  duckdb::idx_t _index_id = 0;
  duckdb::idx_t _table_id = 0;

  duckdb::SchemaCatalogEntry& _schema;
  std::shared_ptr<search::InvertedIndexStorage> _storage;
  std::shared_ptr<const InvertedIndexConfig> _config;

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
  const duckdb::TableCatalogEntry& table,
  duckdb::optional_ptr<const duckdb::IndexCatalogEntry> inverted);

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
