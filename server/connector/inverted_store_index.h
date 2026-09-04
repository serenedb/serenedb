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

#include "basics/containers/flat_hash_map.h"
#include "catalog1/entry/inverted_index.h"
#include "connector/column_id.h"
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

using catalog::InvertedIndexFieldOptions;
using catalog::PkColumnKind;
using catalog::PkPolicy;

// The resolved policy travels in the index's WITH options, under the private
// keys on InvertedStoreIndex below.
void WritePkPolicy(PkPolicy policy,
                   duckdb::case_insensitive_map_t<duckdb::Value>& into);
PkPolicy ReadPkPolicy(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

// Decides each key's opclass at CREATE, where a transaction exists, writes the
// answer into `info.options` and registers the dependency on any text search
// dictionary it resolved. After this the key shape is a pure function of the
// options, which is what lets the entry's constructor decode without one.
// The dictionaries an index's persisted keys name, resolved against the
// caller's transaction. Non-owning pointers into MVCC versions, so they are
// resolved per operation rather than frozen on the entry.
catalog::TokenizerMap ResolveKeyTokenizers(
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  duckdb::SchemaCatalogEntry& schema,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

// The same, for a caller holding only the (const) entry: the catalog and its
// schema are re-acquired from this session, which is where the mutable handles
// a lookup needs come from.
catalog::TokenizerMap ResolveKeyTokenizers(
  duckdb::ClientContext& context, const duckdb::IndexCatalogEntry& entry);

void ResolveAndPersistInvertedKeys(
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  duckdb::SchemaCatalogEntry& schema, duckdb::CreateIndexInfo& info,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
  duckdb::idx_t table_id, std::span<const ColumnId> column_ids);

// Rows handed over by WAL replay. Ownership transfers, because a worker may
// hold them past the call that delivered them and retire them in WAL order --
// which is what distinguishes replay from a commit, where the index scans rows
// it already owns. Nothing produces one until the catalog WAL lands.
struct ReplayBatch {
  duckdb::DataChunk data;
  duckdb::Vector row_ids{duckdb::LogicalType::ROW_TYPE};
};

// The inverted index as a first-class index on store tables: postings live in
// the iresearch storage keyed by AppendSigned(rowid) PK bytes, fed at COMMIT
// time with final row ids through the committing connection's tokenizer /
// transaction machinery (see CurrentCommittingContext). Modelled on duckdb's
// own ART: DuckIndexEntry is the catalog entry, there is no definition object,
// and the type-specific configuration and data are members here.
class InvertedStoreIndex final : public duckdb::BoundIndex {
 public:
  static constexpr const char* kTypeName = catalog::kInvertedIndexTypeName;
  // The two ids Create resolves its catalog entry by. An index entry carries
  // them in its options, so building the bound index needs nothing but what
  // duckdb hands the registry.
  static constexpr const char* kTableIdOption = "sdb_table_id";
  static constexpr const char* kIndexIdOption = "sdb_index_id";
  // The resolved row-key policy. CREATE INDEX derives it from the key shape
  // the index turns out to have, which only that statement knows, so it writes
  // the answer here rather than leaving every reader to re-derive it. Private,
  // like the two ids above: pg_class.reloptions echoes the ten numeric options
  // and nothing else.
  static constexpr const char* kPkTermOption = "sdb_pk_term";
  static constexpr const char* kPkColumnOption = "sdb_pk_column";

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
                     std::shared_ptr<search::InvertedIndexStorage> storage,
                     std::shared_ptr<const InvertedIndexFieldOptions> config,
                     catalog::TokenizerMap tokenizers,
                     std::optional<catalog::ScorerOptions> top_k_scorer);
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

  // The payload lives in the iresearch storage, never in the duckdb file, so
  // both serialization paths write nothing. duckdb's defaults throw; these are
  // what stop a checkpoint or a WAL write from trying to persist the index.
  duckdb::IndexStorageInfo SerializeToDisk(
    duckdb::QueryContext context,
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) override;
  duckdb::IndexStorageInfo SerializeToWAL(
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) override;

  // Forces the storage durable before the store WAL truncates at checkpoint.
  // Not an override yet: duckdb gains the call site with the catalog WAL, so
  // until then nothing invokes this and an un-flushed tail is not protected.
  void CheckpointBarrier();

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
  const auto& Storage() const noexcept { return _storage; }

  const irs::IndexFieldOptions& FieldOptions() const noexcept {
    return *_config;
  }
  // Resolved and parsed at construction, where a ClientContext exists, rather
  // than on the entry's config, which is built without one. Per-operation
  // resolution is what the catalog would prefer, but reaching an entry from a
  // bound index needs the index identity that is still missing.
  const catalog::TokenizerMap& Tokenizers() const noexcept {
    return _tokenizers;
  }
  const std::optional<catalog::ScorerOptions>& TopKScorer() const noexcept {
    return _top_k_scorer;
  }
  std::span<const catalog::InvertedIndexKey> Keys() const noexcept {
    return _config->keys;
  }
  duckdb::optional_ptr<const duckdb::ParsedExpression> Predicate()
    const noexcept {
    return _config->predicate.get();
  }

  std::shared_ptr<const irs::IndexFieldOptions> SharedFieldOptions()
    const noexcept {
    return _config;
  }
  const std::shared_ptr<const InvertedIndexFieldOptions>& SharedConfig()
    const noexcept {
    return _config;
  }

  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& Expressions()
    const noexcept {
    return bound_expressions;
  }
  // The keys that are a bare column, by field id.
  std::span<const ColumnId> IndexedColumns() const noexcept {
    return _config->indexed_columns;
  }
  bool HasPredicate() const noexcept { return _config->predicate != nullptr; }

  // Lets recovery pair a bound index with the storage it replays into, so each
  // index's refresh can follow its own FinishReplay instead of a global one.
  duckdb::idx_t IndexId() const noexcept { return _index_id; }
  duckdb::idx_t TableId() const noexcept { return _table_id; }

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

  std::shared_ptr<search::InvertedIndexStorage> _storage;
  std::shared_ptr<const InvertedIndexFieldOptions> _config;

  catalog::TokenizerMap _tokenizers;
  std::optional<catalog::ScorerOptions> _top_k_scorer;

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

duckdb::optional_ptr<const InvertedStoreIndex> PublishNewInvertedIndex(
  duckdb::ClientContext& context, duckdb::DataTable& storage,
  duckdb::SchemaCatalogEntry& schema, duckdb::idx_t database_id,
  duckdb::idx_t table_id, duckdb::idx_t index_id, duckdb::CreateIndexInfo& info,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs,
  const catalog::InvertedIndexEntry& entry);

// The storage half of the above for a view-backed index: a view has no
// DataTable to register a bound index in, and nothing live-feeds it, so the
// build only needs the iresearch directory. Every per-field answer the writers
// want comes off the entry's own config.
std::shared_ptr<search::InvertedIndexStorage> PublishViewInvertedIndex(
  duckdb::ClientContext& context, duckdb::SchemaCatalogEntry& schema,
  duckdb::idx_t database_id, duckdb::idx_t relation_id, duckdb::idx_t index_id,
  const duckdb::CreateIndexInfo& info);

// The bound index the table holds for `entry`, or null when the entry is not
// an inverted index or is not bound on its table. Binding is a side effect:
// duckdb builds a BoundIndex lazily, and this is the point that forces it.
duckdb::optional_ptr<const InvertedStoreIndex> FindInvertedStore(
  duckdb::ClientContext& context, const duckdb::IndexCatalogEntry& entry);

inline bool IsInvertedIndex(const duckdb::IndexCatalogEntry& entry) {
  return entry.index_type == InvertedStoreIndex::kTypeName;
}

}  // namespace sdb::connector
