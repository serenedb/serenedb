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
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <duckdb/storage/storage_index.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <duckdb/storage/table/table_index_list.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_sink_writer_base.h"
#include "query/transaction.h"
#include "search/inverted_index_storage.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

using catalog::InvertedIndexConfig;
using catalog::PkColumnKind;
using catalog::PkPolicy;

class InvertedStoreIndex final : public duckdb::BoundIndex {
 public:
  static constexpr const char* kTypeName = catalog::kInvertedIndexTypeName;

  static duckdb::unique_ptr<duckdb::BoundIndex> Create(
    duckdb::CreateIndexInput& input);

  static duckdb::IndexType GetInvertedIndexType();

  InvertedStoreIndex(duckdb::CreateIndexInput& input, duckdb::idx_t index_id,
                     std::shared_ptr<search::InvertedIndexStorage> storage,
                     std::shared_ptr<const InvertedIndexConfig> config,
                     catalog::IndexTokenizers tokenizers, bool has_predicate);
  ~InvertedStoreIndex() final;

  duckdb::ErrorData Append(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) final {
    return AppendImpl(chunk, row_ids);
  }
  duckdb::ErrorData Insert(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) final {
    return AppendImpl(chunk, row_ids);
  }
  duckdb::ErrorData FinishAppend() final;
  void Delete(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
              duckdb::Vector& row_ids) final;
  duckdb::idx_t TryDelete(
    duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
    duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
    duckdb::optional_ptr<duckdb::SelectionVector> non_deleted_sel) final;
  bool RemovalNeedsColumnValues() const final { return false; }

  void FinishReplay();

  duckdb::IndexStorageInfo SerializeToDisk(
    duckdb::QueryContext context,
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) final;
  duckdb::IndexStorageInfo SerializeToWAL(
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) final;

  void ResetStorage(duckdb::IndexLock&) final {}
  bool MergeIndexes(duckdb::IndexLock&, duckdb::BoundIndex&) final {
    return true;
  }
  void Vacuum(duckdb::IndexLock&) final {}
  duckdb::idx_t GetInMemorySize(duckdb::IndexLock&) const final { return 0; }
  void Verify(duckdb::IndexLock&) final {}
  std::string ToString(duckdb::IndexLock&, bool) final {
    return "inverted store index";
  }
  void VerifyAllocations(duckdb::IndexLock&) final {}
  void VerifyBuffers(duckdb::IndexLock&) final {}
  std::string GetConstraintViolationMessage(duckdb::VerifyExistenceType,
                                            duckdb::idx_t,
                                            duckdb::DataChunk&) const final {
    return "inverted store index constraint violation";
  }

 public:
  const auto& Storage() const noexcept { return _storage; }

  duckdb::idx_t IndexId() const noexcept { return _index_id; }

  void PrepareFeed(query::Transaction& transaction,
                   duckdb::ClientContext& context, duckdb::idx_t rows);

  static bool AnyBound() noexcept;

  static bool AppendLocal(
    duckdb::DuckTransaction& transaction, duckdb::TableIndexList& index_list,
    duckdb::RowGroupCollection& source,
    const duckdb::vector<duckdb::StorageIndex>& mapped_column_ids,
    duckdb::row_t row_start, duckdb::ErrorData& error);

 private:
  struct ReplayOp;
  struct FeedQueue;
  struct ReplaySlot;
  struct ReplaySession;
  struct LiveFeed;
  struct FeedTask;
  struct RangeTask;

  duckdb::ErrorData AppendImpl(duckdb::DataChunk& chunk,
                               duckdb::Vector& row_ids);

  irs::IndexWriter::Transaction NewTransaction();
  duckdb::idx_t Evaluate(duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
                         duckdb::DataChunk& results, duckdb::Vector& rows,
                         duckdb::ExpressionExecutor* executor = nullptr);
  void Feed(DuckDBSinkIndexWriter& writer, irs::IndexWriter::Transaction& trx,
            duckdb::DataChunk& results, duckdb::Vector& rows,
            duckdb::idx_t count);

  ReplaySession* ReplaySessionForEntry();
  void Enqueue(duckdb::TaskExecutor& executor, FeedQueue& queue,
               std::unique_ptr<ReplayOp> op);
  void Apply(FeedQueue& queue, ReplayOp& op);
  static std::unique_ptr<ReplayOp> CopyInsert(duckdb::DataChunk& results,
                                              duckdb::Vector& rows,
                                              duckdb::idx_t count);
  std::unique_ptr<ReplayOp> CopyInsertShared(query::Transaction& transaction,
                                             duckdb::DataChunk& chunk,
                                             duckdb::DataChunk& results,
                                             duckdb::Vector& rows,
                                             duckdb::idx_t count);
  void ReplayAppend(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);
  void ReplayDelete(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);
  bool CommitReplay(ReplaySession& session,
                    std::span<irs::IndexWriter::Transaction* const> trxs,
                    const search::WalCursor* cursor);
  bool CommitReplaySlots(ReplaySession& session,
                         const search::WalCursor* cursor);

  duckdb::idx_t _index_id = 0;

  std::shared_ptr<search::InvertedIndexStorage> _storage;
  std::shared_ptr<const InvertedIndexConfig> _config;
  catalog::IndexTokenizers _tokenizers;
  bool _has_predicate = false;

  std::unique_ptr<ReplaySession> _replay;
  std::unique_ptr<LiveFeed> _live;
};

struct PublishedInvertedIndex {
  std::shared_ptr<search::InvertedIndexStorage> storage;
  duckdb::idx_t rowid_horizon = 0;
};

PublishedInvertedIndex PublishInvertedIndex(
  duckdb::ClientContext& context, catalog::InvertedIndexEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs);

inline bool IsInvertedIndex(const duckdb::IndexCatalogEntry& entry) {
  return entry.index_type == InvertedStoreIndex::kTypeName;
}

}  // namespace sdb::connector
