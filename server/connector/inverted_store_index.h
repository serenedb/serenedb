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
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog1/entry/inverted_index.h"
#include "connector/duckdb_index_utils.h"
#include "search/inverted_index_storage.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

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

  static duckdb::unique_ptr<duckdb::BoundIndex> Create(
    duckdb::CreateIndexInput& input);

  static duckdb::IndexType GetInvertedIndexType();

  InvertedStoreIndex(duckdb::CreateIndexInput& input,
                     duckdb::SchemaCatalogEntry& schema, duckdb::idx_t index_id,
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

  duckdb::IndexStorageInfo SerializeToDisk(
    duckdb::QueryContext context,
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) override;
  duckdb::IndexStorageInfo SerializeToWAL(
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) override;

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

  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& Expressions()
    const noexcept {
    return bound_expressions;
  }

  duckdb::idx_t IndexId() const noexcept { return _index_id; }

 private:
  duckdb::ErrorData AppendImpl(duckdb::DataChunk& chunk,
                               duckdb::Vector& row_ids);

  std::shared_ptr<InvertedFeedSession> EnsureInvertedFeedSession();

  duckdb::idx_t _index_id = 0;

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

std::shared_ptr<search::InvertedIndexStorage> PublishInvertedIndex(
  duckdb::ClientContext& context, catalog::InvertedIndexEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs);

inline bool IsInvertedIndex(const duckdb::IndexCatalogEntry& entry) {
  return entry.index_type == InvertedStoreIndex::kTypeName;
}

}  // namespace sdb::connector
