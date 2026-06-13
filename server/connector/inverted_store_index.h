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
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"

namespace sdb {

class ConnectionContext;

}  // namespace sdb
namespace sdb::catalog {

class Table;

}  // namespace sdb::catalog
namespace sdb::connector {

// The inverted index as a first-class index on store tables: postings live
// in the iresearch shard keyed by AppendSigned(rowid) PK bytes, fed at
// COMMIT time with final row ids through the committing connection's
// tokenizer/transaction machinery (see CurrentCommittingContext). The
// catalog InvertedIndex/shard linkage rides CreateIndexInfo options
// (sdb_table_id / sdb_index_id).
class InvertedStoreIndex final : public duckdb::BoundIndex {
 public:
  static constexpr const char* kTypeName = "inverted";
  static constexpr const char* kTableIdOption = "sdb_table_id";
  static constexpr const char* kIndexIdOption = "sdb_index_id";

  InvertedStoreIndex(
    const std::string& name, duckdb::TableIOManager& io,
    const duckdb::vector<duckdb::column_t>& column_ids,
    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
    duckdb::AttachedDatabase& db, ObjectId table_id, ObjectId index_id);
  ~InvertedStoreIndex() override;

  duckdb::ErrorData Append(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) override;
  duckdb::ErrorData Insert(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) override;
  void Delete(duckdb::IndexLock& l, duckdb::DataChunk& chunk,
              duckdb::Vector& row_ids) override;
  idx_t TryDelete(
    duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
    duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
    duckdb::optional_ptr<duckdb::SelectionVector> non_deleted_sel) override;

  // Called by duckdb before each buffered WAL-replay range, with that range's
  // store-WAL byte offset. Operations at/below the shard's durable cursor are
  // already in the segments; we record the offset so ReplayAppend/ReplayDelete
  // can skip them.
  void OnReplayRange(duckdb::idx_t commit_offset) override;

  // Called by duckdb after every buffered WAL-replay insert/delete for this
  // bind has been delivered (via Append/Delete with no committing context).
  // Commits the accumulated replay transaction into the iresearch shard.
  void FinishReplay() override;

  void ResetStorage(duckdb::IndexLock&) override {}
  bool MergeIndexes(duckdb::IndexLock&, duckdb::BoundIndex&) override {
    return true;
  }
  void Vacuum(duckdb::IndexLock&) override {}
  idx_t GetInMemorySize(duckdb::IndexLock&) override { return 0; }
  void Verify(duckdb::IndexLock&) override {}
  std::string ToString(duckdb::IndexLock&, bool) override;
  void VerifyAllocations(duckdb::IndexLock&) override {}
  void VerifyBuffers(duckdb::IndexLock&) override {}
  duckdb::IndexStorageInfo SerializeToDisk(
    duckdb::QueryContext,
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) override;
  duckdb::IndexStorageInfo SerializeToWAL(
    const duckdb::case_insensitive_map_t<duckdb::Value>& options) override;
  std::string GetConstraintViolationMessage(duckdb::VerifyExistenceType, idx_t,
                                            duckdb::DataChunk&) override;

 public:
  // Feeds rows with a known connection (initial CREATE INDEX build runs in
  // normal execution; commit-time appends resolve it thread-locally).
  duckdb::ErrorData AppendRows(
    ConnectionContext& conn, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
    std::span<const catalog::Column::Id> chunk_column_ids);

  static std::vector<catalog::Column::Id> TableChunkColumnIds(
    const catalog::Table& table);

 private:
  duckdb::ErrorData AppendImpl(duckdb::DataChunk& chunk,
                               duckdb::Vector& row_ids);

  // Replay path: a transaction held open across one ApplyBufferedReplays
  // pass, feeding the shard delete-then-insert (idempotent against postings
  // iresearch already made durable ahead of the last checkpoint), committed
  // once in FinishReplay. Built lazily on the first replayed operation.
  struct ReplaySession;
  ReplaySession& EnsureReplaySession();
  void ReplayAppend(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);
  void ReplayDelete(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);

  // Minimal IndexStorageInfo (catalog ids; postings live in the iresearch
  // shard's own files). Shared by SerializeToDisk/SerializeToWAL.
  duckdb::IndexStorageInfo MakeStorageInfo() const;
  // Force the shard durable before a checkpoint truncates the store WAL; veto
  // (throw) if the shard is out of sync. No-op at CREATE INDEX time.
  void CheckpointBarrier() const;

  ObjectId _table_id;
  ObjectId _index_id;
  std::unique_ptr<ReplaySession> _replay;
  // Store-WAL byte offset of the replay range currently being delivered by
  // ApplyBufferedReplays (set by OnReplayRange). 0 = unknown (don't skip).
  duckdb::idx_t _replay_commit_offset = 0;
};

// Attaches create_instance + the build pipeline for store-table CREATE
// INDEX to the registered "inverted" index type.
void AttachInvertedStoreIndexCallbacks(duckdb::IndexType& type);

}  // namespace sdb::connector
