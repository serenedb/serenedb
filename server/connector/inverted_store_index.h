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
namespace duckdb {

class DataTable;

}  // namespace duckdb
namespace sdb::catalog {

class InvertedIndex;
class Table;

}  // namespace sdb::catalog
namespace sdb::connector {

// The inverted index as a first-class index on store tables: postings live
// in the iresearch storage keyed by AppendSigned(rowid) PK bytes, fed at
// COMMIT time with final row ids through the committing connection's
// tokenizer/transaction machinery (see CurrentCommittingContext). The
// catalog InvertedIndex/storage linkage rides the injected ids.
class InvertedStoreIndex final : public duckdb::BoundIndex {
 public:
  static constexpr const char* kTypeName = "inverted";

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
  duckdb::idx_t TryDelete(
    duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
    duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
    duckdb::optional_ptr<duckdb::SelectionVector> non_deleted_sel) override;

  // Payload lives in the iresearch storage: the checkpoint writes no storage
  // info (the index is re-injected from the catalog at attach) but still runs
  // CheckpointBarrier, which forces the storage durable -- or vetoes -- before
  // the store WAL truncates.
  bool IsExternal() const override { return true; }
  void CheckpointBarrier() override;

  // Called by duckdb after every buffered WAL-replay insert/delete for this
  // bind has been delivered (via Append/Delete with no committing context).
  // Commits the accumulated replay transaction into the iresearch storage.
  void FinishReplay() override;

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

  // Replay path: a single iresearch batch held open across one
  // ApplyBufferedReplays pass. Each buffered WAL op is streamed straight into
  // the batch in WAL order on its own strictly-ascending sub-tick (insert ->
  // DuckDBSearchSinkInsertWriter, delete -> a tick-bound Remove), then
  // committed once in FinishReplay with last_tick placing every op above the
  // durable recovery tick. Tick-bound removes give last-op-wins for free (incl.
  // TRUNCATE
  // + rowid reuse), so no dedup is needed. Built lazily on the first replayed
  // operation.
  struct ReplaySession;
  ReplaySession& EnsureReplaySession();
  duckdb::idx_t ReplayCommitOffset() const;
  void ReplayAppend(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);
  void ReplayDelete(duckdb::DataChunk& chunk, duckdb::Vector& row_ids);

  ObjectId _table_id;
  ObjectId _index_id;
  std::unique_ptr<ReplaySession> _replay;
};

// Builds a bound inverted index over store table `storage` for the catalog
// index `inverted`, ready for TableIndexList injection.
duckdb::unique_ptr<InvertedStoreIndex> MakeInjectedInvertedIndex(
  duckdb::DataTable& storage, const catalog::Table& table,
  const catalog::InvertedIndex& inverted);

// DBConfig::external_index_provider target: whenever a fresh store DataTable
// comes alive (attach checkpoint load, WAL-replay CREATE TABLE, reconciler
// recreate), injects every inverted index the catalog records for it --
// before any of the table's WAL operations replay.
void InjectExternalIndexes(duckdb::DataTable& storage);

}  // namespace sdb::connector
