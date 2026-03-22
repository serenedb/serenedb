////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <velox/common/memory/MemoryPool.h>
#include <velox/connectors/Connector.h>
#include <velox/type/Type.h>
#include <velox/vector/FlatVector.h>

#include <optional>
#include <string>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"

namespace sdb::connector {

struct WALRecoveryRange {
  rocksdb::SequenceNumber start_sequence = 0;
  rocksdb::SequenceNumber end_sequence = 0;

  bool IsEmpty() const noexcept { return start_sequence >= end_sequence; }
};

/// DataSource that reads rows from the RocksDB WAL between two sequence
/// numbers. Used during index recovery to replay only the delta that the
/// index missed, instead of scanning the entire table.
///
/// WAL entries are per-column PutCF/DeleteCF calls. This source groups them
/// by primary key, reassembles complete rows, and emits RowVectorPtr batches
/// through the standard DataSource::next() interface.
class WALDataSource final : public velox::connector::DataSource {
 public:
  WALDataSource(velox::memory::MemoryPool& memory_pool,
                rocksdb::TransactionDB& db,
                rocksdb::ColumnFamilyHandle& cf, velox::RowTypePtr read_type,
                ObjectId object_key,
                std::vector<catalog::Column::Id> column_ids,
                WALRecoveryRange range);

  void addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  std::optional<velox::RowVectorPtr> next(
    uint64_t size, velox::ContinueFuture& future) override;

  void addDynamicFilter(
    velox::column_index_t,
    const std::shared_ptr<velox::common::Filter>&) override {}

  uint64_t getCompletedBytes() override { return 0; }
  uint64_t getCompletedRows() override { return _produced; }
  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
    override {
    return {};
  }
  void cancel() override {}

 private:
  /// Inner WriteBatch handler that filters and collects entries for our table.
  class WALBatchHandler final : public rocksdb::WriteBatch::Handler {
   public:
    WALBatchHandler(WALDataSource& source) : _source{source} {}

    rocksdb::Status PutCF(uint32_t column_family_id,
                          const rocksdb::Slice& key,
                          const rocksdb::Slice& value) override;

    rocksdb::Status DeleteCF(uint32_t column_family_id,
                             const rocksdb::Slice& key) override;

    rocksdb::Status SingleDeleteCF(uint32_t column_family_id,
                                   const rocksdb::Slice& key) override;

    rocksdb::Status DeleteRangeCF(uint32_t, const rocksdb::Slice&,
                                  const rocksdb::Slice&) override {
      return rocksdb::Status::OK();
    }

    void LogData(const rocksdb::Slice&) override {}

    rocksdb::Status MarkNoop(bool) override {
      return rocksdb::Status::OK();
    }

    rocksdb::Status MarkBeginPrepare(bool = false) override {
      return rocksdb::Status::OK();
    }

    rocksdb::Status MarkEndPrepare(const rocksdb::Slice&) override {
      return rocksdb::Status::OK();
    }

    rocksdb::Status MarkCommit(const rocksdb::Slice&) override {
      return rocksdb::Status::OK();
    }

    rocksdb::Status MarkRollback(const rocksdb::Slice&) override {
      return rocksdb::Status::OK();
    }

   private:
    WALDataSource& _source;
  };

  struct RowAccumulator {
    std::vector<std::string> column_values;
    std::vector<bool> column_present;
    bool deleted = false;
  };

  // Check if key belongs to our table and default CF
  bool IsOurKey(uint32_t column_family_id,
                const rocksdb::Slice& key) const;

  // Extract Column::Id from a full RocksDB key
  catalog::Column::Id ExtractColumnId(const rocksdb::Slice& key) const;

  // Extract PK portion from a full RocksDB key
  std::string ExtractPK(const rocksdb::Slice& key) const;

  // Find column index in _column_ids for a given Column::Id
  std::optional<size_t> FindColumnIndex(catalog::Column::Id col_id) const;

  // Process a PutCF entry: store value in accumulator
  void HandlePut(const rocksdb::Slice& key, const rocksdb::Slice& value);

  // Process a DeleteCF entry: mark row as deleted in accumulator
  void HandleDelete(const rocksdb::Slice& key);

  // Build a RowVectorPtr from accumulated rows (up to max_rows)
  velox::RowVectorPtr FlushAccumulator(uint64_t max_rows);

  // Drain more WAL batches until we have enough rows or WAL is exhausted
  bool DrainWAL(uint64_t target_rows);

  velox::memory::MemoryPool& _memory_pool;
  rocksdb::TransactionDB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
  velox::RowTypePtr _read_type;
  ObjectId _object_key;
  std::vector<catalog::Column::Id> _column_ids;
  WALRecoveryRange _range;

  std::unique_ptr<rocksdb::TransactionLogIterator> _wal_iterator;
  bool _wal_exhausted = false;
  bool _finished = false;
  uint64_t _produced = 0;

  uint32_t _default_cf_id = 0;

  // PK -> accumulated column values
  // Using map to keep insertion order deterministic
  std::map<std::string, RowAccumulator> _pending_rows;

  std::shared_ptr<velox::connector::ConnectorSplit> _current_split;
};

}  // namespace sdb::connector
