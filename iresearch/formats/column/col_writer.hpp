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

#include <cstddef>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <duckdb/common/types/vector.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/async/future.hpp>

#include "iresearch/formats/ann_build_env.hpp"
#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/flush_state.hpp"
#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs {

AnnFlushBound AnnBound(const AnnInfo& info, uint64_t rows);

class ColWriter final {
 public:
  ColWriter(duckdb::DatabaseInstance& db, FileTracker& tracker,
            WriteTier tier = WriteTier::Flush);
  ~ColWriter();

  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  void SetFieldOptions(const IndexFieldOptions* field_options) noexcept;

  void SetHnswBases(HnswBases* bases) noexcept { _hnsw_bases = bases; }

  void SetBudget(MemoryBudget& budget) noexcept { _budget = &budget; }
  MemoryBudget& Budget() const noexcept { return *_budget; }

  static constexpr size_t kSealBound =
    2 * duckdb::Storage::MAX_BLOCK_ALLOC_SIZE;

  void SetDeferredSeal(bool deferred) noexcept {
    SDB_ASSERT(_columns.empty());
    _deferred = deferred;
  }

  void SetRowGroupStarts(std::vector<uint64_t> starts) noexcept {
    SDB_ASSERT(_columns.empty());
    _row_group_starts = std::move(starts);
  }

  uint64_t RowGroupRows(uint64_t row, uint64_t uniform) const noexcept;

  std::vector<StagedColumn> Snapshot(uint64_t rows) const;
  std::vector<NormColumnSpec> SealNorms(uint64_t rows);
  size_t StagedBytes() const noexcept;

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type);

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           ColumnWriterOptions options);

  AnnWriter& AttachAnn(field_id column_id, AnnInfo info);

  bool HasAnn(field_id column_id) const noexcept;

  void SetIdxWriter(IdxWriter& idx) noexcept;

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  NormColumnWriter& StreamNormColumn(field_id id);

  std::vector<std::unique_ptr<AnnWriter>> TakeAnnWriters() noexcept;

  AnnFlushBound AnnBound(uint64_t rows) const;

  std::unique_ptr<ColReader> TakeReader() noexcept {
    return std::move(_reader);
  }

  bool Commit(uint64_t target_row);
  bool Commit(uint64_t target_row, absl::FunctionRef<bool()> progress);

  yaclib::Task<bool> ComputeAnn(const AnnBuildEnv* env);
  yaclib::Task<bool> ComputeAnn(const AnnBuildEnv* env,
                                absl::FunctionRef<bool()> progress);

  void Rollback() noexcept;

  WriteContext& WriteCtx() const noexcept { return *_write_ctx; }
  duckdb::WriteStream& Out() const noexcept { return *_out; }
  duckdb::DatabaseInstance& Database() const noexcept { return *_db; }

 private:
  struct AnnEntry {
    field_id column_id;
    AnnInfo info;
    std::unique_ptr<AnnWriter> writer;
  };

  struct ColumnEntry {
    std::unique_ptr<ColumnWriter> writer;
    ColumnMeta meta;
  };

  static void Close(ColumnEntry& entry);

  void EnsureOut();
  NormColumnWriter& AddNormColumn(field_id id, uint32_t row_group_size);
  bool Empty() const noexcept;

  FileTracker& _tracker;
  std::string _filename;
  duckdb::DatabaseInstance* _db;
  WriteTier _tier;
  const IndexFieldOptions* _field_options = nullptr;
  HnswBases* _hnsw_bases = nullptr;
  duckdb::TempBufferPoolReservation _own_budget{EmptyBudget()};
  MemoryBudget* _budget = &_own_budget;
  std::unique_ptr<duckdb::BufferedFileWriter> _out;
  std::unique_ptr<ColReader> _reader;
  std::unique_ptr<WriteContext> _write_ctx;
  std::vector<ColumnEntry> _columns;
  irs::containers::FlatHashMap<field_id, size_t> _by_id;
  std::vector<std::unique_ptr<NormColumnWriter>> _norms;
  irs::containers::FlatHashMap<field_id, NormColumnWriter*> _norm_by_id;
  std::vector<AnnEntry> _ann_writers;
  irs::containers::FlatHashMap<field_id, size_t> _ann_by_id;
  std::vector<uint64_t> _row_group_starts;
  bool _committed = false;
  bool _deferred = false;
};

}  // namespace irs
