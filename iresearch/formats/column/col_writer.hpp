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
#include <duckdb/common/types/vector.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/async/future.hpp>

#include "iresearch/formats/ann_build_env.hpp"
#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/col_file_writer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class ColWriter final {
 public:
  ColWriter(Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db, WriteTier tier = WriteTier::Flush);
  ~ColWriter();

  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  void SetFieldOptions(const IndexFieldOptions* field_options) noexcept;

  void SetHnswBases(HnswBases* bases) noexcept { _hnsw_bases = bases; }

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type);

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           ColumnWriterOptions options);

  AnnWriter& AttachAnn(field_id column_id, AnnInfo info);

  void SetIdxWriter(IdxWriter& idx) noexcept;

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  NormColumnWriter& StreamNormColumn(field_id id);

  std::vector<std::unique_ptr<AnnWriter>> TakeAnnWriters() noexcept;

  bool Commit(uint64_t target_row);
  bool Commit(uint64_t target_row, absl::FunctionRef<bool()> progress);

  yaclib::Task<bool> ComputeAnn(const AnnBuildEnv* env);
  yaclib::Task<bool> ComputeAnn(const AnnBuildEnv* env,
                                absl::FunctionRef<bool()> progress);

  void Rollback() noexcept;

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

  Directory* _dir;
  std::string _segment_name;
  duckdb::DatabaseInstance* _db;
  WriteTier _tier;
  const IndexFieldOptions* _field_options = nullptr;
  HnswBases* _hnsw_bases = nullptr;
  std::unique_ptr<ColFileWriter> _file;
  std::unique_ptr<WriteContext> _write_ctx;
  std::vector<ColumnEntry> _columns;
  irs::containers::FlatHashMap<field_id, size_t> _by_id;
  std::vector<std::unique_ptr<NormColumnWriter>> _norms;
  irs::containers::FlatHashMap<field_id, NormColumnWriter*> _norm_by_id;
  std::vector<AnnEntry> _ann_writers;
  irs::containers::FlatHashMap<field_id, size_t> _ann_by_id;
  bool _committed = false;
};

}  // namespace irs
