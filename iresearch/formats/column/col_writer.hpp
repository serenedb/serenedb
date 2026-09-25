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

#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/async/future.hpp>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class AnnWriter;
class IdxWriter;
struct AnnBuildEnv;

class ColWriter final {
 public:
  ColWriter(Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db, WriteTier tier = WriteTier::Flush);
  ~ColWriter();

  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  void SetFieldOptions(const IndexFieldOptions* field_options) noexcept;

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type);

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           bool skip_validity, uint32_t row_group_size,
                           duckdb::CompressionType compression =
                             duckdb::CompressionType::COMPRESSION_AUTO,
                           bool hyperloglog = false,
                           ColCodecParams codec_params = {});

  AnnWriter& AttachAnn(field_id column_id, AnnInfo info);

  void SetIdxWriter(IdxWriter& idx) noexcept;

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  std::span<const std::unique_ptr<NormColumnWriter>> NormWriters()
    const noexcept {
    return _norm_writers;
  }

  std::vector<std::unique_ptr<AnnWriter>> TakeAnnWriters() noexcept;

  bool Commit(uint64_t target_row);
  bool Commit(uint64_t target_row, absl::FunctionRef<bool()> progress);

  yaclib::Task<bool> ComputeAnn(const AnnBuildEnv* env);
  yaclib::Task<bool> ComputeAnn(const AnnBuildEnv* env,
                                absl::FunctionRef<bool()> progress);

  void Rollback() noexcept;

  WriteContext& WriteCtx() const noexcept { return *_write_ctx; }
  IndexOutput& Out() const noexcept { return *_out; }

 private:
  struct AnnEntry {
    field_id column_id;
    AnnInfo info;
    std::unique_ptr<AnnWriter> writer;
  };

  void EnsureOut();
  bool Empty() const noexcept;
  ColumnWriter& OpenColumnInternal(field_id id, duckdb::LogicalType type,
                                   bool skip_validity, uint32_t row_group_size,
                                   duckdb::CompressionType forced,
                                   bool hyperloglog,
                                   ColCodecParams codec_params);

  Directory* _dir;
  std::string _segment_name;
  std::string _filename;
  duckdb::DatabaseInstance* _db;
  WriteTier _tier;
  const IndexFieldOptions* _field_options = nullptr;
  IndexOutput::ptr _out;
  std::unique_ptr<WriteContext> _write_ctx;
  std::vector<std::unique_ptr<ColumnWriter>> _columns;
  irs::containers::FlatHashMap<field_id, ColumnWriter*> _by_id;
  std::vector<std::unique_ptr<NormColumnWriter>> _norm_writers;
  irs::containers::FlatHashMap<field_id, NormColumnWriter*> _norm_by_id;
  std::vector<std::unique_ptr<AnnEntry>> _ann_writers;
  irs::containers::FlatHashMap<field_id, AnnEntry*> _ann_by_id;
  bool _committed = false;
};

}  // namespace irs
