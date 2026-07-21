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
#include <duckdb/common/types/vector.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"

namespace irs {

class IvfWriter;

class ColWriter final {
 public:
  ColWriter(Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db);
  ~ColWriter();

  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  void SetFieldOptions(const IndexFieldOptions* field_options) noexcept;

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type);

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           bool skip_validity, uint32_t row_group_size,
                           duckdb::CompressionType compression =
                             duckdb::CompressionType::COMPRESSION_AUTO,
                           bool hyperloglog = false);

  IvfWriter& AttachIVF(field_id column_id, IvfInfo info);

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  std::span<const std::unique_ptr<NormColumnWriter>> NormWriters()
    const noexcept {
    return _norm_writers;
  }

  std::vector<std::unique_ptr<IvfWriter>> TakeIvfWriters() noexcept;

  void Commit(uint64_t target_row);

  void Rollback() noexcept;

  WriteContext& WriteCtx() const noexcept { return *_write_ctx; }
  IndexOutput& Out() const noexcept { return *_out; }

 private:
  struct IvfEntry {
    field_id column_id;
    IvfInfo info;
    std::unique_ptr<IvfWriter> writer;
  };

  void EnsureOut();
  bool Empty() const noexcept;
  ColumnWriter& OpenColumnInternal(field_id id, duckdb::LogicalType type,
                                   bool skip_validity, uint32_t row_group_size,
                                   duckdb::CompressionType forced,
                                   bool hyperloglog);

  Directory* _dir;
  std::string _segment_name;
  std::string _filename;
  duckdb::DatabaseInstance* _db;
  const IndexFieldOptions* _field_options = nullptr;
  IndexOutput::ptr _out;
  std::unique_ptr<WriteContext> _write_ctx;
  std::vector<std::unique_ptr<ColumnWriter>> _columns;
  sdb::containers::FlatHashMap<field_id, ColumnWriter*> _by_id;
  std::vector<std::unique_ptr<NormColumnWriter>> _norm_writers;
  sdb::containers::FlatHashMap<field_id, NormColumnWriter*> _norm_by_id;
  std::vector<std::unique_ptr<IvfEntry>> _ivf_writers;
  sdb::containers::FlatHashMap<field_id, IvfEntry*> _ivf_by_id;
  bool _committed = false;
};

}  // namespace irs
