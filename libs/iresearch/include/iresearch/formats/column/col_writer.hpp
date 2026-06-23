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

#include <absl/functional/any_invocable.h>

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class Directory;

class ColumnReader;
class ColumnWriter;
class NormColumnWriter;
class ReadContext;

class ColWriter final {
 public:
  ColWriter(Directory& dir, std::string_view segment_name,
            duckdb::DatabaseInstance& db,
            const ColumnOptionsProvider* column_options = nullptr,
            const NormColumnOptionsProvider* norm_column_options = nullptr);
  ~ColWriter();

  ColWriter(const ColWriter&) = delete;
  ColWriter& operator=(const ColWriter&) = delete;

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type);

  ColumnWriter& OpenColumn(field_id id, duckdb::LogicalType type,
                           bool skip_validity, uint32_t row_group_size,
                           duckdb::CompressionType compression,
                           bool hyperloglog);

  NormColumnWriter* OpenNormColumn(field_id id);

  NormColumnWriter& OpenNormColumn(field_id id, uint32_t row_group_size);

  std::span<const std::unique_ptr<NormColumnWriter>> NormWriters()
    const noexcept;

  using CommitHook =
    absl::AnyInvocable<void(ColWriter&, std::span<const field_id>)>;
  void SetCommitHook(CommitHook hook);

  std::unique_ptr<ColumnReader> ReopenColumn(field_id id) const;
  ReadContext& CommitReadContext() noexcept;

  void Commit(uint64_t target_row);
  void Rollback() noexcept;

 private:
  void EnsureOut();
  bool Empty() const noexcept;

  struct Impl;
  std::unique_ptr<Impl> _impl;
};

}  // namespace irs
