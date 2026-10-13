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
#include <memory>
#include <span>

#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_meta.hpp"

namespace irs {

struct SubReader;
struct DocRemap;

class ColReader;
class ColWriter;

struct MergeSource {
  const SubReader* reader;
  const ColReader* col_reader;
  uint64_t alive_count;
  const DocRemap* remap = nullptr;
};

class ColumnMerger {
 public:
  ColumnMerger(std::span<const MergeSource> sources,
               const IndexFieldOptions* field_options);
  ~ColumnMerger();

  ColumnMerger(const ColumnMerger&) = delete;
  ColumnMerger& operator=(const ColumnMerger&) = delete;

  std::span<const field_id> Columns() const noexcept;

  uint64_t StoredBytes(field_id column) const;

  uint32_t RowGroupSize() const noexcept;

  bool Splittable(field_id column) const;

  bool IsText(field_id column) const;

  bool Merge(field_id column, ColWriter& output,
             absl::FunctionRef<bool()> progress);

  bool Train(field_id column, ColWriter& output,
             absl::FunctionRef<bool()> progress);

  bool MergeRange(field_id column, uint32_t part, uint64_t begin, uint64_t end,
                  ColWriter& output, absl::FunctionRef<bool()> progress);

 private:
  struct State;
  std::unique_ptr<State> _state;
};

}  // namespace irs
