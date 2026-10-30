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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class ColWriter;
struct SortSpec;

struct DocOrder {
  std::vector<uint32_t> new2old;
  std::vector<uint32_t> old2new;

  bool Empty() const noexcept { return new2old.empty(); }

  doc_id_t NewDoc(size_t row) const noexcept {
    return static_cast<doc_id_t>(Empty() ? row : old2new[row]) +
           doc_limits::min();
  }

  size_t OldRow(doc_id_t doc) const noexcept {
    const size_t row = doc - doc_limits::min();
    return Empty() ? row : new2old[row];
  }

  void MaskRows(DocumentMaskBuilder& mask, size_t from, size_t to) const;
};

std::vector<uint32_t> FlushOrder(duckdb::DatabaseInstance& db,
                                 const SortSpec& spec, ColWriter& columns,
                                 uint64_t rows);

DocOrder MakeDocOrder(std::vector<uint32_t> new2old);

}  // namespace irs
