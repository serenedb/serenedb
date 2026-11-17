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

#include "iresearch/formats/ann_entries.hpp"

#include <duckdb/common/vector/flat_vector.hpp>

#include "iresearch/formats/column/list_column_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

constexpr uint64_t kMaxEntries = doc_limits::eof() - doc_limits::min();

}  // namespace

AnnEntries::AnnEntries(const ColumnReader& list, ReadContext& ctx)
  : _row_first(list.RowCount() + 1, 0) {
  const auto& reader = utils::downCast<ListColumnReader>(list);
  const uint64_t rows = list.RowCount();
  ColumnReader::VectorScratch scratch{duckdb::LogicalType::UBIGINT};
  auto scan = list.InitScan(ctx);
  for (uint64_t row = 0; row < rows;) {
    const auto want = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, rows - row));
    auto& ends = scratch.Reset();
    const auto got = reader.ScanEnds(scan, ends, want);
    SDB_ASSERT(got != 0 && got <= want);
    const auto* data = duckdb::FlatVector::GetData<uint64_t>(ends);
    for (duckdb::idx_t i = 0; i < got; ++i) {
      if (data[i] >= kMaxEntries) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        ERR_MSG("A segment of a vector index holds at most ",
                                kMaxEntries, " vectors"));
      }
      _row_first[row + i + 1] = static_cast<uint32_t>(data[i]);
    }
    row += got;
  }
}

}  // namespace irs
