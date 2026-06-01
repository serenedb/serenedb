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
////////////////////////////////////////////////////////////////////////////////

#include "formats/column/test_cs_helpers.hpp"

#include <gtest/gtest.h>

#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>

#include "basics/assert.h"
#include "iresearch/utils/type_limits.hpp"
#include "query/duckdb_engine.h"

namespace irs::tests {

std::unique_ptr<columnstore::Writer> MakeCsWriter(
  Directory& dir, std::string_view segment_name) {
  return std::make_unique<columnstore::Writer>(
    dir, segment_name, ::sdb::query::DuckDBEngine::Instance().instance());
}

std::unique_ptr<columnstore::Reader> MakeCsReader(
  const Directory& dir, std::string_view segment_name) {
  // Reader ctor throws on a missing file; for legacy-style "absent
  // column = empty index" tests, the caller checks Has*() afterwards.
  // Returning nullptr on a missing .cs file matches the legacy
  // ColumnstoreReader::prepare behaviour (no exception, empty result).
  bool exists = false;
  std::string filename;
  filename.reserve(segment_name.size() + 3);
  filename.append(segment_name);
  filename.append(".cs");
  if (!dir.exists(exists, filename) || !exists) {
    return nullptr;
  }
  return std::make_unique<columnstore::Reader>(
    dir, segment_name, ::sdb::query::DuckDBEngine::Instance().instance());
}

columnstore::ColumnWriter& OpenBlobColumn(columnstore::Writer& w, field_id id) {
  return w.OpenColumn(id, duckdb::LogicalType::BLOB);
}

namespace {

duckdb::Vector MakeOneRowBlobVector() {
  duckdb::Vector v{duckdb::LogicalType::BLOB, /*capacity=*/1};
  return v;
}

}  // namespace

void AppendBlob(columnstore::ColumnWriter& cw, doc_id_t doc,
                bytes_view payload) {
  SDB_ASSERT(doc_limits::valid(doc));
  duckdb::Vector v = MakeOneRowBlobVector();
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
  slots[0] = duckdb::StringVector::AddStringOrBlob(
    v, reinterpret_cast<const char*>(payload.data()), payload.size());
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  const uint64_t row = static_cast<uint64_t>(doc) - doc_limits::min();
  cw.Append(row, v, /*count=*/1);
}

void AppendNullBlob(columnstore::ColumnWriter& cw, doc_id_t doc) {
  SDB_ASSERT(doc_limits::valid(doc));
  duckdb::Vector v = MakeOneRowBlobVector();
  duckdb::FlatVector::SetNull(v, 0, true);
  const uint64_t row = static_cast<uint64_t>(doc) - doc_limits::min();
  cw.Append(row, v, /*count=*/1);
}

bool VisitBlobColumn(const columnstore::Reader& cs_reader,
                     const columnstore::ColumnReader& column,
                     const std::function<bool(doc_id_t, bytes_view)>& visitor) {
  duckdb::Vector batch{duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE};
  columnstore::RgWindow window{};
  columnstore::ReadContext ctx{cs_reader};
  for (uint64_t row_pos = 0; row_pos < column.RowCount();) {
    window = column.Locate(row_pos, window);
    auto seg = column.OpenSegment(window.rg, ctx);
    const auto rg_count = static_cast<duckdb::idx_t>(window.end - window.begin);
    duckdb::ColumnScanState state{nullptr};
    seg->InitializeScan(state);
    duckdb::idx_t scanned = 0;
    while (scanned < rg_count) {
      const auto take =
        std::min<duckdb::idx_t>(rg_count - scanned, STANDARD_VECTOR_SIZE);
      seg->Scan(state, take, batch, 0,
                duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      state.offset_in_column += take;
      const auto* slots = duckdb::FlatVector::GetData<duckdb::string_t>(batch);
      const auto& validity = duckdb::FlatVector::Validity(batch);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto doc =
          static_cast<doc_id_t>(window.begin + scanned + k + doc_limits::min());
        if (!validity.RowIsValid(k)) {
          continue;
        }
        const auto& s = slots[k];
        const bytes_view payload{
          reinterpret_cast<const byte_type*>(s.GetData()),
          static_cast<size_t>(s.GetSize())};
        if (!visitor(doc, payload)) {
          return false;
        }
      }
      scanned += take;
    }
    row_pos = window.end;
  }
  return true;
}

void AssertBlobColumn(const columnstore::Reader& cs_reader,
                      const columnstore::ColumnReader& column,
                      std::span<const ExpectedBlobRow> expected) {
  size_t i = 0;
  VisitBlobColumn(cs_reader, column, [&](doc_id_t doc, bytes_view payload) {
    if (i >= expected.size()) {
      ADD_FAILURE() << "extra cs row: doc=" << doc
                    << " size=" << payload.size();
      return false;
    }
    EXPECT_EQ(expected[i].doc, doc) << "row " << i;
    EXPECT_EQ(expected[i].payload, payload) << "row " << i;
    ++i;
    return true;
  });
  EXPECT_EQ(i, expected.size());
}

}  // namespace irs::tests
