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

// Regression coverage for server/pg/serialize.cpp:GetArraySlice.
//
// The bug: for ARRAY-typed parents, the child slot was computed as
// `row * size` (output position) instead of `parent.sel->get_index(row) *
// size` (sel-translated source row). Wrong for any ARRAY parent whose
// unified format has a non-identity sel -- CONSTANT (all rows -> source
// row 0) and DICTIONARY (each row -> permuted source row).
//
// SQL-level repros are unreliable because DuckDB usually flattens these
// vectors before they reach the wire layer. These tests build the parent
// vectors directly so the bug is reachable. Note: Vector::Slice on an
// array buffer flattens via VectorArrayBuffer::FlattenSlice, but
// Vector::Dictionary(reusable_dict, sel, count) bypasses Slice and
// installs a DictionaryBuffer directly -- DebugTransformToDictionary
// uses this and works for ARRAY (only STRUCT is rejected at vector.cpp:224).

#include <gtest/gtest.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <memory>
#include <string>

#include "basics/message_buffer.h"
#include "pg/serialize.h"

namespace {

class PgSerializeArrayTest : public ::testing::Test {
 protected:
  std::string _collected;
  std::unique_ptr<sdb::message::Buffer> _buffer;
  sdb::pg::SerializationContext _ctx;

  void SetUp() override {
    // flush_size=1 so every Commit(true) drains via the send callback into
    // `_collected`. types_cache is only consulted by STRUCT serialization;
    // initialize it for safety even though our test types don't need it.
    _buffer = std::make_unique<sdb::message::Buffer>(
      /*min_growth=*/64, /*max_growth=*/4096, /*flush_size=*/1,
      [this](sdb::message::SequenceView view) {
        for (auto buf : view) {
          _collected.append(static_cast<const char*>(buf.data()), buf.size());
        }
      });
    _ctx.buffer = _buffer.get();
    _ctx.types_cache = std::make_unique<sdb::pg::TypesSerializationCache>();
  }

  // Drain one logical "row's worth" of writes from the buffer into a
  // returned string and clear `_collected` for the next row.
  std::string DrainRow() {
    _buffer->Commit(/*need_flush=*/true);
    _buffer->FlushDone();
    auto out = _collected;
    _collected.clear();
    return out;
  }

  // Each text serialization writes a 4-byte big-endian length prefix then
  // the payload. Strip the prefix for assertion.
  static std::string StripLengthPrefix(std::string_view raw) {
    if (raw.size() < 4) {
      return std::string(raw);
    }
    return std::string(raw.substr(4));
  }
};

TEST_F(PgSerializeArrayTest, ConstantArrayReadsSourceRowZeroForAllRows) {
  // CONSTANT ARRAY: parent vector has Size()==1 in source space, sel maps
  // every output row to source row 0. Under the old `row * size` code,
  // output row 1+ would index past the child storage end (child has only
  // `array_size` elements). With the fix, all output rows resolve to
  // source row 0 -> the same array contents.
  const auto array_type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::INTEGER, 3);
  duckdb::Vector constant_array(array_type, /*capacity=*/1);
  constant_array.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  auto& child = duckdb::ArrayVector::GetChildMutable(constant_array);
  auto* data = duckdb::FlatVector::GetDataMutable<int32_t>(child);
  data[0] = 42;
  data[1] = 43;
  data[2] = 44;
  ASSERT_EQ(constant_array.GetVectorType(),
            duckdb::VectorType::CONSTANT_VECTOR);

  duckdb::RecursiveUnifiedVectorFormat fmt;
  // Tell the unify path we have N logical rows to serialize from this
  // constant source. The constant vector layout itself is independent of
  // N -- only the produced sel covers row 0..N-1, each mapping to 0.
  constexpr duckdb::idx_t kRows = 5;
  duckdb::Vector::RecursiveToUnifiedFormat(constant_array, kRows, fmt);

  auto fn =
    sdb::pg::GetSerialization(array_type, sdb::pg::VarFormat::Text, _ctx);
  ASSERT_NE(fn, nullptr);

  for (duckdb::idx_t i = 0; i < kRows; ++i) {
    fn(_ctx, fmt, i);
    EXPECT_EQ(StripLengthPrefix(DrainRow()), "{42,43,44}")
      << "row " << i
      << " (constant array should read source row 0 for all "
         "logical rows; old `i * array_size` read off the end of child "
         "storage)";
  }
}

TEST_F(PgSerializeArrayTest, DictionaryArrayPreservesLogicalContents) {
  // Build a flat ARRAY<INTEGER,3> with 4 source rows, then wrap it in a
  // DICTIONARY via Vector::DebugTransformToDictionary -- that goes through
  // Vector::Dictionary(reusable_dict, sel, count) which installs a
  // DictionaryBuffer directly (bypassing Vector::Slice's FlattenSlice
  // path). The transform is a debug round-trip: logical contents must
  // stay identical. Under the buggy code, `row * array_size` reads
  // positions [0..3] of the inverted dict storage (junk content with the
  // wrong row mapping); the fix's sel-translated path picks the actual
  // source-row slot.
  const auto array_type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::INTEGER, 3);
  duckdb::Vector source(array_type, /*capacity=*/4);
  auto& child = duckdb::ArrayVector::GetChildMutable(source);
  auto* data = duckdb::FlatVector::GetDataMutable<int32_t>(child);
  // source row k -> {10k, 10k+1, 10k+2}.
  for (duckdb::idx_t k = 0; k < 4; ++k) {
    data[k * 3 + 0] = static_cast<int32_t>(10 * k);
    data[k * 3 + 1] = static_cast<int32_t>(10 * k + 1);
    data[k * 3 + 2] = static_cast<int32_t>(10 * k + 2);
  }
  duckdb::FlatVector::SetSize(source, 4);

  duckdb::Vector::DebugTransformToDictionary(source);
  ASSERT_EQ(source.GetVectorType(), duckdb::VectorType::DICTIONARY_VECTOR);

  duckdb::RecursiveUnifiedVectorFormat fmt;
  duckdb::Vector::RecursiveToUnifiedFormat(source, fmt);

  auto fn =
    sdb::pg::GetSerialization(array_type, sdb::pg::VarFormat::Text, _ctx);
  ASSERT_NE(fn, nullptr);

  // Logical row i must still serialize as source row i's contents.
  fn(_ctx, fmt, 0);
  EXPECT_EQ(StripLengthPrefix(DrainRow()), "{0,1,2}");
  fn(_ctx, fmt, 1);
  EXPECT_EQ(StripLengthPrefix(DrainRow()), "{10,11,12}");
  fn(_ctx, fmt, 2);
  EXPECT_EQ(StripLengthPrefix(DrainRow()), "{20,21,22}");
  fn(_ctx, fmt, 3);
  EXPECT_EQ(StripLengthPrefix(DrainRow()), "{30,31,32}");
}

}  // namespace
