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

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "basics/message_buffer.h"
#include "pg/deserialize.h"
#include "pg/pg_types.h"
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
    _ctx.types_cache = std::make_unique<sdb::pg::TypesSerializationCache>();
  }

  // Run one column serializer through a Writer (the wire layer's per-row unit)
  // and return the drained bytes. flush_size=1 means the Commit flushes
  // straight into `_collected`.
  std::string RenderRow(const sdb::pg::SerializationFunction& fn,
                        const duckdb::RecursiveUnifiedVectorFormat& fmt,
                        duckdb::idx_t row) {
    {
      sdb::message::Writer writer{*_buffer};
      _ctx.writer = &writer;
      fn(_ctx, fmt, row);
      writer.Commit(/*need_flush=*/true);
    }
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

  // Serialize one row of `value` (held in a flat 1-row vector of `type`) in the
  // given format and return the raw int32-length-framed wire bytes.
  std::string SerializeOne(const duckdb::LogicalType& type,
                           const duckdb::Value& value,
                           sdb::pg::VarFormat format) {
    duckdb::Vector vec(type, /*capacity=*/1);
    vec.SetValue(0, value);
    duckdb::RecursiveUnifiedVectorFormat fmt;
    duckdb::Vector::RecursiveToUnifiedFormat(vec, 1, fmt);
    auto fn = sdb::pg::GetSerialization(type, format, _ctx);
    EXPECT_NE(fn, nullptr);
    return RenderRow(fn, fmt, 0);
  }

  // Decode the int32-length-framed wire body back into a duckdb::Value of
  // `type`, using the same GetDeserialization the wire layer uses.
  static duckdb::Value Deserialize(const duckdb::LogicalType& type,
                                   std::string_view framed,
                                   sdb::pg::VarFormat format) {
    EXPECT_GE(framed.size(), 4u);
    const auto body = framed.substr(4);
    sdb::pg::DeserializeContext dctx{};
    duckdb::Value out(type);
    sdb::pg::ValueSink sink{type, out};
    auto fn = sdb::pg::GetDeserialization<sdb::pg::ValueSink>(type, format);
    EXPECT_NE(fn, nullptr);
    EXPECT_TRUE(fn(dctx, body, sink));
    return out;
  }

  static int32_t LoadBE32(std::string_view s, size_t at) {
    return absl::big_endian::Load<int32_t>(s.data() + at);
  }
};

// LIST(INT) value from a vector of optional<int32>.
duckdb::Value IntList(const std::vector<std::optional<int32_t>>& xs) {
  duckdb::vector<duckdb::Value> vals;
  for (const auto& x : xs) {
    vals.push_back(x ? duckdb::Value::INTEGER(*x)
                     : duckdb::Value(duckdb::LogicalType::INTEGER));
  }
  return duckdb::Value::LIST(duckdb::LogicalType::INTEGER, std::move(vals));
}

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
    EXPECT_EQ(StripLengthPrefix(RenderRow(fn, fmt, i)), "{42,43,44}")
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
  EXPECT_EQ(StripLengthPrefix(RenderRow(fn, fmt, 0)), "{0,1,2}");
  EXPECT_EQ(StripLengthPrefix(RenderRow(fn, fmt, 1)), "{10,11,12}");
  EXPECT_EQ(StripLengthPrefix(RenderRow(fn, fmt, 2)), "{20,21,22}");
  EXPECT_EQ(StripLengthPrefix(RenderRow(fn, fmt, 3)), "{30,31,32}");
}

// A ragged LIST(LIST(INT)) [[1],[2,3]] has no PG rectangular multi-dim
// representation, so binary serialization throws; text renders it as nested
// braces.
TEST_F(PgSerializeArrayTest, RaggedNestedListThrowsInBinaryRendersInText) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER));
  const auto value =
    duckdb::Value::LIST(duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER),
                        {IntList({1}), IntList({2, 3})});

  // Text first (clean buffer): nested braces handle the ragged shape.
  EXPECT_EQ(
    StripLengthPrefix(SerializeOne(type, value, sdb::pg::VarFormat::Text)),
    "{{1},{2,3}}");

  // Binary throws; any uncommitted bytes left behind are dropped at teardown.
  duckdb::Vector vec(type, /*capacity=*/1);
  vec.SetValue(0, value);
  duckdb::RecursiveUnifiedVectorFormat fmt;
  duckdb::Vector::RecursiveToUnifiedFormat(vec, 1, fmt);
  auto fn = sdb::pg::GetSerialization(type, sdb::pg::VarFormat::Binary, _ctx);
  ASSERT_NE(fn, nullptr);
  sdb::message::Writer writer{*_buffer};
  _ctx.writer = &writer;
  EXPECT_ANY_THROW(fn(_ctx, fmt, 0));
}

// [[]] (one empty sub-list) must stay distinct from [] (empty outer): the outer
// has ndim=1 size=1 with one inner ndim=0 element; the empty outer has ndim=0.
// Both must round-trip to their own shape.
TEST_F(PgSerializeArrayTest, EmptyInnerVsEmptyOuterDistinct) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER));
  const auto inner_empty = duckdb::Value::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), {IntList({})});
  const auto outer_empty = duckdb::Value::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), {});

  const auto fi = SerializeOne(type, inner_empty, sdb::pg::VarFormat::Binary);
  const auto fo = SerializeOne(type, outer_empty, sdb::pg::VarFormat::Binary);
  EXPECT_NE(fi, fo);
  // [[]] is rectangular -> multi-dim ndim=2 with dims [1,0] (one empty inner);
  // [] is the empty outer -> ndim=0.
  EXPECT_EQ(LoadBE32(std::string_view{fi}.substr(4), 0), 2);
  EXPECT_EQ(LoadBE32(std::string_view{fo}.substr(4), 0), 0);

  EXPECT_EQ(Deserialize(type, fi, sdb::pg::VarFormat::Binary).ToString(),
            inner_empty.ToString());
  EXPECT_EQ(Deserialize(type, fo, sdb::pg::VarFormat::Binary).ToString(),
            outer_empty.ToString());
}

// A NULL sub-list [[1],NULL,[2]] cannot sit in a PG rectangular multi-dim array
// (only leaf elements may be NULL), so binary serialization throws; text
// renders the NULL inline.
TEST_F(PgSerializeArrayTest, NullSubListThrowsInBinaryRendersInText) {
  const auto inner = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  const auto type = duckdb::LogicalType::LIST(inner);
  const auto value = duckdb::Value::LIST(
    inner, {IntList({1}), duckdb::Value(inner), IntList({2})});

  // Text first (clean buffer).
  EXPECT_EQ(
    StripLengthPrefix(SerializeOne(type, value, sdb::pg::VarFormat::Text)),
    "{{1},NULL,{2}}");

  // Binary throws.
  duckdb::Vector vec(type, /*capacity=*/1);
  vec.SetValue(0, value);
  duckdb::RecursiveUnifiedVectorFormat fmt;
  duckdb::Vector::RecursiveToUnifiedFormat(vec, 1, fmt);
  auto fn = sdb::pg::GetSerialization(type, sdb::pg::VarFormat::Binary, _ctx);
  ASSERT_NE(fn, nullptr);
  sdb::message::Writer writer{*_buffer};
  _ctx.writer = &writer;
  EXPECT_ANY_THROW(fn(_ctx, fmt, 0));
}

// All-ARRAY fixed nested ARRAY(ARRAY(INT,2),2) stays on the multi-dim path:
// rectangular ndim=2 header with the scalar leaf OID (int4, not _int4).
TEST_F(PgSerializeArrayTest, AllArrayFixedNestedIsMultiDim) {
  const auto inner =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::INTEGER, 2);
  const auto type = duckdb::LogicalType::ARRAY(inner, 2);
  const auto value = duckdb::Value::ARRAY(
    inner, {duckdb::Value::ARRAY(
              duckdb::LogicalType::INTEGER,
              {duckdb::Value::INTEGER(1), duckdb::Value::INTEGER(2)}),
            duckdb::Value::ARRAY(
              duckdb::LogicalType::INTEGER,
              {duckdb::Value::INTEGER(3), duckdb::Value::INTEGER(4)})});

  const auto framed = SerializeOne(type, value, sdb::pg::VarFormat::Binary);
  const auto body = std::string_view{framed}.substr(4);
  EXPECT_EQ(LoadBE32(body, 0), 2);  // ndim=2 (rectangular flatten)
  // Multi-dim uses the SCALAR leaf OID (int4=23), not the array OID.
  EXPECT_EQ(LoadBE32(body, 8), sdb::pg::Type2Oid(duckdb::LogicalType::INTEGER,
                                                 /*in_array=*/false));
  EXPECT_EQ(LoadBE32(body, 12), 2);  // dim0
  EXPECT_EQ(LoadBE32(body, 20), 2);  // dim1
}

}  // namespace
