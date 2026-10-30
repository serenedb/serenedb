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

#include <cstdint>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <string>

#include "gtest/gtest.h"
#include "iresearch/utils/serializer.hpp"

namespace {

struct RecordV1 {
  uint32_t a = 0;
  bool b = false;
  std::string c;
};

struct RecordV2 {
  uint32_t a = 0;
  bool b = false;
  std::string c;
  uint8_t d = 0;
  uint64_t e = 0;
};

template<typename T>
std::string Serialize(const T& value) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer out{stream};
  irs::utils::WriteTuple(out, value);
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

template<typename T>
T Deserialize(const std::string& bytes) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer in{stream};
  T value;
  irs::utils::ReadTuple(in, value);
  return value;
}

}  // namespace

TEST(SerializerTest, AggregateRoundTrip) {
  const auto v = Deserialize<RecordV2>(
    Serialize(RecordV2{.a = 7, .b = true, .c = "seven", .d = 9, .e = 11}));
  EXPECT_EQ(v.a, 7u);
  EXPECT_TRUE(v.b);
  EXPECT_EQ(v.c, "seven");
  EXPECT_EQ(v.d, 9);
  EXPECT_EQ(v.e, 11u);
}

TEST(SerializerTest, AppendedTrailingFieldsReadAsDefaults) {
  const auto v =
    Deserialize<RecordV2>(Serialize(RecordV1{.a = 3, .b = true, .c = "old"}));
  EXPECT_EQ(v.a, 3u);
  EXPECT_TRUE(v.b);
  EXPECT_EQ(v.c, "old");
  EXPECT_EQ(v.d, 0);
  EXPECT_EQ(v.e, 0u);
}

TEST(SerializerTest, ExtraSerializedFieldsAreRejected) {
  EXPECT_ANY_THROW(Deserialize<RecordV1>(
    Serialize(RecordV2{.a = 1, .b = false, .c = "new", .d = 2, .e = 3})));
}
