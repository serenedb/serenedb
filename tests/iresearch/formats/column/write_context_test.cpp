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

#include <absl/base/internal/endian.h>

#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <string>
#include <string_view>

#include "tests_param.hpp"
#include "tests_shared.hpp"

namespace {

constexpr std::string_view kFile = "blocks";

class WriteContextTest : public tests::DirectoryTestCaseBase<> {};

TEST_P(WriteContextTest, StreamAndOverflowStringsFollowWhatTheFileHolds) {
  duckdb::DuckDB db;
  auto out = dir().create(kFile);
  ASSERT_NE(out, nullptr);
  {
    irs::WriteContext ctx{*db.instance, *out};
    constexpr std::string_view kHead = "ab";
    out->WriteData(reinterpret_cast<const irs::byte_type*>(kHead.data()),
                   kHead.size());
    EXPECT_EQ(2U, ctx.Position());
    constexpr std::string_view kStream = "stream";
    ctx.Append(reinterpret_cast<duckdb::const_data_ptr_t>(kStream.data()),
               kStream.size());
    EXPECT_EQ(8U, ctx.Position());
    duckdb::UncompressedStringSegmentState state;
    duckdb::block_id_t block = 0;
    int32_t offset = -1;
    ctx.WriteString(state, duckdb::string_t{"overflow"}, block, offset);
    EXPECT_EQ(8, block);
    EXPECT_EQ(0, offset);
    EXPECT_EQ(20U, ctx.Position());
  }
  out.reset();
  auto in = dir().open(kFile, irs::IOAdvice::SEQUENTIAL);
  ASSERT_NE(in, nullptr);
  std::string bytes(in->Length(), '\0');
  in->ReadData(reinterpret_cast<duckdb::data_ptr_t>(bytes.data()),
               bytes.size());
  ASSERT_EQ(20U, bytes.size());
  EXPECT_EQ("abstream", bytes.substr(0, 8));
  EXPECT_EQ(8U, absl::little_endian::Load32(bytes.data() + 8));
  EXPECT_EQ("overflow", bytes.substr(12, 8));
}

constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(WriteContext, WriteContextTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         tests::DirectoryTestCaseBase<>::to_string);

}  // namespace
