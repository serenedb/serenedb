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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <iresearch/formats/column/col_block_sink.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/crc.hpp>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {

constexpr std::string_view kFile = "blocks";

std::string ReadAll(const irs::MemoryDirectory& dir) {
  auto in = dir.open(kFile, irs::IOAdvice::NORMAL);
  EXPECT_NE(in, nullptr);
  std::string bytes(in->Length(), '\0');
  in->ReadData(reinterpret_cast<duckdb::data_ptr_t>(bytes.data()),
               bytes.size());
  return bytes;
}

TEST(ColBlockSinkTest, AlignedAppendsStartAtMultiplesOfEight) {
  irs::MemoryDirectory dir;
  auto out = dir.create(kFile);
  ASSERT_NE(out, nullptr);
  irs::ColBlockSink blocks{*out};
  EXPECT_EQ(0U, blocks.Append("abc"));
  const std::string_view parts[] = {"xy", "z"};
  EXPECT_EQ(8U, blocks.AppendAligned(parts));
  EXPECT_EQ(11U, blocks.Position());
  EXPECT_EQ(16U, blocks.AppendAligned(parts));
  const std::string_view nothing[] = {""};
  EXPECT_EQ(24U, blocks.AppendAligned(nothing));
  EXPECT_EQ(24U, blocks.Append("q"));
  EXPECT_EQ(32U, blocks.AppendAligned(parts));
  out.reset();
  std::string expected = "abc";
  expected.append(5, '\0');
  expected += "xyz";
  expected.append(5, '\0');
  expected += "xyz";
  expected.append(5, '\0');
  expected += "q";
  expected.append(7, '\0');
  expected += "xyz";
  EXPECT_EQ(expected, ReadAll(dir));
}

TEST(ColBlockSinkTest, AppendStringWritesItsLengthFirst) {
  irs::MemoryDirectory dir;
  auto out = dir.create(kFile);
  ASSERT_NE(out, nullptr);
  irs::ColBlockSink blocks{*out};
  EXPECT_EQ(0U, blocks.Append("ab"));
  EXPECT_EQ(2U, blocks.AppendString("hello"));
  EXPECT_EQ(11U, blocks.AppendString(""));
  EXPECT_EQ(15U, blocks.Position());
  out.reset();
  const auto bytes = ReadAll(dir);
  ASSERT_EQ(15U, bytes.size());
  uint32_t length = 0;
  std::memcpy(&length, bytes.data() + 2, sizeof(length));
  EXPECT_EQ(5U, length);
  EXPECT_EQ("hello", bytes.substr(6, 5));
  std::memcpy(&length, bytes.data() + 11, sizeof(length));
  EXPECT_EQ(0U, length);
}

TEST(ColBlockSinkTest, ConcurrentAppendsKeepEveryBlockWhole) {
  constexpr size_t kThreads = 8;
  constexpr size_t kBlocks = 500;
  irs::MemoryDirectory dir;
  auto out = dir.create(kFile);
  ASSERT_NE(out, nullptr);
  irs::ColBlockSink blocks{*out};
  std::vector<std::vector<std::pair<uint64_t, std::string>>> written(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (size_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&blocks, &written, t] {
      for (size_t b = 0; b < kBlocks; ++b) {
        std::string head(1 + (t * 7 + b) % 13, static_cast<char>('a' + t));
        std::string tail(1 + b % 5, static_cast<char>('A' + b % 26));
        const std::string_view parts[] = {head, tail};
        const auto offset = blocks.AppendAligned(parts);
        written[t].emplace_back(offset, head + tail);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  out.reset();
  const auto bytes = ReadAll(dir);
  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  for (const auto& blocks_of_thread : written) {
    for (const auto& [offset, block] : blocks_of_thread) {
      ASSERT_EQ(0U, offset % 8) << "offset " << offset;
      ASSERT_LE(offset + block.size(), bytes.size());
      ASSERT_EQ(block, bytes.substr(offset, block.size()))
        << "offset " << offset;
      ranges.emplace_back(offset, offset + block.size());
    }
  }
  std::ranges::sort(ranges);
  for (size_t i = 1; i < ranges.size(); ++i) {
    ASSERT_LE(ranges[i - 1].second, ranges[i].first);
  }
}

TEST(ColBlockSinkTest, WriteContextWritesThroughTheSink) {
  duckdb::DuckDB db;
  irs::MemoryDirectory dir;
  auto out = dir.create(kFile);
  ASSERT_NE(out, nullptr);
  irs::ColBlockSink blocks{*out};
  irs::WriteContext ctx{*db.instance, blocks};
  blocks.Append("ab");
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
  out.reset();
  const auto bytes = ReadAll(dir);
  ASSERT_EQ(20U, bytes.size());
  EXPECT_EQ("abstream", bytes.substr(0, 8));
  uint32_t length = 0;
  std::memcpy(&length, bytes.data() + 8, sizeof(length));
  EXPECT_EQ(8U, length);
  EXPECT_EQ("overflow", bytes.substr(12, 8));
}

TEST(ColBlockSinkTest, TheRunningChecksumCoversEveryAppend) {
  irs::MemoryDirectory dir;
  auto out = dir.create(kFile);
  ASSERT_NE(out, nullptr);
  irs::ColBlockSink blocks{*out};
  blocks.Append("dictionary");
  const std::string_view parts[] = {"block", "-part"};
  blocks.AppendAligned(parts);
  blocks.AppendString("overflow");
  const auto checksum = out->Checksum();
  out.reset();
  const auto bytes = ReadAll(dir);
  irs::Crc32c crc;
  crc.process_bytes(bytes.data(), bytes.size());
  EXPECT_EQ(crc.checksum(), checksum);
}

}  // namespace
