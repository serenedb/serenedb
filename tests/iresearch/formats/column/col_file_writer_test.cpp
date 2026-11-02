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

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <filesystem>
#include <iresearch/formats/column/col_file_writer.hpp>
#include <iresearch/formats/column/internal/write_context.hpp>
#include <iresearch/formats/format_utils.hpp>
#include <iresearch/store/fs_directory.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/crc.hpp>
#include <iterator>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "tests_param.hpp"
#include "tests_shared.hpp"

namespace {

constexpr std::string_view kFile = "blocks";

constexpr auto kNoFooter = [](duckdb::BinarySerializer&) {};

std::string ReadAll(irs::Directory& dir) {
  auto in = dir.open(kFile, irs::IOAdvice::SEQUENTIAL);
  EXPECT_NE(in, nullptr);
  std::string bytes(in->Length(), '\0');
  in->ReadData(reinterpret_cast<duckdb::data_ptr_t>(bytes.data()),
               bytes.size());
  return bytes;
}

irs::format_utils::Footer ReadFooter(irs::Directory& dir) {
  auto in = dir.open(kFile, irs::IOAdvice::SEQUENTIAL);
  EXPECT_NE(in, nullptr);
  return irs::format_utils::ReadFooter(*in, kFile);
}

std::string ReadData(irs::Directory& dir) {
  return ReadAll(dir).substr(0, ReadFooter(dir).data_len);
}

class ColFileWriterTest : public tests::DirectoryTestCaseBase<> {};

uint32_t Crc(std::string_view bytes) {
  irs::Crc32c crc;
  crc.process_bytes(bytes.data(), bytes.size());
  return crc.checksum();
}

struct Written {
  uint64_t offset = 0;
  std::string bytes;
  bool aligned = false;
};

constexpr size_t kMixedSizes[] = {
  1, 7, 63, 1000, 4096, 16 << 10, (16 << 10) + 1, 40000, 70000, 200000,
};

constexpr size_t kGrowingSizes[] = {
  0, 1, 4096, 16 << 10, (16 << 10) + 1, 64 << 10, 70000, 3, 200000, 8,
};

std::string Content(size_t thread, size_t block, size_t size) {
  std::string bytes(size, '\0');
  for (size_t i = 0; i != size; ++i) {
    bytes[i] = static_cast<char>(1 + (thread * 131 + block * 31 + i * 7) % 251);
  }
  return bytes;
}

std::vector<Written> AppendConcurrently(irs::ColFileWriter& file,
                                        size_t threads, size_t count) {
  std::vector<std::vector<Written>> written(threads);
  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (size_t t = 0; t != threads; ++t) {
    workers.emplace_back([&file, &written, t, count] {
      for (size_t b = 0; b != count; ++b) {
        const auto size =
          kMixedSizes[(t * 3 + b) % std::size(kMixedSizes)] + b % 5;
        auto bytes = Content(t, b, size);
        if (b % 3 == 0) {
          const std::string_view whole{bytes};
          const std::string_view parts[] = {whole.substr(0, size / 2),
                                            whole.substr(size / 2)};
          const auto offset = file.AppendAligned(parts);
          written[t].emplace_back(offset, std::move(bytes), true);
        } else if (b % 3 == 1) {
          const auto offset = file.Append(bytes);
          written[t].emplace_back(offset, std::move(bytes));
        } else {
          const auto offset = file.AppendString(bytes);
          std::string framed(sizeof(uint32_t), '\0');
          absl::little_endian::Store32(framed.data(),
                                       static_cast<uint32_t>(size));
          framed += bytes;
          written[t].emplace_back(offset, std::move(framed));
        }
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  std::vector<Written> all;
  for (auto& of_thread : written) {
    absl::c_move(of_thread, std::back_inserter(all));
  }
  return all;
}

void ExpectLaidOut(std::string_view file, uint64_t begin, uint64_t end,
                   std::vector<Written> written) {
  absl::c_sort(written, [](const Written& lhs, const Written& rhs) {
    return lhs.offset < rhs.offset;
  });
  uint64_t at = begin;
  for (const auto& block : written) {
    ASSERT_LE(at, block.offset);
    const auto gap = block.offset - at;
    ASSERT_TRUE(gap == 0 || (block.aligned && gap < 8))
      << "gap " << gap << " before " << block.offset;
    ASSERT_EQ(std::string(gap, '\0'), file.substr(at, gap));
    if (block.aligned) {
      ASSERT_EQ(0U, block.offset % 8) << "offset " << block.offset;
    }
    ASSERT_LE(block.offset + block.bytes.size(), end);
    ASSERT_EQ(block.bytes, file.substr(block.offset, block.bytes.size()))
      << "offset " << block.offset;
    at = block.offset + block.bytes.size();
  }
  ASSERT_EQ(end, at);
}

TEST_P(ColFileWriterTest, AlignedAppendsStartAtMultiplesOfEight) {
  irs::ColFileWriter file{dir().create(kFile)};
  EXPECT_EQ(0U, file.Append("abc"));
  const std::string_view parts[] = {"xy", "z"};
  EXPECT_EQ(8U, file.AppendAligned(parts));
  EXPECT_EQ(11U, file.Position());
  EXPECT_EQ(16U, file.AppendAligned(parts));
  const std::string_view nothing[] = {""};
  EXPECT_EQ(24U, file.AppendAligned(nothing));
  EXPECT_EQ(24U, file.Append("q"));
  EXPECT_EQ(32U, file.AppendAligned(parts));
  file.Finish(kNoFooter);
  std::string expected = "abc";
  expected.append(5, '\0');
  expected += "xyz";
  expected.append(5, '\0');
  expected += "xyz";
  expected.append(5, '\0');
  expected += "q";
  expected.append(7, '\0');
  expected += "xyz";
  EXPECT_EQ(expected, ReadData(dir()));
}

TEST_P(ColFileWriterTest, AppendStringWritesItsLengthFirst) {
  irs::ColFileWriter file{dir().create(kFile)};
  EXPECT_EQ(0U, file.Append("ab"));
  EXPECT_EQ(2U, file.AppendString("hello"));
  EXPECT_EQ(11U, file.AppendString(""));
  EXPECT_EQ(15U, file.Position());
  file.Finish(kNoFooter);
  const auto bytes = ReadData(dir());
  ASSERT_EQ(15U, bytes.size());
  uint32_t length = 0;
  std::memcpy(&length, bytes.data() + 2, sizeof(length));
  EXPECT_EQ(5U, length);
  EXPECT_EQ("hello", bytes.substr(6, 5));
  std::memcpy(&length, bytes.data() + 11, sizeof(length));
  EXPECT_EQ(0U, length);
}

TEST_P(ColFileWriterTest, OneProducersAppendsFollowEachOther) {
  irs::ColFileWriter file{dir().create(kFile)};
  EXPECT_EQ(0U, file.Append("head"));
  uint64_t at = file.Position();
  ASSERT_EQ(4U, at);
  std::string expected = "head";
  for (size_t i = 0; i != std::size(kGrowingSizes); ++i) {
    const auto bytes = Content(0, i, kGrowingSizes[i]);
    ASSERT_EQ(at, file.Append(bytes));
    at += bytes.size();
    ASSERT_EQ(at, file.Position());
    expected += bytes;
  }
  file.Finish(kNoFooter);
  EXPECT_EQ(expected, ReadData(dir()));
}

TEST_P(ColFileWriterTest, ConcurrentAppendsKeepEveryBlockWhole) {
  constexpr size_t kThreads = 8;
  constexpr size_t kBlocks = 500;
  irs::ColFileWriter file{dir().create(kFile)};
  std::vector<std::vector<std::pair<uint64_t, std::string>>> written(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (size_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&file, &written, t] {
      for (size_t b = 0; b < kBlocks; ++b) {
        std::string head(1 + (t * 7 + b) % 13, static_cast<char>('a' + t));
        std::string tail(1 + b % 5, static_cast<char>('A' + b % 26));
        const std::string_view parts[] = {head, tail};
        const auto offset = file.AppendAligned(parts);
        written[t].emplace_back(offset, head + tail);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  file.Finish(kNoFooter);
  const auto bytes = ReadData(dir());
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

TEST_P(ColFileWriterTest, ConcurrentProducersWriteEveryByteAndItsChecksum) {
  irs::ColFileWriter file{dir().create(kFile)};
  const auto written = AppendConcurrently(file, 4, 60);
  const auto end = file.Position();
  file.Finish(kNoFooter);
  const auto footer = ReadFooter(dir());
  const auto bytes = ReadData(dir());
  ASSERT_EQ(end, footer.data_len);
  ExpectLaidOut(bytes, 0, end, written);
  EXPECT_EQ(Crc(bytes), footer.data_expected_crc32c);
}

TEST_P(ColFileWriterTest, WriteContextWritesThroughTheFile) {
  duckdb::DuckDB db;
  irs::ColFileWriter file{dir().create(kFile)};
  irs::WriteContext ctx{*db.instance, file};
  file.Append("ab");
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
  file.Finish(kNoFooter);
  const auto bytes = ReadData(dir());
  ASSERT_EQ(20U, bytes.size());
  EXPECT_EQ("abstream", bytes.substr(0, 8));
  uint32_t length = 0;
  std::memcpy(&length, bytes.data() + 8, sizeof(length));
  EXPECT_EQ(8U, length);
  EXPECT_EQ("overflow", bytes.substr(12, 8));
}

TEST_P(ColFileWriterTest, TheFooterChecksumCoversEveryAppend) {
  irs::ColFileWriter file{dir().create(kFile)};
  file.Append("dictionary");
  const std::string_view parts[] = {"block", "-part"};
  file.AppendAligned(parts);
  file.Append(Content(1, 2, 3 * irs::ColFileWriter::kChunkSize + 100000));
  file.AppendString("overflow");
  file.AppendAligned(parts);
  file.Finish(kNoFooter);
  EXPECT_EQ(Crc(ReadData(dir())), ReadFooter(dir()).data_expected_crc32c);
}

TEST_P(ColFileWriterTest, BytesWrittenBeforeTheBlocksKeepTheirPlace) {
  auto out = dir().create(kFile);
  ASSERT_NE(out, nullptr);
  const std::string header = "header";
  out->WriteData(reinterpret_cast<const irs::byte_type*>(header.data()),
                 header.size());
  irs::ColFileWriter file{std::move(out)};
  const auto written = AppendConcurrently(file, 3, 20);
  const auto data_end = file.Position();
  file.Finish(kNoFooter);
  const auto footer = ReadFooter(dir());
  const auto bytes = ReadAll(dir());
  ASSERT_EQ(data_end, footer.data_len);
  EXPECT_EQ(header, bytes.substr(0, header.size()));
  ExpectLaidOut(bytes, header.size(), data_end, written);
  EXPECT_EQ(Crc(std::string_view{bytes}.substr(0, data_end)),
            footer.data_expected_crc32c);
}

TEST(ColFileWriterWriteErrorTest, AFailedWriteReachesItsWriterAndFreesTheRole) {
  if (!std::filesystem::exists("/dev/full")) {
    GTEST_SKIP() << "no /dev/full";
  }
  irs::FSDirectory dev{"/dev"};
  irs::ColFileWriter file{dev.create("full")};
  const auto chunk = Content(0, 0, irs::ColFileWriter::kChunkSize);
  ASSERT_SQL_ERROR(file.Append(chunk), ERRCODE_DISK_FULL);
  ASSERT_SQL_ERROR(file.Append(chunk), ERRCODE_DISK_FULL);
  EXPECT_EQ(2 * irs::ColFileWriter::kChunkSize, file.Position());
}

TEST(ColFileWriterWriterTest,
     AppendsGoOnWhileTheWriterIsHeldUntilTheQueueIsFull) {
  constexpr std::string_view kPoint = "irs::ColFileWriter::Write";
  if (!duckdb::FaultPoints::Arm("irs::ColFileWriter::Write")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const auto parked = absl::StrCat(kPoint, ":parked");
  const absl::Cleanup disarm = [&] {
    duckdb::FaultPoints::Disarm(kPoint);
    duckdb::FaultPoints::Disarm(parked);
  };
  constexpr auto kChunk = irs::ColFileWriter::kChunkSize;
  constexpr auto kChunks = irs::ColFileWriter::kMaxQueued / kChunk + 1;
  irs::MemoryDirectory dir;
  irs::ColFileWriter file{dir.create(kFile)};
  const auto first = Content(0, 0, kChunk);
  std::thread writer{[&] { file.Append(first); }};
  while (!duckdb::FaultPoints::Armed(parked)) {
    absl::SleepFor(absl::Milliseconds(1));
  }
  EXPECT_EQ(kChunk, file.Append("small"));
  std::atomic_bool filled = false;
  std::string expected = first + "small";
  for (size_t i = 0; i != kChunks; ++i) {
    expected += Content(1, i, kChunk);
  }
  std::thread filler{[&] {
    for (size_t i = 0; i != kChunks; ++i) {
      file.Append(Content(1, i, kChunk));
    }
    filled = true;
  }};
  absl::SleepFor(absl::Milliseconds(200));
  EXPECT_FALSE(filled.load());
  duckdb::FaultPoints::Arm(absl::StrCat(kPoint, ":release"));
  writer.join();
  filler.join();
  EXPECT_TRUE(filled.load());
  file.Finish(kNoFooter);
  EXPECT_TRUE(ReadData(dir) == expected);
}

constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(ColFileWriter, ColFileWriterTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         tests::DirectoryTestCaseBase<>::to_string);

}  // namespace
