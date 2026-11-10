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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <chrono>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <future>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/store/data_input.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <thread>
#include <vector>

#include "tests_shared.hpp"

namespace {

constexpr uint64_t kBlock = duckdb::ExternalFileCache::BLOCK_SIZE;
constexpr uint64_t kBlocks = 16;
constexpr uint64_t kRead = 64;

irs::byte_type ExpectedByte(uint64_t offset) {
  return static_cast<irs::byte_type>(offset * 31 + offset / kBlock * 7);
}

void AppendBlocks(const duckdb::shared_ptr<duckdb::ExternalFileCacheFile>& file,
                  uint64_t blocks, bool expected = true) {
  std::vector<irs::byte_type> bytes(blocks * kBlock);
  if (expected) {
    for (uint64_t i = 0; i != bytes.size(); ++i) {
      bytes[i] = ExpectedByte(i);
    }
  }
  duckdb::ExternalFileCacheAppender appender{file};
  appender.Append(bytes.data(), bytes.size());
  appender.Finish();
}

class IndexInputTest : public TestBase {};

TEST_F(IndexInputTest, pins_outlive_evictions) {
  duckdb::DBConfig config;
  config.options.maximum_memory = (kBlocks + 4) * kBlock;
  config.options.temporary_directory = (test_dir() / "spill").string();
  duckdb::DuckDB db{nullptr, &config};
  duckdb::ExternalFileCache cache{*db.instance};
  auto file = cache.CreateUnbackedFile("memory://block_input_test/stable");
  AppendBlocks(file, kBlocks);
  auto filler = cache.CreateUnbackedFile("memory://block_input_test/filler");
  AppendBlocks(filler, kBlocks, false);

  const irs::InputFile blocks{file};
  irs::IndexInput in{blocks};
  std::vector<irs::PinnedBytes> pinned;
  for (uint64_t pass = 0; pass != 2; ++pass) {
    for (uint64_t block = 0; block != kBlocks; ++block) {
      in.Seek(block * kBlock + 100);
      auto bytes = in.Pin(kRead);
      ASSERT_EQ(kRead, bytes.bytes.size());
      EXPECT_EQ(block * kBlock + 100 + kRead, in.Position());
      pinned.push_back(std::move(bytes));
    }
  }
  for (uint64_t i = 0; i != pinned.size(); ++i) {
    const auto offset = i % kBlocks * kBlock + 100;
    for (uint64_t j = 0; j != kRead; ++j) {
      ASSERT_EQ(ExpectedByte(offset + j), pinned[i].bytes[j]) << i << " " << j;
    }
  }

  const auto across = in.Pin(kBlock - 10, 20);
  ASSERT_EQ(20, across.bytes.size());
  for (uint64_t j = 0; j != 20; ++j) {
    ASSERT_EQ(ExpectedByte(kBlock - 10 + j), across.bytes[j]) << j;
  }
}

#ifdef SDB_FAULT_INJECTION
void AwaitParked(std::string_view fault) {
  while (!duckdb::FaultPoints::Disarm(absl::StrCat(fault, ":parked"))) {
    std::this_thread::yield();
  }
}

struct ReadWhileParked {
  bool waited = false;
  irs::byte_type first = 0;
};

ReadWhileParked ReadWhileParkedAt(const irs::InputFile& file, uint64_t block,
                                  std::string_view fault) {
  auto reader = std::async(std::launch::async, [&] {
    const irs::IndexInput in{file};
    return in.Pin(block * kBlock, kRead).bytes[0];
  });
  ReadWhileParked result;
  result.waited = reader.wait_for(std::chrono::milliseconds(200)) ==
                  std::future_status::timeout;
  EXPECT_TRUE(duckdb::FaultPoints::Arm(absl::StrCat(fault, ":release")));
  EXPECT_NO_THROW(result.first = reader.get());
  return result;
}

TEST_F(IndexInputTest, reads_wait_for_an_eviction_writing_a_temporary_copy) {
  duckdb::DBConfig config;
  config.options.maximum_memory = 4 * kBlock;
  config.options.temporary_directory = (test_dir() / "spill").string();
  duckdb::DuckDB db{nullptr, &config};
  duckdb::ExternalFileCache cache{*db.instance};
  auto file = cache.CreateUnbackedFile("memory://block_input_test/unloading");
  AppendBlocks(file, 2);
  absl::Cleanup clear = [] { duckdb::FaultPoints::Clear(); };
  ASSERT_TRUE(duckdb::FaultPoints::Arm("block_unload_before_temporary_write"));
  auto& buffer_manager = duckdb::BufferManager::GetBufferManager(*db.instance);
  auto evictor = std::async(std::launch::async, [&] {
    return buffer_manager.Allocate(duckdb::MemoryTag::IN_MEMORY_TABLE,
                                   2 * kBlock);
  });
  AwaitParked("block_unload_before_temporary_write");
  const uint64_t victim = cache.TryPinRange(*file, 0, kBlock).empty() ? 0 : 1;
  ASSERT_TRUE(cache.TryPinRange(*file, victim * kBlock, kBlock).empty());
  const irs::InputFile blocks{file};
  const auto read =
    ReadWhileParkedAt(blocks, victim, "block_unload_before_temporary_write");
  EXPECT_TRUE(evictor.get().IsValid());
  EXPECT_TRUE(read.waited);
  EXPECT_EQ(ExpectedByte(victim * kBlock), read.first);
}

TEST_F(IndexInputTest, reads_wait_for_another_reader_loading_a_spilled_block) {
  duckdb::DBConfig config;
  config.options.maximum_memory = 4 * kBlock;
  config.options.temporary_directory = (test_dir() / "spill").string();
  duckdb::DuckDB db{nullptr, &config};
  duckdb::ExternalFileCache cache{*db.instance};
  auto file = cache.CreateUnbackedFile("memory://block_input_test/loading");
  AppendBlocks(file, 2);
  duckdb::BufferManager::GetBufferManager(*db.instance).SetMemoryLimit(kBlock);
  cache.Pin(*file, 1);
  ASSERT_TRUE(cache.TryPinRange(*file, 0, kBlock).empty());
  absl::Cleanup clear = [] { duckdb::FaultPoints::Clear(); };
  ASSERT_TRUE(duckdb::FaultPoints::Arm("block_load_before_loaded"));
  const irs::InputFile blocks{file};
  auto loader = std::async(std::launch::async, [&] {
    const irs::IndexInput in{blocks};
    return in.Pin(0, kRead).bytes[0];
  });
  AwaitParked("block_load_before_loaded");
  const auto read = ReadWhileParkedAt(blocks, 0, "block_load_before_loaded");
  irs::byte_type loaded = 0;
  EXPECT_NO_THROW(loaded = loader.get());
  EXPECT_EQ(ExpectedByte(0), loaded);
  EXPECT_TRUE(read.waited);
  EXPECT_EQ(ExpectedByte(0), read.first);
}
#endif

TEST_F(IndexInputTest, column_blocks_inside_a_cache_block_are_read_in_place) {
  auto& db = irs::DuckDBEngine::Instance().instance();
  duckdb::ExternalFileCache cache{db};
  auto file = cache.CreateUnbackedFile("memory://block_input_test/columns");
  AppendBlocks(file, 2);
  const irs::InputFile blocks{file};
  auto& buffers = duckdb::BufferManager::GetBufferManager(db);
  irs::ReadContext ctx{db, &blocks};
  {
    auto inside = ctx.RegisterColBlock(8, 1000);
    EXPECT_EQ(0, inside->GetMemory().GetMemoryUsage());
    const auto pin = buffers.Pin(inside);
    EXPECT_EQ(blocks.Pin(8, 1000).bytes.data(), pin.Ptr());
    for (uint64_t j = 0; j != 1000; ++j) {
      ASSERT_EQ(ExpectedByte(8 + j), pin.Ptr()[j]) << j;
    }
  }
  {
    auto across = ctx.RegisterColBlock(kBlock - 512, 1024);
    EXPECT_NE(0, across->GetMemory().GetMemoryUsage());
    const auto pin = buffers.Pin(across);
    for (uint64_t j = 0; j != 1024; ++j) {
      ASSERT_EQ(ExpectedByte(kBlock - 512 + j), pin.Ptr()[j]) << j;
    }
  }
}

TEST_F(IndexInputTest, pins_across_blocks_keep_alignment) {
  duckdb::ExternalFileCache cache{irs::DuckDBEngine::Instance().instance()};
  auto file = cache.CreateUnbackedFile("memory://block_input_test/aligned");
  AppendBlocks(file, 2);

  constexpr uint64_t kAlign = __STDCPP_DEFAULT_NEW_ALIGNMENT__;
  const irs::InputFile blocks{file};
  const irs::IndexInput in{blocks};
  for (const uint64_t count : {kAlign + 4, 4 * kAlign}) {
    const auto offset = kBlock - kAlign;
    const auto pinned = in.Pin(offset, count);
    const auto* data = pinned.bytes.data();
    ASSERT_EQ(0, reinterpret_cast<uintptr_t>(data) % kAlign) << count;
    for (uint64_t j = 0; j != count; ++j) {
      ASSERT_EQ(ExpectedByte(offset + j), data[j]) << count << " " << j;
    }
  }
}

TEST_F(IndexInputTest, peek_refills_an_empty_window_and_reads_cross_blocks) {
  duckdb::ExternalFileCache cache{irs::DuckDBEngine::Instance().instance()};
  auto file = cache.CreateUnbackedFile("memory://block_input_test/peek");
  AppendBlocks(file, 3);

  const irs::InputFile blocks{file};
  ASSERT_FALSE(blocks.Contiguous());
  irs::IndexInput in{blocks};
  in.Seek(kBlock + 10);
  const auto inside = in.Peek(kRead);
  ASSERT_EQ(kRead, inside.size());
  EXPECT_EQ(ExpectedByte(kBlock + 10), inside[0]);
  EXPECT_EQ(kBlock + 10, in.Position());

  in.Seek(2 * kBlock - 5);
  EXPECT_TRUE(in.Peek(kRead).empty());
  irs::byte_type across[kRead];
  in.ReadData(across, kRead);
  for (uint64_t j = 0; j != kRead; ++j) {
    ASSERT_EQ(ExpectedByte(2 * kBlock - 5 + j), across[j]) << j;
  }
  EXPECT_EQ(2 * kBlock - 5 + kRead, in.Position());

  in.Seek(3 * kBlock - 2);
  EXPECT_ANY_THROW(in.ReadI32());
}

TEST_F(IndexInputTest, checked_cursor_over_bytes_stops_at_their_end) {
  const irs::bstring bytes{irs::byte_type{1}, irs::byte_type{2},
                           irs::byte_type{3}};
  irs::IndexInput in{bytes};
  EXPECT_EQ(1, in.ReadByte());
  EXPECT_EQ(1, in.Position());
  EXPECT_EQ(3, in.Length());
  EXPECT_EQ(3, in.Pin(2, 1).bytes[0]);
  EXPECT_ANY_THROW(in.Pin(2, 2));
  EXPECT_ANY_THROW(in.ReadI32());
  in.Seek(5);
  EXPECT_ANY_THROW(in.ReadByte());
}

}  // namespace
