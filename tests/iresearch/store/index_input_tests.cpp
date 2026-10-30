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

#include <duckdb/storage/block_cache/block_cache.hpp>
#include <iresearch/store/data_input.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <vector>

#include "tests_shared.hpp"

namespace {

constexpr uint64_t kBlock = duckdb::BlockCache::BLOCK_SIZE;
constexpr uint64_t kBlocks = 16;
constexpr uint64_t kRead = 64;

irs::byte_type ExpectedByte(uint64_t offset) {
  return static_cast<irs::byte_type>(offset * 31 + offset / kBlock * 7);
}

class IndexInputTest : public TestBase {};

TEST_F(IndexInputTest, pins_outlive_evictions) {
  duckdb::BlockCache cache{irs::DuckDBEngine::Instance().instance(),
                           4 * kBlock};
  cache.SetSpill((test_dir() / "spill").string(), 0);
  auto file = cache.CreateUnbackedFile("memory://block_input_test/stable");
  for (uint64_t block = 0; block != kBlocks; ++block) {
    auto pin = cache.CreateBlock(*file, block);
    for (uint64_t i = 0; i != kBlock; ++i) {
      pin.GetDataMutable()[i] = ExpectedByte(block * kBlock + i);
    }
  }
  cache.SetUnbackedSize(*file, kBlocks * kBlock);

  const irs::InputFile blocks{cache, file};
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

TEST_F(IndexInputTest, pins_across_blocks_keep_alignment) {
  duckdb::BlockCache cache{irs::DuckDBEngine::Instance().instance(),
                           4 * kBlock};
  cache.SetSpill((test_dir() / "spill").string(), 0);
  auto file = cache.CreateUnbackedFile("memory://block_input_test/aligned");
  for (uint64_t block = 0; block != 2; ++block) {
    auto pin = cache.CreateBlock(*file, block);
    for (uint64_t i = 0; i != kBlock; ++i) {
      pin.GetDataMutable()[i] = ExpectedByte(block * kBlock + i);
    }
  }
  cache.SetUnbackedSize(*file, 2 * kBlock);

  constexpr uint64_t kAlign = __STDCPP_DEFAULT_NEW_ALIGNMENT__;
  const irs::InputFile blocks{cache, file};
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
  duckdb::BlockCache cache{irs::DuckDBEngine::Instance().instance(),
                           4 * kBlock};
  cache.SetSpill((test_dir() / "spill").string(), 0);
  auto file = cache.CreateUnbackedFile("memory://block_input_test/peek");
  for (uint64_t block = 0; block != 3; ++block) {
    auto pin = cache.CreateBlock(*file, block);
    for (uint64_t i = 0; i != kBlock; ++i) {
      pin.GetDataMutable()[i] = ExpectedByte(block * kBlock + i);
    }
  }
  cache.SetUnbackedSize(*file, 3 * kBlock);

  const irs::InputFile blocks{cache, file};
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
