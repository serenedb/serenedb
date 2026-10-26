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

TEST_F(IndexInputTest, stable_reads_outlive_evictions) {
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

  {
    const irs::InputFile blocks{cache, file};
    irs::IndexInput in{blocks};
    std::vector<const irs::byte_type*> stable;
    for (uint64_t pass = 0; pass != 2; ++pass) {
      for (uint64_t block = 0; block != kBlocks; ++block) {
        const auto* data = in.ReadStable(block * kBlock + 100, kRead);
        ASSERT_NE(nullptr, data);
        stable.push_back(data);
      }
    }
    for (uint64_t i = 0; i != stable.size(); ++i) {
      const auto offset = i % kBlocks * kBlock + 100;
      for (uint64_t j = 0; j != kRead; ++j) {
        ASSERT_EQ(ExpectedByte(offset + j), stable[i][j]) << i << " " << j;
      }
    }

    in.Seek(kBlock - 10);
    EXPECT_EQ(nullptr, in.ReadStable(20));
    EXPECT_EQ(kBlock - 10, in.Position());
  }
}

TEST_F(IndexInputTest, volatile_reads_across_blocks_keep_alignment) {
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
  irs::IndexInput in{blocks};
  for (const uint64_t count : {kAlign + 4, 4 * kAlign}) {
    const auto offset = kBlock - kAlign;
    const auto* data = in.ReadVolatile(offset, count);
    ASSERT_EQ(0, reinterpret_cast<uintptr_t>(data) % kAlign) << count;
    for (uint64_t j = 0; j != count; ++j) {
      ASSERT_EQ(ExpectedByte(offset + j), data[j]) << count << " " << j;
    }
  }
}

}  // namespace
