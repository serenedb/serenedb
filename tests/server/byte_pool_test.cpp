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

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

#include "server/utils/byte_pool.h"

namespace {

using BytePool = sdb::utils::BytePool<struct TestTag>;
using PooledBytes = sdb::utils::PooledBytes<BytePool>;

TEST(UtilsBytePool, TagsDoNotShareBlocks) {
  using Other = sdb::utils::BytePool<struct OtherTag>;
  auto* block = BytePool::Allocate(2000);
  BytePool::Free(block);
  auto* other = Other::Allocate(2000);
  EXPECT_NE(other, block);
  Other::Free(other);
  auto* again = BytePool::Allocate(2000);
  EXPECT_EQ(again, block);
  BytePool::Free(again);
}

TEST(UtilsBytePool, ReusesTheLastFreedBlockFirst) {
  auto* first = BytePool::Allocate(3000);
  auto* second = BytePool::Allocate(3000);
  BytePool::Free(first);
  BytePool::Free(second);
  auto* a = BytePool::Allocate(3000);
  auto* b = BytePool::Allocate(3000);
  EXPECT_EQ(a, second);
  EXPECT_EQ(b, first);
  BytePool::Free(a);
  BytePool::Free(b);
}

TEST(UtilsBytePool, SizesOfOneClassShareBlocks) {
  auto* block = BytePool::Allocate(5000);
  BytePool::Free(block);
  auto* smaller = BytePool::Allocate(4100);
  EXPECT_EQ(smaller, block);
  BytePool::Free(smaller);
  auto* other = BytePool::Allocate(9000);
  EXPECT_NE(other, block);
  BytePool::Free(other);
}

TEST(UtilsBytePool, OverflowDropsTheOldestBlock) {
  constexpr size_t kSize = 300 * 1024;
  const size_t slots = BytePool::Slots();
  std::vector<void*> blocks;
  for (size_t i = 0; i <= slots; ++i) {
    blocks.emplace_back(BytePool::Allocate(kSize));
  }
  for (auto* block : blocks) {
    BytePool::Free(block);
  }
  std::vector<void*> reused;
  for (size_t i = 0; i < slots; ++i) {
    reused.emplace_back(BytePool::Allocate(kSize));
    EXPECT_EQ(reused.back(), blocks[slots - i]);
  }
  for (auto* block : reused) {
    BytePool::Free(block);
  }
}

TEST(UtilsBytePool, BlocksAreAligned) {
  for (const size_t size :
       {size_t{1}, size_t{63}, size_t{64}, size_t{65}, size_t{16} << 10,
        BytePool::kMaxPooledBytes, BytePool::kMaxPooledBytes + 1}) {
    auto* block = BytePool::Allocate(size);
    ASSERT_NE(block, nullptr) << size;
    EXPECT_EQ(reinterpret_cast<uintptr_t>(block) % alignof(std::max_align_t),
              0u)
      << size;
    std::memset(block, 0xCD, size);
    BytePool::Free(block);
  }
  BytePool::Free(nullptr);
}

TEST(UtilsBytePool, ConcurrentUseKeepsBlocksPrivate) {
  constexpr int kThreads = 8;
  constexpr int kRounds = 5000;
  std::vector<std::thread> threads;
  std::vector<int> corrupted(kThreads, 0);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([t, &corrupted] {
      for (int i = 0; i < kRounds; ++i) {
        const size_t size = size_t{64} << ((t + i) % 12);
        PooledBytes first{size};
        PooledBytes second{size};
        std::memset(first.Data(), t, first.Size());
        std::memset(second.Data(), t + 1, second.Size());
        if (first.Data()[size - 1] != static_cast<uint8_t>(t) ||
            second.Data()[0] != static_cast<uint8_t>(t + 1)) {
          ++corrupted[t];
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (int t = 0; t < kThreads; ++t) {
    EXPECT_EQ(corrupted[t], 0) << t;
  }
}

}  // namespace
