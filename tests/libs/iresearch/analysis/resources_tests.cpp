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

#include <duckdb.hpp>
#include <duckdb/storage/buffer/buffer_pool.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/shared_object_cache.hpp>

#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/stopword_set.hpp"
#include "iresearch/analysis/tokenizer_pool.hpp"

namespace {

using irs::analysis::StopwordSet;
using irs::analysis::TokenizerPool;

struct SharedResourcesTest : ::testing::Test {
  duckdb::DuckDB db{nullptr};
  duckdb::SharedObjectCache& cache{db.instance->GetSharedObjectCache()};
  duckdb::ObjectCache& object_cache{db.instance->GetObjectCache()};
  duckdb::BufferPool& pool{
    duckdb::BufferManager::GetBufferManager(*db.instance).GetBufferPool()};

  duckdb::shared_ptr<const StopwordSet> Stopwords(
    std::vector<std::string> words) {
    return StopwordSet::GetOrBuild(
      cache, duckdb::make_uniq<StopwordSet>(std::move(words)));
  }
};

TEST_F(SharedResourcesTest, stopwords_intern_by_content) {
  const auto a = Stopwords({"a", "b", "c"});
  const auto b = Stopwords({"c", "a", "b"});
  const auto c = Stopwords({"a", "b"});
  EXPECT_EQ(a.get(), b.get());
  EXPECT_NE(a.get(), c.get());
  EXPECT_EQ(2u, cache.GetEntryCount());
}

TEST_F(SharedResourcesTest, stopwords_digest_distinguishes_word_boundaries) {
  const auto a = Stopwords({"ab", "c"});
  const auto b = Stopwords({"a", "bc"});
  EXPECT_NE(a.get(), b.get());
  EXPECT_TRUE(a->Contains(std::string_view{"ab"}));
  EXPECT_FALSE(a->Contains(std::string_view{"a"}));
  EXPECT_TRUE(b->Contains(std::string_view{"bc"}));
  EXPECT_FALSE(b->Contains(std::string_view{"c"}));
}

TEST_F(SharedResourcesTest, resource_dies_with_last_user_and_is_accounted) {
  const auto before = pool.GetUsedMemory();
  auto a = Stopwords({"aaaaaaaaaa", "bbbbbbbbbb"});
  const auto bytes = a->GetEstimatedCacheMemory().GetIndex();
  ASSERT_GT(bytes, 0u);
  EXPECT_EQ(before + bytes, pool.GetUsedMemory());
  EXPECT_EQ(bytes, cache.GetMemoryUsage());
  auto b = a;
  a.reset();
  EXPECT_EQ(before + bytes, pool.GetUsedMemory());
  b.reset();
  EXPECT_EQ(before, pool.GetUsedMemory());
  EXPECT_EQ(0u, cache.GetEntryCount());
  EXPECT_EQ(0u, cache.GetMemoryUsage());
}

irs::analysis::Tokenizer::ptr MakeTokenizer() {
  return std::make_unique<irs::KeywordTokenizer>();
}

TEST_F(SharedResourcesTest, one_pool_per_dictionary) {
  auto a = TokenizerPool::Get(*db.instance, "42");
  auto b = TokenizerPool::Get(*db.instance, "42");
  auto c = TokenizerPool::Get(*db.instance, "43");
  EXPECT_EQ(a.get(), b.get());
  EXPECT_NE(a.get(), c.get());
}

TEST_F(SharedResourcesTest, pool_recycles_released_instances) {
  auto pool_a = TokenizerPool::Get(*db.instance, "42");
  EXPECT_EQ(nullptr, pool_a->Acquire());
  auto first = MakeTokenizer();
  auto second = MakeTokenizer();
  auto* first_raw = first.get();
  auto* second_raw = second.get();
  pool_a->Release(std::move(first));
  pool_a->Release(std::move(second));
  EXPECT_EQ(2u, pool_a->IdleCount());
  EXPECT_EQ(second_raw, pool_a->Acquire().get());
  EXPECT_EQ(first_raw, pool_a->Acquire().get());
  EXPECT_EQ(nullptr, pool_a->Acquire());
}

TEST_F(SharedResourcesTest, pool_caps_idle_instances) {
  auto small = duckdb::make_shared_ptr<TokenizerPool>(*db.instance, "cap", 2);
  small->Release(MakeTokenizer());
  small->Release(MakeTokenizer());
  small->Release(MakeTokenizer());
  EXPECT_EQ(2u, small->IdleCount());
}

TEST_F(SharedResourcesTest, idle_instances_yield_to_memory_pressure) {
  auto pool_a = TokenizerPool::Get(*db.instance, "42");
  pool_a->Release(MakeTokenizer());
  EXPECT_EQ(1u, pool_a->IdleCount());
  EXPECT_EQ(1u, object_cache.GetEntryCount());
  object_cache.EvictToReduceMemory(size_t{1} << 40);
  EXPECT_EQ(0u, pool_a->IdleCount());
  EXPECT_EQ(0u, object_cache.GetEntryCount());
  pool_a->Release(MakeTokenizer());
  EXPECT_EQ(1u, pool_a->IdleCount());
  EXPECT_EQ(1u, object_cache.GetEntryCount());
}

TEST_F(SharedResourcesTest, pool_dies_with_last_holder) {
  const auto before = pool.GetUsedMemory();
  auto pool_a = TokenizerPool::Get(*db.instance, "42");
  pool_a->Release(MakeTokenizer());
  auto pool_b = pool_a;
  pool_a.reset();
  EXPECT_EQ(1u, pool_b->IdleCount());
  pool_b.reset();
  EXPECT_EQ(0u, cache.GetEntryCount());
  EXPECT_EQ(0u, object_cache.GetEntryCount());
  EXPECT_EQ(before, pool.GetUsedMemory());
  auto again = TokenizerPool::Get(*db.instance, "42");
  EXPECT_EQ(nullptr, again->Acquire());
}

TEST(tokenizer_pool_test, pool_outlives_database) {
  duckdb::shared_ptr<TokenizerPool> survivor;
  {
    duckdb::DuckDB db{nullptr};
    survivor = TokenizerPool::Get(*db.instance, "42");
    survivor->Release(std::make_unique<irs::KeywordTokenizer>());
  }
  EXPECT_EQ(nullptr, survivor->Acquire());
  survivor->Release(std::make_unique<irs::KeywordTokenizer>());
  EXPECT_EQ(0u, survivor->IdleCount());
  survivor.reset();
}

}  // namespace
