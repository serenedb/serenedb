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

#include <duckdb/common/vector/dictionary_vector.hpp>
#include <iresearch/formats/column/dictionary_cache.hpp>

#include "tests_shared.hpp"

namespace {

duckdb::buffer_ptr<duckdb::DictionaryEntry> MakeDictionary() {
  return duckdb::DictionaryVector::CreateReusableDictionary(
    duckdb::LogicalType::VARCHAR, 4);
}

class DictionaryCacheTest : public ::testing::Test {
 protected:
  void TearDown() override { irs::BlockDictionaryCache::SetLimit(-1); }
};

}  // namespace

TEST_F(DictionaryCacheTest, KeepsTheFirstDictionaryUntilReleased) {
  irs::BlockDictionaryCache::SetLimit(1000);
  const auto base = irs::BlockDictionaryCache::Used();
  {
    irs::BlockDictionaryCache slot;
    ASSERT_EQ(nullptr, slot.Get().get());
    const auto dictionary = MakeDictionary();
    slot.Put(dictionary, 100);
    ASSERT_EQ(dictionary.get(), slot.Get().get());
    ASSERT_EQ(base + 100, irs::BlockDictionaryCache::Used());
    slot.Put(MakeDictionary(), 100);
    ASSERT_EQ(dictionary.get(), slot.Get().get());
    ASSERT_EQ(base + 100, irs::BlockDictionaryCache::Used());
  }
  ASSERT_EQ(base, irs::BlockDictionaryCache::Used());
}

TEST_F(DictionaryCacheTest, EvictsTheLeastRecentlyUsed) {
  const auto base = irs::BlockDictionaryCache::Used();
  irs::BlockDictionaryCache::SetLimit(base + 250);
  irs::BlockDictionaryCache a;
  irs::BlockDictionaryCache b;
  irs::BlockDictionaryCache c;
  a.Put(MakeDictionary(), 100);
  b.Put(MakeDictionary(), 100);
  ASSERT_NE(nullptr, a.Get().get());
  c.Put(MakeDictionary(), 100);
  ASSERT_EQ(nullptr, b.Get().get());
  ASSERT_NE(nullptr, a.Get().get());
  ASSERT_NE(nullptr, c.Get().get());
  ASSERT_EQ(base + 200, irs::BlockDictionaryCache::Used());
  b.Put(MakeDictionary(), 100);
  ASSERT_EQ(nullptr, a.Get().get());
  ASSERT_NE(nullptr, b.Get().get());
  ASSERT_EQ(base + 200, irs::BlockDictionaryCache::Used());
}

TEST_F(DictionaryCacheTest, DeclinesWhatCannotFit) {
  const auto base = irs::BlockDictionaryCache::Used();
  irs::BlockDictionaryCache slot;
  irs::BlockDictionaryCache::SetLimit(base + 50);
  slot.Put(MakeDictionary(), 100);
  ASSERT_EQ(nullptr, slot.Get().get());
  irs::BlockDictionaryCache::SetLimit(0);
  slot.Put(MakeDictionary(), 1);
  ASSERT_EQ(nullptr, slot.Get().get());
  ASSERT_EQ(base, irs::BlockDictionaryCache::Used());
}
