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

#pragma once

#include <bit>
#include <cstdint>
#include <utility>

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

class Bitset : public Root {
 public:
  explicit Bitset(search::BitsetStorage&& set) noexcept
    : _set{std::move(set)} {}

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    constexpr auto kBits = search::BitsetStorage::kBits;
    const auto* const words = _set.Words();
    const auto count = _set.WordCount();
    uint32_t n = 0;

    for (; _word != count; ++_word) {
      const auto word = words[_word];
      if (word == 0) {
        continue;
      }
      const auto card = static_cast<uint32_t>(std::popcount(word));
      if (n + card > capacity) {
        return n;
      }
      n = static_cast<uint32_t>(MaterializeWord(_word * kBits, word, out + n) -
                                out);
    }
    return n;
  }

 private:
  search::BitsetStorage _set;
  uint32_t _word = 0;
};

}  // namespace irs::docs
namespace irs::search {

template<>
inline constexpr uint64_t kFoldPostings<docs::Root::ptr> = 4;

template<>
inline docs::Root::ptr MakeBitsetNode<docs::Root::ptr>(BitsetBuckets&& buckets,
                                                       const IndexInput& doc,
                                                       doc_id_t docs_count,
                                                       TableFilter*) {
  return memory::make_managed<docs::Bitset>(
    BuildBitset(buckets, doc, docs_count));
}

}  // namespace irs::search
