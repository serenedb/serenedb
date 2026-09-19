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

#include <algorithm>
#include <cstdint>
#include <utility>

#include "iresearch/search/detail/bitset_build.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/bitset_storage.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

class BooleanBitset : public Root {
 public:
  explicit BooleanBitset(detail::BitsetStorage&& set) noexcept
    : _set{std::move(set)} {}

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out) final {
    constexpr auto kBits = detail::BitsetStorage::kBits;
    constexpr auto kMin = detail::BitsetStorage::kMin;
    const auto* const words = _set.Words();
    const uint64_t total = uint64_t{_set.WordCount()} * kBits;
    const uint64_t lo = min - kMin;
    const uint64_t hi =
      std::min<uint64_t>(doc_limits::eof(max) ? total : max - kMin, total);
    if (lo >= hi) {
      return 0;
    }
    auto w = static_cast<uint32_t>(lo / kBits);
    auto word = words[w] & (~uint64_t{0} << (lo % kBits));
    const auto last = static_cast<uint32_t>((hi - 1) / kBits);
    uint32_t n = 0;
    for (;; word = words[++w]) {
      if (w == last) [[unlikely]] {
        if (const auto tail = hi % kBits; tail != 0) {
          word &= (uint64_t{1} << tail) - 1;
        }
        if (word != 0) {
          n = static_cast<uint32_t>(
            MaterializeWord(kMin + w * kBits, word, out + n) - out);
        }
        return n;
      }
      if (word != 0) {
        n = static_cast<uint32_t>(
          MaterializeWord(kMin + w * kBits, word, out + n) - out);
      }
    }
  }

 private:
  detail::BitsetStorage _set;
};

}  // namespace irs::docs
namespace irs::detail {

template<>
inline constexpr uint64_t kFoldPostings<docs::Root::ptr> = 4;
template<>
inline constexpr FoldEmit kFoldEmit<docs::Root::ptr>{0.0, 1.0, true};

template<>
inline docs::Root::ptr MakeBitsetNode<docs::Root::ptr>(BitsetBuckets&& buckets,
                                                       const IndexInput& doc,
                                                       doc_id_t docs_count,
                                                       TableFilter*) {
  return memory::make_managed<docs::BooleanBitset>(
    BuildBitset(buckets, doc, docs_count));
}

}  // namespace irs::detail
