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

#include "iresearch/search/count/root.hpp"
#include "iresearch/search/detail/bitset_build.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/bitset_storage.hpp"
#include "iresearch/search/detail/lazy_bitset.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::count {

template<typename Table>
class BooleanBitset : public Root {
 public:
  BooleanBitset(detail::BitsetBuckets&& buckets, const IndexInput& doc,
                doc_id_t docs_count, Table table) noexcept
    : _buckets{std::move(buckets)},
      _doc{&doc},
      _docs_count{docs_count},
      _table{table} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    constexpr auto kBits = detail::BitsetStorage::kBits;
    max = std::min<doc_id_t>(max, detail::BitsetStorage::kMin + _docs_count);
    if (min >= max) {
      return 0;
    }
    const auto docs = max - min;
    const auto count = static_cast<uint32_t>((docs + kBits - 1) / kBits);
    if (_set.WordCount() < count) {
      _set = detail::BitsetStorage{docs};
    } else {
      std::fill_n(_set.Words(), count, uint64_t{0});
    }
    auto* const words = _set.Words();
    detail::FillBitset(_buckets, *_doc, _docs_count, min, max, words);
    if (const auto tail = docs % kBits; tail != 0) {
      words[count - 1] &= (uint64_t{1} << tail) - 1;
    }
    if constexpr (kTable) {
      return _table.Count(min, words, count);
    } else {
      return detail::CountBitRange(words, 0, docs);
    }
  }

 private:
  static constexpr bool kTable = !std::is_same_v<Table, utils::Empty>;

  detail::BitsetBuckets _buckets;
  detail::BitsetStorage _set;
  const IndexInput* _doc;
  doc_id_t _docs_count;
  [[no_unique_address]] detail::Narrowing<Table> _table;
};

}  // namespace irs::count
namespace irs::detail {

template<>
inline constexpr bool kRangeBitset<count::Root::ptr> = true;
template<>
inline constexpr FoldEmit kFoldEmit<count::Root::ptr>{0.3, 1.0, false};

template<>
inline count::Root::ptr MakeBitsetNode<count::Root::ptr>(
  BitsetBuckets&& buckets, const IndexInput& doc, doc_id_t docs_count,
  TableFilter* table) {
  if (table != nullptr) {
    return memory::make_managed<count::BooleanBitset<TableFilter*>>(
      std::move(buckets), doc, docs_count, table);
  }
  return memory::make_managed<count::BooleanBitset<utils::Empty>>(
    std::move(buckets), doc, docs_count, utils::Empty{});
}

}  // namespace irs::detail
