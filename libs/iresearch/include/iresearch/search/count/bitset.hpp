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
#include <type_traits>
#include <utility>

#include "basics/empty.hpp"
#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/count/root.hpp"

namespace irs::count {

template<typename Table>
class Bitset : public Root {
 public:
  Bitset(search::BitsetBuckets&& buckets, const IndexInput& doc,
         doc_id_t docs_count, Table table) noexcept
    : _buckets{std::move(buckets)},
      _doc{&doc},
      _docs_count{docs_count},
      _table{table} {}

  uint64_t Run() final {
    auto set = search::BuildBitset(_buckets, *_doc, _docs_count);
    return _table.Count(0, set.Words(), set.WordCount());
  }

 private:
  search::BitsetBuckets _buckets;
  const IndexInput* _doc;
  doc_id_t _docs_count;
  [[no_unique_address]] search::Narrowing<Table> _table;
};

}  // namespace irs::count
namespace irs::search {

template<>
inline constexpr uint64_t kFoldPostings<count::Root::ptr> = 4;

template<>
inline count::Root::ptr MakeBitsetNode<count::Root::ptr>(
  BitsetBuckets&& buckets, const IndexInput& doc, doc_id_t docs_count,
  TableFilter* table) {
  if (table != nullptr) {
    return memory::make_managed<count::Bitset<TableFilter*>>(
      std::move(buckets), doc, docs_count, table);
  }
  return memory::make_managed<count::Bitset<utils::Empty>>(
    std::move(buckets), doc, docs_count, utils::Empty{});
}

}  // namespace irs::search
