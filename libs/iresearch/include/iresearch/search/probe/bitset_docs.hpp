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
#include <utility>

#include "basics/shared.hpp"
#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

class BitsetDocs {
 public:
  static constexpr auto kBits = search::BitsetStorage::kBits;

  explicit BitsetDocs(search::BitsetStorage&& set) noexcept
    : _set{std::move(set)}, _words{_set.Words()}, _count{_set.WordCount()} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    const auto word = target / kBits;
    if (word >= _count) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto rest = _words[word] & (~uint64_t{0} << (target % kBits));
    return word * kBits + std::countr_zero(rest);
  }

 private:
  search::BitsetStorage _set;
  const uint64_t* _words;
  uint32_t _count;
};

}  // namespace irs::probe
