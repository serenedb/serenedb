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

#include "iresearch/search/detail/bitset_storage.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

class BitsetDocs {
 public:
  static constexpr auto kBits = detail::BitsetStorage::kBits;

  explicit BitsetDocs(detail::BitsetStorage&& set) noexcept
    : _set{std::move(set)},
      _words{_set.Words()},
      _count{_set.WordCount()},
      _min{_set.Min()} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    SDB_ASSERT(target >= _min);
    const auto offset = target - _min;
    const auto word = offset / kBits;
    if (word >= _count) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto rest = _words[word] & (~uint64_t{0} << (offset % kBits));
    return _min + word * kBits + std::countr_zero(rest);
  }

  IRS_FORCE_INLINE bool Test(doc_id_t doc) const noexcept {
    SDB_ASSERT(doc >= _min);
    const auto offset = doc - _min;
    const auto word = offset / kBits;
    if (word >= _count) [[unlikely]] {
      return false;
    }
    return ((_words[word] >> (offset % kBits)) & 1) != 0;
  }

 private:
  detail::BitsetStorage _set;
  const uint64_t* _words;
  uint32_t _count;
  doc_id_t _min;
};

}  // namespace irs::probe
