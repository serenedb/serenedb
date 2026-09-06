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

#include <utility>

#include "iresearch/search/common/bitset_storage.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

class BitsetDocs {
 public:
  explicit BitsetDocs(search::BitsetStorage&& set) noexcept
    : _set{std::move(set)} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    if (_doc >= max) {
      return _doc;
    }
    search::OrWindow(_set, min, max, mask);
    return _doc = search::NextBit(_set, max);
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    search::AndWindow(_set, min, max, mask);
    return _doc = search::NextBit(_set, max);
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    if (_doc >= max) {
      return _doc;
    }
    search::AndNotWindow(_set, min, max, mask);
    return _doc = search::NextBit(_set, max);
  }

  search::BitsetStorage* Folded() noexcept { return &_set; }

 private:
  search::BitsetStorage _set;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::fill
