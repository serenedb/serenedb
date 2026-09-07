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

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

class SingleDocs {
 public:
  explicit SingleDocs(doc_id_t doc) noexcept : _doc{doc} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    return FillOrImpl(min, max, mask, [](size_t) noexcept {});
  }

  doc_id_t FillSum(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillOrImpl(min, max, mask, [=](size_t offset) IRS_FORCE_INLINE {
      Merge<ScoreMergeType::Sum>(scores[offset], constant);
    });
  }

  doc_id_t FillMax(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillOrImpl(min, max, mask, [=](size_t offset) IRS_FORCE_INLINE {
      Merge<ScoreMergeType::Max>(scores[offset], constant);
    });
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    const auto words = search::WindowWords(min, max);
    if (_doc < min || _doc >= max) {
      search::Clear(mask, words);
      return _doc >= max ? _doc : (_doc = doc_limits::eof());
    }
    const size_t offset = _doc - min;
    const auto word = mask[offset / search::kWindowBits] &
                      (uint64_t{1} << (offset % search::kWindowBits));
    search::Clear(mask, words);
    mask[offset / search::kWindowBits] = word;
    return _doc = doc_limits::eof();
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    if (_doc >= max) {
      return _doc;
    }
    if (_doc >= min) {
      const size_t offset = _doc - min;
      UnsetBit(mask[offset / search::kWindowBits],
               offset % search::kWindowBits);
    }
    return _doc = doc_limits::eof();
  }

 private:
  template<typename Write>
  doc_id_t FillOrImpl(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                      Write&& write) {
    if (_doc >= max) {
      return _doc;
    }
    if (_doc >= min) {
      const size_t offset = _doc - min;
      SetBit(mask[offset / search::kWindowBits], offset % search::kWindowBits);
      write(offset);
    }
    return _doc = doc_limits::eof();
  }

  doc_id_t _doc;
};

}  // namespace irs::fill
