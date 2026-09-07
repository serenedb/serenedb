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
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

class AllDocs {
 public:
  explicit AllDocs(const SubReader& segment) noexcept
    : _last{static_cast<doc_id_t>(segment.docs_count())} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    return FillOrImpl(min, max, mask, [](doc_id_t, doc_id_t) noexcept {});
  }

  doc_id_t FillSum(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillOrImpl(min, max, mask,
                      [=](doc_id_t begin, doc_id_t end) IRS_FORCE_INLINE {
                        for (auto i = begin; i != end; ++i) {
                          Merge<ScoreMergeType::Sum>(scores[i], constant);
                        }
                      });
  }

  doc_id_t FillMax(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                   score_t* IRS_RESTRICT scores, score_t constant) {
    return FillOrImpl(min, max, mask,
                      [=](doc_id_t begin, doc_id_t end) IRS_FORCE_INLINE {
                        for (auto i = begin; i != end; ++i) {
                          Merge<ScoreMergeType::Max>(scores[i], constant);
                        }
                      });
  }

  doc_id_t FillAnd(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    doc_id_t begin = 0;
    doc_id_t end = 0;
    if (!Span(min, max, begin, end)) {
      search::Clear(mask, search::WindowWords(min, max));
      return doc_limits::eof();
    }
    if (begin != min) {
      search::ClearInclusive(mask, 0, begin - min - 1);
    }
    if (end != max) {
      search::ClearInclusive(mask, end - min, max - min - 1);
    }
    return end > _last ? doc_limits::eof() : end;
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask) {
    doc_id_t begin = 0;
    doc_id_t end = 0;
    if (!Span(min, max, begin, end)) {
      return doc_limits::eof();
    }
    search::ClearInclusive(mask, begin - min, end - min - 1);
    return end > _last ? doc_limits::eof() : end;
  }

 private:
  template<typename Write>
  doc_id_t FillOrImpl(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                      Write&& write) {
    doc_id_t begin = 0;
    doc_id_t end = 0;
    if (!Span(min, max, begin, end)) {
      return doc_limits::eof();
    }
    SetBitRange(mask, begin - min, end - min);
    write(begin - min, end - min);
    return end > _last ? doc_limits::eof() : end;
  }

  bool Span(doc_id_t min, doc_id_t max, doc_id_t& begin,
            doc_id_t& end) const noexcept {
    begin = std::max(min, doc_limits::min());
    end = std::min(max, _last + 1);
    return begin < end;
  }

  doc_id_t _last;
};

}  // namespace irs::fill
