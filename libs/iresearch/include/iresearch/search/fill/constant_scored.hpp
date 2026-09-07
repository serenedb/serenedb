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

#include "basics/assert.h"
#include "iresearch/search/fill/concept.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<ConstantProducer Approx>
class ConstantScored {
 public:
  template<typename... Args>
  ConstantScored(ScoreMergeType merge, score_t value, Args&&... args)
    : _approx{std::forward<Args>(args)...},
      _value{value},
      _max{merge == ScoreMergeType::Max} {
    SDB_ASSERT(merge != ScoreMergeType::Noop);
  }

  ConstantScored(ConstantScored&&) = delete;
  ConstantScored& operator=(ConstantScored&&) = delete;

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores) {
    return _max ? _approx.FillMax(min, max, mask, scores, _value)
                : _approx.FillSum(min, max, mask, scores, _value);
  }

 private:
  Approx _approx;
  score_t _value;
  bool _max;
};

}  // namespace irs::fill
