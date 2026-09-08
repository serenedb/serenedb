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

#include "constant_score.hpp"

#include <absl/strings/str_cat.h>

#include "basics/down_cast.h"
#include "iresearch/search/volatile_boost_score.hpp"

namespace irs {

const ConstantScore& ForceConstScore() noexcept {
  static constexpr ConstantScore kForce;
  return kForce;
}

const ConstantScore& DefaultConstScore() noexcept {
  static constexpr ConstantScore kDefault;
  return kDefault;
}

ScoreFunction ConstantScore::PrepareScorer(const ScoreContext& ctx) const {
  return MakeVolatileBoostScore(ctx, ctx.boost * _value);
}

bool ConstantScore::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  return sdb::basics::downCast<ConstantScore>(other)._value == _value;
}

std::string ConstantScore::ToString() const {
  return absl::StrCat("constant(value=", _value, ")");
}

}  // namespace irs
