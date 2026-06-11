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

#include "iresearch/search/optimizer/range_rules.hpp"

#include <memory>

#include "iresearch/search/search_range.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs::optimizer {

bool RangeDegenerateRule::Apply(Filter::ptr& slot,
                                const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<ByRange>(*slot);
  const auto& rng = node.options().range;
  if (rng.min_type == BoundType::Unbounded ||
      rng.max_type == BoundType::Unbounded || rng.min != rng.max) {
    return false;
  }
  if (rng.min_type == BoundType::Inclusive &&
      rng.max_type == BoundType::Inclusive) {
    auto by_term = std::make_unique<ByTerm>();
    *by_term->mutable_field_id() = node.field_id();
    by_term->mutable_options()->term = rng.min;
    by_term->boost(node.Boost());
    slot = std::move(by_term);
    return true;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool GranularRangeDegenerateRule::Apply(Filter::ptr& slot,
                                        const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<ByGranularRange>(*slot);
  const auto& rng = node.options().range;
  if (rng.min.empty() || rng.max.empty() ||
      rng.min.front() != rng.max.front()) {
    return false;
  }
  if (rng.min_type == BoundType::Inclusive &&
      rng.max_type == BoundType::Inclusive) {
    auto by_term = std::make_unique<ByTerm>();
    *by_term->mutable_field_id() = node.field_id();
    by_term->mutable_options()->term = rng.min.front();
    by_term->boost(node.Boost());
    slot = std::move(by_term);
    return true;
  }
  slot = std::make_unique<Empty>();
  return true;
}

}  // namespace irs::optimizer
