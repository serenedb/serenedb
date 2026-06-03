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

#include "exclude_filter.hpp"

#include "iresearch/search/boolean_query.hpp"

namespace irs {

Filter::Query::ptr Exclude::prepare(const PrepareContext& ctx) const {
  if (!_child) {
    return Query::empty();
  }

  const PrepareContext sub_ctx = ctx.Boost(Boost());

  auto all_docs = MakeAllDocsFilter(kNoBoost);
  const std::array<const irs::Filter*, 1> incl{all_docs.get()};
  const std::array<const irs::Filter*, 1> excl{_child.get()};

  auto q = memory::make_tracked<AndQuery>(sub_ctx.memory);
  q->prepare(sub_ctx, ScoreMergeType::Sum, incl, excl);
  return q;
}

bool Exclude::equals(const irs::Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<Exclude>(rhs);
  return (!empty() && !typed_rhs.empty() && *_child == *typed_rhs._child) ||
         (empty() && typed_rhs.empty());
}

}  // namespace irs
