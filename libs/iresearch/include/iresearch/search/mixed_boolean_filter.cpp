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

#include "mixed_boolean_filter.hpp"

#include "iresearch/search/boolean_query.hpp"

namespace irs {

Filter::Query::ptr MixedBooleanFilter::prepare(
  const PrepareContext& ctx) const {
  if (_and->empty()) {
    return _or->prepare(ctx);
  }
  if (_or->empty()) {
    return _and->prepare(ctx);
  }
  auto q = memory::make_tracked<BoostQuery>(ctx.memory);
  q->Prepare(ctx, *_and, *_or);
  return q;
}

bool MixedBooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<MixedBooleanFilter>(rhs);
  return *_and == *typed_rhs._and && *_or == *typed_rhs._or;
}

}  // namespace irs
