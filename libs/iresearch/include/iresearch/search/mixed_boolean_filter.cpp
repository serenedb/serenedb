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

namespace irs {

Filter::Query::ptr MixedBooleanFilter::prepare(
  const PrepareContext& ctx) const {
  if (_or->empty()) {
    return _and->prepare(ctx);
  }
  if (_and->empty()) {
    return _or->prepare(ctx);
  }
  return _root.prepare(ctx);
}

bool MixedBooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<MixedBooleanFilter>(rhs);
  return _root == typed_rhs._root;
}

}  // namespace irs
