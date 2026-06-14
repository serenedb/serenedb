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

#include "basics/assert.h"
#include "iresearch/search/boolean_filter.hpp"

namespace irs {

class MixedBooleanFilter final : public FilterWithType<MixedBooleanFilter>,
                                 public AllDocsProvider {
 public:
  MixedBooleanFilter() : MixedBooleanFilter({}, {}) {}

  MixedBooleanFilter(std::vector<Filter::ptr> required,
                     std::vector<Filter::ptr> optional)
    : _and{std::make_unique<And>()}, _or{std::make_unique<Or>()} {
    auto& and_node = sdb::basics::downCast<And>(*_and);
    for (auto& filter : required) {
      and_node.add(std::move(filter));
    }
    auto& or_node = sdb::basics::downCast<Or>(*_or);
    for (auto& filter : optional) {
      or_node.add(std::move(filter));
    }
  }

  MixedBooleanFilter(MixedBooleanFilter&&) = default;
  MixedBooleanFilter& operator=(MixedBooleanFilter&&) = default;

  auto& GetRequired(this auto& self) noexcept {
    SDB_VERIFY(self._and->type() == irs::Type<And>::id());
    return sdb::basics::downCast<And>(*self._and);
  }
  auto& GetOptional(this auto& self) noexcept {
    SDB_VERIFY(self._or->type() == irs::Type<Or>::id());
    return sdb::basics::downCast<Or>(*self._or);
  }

  Filter::ptr& RequiredSlot() noexcept { return _and; }
  Filter::ptr& OptionalSlot() noexcept { return _or; }

  bool empty() const noexcept {
    return HasNoClauses(*_and) && HasNoClauses(*_or);
  }

  Query::ptr prepare(const PrepareContext& ctx) const final;

 private:
  static bool HasNoClauses(const Filter& filter) noexcept {
    const auto tid = filter.type();
    if (tid == irs::Type<And>::id() || tid == irs::Type<Or>::id()) {
      return sdb::basics::downCast<BooleanFilter>(filter).empty();
    }
    return false;
  }

  bool equals(const Filter& rhs) const noexcept final;

  Filter::ptr _and;
  Filter::ptr _or;
};

}  // namespace irs
