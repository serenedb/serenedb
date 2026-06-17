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
  auto& GetRequired() const noexcept {
    SDB_VERIFY(RequiredSlot()->type() == irs::Type<And>::id());
    return sdb::basics::downCast<And>(*RequiredSlot());
  }
  auto& GetOptional() const noexcept {
    SDB_VERIFY(OptionalSlot()->type() == irs::Type<Or>::id());
    return sdb::basics::downCast<Or>(*OptionalSlot());
  }

  MixedBooleanFilter() : MixedBooleanFilter({}, {}) {}

  MixedBooleanFilter(std::vector<Filter::ptr> required,
                     std::vector<Filter::ptr> optional)
    : _filters{{std::make_unique<And>(std::move(required)),
                std::make_unique<Or>(std::move(optional))}} {}

  MixedBooleanFilter(MixedBooleanFilter&&) = default;
  MixedBooleanFilter& operator=(MixedBooleanFilter&&) = default;

  const Filter::ptr& RequiredSlot() const noexcept { return _filters[0]; }
  const Filter::ptr& OptionalSlot() const noexcept { return _filters[1]; }
  Filter::ptr& RequiredSlot() noexcept { return _filters[0]; }
  Filter::ptr& OptionalSlot() noexcept { return _filters[1]; }

  bool empty() const noexcept {
    return HasNoClauses(*RequiredSlot()) && HasNoClauses(*OptionalSlot());
  }

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;

  std::span<Filter::ptr> GetChildren() final { return _filters; }

 private:
  static bool HasNoClauses(const Filter& filter) noexcept {
    const auto tid = filter.type();
    if (tid == irs::Type<And>::id() || tid == irs::Type<Or>::id()) {
      return sdb::basics::downCast<BooleanFilter>(filter).empty();
    }
    return false;
  }

  bool equals(const Filter& rhs) const noexcept final;

  // [_required, _optional]
  std::array<Filter::ptr, 2> _filters;
};

}  // namespace irs
