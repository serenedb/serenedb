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

#include "iresearch/search/boolean_filter.hpp"

namespace irs {

class MixedBooleanFilter final : public FilterWithType<MixedBooleanFilter>,
                                 public AllDocsProvider {
 public:
  MixedBooleanFilter() : _and{&_root.add<And>()}, _or{&_root.add<Or>()} {}

  auto& GetRequired(this auto& self) noexcept { return *self._and; }

  auto& GetOptional(this auto& self) noexcept { return *self._or; }

  bool empty() const noexcept { return _and->empty() && _or->empty(); }

  Query::ptr prepare(const PrepareContext& ctx) const final;

 protected:
  bool equals(const Filter& rhs) const noexcept final;

 private:
  And _root;
  And* _and;
  Or* _or;
};

}  // namespace irs
