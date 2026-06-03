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

#include "iresearch/search/all_docs_provider.hpp"
#include "iresearch/search/filter.hpp"

namespace irs {

class Exclude final : public FilterWithType<Exclude>, public AllDocsProvider {
 public:
  const Filter* Child() const noexcept { return _child.get(); }

  Filter::ptr& ChildSlot() noexcept { return _child; }

  void clear() { _child.reset(); }
  bool empty() const noexcept { return nullptr == _child; }

  Query::ptr prepare(const PrepareContext& ctx) const final;

 protected:
  bool equals(const Filter& rhs) const noexcept final;

 private:
  Filter::ptr _child;
};

}  // namespace irs
