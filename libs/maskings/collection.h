////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string_view>
#include <vector>

#include "basics/common.h"
#include "maskings/attribute_masking.h"
#include "maskings/collection_filter.h"
#include "maskings/collection_selection.h"
#include "maskings/parse_result.h"

namespace vpack {

class Slice;
}
namespace sdb {
namespace maskings {

class Collection {
 public:
  static ParseResult<Collection> parse(Maskings* maskings, vpack::Slice def);

 public:
  Collection() {}

  Collection(CollectionSelection selection,
             const std::vector<AttributeMasking>& maskings)
    : _selection(selection), _maskings(maskings) {}

  CollectionSelection selection() const noexcept { return _selection; }

  MaskingFunction* masking(const std::vector<std::string_view>& path) const;

 private:
  CollectionSelection _selection;
  // LATER: CollectionFilter _filter;
  std::vector<AttributeMasking> _maskings;
};

}  // namespace maskings
}  // namespace sdb
