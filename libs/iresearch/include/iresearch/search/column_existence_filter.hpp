////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "filter.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class ByColumnExistence;

// Options for column existence filter
struct ByColumnExistenceOptions {
  using FilterType = ByColumnExistence;

  bool operator==(const ByColumnExistenceOptions&) const noexcept = default;
};

// User-side column existence filter
class ByColumnExistence final
  : public FilterWithOptions<ByColumnExistenceOptions> {
 public:
  field_id id() const noexcept { return _id; }
  field_id* mutable_id() noexcept { return &_id; }

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

 protected:
  bool equals(const Filter& rhs) const noexcept final {
    return FilterWithOptions<ByColumnExistenceOptions>::equals(rhs) &&
           _id == sdb::basics::downCast<ByColumnExistence>(rhs)._id;
  }

 private:
  field_id _id{irs::field_limits::invalid()};
};

}  // namespace irs
