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
#include "iresearch/utils/string.hpp"

namespace irs {

class ByColumnExistence;

using ColumnAcceptor = bool (*)(std::string_view prefix, std::string_view name);

// Options for column existence filter
struct ByColumnExistenceOptions {
  using FilterType = ByColumnExistence;

  // If set approves column matched the specified prefix
  ColumnAcceptor acceptor{};

  bool operator==(const ByColumnExistenceOptions& rhs) const noexcept {
    return acceptor == rhs.acceptor;
  }
};

// User-side column existence filter
class ByColumnExistence final
  : public FilterWithField<ByColumnExistenceOptions> {
 public:
  Query::ptr prepare(const PrepareContext& ctx) const final;
};

}  // namespace irs
