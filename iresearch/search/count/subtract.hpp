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

#include <cstdint>
#include <utility>

#include "iresearch/search/count/root.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::count {

class Subtract : public Root {
 public:
  Subtract(Root::ptr total, Root::ptr excluded) noexcept
    : _total{std::move(total)}, _excluded{std::move(excluded)} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    const auto total = _total->Run(min, max);
    const auto excluded = _excluded->Run(min, max);
    SDB_ASSERT(excluded <= total);
    return total - excluded;
  }

 private:
  Root::ptr _total;
  Root::ptr _excluded;
};

}  // namespace irs::count
