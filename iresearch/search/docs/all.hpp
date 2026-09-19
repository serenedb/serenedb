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

#include <algorithm>

#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

class All : public Root {
 public:
  explicit All(doc_id_t count) noexcept : _end{doc_limits::min() + count} {}

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* IRS_RESTRICT out) final {
    const auto stop = std::min(max, _end);
    const auto n = min < stop ? static_cast<uint32_t>(stop - min) : 0;
    for (uint32_t i = 0; i != n; ++i) {
      out[i] = min + i;
    }
    return n;
  }

 private:
  doc_id_t _end;
};

}  // namespace irs::docs
