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

#include <utility>

#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/docs/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::docs {

class Masked : public Root {
 public:
  Masked(Root::ptr&& plan, const DocumentMask& mask) noexcept
    : _plan{std::move(plan)}, _mask{&mask} {}

  uint32_t Run(doc_id_t* IRS_RESTRICT out, uint32_t capacity) final {
    SDB_ASSERT(capacity >= doc_limits::kMinCapacity);
    for (;;) {
      const auto n = _plan->Run(out, capacity);
      if (n == 0) {
        return 0;
      }
      uint32_t kept = 0;
      for (uint32_t i = 0; i != n; ++i) {
        const auto doc = out[i];
        out[kept] = doc;
        kept += static_cast<uint32_t>(!_mask->contains(doc));
      }
      if (kept != 0) {
        return kept;
      }
    }
  }

 private:
  Root::ptr _plan;
  const DocumentMask* _mask;
};

}  // namespace irs::docs
