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

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/skip_walk.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
bool StepToLive(SkipWalk<InputType>& walk, InputType& in, doc_id_t live,
                uint32_t& left, doc_id_t& last) {
  if (live - last <= doc_limits::kBlockSize || !walk.Armed()) {
    return true;
  }
  const auto remaining = walk.Seek(live, in);
  if (remaining == 0) {
    left = 0;
    return false;
  }
  left = remaining;
  in.Seek(walk.Landing().doc_ptr);
  last = walk.Landing().doc;
  return true;
}

}  // namespace irs::search
