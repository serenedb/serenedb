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

#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

inline thread_local doc_id_t gFoldReach = 0;

class FoldReachScope {
 public:
  explicit FoldReachScope(doc_id_t reach) noexcept : _saved{gFoldReach} {
    gFoldReach = reach;
  }

  FoldReachScope(const FoldReachScope&) = delete;
  FoldReachScope& operator=(const FoldReachScope&) = delete;

  ~FoldReachScope() { gFoldReach = _saved; }

 private:
  doc_id_t _saved;
};

inline bool FoldReachesSegment(doc_id_t docs_count) noexcept {
  return gFoldReach == 0 || gFoldReach >= docs_count;
}

}  // namespace irs::detail
