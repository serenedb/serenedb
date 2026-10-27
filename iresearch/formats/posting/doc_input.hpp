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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

template<typename Input, typename Metas>
void PrefetchDocExtents(const Input& in, Metas&& metas) {
  std::vector<ByteExtent> extents;
  for (const PostingMeta& meta : metas) {
    if (const auto extent = DocExtent(meta); extent != 0) {
      extents.emplace_back(meta.doc_start, extent);
    }
  }
  uint64_t budget = kMaxPrefetch;
  for (const auto& extent : CoalesceExtents(extents, file_utils::kPage)) {
    if (budget == 0) {
      break;
    }
    const auto size = std::min(extent.size, budget);
    Hint(in, extent.offset, size);
    budget -= size;
  }
}

template<typename InputType>
InputType OpenDocInput(const PostingMeta& meta, const IndexInput& doc_in) {
  if (meta.inline_size != 0) {
    return InputType{meta.Inline()};
  }
  InputType in{doc_in};
  in.Seek(meta.doc_start);
  return in;
}

}  // namespace irs
