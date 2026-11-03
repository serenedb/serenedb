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

#include <memory>
#include <utility>

#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/small_vector.hpp"

namespace irs {

inline constexpr size_t kPrefetchedExtents = 64;

using FileRanges = containers::SmallVector<FileRange, kPrefetchedExtents>;

template<typename Input, typename Metas>
void PrefetchDocExtents(const Input& in, Metas&& metas) {
  FileRanges extents;
  for (const PostingMeta& meta : metas) {
    if (const auto extent = DocExtent(meta); extent != 0) {
      extents.emplace_back(meta.doc_start, meta.doc_start + extent);
    }
  }
  absl::c_sort(extents);
  FileRanges ranges;
  for (const auto& [from, to] : extents) {
    AppendRange(ranges, from, to);
  }
  HintRanges(in, ranges);
}

template<typename InputType>
InputType OpenDocInput(const PostingMeta& meta, const InputFile& doc_in) {
  if (meta.inline_size != 0) {
    return InputType{meta.Inline()};
  }
  InputType in{doc_in};
  in.Seek(meta.doc_start);
  return in;
}

}  // namespace irs
