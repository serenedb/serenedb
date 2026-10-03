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
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "iresearch/error/error.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

class InlineDocInput final : public BytesViewInput {
 public:
  explicit InlineDocInput(bytes_view data) noexcept {
    SDB_ASSERT(data.size() <= sizeof(_data));
    std::memcpy(_data, data.data(), data.size());
    reset(_data, data.size());
  }

  InlineDocInput(const InlineDocInput& other) noexcept
    : InlineDocInput{bytes_view{other._data, other.Length()}} {
    Seek(other.Position());
  }

  ptr Dup() const final { return std::make_unique<InlineDocInput>(*this); }

 private:
  byte_type _data[PostingMeta::kInlineBytes];
};

template<typename Metas>
void PrefetchDocExtents(const IndexInput& in, Metas&& metas) {
  std::vector<std::pair<uint64_t, uint64_t>> ranges;
  for (const PostingMeta& meta : metas) {
    if (const auto extent = DocExtent(meta); extent != 0) {
      ranges.emplace_back(meta.doc_start, meta.doc_start + extent);
    }
  }
  if (ranges.empty()) {
    return;
  }
  absl::c_sort(ranges);
  size_t n = 0;
  for (const auto& range : ranges) {
    if (n != 0 && range.first <= ranges[n - 1].second + file_utils::kPage) {
      ranges[n - 1].second = std::max(ranges[n - 1].second, range.second);
    } else {
      ranges[n++] = range;
    }
  }
  uint64_t budget = kMaxPrefetch;
  for (size_t i = 0; i != n && budget != 0; ++i) {
    const auto size = std::min(ranges[i].second - ranges[i].first, budget);
    Hint(in, ranges[i].first, size);
    budget -= size;
  }
}

inline IndexInput::ptr OpenDocInput(const PostingMeta& meta,
                                    const IndexInput& doc_in) {
  if (meta.inline_size != 0) {
    return std::make_unique<InlineDocInput>(meta.Inline());
  }
  auto in = doc_in.Reopen();
  if (!in) [[unlikely]] {
    throw IoError{"failed to reopen document input"};
  }
  in->Seek(meta.doc_start);
  return in;
}

}  // namespace irs
