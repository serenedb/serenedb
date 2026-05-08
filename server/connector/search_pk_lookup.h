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
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <numeric>
#include <ranges>
#include <string_view>
#include <vector>

#include "connector/search_remove_filter.hpp"

namespace sdb::connector {

struct SegmentPkIterator {
  irs::ResettableDocIterator::ptr iter;
  const irs::PayAttr* value = nullptr;

  explicit operator bool() const noexcept {
    return static_cast<bool>(iter) && value != nullptr;
  }

  void Reset() noexcept {
    iter.reset();
    value = nullptr;
  }
};

bool OpenSegmentPkIterator(const irs::SubReader& segment,
                           SegmentPkIterator& out);

template<typename Hits, typename Proj, typename OnSegment, typename OnDoc>
void VisitSegmentsSorted(const Hits& hits, Proj&& proj,
                         std::vector<uint32_t>& scratch_idx,
                         OnSegment&& on_segment, OnDoc&& on_doc) {
  const size_t n = std::ranges::size(hits);
  scratch_idx.resize(n);
  std::iota(scratch_idx.begin(), scratch_idx.end(), uint32_t{0});
  std::ranges::sort(scratch_idx, {}, [&](uint32_t i) { return proj(hits[i]); });

  size_t i = 0;
  while (i < n) {
    const auto [seg_id, _] = proj(hits[scratch_idx[i]]);
    if (!on_segment(seg_id)) {
      while (i < n && proj(hits[scratch_idx[i]]).first == seg_id) {
        ++i;
      }
      continue;
    }
    while (i < n) {
      auto [seg, doc] = proj(hits[scratch_idx[i]]);
      if (seg != seg_id) {
        break;
      }
      on_doc(scratch_idx[i], seg, doc);
      ++i;
    }
  }
}

template<typename Hits, typename Proj, typename Sink>
void LookupSegmentsValues(const Hits& hits, Proj&& proj,
                          const irs::IndexReader& reader,
                          std::vector<uint32_t>& scratch_idx, Sink&& sink) {
  SegmentPkIterator it;
  VisitSegmentsSorted(
    hits, std::forward<Proj>(proj), scratch_idx,
    [&](uint32_t seg) { return OpenSegmentPkIterator(reader[seg], it); },
    [&](uint32_t orig, uint32_t /*seg*/, uint32_t doc) {
      if (it.iter->seek(doc) == doc) {
        sink(orig, irs::ViewCast<char>(it.value->value));
      }
    });
}

}  // namespace sdb::connector
