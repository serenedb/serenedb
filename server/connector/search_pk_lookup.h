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

// Walks segments + doc_ids (parallel ranges) in (segment, doc_id) sorted
// order, calling `sink(orig_idx, pk_view)` per resolved row. Unresolved rows
// (segment open or seek failure) are silently dropped -- caller fills its
// own sentinel if needed.
template<typename Segments, typename DocIds, typename Sink>
void LookupSegmentsValues(const Segments& segments, const DocIds& doc_ids,
                          const irs::IndexReader& reader, size_t n,
                          Sink&& sink) {
  std::vector<size_t> idx(n);
  std::iota(idx.begin(), idx.end(), 0);
  std::ranges::sort(
    idx, {}, [&](size_t i) { return std::pair{segments[i], doc_ids[i]}; });

  size_t i = 0;
  while (i < n) {
    const auto seg_id = segments[idx[i]];
    SegmentPkIterator it;
    const bool opened = OpenSegmentPkIterator(reader[seg_id], it);
    if (!opened) {
      while (i < n && segments[idx[i]] == seg_id) {
        ++i;
      }
      continue;
    }
    while (i < n && segments[idx[i]] == seg_id) {
      const auto doc_id = doc_ids[idx[i]];
      const bool seeked = it.iter->seek(doc_id) == doc_id;
      if (!seeked) {
        ++i;
        continue;
      }
      const auto& val = it.value->value;
      sink(idx[i], std::string_view{reinterpret_cast<const char*>(val.data()),
                                    val.size()});
      ++i;
    }
  }
}

}  // namespace sdb::connector
