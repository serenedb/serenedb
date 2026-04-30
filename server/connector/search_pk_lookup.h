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
#include <optional>
#include <ranges>
#include <string>
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

void LookupSegmentsValues(const auto& segments, const auto& doc_ids,
                          const irs::IndexReader& reader, auto& result) {
  SDB_ASSERT(result.size() == segments.size());
  SDB_ASSERT(result.size() == doc_ids.size());
  const size_t n = result.size();
  if (n == 0) {
    return;
  }

  struct Entry {
    uint32_t seg;
    irs::doc_id_t doc;
    uint32_t idx;
  };
  std::vector<Entry> entries(n);
  for (size_t i = 0; i < n; ++i) {
    entries[i] = {static_cast<uint32_t>(segments[i]),
                  static_cast<irs::doc_id_t>(doc_ids[i]),
                  static_cast<uint32_t>(i)};
  }
  absl::c_sort(entries, [](const Entry& a, const Entry& b) noexcept {
    return a.seg < b.seg || (a.seg == b.seg && a.doc < b.doc);
  });

  size_t i = 0;
  while (i < n) {
    const auto seg_id = entries[i].seg;
    SegmentPkIterator it;
    const bool opened = OpenSegmentPkIterator(reader[seg_id], it);
    if (!opened) {
      while (i < n && entries[i].seg == seg_id) {
        ++i;
      }
      continue;
    }
    auto* iter = it.iter.get();
    const auto* pay = it.value;
    while (i < n && entries[i].seg == seg_id) {
      const auto doc_id = entries[i].doc;
      if (iter->seek(doc_id) == doc_id) {
        result[entries[i].idx] = irs::ViewCast<char>(pay->value);
      }
      ++i;
    }
  }
}

}  // namespace sdb::connector
