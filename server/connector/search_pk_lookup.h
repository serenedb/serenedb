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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <optional>

#include "connector/search_remove_filter.hpp"

namespace sdb::connector {

std::optional<irs::bytes_view> LookupPkForDoc(const irs::SubReader& segment,
                                              irs::doc_id_t doc_id);

std::optional<irs::bytes_view> LookupPkForPackedId(
  const irs::IndexReader& reader, uint64_t packed_id);

struct SegmentPkIterator {
  irs::DocIterator::ptr iter;
  const irs::PayAttr* value = nullptr;

  explicit operator bool() const noexcept {
    return static_cast<bool>(iter) && value != nullptr;
  }

  void reset() noexcept {
    iter.reset();
    value = nullptr;
  }
};

bool OpenSegmentPkIterator(const irs::SubReader& segment,
                           SegmentPkIterator& out);

}  // namespace sdb::connector
