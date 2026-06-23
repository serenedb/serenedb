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

#include "connector/duckdb_ann_filter.h"

#include <iresearch/formats/hnsw/hnsw_reader.hpp>

#include "basics/assert.h"

namespace sdb::connector {

TextScanFilter::TextScanFilter(const irs::Filter& filter,
                               irs::PrepareCollector& collector)
  : _filter{filter}, _collector{collector} {}

void TextScanFilter::Reset(const irs::SubReader& segment) {
  _query = _filter.PrepareSegment(segment, {.collector = &_collector});
  SDB_ASSERT(_query);
  _it = _query->Execute({}, irs::StatsBuffer::Empty());
  SDB_ASSERT(_it);
}

bool TextScanFilter::is_member(faiss::idx_t id) const {
  auto [_, doc_id] = irs::UnpackSegmentWithDoc(id);
  return _it->seek(doc_id) == doc_id;
}

}  // namespace sdb::connector
