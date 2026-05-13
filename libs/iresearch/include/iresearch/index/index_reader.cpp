////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "index_reader.hpp"

#include "basics/resource_manager.hpp"
#include "iresearch/columnstore/hnsw.hpp"

namespace irs {
namespace {

const SegmentInfo kEmptyInfo;

struct EmptySubReader final : SubReader {
  uint64_t CountMappedMemory() const final { return 0; }

  NormReader::ptr norms(field_id) const final { return {}; }
  const SegmentInfo& Meta() const final { return kEmptyInfo; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  DocIterator::ptr docs_iterator() const final { return DocIterator::empty(); }
  const irs::TermReader* field(std::string_view) const final { return nullptr; }
  irs::FieldIterator::ptr fields() const final {
    return irs::FieldIterator::empty();
  }
};

const EmptySubReader kEmpty;

}  // namespace

void IndexReader::Search(field_id field, HNSWSearchInfo info,
                         HNSWAnnSearchBuffer& buffer) const {
  for (size_t segment_id = 0; segment_id < this->size(); ++segment_id) {
    const auto& segment = (*this)[segment_id];
    segment.Search(field, std::move(info), buffer, segment_id);
  }
}

void IndexReader::RangeSearch(field_id field, HNSWRangeSearchInfo info,
                              HNSWRangeSearchBuffer& buffer) const {
  for (uint32_t segment_id = 0; segment_id < this->size(); ++segment_id) {
    const auto& segment = (*this)[segment_id];
    segment.RangeSearch(field, info, buffer, segment_id);
  }
}

void SubReader::Search(field_id field, HNSWSearchInfo info,
                       HNSWAnnSearchBuffer& buffer, uint32_t segment_id) const {
  const auto* hr = HNSW(field);
  if (hr == nullptr) {
    return;
  }
  HNSWResultHandler handler{
    1,
    buffer.dis.data(),
    buffer.ids.data(),
    info.top_k,
  };
  auto& cache = hr->PrepareCache(buffer.cache);
  HNSWSearchContext context{
    info, segment_id, buffer.vt, handler, cache,
  };
  hr->Search(context);
                            uint32_t segment_id) const {
  const auto* hr = HNSW(field);
  if (hr == nullptr) {
    return;
  }
  faiss::RangeSearchResult seg_result{1};
  HNSWRangeResultHandler handler{&seg_result, info.radius};
  auto& cache = hr->PrepareCache(buffer.cache);
  HNSWRangeSearchContext context{
    info, segment_id, buffer.vt, handler, cache,
  };
  hr->RangeSearch(context);
  size_t sz = seg_result.lims[1] - seg_result.lims[0];
  buffer.dis.reserve(buffer.dis.size() + sz);
  buffer.ids.reserve(buffer.ids.size() + sz);
  for (size_t i = seg_result.lims[0]; i < seg_result.lims[1]; ++i) {
    buffer.dis.emplace_back(seg_result.distances[i]);
    buffer.ids.emplace_back(seg_result.labels[i]);
  }
}

const SubReader& SubReader::empty() noexcept { return kEmpty; }

#ifdef SDB_DEV
IResourceManager IResourceManager::gForbidden;
#endif
IResourceManager IResourceManager::gNoop;
ResourceManagementOptions ResourceManagementOptions::gDefault;

}  // namespace irs
