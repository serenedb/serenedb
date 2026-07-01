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

#include "iresearch/formats/hnsw/hnsw_reader.hpp"

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/column_distance.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

template<typename Info, typename Handler, typename Begin>
void RunSearch(const faiss::HNSW& hnsw, const HNSWInfo& hnsw_info,
               const Info& info, ChunkedVectorCache& cache,
               faiss::VisitedTable& vt, Handler& res, Begin&& begin) {
  ColumnDistance dis{hnsw_info, &cache};
  dis.set_query(reinterpret_cast<const float*>(info.query));
  vt.visited.resize(hnsw.levels.size(), 0);
  vt.advance();
  begin(res);
  hnsw.search(dis, nullptr, res, vt, &info.params);
}

template<typename T>
void ReadVector(IndexInput& in, T& vec) {
  uint32_t size = irs::read<uint32_t>(in);
  vec.resize(size);
  if (size == 0) {
    return;
  }
  in.ReadData(reinterpret_cast<byte_type*>(vec.data()),
              sizeof(*vec.data()) * size);
}

}  // namespace

void ReadHNSW(IndexInput& in, faiss::HNSW& hnsw) {
  ReadVector(in, hnsw.assign_probas);
  ReadVector(in, hnsw.cum_nneighbor_per_level);
  ReadVector(in, hnsw.levels);
  ReadVector(in, hnsw.offsets);
  ReadVector(in, hnsw.neighbors);

  hnsw.entry_point = irs::read<int32_t>(in);
  hnsw.max_level = irs::read<int>(in);
  hnsw.efConstruction = irs::read<int>(in);
  hnsw.efSearch = irs::read<int>(in);
}

uint64_t PackSegmentWithDoc(uint32_t segment, doc_id_t doc) {
  return (static_cast<uint64_t>(segment) << 32) | static_cast<uint64_t>(doc);
}

std::pair<uint32_t, doc_id_t> UnpackSegmentWithDoc(uint64_t id) {
  uint32_t segment = static_cast<uint32_t>(id >> 32);
  doc_id_t doc =
    static_cast<doc_id_t>(id & std::numeric_limits<uint32_t>::max());
  return {segment, doc};
}

HnswReader::HnswReader(field_id id, std::shared_ptr<const faiss::HNSW> hnsw,
                       HNSWInfo info, const ColumnReader& vector_column)
  : _id{id},
    _info{std::move(info)},
    _vector_column{vector_column},
    _hnsw{std::move(hnsw)} {
  SDB_ENSURE(vector_column.ArraySize() == static_cast<uint64_t>(_info.d),
             sdb::ERROR_INTERNAL, "HnswReader: ARRAY size ",
             vector_column.ArraySize(), " does not match HNSWInfo.d ", _info.d);
}

HnswReader::~HnswReader() = default;

void HnswReader::Search(HNSWSearchContext& ctx) const {
  HNSWSegmentResultHandler res{ctx.segment_id, ctx.handler,
                               ctx.info.global_threshold, ctx.docs_mask};
  RunSearch(*_hnsw, _info, ctx.info, ctx.cache, ctx.vt, res,
            [](HNSWSegmentResultHandler& r) { r.begin(0, false); });
}

void HnswReader::RangeSearch(HNSWRangeSearchContext& ctx) const {
  HNSWRangeSegmentResultHandler res{ctx.segment_id, ctx.handler, ctx.docs_mask};
  RunSearch(*_hnsw, _info, ctx.info, ctx.cache, ctx.vt, res,
            [](HNSWRangeSegmentResultHandler& r) { r.begin(0); });
}

ChunkedVectorCache& HnswReader::PrepareCache(ChunkedVectorCache& slot,
                                             ReadContext& ctx) const {
  const auto* child = _vector_column.Child();
  SDB_ASSERT(child);
  slot.Rebind(*child, _vector_column.ArraySize(), ctx);
  return slot;
}

}  // namespace irs
