////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/formats/hnsw_index.hpp"

#include <faiss/utils/distances.h>

#include "basics/system-compiler.h"
#include "iresearch/formats/columnstore2.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/attribute_provider.hpp"

namespace irs {

namespace {

template<typename T>
void Read(IndexInput& in, T& value) {
  in.ReadBytes(reinterpret_cast<byte_type*>(&value), sizeof(T));
}

template<typename T>
void WriteVector(DataOutput& out, const T& vec) {
  out.WriteU32(vec.size());
  out.WriteBytes(reinterpret_cast<const byte_type*>(vec.data()),
                 sizeof(*vec.data()) * vec.size());
}

template<typename T>
void ReadVector(IndexInput& in, T& vec) {
  uint32_t size = irs::read<uint32_t>(in);
  vec.resize(size);
  in.ReadBytes(reinterpret_cast<byte_type*>(vec.data()),
               sizeof(*vec.data()) * size);
}
}  // namespace

void WriteHNSW(DataOutput& out, const faiss::HNSW& hnsw) {
  WriteVector(out, hnsw.assign_probas);
  WriteVector(out, hnsw.cum_nneighbor_per_level);
  WriteVector(out, hnsw.levels);
  WriteVector(out, hnsw.offsets);
  WriteVector(out, hnsw.neighbors);

  out.WriteU32(hnsw.entry_point);
  out.WriteU32(hnsw.max_level);
  out.WriteU32(hnsw.efConstruction);
  out.WriteU32(hnsw.efSearch);
}

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

const float* ColumnDistanceBase::LoadData(faiss::idx_t id,
                                          ResettableDocIterator::ptr& it) {
  it->reset();
  doc_id_t doc = static_cast<doc_id_t>(id);
  auto next_doc = it->seek(doc);
  if (next_doc != doc) {
    SDB_THROW(sdb::ERROR_INTERNAL, "Failed to load vector data for doc: ", doc);
  }
  SDB_ASSERT(doc == next_doc);
  auto* payload = irs::get<irs::PayAttr>(*it);
  SDB_ASSERT(payload);
  SDB_ASSERT(payload->value.size() == _info.d * sizeof(float));
  return reinterpret_cast<const float*>(payload->value.data());
}

ColumnSearchDistance::ColumnSearchDistance(ResettableDocIterator::ptr&& it,
                                           irs::HNSWInfo info)
  : ColumnDistanceBase{std::move(info)}, _it{std::move(it)} {}

float ColumnSearchDistance::operator()(faiss::idx_t id) {
  const float* data = LoadData(id, _it);
  switch (_info.metric) {
    case faiss::MetricType::METRIC_L2:
      return faiss::fvec_L2sqr(_q, data, _info.d);
    default:
      SDB_UNREACHABLE();
  }
}

ColumnIndexDistance::ColumnIndexDistance(ResettableDocIterator::ptr&& lit,
                                         ResettableDocIterator::ptr&& rit,
                                         HNSWInfo info)
  : ColumnDistanceBase{std::move(info)},
    _lit{std::move(lit)},
    _rit{std::move(rit)} {}

float ColumnIndexDistance::operator()(faiss::idx_t id) {
  const float* data = LoadData(id, _lit);
  switch (_info.metric) {
    case faiss::MetricType::METRIC_L2:
      return faiss::fvec_L2sqr(_q, data, _info.d);
    default:
      SDB_UNREACHABLE();
  }
}

float ColumnIndexDistance::symmetric_dis(faiss::idx_t i, faiss::idx_t j) {
  const float* data_i = LoadData(i, _lit);
  const float* data_j = LoadData(j, _rit);
  switch (_info.metric) {
    case faiss::MetricType::METRIC_L2:
      return faiss::fvec_L2sqr(data_i, data_j, _info.d);
    default:
      SDB_UNREACHABLE();
  }
}

HNSWIndexReader::HNSWIndexReader(faiss::HNSW&& hnsw, const ColumnReader& reader,
                                 HNSWInfo info)
  : _hnsw{std::move(hnsw)}, _info{std::move(info)}, _reader{reader} {}

void HNSWIndexReader::Search(HNSWSearchContext& context) const {
  // TODO(codeworse): support other value types
  ColumnSearchDistance dis{_reader.iterator(ColumnHint::Normal), _info};
  dis.set_query(reinterpret_cast<const float*>(context.info.query));

  HNSWSegmentResultHandler res{context.segment_id, context.handler};
  context.vt.visited.resize(_hnsw.levels.size(), 0);
  context.vt.advance();
  res.Begin(0, false);
  _hnsw.search(dis, nullptr, res, context.vt, &context.info.params);
}

void HNSWIndexWriter::Add(const float* data, doc_id_t doc) {
  if (doc >= _vt.visited.size()) {
    size_t prev_size = _vt.visited.size();
    size_t next_size = doc == 0 ? 1 : _vt.visited.size() * 2;
    while (next_size <= doc) {
      next_size *= 2;
    }
    _vt.visited.resize(next_size, 0);
    _hnsw.prepare_level_tab(next_size - prev_size, false);
  }

  _max_doc = std::max(_max_doc, doc);
  SDB_ASSERT(_vt.visited.size() >= _max_doc + 1);
  SDB_ASSERT(_hnsw.levels.size() >= _max_doc + 1);

  faiss::idx_t id = static_cast<faiss::idx_t>(doc);
  _dis.Update(_update_iterator);
  _dis.set_query(data);
  int level = _hnsw.levels[id] - 1;
  _vt.advance();
  _hnsw.add_with_locks(_dis, level, id, _vt, false);
}

}  // namespace irs
