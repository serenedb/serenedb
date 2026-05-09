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

#include "iresearch/formats/column/hnsw_index.hpp"

#include <cmath>
#include <type_traits>

#include "basics/system-compiler.h"
#include "iresearch/formats/column/common.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/vector.hpp"

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

struct L2SqrDist {
  static float Compute(const byte_type* l, const byte_type* r, uint16_t d) {
    return irs::vector::L2Space<float, float, float>::Dist(l, r, d);
  }
};

struct NegativeIPDist {
  static float Compute(const byte_type* l, const byte_type* r, uint16_t d) {
    return -irs::vector::DotProductImpl<float, float>::Compute(l, r, d);
  }
};

struct L1Dist {
  static float Compute(const byte_type* l, const byte_type* r, uint16_t d) {
    return irs::vector::L1Space<float, float, float>::Dist(l, r, d);
  }
};

struct CosineDist {
  static float Compute(const byte_type* l, const byte_type* r, uint16_t d) {
    const auto [ll, lr, rr] =
      irs::vector::CosineDistanceImpl<float, float, float>::Compute(l, r, d);
    const float denom = std::sqrt(ll) * std::sqrt(rr);
    return denom == 0.f ? 1.f : 1.f - lr / denom;
  }
};

template<class F>
decltype(auto) DispatchMetric(HNSWMetric metric, F&& f) {
  switch (metric) {
    case HNSWMetric::L2Sqr:
      return f(std::type_identity<L2SqrDist>{});
    case HNSWMetric::NegativeIP:
      return f(std::type_identity<NegativeIPDist>{});
    case HNSWMetric::L1:
      return f(std::type_identity<L1Dist>{});
    case HNSWMetric::Cosine:
      return f(std::type_identity<CosineDist>{});
  }
  SDB_UNREACHABLE();
}

template<typename Dist>
class HNSWIndexWriterImpl final : public HNSWIndexWriter {
 public:
  HNSWIndexWriterImpl(
    HNSWInfo info,
    absl::AnyInvocable<ResettableDocIterator::ptr()> make_iterator,
    absl::AnyInvocable<void(ResettableDocIterator::ptr&)> update_iterator)
    : _max_doc{info.max_doc},
      _hnsw{info.m},
      _vt(info.max_doc + 1),
      _dis{make_iterator(), make_iterator(), info},
      _update_iterator{std::move(update_iterator)} {
    _hnsw.efConstruction = info.ef_construction;
    _hnsw.prepare_level_tab(_max_doc + 1, false);
  }

  void Add(const float* data, doc_id_t doc) final {
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

  void Serialize(DataOutput& out) const final { WriteHNSW(out, _hnsw); }

 private:
  doc_id_t _max_doc;
  faiss::HNSW _hnsw;
  faiss::VisitedTable _vt;
  ColumnIndexDistance<Dist> _dis;
  absl::AnyInvocable<void(ResettableDocIterator::ptr&)> _update_iterator;
};

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
  auto* payload = irs::get<PayAttr>(*it);
  SDB_ASSERT(payload);
  SDB_ASSERT(payload->value.size() == _dim * sizeof(float));
  return reinterpret_cast<const float*>(payload->value.data());
}

std::unique_ptr<HNSWIndexWriter> MakeHNSWIndexWriter(
  HNSWInfo info,
  absl::AnyInvocable<ResettableDocIterator::ptr()> make_iterator,
  absl::AnyInvocable<void(ResettableDocIterator::ptr&)> update_iterator) {
  return DispatchMetric(info.metric, [&](auto tag) {
    using Dist = typename decltype(tag)::type;
    return std::unique_ptr<HNSWIndexWriter>{new HNSWIndexWriterImpl<Dist>{
      std::move(info), std::move(make_iterator), std::move(update_iterator)}};
  });
}

HNSWIndexReader::HNSWIndexReader(faiss::HNSW&& hnsw, const ColumnReader& reader,
                                 HNSWInfo info)
  : _hnsw{std::move(hnsw)}, _info{std::move(info)}, _reader{reader} {}

void HNSWIndexReader::Search(HNSWSearchContext& context) const {
  DispatchMetric(_info.metric, [&](auto tag) {
    using Dist = typename decltype(tag)::type;
    // TODO(codeworse): support other value types
    ColumnSearchDistance<Dist> dis{_reader.iterator(ColumnHint::Normal), _info};
    dis.set_query(reinterpret_cast<const float*>(context.info.query));

    HNSWSegmentResultHandler res{context.segment_id, context.handler,
                                 context.info.global_threshold};
    context.vt.visited.resize(_hnsw.levels.size(), 0);
    context.vt.advance();
    res.begin(0, false);
    _hnsw.search(dis, nullptr, res, context.vt, &context.info.params);
  });
}

void HNSWIndexReader::RangeSearch(HNSWRangeSearchContext& context) const {
  DispatchMetric(_info.metric, [&](auto tag) {
    using Dist = typename decltype(tag)::type;
    ColumnSearchDistance<Dist> dis{_reader.iterator(ColumnHint::Normal), _info};
    dis.set_query(reinterpret_cast<const float*>(context.info.query));

    HNSWRangeSegmentResultHandler res{context.segment_id, context.handler};
    context.vt.visited.resize(_hnsw.levels.size(), 0);
    context.vt.advance();
    res.begin(0);
    _hnsw.search(dis, nullptr, res, context.vt, &context.info.params);
  });
}

}  // namespace irs
