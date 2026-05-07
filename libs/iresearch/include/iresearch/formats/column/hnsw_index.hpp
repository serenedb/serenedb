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

#pragma once

#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/DistanceComputer.h>
#include <faiss/impl/HNSW.h>
#include <faiss/impl/ResultHandler.h>
#include <faiss/impl/io.h>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {

struct ColumnReader;
class IndexReader;

void WriteHNSW(DataOutput& out, const faiss::HNSW& hnsw);
void ReadHNSW(IndexInput& in, faiss::HNSW& hnsw);

uint64_t PackSegmentWithDoc(uint32_t segment, doc_id_t doc);

std::pair<uint32_t, doc_id_t> UnpackSegmentWithDoc(uint64_t id);

using HNSWResultHandler = faiss::HeapBlockResultHandler<faiss::HNSW::C>;
using HNSWRangeResultHandler =
  faiss::RangeSearchBlockResultHandler<faiss::HNSW::C>;

class HNSWSegmentResultHandler : public HNSWResultHandler::SingleResultHandler {
 public:
  explicit HNSWSegmentResultHandler(uint32_t segment_id,
                                    HNSWResultHandler& handler,
                                    float global_threshold)
    : HNSWResultHandler::SingleResultHandler{handler}, _segment_id{segment_id} {
    threshold = global_threshold;
  }

  bool add_result(float dis, int64_t idx) final {
    return HNSWResultHandler::SingleResultHandler::add_result(
      dis, PackSegmentWithDoc(_segment_id, static_cast<doc_id_t>(idx)));
  }

 private:
  uint32_t _segment_id;
};

class HNSWRangeSegmentResultHandler
  : public HNSWRangeResultHandler::SingleResultHandler {
 public:
  explicit HNSWRangeSegmentResultHandler(uint32_t segment_id,
                                         HNSWRangeResultHandler& handler)
    : HNSWRangeResultHandler::SingleResultHandler{handler},
      _segment_id{segment_id} {}

  bool add_result(float dis, int64_t idx) final {
    return HNSWRangeResultHandler::SingleResultHandler::add_result(
      dis, PackSegmentWithDoc(_segment_id, static_cast<doc_id_t>(idx)));
  }

 private:
  uint32_t _segment_id;
};

class ColumnDistanceBase : public faiss::DistanceComputer {
 public:
  explicit ColumnDistanceBase(HNSWMetric metric, int32_t dim);

  void set_query(const float* x) final {
    SDB_ASSERT(x != nullptr);
    _q = x;
  }

 protected:
  const float* LoadData(faiss::idx_t id, ResettableDocIterator::ptr& it);

  const float* _q = nullptr;
  float (*const _dist)(const byte_type*, const byte_type*, uint16_t) = nullptr;
  int32_t _dim;
};

class ColumnSearchDistance : public ColumnDistanceBase {
 public:
  explicit ColumnSearchDistance(ResettableDocIterator::ptr&& it, HNSWInfo info);

  float operator()(faiss::idx_t id) final;

  float symmetric_dis(faiss::idx_t i, faiss::idx_t j) final {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "symmetric distance is not supported in search distance");
  }

 private:
  ResettableDocIterator::ptr _it;
};

class ColumnIndexDistance final : public ColumnDistanceBase {
 public:
  explicit ColumnIndexDistance(ResettableDocIterator::ptr&& lit,
                               ResettableDocIterator::ptr&& rit, HNSWInfo info);

  float operator()(faiss::idx_t id) final;

  float symmetric_dis(faiss::idx_t i, faiss::idx_t j) final;

  void Update(
    absl::AnyInvocable<void(ResettableDocIterator::ptr&)>& update_iterator) {
    update_iterator(_lit);
    update_iterator(_rit);
  }

 private:
  ResettableDocIterator::ptr _lit;
  ResettableDocIterator::ptr _rit;
};

struct HNSWSearchBuffer {
  std::span<float> dis;
  std::span<int64_t> ids;
  float max_dist;
  faiss::VisitedTable vt{0};

  HNSWSearchBuffer(float* dis_data, int64_t* ids_data, size_t size,
                   float max_dist = std::numeric_limits<float>::max())
    : dis{dis_data, size}, ids{ids_data, size}, max_dist{max_dist} {
    ResetValues();
  }

  void ReorderResult() {
    faiss::heap_reorder<faiss::HNSW::C>(dis.size(), dis.data(), ids.data());
  }

  void ResetValues() {
    std::ranges::fill(dis, max_dist);
    std::ranges::fill(ids, -1);
  }
};

struct HNSWSearchInfo {
  const byte_type* query;
  size_t top_k;
  faiss::SearchParametersHNSW params;
  float global_threshold = faiss::HNSW::C::neutral();
};

struct HNSWSearchContext {
  HNSWSearchInfo info;
  uint32_t segment_id;
  faiss::VisitedTable& vt;
  HNSWResultHandler& handler;
};

struct HNSWRangeSearchInfo {
  const byte_type* query;
  float radius;
  faiss::SearchParametersHNSW params;
};

struct HNSWRangeSearchBuffer {
  std::vector<float> dis;
  std::vector<int64_t> ids;
  faiss::VisitedTable vt{0};
};

struct HNSWRangeSearchContext {
  HNSWRangeSearchInfo info;
  uint32_t segment_id;
  faiss::VisitedTable& vt;
  HNSWRangeResultHandler& handler;
};

class HNSWIndexWriter {
 public:
  explicit HNSWIndexWriter(
    HNSWInfo info,
    absl::AnyInvocable<ResettableDocIterator::ptr()> make_iterator,
    absl::AnyInvocable<void(ResettableDocIterator::ptr&)> update_iterator)
    : _max_doc{info.max_doc},
      _hnsw{info.m},
      _vt{info.max_doc + 1},
      _dis{make_iterator(), make_iterator(), info},
      _update_iterator{std::move(update_iterator)} {
    _hnsw.efConstruction = info.ef_construction;
    _hnsw.prepare_level_tab(_max_doc + 1, false);
  }

  void Add(const float* data, doc_id_t doc);

  void Serialize(DataOutput& out) const { WriteHNSW(out, _hnsw); }

 private:
  doc_id_t _max_doc;
  faiss::HNSW _hnsw;
  faiss::VisitedTable _vt;
  ColumnIndexDistance _dis;
  absl::AnyInvocable<void(ResettableDocIterator::ptr&)> _update_iterator;
};

class HNSWIndexReader {
 public:
  explicit HNSWIndexReader(faiss::HNSW&& hnsw, const ColumnReader& reader,
                           HNSWInfo info);

  void Search(HNSWSearchContext& context) const;
  void RangeSearch(HNSWRangeSearchContext& context) const;

 private:
  friend class HNSWIterator;
  mutable faiss::HNSW _hnsw;  // can change search parameters
  const HNSWInfo _info;
  const ColumnReader& _reader;
};

}  // namespace irs
