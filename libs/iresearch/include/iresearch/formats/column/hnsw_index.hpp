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
  explicit ColumnDistanceBase(int32_t dim) : _dim{dim} {}

  void set_query(const float* x) final {
    SDB_ASSERT(x != nullptr);
    _q = x;
  }

 protected:
  const float* LoadData(faiss::idx_t id, ResettableDocIterator::ptr& it);

  const float* _q = nullptr;
  int32_t _dim;
};

template<typename Dist>
class ColumnSearchDistance final : public ColumnDistanceBase {
 public:
  explicit ColumnSearchDistance(ResettableDocIterator::ptr&& it, HNSWInfo info)
    : ColumnDistanceBase{info.d}, _it{std::move(it)} {}

  float operator()(faiss::idx_t id) final {
    const float* data = LoadData(id, _it);
    const auto* lhs = reinterpret_cast<const irs::byte_type*>(_q);
    const auto* rhs = reinterpret_cast<const irs::byte_type*>(data);
    const auto d = static_cast<uint16_t>(_dim);
    return Dist::Compute(lhs, rhs, d);
  }

  float symmetric_dis(faiss::idx_t i, faiss::idx_t j) final {
    SDB_THROW(sdb::ERROR_INTERNAL,
              "symmetric distance is not supported in search distance");
  }

 private:
  ResettableDocIterator::ptr _it;
};

template<typename Dist>
class ColumnIndexDistance final : public ColumnDistanceBase {
 public:
  explicit ColumnIndexDistance(ResettableDocIterator::ptr&& lit,
                               ResettableDocIterator::ptr&& rit, HNSWInfo info)
    : ColumnDistanceBase{info.d},
      _lit{std::move(lit)},
      _rit{std::move(rit)} {}

  float operator()(faiss::idx_t id) final {
    const float* data = LoadData(id, _lit);
    const auto* lhs = reinterpret_cast<const irs::byte_type*>(_q);
    const auto* rhs = reinterpret_cast<const irs::byte_type*>(data);
    const auto d = static_cast<uint16_t>(_dim);
    return Dist::Compute(lhs, rhs, d);
  }

  float symmetric_dis(faiss::idx_t i, faiss::idx_t j) final {
    const float* data_i = LoadData(i, _lit);
    const float* data_j = LoadData(j, _rit);
    const auto* lhs = reinterpret_cast<const irs::byte_type*>(data_i);
    const auto* rhs = reinterpret_cast<const irs::byte_type*>(data_j);
    const auto d = static_cast<uint16_t>(_dim);
    return Dist::Compute(lhs, rhs, d);
  }

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
  virtual ~HNSWIndexWriter() = default;

  virtual void Add(const float* data, doc_id_t doc) = 0;

  virtual void Serialize(DataOutput& out) const = 0;
};

std::unique_ptr<HNSWIndexWriter> MakeHNSWIndexWriter(
  HNSWInfo info,
  absl::AnyInvocable<ResettableDocIterator::ptr()> make_iterator,
  absl::AnyInvocable<void(ResettableDocIterator::ptr&)> update_iterator);

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
