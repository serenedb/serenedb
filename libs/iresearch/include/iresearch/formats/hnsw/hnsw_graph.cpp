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

#include "iresearch/formats/hnsw/hnsw_graph.hpp"

#include <faiss/utils/distances.h>

#include <cmath>

#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

template<typename T>
void WriteArray(DataOutput& out, const std::vector<T>& v) {
  out.WriteU64(v.size());
  if (!v.empty()) {
    out.WriteData(reinterpret_cast<const byte_type*>(v.data()),
                  sizeof(T) * v.size());
  }
}

const float* BatchRow(const float* base, uint32_t d, uint32_t id) noexcept {
  return base + static_cast<size_t>(id) * d;
}

template<typename T>
void ReadArray(IndexInput& in, std::vector<T>& v) {
  const auto size = static_cast<size_t>(in.ReadI64());
  v.resize(size);
  if (size != 0) {
    in.ReadData(reinterpret_cast<byte_type*>(v.data()), sizeof(T) * size);
  }
}

}  // namespace

void HnswGraph::Reset(size_t nodes, uint32_t m) {
  _m = m != 0 ? m : kHnswDefaultM;
  _m0 = 2 * _m;
  _levels.assign(nodes, 0);
  _offsets.clear();
  _neighbors.clear();
  _entry = kHnswInvalidNode;
  _max_level = 0;
}

void HnswGraph::AllocateLinks() {
  _offsets.resize(_levels.size() + 1);
  uint64_t total = 0;
  for (size_t i = 0; i < _levels.size(); ++i) {
    _offsets[i] = total;
    if (_levels[i] != 0) {
      total += _m0 + static_cast<uint64_t>(_levels[i] - 1) * _m;
    }
  }
  _offsets.back() = total;
  _neighbors.assign(total, kHnswInvalidNode);
}

size_t HnswGraph::ByteSize() const noexcept {
  return _levels.size() * sizeof(uint8_t) + _offsets.size() * sizeof(uint64_t) +
         _neighbors.size() * sizeof(uint32_t);
}

void HnswGraph::Serialize(DataOutput& out) const {
  out.WriteU32(_m);
  out.WriteU32(_entry);
  out.WriteU32(_max_level);
  WriteArray(out, _levels);
  WriteArray(out, _offsets);
  WriteArray(out, _neighbors);
}

HnswGraph HnswGraph::Deserialize(IndexInput& in) {
  HnswGraph g;
  g._m = static_cast<uint32_t>(in.ReadI32());
  g._m0 = 2 * g._m;
  g._entry = static_cast<uint32_t>(in.ReadI32());
  g._max_level = static_cast<uint32_t>(in.ReadI32());
  ReadArray(in, g._levels);
  ReadArray(in, g._offsets);
  ReadArray(in, g._neighbors);
  return g;
}

void HnswBatchL2Sqr(const float* q, const float* base, uint32_t d,
                    std::span<const uint32_t> ids, score_t* out) noexcept {
  size_t i = 0;
  for (; i + 4 <= ids.size(); i += 4) {
    float d0 = 0.f;
    float d1 = 0.f;
    float d2 = 0.f;
    float d3 = 0.f;
    faiss::fvec_L2sqr_batch_4(q, BatchRow(base, d, ids[i]),
                              BatchRow(base, d, ids[i + 1]),
                              BatchRow(base, d, ids[i + 2]),
                              BatchRow(base, d, ids[i + 3]), d, d0, d1, d2, d3);
    out[i] = -d0;
    out[i + 1] = -d1;
    out[i + 2] = -d2;
    out[i + 3] = -d3;
  }
  for (; i < ids.size(); ++i) {
    out[i] = -faiss::fvec_L2sqr(q, BatchRow(base, d, ids[i]), d);
  }
}

void HnswBatchIp(const float* q, const float* base, uint32_t d,
                 std::span<const uint32_t> ids, score_t* out) noexcept {
  size_t i = 0;
  for (; i + 4 <= ids.size(); i += 4) {
    float d0 = 0.f;
    float d1 = 0.f;
    float d2 = 0.f;
    float d3 = 0.f;
    faiss::fvec_inner_product_batch_4(
      q, BatchRow(base, d, ids[i]), BatchRow(base, d, ids[i + 1]),
      BatchRow(base, d, ids[i + 2]), BatchRow(base, d, ids[i + 3]), d, d0, d1,
      d2, d3);
    out[i] = d0;
    out[i + 1] = d1;
    out[i + 2] = d2;
    out[i + 3] = d3;
  }
  for (; i < ids.size(); ++i) {
    out[i] = faiss::fvec_inner_product(q, BatchRow(base, d, ids[i]), d);
  }
}

uint32_t HnswRandomLevel(uint64_t& rng_state, uint32_t m) noexcept {
  rng_state ^= rng_state << 13;
  rng_state ^= rng_state >> 7;
  rng_state ^= rng_state << 17;
  const double u =
    static_cast<double>(rng_state >> 11) / static_cast<double>(1ULL << 53);
  const double factor = 1.0 / std::log(static_cast<double>(std::max(m, 2U)));
  const double sample = -std::log(u > 0.0 ? u : 1e-12) * factor;
  return static_cast<uint32_t>(sample) + 1;
}

}  // namespace irs
