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

#include "iresearch/formats/ivf/centroids.hpp"

#include <algorithm>
#include <utility>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

enum class CentroidKind : uint8_t { Flat = 0 };

constexpr uint64_t kHeaderSize = 2 * sizeof(uint8_t) + 2 * sizeof(uint32_t);

}  // namespace

FlatCentroids::FlatCentroids(VectorMetric metric, uint32_t nlist, uint32_t d,
                             std::vector<float> centroids)
  : _metric{metric}, _nlist{nlist}, _d{d}, _centroids{std::move(centroids)} {}

void FlatCentroids::Search(std::span<const float> query, uint32_t nprobe,
                           std::vector<uint32_t>& out) const {
  if (_nlist == 0) {
    return;
  }
  nprobe = std::min<uint32_t>(std::max<uint32_t>(nprobe, 1), _nlist);

  const auto dist = ResolveVectorDistance(_metric);
  const bool nearest_is_largest = VectorMetricNearestIsLargest(_metric);
  const auto* q = reinterpret_cast<const byte_type*>(query.data());
  const auto d = static_cast<uint16_t>(_d);

  std::vector<std::pair<float, uint32_t>> scored;
  scored.reserve(_nlist);
  for (uint32_t c = 0; c < _nlist; ++c) {
    const auto* cv = reinterpret_cast<const byte_type*>(Centroid(c));
    scored.emplace_back(dist(q, cv, d), c);
  }

  const auto mid = scored.begin() + nprobe;
  std::partial_sort(
    scored.begin(), mid, scored.end(),
    [nearest_is_largest](const auto& l, const auto& r) noexcept {
      return nearest_is_largest ? l.first > r.first : l.first < r.first;
    });

  out.reserve(out.size() + nprobe);
  for (auto it = scored.begin(); it != mid; ++it) {
    out.push_back(it->second);
  }
}

void FlatCentroids::Serialize(IndexOutput& out) const {
  out.WriteByte(static_cast<byte_type>(CentroidKind::Flat));
  out.WriteByte(static_cast<byte_type>(_metric));
  out.WriteU32(_nlist);
  out.WriteU32(_d);
  if (!_centroids.empty()) {
    out.WriteData(reinterpret_cast<const byte_type*>(_centroids.data()),
                  _centroids.size() * sizeof(float));
  }
}

FlatCentroids FlatCentroids::Deserialize(IndexInput& in, uint64_t byte_size) {
  const auto kind = static_cast<CentroidKind>(in.ReadByte());
  SDB_ENSURE(kind == CentroidKind::Flat, sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
             "idx: unknown centroid structure kind ",
             static_cast<uint32_t>(kind));

  FlatCentroids out;
  out._metric = static_cast<VectorMetric>(in.ReadByte());
  out._nlist = static_cast<uint32_t>(in.ReadI32());
  out._d = static_cast<uint32_t>(in.ReadI32());

  const uint64_t count = static_cast<uint64_t>(out._nlist) * out._d;
  SDB_ENSURE(kHeaderSize + count * sizeof(float) == byte_size,
             sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
             "idx: flat centroid byte size mismatch (header+nlist*d*4=",
             kHeaderSize + count * sizeof(float), ", expected ", byte_size, ")");

  out._centroids.resize(count);
  if (count != 0) {
    in.ReadData(reinterpret_cast<byte_type*>(out._centroids.data()),
                count * sizeof(float));
  }
  return out;
}

}  // namespace irs
