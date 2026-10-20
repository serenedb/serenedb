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

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct HnswByteBasis {
  std::vector<int8_t> codes;
  std::vector<float> scales;
};

class HnswBases {
 public:
  using Basis = std::shared_ptr<const std::vector<float>>;
  using ByteBasis = std::shared_ptr<const HnswByteBasis>;

  Basis Find(field_id field) const;

  Basis Publish(field_id field, Basis basis);

  Basis Share(field_id field, std::vector<float> basis);

  ByteBasis Bytes(const Basis& basis);

 private:
  struct Narrowed {
    std::weak_ptr<const std::vector<float>> basis;
    std::weak_ptr<const HnswByteBasis> bytes;
  };

  mutable absl::Mutex _mutex;
  absl::flat_hash_map<field_id, Basis> _published;
  std::vector<std::weak_ptr<const std::vector<float>>> _loaded;
  std::vector<Narrowed> _narrowed;
};

HnswBases::ByteBasis MakeByteBasis(std::span<const float> basis);

struct PanoramaQuery {
  std::vector<float> rotated;
  std::vector<float> tails;
};

class AnnQueryCache {
 public:
  using Basis = HnswBases::Basis;
  using ByteBasis = HnswBases::ByteBasis;

  AnnQueryCache() = default;
  AnnQueryCache(const AnnQueryCache&) noexcept {}
  AnnQueryCache& operator=(const AnnQueryCache&) noexcept { return *this; }

  std::shared_ptr<const PanoramaQuery> Get(const Basis& basis,
                                           const ByteBasis& bytes,
                                           std::span<const float> query);

  std::shared_ptr<const QuantizerCodebook> Codebook(
    const QuantizerStats& stats, std::span<const float> query);

 private:
  struct Entry {
    Basis basis;
    std::shared_ptr<PanoramaQuery> query;
    std::vector<int16_t> narrow;
    float unit = 0.f;
    std::atomic_uint32_t next{0};
    std::atomic_uint32_t done{0};
    std::atomic_bool ready{false};
  };

  struct SharedCodebook {
    std::vector<byte_type> key;
    std::shared_ptr<const QuantizerCodebook> codebook;
  };

  absl::Mutex _mutex;
  std::vector<std::unique_ptr<Entry>> _entries;
  std::vector<SharedCodebook> _codebooks;
};

std::shared_ptr<const PanoramaQuery> RotateQuery(
  AnnQueryCache* cache, const HnswBases::Basis& basis,
  const HnswBases::ByteBasis& bytes, std::span<const float> query);

std::shared_ptr<const QuantizerCodebook> QueryCodebook(
  AnnQueryCache* cache, const QuantizerStats& stats,
  std::span<const float> query);

}  // namespace irs
