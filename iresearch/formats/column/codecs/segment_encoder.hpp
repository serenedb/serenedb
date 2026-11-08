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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/blob_state.tpp"
#include "iresearch/formats/column/codecs/fsst12_state.hpp"
#include "iresearch/formats/column/codecs/fsst_plus_state.hpp"
#include "iresearch/formats/column/codecs/fsst_state.hpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/onpair_state.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_seal.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/codecs/zxc.hpp"

namespace irs::codecs::text {

struct SegmentRecycler {
  std::vector<Segment*>* free = nullptr;

  void operator()(Segment* seg) const noexcept { free->push_back(seg); }
};

using PooledSegment = std::unique_ptr<Segment, SegmentRecycler>;

class SegmentEncoder {
 public:
  SegmentEncoder(const ColCodecParams& params, const duckdb::LogicalType& type,
                 EstimatorState& estimators, duckdb::BufferManager& buffers)
    : _params{params},
      _type{type},
      _estimators{estimators},
      _buffers{buffers} {}

  PooledSegment Cut(const Config& config, const StringAccumulator& acc,
                    uint64_t begin);

  void Compress(const Config& config, const StringAccumulator& acc,
                uint64_t begin, SegmentSink sink);

  void SealChunks(const Config& config, std::span<const WriteChunk> chunks,
                  SegmentSink sink);

  PooledSegment Encode(const Config& config, const StringAccumulator& acc,
                       uint64_t begin, uint64_t end);

 private:
  struct States {
    std::optional<BlobState<Lz4>> lz4;
    std::optional<BlobState<Lz4Hc>> lz4_hc;
    std::optional<BlobState<Zstd>> zstd;
    std::optional<BlobState<Zxc>> zxc;
    std::optional<FsstState> fsst;
    std::optional<FsstPlusState> fsst_plus;
    std::optional<OnPairState> onpair;
    std::optional<Fsst12State> fsst12;
  };

  template<typename F>
  auto With(const Config& config, F&& f);

  template<typename State>
  State& Get(std::optional<State>& slot);

  PooledSegment Acquire();

  const ColCodecParams& _params;
  const duckdb::LogicalType& _type;
  EstimatorState& _estimators;
  duckdb::BufferManager& _buffers;
  DedupScratch _dedup;
  States _states;
  std::vector<std::unique_ptr<Segment>> _pool;
  std::vector<Segment*> _free;
};

}  // namespace irs::codecs::text
