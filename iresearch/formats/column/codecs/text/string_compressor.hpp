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
#include <span>
#include <tuple>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/bytes/zxc.hpp"
#include "iresearch/formats/column/codecs/text/blob.hpp"
#include "iresearch/formats/column/codecs/text/fsst.hpp"
#include "iresearch/formats/column/codecs/text/fsst_plus.hpp"
#include "iresearch/formats/column/codecs/text/onpair.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_accumulator.hpp"

namespace irs::codecs::text {

struct SegmentRecycler {
  std::vector<Segment*>* free = nullptr;

  void operator()(Segment* seg) const noexcept { free->push_back(seg); }
};

using PooledSegment = std::unique_ptr<Segment, SegmentRecycler>;

class StringCompressor {
 public:
  StringCompressor(const ColCodecParams& params,
                   const duckdb::LogicalType& type, EstimatorState& estimators,
                   duckdb::BufferManager& buffers)
    : _params{params},
      _type{type},
      _estimators{estimators},
      _buffers{buffers} {}

  PooledSegment CompressSegment(const Config& config,
                                const StringAccumulator& acc, uint64_t begin);

  PooledSegment TrialCompress(const Config& config,
                              const StringAccumulator& acc, uint64_t begin,
                              uint64_t end);

  void Compress(const Config& config, const StringAccumulator& acc,
                uint64_t begin, ColumnDataCheckpointData& data);

  void CompressChunks(const Config& config, ColumnDataCheckpointData& data);

  DedupScratch& Dedup() noexcept { return _dedup; }

 private:
  template<typename State>
  State& StateOf() {
    auto& slot = std::get<std::optional<State>>(_states);
    if (!slot) {
      if constexpr (Sorted(State::kCodec)) {
        slot.emplace(_type, _dedup, _estimators, _params.segment_target);
      } else {
        slot.emplace(_type, _dedup, _estimators);
      }
    }
    return *slot;
  }

  template<typename Visit>
  void WithState(const Config& config, Visit&& visit);

  PooledSegment Acquire();

  using States =
    std::tuple<std::optional<blob::CompressionState<Lz4>>,
               std::optional<blob::CompressionState<Lz4Hc>>,
               std::optional<blob::CompressionState<Zstd>>,
               std::optional<blob::CompressionState<Zxc>>,
               std::optional<fsst::CompressionState<fsst::Fsst8>>,
               std::optional<fsst_plus::CompressionState>,
               std::optional<onpair::CompressionState>,
               std::optional<fsst::CompressionState<fsst::Fsst12>>>;

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
