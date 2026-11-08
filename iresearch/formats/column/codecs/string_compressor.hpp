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
#include <tuple>
#include <vector>

#include "iresearch/formats/column/codecs/blob_compression_state.tpp"
#include "iresearch/formats/column/codecs/fsst12_compression_state.hpp"
#include "iresearch/formats/column/codecs/fsst_compression_state.hpp"
#include "iresearch/formats/column/codecs/fsst_plus_compression_state.hpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/onpair_compression_state.hpp"
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

class StringCompressor {
 public:
  StringCompressor(const ColCodecParams& params,
                   const duckdb::LogicalType& type, EstimatorState& estimators,
                   duckdb::BufferManager& buffers)
    : _params{params},
      _type{type},
      _estimators{estimators},
      _buffers{buffers} {}

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

  PooledSegment Acquire();

  uint32_t Target() const noexcept { return _params.segment_target; }

 private:
  using States = std::tuple<std::optional<BlobCompressionState<Lz4>>,
                            std::optional<BlobCompressionState<Lz4Hc>>,
                            std::optional<BlobCompressionState<Zstd>>,
                            std::optional<BlobCompressionState<Zxc>>,
                            std::optional<FsstCompressionState>,
                            std::optional<FsstPlusCompressionState>,
                            std::optional<OnPairCompressionState>,
                            std::optional<Fsst12CompressionState>>;

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
