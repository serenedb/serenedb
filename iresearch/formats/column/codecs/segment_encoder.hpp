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

class SegmentEncoder {
 public:
  SegmentEncoder(const ColCodecParams& params, const duckdb::LogicalType& type,
                 StringTuning& tuning, duckdb::BufferManager& buffers)
    : _params{params}, _type{type}, _tuning{tuning}, _buffers{buffers} {}

  Segment* Cut(const Config& config, const StringAccumulator& acc,
               uint64_t begin);

  void SealChunks(const Config& config, std::span<const WriteChunk> chunks,
                  SegmentSink sink);

  Segment* Encode(const Config& config, const StringAccumulator& acc,
                  uint64_t begin, uint64_t end);

  uint64_t Price(const Config& config, const StringAccumulator& acc,
                 uint64_t begin, uint64_t end);

  void Retrain() noexcept;

  void Release(Segment* seg) { _free.push_back(seg); }

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

  const Profile& Measure(const StringAccumulator& acc, Shape shape,
                         uint64_t begin, uint64_t end);

  Segment* Acquire();

  const ColCodecParams& _params;
  const duckdb::LogicalType& _type;
  StringTuning& _tuning;
  duckdb::BufferManager& _buffers;
  DedupScratch _dedup;
  TrainedDictionaries _dictionaries;
  States _states;
  std::vector<std::unique_ptr<Segment>> _pool;
  std::vector<Segment*> _free;
  Profile _profile;
  std::vector<uint32_t> _profile_codes;
  uint64_t _profile_begin = 0;
  uint64_t _profile_end = 0;
  bool _measured = false;
};

}  // namespace irs::codecs::text
