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

#include "iresearch/formats/column/codecs/blob_encoder.tpp"
#include "iresearch/formats/column/codecs/fsst12_chain_encoder.hpp"
#include "iresearch/formats/column/codecs/fsst_chain_encoder.hpp"
#include "iresearch/formats/column/codecs/fsst_plus_encoder.hpp"
#include "iresearch/formats/column/codecs/leaf_encoder.hpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/onpair_encoder.hpp"
#include "iresearch/formats/column/codecs/segment_builder.hpp"
#include "iresearch/formats/column/codecs/string_seal.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/codecs/zxc.hpp"

namespace irs::codecs::text {

class SegmentEncoder {
 public:
  SegmentEncoder(const ColCodecParams& params, const duckdb::LogicalType& type,
                 StringTuning& tuning)
    : _params{params}, _tuning{tuning}, _builder{type, _dedup, tuning.codes} {}

  Segment* Cut(const Config& config, const StringAccumulator& acc,
               uint64_t begin);

  Segment* Cut(const Config& config, ChunkCursor& cursor, uint64_t begin,
               uint64_t end);

  Segment* Encode(const Config& config, const StringAccumulator& acc,
                  uint64_t begin, uint64_t end);

  uint64_t Price(const Config& config, const StringAccumulator& acc,
                 uint64_t begin, uint64_t end);

  void Retrain() noexcept;

  void Release(Segment* seg) { _free.push_back(seg); }

 private:
  struct LeafEncoders {
    std::optional<BlobEncoder<Lz4>> lz4;
    std::optional<BlobEncoder<Lz4Hc>> lz4_hc;
    std::optional<BlobEncoder<Zstd>> zstd;
    std::optional<BlobEncoder<Zxc>> zxc;
    std::optional<FsstChainEncoder> fsst;
    std::optional<FsstPlusEncoder> fsst_plus;
    std::optional<OnPairEncoder> onpair;
    std::optional<Fsst12ChainEncoder> fsst12;
  };

  template<typename F>
  auto With(const Config& config, F&& f);

  template<typename Encoder>
  Encoder& Get(std::optional<Encoder>& slot);

  template<typename Cursor>
  Segment* CutWith(const Config& config, Cursor& cursor, uint64_t begin,
                   uint64_t end, size_t distinct);

  const Profile& Measure(const StringAccumulator& acc, Shape shape,
                         uint64_t begin, uint64_t end);

  void Append(const duckdb::string_t& value);

  Segment* Acquire();

  const ColCodecParams& _params;
  StringTuning& _tuning;
  DedupScratch _dedup;
  SegmentBuilder _builder;
  TrainedDictionaries _dictionaries;
  LeafEncoders _encoders;
  std::vector<std::unique_ptr<Segment>> _pool;
  std::vector<Segment*> _free;
  Profile _profile;
  uint64_t _profile_begin = 0;
  uint64_t _profile_end = 0;
  bool _measured = false;
};

}  // namespace irs::codecs::text
