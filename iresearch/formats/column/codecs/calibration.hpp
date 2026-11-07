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
#include <span>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/segment_builder.hpp"
#include "iresearch/formats/column/codecs/segment_encoder.hpp"
#include "iresearch/formats/column/codecs/string_seal.hpp"

namespace irs::codecs::text {

struct LeafPlan {
  ByteCodec leaf;
  std::span<const uint8_t> ladder;
};

Config NamedConfig(const StringChoice& named,
                   const ColCodecParams& params) noexcept;

class Calibration {
 public:
  Calibration(const StringAccumulator& acc, const StringChoice* named,
              const ColCodecParams& params, StringTuning& tuning,
              SegmentEncoder& encoder)
    : _acc{acc},
      _named{named},
      _params{params},
      _tuning{tuning},
      _encoder{encoder} {}

  Config CutConfig() const noexcept;

  bool Drifted(const Segment& seg, bool full) const noexcept;

  Segment* Calibrate(Segment* cut, bool retune, bool drift);

 private:
  struct Candidate {
    Config config;
    uint64_t bytes = 0;
  };

  Candidate Tune(Shape shape, const LeafPlan& plan, bool retune);

  uint64_t PriceOf(Config config);

  Segment* Smallest(Shape shape, Segment* best) const noexcept;

  Segment* Cached(const Config& config) const noexcept;

  Segment* Trial(Config config);

  const StringAccumulator& _acc;
  const StringChoice* _named;
  const ColCodecParams& _params;
  StringTuning& _tuning;
  SegmentEncoder& _encoder;
  std::vector<Segment*> _live;
  uint64_t _begin = 0;
  uint64_t _end = 0;
  Candidate _smallest;
};

}  // namespace irs::codecs::text
