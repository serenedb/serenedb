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

#include "iresearch/formats/column/codecs/segment_encoder.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_seal.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"

namespace irs::codecs::text {

struct Estimate {
  Config config;
  uint64_t bytes = 0;
};

class StringWindow {
 public:
  StringWindow(const StringAccumulator& acc, SegmentEncoder& encoder,
               Segment& first)
    : _acc{acc},
      _encoder{encoder},
      _begin{first.begin},
      _end{first.begin + first.rows},
      _encoded{&first} {}

  StringWindow(const StringWindow&) = delete;
  StringWindow& operator=(const StringWindow&) = delete;

  uint64_t Price(const Config& config);

  Segment& Encode(const Config& config);

  Segment* Encoded(const Config& config) const noexcept;

  std::span<Segment* const> Encodings() const noexcept { return _encoded; }

  void Keep(const Segment& kept);

 private:
  const StringAccumulator& _acc;
  SegmentEncoder& _encoder;
  uint64_t _begin;
  uint64_t _end;
  std::vector<Segment*> _encoded;
};

}  // namespace irs::codecs::text
