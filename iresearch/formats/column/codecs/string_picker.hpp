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

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_compressor.hpp"
#include "iresearch/formats/column/codecs/string_seal.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"

namespace irs::codecs::text {

Config NamedConfig(const StringChoice& named,
                   const ColCodecParams& params) noexcept;

class StringPicker {
 public:
  StringPicker(const StringAccumulator& acc, const ColCodecParams& params,
               StringTuning& tuning, StringCompressor& compressor)
    : _acc{acc}, _params{params}, _tuning{tuning}, _compressor{compressor} {}

  PooledSegment First();

 private:
  Config Current() const noexcept;

  bool Drifted(const Segment& seg) const noexcept;

  PooledSegment DetectBestCompressionMethod(PooledSegment first, bool drift);

  const StringAccumulator& _acc;
  const ColCodecParams& _params;
  StringTuning& _tuning;
  StringCompressor& _compressor;
};

}  // namespace irs::codecs::text
