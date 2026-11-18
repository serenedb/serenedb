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

#include <span>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_accumulator.hpp"
#include "iresearch/formats/column/codecs/text/string_compressor.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"

namespace irs::codecs::text {

class Strings final : public State {
 public:
  static constexpr StateId kId = StateId::Strings;

  explicit Strings(ColumnDataCheckpointData& data);

  Estimate Analyze(ColumnDataCheckpointData& data);
  bool Compress(ColumnDataCheckpointData& data);

 private:
  const StringAccumulator& Accumulated(ColumnDataCheckpointData& data);
  PooledSegment DetectBestCompressionMethod(ColumnDataCheckpointData& data);

  StringTuning& _tuning;
  StringCompressor _compressor;
  StringAccumulator _acc;
  PooledSegment _first;
  bool _accumulated = false;
};

extern const CompressionFunction kStringsFunction;

}  // namespace irs::codecs::text
