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
#include <memory>
#include <span>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_compressor.hpp"
#include "iresearch/formats/column/codecs/string_seal.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/formats/column/codecs/string_window.hpp"

namespace irs::codecs::text {

struct CompressionMethod {
  std::span<const uint8_t> levels;
  bool local = false;
  uint8_t max_trial_level = 0;
};

struct AnalyzeInput {
  StringWindow& window;
  Shape shape;
  const CompressionMethod& method;
  bool tune = false;
  TunedParameters& tuned;
};

struct Analysis {
  Config config;
  uint64_t score = 0;
};

Analysis AnalyzeLz4(const AnalyzeInput& input);
Analysis AnalyzeFsst(const AnalyzeInput& input);

struct StringCompressionFunction {
  StringCompressionFunction(const StringCompressionFunction&) = delete;
  StringCompressionFunction& operator=(const StringCompressionFunction&) =
    delete;

  virtual PooledSegment CompressSegment(StringCompressor& compressor,
                                        const Config& config,
                                        const StringAccumulator& acc,
                                        uint64_t begin) const = 0;
  virtual void Compress(StringCompressor& compressor, const Config& config,
                        const StringAccumulator& acc, uint64_t begin,
                        SegmentSink sink) const = 0;
  virtual PooledSegment TrialCompress(StringCompressor& compressor,
                                      const Config& config,
                                      const StringAccumulator& acc,
                                      uint64_t begin, uint64_t end) const = 0;
  virtual void CompressChunks(StringCompressor& compressor,
                              const Config& config,
                              std::span<const WriteChunk> chunks,
                              SegmentSink sink) const = 0;

 protected:
  constexpr StringCompressionFunction() = default;
  ~StringCompressionFunction() = default;
};

const StringCompressionFunction& GetStringCompressionFunction(
  const Config& config);

const StringCompressionFunction& Lz4CompressionFunction();
const StringCompressionFunction& Lz4HcCompressionFunction();
const StringCompressionFunction& ZstdCompressionFunction();
const StringCompressionFunction& ZxcCompressionFunction();
const StringCompressionFunction& FsstCompressionFunction();
const StringCompressionFunction& FsstPlusCompressionFunction();
const StringCompressionFunction& OnPairCompressionFunction();
const StringCompressionFunction& Fsst12CompressionFunction();

}  // namespace irs::codecs::text
