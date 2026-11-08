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

struct StringAnalyzeState {
  virtual ~StringAnalyzeState() = default;

  template<typename T>
  T& Cast() noexcept {
    return static_cast<T&>(*this);
  }

  Config config;
};

struct StringCompressionFunction {
  ByteCodec leaf = ByteCodec::Lz4;
  std::unique_ptr<StringAnalyzeState> (*init_analyze)(
    const AnalyzeInput& input) = nullptr;
  bool (*analyze)(StringAnalyzeState& state,
                  const AnalyzeInput& input) = nullptr;
  uint64_t (*final_analyze)(StringAnalyzeState& state) = nullptr;
  PooledSegment (*compress_segment)(StringCompressor& compressor,
                                    const Config& config,
                                    const StringAccumulator& acc,
                                    uint64_t begin) = nullptr;
  void (*compress)(StringCompressor& compressor, const Config& config,
                   const StringAccumulator& acc, uint64_t begin,
                   SegmentSink sink) = nullptr;
  PooledSegment (*trial_compress)(StringCompressor& compressor,
                                  const Config& config,
                                  const StringAccumulator& acc, uint64_t begin,
                                  uint64_t end) = nullptr;
  void (*compress_chunks)(StringCompressor& compressor, const Config& config,
                          std::span<const WriteChunk> chunks,
                          SegmentSink sink) = nullptr;
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
