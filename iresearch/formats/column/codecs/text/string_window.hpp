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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/text/blob.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_accumulator.hpp"
#include "iresearch/formats/column/codecs/text/string_compressor.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"

namespace irs::codecs::text {

class StringWindow {
 public:
  StringWindow(const StringAccumulator& acc, StringCompressor& compressor,
               PooledSegment first, const NumericTuning& codes)
    : _acc{acc},
      _compressor{compressor},
      _codes{codes},
      _first{*first},
      _begin{first->begin},
      _end{first->begin + first->rows} {
    _encoded.emplace_back(std::move(first));
  }

  StringWindow(const StringWindow&) = delete;
  StringWindow& operator=(const StringWindow&) = delete;

  uint64_t Analyze(const Config& config);

  bool Exact(const Config& config) const noexcept {
    return Find(config) != _encoded.size();
  }

  uint64_t Entries();

  size_t TrialCompress(const Config& config);

  size_t Encodings() const noexcept { return _encoded.size(); }

  const Segment& At(size_t index) const noexcept { return *_encoded[index]; }

  PooledSegment Take(size_t index) && noexcept {
    return std::move(_encoded[index]);
  }

 private:
  struct Spans {
    blob::FrameShape frame;
    blob::FrameSpans spans;
  };

  struct Measured {
    bool done = false;
    bool assembled = false;
    bool coded = false;
    Profile profile;
    std::vector<uint32_t> codes;
    ScratchVector<uint32_t> locals;
    uint64_t codes_bytes = 0;
    std::string bytes;
    std::vector<Spans> spans;
  };

  struct SizeEstimators {
    std::optional<blob::SizeEstimator<Lz4>> lz4;
    std::optional<blob::SizeEstimator<Lz4Hc>> lz4_hc;
  };

  size_t Find(const Config& config) const noexcept;

  uint64_t RawBound(Shape shape) const noexcept;

  Measured& Rows(Shape shape);

  std::string_view Bytes(Measured& rows);

  uint64_t CodesSize(Measured& rows);

  const blob::FrameSpans& Split(Measured& rows, blob::FrameShape frame);

  const StringAccumulator& _acc;
  StringCompressor& _compressor;
  const NumericTuning& _codes;
  const Segment& _first;
  uint64_t _begin;
  uint64_t _end;
  std::vector<PooledSegment> _encoded;
  DedupScratch _dedup;
  Measured _measured[kShapeCount];
  SizeEstimators _size_estimators;
};

}  // namespace irs::codecs::text
