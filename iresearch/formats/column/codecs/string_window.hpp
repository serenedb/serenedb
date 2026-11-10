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
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/blob_frames.hpp"
#include "iresearch/formats/column/codecs/blob_pricer.tpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/segment_rows.hpp"
#include "iresearch/formats/column/codecs/string_compressor.hpp"
#include "iresearch/formats/column/codecs/string_seal.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"

namespace irs::codecs::text {

class StringWindow {
 public:
  StringWindow(const StringAccumulator& acc, StringCompressor& compressor,
               Segment& first, const NumericTuning& codes)
    : _acc{acc},
      _compressor{compressor},
      _codes{codes},
      _first{first},
      _begin{first.begin},
      _end{first.begin + first.rows},
      _encoded{&first} {}

  StringWindow(const StringWindow&) = delete;
  StringWindow& operator=(const StringWindow&) = delete;

  uint64_t Analyze(const Config& config);

  bool Exact(const Config& config) const noexcept {
    return static_cast<bool>(Encoded(config));
  }

  uint64_t Entries();

  Segment& TrialCompress(const Config& config);

  std::span<Segment* const> Encodings() const noexcept { return _encoded; }

  PooledSegment Take(const Segment& seg);

 private:
  struct Spans {
    FrameShape frame;
    FrameSpans spans;
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

  struct Pricers {
    std::optional<BlobPricer<Lz4>> lz4;
    std::optional<BlobPricer<Lz4Hc>> lz4_hc;
  };

  Segment* Encoded(const Config& config) const noexcept;

  uint64_t RawBound(Shape shape) const noexcept;

  Measured& Rows(Shape shape);

  std::string_view Bytes(Measured& rows);

  uint64_t CodesSize(Measured& rows);

  const FrameSpans& Split(Measured& rows, FrameShape frame);

  const StringAccumulator& _acc;
  StringCompressor& _compressor;
  const NumericTuning& _codes;
  const Segment& _first;
  uint64_t _begin;
  uint64_t _end;
  std::vector<Segment*> _encoded;
  std::vector<PooledSegment> _owned;
  DedupScratch _dedup;
  Measured _measured[2];
  Pricers _pricers;
};

}  // namespace irs::codecs::text
