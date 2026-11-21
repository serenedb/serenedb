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

#include "iresearch/formats/column/codecs/text/lz4.hpp"

#include <cstdint>
#include <utility>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/text/blob.hpp"
#include "iresearch/formats/column/codecs/text/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/text/plain_scan.tpp"
#include "iresearch/formats/column/codecs/text/text_encoder.hpp"

/*
lz4 frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 16 KiB; or the
same frames, each compressed with the segment's trained dictionary; or
64 KiB frames without a dictionary. An entry longer than a frame holds is a
frame of its own.
*/

namespace irs::codecs {
namespace {

constexpr double kReadPrice = 1.05;

using PlainLz4 = text::blob::Lz4CompressionState<text::PlainRows>;
using DictLz4 = text::blob::Lz4CompressionState<text::DictionaryRows>;

template<typename Compression>
double AnalyzeLz4(ColumnDataCheckpointData& data, double bound) {
  return text::AnalyzeHinted<Compression>(
    data, bound,
    [&](text::TextEncoder<Compression>& encoder, text::Values& values,
        bool moved) {
      if (!values.Repeats() || !encoder.Contests(data, moved)) {
        return kNoScore;
      }
      const auto price = data.params.tier == WriteTier::Merge &&
                             Compression::kShape == Shape::Dictionary
                           ? 1.0
                           : kReadPrice;
      const auto priced = [&](uint64_t bytes) {
        return text::Score(values, Compression::kShape, price, bytes);
      };
      const auto measured = encoder.Codec().Measure(
        encoder, values, data.params.tier, bound, priced);
      encoder.Contest(measured.config);
      return priced(measured.bytes);
    });
}

template<typename Compression>
constexpr CompressionFunction kFunction{
  .analyze = AnalyzeLz4<Compression>,
  .compress = text::CompressText<Compression>,
};

}  // namespace

constinit const Codec kLz4Codec =
  text::StringCodec<text::PlainScan<text::blob::Lz4Scan>, PlainLz4,
                    kFunction<PlainLz4>>();

constinit const Codec kDictLz4Codec =
  text::StringCodec<text::DictionaryScan<text::blob::Lz4Scan>, DictLz4,
                    kFunction<DictLz4>>();

}  // namespace irs::codecs
