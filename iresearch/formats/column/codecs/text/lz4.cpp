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

using PlainLz4 = text::blob::Lz4CompressionState<text::PlainRows>;
using DictLz4 = text::blob::Lz4CompressionState<text::DictionaryRows>;

template<typename Compression, FrameDictionary kFrames>
double AnalyzeFrames(ColumnDataCheckpointData& data, double bound) {
  auto& encoder = data.Get<text::TextEncoder<Compression>>();
  auto& lz4 = encoder.Codec();
  const auto score = lz4.AnalyzedOnce([&] {
    auto& values = data.Get<text::Values>();
    if (!values.Repeats() || !encoder.Contests(data)) {
      return kNoScore;
    }
    const auto price =
      data.preferences->text_lz4_price[std::to_underlying(Compression::kShape)];
    const auto priced = [&](uint64_t bytes) {
      return text::Score(values, Compression::kShape, price, bytes);
    };
    const auto measured =
      lz4.Measure(encoder, values, data.params.tier, bound, priced);
    encoder.Contest(measured.config);
    return priced(measured.bytes);
  });
  return encoder.Contested().layout == kFrames ? score : kNoScore;
}

template<typename Compression, FrameDictionary kFrames>
bool CompressFrames(ColumnDataCheckpointData& data) {
  auto& encoder = data.Get<text::TextEncoder<Compression>>();
  auto& values = data.Get<text::Values>();
  const auto* config = encoder.Chosen(values);
  return values.Repeats() && config && config->layout == kFrames &&
         encoder.Compress(data);
}

template<FrameDictionary kFrames>
double AnalyzeLz4(ColumnDataCheckpointData& data, double bound) {
  return AnalyzeFrames<PlainLz4, kFrames>(data, bound);
}

template<FrameDictionary kFrames>
bool CompressLz4(ColumnDataCheckpointData& data) {
  return CompressFrames<PlainLz4, kFrames>(data);
}

template<FrameDictionary kFrames>
double AnalyzeDictLz4(ColumnDataCheckpointData& data, double bound) {
  return AnalyzeFrames<DictLz4, kFrames>(data, bound);
}

template<FrameDictionary kFrames>
bool CompressDictLz4(ColumnDataCheckpointData& data) {
  return CompressFrames<DictLz4, kFrames>(data);
}

template<FrameDictionary kFrames>
constexpr CompressionFunction kLz4{
  .analyze = AnalyzeLz4<kFrames>,
  .compress = CompressLz4<kFrames>,
  .codec = CodecType::Lz4,
};

template<FrameDictionary kFrames>
constexpr CompressionFunction kDictLz4{
  .analyze = AnalyzeDictLz4<kFrames>,
  .compress = CompressDictLz4<kFrames>,
  .codec = CodecType::DictLz4,
};

constexpr const CompressionFunction* kLz4Contenders[] = {
  &kLz4<FrameDictionary::FirstFrame>,
  &kLz4<FrameDictionary::Local>,
  &kLz4<FrameDictionary::None>,
};

constexpr const CompressionFunction* kDictLz4Contenders[] = {
  &kDictLz4<FrameDictionary::FirstFrame>,
  &kDictLz4<FrameDictionary::Local>,
  &kDictLz4<FrameDictionary::None>,
};

}  // namespace

CompressionFunctions Lz4TextContenders(Shape shape) {
  if (shape == Shape::Plain) {
    return kLz4Contenders;
  }
  return kDictLz4Contenders;
}

constinit const Codec kLz4Codec =
  text::StringCodec<text::PlainScan<text::blob::Lz4Scan>, PlainLz4>();

constinit const Codec kDictLz4Codec =
  text::StringCodec<text::DictionaryScan<text::blob::Lz4Scan>, DictLz4>();

}  // namespace irs::codecs
