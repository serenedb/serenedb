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

#include "iresearch/formats/column/codecs/leaf_encoder.hpp"

#include "iresearch/formats/column/codecs/numeric.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kEntriesFirstRatio = 0.5;
constexpr double kMispredicted = 2.0;

}  // namespace

uint64_t CodesBytes(Shape shape, uint64_t rows, uint64_t entries, uint64_t runs,
                    const NumericTuning& tuning) noexcept {
  return shape == Shape::Dedup ? EstimateCodes(tuning, rows, entries, runs) : 0;
}

uint64_t EstimateEntries(const SegmentEntries& rows,
                         const RatioHistory& hist) noexcept {
  return Packed(EntryCount(rows), rows.max_len) +
         (rows.raw / kFsstFrameRawBytes + 1) * kFrameMetaSize +
         static_cast<uint64_t>(static_cast<double>(rows.raw) *
                               Ratio(hist, kEntriesFirstRatio));
}

void RecordRatio(RatioHistory& hist, uint64_t raw, uint64_t comp,
                 uint32_t target) noexcept {
  if (hist.raw != 0 && raw != 0 && comp * kDriftMinFraction >= target) {
    const auto predicted = static_cast<double>(raw) *
                           static_cast<double>(hist.comp) /
                           static_cast<double>(hist.raw);
    const auto actual = static_cast<double>(comp);
    if (actual > predicted * kMispredicted ||
        actual * kMispredicted < predicted) {
      hist = {};
    }
  }
  hist.raw += raw;
  hist.comp += comp;
}

}  // namespace irs::codecs::text
