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

#include "iresearch/formats/column/codecs/fsst_chain_encoder.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <duckdb/common/bitpacking.hpp>

#include "iresearch/formats/column/codecs/string_layout.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kFsstFirstRatio = 0.5;
constexpr double kMispredicted = 2.0;

void Record(RatioHistory& hist, uint64_t raw, uint64_t comp,
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

}  // namespace

uint64_t FsstChainEncoder::Estimate(const SegmentBuilder& rows) const noexcept {
  const auto entry_count = rows.Entries().size() + rows.FirstEntry();
  return Packed(entry_count, rows.MaxLen()) +
         (rows.Raw() / kFsstFrameRawBytes + 1) * kFrameMetaSize +
         static_cast<uint64_t>(
           static_cast<double>(rows.Raw()) *
           Ratio(_history[static_cast<size_t>(_shape)], kFsstFirstRatio));
}

EncodedFrames FsstChainEncoder::Finish(const SegmentBuilder& rows,
                                       std::span<uint32_t> lengths) {
  const auto entries = rows.Entries();
  Encode(entries, rows.FirstEntry());
  absl::c_copy(_entry_lengths, lengths.begin());
  _lcp_stream.resize(duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(
                       entries.size() + rows.FirstEntry()),
                     0);
  const auto symtab = _codec.SymbolTable();
  Record(_history[static_cast<size_t>(_shape)], rows.Raw(),
         _data.size() + symtab.size(), _target);
  return {
    .codec = kCodec,
    .frames = _frames,
    .max_length = _max_enc,
    .lcps = _lcp_stream,
    .max_lcp = _max_lcp,
    .symtab = symtab,
  };
}

void FsstChainEncoder::Encode(std::span<const std::string_view> entries,
                              uint32_t first_entry) {
  _suffixes.clear();
  _lcp_stream.assign(first_entry, 0);
  _max_lcp = 0;
  _frames.clear();
  std::string_view prev;
  uint64_t frame_raw = 0;
  uint64_t frame_first = 0;
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto sv = entries[i];
    auto lcp =
      static_cast<uint32_t>(absl::FindLongestCommonPrefix(prev, sv).size());
    if (frame_raw != 0 && frame_raw + sv.size() > kFsstFrameRawBytes) {
      _frames.push_back(FrameMeta{static_cast<uint32_t>(frame_first),
                                  static_cast<uint32_t>(frame_raw), 0, 0});
      frame_raw = 0;
      frame_first = i;
    }
    if ((i - frame_first) % kChainRestart == 0) {
      lcp = 0;
    }
    _lcp_stream.push_back(lcp);
    _max_lcp = std::max(_max_lcp, lcp);
    _suffixes.push_back(sv.substr(lcp));
    frame_raw += sv.size();
    prev = sv;
  }
  if (!entries.empty()) {
    _frames.push_back(FrameMeta{static_cast<uint32_t>(frame_first),
                                static_cast<uint32_t>(frame_raw), 0, 0});
  }
  _codec.Encode(_suffixes, _data, _entry_lengths);
  size_t off = 0;
  size_t next = 0;
  _max_enc = 0;
  for (size_t f = 0; f < _frames.size(); ++f) {
    const auto end =
      f + 1 < _frames.size() ? _frames[f + 1].first_entry : entries.size();
    _frames[f].comp_off = static_cast<uint32_t>(off);
    for (; next < end; ++next) {
      off += _entry_lengths[next];
      _max_enc = std::max(_max_enc, _entry_lengths[next]);
    }
    _frames[f].comp_len = static_cast<uint32_t>(off - _frames[f].comp_off);
    _frames[f].first_entry += first_entry;
  }
}

}  // namespace irs::codecs::text
