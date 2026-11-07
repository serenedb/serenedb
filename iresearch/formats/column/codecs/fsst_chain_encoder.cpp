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

uint64_t FsstChainEncoder::Estimate(const SegmentEntries& rows) const noexcept {
  return EstimateEntries(rows, _history[static_cast<size_t>(_shape)]);
}

EncodedFrames FsstChainEncoder::Finish(const SegmentEntries& rows,
                                       std::span<uint32_t> lengths) {
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  _suffixes.clear();
  _lcp_stream.assign(first_entry, 0);
  _frames.clear();
  uint32_t max_lcp = 0;
  std::string_view prev;
  uint64_t frame_raw = 0;
  uint64_t frame_first = 0;
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto sv = entries[i];
    if (frame_raw != 0 && frame_raw + sv.size() > kFsstFrameRawBytes) {
      _frames.emplace_back(static_cast<uint32_t>(frame_first),
                           static_cast<uint32_t>(frame_raw), 0, 0);
      frame_raw = 0;
      frame_first = i;
    }
    const uint32_t lcp =
      (i - frame_first) % kChainRestart == 0
        ? 0
        : static_cast<uint32_t>(absl::FindLongestCommonPrefix(prev, sv).size());
    _lcp_stream.emplace_back(lcp);
    max_lcp = std::max(max_lcp, lcp);
    _suffixes.emplace_back(sv.substr(lcp));
    frame_raw += sv.size();
    prev = sv;
  }
  if (!entries.empty()) {
    _frames.emplace_back(static_cast<uint32_t>(frame_first),
                         static_cast<uint32_t>(frame_raw), 0, 0);
  }
  _codec.Encode(_suffixes, _data, _entry_lengths);
  size_t off = 0;
  uint32_t max_enc = 0;
  for (size_t f = 0; f < _frames.size(); ++f) {
    auto& frame = _frames[f];
    const size_t end =
      f + 1 < _frames.size() ? _frames[f + 1].first_entry : entries.size();
    frame.comp_off = static_cast<uint32_t>(off);
    for (size_t e = frame.first_entry; e < end; ++e) {
      off += _entry_lengths[e];
      max_enc = std::max(max_enc, _entry_lengths[e]);
    }
    frame.comp_len = static_cast<uint32_t>(off - frame.comp_off);
    frame.first_entry += first_entry;
  }
  absl::c_copy(_entry_lengths, lengths.begin());
  _lcp_stream.resize(
    duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount(rows)),
    0);
  const auto symtab = _codec.SymbolTable();
  RecordRatio(_history[static_cast<size_t>(_shape)], rows.raw,
              _data.size() + symtab.size(), _target);
  return {
    .codec = kCodec,
    .frames = _frames,
    .max_length = max_enc,
    .lcps = _lcp_stream,
    .max_lcp = max_lcp,
    .symtab = symtab,
  };
}

}  // namespace irs::codecs::text
