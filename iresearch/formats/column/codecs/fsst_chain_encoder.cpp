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
#include <duckdb/common/types.hpp>
#include <limits>

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

uint64_t EstimateFsst(const SegmentBuilder& rows,
                      const RatioHistory& hist) noexcept {
  return Packed(rows.EntryCount(), rows.MaxLen()) +
         (rows.Raw() / kFsstFrameRawBytes + 1) * kFrameMetaSize +
         static_cast<uint64_t>(static_cast<double>(rows.Raw()) *
                               Ratio(hist, kFsstFirstRatio));
}

constexpr uint32_t kNoNode = std::numeric_limits<uint32_t>::max();
constexpr uint64_t kGroupCost = sizeof(uint32_t);

size_t SharedCodes(std::string_view a, std::string_view b) noexcept {
  auto n = absl::FindLongestCommonPrefix(a, b).size();
  while (n != 0 && static_cast<uint8_t>(a[n - 1]) == FSST_ESC) {
    --n;
  }
  return n;
}

void AppendU32(std::string& out, uint32_t value) {
  char bytes[sizeof(uint32_t)];
  duckdb::Store<uint32_t>(value, duckdb::data_ptr_cast(bytes));
  out.append(bytes, sizeof(bytes));
}

}  // namespace

uint64_t FsstChainEncoder::Estimate(const SegmentBuilder& rows) const noexcept {
  return EstimateFsst(rows, _history[static_cast<size_t>(_shape)]);
}

EncodedFrames FsstChainEncoder::Finish(const SegmentBuilder& rows,
                                       std::span<uint32_t> lengths) {
  const auto entries = rows.Entries();
  const auto first_entry = rows.FirstEntry();
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
  _lcp_stream.resize(duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(
                       rows.EntryCount()),
                     0);
  const auto symtab = _codec.SymbolTable();
  Record(_history[static_cast<size_t>(_shape)], rows.Raw(),
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

uint64_t FsstPlusEncoder::Estimate(const SegmentBuilder& rows) const noexcept {
  return EstimateFsst(rows, _history[static_cast<size_t>(_shape)]);
}

EncodedFrames FsstPlusEncoder::Finish(const SegmentBuilder& rows,
                                      std::span<uint32_t> lengths) {
  const auto entries = rows.Entries();
  const auto first_entry = rows.FirstEntry();
  const auto n = entries.size();
  _codec.Encode(entries, _encoded, _encoded_lengths);
  _codes.resize(n);
  uint64_t at = 0;
  for (size_t i = 0; i < n; ++i) {
    _codes[i] = std::string_view{_encoded}.substr(at, _encoded_lengths[i]);
    at += _encoded_lengths[i];
  }
  _shared.assign(n, 0);
  _ids.assign(first_entry + n, 0);
  _ends.clear();
  _prefixes.clear();
  Cleave(first_entry);
  _data.clear();
  _frames.clear();
  uint32_t max_length = 0;
  uint64_t frame_raw = 0;
  uint64_t frame_first = 0;
  uint64_t frame_off = 0;
  const auto close = [&](uint64_t end) {
    _frames.emplace_back(static_cast<uint32_t>(frame_first + first_entry),
                         static_cast<uint32_t>(frame_raw),
                         static_cast<uint32_t>(frame_off),
                         static_cast<uint32_t>(end - frame_off));
  };
  for (size_t i = 0; i < n; ++i) {
    if (frame_raw != 0 && frame_raw + entries[i].size() > kFsstFrameRawBytes) {
      close(_data.size());
      frame_raw = 0;
      frame_first = i;
      frame_off = _data.size();
    }
    const auto suffix = _codes[i].substr(_shared[i]);
    _data.append(suffix);
    lengths[i] = static_cast<uint32_t>(suffix.size());
    max_length = std::max(max_length, lengths[i]);
    frame_raw += entries[i].size();
  }
  if (n != 0) {
    close(_data.size());
  }
  const auto table = _codec.SymbolTable();
  _side.assign(table);
  AppendU32(_side, static_cast<uint32_t>(_ends.size()));
  for (const auto end : _ends) {
    AppendU32(_side, end);
  }
  _side.append(_prefixes);
  _ids.resize(duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(
                rows.EntryCount()),
              0);
  Record(_history[static_cast<size_t>(_shape)], rows.Raw(),
         _data.size() + _side.size(), _target);
  return {
    .codec = kCodec,
    .frames = _frames,
    .max_length = max_length,
    .lcps = _ids,
    .max_lcp = static_cast<uint32_t>(_ends.size()),
    .symtab = _side,
    .dictionary_size = static_cast<uint32_t>(table.size()),
  };
}

void FsstPlusEncoder::Cleave(uint32_t first_entry) {
  const auto n = static_cast<uint32_t>(_codes.size());
  if (n < 2) {
    return;
  }
  const uint32_t m = n - 1;
  _lcp.resize(m);
  for (uint32_t i = 0; i < m; ++i) {
    _lcp[i] = static_cast<uint32_t>(SharedCodes(_codes[i], _codes[i + 1]));
  }
  _tree.assign(m, {kNoNode, kNoNode});
  _order.clear();
  uint32_t root = 0;
  for (uint32_t i = 0; i < m; ++i) {
    uint32_t last = kNoNode;
    while (!_order.empty() && _lcp[_order.back()] > _lcp[i]) {
      last = _order.back();
      _order.pop_back();
    }
    _tree[i].left = last;
    if (_order.empty()) {
      root = i;
    } else {
      _tree[_order.back()].right = i;
    }
    _order.push_back(i);
  }
  _order.clear();
  _ranges.clear();
  _ranges.push_back({0, 0, root});
  while (!_ranges.empty()) {
    const auto i = _ranges.back().node;
    _ranges.pop_back();
    _order.push_back(i);
    for (const auto child : {_tree[i].left, _tree[i].right}) {
      if (child != kNoNode) {
        _ranges.push_back({0, 0, child});
      }
    }
  }
  _size.resize(m);
  _saved.resize(m);
  _whole.assign(m, 0);
  for (auto it = _order.rbegin(); it != _order.rend(); ++it) {
    const auto i = *it;
    const auto [left, right] = _tree[i];
    const uint64_t below = (left == kNoNode ? 0 : _saved[left]) +
                           (right == kNoNode ? 0 : _saved[right]);
    _size[i] = 1 + (left == kNoNode ? 0 : _size[left]) +
               (right == kNoNode ? 0 : _size[right]);
    const uint64_t whole = uint64_t{_size[i]} * _lcp[i];
    if (whole >= below + kGroupCost) {
      _saved[i] = whole - kGroupCost;
      _whole[i] = 1;
    } else {
      _saved[i] = below;
    }
  }
  _ranges.clear();
  _ranges.push_back({0, m, root});
  while (!_ranges.empty()) {
    const auto [first, last, mid] = _ranges.back();
    _ranges.pop_back();
    if (first == last) {
      continue;
    }
    if (!_whole[mid]) {
      _ranges.push_back({mid + 1, last, _tree[mid].right});
      _ranges.push_back({first, mid, _tree[mid].left});
      continue;
    }
    const auto id = static_cast<uint32_t>(_ends.size()) + 1;
    _prefixes.append(_codes[first].substr(0, _lcp[mid]));
    _ends.push_back(static_cast<uint32_t>(_prefixes.size()));
    for (auto i = first; i <= last; ++i) {
      _shared[i] = _lcp[mid];
      _ids[first_entry + i] = id;
    }
  }
}

}  // namespace irs::codecs::text
