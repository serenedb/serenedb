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

#include "iresearch/formats/column/codecs/fsst_plus_encoder.hpp"

#include <absl/strings/match.h>

#include <algorithm>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types.hpp>
#include <limits>

#include "iresearch/formats/column/codecs/string_layout.hpp"

namespace irs::codecs::text {
namespace {

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
  RecordRatio(_history[static_cast<size_t>(_shape)], rows.Raw(),
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
