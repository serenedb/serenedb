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

#include "iresearch/formats/column/codecs/onpair.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <duckdb/common/helper.hpp>
#include <numeric>

#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/onpair_leaf.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs {
namespace {

constexpr double kTrainFraction = 0.15;
constexpr uint64_t kTrainMinBytes = 64 * 1024;
constexpr uint8_t kMaxThreshold = 255;
constexpr uint8_t kMinThreshold = 2;
constexpr uint32_t kSingles = 256;

uint32_t PairKey(uint32_t left, uint32_t right) noexcept {
  return (left << 16) | right;
}

class Controller {
 public:
  Controller(uint32_t capacity, uint64_t budget) noexcept
    : _capacity{capacity},
      _budget{budget},
      _interval{std::max<uint32_t>(capacity / 128, 64)},
      _next{_interval} {}

  uint8_t Threshold() const noexcept { return _threshold; }

  bool Scan(uint64_t bytes) noexcept {
    _scanned += bytes;
    return _scanned > _budget;
  }

  void Created() noexcept {
    if (++_created < _next) {
      return;
    }
    const auto entries = _created - _created_at;
    const auto bytes = _scanned - _scanned_at;
    const double recent =
      bytes > 0 ? static_cast<double>(entries) / static_cast<double>(bytes)
                : 1e9;
    const uint64_t entries_left =
      _capacity > _created ? _capacity - _created : 1;
    const uint64_t bytes_left = _budget > _scanned ? _budget - _scanned : 1;
    const double target =
      static_cast<double>(entries_left) / static_cast<double>(bytes_left);
    const double ratio = target > 0.0 ? recent / target : 1e9;
    if (ratio > 2.0 && _threshold < kMaxThreshold) {
      ++_threshold;
    } else if (ratio < 0.5 && _threshold > kMinThreshold) {
      --_threshold;
    }
    _created_at = _created;
    _scanned_at = _scanned;
    _next = _created + _interval;
  }

 private:
  uint32_t _capacity;
  uint64_t _budget;
  uint32_t _interval;
  uint32_t _next;
  uint32_t _created = 0;
  uint32_t _created_at = 0;
  uint64_t _scanned = 0;
  uint64_t _scanned_at = 0;
  uint8_t _threshold = kMinThreshold;
};

}  // namespace

bool OnPairTable::Open(std::string_view side) {
  if (side.size() < kOnPairSideHeader) {
    return false;
  }
  _bits = static_cast<uint8_t>(side[0]);
  const auto count = duckdb::Load<uint32_t>(
    reinterpret_cast<duckdb::const_data_ptr_t>(side.data() + sizeof(uint32_t)));
  if (_bits == 0 || _bits > 16 || count == 0 ||
      count > (uint64_t{1} << _bits)) {
    return false;
  }
  const auto rest = side.substr(kOnPairSideHeader);
  const auto nibbles = (uint64_t{count} + 1) / 2;
  if (rest.size() < nibbles) {
    return false;
  }
  const auto bytes = rest.substr(nibbles);
  _tokens.resize(count);
  uint32_t at = 0;
  for (uint32_t t = 0; t < count; ++t) {
    const auto packed = static_cast<uint8_t>(rest[t / 2]) >> (4 * (t % 2));
    const uint32_t len = (packed & 0xF) + 1;
    _tokens[t] = at | (len << 24);
    at += len;
  }
  if (at != bytes.size()) {
    return false;
  }
  _bytes.assign(bytes);
  _bytes.append(kOnPairMaxToken, '\0');
  _count = count;
  _mask = static_cast<uint32_t>((uint64_t{1} << _bits) - 1);
  return true;
}

void OnPairTable::Corrupted() {
  SDB_ENSURE(false, "col codec: corrupted entry");
  SDB_UNREACHABLE();
}

OnPairCodec::OnPairCodec()
  : _lengths{std::make_unique<std::array<uint16_t, 65536>>()} {}

void OnPairCodec::Reset() {
  _ids.clear();
  _tokens.clear();
  _pairs.clear();
  _lengths->fill(0);
  for (uint32_t b = 0; b < kSingles; ++b) {
    const auto byte = static_cast<char>(b);
    Insert(std::string_view{&byte, 1});
  }
}

uint32_t OnPairCodec::Insert(std::string_view token) {
  const auto [it, fresh] =
    _ids.try_emplace(token, static_cast<uint32_t>(_tokens.size()));
  if (!fresh) {
    return it->second;
  }
  _tokens.emplace_back(token);
  if (token.size() > 1) {
    (*_lengths)[static_cast<uint8_t>(token[0]) |
                (static_cast<uint8_t>(token[1]) << 8)] |=
      static_cast<uint16_t>(1u << (token.size() - 1));
  }
  return it->second;
}

OnPairCodec::Match OnPairCodec::Longest(std::string_view rest) const {
  if (rest.size() > 1) {
    uint32_t lengths = (*_lengths)[static_cast<uint8_t>(rest[0]) |
                                   (static_cast<uint8_t>(rest[1]) << 8)] &
                       ((1u << std::min<size_t>(rest.size(), 16)) - 1);
    while (lengths > 1) {
      const auto len = static_cast<uint32_t>(std::bit_width(lengths));
      if (const auto it = _ids.find(rest.substr(0, len)); it != _ids.end()) {
        return {it->second, len};
      }
      lengths &= ~(1u << (len - 1));
    }
  }
  return {_ids.find(rest.substr(0, 1))->second, 1};
}

void OnPairCodec::Train(std::span<const duckdb::string_t> entries,
                        uint64_t raw) {
  Reset();
  const uint32_t cap = uint32_t{1} << kOnPairDictionaryBits;
  const uint64_t budget = std::min(
    raw, std::max<uint64_t>(
           static_cast<uint64_t>(static_cast<double>(raw) * kTrainFraction),
           kTrainMinBytes));
  Controller ctrl{cap - kSingles, budget};
  const uint64_t stride = std::max<uint64_t>(1, budget == 0 ? 1 : raw / budget);
  bool stop = false;
  for (uint64_t start = 0; start < stride && !stop; ++start) {
    for (uint64_t i = start; i < entries.size() && !stop; i += stride) {
      const std::string_view row{entries[i].GetData(), entries[i].GetSize()};
      if (row.empty()) {
        continue;
      }
      auto prev = Longest(row.substr(0, std::min<size_t>(row.size(), 16)));
      size_t pos = prev.len;
      if (ctrl.Scan(prev.len)) {
        stop = true;
        break;
      }
      while (pos < row.size()) {
        const auto cur =
          Longest(row.substr(pos, std::min<size_t>(row.size() - pos, 16)));
        if (ctrl.Scan(cur.len)) {
          stop = true;
          break;
        }
        const auto pair_len = prev.len + cur.len;
        if (pair_len <= kOnPairMaxToken) {
          const auto key = PairKey(prev.id, cur.id);
          auto& count = _pairs[key];
          count = count == kMaxThreshold ? count : count + 1;
          if (count >= ctrl.Threshold()) {
            const auto id = Insert(row.substr(pos - prev.len, pair_len));
            if (_tokens.size() == cap) {
              stop = true;
              break;
            }
            ctrl.Created();
            _pairs.erase(key);
            prev = {id, pair_len};
            pos += cur.len;
            continue;
          }
        }
        prev = cur;
        pos += cur.len;
      }
    }
  }
  Finalize();
}

void OnPairCodec::Finalize() {
  _order.resize(_tokens.size());
  std::iota(_order.begin(), _order.end(), 0);
  absl::c_stable_sort(
    _order, [&](uint32_t a, uint32_t b) { return _tokens[a] < _tokens[b]; });
  std::vector<std::string> sorted;
  sorted.reserve(_tokens.size());
  for (const auto t : _order) {
    sorted.emplace_back(std::move(_tokens[t]));
  }
  _tokens = std::move(sorted);
  _ids.clear();
  for (uint32_t id = 0; id < _tokens.size(); ++id) {
    _ids.try_emplace(_tokens[id], id);
  }
  _pairs.clear();
  _bits =
    static_cast<uint8_t>(std::max<int>(1, std::bit_width(_tokens.size() - 1)));
  const auto n = static_cast<uint32_t>(_tokens.size());
  _keys.resize(n);
  _firsts.assign(65537, 0);
  for (uint32_t t = 0; t < n; ++t) {
    _keys[t] = KeyOf(_tokens[t]);
    ++_firsts[static_cast<uint32_t>(_keys[t] >> 112) + 1];
  }
  for (uint32_t p = 1; p < _firsts.size(); ++p) {
    _firsts[p] += _firsts[p - 1];
  }
  _chains.resize(n);
  _stack.clear();
  for (uint32_t t = 0; t < n; ++t) {
    const std::string_view token = _tokens[t];
    while (!_stack.empty() && !token.starts_with(_tokens[_stack.back()])) {
      _stack.pop_back();
    }
    _stack.push_back(t);
    auto& chain = _chains[t];
    chain.lens = 0;
    for (const auto s : _stack) {
      const auto len = _tokens[s].size();
      chain.lens |= static_cast<uint16_t>(1u << (len - 1));
      for (auto l = len; l <= kOnPairMaxToken; ++l) {
        chain.ids[l - 1] = static_cast<uint16_t>(s);
      }
    }
  }
}

OnPairCodec::Key OnPairCodec::KeyOf(std::string_view bytes) noexcept {
  if (bytes.size() >= kOnPairMaxToken) {
    return (Key{absl::big_endian::Load64(bytes.data())} << 64) |
           absl::big_endian::Load64(bytes.data() + 8);
  }
  char buf[kOnPairMaxToken] = {};
  std::memcpy(buf, bytes.data(), bytes.size());
  return (Key{absl::big_endian::Load64(buf)} << 64) |
         absl::big_endian::Load64(buf + 8);
}

void OnPairCodec::Encode(std::span<const duckdb::string_t> entries,
                         std::span<uint16_t> codes,
                         std::span<uint32_t> counts) const {
  struct Lane {
    const char* rest;
    size_t left;
    uint16_t* out;
    uint16_t* begin;
    size_t entry;
  };
  constexpr size_t kLanes = 4;
  std::array<Lane, kLanes> lanes{};
  size_t next = 0;
  uint64_t offset = 0;
  const auto take = [&](Lane& lane) {
    while (next < entries.size()) {
      const auto& entry = entries[next];
      const auto size = entry.GetSize();
      auto* out = codes.data() + offset;
      offset += size;
      if (size == 0) {
        counts[next++] = 0;
        continue;
      }
      lane = {entry.GetData(), size, out, out, next++};
      return true;
    }
    return false;
  };
  size_t active = 0;
  while (active < kLanes && take(lanes[active])) {
    ++active;
  }
  const auto* keys = _keys.data();
  std::array<Key, kLanes> key{};
  std::array<uint32_t, kLanes> lo{};
  std::array<uint32_t, kLanes> n{};
  while (active != 0) {
    uint32_t widest = 1;
    for (size_t k = 0; k < kLanes; ++k) {
      if (k >= active) {
        lo[k] = 0;
        n[k] = 1;
        continue;
      }
      const auto& lane = lanes[k];
      key[k] = KeyOf({lane.rest, std::min(lane.left, kOnPairMaxToken)});
      const auto prefix = static_cast<uint32_t>(key[k] >> 112);
      lo[k] = _firsts[prefix] == 0 ? 0 : _firsts[prefix] - 1;
      n[k] = _firsts[prefix + 1] - lo[k];
      widest = std::max(widest, n[k]);
    }
    while (widest > 1) {
      for (size_t k = 0; k < kLanes; ++k) {
        const auto half = n[k] / 2;
        lo[k] = keys[lo[k] + half] <= key[k] ? lo[k] + half : lo[k];
        n[k] -= half;
      }
      widest -= widest / 2;
    }
    for (size_t k = 0; k < active;) {
      auto& lane = lanes[k];
      const auto diff = keys[lo[k]] ^ key[k];
      const auto common = diff == 0
                            ? kOnPairMaxToken
                            : static_cast<size_t>(std::countl_zero(diff) / 8);
      const auto& chain = _chains[lo[k]];
      const auto fits = chain.lens & ((1u << std::min(common, lane.left)) - 1);
      const auto len = static_cast<uint32_t>(std::bit_width(fits));
      *lane.out++ = chain.ids[len - 1];
      lane.rest += len;
      lane.left -= len;
      if (lane.left == 0) {
        counts[lane.entry] = static_cast<uint32_t>(lane.out - lane.begin);
        if (!take(lane)) {
          --active;
          lane = lanes[active];
          lo[k] = lo[active];
          key[k] = key[active];
          continue;
        }
      }
      ++k;
    }
  }
}

void OnPairCodec::Export(std::string& side) const {
  side.assign(kOnPairSideHeader, '\0');
  side[0] = static_cast<char>(_bits);
  duckdb::Store<uint32_t>(
    static_cast<uint32_t>(_tokens.size()),
    duckdb::data_ptr_cast(side.data() + sizeof(uint32_t)));
  const auto nibbles = (_tokens.size() + 1) / 2;
  const auto at = side.size();
  side.resize(at + nibbles, '\0');
  for (size_t t = 0; t < _tokens.size(); ++t) {
    side[at + t / 2] =
      static_cast<char>(static_cast<uint8_t>(side[at + t / 2]) |
                        ((_tokens[t].size() - 1) << (4 * (t % 2))));
  }
  for (const auto& token : _tokens) {
    side.append(token);
  }
}

const duckdb::CompressionFunction& OnPairFunction() {
  static const auto kFunction =
    text::StringFunction<text::PlainScan<text::OnPairLeaf>>(
      duckdb::CompressionType::COMPRESSION_COL_ONPAIR);
  return kFunction;
}

const duckdb::CompressionFunction& DictOnPairFunction() {
  static const auto kFunction =
    text::StringFunction<text::DictionaryScan<text::OnPairLeaf>>(
      duckdb::CompressionType::COMPRESSION_COL_DICT_ONPAIR);
  return kFunction;
}

}  // namespace irs::codecs
