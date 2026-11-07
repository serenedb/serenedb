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

#include <algorithm>
#include <bit>
#include <duckdb/common/helper.hpp>
#include <numeric>

#include "iresearch/formats/column/codecs/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/onpair_leaf.tpp"
#include "iresearch/formats/column/codecs/plain_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

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
  _starts.resize(count + 1);
  uint32_t at = 0;
  for (uint32_t t = 0; t < count; ++t) {
    const auto packed = static_cast<uint8_t>(rest[t / 2]) >> (4 * (t % 2));
    _starts[t] = at;
    at += (packed & 0xF) + 1;
  }
  _starts[count] = at;
  if (at != bytes.size()) {
    return false;
  }
  _bytes.assign(bytes);
  _bytes.append(kOnPairMaxToken, '\0');
  _count = count;
  _mask = static_cast<uint32_t>((uint64_t{1} << _bits) - 1);
  return true;
}

uint64_t OnPairTable::Decode(const uint8_t* stream, uint64_t first,
                             uint64_t count, char* out,
                             uint64_t capacity) const {
  uint64_t off = 0;
  for (uint64_t i = first; i < first + count; ++i) {
    const auto bit = i * _bits;
    uint32_t word = 0;
    std::memcpy(&word, stream + bit / 8, 3);
    const auto code = (word >> (bit % 8)) & _mask;
    SDB_ENSURE(code < _count, "col codec: corrupted entry");
    const auto from = _starts[code];
    const auto len = _starts[code + 1] - from;
    SDB_ENSURE(off + len <= capacity, "col codec: corrupted entry");
    if (off + kOnPairMaxToken <= capacity) {
      std::memcpy(out + off, _bytes.data() + from, kOnPairMaxToken);
    } else {
      std::memcpy(out + off, _bytes.data() + from, len);
    }
    off += len;
  }
  return off;
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

void OnPairCodec::Train(std::span<const std::string_view> entries,
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
      const auto row = entries[i];
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
}

void OnPairCodec::Encode(std::string_view entry,
                         std::vector<uint16_t>& codes) const {
  size_t pos = 0;
  while (pos < entry.size()) {
    const auto m =
      Longest(entry.substr(pos, std::min<size_t>(entry.size() - pos, 16)));
    codes.push_back(static_cast<uint16_t>(m.id));
    pos += m.len;
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
