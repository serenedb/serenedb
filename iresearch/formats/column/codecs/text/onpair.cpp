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

#include "iresearch/formats/column/codecs/text/onpair.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/text/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/text/plain_scan.tpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs::text::onpair {

inline std::string_view Side(const Header& h,
                             duckdb::const_data_ptr_t base) noexcept {
  return {reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size};
}

inline FrameMeta TokenFrame(const FrameMeta& m, uint8_t bits) noexcept {
  const auto stream = m.comp_len < kPadding ? 0 : m.comp_len - kPadding;
  return {m.first_entry, m.raw_len, 0,
          static_cast<uint32_t>(uint64_t{stream} * 8 / bits)};
}

inline uint64_t FirstToken(ChainGroups& groups, PackedReader& counts,
                           const FrameMeta& tokens, uint32_t f, uint32_t e) {
  const auto g = (e - tokens.first_entry) / kChainRestart;
  uint64_t first = groups.Offset(counts, tokens, f, g);
  for (auto k = tokens.first_entry + g * kChainRestart; k < e; ++k) {
    first += counts.At(k);
  }
  return first;
}

struct Scan {
  static constexpr uint32_t kEntryGroup = 1;

  template<typename Frames>
  Scan(const ColumnSegment&, const Frames& s) {
    SDB_ENSURE(table.Open(Side(s.header, s.base)),
               "col codec: corrupted symbol table");
    groups.Reset(static_cast<uint32_t>(s.frames.size()), [&](uint32_t f) {
      return std::pair{s.frames[f].first_entry, s.FrameEnd(f)};
    });
    entry_decoded.assign(s.header.entry_count, 0);
  }

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto tokens = TokenFrame(m, table.Bits());
    const auto* stream =
      reinterpret_cast<const uint8_t*>(s.Data()) + m.comp_off;
    const auto end = s.FrameEnd(f);
    uint64_t token = 0;
    uint64_t off = 0;
    for (auto e = m.first_entry; e < end; ++e) {
      const uint64_t count = s.length_reader.At(e);
      SDB_ENSURE(token + count <= tokens.comp_len,
                 "col codec: corrupted entry");
      const auto n =
        table.Decode(stream, token, count, out + off, m.raw_len - off);
      const std::string_view v{out + off, n};
      SDB_ASSERT(!Frames::kDedup || e == m.first_entry ||
                 View(s.values[e - 1]) < v);
      s.Keep(e, v);
      off += n;
      token += count;
    }
    SDB_ENSURE(off == m.raw_len, "col codec: corrupted frame");
  }

  template<typename Frames>
  std::string_view Decode(Frames& s, uint32_t e, uint32_t f, std::string& buf) {
    const auto& m = s.frames[f];
    const auto tokens = TokenFrame(m, table.Bits());
    const auto first = FirstToken(groups, s.length_reader, tokens, f, e);
    const uint64_t count = s.length_reader.At(e);
    SDB_ENSURE(first + count <= tokens.comp_len, "col codec: corrupted entry");
    if (const auto need = table.Capacity(count); buf.size() < need) {
      buf.resize(std::max<uint64_t>(need, buf.size() * 2));
    }
    const auto n =
      table.Decode(reinterpret_cast<const uint8_t*>(s.Data()) + m.comp_off,
                   first, count, buf.data(), buf.size());
    return {buf.data(), n};
  }

  template<typename Frames>
  void EnsureEntry(Frames& s, uint32_t e, uint32_t f) {
    if (entry_decoded[e]) {
      return;
    }
    const auto v = Decode(s, e, f, probe);
    char* out = duckdb::char_ptr_cast(
      duckdb::StringVector::GetStringAllocator(s.dictionary->data)
        .Allocate(v.size()));
    if (!v.empty()) {
      std::memcpy(out, v.data(), v.size());
    }
    s.Keep(e, std::string_view{out, v.size()});
    entry_heap_bytes += v.size();
    entry_decoded[e] = 1;
  }

  template<typename Frames, typename Before>
  uint32_t Bound(Frames& s, Before before) {
    if (s.frames.empty()) {
      return s.header.entry_count;
    }
    return static_cast<uint32_t>(PartitionIndex(
      s.frames[0].first_entry, s.header.entry_count, [&](size_t i) {
        const auto e = static_cast<uint32_t>(i);
        const auto f = s.FrameOf(e);
        if (s.decoded[f] || entry_decoded[e]) {
          return before(View(s.values[e]));
        }
        return before(Decode(s, e, f, probe));
      }));
  }

  uint64_t HeapBytes() const noexcept {
    return entry_heap_bytes + table.Bytes();
  }

  Table table;
  ChainGroups groups;
  std::vector<uint8_t> entry_decoded;
  uint64_t entry_heap_bytes = 0;
  std::string probe;
};

struct Fetch {
  void Reset(const Header&, duckdb::data_ptr_t) {
    table.reset();
    groups.Clear();
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(const ColumnSegment&, const Header& h, duckdb::data_ptr_t base,
            PackedReader& counts, uint32_t lo, const FrameMeta& f,
            uint32_t entry, duckdb::Vector& result, duckdb::idx_t result_idx,
            bool) {
    if (!table) {
      Open(h, base);
    }
    const auto tokens = TokenFrame(f, table->Bits());
    const auto first = FirstToken(groups, counts, tokens, lo, entry);
    const uint64_t count = counts.At(entry);
    SDB_ENSURE(first + count <= tokens.comp_len, "col codec: corrupted entry");
    if (const auto need = table->Capacity(count); buf.size() < need) {
      buf.resize(std::max<uint64_t>(need, buf.size() * 2));
    }
    const auto n = table->Decode(
      reinterpret_cast<const uint8_t*>(base + h.off_data) + f.comp_off, first,
      count, buf.data(), buf.size());
    ++walked;
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, buf.data(), n);
  }

  void Open(const Header& h, duckdb::const_data_ptr_t base) {
    table.emplace();
    SDB_ENSURE(table->Open(Side(h, base)), "col codec: corrupted symbol table");
    const auto* frames = base + h.off_frames;
    groups.Reset(h.frame_count, [&](uint32_t i) {
      return std::pair{FrameAt(frames, i).first_entry,
                       i + 1 < h.frame_count
                         ? FrameAt(frames, i + 1).first_entry
                         : h.entry_count};
    });
  }

  std::optional<Table> table;
  ChainGroups groups;
  std::string buf;
  uint64_t walked = 0;
};

struct Leaf {
  static constexpr ByteCodec kCodec = ByteCodec::OnPair;
  using Scan = onpair::Scan;
  using Fetch = onpair::Fetch;
};

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

bool Table::Open(std::string_view side) {
  if (side.size() < kSideHeader) {
    return false;
  }
  _bits = static_cast<uint8_t>(side[0]);
  const auto count = duckdb::Load<uint32_t>(
    reinterpret_cast<duckdb::const_data_ptr_t>(side.data() + sizeof(uint32_t)));
  if (_bits == 0 || _bits > 16 || count == 0 ||
      count > (uint64_t{1} << _bits)) {
    return false;
  }
  const auto rest = side.substr(kSideHeader);
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
  _bytes.append(kMaxToken, '\0');
  _count = count;
  _mask = static_cast<uint32_t>((uint64_t{1} << _bits) - 1);
  return true;
}

void Table::Corrupted() {
  SDB_ENSURE(false, "col codec: corrupted entry");
  SDB_UNREACHABLE();
}

Codec::Codec() : _lengths{std::make_unique<std::array<uint16_t, 65536>>()} {}

void Codec::Reset() {
  _ids.clear();
  _tokens.clear();
  _pairs.clear();
  _lengths->fill(0);
  for (uint32_t b = 0; b < kSingles; ++b) {
    const auto byte = static_cast<char>(b);
    Insert(std::string_view{&byte, 1});
  }
}

uint32_t Codec::Insert(std::string_view token) {
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

Codec::Match Codec::Longest(const char* rest, size_t left) const {
  if (left > 1) {
    uint32_t lengths = (*_lengths)[static_cast<uint8_t>(rest[0]) |
                                   (static_cast<uint8_t>(rest[1]) << 8)] &
                       ((1u << std::min<size_t>(left, kMaxToken)) - 1);
    while (lengths > 1) {
      const auto len = static_cast<uint32_t>(std::bit_width(lengths));
      if (const auto it = _ids.find(std::string_view{rest, len});
          it != _ids.end()) {
        return {it->second, len};
      }
      lengths &= ~(1u << (len - 1));
    }
  }
  return {_ids.find(std::string_view{rest, 1})->second, 1};
}

void Codec::Train(std::span<const duckdb::string_t> entries, uint64_t raw) {
  Reset();
  const uint32_t cap = uint32_t{1} << kDictionaryBits;
  const uint64_t budget = std::min(
    raw, std::max<uint64_t>(
           static_cast<uint64_t>(static_cast<double>(raw) * kTrainFraction),
           kTrainMinBytes));
  Controller ctrl{cap - kSingles, budget};
  const uint64_t stride = std::max<uint64_t>(1, budget == 0 ? 1 : raw / budget);
  bool stop = false;
  for (uint64_t start = 0; start < stride && !stop; ++start) {
    for (uint64_t i = start; i < entries.size() && !stop; i += stride) {
      const auto& row = entries[i];
      const size_t size = row.GetSize();
      if (size == 0) {
        continue;
      }
      const char* data = row.GetData();
      auto prev = Longest(data, size);
      size_t pos = prev.len;
      if (ctrl.Scan(prev.len)) {
        stop = true;
        break;
      }
      while (pos < size) {
        const auto cur = Longest(data + pos, size - pos);
        if (ctrl.Scan(cur.len)) {
          stop = true;
          break;
        }
        const auto pair_len = prev.len + cur.len;
        if (pair_len <= kMaxToken) {
          const auto key = PairKey(prev.id, cur.id);
          auto& count = _pairs[key];
          count = count == kMaxThreshold ? count : count + 1;
          if (count >= ctrl.Threshold()) {
            const auto id = Insert({data + pos - prev.len, pair_len});
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

void Codec::Finalize() {
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
      for (auto l = len; l <= kMaxToken; ++l) {
        chain.ids[l - 1] = static_cast<uint16_t>(s);
      }
    }
  }
}

Codec::Key Codec::KeyOf(std::string_view bytes) noexcept {
  if (bytes.size() >= kMaxToken) {
    return (Key{absl::big_endian::Load64(bytes.data())} << 64) |
           absl::big_endian::Load64(bytes.data() + 8);
  }
  char buf[kMaxToken] = {};
  std::memcpy(buf, bytes.data(), bytes.size());
  return (Key{absl::big_endian::Load64(buf)} << 64) |
         absl::big_endian::Load64(buf + 8);
}

void Codec::Encode(std::span<const duckdb::string_t> entries,
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
      key[k] = KeyOf({lane.rest, std::min(lane.left, kMaxToken)});
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
      const auto common =
        diff == 0 ? kMaxToken : static_cast<size_t>(std::countl_zero(diff) / 8);
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

void Codec::Export(std::string& side) const {
  side.assign(kSideHeader, '\0');
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

void CompressionState::FinalizeCompress() {
  if (_rows.SegmentShape() == Shape::Dictionary) {
    _rows.Sort(_sort);
  }
  const auto lengths = _rows.Lengths();
  const auto rows = _rows.View();
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  _codec.Train(entries, rows.raw);
  uint64_t total = 0;
  for (const auto& entry : entries) {
    total += entry.GetSize();
  }
  _codes.resize(total);
  _codec.Encode(entries, _codes, lengths);
  _frames.clear();
  uint32_t max_codes = 0;
  uint64_t frame_raw = 0;
  size_t frame_first = 0;
  uint64_t frame_region = 0;
  uint64_t region = 0;
  const auto close = [&](size_t end) {
    const auto at = Written();
    Pack(entries, lengths, frame_first, end, frame_region);
    _frames.emplace_back(static_cast<uint32_t>(frame_first + first_entry),
                         static_cast<uint32_t>(frame_raw),
                         static_cast<uint32_t>(at),
                         static_cast<uint32_t>(Written() - at));
  };
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto size = entries[i].GetSize();
    if (frame_raw != 0 && frame_raw + size > kFsstFrameRawBytes) {
      close(i);
      frame_raw = 0;
      frame_first = i;
      frame_region = region;
    }
    max_codes = std::max(max_codes, lengths[i]);
    frame_raw += size;
    region += size;
  }
  if (!entries.empty()) {
    close(entries.size());
  }
  _codec.Export(_side);
  RecordRatio(_history[static_cast<size_t>(_rows.SegmentShape())], rows.raw,
              Written() + _side.size(), _target);
  _rows.Write(
    {
      .codec = kCodec,
      .frames = _frames,
      .max_length = max_codes,
      .symtab = _side,
    },
    *_out);
}

void CompressionState::Pack(std::span<const duckdb::string_t> entries,
                            std::span<const uint32_t> lengths, size_t first,
                            size_t end, uint64_t region) {
  const auto bits = _codec.Bits();
  uint64_t count = 0;
  for (auto e = first; e < end; ++e) {
    count += lengths[e];
  }
  const uint64_t bytes = (count * bits + 7) / 8;
  auto* out = _out->bytes.Extend(bytes + kPadding);
  uint64_t acc = 0;
  uint32_t fill = 0;
  for (auto e = first; e < end; ++e) {
    const auto* codes = _codes.data() + region;
    for (uint32_t c = 0; c < lengths[e]; ++c) {
      acc |= uint64_t{codes[c]} << fill;
      fill += bits;
      while (fill >= 8) {
        *out++ = static_cast<char>(acc & 0xFF);
        acc >>= 8;
        fill -= 8;
      }
    }
    region += entries[e].GetSize();
  }
  if (fill != 0) {
    *out++ = static_cast<char>(acc & 0xFF);
  }
  std::memset(out, 0, kPadding);
}

}  // namespace irs::codecs::text::onpair
namespace irs::codecs {

constinit const Codec kOnPairCodec =
  text::StringCodec<text::PlainScan<text::onpair::Leaf>>();

constinit const Codec kDictOnPairCodec =
  text::StringCodec<text::DictionaryScan<text::onpair::Leaf>>();

}  // namespace irs::codecs
