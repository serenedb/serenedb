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

#include <absl/strings/match.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/text/fsst.hpp"
#include "iresearch/formats/column/codecs/text/plain_scan.tpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text::fsst_plus {
namespace {

template<typename Rows>
class CompressionState final : public SortedCompressionState<Rows> {
  using Base = SortedCompressionState<Rows>;

 public:
  static constexpr ByteCodec kCodec = ByteCodec::FsstPlus;

  CompressionState(ColumnDataCheckpointData& data, DedupScratch& dedup)
    : Base{kCodec, data.type, dedup, data.tuning.strings,
           data.params.segment_target},
      _codec{data.Get<fsst::SharedEncoder<fsst::Fsst8, kCodec>>().encoder} {}

  void FinalizeCompress();

 private:
  using Base::_frames;
  using Base::_out;
  using Base::_rows;
  using Base::_target;
  using Base::Written;

  struct Node {
    uint32_t left;
    uint32_t right;
  };

  struct Range {
    uint32_t first;
    uint32_t last;
    uint32_t node;
  };

  void Cleave(uint32_t first_entry);

  fsst::Encoder<fsst::Fsst8>& _codec;
  std::string _encoded;
  std::string _prefixes;
  std::string _side;
  std::vector<uint32_t> _encoded_lengths;
  std::vector<std::string_view> _codes;
  std::vector<uint32_t> _shared;
  std::vector<uint32_t> _ids;
  std::vector<uint32_t> _ends;
  std::vector<uint32_t> _lcp;
  std::vector<Node> _tree;
  std::vector<uint32_t> _order;
  std::vector<uint32_t> _size;
  std::vector<uint64_t> _saved;
  std::vector<uint8_t> _whole;
  std::vector<Range> _ranges;
};

class Prefixes {
 public:
  bool Open(std::string_view section, uint32_t table_size,
            fsst::Decoder<fsst::Fsst8>& decoder) {
    SDB_ASSERT(section.size() >=
               static_cast<uint64_t>(table_size) + sizeof(uint32_t));
    if (table_size != 0 &&
        !decoder.Import(nullptr, section.substr(0, table_size))) {
      return false;
    }
    const auto rest = section.substr(table_size);
    _count = duckdb::Load<uint32_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(rest.data()));
    const auto head = sizeof(uint32_t) * (uint64_t{_count} + 1);
    if (head > rest.size() || (table_size == 0 && _count != 0)) {
      return false;
    }
    _ends = rest.data() + sizeof(uint32_t);
    _bytes = rest.substr(head);
    uint32_t prev = 0;
    for (uint32_t g = 0; g < _count; ++g) {
      const auto end = End(g);
      if (end <= prev || end > _bytes.size()) {
        return false;
      }
      prev = end;
    }
    return prev == _bytes.size();
  }

  uint32_t Count() const noexcept { return _count; }

  std::string_view Encoded(uint32_t id) const noexcept {
    SDB_ASSERT(id != 0 && id <= _count);
    const auto from = id == 1 ? 0 : End(id - 2);
    return _bytes.substr(from, End(id - 1) - from);
  }

 private:
  uint32_t End(uint32_t g) const noexcept {
    return duckdb::Load<uint32_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(_ends + g * sizeof(uint32_t)));
  }

  const char* _ends = nullptr;
  std::string_view _bytes;
  uint32_t _count = 0;
};

std::string_view SymbolTable(const Header& h,
                             duckdb::const_data_ptr_t base) noexcept {
  return {reinterpret_cast<const char*>(base + h.off_symtab), h.symtab_size};
}

uint64_t EntryOffset(ChainGroups& groups, PackedReader& lengths,
                     const FrameMeta& m, uint32_t f, uint32_t e) {
  const auto g = (e - m.first_entry) / kChainRestart;
  uint64_t offset = groups.Offset(lengths, m, f, g);
  for (auto k = m.first_entry + g * kChainRestart; k < e; ++k) {
    offset += lengths.At(k);
  }
  return offset;
}

struct Fetch;

struct Scan {
  static constexpr ByteCodec kCodec = ByteCodec::FsstPlus;
  using Fetch = fsst_plus::Fetch;

  template<typename Frames>
  Scan(const ColumnSegment&, const Frames& s)
    : ids{s.base + s.header.off_lcps, s.header.lcp_width} {
    if (!prefixes.Open(SymbolTable(s.header, s.base), s.header.dictionary_size,
                       decoder)) [[unlikely]] {
      ThrowCorrupted("text", "symbol table is inconsistent");
    }
    prefix_ends.assign(prefixes.Count() + 1, 0);
    for (uint32_t id = 1; id <= prefixes.Count(); ++id) {
      const auto encoded = prefixes.Encoded(id);
      const auto at = heap.size();
      heap.resize(at + encoded.size() * fsst::kMaxExpansion);
      const auto room = heap.size() - at;
      const auto n =
        decoder.Decode(encoded.data(), encoded.size(), heap.data() + at, room);
      if (n > room) [[unlikely]] {
        ThrowCorrupted("text", "prefix is inconsistent");
      }
      heap.resize(at + n);
      prefix_ends[id] = heap.size();
    }
    groups.Reset(static_cast<uint32_t>(s.frames.size()), [&](uint32_t f) {
      return std::pair{s.frames[f].first_entry, s.FrameEnd(f)};
    });
    entry_decoded.assign(s.header.entry_count, 0);
  }

  std::string_view Prefix(uint32_t id) const noexcept {
    return id == 0
             ? std::string_view{}
             : std::string_view{heap}.substr(
                 prefix_ends[id - 1], prefix_ends[id] - prefix_ends[id - 1]);
  }

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto end = s.FrameEnd(f);
    const uint64_t enc_end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
    uint64_t enc_off = m.comp_off;
    uint64_t off = 0;
    for (auto e = m.first_entry; e < end; ++e) {
      const auto id = ids.At(e);
      const uint64_t enc = s.length_reader.At(e);
      if (!(id <= prefixes.Count() && enc_off + enc <= enc_end)) [[unlikely]] {
        ThrowCorrupted("text", "entry is inconsistent");
      }
      const auto prefix = Prefix(id);
      if (off + prefix.size() > m.raw_len) [[unlikely]] {
        ThrowCorrupted("text", "frame is inconsistent");
      }
      if (!prefix.empty()) {
        std::memcpy(out + off, prefix.data(), prefix.size());
      }
      const auto room = m.raw_len - off - prefix.size();
      const auto dec = enc == 0
                         ? 0
                         : decoder.Decode(s.Data() + enc_off, enc,
                                          out + off + prefix.size(), room);
      if (dec > room) [[unlikely]] {
        ThrowCorrupted("text", "entry is inconsistent");
      }
      const std::string_view v{out + off, prefix.size() + dec};
      s.Keep(e, v);
      off += v.size();
      enc_off += enc;
    }
    if (off != m.raw_len || enc_off != enc_end) [[unlikely]] {
      ThrowCorrupted("text", "frame is inconsistent");
    }
  }

  template<typename Frames, typename Out>
  std::string_view Decode(Frames& s, uint32_t e, uint32_t f, Out& out) {
    const auto& m = s.frames[f];
    const auto enc_off = EntryOffset(groups, s.length_reader, m, f, e);
    const uint64_t enc = s.length_reader.At(e);
    const auto id = ids.At(e);
    if (!(id <= prefixes.Count() &&
          enc_off + enc <= static_cast<uint64_t>(m.comp_off) + m.comp_len))
      [[unlikely]] {
      ThrowCorrupted("text", "entry is inconsistent");
    }
    const auto prefix = Prefix(id);
    const auto room = enc * fsst::kMaxExpansion;
    auto* at = Reserve(out, prefix.size() + room);
    if (!prefix.empty()) {
      std::memcpy(at, prefix.data(), prefix.size());
    }
    const auto dec = enc == 0 ? 0
                              : decoder.Decode(s.Data() + enc_off, enc,
                                               at + prefix.size(), room);
    if (dec > room) [[unlikely]] {
      ThrowCorrupted("text", "entry is inconsistent");
    }
    if constexpr (std::is_same_v<Out, duckdb::ArenaAllocator>) {
      out.ShrinkHead(room - dec);
    }
    return {at, prefix.size() + dec};
  }

  template<typename Frames>
  void EnsureEntry(Frames& s, uint32_t e, uint32_t f) {
    if (entry_decoded[e]) {
      return;
    }
    const auto v = Decode(
      s, e, f, duckdb::StringVector::GetStringAllocator(s.dictionary->data));
    s.Keep(e, v);
    entry_heap_bytes += v.size();
    entry_decoded[e] = 1;
  }

  template<typename Frames, typename Entry>
  void Ensure(Frames& s, duckdb::idx_t, Entry, uint32_t first, uint32_t end) {
    s.EnsureRange(first, end);
  }

  template<typename Frames, typename Entry>
  void Select(Frames& s, duckdb::Vector& result, duckdb::idx_t result_offset,
              duckdb::idx_t count, Entry entry_at, uint32_t window_first,
              uint32_t window_end) {
    s.EnsureSparse(count, entry_at, window_first, window_end);
    s.WriteRefs(result, result_offset, count, entry_at);
  }

  template<typename Frames, typename Before>
  uint32_t Bound(Frames& s, Before before) {
    SDB_ASSERT(!s.frames.empty());
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

  uint64_t HeapBytes() const noexcept { return entry_heap_bytes + heap.size(); }

  fsst::Decoder<fsst::Fsst8> decoder;
  Prefixes prefixes;
  PackedReader ids{nullptr, 0};
  ChainGroups groups;
  std::string heap;
  std::vector<uint64_t> prefix_ends;
  std::vector<uint8_t> entry_decoded;
  uint64_t entry_heap_bytes = 0;
  std::string probe;
};

struct Fetch {
  void Reset(const Header& h, duckdb::data_ptr_t base) {
    decoder.reset();
    groups.Clear();
    ids = PackedReader{base + h.off_lcps, h.lcp_width};
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(const ColumnSegment&, const Header& h, duckdb::data_ptr_t base,
            PackedReader& lengths, uint32_t lo, const FrameMeta& f,
            uint32_t entry, duckdb::Vector& result, duckdb::idx_t result_idx,
            bool) {
    if (groups.Empty()) {
      Open(h, base);
    }
    const auto enc_off = EntryOffset(groups, lengths, f, lo, entry);
    const uint64_t enc = lengths.At(entry);
    const auto id = ids.At(entry);
    if (!(id <= prefixes.Count() &&
          enc_off + enc <= static_cast<uint64_t>(f.comp_off) + f.comp_len))
      [[unlikely]] {
      ThrowCorrupted("text", "entry is inconsistent");
    }
    const auto prefix = id == 0 ? std::string_view{} : prefixes.Encoded(id);
    const auto capacity = (prefix.size() + enc) * fsst::kMaxExpansion;
    auto* at = Reserve(buf, capacity);
    const auto head =
      prefix.empty()
        ? 0
        : decoder->Decode(prefix.data(), prefix.size(), at, capacity);
    if (head > capacity) [[unlikely]] {
      ThrowCorrupted("text", "prefix is inconsistent");
    }
    const auto room = capacity - head;
    const auto dec =
      enc == 0 ? 0
               : decoder->Decode(
                   reinterpret_cast<const char*>(base + h.off_data) + enc_off,
                   enc, at + head, room);
    if (dec > room) [[unlikely]] {
      ThrowCorrupted("text", "entry is inconsistent");
    }
    ++walked;
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    out[result_idx] =
      duckdb::StringVector::AddStringOrBlob(result, at, head + dec);
  }

  void Open(const Header& h, duckdb::const_data_ptr_t base) {
    decoder.emplace();
    if (!prefixes.Open(SymbolTable(h, base), h.dictionary_size, *decoder))
      [[unlikely]] {
      ThrowCorrupted("text", "symbol table is inconsistent");
    }
    const auto* table = base + h.off_frames;
    groups.Reset(h.frame_count, [&](uint32_t i) {
      return std::pair{FrameAt(table, i).first_entry,
                       i + 1 < h.frame_count ? FrameAt(table, i + 1).first_entry
                                             : h.entry_count};
    });
  }

  std::optional<fsst::Decoder<fsst::Fsst8>> decoder;
  Prefixes prefixes;
  PackedReader ids{nullptr, 0};
  ChainGroups groups;
  std::string buf;
  uint64_t walked = 0;
};

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

template<typename Rows>
void CompressionState<Rows>::FinalizeCompress() {
  this->Sort();
  const auto lengths = _rows.Lengths();
  const auto rows = _rows.View();
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  const auto n = entries.size();
  _shared.assign(n, 0);
  _encoded_lengths.resize(n);
  uint64_t total = 0;
  for (const auto& entry : entries) {
    total += entry.GetSize();
  }
  fsst::Encoded encoded;
  _encoded.resize_and_overwrite(
    fsst::kEncodeExpansion * total + fsst::kEncodeSlack,
    [&](char* out, size_t capacity) {
      encoded =
        _codec.Encode(entries, _shared, out, capacity, _encoded_lengths);
      return encoded.bytes;
    });
  SDB_ENSURE(encoded.strings == n, "fsst: compression ran out of space");
  _codes.resize(n);
  uint64_t at = 0;
  for (size_t i = 0; i < n; ++i) {
    _codes[i] = std::string_view{_encoded}.substr(at, _encoded_lengths[i]);
    at += _encoded_lengths[i];
  }
  _ids.assign(first_entry + n, 0);
  _ends.clear();
  _prefixes.clear();
  Cleave(first_entry);
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
    const auto size = entries[i].GetSize();
    if (frame_raw != 0 && frame_raw + size > kFsstFrameRawBytes) {
      close(Written());
      frame_raw = 0;
      frame_first = i;
      frame_off = Written();
    }
    const auto suffix = _codes[i].substr(_shared[i]);
    if (!suffix.empty()) {
      std::memcpy(_out->bytes.Extend(suffix.size()), suffix.data(),
                  suffix.size());
    }
    lengths[i] = static_cast<uint32_t>(suffix.size());
    max_length = std::max(max_length, lengths[i]);
    frame_raw += size;
  }
  if (n != 0) {
    close(Written());
  }
  const auto table = _codec.SymbolTable();
  _side.assign(table);
  AppendU32(_side, static_cast<uint32_t>(_ends.size()));
  for (const auto end : _ends) {
    AppendU32(_side, end);
  }
  _side.append(_prefixes);
  _ids.resize(
    duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount(rows)),
    0);
  RecordRatio(this->History(), rows.raw, Written() + _side.size(), _target);
  _rows.Write(
    {
      .codec = kCodec,
      .frames = _frames,
      .max_length = max_length,
      .lcps = _ids,
      .max_lcp = static_cast<uint32_t>(_ends.size()),
      .symtab = _side,
      .dictionary_size = static_cast<uint32_t>(table.size()),
    },
    *_out);
}

template<typename Rows>
void CompressionState<Rows>::Cleave(uint32_t first_entry) {
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

}  // namespace
}  // namespace irs::codecs::text::fsst_plus
namespace irs::codecs {

/*
FSST+ segments (fsst_plus, dict_fsst_plus) keep the layout of the fsst
segments (fsst_front.cpp), with entries grouped instead of front-coded: a group
of neighbouring entries stores the FSST codes they share once, and each entry
stores the rest of its codes.
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each entry's codes after its group's     |
|   shared codes                                       |
+------------------------------------------------------+
|                    Entry groups                      |
|   +----------------------------------------------+   |
|   |   bitpacked group[entry_count]               |   |
|   +----------------------------------------------+   |
|   1-based group of each entry, 0 for none            |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table, dictionary_size bytes   |   |
|   |   uint32 group_count                         |   |
|   |   uint32 shared_end[group_count]             |   |
|   |   shared codes of every group                |   |
|   +----------------------------------------------+   |
|   group g's shared codes end at shared_end[g - 1]    |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   each entry's codes after its group's       |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries; a       |
|   longer entry is a frame of its own                 |
+------------------------------------------------------+
*/

constinit const Codec kFsstPlusCodec =
  text::StringCodec<text::PlainScan<text::fsst_plus::Scan>,
                    text::fsst_plus::CompressionState<text::PlainRows>>();

constinit const Codec kDictFsstPlusCodec =
  text::StringCodec<text::DictionaryScan<text::fsst_plus::Scan>,
                    text::fsst_plus::CompressionState<text::DictionaryRows>>();

}  // namespace irs::codecs
