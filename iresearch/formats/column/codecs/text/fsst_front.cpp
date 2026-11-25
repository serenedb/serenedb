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

#include <fsst12.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/text/fsst.hpp"
#include "iresearch/formats/column/codecs/text/plain_scan.tpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/text/text_function.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text::fsst {
namespace {

struct Fsst12 {
  static constexpr ByteCodec kCodec = ByteCodec::Fsst12;
  static constexpr bool kSharedTable = true;

  using Encoding = duckdb_fsst12_encoder_t;
  using Table = duckdb_fsst12_decoder_t;
  using Exported = std::vector<unsigned char>;

  static Encoding* Create(size_t n, size_t* lengths,
                          const unsigned char** strings) {
    return duckdb_fsst12_create(n, lengths, strings, 0);
  }

  static void Destroy(Encoding* encoder) noexcept {
    duckdb_fsst12_destroy(encoder);
  }

  static size_t Export(Encoding* encoder, Exported& table) {
    table.resize(sizeof(Table));
    return duckdb_fsst12_export(encoder, table.data());
  }

  static size_t Compress(Encoding* encoder, size_t n, size_t* lengths,
                         const unsigned char** strings, size_t capacity,
                         unsigned char* out, size_t* out_lengths,
                         unsigned char** out_strings) {
    return duckdb_fsst12_compress(encoder, n, lengths, strings, capacity, out,
                                  out_lengths, out_strings);
  }

  using Imported = std::shared_ptr<const ImportedFsst12>;

  static bool Import(Imported& imported, std::string_view table) {
    auto fresh = std::make_shared<ImportedFsst12>();
    fresh->valid =
      duckdb_fsst12_import(&fresh->table,
                           reinterpret_cast<const unsigned char*>(table.data()),
                           table.size()) == table.size();
    imported = std::move(fresh);
    return imported->valid;
  }

  static bool Import(Imported& imported, std::string_view table,
                     const ColumnSegment& segment, bool shared) {
    auto* slot = shared ? segment.symbols : nullptr;
    if (!slot) {
      return Import(imported, table);
    }
    if (slot->fsst12 && slot->file_id == segment.cache_slot.file_id &&
        slot->offset == segment.shared_offset &&
        slot->size == segment.shared_size) {
      imported = slot->fsst12;
      return imported->valid;
    }
    Import(imported, table);
    *slot = {
      .file_id = segment.cache_slot.file_id,
      .offset = segment.shared_offset,
      .size = segment.shared_size,
      .fsst12 = imported,
    };
    return imported->valid;
  }

  static size_t Decompress(const Imported& imported, const unsigned char* in,
                           size_t size, unsigned char* out,
                           size_t capacity) noexcept {
    return imported && imported->valid
             ? duckdb_fsst12_decompress(&imported->table, size, in, capacity,
                                        out)
             : DUCKDB_FSST12_DECOMPRESS_INVALID_INPUT;
  }

  static size_t DecompressPrefix(const Imported& imported,
                                 const unsigned char* in, size_t size,
                                 unsigned char* out, size_t bytes) noexcept {
    return imported && imported->valid
             ? duckdb_fsst12_decompress_prefix(&imported->table, size, in,
                                               bytes, out)
             : DUCKDB_FSST12_DECOMPRESS_INVALID_INPUT;
  }
};

}  // namespace
}  // namespace irs::codecs::text::fsst
namespace irs::codecs::text::fsst_front {
namespace {

constexpr uint64_t kChainGroupCost = 180;
constexpr uint64_t kChainMinRowBytes = 8;
constexpr uint64_t kCopyChunk = 32;
static_assert(kCopyChunk > fsst::kPrefixSlack);

IRS_FORCE_INLINE void CopyChunks(char* to, const char* from,
                                 uint64_t bytes) noexcept {
  uint64_t at = 0;
  do {
    char chunk[kCopyChunk];
    std::memcpy(chunk, from + at, kCopyChunk);
    std::memcpy(to + at, chunk, kCopyChunk);
    at += kCopyChunk;
  } while (at < bytes);
}

struct ChainEnd {
  uint32_t entry;
  uint64_t written;
  uint64_t enc_off;
  uint64_t prev_len;
};

uint64_t ChainCapacity(PackedReader& lengths, PackedReader& lcps,
                       uint32_t first, uint32_t end) {
  uint64_t capacity = 0;
  for (auto e = first; e < end; ++e) {
    capacity += (e == first ? 0 : lcps.At(e)) +
                uint64_t{lengths.At(e)} * fsst::kMaxExpansion;
  }
  return capacity;
}

template<typename Impl>
uint64_t DecodeSuffix(const fsst::Decoder<Impl>& fsst, const char* in,
                      uint64_t enc, uint64_t lcp, std::string& buf) {
  const auto room = enc * fsst::kMaxExpansion;
  auto* at = Reserve(buf, lcp + room) + lcp;
  const auto dec = enc == 0 ? 0 : fsst.Decode(in, enc, at, room);
  if (dec > room) [[unlikely]] {
    ThrowCorrupted("text", "entry is inconsistent");
  }
  return lcp + dec;
}

template<typename Impl>
uint64_t DecodeShared(const fsst::Decoder<Impl>& fsst, const char* in,
                      uint64_t enc, uint64_t lcp, uint64_t shared, char* out) {
  SDB_ASSERT(lcp < shared);
  const auto bytes = shared - lcp;
  const auto dec = fsst.DecodePrefix(in, enc, out + lcp, bytes);
  if (dec < bytes || dec - bytes > fsst::kPrefixSlack) [[unlikely]] {
    ThrowCorrupted("text", "front coding is inconsistent");
  }
  return lcp + dec;
}

template<typename Impl, typename Visit>
ChainEnd WalkChain(const fsst::Decoder<Impl>& fsst, const char* in,
                   PackedReader& lengths, PackedReader& lcps, ChainEnd from,
                   uint32_t end, uint64_t enc_end, char* out, uint64_t capacity,
                   Visit&& visit) {
  uint64_t off = from.written;
  uint64_t prev_len = from.prev_len;
  uint64_t prev_off = off - prev_len;
  uint64_t enc_off = from.enc_off;
  for (auto e = from.entry; e < end; ++e) {
    const uint64_t lcp = lcps.At(e);
    const uint64_t enc = lengths.At(e);
    if ((lcp | enc) == 0) {
      visit(e, std::string_view{out + off, 0});
      prev_off = off;
      prev_len = 0;
      continue;
    }
    if (!(lcp <= prev_len && enc_off + enc <= enc_end && off + lcp <= capacity))
      [[unlikely]] {
      ThrowCorrupted("text", "front coding is inconsistent");
    }
    const auto room = capacity - (off + lcp);
    size_t dec = 0;
    if (enc * fsst::kMaxExpansion + kCopyChunk <= room) {
      CopyChunks(out + off, out + prev_off, lcp);
      if (enc != 0) {
        dec = fsst.DecodePrefix(in + enc_off, enc, out + off + lcp, room);
      }
    } else {
      if (lcp != 0) {
        std::memcpy(out + off, out + prev_off, lcp);
      }
      if (enc != 0) {
        dec = fsst.Decode(in + enc_off, enc, out + off + lcp, room);
      }
    }
    if (dec > room) [[unlikely]] {
      ThrowCorrupted("text", "entry is inconsistent");
    }
    const std::string_view value{out + off, lcp + dec};
    visit(e, value);
    prev_off = off;
    prev_len = value.size();
    off += value.size();
    enc_off += enc;
  }
  return {
    .entry = end, .written = off, .enc_off = enc_off, .prev_len = prev_len};
}

template<typename Impl, typename Visit>
uint32_t WalkInPlace(const fsst::Decoder<Impl>& fsst, const char* in,
                     PackedReader& lengths, PackedReader& lcps, uint32_t first,
                     uint32_t end, uint64_t enc_off, uint64_t enc_end,
                     std::string& buf, Visit&& visit) {
  uint64_t len = 0;
  for (auto e = first; e < end; ++e) {
    const uint64_t lcp = e == first ? 0 : lcps.At(e);
    const uint64_t enc = lengths.At(e);
    if (!(lcp <= len && enc_off + enc <= enc_end)) [[unlikely]] {
      ThrowCorrupted("text", "front coding is inconsistent");
    }
    len = DecodeSuffix(fsst, in + enc_off, enc, lcp, buf);
    if (!visit(e, std::string_view{buf.data(), len})) {
      return e;
    }
    enc_off += enc;
  }
  return end;
}

struct ChainTail {
  const char* in;
  uint64_t enc;
  uint64_t lcp;
};

template<typename Impl>
ChainTail DecodeChainHeads(const fsst::Decoder<Impl>& fsst, const char* in,
                           PackedReader& lengths, PackedReader& lcps,
                           uint32_t restart, uint32_t entry, uint64_t enc_off,
                           uint64_t enc_end, std::string& buf,
                           uint64_t& walked) {
  const uint32_t n = entry - restart + 1;
  SDB_ASSERT(n <= kChainRestart);
  std::array<uint64_t, kChainRestart + 1> off;
  std::array<uint32_t, kChainRestart> lcp;
  std::array<uint32_t, kChainRestart> enc;
  lengths.Read(restart, n, enc.data());
  off[0] = enc_off;
  for (uint32_t i = 0; i < n; ++i) {
    off[i + 1] = off[i] + enc[i];
  }
  if (off[n] > enc_end) [[unlikely]] {
    ThrowCorrupted("text", "front coding is inconsistent");
  }
  ++walked;
  if (n == 1 || lcps.At(entry) == 0) {
    return {.in = in + off[n - 1], .enc = enc[n - 1], .lcp = 0};
  }
  lcps.Read(restart, n, lcp.data());
  lcp[0] = 0;
  std::array<uint32_t, kChainRestart> used;
  uint32_t count = 0;
  uint64_t need = std::numeric_limits<uint64_t>::max();
  for (uint32_t i = n; need != 0;) {
    SDB_ASSERT(i != 0);
    --i;
    if (lcp[i] < need) {
      used[count++] = i;
      need = lcp[i];
    }
  }
  uint64_t len = 0;
  while (count > 1) {
    const auto i = used[--count];
    const uint64_t shared = lcp[used[count - 1]];
    SDB_ASSERT(lcp[i] <= len);
    len = DecodeShared(fsst, in + off[i], off[i + 1] - off[i], lcp[i], shared,
                       Reserve(buf, shared + fsst::kPrefixSlack));
    ++walked;
  }
  SDB_ASSERT(lcp[n - 1] <= len);
  return {.in = in + off[n - 1], .enc = enc[n - 1], .lcp = lcp[n - 1]};
}

bool SharedTable(const Header& h, const ColumnSegment& segment) noexcept {
  return h.symtab_size == 0 && segment.shared_size != 0;
}

template<typename Impl>
struct Fetch;

template<typename Impl>
struct Scan {
  static constexpr ByteCodec kCodec = Impl::kCodec;
  using Fetch = fsst_front::Fetch<Impl>;

  template<typename Frames>
  Scan(const ColumnSegment& segment, const Frames& s)
    : lcps{s.base + s.header.off_lcps, s.header.lcp_width} {
    const auto table = SymbolTable(segment, s.header, s.base, probe);
    if (!table.empty() &&
        !decoder.Import(table, segment, SharedTable(s.header, segment)))
      [[unlikely]] {
      ThrowCorrupted("text", "symbol table is inconsistent");
    }
    groups.Reset(static_cast<uint32_t>(s.frames.size()), [&](uint32_t f) {
      return std::pair{s.frames[f].first_entry, s.FrameEnd(f)};
    });
    group_decoded.assign(groups.Count(), 0);
  }

  template<typename Frames>
  ChainEnd Advance(Frames& s, uint32_t f, uint32_t end, char* out) {
    const auto& m = s.frames[f];
    return WalkChain(decoder, s.Data(), s.length_reader, lcps,
                     prefixes.empty() ? ChainEnd{.entry = m.first_entry,
                                                 .written = 0,
                                                 .enc_off = m.comp_off,
                                                 .prev_len = 0}
                                      : prefixes[f],
                     end, static_cast<uint64_t>(m.comp_off) + m.comp_len, out,
                     m.raw_len,
                     [&](uint32_t e, std::string_view v) { s.Keep(e, v); });
  }

  template<typename Frames>
  void DecodeFrame(Frames& s, uint32_t f, char* out) {
    const auto& m = s.frames[f];
    const auto chain = Advance(s, f, s.FrameEnd(f), out);
    if (chain.written != m.raw_len ||
        chain.enc_off != static_cast<uint64_t>(m.comp_off) + m.comp_len)
      [[unlikely]] {
      ThrowCorrupted("text", "frame is inconsistent");
    }
  }

  template<typename Frames>
  void DecodeSpan(Frames& s, uint32_t f, uint32_t lo, uint32_t hi) {
    SDB_ASSERT(!s.decoded[f] && lo < hi);
    const auto prefix =
      prefixes.empty() ? s.frames[f].first_entry : prefixes[f].entry;
    if (lo > prefix) {
      DecodeDetached(s, f, lo, hi);
      return;
    }
    if (hi == s.FrameEnd(f)) {
      s.DecodeFrame(f);
      return;
    }
    if (prefixes.empty()) {
      prefixes.resize(s.frames.size());
      for (uint32_t i = 0; i < s.frames.size(); ++i) {
        prefixes[i] = {.entry = s.frames[i].first_entry,
                       .written = 0,
                       .enc_off = s.frames[i].comp_off,
                       .prev_len = 0};
      }
    }
    prefixes[f] = Advance(s, f, std::max(hi, prefix), s.FrameOut(f));
  }

  template<typename Frames>
  void DecodeDetached(Frames& s, uint32_t f, uint32_t lo, uint32_t hi) {
    const auto& m = s.frames[f];
    const auto g = (lo - m.first_entry) / kChainRestart;
    auto& arena = duckdb::StringVector::GetStringAllocator(s.dictionary->data);
    auto* out = duckdb::char_ptr_cast(arena.Allocate(m.raw_len));
    const auto chain = WalkChain(
      decoder, s.Data(), s.length_reader, lcps,
      {.entry = m.first_entry + g * kChainRestart,
       .written = 0,
       .enc_off = groups.Offset(s.length_reader, m, f, g),
       .prev_len = 0},
      hi, static_cast<uint64_t>(m.comp_off) + m.comp_len, out, m.raw_len,
      [&](uint32_t e, std::string_view v) { s.Keep(e, v); });
    arena.ShrinkHead(m.raw_len - chain.written);
    group_heap_bytes += chain.written;
    const auto done = hi == s.FrameEnd(f)
                        ? groups.Groups(f)
                        : (hi - m.first_entry) / kChainRestart;
    for (auto k = g; k < done; ++k) {
      group_decoded[groups.Base(f) + k] = 1;
    }
  }

  template<typename Frames>
  void EnsureEntry(Frames& s, uint32_t e, uint32_t f) {
    const auto g = (e - s.frames[f].first_entry) / kChainRestart;
    if (!group_decoded[groups.Base(f) + g]) {
      DecodeGroup(s, f, g);
    }
  }

  template<typename Frames>
  void EnsureRow(Frames& s, uint32_t e, uint32_t f) {
    const auto at = e - s.frames[f].first_entry;
    EnsureRows(s, f, at / kChainRestart, uint32_t{1} << at % kChainRestart);
  }

  template<typename Frames>
  void DecodeInto(Frames& s, duckdb::Vector& result,
                  duckdb::idx_t result_offset, uint32_t first, uint32_t end) {
    SDB_ASSERT(first < end);
    const auto head = s.FrameOf(first);
    auto last = head;
    uint64_t capacity = s.frames[head].raw_len;
    while (s.FrameEnd(last) < end) {
      ++last;
      capacity += s.frames[last].raw_len;
    }
    auto& arena = duckdb::StringVector::GetStringAllocator(result);
    auto* out = duckdb::char_ptr_cast(arena.Allocate(capacity));
    auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(
      result, end - first, result_offset);
    const auto visit = [&](uint32_t e, std::string_view v) IRS_FORCE_INLINE {
      if (e >= first) {
        writer.WriteStringRef(
          duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())});
      }
    };
    const auto g = (first - s.frames[head].first_entry) / kChainRestart;
    uint64_t written = 0;
    for (auto f = head; f <= last; ++f) {
      const auto& m = s.frames[f];
      const auto frame_end = s.FrameEnd(f);
      const auto from =
        f == head ? m.first_entry + g * kChainRestart : m.first_entry;
      const auto to = std::min(end, frame_end);
      const uint64_t enc_end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
      const auto chain = WalkChain(
        decoder, s.Data(), s.length_reader, lcps,
        {.entry = from,
         .written = written,
         .enc_off =
           f == head ? groups.Offset(s.length_reader, m, f, g) : m.comp_off,
         .prev_len = 0},
        to, enc_end, out, capacity, visit);
      if (from == m.first_entry && to == frame_end &&
          (chain.written - written != m.raw_len || chain.enc_off != enc_end))
        [[unlikely]] {
        ThrowCorrupted("text", "frame is inconsistent");
      }
      written = chain.written;
    }
    arena.ShrinkHead(capacity - written);
  }

  template<typename Frames, typename Entry>
  void Select(Frames& s, duckdb::Vector& result, duckdb::idx_t result_offset,
              duckdb::idx_t count, Entry entry_at, uint32_t window_first,
              uint32_t window_end) {
    auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count,
                                                               result_offset);
    auto& arena = duckdb::StringVector::GetStringAllocator(result);
    bool referenced = false;
    auto f = group_frame;
    for (duckdb::idx_t i = 0; i < count;) {
      const auto e = static_cast<uint32_t>(entry_at(i));
      SDB_ASSERT(f == kNoFrame || e >= s.frames[f].first_entry);
      if (f == kNoFrame || e >= s.FrameEnd(f)) {
        f = s.FrameOf(e);
        if (f + 1 < s.frames.size()) {
          const auto* next = s.Data() + s.frames[f + 1].comp_off;
          __builtin_prefetch(next);
          __builtin_prefetch(next + 64);
        }
      }
      const auto first = s.frames[f].first_entry;
      const auto end = s.FrameEnd(f);
      auto last = (e - first) / kChainRestart;
      uint64_t touched = 1;
      auto j = i + 1;
      for (; j < count; ++j) {
        const auto next = static_cast<uint32_t>(entry_at(j));
        SDB_ASSERT(next >= first);
        if (next >= end) {
          break;
        }
        const auto group = (next - first) / kChainRestart;
        touched += group != last;
        last = group;
      }
      if (!s.decoded[f]) {
        const auto lo = std::max(window_first, first);
        const auto hi = std::min(window_end, end);
        SDB_ASSERT(lo < hi);
        const uint64_t range =
          (hi - 1 - first) / kChainRestart - (lo - first) / kChainRestart + 1;
        const uint64_t entry_bytes = std::max<uint64_t>(
          s.frames[f].raw_len / (end - first), kChainMinRowBytes);
        const uint64_t total =
          (end - first + kChainRestart - 1) / kChainRestart;
        if ((touched * kChainRestart <= kSparseRows && range < total) ||
            (j - i) * entry_bytes + touched * kChainGroupCost <=
              range * kChainRestart * entry_bytes) {
          while (i < j) {
            const auto group =
              (static_cast<uint32_t>(entry_at(i)) - first) / kChainRestart;
            uint32_t rows = 0;
            for (; i < j; ++i) {
              const auto at = static_cast<uint32_t>(entry_at(i)) - first;
              if (at / kChainRestart != group) {
                break;
              }
              rows |= uint32_t{1} << at % kChainRestart;
            }
            DecodeRows(
              s, f, group, rows, 0, arena, [&](uint32_t, std::string_view v) {
                writer.WriteStringRef(
                  duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())});
              });
          }
          continue;
        }
        DecodeSpan(s, f, lo, hi);
      }
      referenced = true;
      for (; i < j; ++i) {
        writer.WriteStringRef(s.values[entry_at(i)]);
      }
    }
    group_frame = f;
    if (referenced) {
      s.ReferenceHeap(result);
    }
  }

  template<typename Frames>
  void EnsureRows(Frames& s, uint32_t f, uint32_t g, uint32_t rows) {
    const auto index = groups.Base(f) + g;
    if (group_decoded[index]) {
      return;
    }
    if (group_rows.empty()) {
      group_rows.assign(groups.Count(), 0);
    }
    const uint32_t kept = group_rows[index];
    rows &= ~kept;
    if (rows == 0) {
      return;
    }
    const auto restart = s.frames[f].first_entry + g * kChainRestart;
    DecodeRows(s, f, g, rows, kept,
               duckdb::StringVector::GetStringAllocator(s.dictionary->data),
               [&](uint32_t row, std::string_view v) {
                 s.Keep(restart + row, v);
                 group_heap_bytes += v.size();
               });
    group_rows[index] = static_cast<uint16_t>(kept | rows);
  }

  template<typename Frames, typename Visit>
  IRS_FORCE_INLINE void DecodeRows(Frames& s, uint32_t f, uint32_t g,
                                   uint32_t rows, uint32_t kept,
                                   duckdb::ArenaAllocator& arena,
                                   Visit&& visit) {
    const auto restart = s.frames[f].first_entry + g * kChainRestart;
    const uint32_t n = std::bit_width(rows);
    SDB_ASSERT(n != 0 && n <= kChainRestart && restart + n <= s.FrameEnd(f));
    std::array<uint64_t, kChainRestart + 1> off;
    std::array<uint32_t, kChainRestart> lcp;
    std::array<uint32_t, kChainRestart> enc;
    lcps.Read(restart, n, lcp.data());
    s.length_reader.Read(restart, n, enc.data());
    lcp[0] = 0;
    off[0] = groups.Offset(s.length_reader, s.frames[f], f, g);
    for (uint32_t i = 0; i < n; ++i) {
      off[i + 1] = off[i] + enc[i];
    }
    if (off[n] > s.header.data_size) [[unlikely]] {
      ThrowCorrupted("text", "front coding is inconsistent");
    }
    std::array<uint32_t, kChainRestart> shared;
    uint32_t whole = 0;
    uint32_t heads = 0;
    uint32_t sources = 0;
    uint64_t need = 0;
    uint64_t capacity = 0;
    for (uint32_t i = n; i-- > 0;) {
      if (need == 0) {
        const uint32_t below = rows & ((uint32_t{2} << i) - 1);
        if (below == 0) {
          break;
        }
        i = static_cast<uint32_t>(std::bit_width(below)) - 1;
      }
      const uint32_t row = rows >> i & 1;
      const uint32_t wanted = row | uint32_t{lcp[i] < need};
      const uint32_t source = wanted & (kept >> i);
      const uint32_t decode = wanted ^ source;
      sources |= source << i;
      whole |= (decode & row) << i;
      heads |= (decode & (row ^ 1)) << i;
      shared[i] = static_cast<uint32_t>(need);
      need = wanted != 0 ? (source != 0 ? 0 : lcp[i]) : need;
      capacity +=
        (decode & row) * (lcp[i] + uint64_t{enc[i]} * fsst::kMaxExpansion);
    }
    auto* out = duckdb::char_ptr_cast(arena.Allocate(capacity + kCopyChunk));
    uint64_t written = 0;
    const char* prev = nullptr;
    uint64_t prev_len = 0;
    bool chained = false;
    bool padded = false;
    const auto copy = [&](char* to, uint64_t bytes) IRS_FORCE_INLINE {
      if (padded) {
        CopyChunks(to, prev, bytes);
      } else {
        std::memcpy(to, prev, bytes);
      }
    };
    for (auto left = whole | heads | sources; left != 0; left &= left - 1) {
      const auto i = static_cast<uint32_t>(std::countr_zero(left));
      if ((sources >> i & 1) != 0) {
        const auto value = View(s.values[restart + i]);
        prev = value.data();
        prev_len = value.size();
        chained = false;
        padded = false;
        continue;
      }
      if (lcp[i] > prev_len) [[unlikely]] {
        ThrowCorrupted("text", "front coding is inconsistent");
      }
      if ((heads >> i & 1) != 0) {
        auto* head = Reserve(chain, shared[i] + fsst::kPrefixSlack);
        if (!chained && lcp[i] != 0) {
          copy(head, lcp[i]);
        }
        prev_len = DecodeShared(decoder, s.Data() + off[i], enc[i], lcp[i],
                                shared[i], head);
        prev = head;
        chained = true;
        padded = true;
        continue;
      }
      chained = false;
      auto* at = out + written;
      if (lcp[i] != 0) {
        copy(at, lcp[i]);
      }
      const auto room = capacity - (written + lcp[i]);
      const auto dec = enc[i] == 0 ? 0
                                   : decoder.Decode(s.Data() + off[i], enc[i],
                                                    at + lcp[i], room);
      if (dec > room) [[unlikely]] {
        ThrowCorrupted("text", "entry is inconsistent");
      }
      prev = at;
      prev_len = lcp[i] + dec;
      padded = true;
      written += prev_len;
      visit(i, std::string_view{at, prev_len});
    }
    arena.ShrinkHead(capacity + kCopyChunk - written);
  }

  template<typename Frames>
  void DecodeGroup(Frames& s, uint32_t f, uint32_t g) {
    const auto first = s.frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, s.FrameEnd(f));
    const auto capacity = ChainCapacity(s.length_reader, lcps, first, end);
    auto& arena = duckdb::StringVector::GetStringAllocator(s.dictionary->data);
    auto* out = duckdb::char_ptr_cast(arena.Allocate(capacity));
    const auto chain =
      WalkChain(decoder, s.Data(), s.length_reader, lcps,
                {.entry = first,
                 .written = 0,
                 .enc_off = groups.Offset(s.length_reader, s.frames[f], f, g),
                 .prev_len = 0},
                end, s.header.data_size, out, capacity,
                [&](uint32_t e, std::string_view v) { s.Keep(e, v); });
    arena.ShrinkHead(capacity - chain.written);
    group_heap_bytes += chain.written;
    group_decoded[groups.Base(f) + g] = 1;
  }

  template<typename Frames>
  std::string_view DecodeHead(Frames& s, uint32_t f, uint32_t g) {
    const auto e = s.frames[f].first_entry + g * kChainRestart;
    if (s.decoded[f] || group_decoded[groups.Base(f) + g]) {
      return View(s.values[e]);
    }
    std::string_view head;
    WalkInPlace(decoder, s.Data(), s.length_reader, lcps, e, e + 1,
                groups.Offset(s.length_reader, s.frames[f], f, g),
                s.header.data_size, probe, [&](uint32_t, std::string_view v) {
                  head = v;
                  return true;
                });
    return head;
  }

  template<typename Frames, typename Before>
  uint32_t Bound(Frames& s, Before before) {
    SDB_ASSERT(!s.frames.empty());
    const auto lo = PartitionIndex(0, s.frames.size(), [&](size_t f) {
      return before(DecodeHead(s, static_cast<uint32_t>(f), 0));
    });
    if (lo == 0) {
      return s.frames[0].first_entry;
    }
    const auto f = static_cast<uint32_t>(lo - 1);
    const auto glo = PartitionIndex(1, groups.Groups(f), [&](size_t i) {
      return before(DecodeHead(s, f, static_cast<uint32_t>(i)));
    });
    const auto g = static_cast<uint32_t>(glo - 1);
    const auto first = s.frames[f].first_entry + g * kChainRestart;
    const auto end = std::min<uint32_t>(first + kChainRestart, s.FrameEnd(f));
    if (s.decoded[f] || group_decoded[groups.Base(f) + g]) {
      for (auto e = first; e < end; ++e) {
        if (!before(View(s.values[e]))) {
          return e;
        }
      }
      return end;
    }
    return WalkInPlace(decoder, s.Data(), s.length_reader, lcps, first, end,
                       groups.Offset(s.length_reader, s.frames[f], f, g),
                       s.header.data_size, probe,
                       [&](uint32_t, std::string_view v) { return before(v); });
  }

  uint64_t HeapBytes() const noexcept { return group_heap_bytes; }

  fsst::Decoder<Impl> decoder;
  PackedReader lcps{nullptr, 0};
  ChainGroups groups;
  std::vector<uint8_t> group_decoded;
  std::vector<uint16_t> group_rows;
  std::vector<ChainEnd> prefixes;
  static_assert(kChainRestart <= 8 * sizeof(uint16_t));
  uint64_t group_heap_bytes = 0;
  uint32_t group_frame = kNoFrame;
  std::string probe;
  std::string chain;
};

template<typename Impl>
struct Fetch {
  void Reset(const Header& h, duckdb::data_ptr_t base) {
    decoder.reset();
    groups.Clear();
    lcps = PackedReader{base + h.off_lcps, h.lcp_width};
    offsets = PackedReader{base + h.off_lengths, h.length_width};
    walked = 0;
  }

  uint64_t Walked() const noexcept { return walked; }

  uint64_t DecodedBytes() const noexcept { return 0; }

  void Emit(const ColumnSegment& segment, const Header& h,
            duckdb::data_ptr_t base, PackedReader& lengths, uint32_t lo,
            const FrameMeta& f, uint32_t entry, duckdb::Vector& result,
            duckdb::idx_t result_idx, bool) {
    if (groups.Empty()) {
      Open(segment, h, base);
    }
    const auto* in = reinterpret_cast<const char*>(base + h.off_data);
    const uint32_t group = (entry - f.first_entry) / kChainRestart;
    const uint32_t restart = f.first_entry + group * kChainRestart;
    const auto tail = DecodeChainHeads(
      *decoder, in, lengths, lcps, restart, entry,
      groups.Offset(offsets, f, lo, group), h.data_size, chain, walked);
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
    if (tail.lcp + tail.enc == 0) {
      out[result_idx] = duckdb::string_t{};
      return;
    }
    auto& heap = duckdb::StringVector::GetStringAllocator(result);
    const auto room = tail.enc * fsst::kMaxExpansion;
    auto* at = Reserve(heap, tail.lcp + room);
    if (tail.lcp != 0) {
      std::memcpy(at, chain.data(), tail.lcp);
    }
    const auto dec =
      tail.enc == 0 ? 0
                    : decoder->Decode(tail.in, tail.enc, at + tail.lcp, room);
    if (dec > room) [[unlikely]] {
      ThrowCorrupted("text", "entry is inconsistent");
    }
    const duckdb::string_t value{at, static_cast<uint32_t>(tail.lcp + dec)};
    heap.ShrinkHead(tail.lcp + room -
                    (value.IsInlined() ? 0 : value.GetSize()));
    out[result_idx] = value;
  }

  void Open(const ColumnSegment& segment, const Header& h,
            duckdb::const_data_ptr_t base) {
    decoder.emplace();
    const auto symbols = SymbolTable(segment, h, base, chain);
    if (!symbols.empty() &&
        !decoder->Import(symbols, segment, SharedTable(h, segment)))
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

  std::optional<fsst::Decoder<Impl>> decoder;
  PackedReader lcps{nullptr, 0};
  PackedReader offsets{nullptr, 0};
  ChainGroups groups;
  std::string chain;
  uint64_t walked = 0;
};

template<typename Impl, typename Rows>
class CompressionState final : public SortedCompressionState<Rows> {
  using Base = SortedCompressionState<Rows>;

 public:
  static constexpr ByteCodec kCodec = Impl::kCodec;

  CompressionState(ColumnDataCheckpointData& data, DedupScratch& dedup)
    : Base{kCodec, data.type, dedup, data.tuning.strings,
           data.params.segment_target},
      _codec{data.Get<fsst::SharedEncoder<Impl>>().encoder} {}

  void FinalizeCompress();

 private:
  using Base::_frames;
  using Base::_out;
  using Base::_rows;
  using Base::_target;

  fsst::Encoder<Impl>& _codec;
  std::vector<uint32_t> _lcp_stream;
};

template<typename Impl, typename Rows>
void CompressionState<Impl, Rows>::FinalizeCompress() {
  this->Sort();
  const auto lengths = _rows.Lengths();
  const auto rows = _rows.View();
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  auto& data = _out->bytes;
  _lcp_stream.assign(first_entry, 0);
  _frames.clear();
  uint32_t max_lcp = 0;
  uint64_t frame_raw = 0;
  uint64_t frame_first = 0;
  uint64_t suffixes = 0;
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];
    const auto size = entry.GetSize();
    if (frame_raw != 0 && frame_raw + size > kFsstFrameRawBytes) {
      _frames.emplace_back(static_cast<uint32_t>(frame_first),
                           static_cast<uint32_t>(frame_raw), 0, 0);
      frame_raw = 0;
      frame_first = i;
    }
    const uint32_t lcp = (i - frame_first) % kChainRestart == 0
                           ? 0
                           : CommonPrefix(entries[i - 1], entry);
    _lcp_stream.emplace_back(lcp);
    max_lcp = std::max(max_lcp, lcp);
    frame_raw += size;
    suffixes += size - lcp;
  }
  if (!entries.empty()) {
    _frames.emplace_back(static_cast<uint32_t>(frame_first),
                         static_cast<uint32_t>(frame_raw), 0, 0);
  }
  const auto skip = std::span{_lcp_stream}.subspan(first_entry);
  size_t done = 0;
  const auto encode = [&](uint64_t capacity) {
    auto* out = data.Extend(capacity);
    const auto encoded =
      _codec.Encode(entries.subspan(done), skip.subspan(done), out, capacity,
                    lengths.subspan(done));
    data.Shrink(capacity - encoded.bytes);
    for (auto i = done; i < done + encoded.strings; ++i) {
      suffixes -= entries[i].GetSize() - skip[i];
    }
    done += encoded.strings;
  };
  encode(suffixes + fsst::kEncodeSlack);
  if (done != entries.size()) {
    encode(fsst::kEncodeExpansion * suffixes + fsst::kEncodeSlack);
    SDB_ENSURE(done == entries.size(), "fsst: compression ran out of space");
  }
  size_t off = 0;
  uint32_t max_enc = 0;
  for (size_t f = 0; f < _frames.size(); ++f) {
    auto& frame = _frames[f];
    const size_t end =
      f + 1 < _frames.size() ? _frames[f + 1].first_entry : entries.size();
    frame.comp_off = static_cast<uint32_t>(off);
    for (size_t e = frame.first_entry; e < end; ++e) {
      off += lengths[e];
      max_enc = std::max(max_enc, lengths[e]);
    }
    frame.comp_len = static_cast<uint32_t>(off - frame.comp_off);
    frame.first_entry += first_entry;
  }
  _lcp_stream.resize(
    duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount(rows)),
    0);
  const auto table = _codec.SymbolTable();
  const auto symtab = Impl::kSharedTable ? std::string_view{} : table;
  RecordRatio(this->History(), rows.raw, data.Size() + symtab.size(), _target);
  _rows.Write(
    {
      .codec = kCodec,
      .frames = _frames,
      .max_length = max_enc,
      .lcps = _lcp_stream,
      .max_lcp = max_lcp,
      .symtab = symtab,
      .shared = Impl::kSharedTable ? table : std::string_view{},
    },
    *_out);
}

}  // namespace
}  // namespace irs::codecs::text::fsst_front
namespace irs::codecs {

/*
Data layout per PlainScan segment, one block of one column; a text column's
row group is cut into segments of about kDefaultColSegmentTarget encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header (string_layout.hpp), 72 bytes       |   |
|   +----------------------------------------------+   |
|   codec of the row codes, level, bit widths,         |
|   counts, section offsets, flags, decoded size       |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each row's FSST-encoded suffix,          |
|   0 for a NULL row                                   |
+------------------------------------------------------+
|                   Shared prefixes                    |
|   +----------------------------------------------+   |
|   |   bitpacked lcp[entry_count]                 |   |
|   +----------------------------------------------+   |
|   bytes shared with the previous entry; 0 on a       |
|   frame's first entry and every 16th after it        |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table with 8-bit codes, about  |   |
|   |   2 KiB at most                              |   |
|   +----------------------------------------------+   |
|   trained once per row group; each segment stores    |
|   a copy; a table with 12-bit codes, up to 36 KiB,   |
|   is stored once for the row group, outside its      |
|   segments: the block's metadata says where, and     |
|   this section is empty                              |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   FSST-encoded suffixes                      |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries; a       |
|   longer entry is a frame of its own                 |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

namespace {

template<typename Impl>
using PlainFsst = text::fsst_front::CompressionState<Impl, text::PlainRows>;

template<typename Impl>
using DictFsst = text::fsst_front::CompressionState<Impl, text::DictionaryRows>;

}  // namespace

constinit const Codec kFsstFrontCodec =
  text::StringCodec<text::PlainScan<text::fsst_front::Scan<text::fsst::Fsst8>>,
                    PlainFsst<text::fsst::Fsst8>>();

constinit const Codec kFsst12FrontCodec =
  text::StringCodec<text::PlainScan<text::fsst_front::Scan<text::fsst::Fsst12>>,
                    PlainFsst<text::fsst::Fsst12>>();

/*
Data layout per DictionaryScan segment, one block of one column; a text
column's row group is cut into segments of about kDefaultColSegmentTarget
encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header (string_layout.hpp), 72 bytes       |   |
|   +----------------------------------------------+   |
|   codec of the row codes, level, bit widths,         |
|   counts, section offsets, flags, decoded size       |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked encoded length[entry_count]      |   |
|   +----------------------------------------------+   |
|   length of each entry's FSST-encoded suffix;        |
|   entry 0 is NULL, then the distinct values in       |
|   sorted order                                       |
+------------------------------------------------------+
|                   Shared prefixes                    |
|   +----------------------------------------------+   |
|   |   bitpacked lcp[entry_count]                 |   |
|   +----------------------------------------------+   |
|   bytes shared with the previous entry; 0 on a       |
|   frame's first entry and every 16th after it        |
+------------------------------------------------------+
|                      Row codes                       |
|   +----------------------------------------------+   |
|   |   numeric codec segment of code[row_count]   |   |
|   +----------------------------------------------+   |
|   row -> entry, code 0 is NULL                       |
+------------------------------------------------------+
|                     Symbol table                     |
|   +----------------------------------------------+   |
|   |   FSST symbol table with 8-bit codes, about  |   |
|   |   2 KiB at most                              |   |
|   +----------------------------------------------+   |
|   trained once per row group; each segment stores    |
|   a copy; a table with 12-bit codes, up to 36 KiB,   |
|   is stored once for the row group, outside its      |
|   segments: the block's metadata says where, and     |
|   this section is empty                              |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   FSST-encoded suffixes                      |   |
|   +----------------------------------------------+   |
|   frames of up to 16 KiB of decoded entries; a       |
|   longer entry is a frame of its own                 |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

constinit const Codec kDictFsstFrontCodec = text::StringCodec<
  text::DictionaryScan<text::fsst_front::Scan<text::fsst::Fsst8>>,
  DictFsst<text::fsst::Fsst8>>();

constinit const Codec kDictFsst12FrontCodec = text::StringCodec<
  text::DictionaryScan<text::fsst_front::Scan<text::fsst::Fsst12>>,
  DictFsst<text::fsst::Fsst12>>();

}  // namespace irs::codecs
