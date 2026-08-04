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

#include "iresearch/formats/index/term_dict.hpp"

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>
#include <fsst.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstring>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/mutex.hpp>
#include <limits>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/bitset.hpp"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "basics/string_utils.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

using namespace irs;
using irs::term_dict::LayoutKind;

constexpr uint8_t kFlagFsst = 1;

constexpr uint32_t kMaxBlockEntries = 1024;
constexpr uint32_t kMaxRestarts = 4096;
// A block's restart count is its entry count divided by the restart interval,
// so the entry cap alone keeps the restart table within its u16 index even at
// the smallest interval.
static_assert(kMaxBlockEntries <= kMaxRestarts);
// Restart offsets are u16, so each area of a block holding more than one
// restart point must fit 64 KiB. A block that holds exactly one -- which is
// what an entry larger than the byte target gets -- has both offsets at zero
// and is therefore unbounded.
constexpr uint32_t kMaxAreaSize = std::numeric_limits<uint16_t>::max();
constexpr uint32_t kRestartEntrySize = 2 * sizeof(uint16_t);
constexpr size_t kFsstSampleBytes = 256 * 1024;
constexpr size_t kFsstMinSamples = 64;
constexpr uint32_t kNoEntry = std::numeric_limits<uint32_t>::max();
// Byte budget a fixed-stride block may exceed the front-coded encoding of the
// same keys by, as a divisor of the front-coded size. The layout removes a
// varint decode and a per-entry copy from every read, which is worth a little;
// the shapes it must not take -- the ones front coding and FSST genuinely
// compress -- lose by 28-55%, and the shapes it must take lose by at most 1%,
// so the threshold sits in a ten-fold gap rather than on a boundary.
constexpr size_t kFixedStrideSlackDiv = 32;
// Layout of a field body, read once per field at prepare.
struct FieldHeader {
  LayoutKind layout{LayoutKind::Var};
  uint8_t flags{};
  // Runs of df <= k_field are inlined in the term's record. Written as 1:
  // a df==1 run is the strictly-better case (zero `.doc` bytes, freq derived
  // from tf). Raising it is a writer default change, not a format break.
  uint8_t k_field{1};
  uint32_t restart_interval{};
  uint32_t row_group_size{};
  uint32_t segment_docs{};
  uint32_t block_count{};
  uint64_t separators_offset{};
  // Where the field's postings start in `.doc`. Restart entries spell their
  // `doc_start` against it, so what they cost follows the field's own size
  // rather than its position in the plane.
  uint64_t doc_base{};
  // Payload fields only: where the field's code stream starts in `.pay`. Terms
  // anchor on lanes of that stream, and this is what places lane 0.
  uint64_t pay_base{};
  bstring min_term;
  bstring max_term;
  bstring fsst_table;
  // `Single` only: the one term's records, exactly as a block payload entry
  // spells them. This is the whole dictionary of such a field.
  bstring payload;

  // Row groups the segment is cut into. Uniform row groups make the grid
  // derivable, so nothing is stored per row group.
  uint32_t RowGroupCount() const noexcept {
    const uint64_t rgs = row_group_size;
    return rgs == 0 ? 1
                    : static_cast<uint32_t>(
                        std::max<uint64_t>(1, (segment_docs + rgs - 1) / rgs));
  }

  // A field of one row group spends nothing on saying so: every term holds
  // exactly one posting list and it sits at row group 0.
  bool Partitioned() const noexcept { return segment_docs > row_group_size; }
};

void WriteFieldHeader(IndexOutput& out, const FieldHeader& header,
                      IndexFeatures features) {
  out.WriteByte(static_cast<byte_type>(header.layout));
  out.WriteByte(header.flags);
  out.WriteByte(header.k_field);
  out.WriteV32(header.restart_interval);
  out.WriteV32(header.row_group_size);
  out.WriteV32(header.segment_docs);
  out.WriteV32(header.block_count);
  out.WriteV64(header.separators_offset);
  out.WriteV64(header.doc_base);
  if (IndexFeatures::None != (features & IndexFeatures::Pay)) {
    out.WriteV64(header.pay_base);
  }
  WriteStr(out, header.min_term);
  WriteStr(out, header.max_term);
  if (header.layout == LayoutKind::Single) {
    WriteStr(out, header.payload);
    return;
  }
  WriteStr(out, header.fsst_table);
}

FieldHeader ReadFieldHeader(DataInput& in, IndexFeatures features) {
  FieldHeader header;
  header.layout = static_cast<LayoutKind>(in.ReadByte());
  header.flags = in.ReadByte();
  header.k_field = in.ReadByte();
  header.restart_interval = in.ReadV32();
  header.row_group_size = in.ReadV32();
  header.segment_docs = in.ReadV32();
  header.block_count = in.ReadV32();
  header.separators_offset = in.ReadV64();
  header.doc_base = in.ReadV64();
  if (IndexFeatures::None != (features & IndexFeatures::Pay)) {
    header.pay_base = in.ReadV64();
  }
  header.min_term = ReadString<bstring>(in);
  header.max_term = ReadString<bstring>(in);
  if (header.layout == LayoutKind::Single) {
    header.payload = ReadString<bstring>(in);
    return header;
  }
  header.fsst_table = ReadString<bstring>(in);
  return header;
}

RowGroupLayout HeaderLayout(const FieldHeader& header) noexcept {
  return {.count = header.RowGroupCount(),
          .rows_per_group = header.row_group_size,
          .segment_docs = header.segment_docs};
}

// String-area entry header: `lcp` and the stored suffix length share one
// varint whenever the length fits 5 bits, which is the common case for short
// terms (byte ngrams) where a second varint would dominate the entry.
constexpr uint32_t kInlineLenMax = 31;

void WriteEntryHeader(BufferedOutput& out, uint32_t lcp, uint32_t len) {
  const uint32_t inline_len = std::min(len, kInlineLenMax);
  out.WriteV32((lcp << 5) | inline_len);
  if (inline_len == kInlineLenMax) {
    out.WriteV32(len - kInlineLenMax);
  }
}

// `vread`'s continuation is out of line, and an entry header is two bytes for
// any field whose neighbouring keys share more than three leading bytes -- i.e.
// nearly every field, so the iteration path would pay that call per entry.
IRS_FORCE_INLINE uint32_t ReadV32Short(const byte_type*& in) noexcept {
  const uint32_t b0 = *in++;
  if (b0 < 0x80) [[likely]] {
    return b0;
  }
  const uint32_t b1 = *in++;
  if (b1 < 0x80) [[likely]] {
    return (b0 & 0x7F) | (b1 << 7);
  }
  in -= 2;
  return vread<uint32_t>(in);
}

// The single-run head packs docs count, row group and the tfΔ flag into one
// varint, which can carry past 32 bits on a wide grid.
IRS_FORCE_INLINE uint64_t ReadV64Short(const byte_type*& in) noexcept {
  const uint64_t b0 = *in++;
  if (b0 < 0x80) [[likely]] {
    return b0;
  }
  const uint64_t b1 = *in++;
  if (b1 < 0x80) [[likely]] {
    return (b0 & 0x7F) | (b1 << 7);
  }
  in -= 2;
  return vread<uint64_t>(in);
}

uint32_t V32Size(uint32_t value) noexcept {
  uint32_t size = 1;
  for (; value >= 0x80; value >>= 7) {
    ++size;
  }
  return size;
}

// What `WriteEntryHeader` will spend, so a block can be costed before it is
// written.
uint32_t EntryHeaderSize(uint32_t lcp, uint32_t len) noexcept {
  const uint32_t inline_len = std::min(len, kInlineLenMax);
  const uint32_t size = V32Size((lcp << 5) | inline_len);
  return inline_len == kInlineLenMax ? size + V32Size(len - kInlineLenMax)
                                     : size;
}

void ReadEntryHeader(const byte_type*& in, uint32_t& lcp,
                     uint32_t& len) noexcept {
  const uint32_t header = ReadV32Short(in);
  lcp = header >> 5;
  len = header & kInlineLenMax;
  if (len == kInlineLenMax) {
    len += ReadV32Short(in);
  }
}

// Derived stream state at one entry of a restart group. Restart entries carry
// the only absolutes; between them everything advances by the sums the records
// spell: `doc_start` by run lengths, `pos_ptr`/`pay_ptr` by per-run stream
// advances, `pos_anchor` by the term's tf modulo the position block -- the
// slot stream is contiguous across a field's terms by construction.
struct EntryChain {
  uint64_t doc_start{};
  uint64_t pos_ptr{};
  uint64_t pay_ptr{};
  uint32_t pos_anchor{};

  void clear() noexcept {
    doc_start = pos_ptr = pay_ptr = 0;
    pos_anchor = 0;
  }
};

uint32_t BitWidth(uint32_t value) noexcept {
  return value == 0 ? 0 : 32 - static_cast<uint32_t>(std::countl_zero(value));
}

// Writer half of the record bitstream: values of stated widths, least
// significant bit first, packed back to back across columns -- one shared
// stream, so no column pays byte alignment of its own. An accumulator takes a
// whole value at a time and spills whole bytes; `width <= kMaxPackWidth` and
// fewer than 8 bits are ever held back, so it never carries more than 38.
class BitPacker {
 public:
  explicit BitPacker(BufferedOutput& out) noexcept : _out{out} {}

  void Push(uint32_t value, uint32_t width) {
    _acc |= uint64_t{value} << _bits;
    _bits += width;
    while (_bits >= 8) {
      _out.WriteByte(static_cast<byte_type>(_acc));
      _acc >>= 8;
      _bits -= 8;
    }
  }

  void Finish() {
    if (_bits != 0) {
      _out.WriteByte(static_cast<byte_type>(_acc));
      _acc = 0;
      _bits = 0;
    }
  }

 private:
  BufferedOutput& _out;
  uint64_t _acc{0};
  uint32_t _bits{0};
};

// Sequential reader of one column of the record bitstream, positioned by bit:
// a column's start is the sum of the widths before it, so skipping a column a
// mode does not need costs nothing. Refills a byte at a time, so it never
// reads past the record's own bytes -- which a wide load off a mapped block
// could.
class BitReader {
 public:
  BitReader(const byte_type* data, uint64_t bit) noexcept
    : _p{data + (bit >> 3)} {
    const uint32_t rem = bit & 7;
    if (rem != 0) {
      _acc = uint64_t{*_p++} >> rem;
      _bits = 8 - rem;
    }
  }

  uint32_t Read(uint32_t width) noexcept {
    while (_bits < width) {
      _acc |= uint64_t{*_p++} << _bits;
      _bits += 8;
    }
    const auto value =
      static_cast<uint32_t>(_acc & ((uint64_t{1} << width) - 1));
    _acc >>= width;
    _bits -= width;
    return value;
  }

 private:
  const byte_type* _p;
  uint64_t _acc{0};
  uint32_t _bits{0};
};

constexpr uint32_t kMaxPackWidth = 31;

// Column layout of a multi-run record: which width-packed per-run columns
// exist is a pure function of the field's features, in this order.
//
// Both `.pay` planes anchor a term, and only one of them can be on: offset
// blocks are variable-byte and lockstep with positions, so their runs need a
// stored byte advance each; per-document payloads are lanes of one stream, so
// the term's anchor plus the df column places every run of it.
struct RecordColumns {
  bool freq = false;
  bool pos = false;
  bool offs = false;
  bool pay = false;
  // Bytes the width table of a multi-run record takes, 5-bit and nibble form
  // -- field constants, so they are settled here rather than recomputed from
  // the flags at each record.
  // rgΔ | df | tfΔ | run_len | pos_ptrΔ | pay_ptrΔ
  uint32_t width_bytes = 0;
  uint32_t width_bytes_nib = 0;
  // Grid constants of a partitioned field: a single-run head spells its row
  // group in exactly the bits the grid needs, and a df == 1 run's inline id
  // is a row-group-local ordinal, so its width is the grid's too.
  uint32_t rg_bits = 0;
  uint32_t rg_mask = 0;
  uint32_t id_bits = 0;

  RecordColumns() = default;

  RecordColumns(IndexFeatures features, uint32_t rg_count,
                uint32_t row_group_size) noexcept
    : freq{IndexFeatures::None != (features & IndexFeatures::Freq)},
      pos{IndexFeatures::None != (features & IndexFeatures::Pos)},
      offs{IndexFeatures::None != (features & IndexFeatures::Offs)},
      pay{IndexFeatures::None != (features & IndexFeatures::Pay)},
      width_bytes{((3U + freq + pos + offs) * 5 + 7) / 8},
      width_bytes_nib{((3U + freq + pos + offs) * 4 + 7) / 8},
      rg_bits{BitWidth(rg_count - 1)},
      rg_mask{(1U << rg_bits) - 1U},
      id_bits{BitWidth(row_group_size - 1)} {}

  bool Anchored() const noexcept { return offs || pay; }
};

// Restart entries carry the only absolutes; everything between them derives.
// `.doc` positions are stored against the field's own region rather than the
// whole plane: a field of `df == 1` terms consumes no `.doc` bytes at all, so
// against its own base every one of its restart entries spells a zero, where
// against the plane each spells the varint of wherever the field happens to sit
// in it.
void WriteAbsolutes(BufferedOutput& out, const RecordColumns& cols,
                    uint64_t doc_base, uint64_t doc_start, uint64_t pos_ptr,
                    uint32_t pos_anchor, uint64_t pay_ptr) {
  SDB_ASSERT(doc_start >= doc_base);
  out.WriteV64(doc_start - doc_base);
  if (cols.pos) {
    out.WriteV64(pos_ptr);
    SDB_ASSERT(pos_anchor < pos_limits::kBlockSize);
    out.WriteByte(static_cast<byte_type>(pos_anchor));
  }
  if (cols.Anchored()) {
    out.WriteV64(pay_ptr);
  }
}

void ReadAbsolutes(const byte_type*& p, const RecordColumns& cols,
                   uint64_t doc_base, EntryChain& chain) {
  chain.doc_start = doc_base + vread<uint64_t>(p);
  if (cols.pos) {
    chain.pos_ptr = vread<uint64_t>(p);
    chain.pos_anchor = *p++;
  }
  if (cols.Anchored()) {
    chain.pay_ptr = vread<uint64_t>(p);
  }
}

// One self-contained record per term: the single-run form (term stats and the
// run's extents; on a partitioned field the head varint fuses the docs count
// with the run's row group at the grid's own bit width and, under freq, a
// tfΔ-present flag -- tf == df is the overwhelming case, so the zero is a
// flag rather than a byte) or the multi-run form -- a width table, one
// contiguous bitstream of per-run columns with the df == 1 ids trailing it at
// the grid's id width, then the sparse `e_skip` varints. A multi-run record
// stores no term totals: the df and tfΔ columns sum to them, and every walk
// mode that wants them decodes those columns anyway. A record never stores a
// file position: restart absolutes plus the sums of its own columns place
// every run in every stream. The score bounds a scored walk reads sit at the
// head of every run in `.doc`; a term-level roll-up of them has no place in
// an API that is per (term, row group), so the record carries none.
void EncodeRecord(BufferedOutput& out, const RecordColumns& cols,
                  bool partitioned, const TermRuns& term) {
  const auto& runs = term.runs;
  SDB_ASSERT(!runs.empty());
  const auto rg_count = static_cast<uint32_t>(runs.size());

  if (rg_count == 1) {
    const bool tfd = cols.freq && [&] {
      SDB_ASSERT(term.freq >= term.docs_count);
      return term.freq != term.docs_count;
    }();
    if (partitioned) {
      SDB_ASSERT(runs[0].rg <= cols.rg_mask);
      const uint32_t shift = 1 + cols.rg_bits + cols.freq;
      out.WriteV64((uint64_t{term.docs_count} << shift) |
                   (uint64_t{tfd} << (1 + cols.rg_bits)) |
                   (uint64_t{runs[0].rg} << 1));
      if (tfd) {
        out.WriteV32(term.freq - term.docs_count);
      }
    } else {
      SDB_ASSERT(runs[0].rg == 0);
      out.WriteV32(term.docs_count);
      if (cols.freq) {
        out.WriteV32(term.freq - term.docs_count);
      }
    }
    const auto& run = runs[0];
    if (run.df == 1) {
      out.WriteV32(run.single_doc);
    } else {
      out.WriteV64(run.run_len);
      if (run.df > doc_limits::kBlockSize) {
        SDB_ENSURE(run.e_skip <= std::numeric_limits<uint32_t>::max(),
                   "term_dict: a run's skip data starts past 4 GiB of `.doc`");
        out.WriteV64(run.e_skip);
      }
    }
    if (cols.pos) {
      out.WriteV64(run.pos_delta);
    }
    if (cols.offs) {
      out.WriteV64(run.pay_delta);
    }
    return;
  }
  SDB_ASSERT(partitioned);

  uint32_t max_gap = runs[0].rg;
  uint32_t max_df = 0;
  uint32_t max_tf = 0;
  uint64_t max_len = 0;
  uint64_t max_pos = 0;
  uint64_t max_pay = 0;
  uint64_t span_len = 0;
  uint64_t span_pos = 0;
  uint64_t span_pay = 0;
  for (size_t i = 0; i != runs.size(); ++i) {
    const auto& run = runs[i];
    if (i != 0) {
      SDB_ASSERT(run.rg > runs[i - 1].rg);
      max_gap = std::max(max_gap, run.rg - runs[i - 1].rg - 1);
    }
    max_df = std::max(max_df, run.df);
    if (cols.freq) {
      SDB_ASSERT(run.tf >= run.df);
      max_tf = std::max(max_tf, run.tf - run.df);
    }
    max_len = std::max(max_len, run.run_len);
    max_pos = std::max(max_pos, run.pos_delta);
    span_len += run.run_len;
    span_pos += run.pos_delta;
    if (cols.offs) {
      max_pay = std::max(max_pay, run.pay_delta);
      span_pay += run.pay_delta;
    }
  }
  // A run is placed by how far it sits from the term's anchor in each stream,
  // and the decoded record spends 32 bits on each of those distances, so a
  // term whose runs span more than that has no representation.
  SDB_ENSURE(std::max({span_len, span_pos, span_pay}) <=
               std::numeric_limits<uint32_t>::max(),
             "term_dict: a term spans more than 4 GiB of one stream");
  const uint32_t w_gap = BitWidth(max_gap);
  const uint32_t w_df = BitWidth(max_df);
  const uint32_t w_tf = BitWidth(max_tf);
  SDB_ENSURE(max_len >> kMaxPackWidth == 0 && max_pos >> kMaxPackWidth == 0 &&
               max_pay >> kMaxPackWidth == 0,
             "term_dict: a run column value exceeds ", kMaxPackWidth, " bits");
  const uint32_t w_len = BitWidth(static_cast<uint32_t>(max_len));
  const uint32_t w_pos = BitWidth(static_cast<uint32_t>(max_pos));
  const uint32_t w_pay = BitWidth(static_cast<uint32_t>(max_pay));
  SDB_ENSURE(
    w_gap <= kMaxPackWidth && w_df <= kMaxPackWidth && w_tf <= kMaxPackWidth,
    "term_dict: a run column of ", std::max({w_gap, w_df, w_tf}),
    " bits exceeds the ", kMaxPackWidth, " bit limit");

  const bool nib = (w_gap | w_df | w_tf | w_len | w_pos | w_pay) <= 15;
  out.WriteV32((rg_count << 2) | (nib ? 2U : 0U) | 1U);

  const uint32_t wbits = nib ? 4 : 5;
  uint64_t widths = 0;
  uint32_t shift = 0;
  const auto add_width = [&](uint32_t w) {
    widths |= uint64_t{w} << shift;
    shift += wbits;
  };
  add_width(w_gap);
  add_width(w_df);
  if (cols.freq) {
    add_width(w_tf);
  }
  add_width(w_len);
  if (cols.pos) {
    add_width(w_pos);
  }
  if (cols.offs) {
    add_width(w_pay);
  }
  const uint32_t width_bytes = nib ? cols.width_bytes_nib : cols.width_bytes;
  for (uint32_t i = 0; i != width_bytes; ++i) {
    out.WriteByte(static_cast<byte_type>(widths >> (i * 8)));
  }

  BitPacker packer{out};
  for (size_t i = 0; i != runs.size(); ++i) {
    packer.Push(i == 0 ? runs[0].rg : runs[i].rg - runs[i - 1].rg - 1, w_gap);
  }
  for (const auto& run : runs) {
    packer.Push(run.df, w_df);
  }
  if (cols.freq) {
    for (const auto& run : runs) {
      packer.Push(run.tf - run.df, w_tf);
    }
  }
  for (const auto& run : runs) {
    packer.Push(static_cast<uint32_t>(run.run_len), w_len);
  }
  if (cols.pos) {
    for (const auto& run : runs) {
      packer.Push(static_cast<uint32_t>(run.pos_delta), w_pos);
    }
  }
  if (cols.offs) {
    for (const auto& run : runs) {
      packer.Push(static_cast<uint32_t>(run.pay_delta), w_pay);
    }
  }
  for (const auto& run : runs) {
    if (run.df == 1) {
      SDB_ASSERT(run.single_doc >> cols.id_bits == 0);
      packer.Push(run.single_doc, cols.id_bits);
    }
  }
  packer.Finish();

  for (const auto& run : runs) {
    if (run.df > doc_limits::kBlockSize) {
      out.WriteV64(run.e_skip);
    }
  }
}

// What an entry's parse is asked to leave behind. `Pass` advances the restart
// group's chains and nothing else; `Stats` adds the term-wide counts, which the
// pass already decodes to size the record; `Full` materializes every run.
enum class ParseMode : uint8_t {
  Pass,
  Stats,
  Full,
};

// One parse for all three uses of an entry: passing over it while walking a
// restart group, reading the counts of a term whose postings nobody wants, and
// decoding the sought term. Returns the parsed byte count.
template<ParseMode Mode>
size_t ParseEntry(const byte_type* in, const RecordColumns& cols,
                  bool partitioned, bool restart, uint64_t doc_base,
                  EntryChain& chain, TermCookie* cookie) {
  const byte_type* p = in;
  if (restart) {
    ReadAbsolutes(p, cols, doc_base, chain);
  }
  // Walking a restart group reads these off every entry it passes, and each
  // is one or two bytes on any shape a leaf block holds, so the out-of-line
  // varint continuation is worth avoiding here.
  uint32_t rg_count = 1;
  uint32_t rg0 = 0;
  uint32_t docs0 = 0;
  bool tfd0 = true;
  bool nib = false;
  if (partitioned) {
    const uint64_t tag = ReadV64Short(p);
    if ((tag & 1) == 0) {
      rg0 = (static_cast<uint32_t>(tag) >> 1) & cols.rg_mask;
      uint64_t rest = tag >> (1 + cols.rg_bits);
      if (cols.freq) {
        tfd0 = (rest & 1) != 0;
        rest >>= 1;
      }
      docs0 = static_cast<uint32_t>(rest);
    } else {
      rg_count = static_cast<uint32_t>(tag) >> 2;
      nib = (tag & 2) != 0;
    }
  }

  if (rg_count == 1) {
    const uint32_t docs_count = partitioned ? docs0 : ReadV32Short(p);
    const uint32_t freq =
      cols.freq ? docs_count + (tfd0 ? ReadV32Short(p) : 0) : 0;
    if constexpr (Mode != ParseMode::Pass) {
      cookie->clear();
      cookie->stats.docs_count = docs_count;
      cookie->stats.freq = freq;
      if constexpr (Mode == ParseMode::Full) {
        // A plane the field lacks never advanced, so its anchor is already
        // zero and spelling it costs one store rather than a branch.
        cookie->doc_start = chain.doc_start;
        cookie->pos_start = chain.pos_ptr;
        cookie->pay_start = chain.pay_ptr;
      }
    }
    uint64_t run_len = 0;
    uint32_t inlined = 0;
    if (docs_count == 1) {
      inlined = vread<uint32_t>(p);
    } else {
      run_len = vread<uint64_t>(p);
      if (docs_count > doc_limits::kBlockSize) {
        const uint64_t e_skip = vread<uint64_t>(p);
        SDB_ASSERT(e_skip <= std::numeric_limits<uint32_t>::max());
        inlined = static_cast<uint32_t>(e_skip);
      }
    }
    if constexpr (Mode == ParseMode::Full) {
      // The term's only run sits on every anchor, so every offset is zero.
      cookie->rgs.emplace_back(rg0, docs_count, freq, 0U, 0U, 0U, inlined,
                               static_cast<uint8_t>(chain.pos_anchor));
    }
    chain.doc_start += run_len;
    if (cols.pos) {
      chain.pos_ptr += vread<uint64_t>(p);
      chain.pos_anchor =
        (chain.pos_anchor + freq) & (pos_limits::kBlockSize - 1);
    }
    if (cols.offs) {
      chain.pay_ptr += vread<uint64_t>(p);
    } else if (cols.pay) {
      chain.pay_ptr += docs_count;
    }
    return static_cast<size_t>(p - in);
  }

  const uint32_t wbits = nib ? 4 : 5;
  const uint32_t wmask = nib ? 15 : 31;
  uint64_t widths = 0;
  const uint32_t width_bytes = nib ? cols.width_bytes_nib : cols.width_bytes;
  for (uint32_t i = 0; i != width_bytes; ++i) {
    widths |= uint64_t{*p++} << (i * 8);
  }
  uint32_t shift = 0;
  const auto next_w = [&] {
    const auto w = static_cast<uint32_t>((widths >> shift) & wmask);
    shift += wbits;
    return w;
  };
  const uint32_t w_gap = next_w();
  const uint32_t w_df = next_w();
  const uint32_t w_tf = cols.freq ? next_w() : 0;
  const uint32_t w_len = next_w();
  const uint32_t w_pos = cols.pos ? next_w() : 0;
  const uint32_t w_pay = cols.offs ? next_w() : 0;

  // One bitstream, column-major: a column starts at the sum of the widths
  // before it, so a mode positions its readers by arithmetic and a column it
  // does not need -- the row groups for a pass, the frequencies for an
  // unpositioned pass -- costs nothing to skip.
  const byte_type* bits = p;
  uint64_t bit = 0;
  const auto column = [&](uint32_t w) {
    BitReader reader{bits, bit};
    bit += uint64_t{rg_count} * w;
    return reader;
  };
  BitReader gaps = column(w_gap);
  BitReader dfs = column(w_df);
  BitReader tfs = column(w_tf);
  BitReader lens = column(w_len);
  BitReader poss = column(w_pos);
  BitReader pays = column(w_pay);

  if constexpr (Mode != ParseMode::Pass) {
    cookie->clear();
    if constexpr (Mode == ParseMode::Full) {
      cookie->doc_start = chain.doc_start;
      cookie->pos_start = chain.pos_ptr;
      cookie->pay_start = chain.pay_ptr;
      cookie->rgs.reserve(rg_count);
    }
  }
  // A pass without positions never needs the term's frequency; every other
  // mode sums it off the tfΔ column, which is what the record stores in place
  // of a total.
  const bool read_tf = cols.freq && (Mode != ParseMode::Pass || cols.pos);
  uint32_t rg = 0;
  uint32_t singles = 0;
  uint32_t skips = 0;
  uint32_t docs_count = 0;
  uint64_t doc_start = chain.doc_start;
  uint64_t pos_ptr = chain.pos_ptr;
  uint64_t pay_ptr = chain.pay_ptr;
  uint32_t tf_sum = 0;
  for (uint32_t i = 0; i != rg_count; ++i) {
    const uint32_t df = dfs.Read(w_df);
    SDB_ASSERT(df != 0);
    docs_count += df;
    singles += df == 1;
    skips += df > doc_limits::kBlockSize;
    const uint64_t run_len = lens.Read(w_len);
    if constexpr (Mode == ParseMode::Full) {
      rg = i == 0 ? gaps.Read(w_gap) : rg + gaps.Read(w_gap) + 1;
      const uint32_t tf = cols.freq ? df + tfs.Read(w_tf) : 0;
      cookie->rgs.emplace_back(
        rg, df, tf, static_cast<uint32_t>(doc_start - chain.doc_start),
        static_cast<uint32_t>(pos_ptr - chain.pos_ptr),
        static_cast<uint32_t>(pay_ptr - chain.pay_ptr), 0U,
        static_cast<uint8_t>((chain.pos_anchor + tf_sum) &
                             (pos_limits::kBlockSize - 1)));
      tf_sum += tf;
    } else if (read_tf) {
      tf_sum += df + tfs.Read(w_tf);
    }
    doc_start += run_len;
    if (cols.pos) {
      pos_ptr += poss.Read(w_pos);
    }
    if (cols.offs) {
      pay_ptr += pays.Read(w_pay);
    }
  }
  // The df == 1 ids trail the columns inside the bitstream at the grid's id
  // width; the sparse `e_skip` varints follow the whole stream. The df column
  // just said how many of each there are, which is what places both.
  p = bits + (bit + uint64_t{singles} * cols.id_bits + 7) / 8;
  if constexpr (Mode == ParseMode::Full) {
    if (singles != 0) {
      BitReader ids{bits, bit};
      for (auto& group : cookie->rgs) {
        if (group.docs_count == 1) {
          group.inlined = ids.Read(cols.id_bits);
        }
      }
    }
    if (skips != 0) {
      for (auto& group : cookie->rgs) {
        if (group.docs_count > doc_limits::kBlockSize) {
          const uint64_t e_skip = vread<uint64_t>(p);
          SDB_ASSERT(e_skip <= std::numeric_limits<uint32_t>::max());
          group.inlined = static_cast<uint32_t>(e_skip);
        }
      }
    }
  } else {
    IRS_IGNORE(rg);
    for (uint32_t k = 0; k != skips; ++k) {
      vread<uint64_t>(p);
    }
  }

  if constexpr (Mode != ParseMode::Pass) {
    cookie->stats.docs_count = docs_count;
    cookie->stats.freq = cols.freq ? tf_sum : 0;
  }
  chain.doc_start = doc_start;
  if (cols.pos) {
    chain.pos_ptr = pos_ptr;
    chain.pos_anchor =
      (chain.pos_anchor + tf_sum) & (pos_limits::kBlockSize - 1);
  }
  if (cols.offs) {
    chain.pay_ptr = pay_ptr;
  } else if (cols.pay) {
    chain.pay_ptr += docs_count;
  }
  return static_cast<size_t>(p - in);
}

static_assert(std::endian::native == std::endian::little);

// The pending-payload arena of a block, written through directly. A record is
// complete when its term is added, so what the encoder emits is already the
// bytes the block will write out -- there is no scratch stream between them.
// The buffer outlives every block of the field, so once it has grown a record
// costs three pointer stores and the bytes themselves.
class PayloadArena final : public BufferedOutput {
 public:
  PayloadArena() noexcept : BufferedOutput{nullptr, nullptr} {}

  const byte_type* Data() const noexcept { return _buffer.data(); }
  size_t Size() const noexcept { return _size; }

  // Opens a window on the arena's tail for one record.
  BufferedOutput& Open() noexcept {
    auto* data = _buffer.data();
    _buf = data + _size;
    _pos = _buf;
    _end = data + _buffer.size();
    return *this;
  }

  // Closes it; the arena keeps exactly what the record wrote.
  uint32_t Close() noexcept {
    const auto size = static_cast<uint32_t>(Length());
    _size += size;
    return size;
  }

  void Clear() noexcept { _size = 0; }

  // Drops the leading `count` bytes -- the records a block has just taken.
  void DropPrefix(size_t count) noexcept {
    SDB_ASSERT(count <= _size);
    _size -= count;
    std::memmove(_buffer.data(), _buffer.data() + count, _size);
  }

 private:
  static constexpr size_t kMinSize = 4096;

  void WriteDirect(const byte_type* b, size_t len) final {
    Grow(len + 1);
    WriteBuffer(b, len);
  }

  void Grow(size_t need) {
    const auto base = static_cast<size_t>(_buf - _buffer.data());
    const auto written = Length();
    _buffer.resize(
      std::max({_buffer.size() * 2, base + written + need, kMinSize}));
    auto* data = _buffer.data();
    _buf = data + base;
    _pos = _buf + written;
    _end = data + _buffer.size();
  }

  bstring _buffer;
  // What the arena holds, as opposed to what it has room for: the buffer only
  // ever grows, so a record never pays for the window it is written through.
  size_t _size{0};
};

size_t CommonPrefix(bytes_view lhs, bytes_view rhs) noexcept {
  const size_t limit = std::min(lhs.size(), rhs.size());
  size_t i = 0;
  for (; i + sizeof(uint64_t) <= limit; i += sizeof(uint64_t)) {
    uint64_t a;
    uint64_t b;
    std::memcpy(&a, lhs.data() + i, sizeof(a));
    std::memcpy(&b, rhs.data() + i, sizeof(b));
    if (a != b) {
      return i + static_cast<size_t>(std::countr_zero(a ^ b)) / 8;
    }
  }
  for (; i != limit && lhs[i] == rhs[i]; ++i) {
  }
  return i;
}

// Three-way comparison that also reports how far the two agree.
int CompareFull(bytes_view key, bytes_view target, size_t& matched) noexcept {
  matched = CommonPrefix(key, target);
  if (matched == key.size()) {
    return matched == target.size() ? 0 : -1;
  }
  if (matched == target.size()) {
    return 1;
  }
  return key[matched] < target[matched] ? -1 : 1;
}

class FsstEncoder : private util::Noncopyable {
 public:
  FsstEncoder() = default;

  ~FsstEncoder() { Reset(); }

  void Reset() noexcept {
    if (_encoder) {
      duckdb_fsst_destroy(_encoder);
      _encoder = nullptr;
    }
    _table.clear();
  }

  struct Decision {
    size_t samples{};
    size_t raw_bytes{};
    size_t encoded_bytes{};
    size_t table_bytes{};
    bool enabled{false};

    size_t MeanRawSuffix() const noexcept {
      return samples ? raw_bytes / samples : 0;
    }
    uint32_t RatioPct() const noexcept {
      return encoded_bytes
               ? static_cast<uint32_t>(raw_bytes * 100 / encoded_bytes)
               : 0;
    }
  };

  const Decision& LastDecision() const noexcept { return _decision; }

  // Trains a symbol table and keeps it only when the sampled suffixes both
  // compress well enough to pay for the table and are short enough that the
  // per-entry decode does not dominate sequential iteration -- the merge and
  // facet-scan path walks every entry and pays the decode with nothing to
  // amortise it against, whereas a point seek touches a handful.
  bool Train(std::span<const bytes_view> samples,
             const term_dict::WriterOptions& options) {
    Reset();
    _decision = {};
    _decision.samples = samples.size();
    if (samples.size() < kFsstMinSamples) {
      return false;
    }
    std::vector<size_t> lengths(samples.size());
    std::vector<unsigned char*> starts(samples.size());
    size_t raw_size = 0;
    for (size_t i = 0; i != samples.size(); ++i) {
      lengths[i] = samples[i].size();
      starts[i] = const_cast<unsigned char*>(samples[i].data());
      raw_size += samples[i].size();
    }
    _decision.raw_bytes = raw_size;
    if (raw_size == 0) {
      return false;
    }
    if (raw_size > size_t{options.fsst_max_mean_suffix} * samples.size() ||
        raw_size < size_t{options.fsst_min_mean_suffix} * samples.size()) {
      return false;
    }
    _encoder = duckdb_fsst_create(samples.size(), lengths.data(), starts.data(),
                                  /*zeroTerminated=*/0);
    if (!_encoder) {
      return false;
    }
    unsigned char table[FSST_MAXHEADER];
    const unsigned int table_size = duckdb_fsst_export(_encoder, table);

    std::vector<size_t> out_len(samples.size());
    std::vector<unsigned char*> out_ptr(samples.size());
    std::vector<unsigned char> out(7 + 2 * raw_size);
    const size_t done = duckdb_fsst_compress(
      _encoder, samples.size(), lengths.data(), starts.data(), out.size(),
      out.data(), out_len.data(), out_ptr.data());
    size_t encoded_size = 0;
    for (size_t i = 0; i != done; ++i) {
      encoded_size += out_len[i];
    }
    _decision.encoded_bytes = encoded_size;
    _decision.table_bytes = table_size;
    if (done != samples.size() || encoded_size == 0 ||
        raw_size * 100 <
          encoded_size * options.fsst_min_ratio_pct + table_size * 100) {
      Reset();
      return false;
    }
    _table.assign(table, table_size);
    _decision.enabled = true;
    return true;
  }

  bool Enabled() const noexcept { return _encoder; }

  bytes_view Table() const noexcept { return _table; }

  // Compresses a whole block worth of suffixes in one call: the per-call
  // setup of `duckdb_fsst_compress` dominates when driven per entry.
  bool EncodeBatch(std::span<const bytes_view> input, bstring& buffer,
                   std::vector<bytes_view>& output) {
    SDB_ASSERT(_encoder);
    output.clear();
    if (input.empty()) {
      return true;
    }
    _lengths.resize(input.size());
    _starts.resize(input.size());
    size_t raw_size = 0;
    for (size_t i = 0; i != input.size(); ++i) {
      _lengths[i] = input[i].size();
      _starts[i] = const_cast<unsigned char*>(input[i].data());
      raw_size += input[i].size();
    }
    sdb::basics::StrResizeAmortized(buffer, 7 + 2 * raw_size);
    _out_lengths.resize(input.size());
    _out_starts.resize(input.size());
    const size_t done = duckdb_fsst_compress(
      _encoder, input.size(), _lengths.data(), _starts.data(), buffer.size(),
      buffer.data(), _out_lengths.data(), _out_starts.data());
    if (done != input.size()) {
      return false;
    }
    output.reserve(input.size());
    for (size_t i = 0; i != input.size(); ++i) {
      output.emplace_back(_out_starts[i], _out_lengths[i]);
    }
    return true;
  }

 private:
  duckdb_fsst_encoder_t* _encoder{};
  bstring _table;
  Decision _decision;
  std::vector<size_t> _lengths;
  std::vector<unsigned char*> _starts;
  std::vector<size_t> _out_lengths;
  std::vector<unsigned char*> _out_starts;
};

// A code expands to at most this many bytes, and the decode loop stores that
// many unconditionally, so `kFsstMaxSymbol * encoded_size` bytes of room is
// both necessary and sufficient for any encoded suffix.
constexpr size_t kFsstMaxSymbol = 8;

class FsstDecoder {
 public:
  void Reset(bytes_view table) {
    _enabled = false;
    if (table.empty()) {
      return;
    }
    bstring copy{table};
    _enabled = duckdb_fsst_import(&_decoder, copy.data()) != 0;
  }

  bool Enabled() const noexcept { return _enabled; }

  // The bytes a code stands for. `FSST_ESC` is not one of them -- it escapes
  // the byte after it rather than naming a symbol -- so a caller that decides
  // a run of codes without decoding it has to reject an escape on its own.
  bytes_view Symbol(size_t code) const noexcept {
    SDB_ASSERT(_enabled && code < FSST_ESC);
    return {reinterpret_cast<const byte_type*>(&_decoder.symbol[code]),
            _decoder.len[code]};
  }

  // Decodes straight into `out`, which must hold `kFsstMaxSymbol *
  // input.size()` bytes. `duckdb_fsst_decompress` bounds every store against
  // the output size and splits into a wide loop, a three-code tail and a
  // byte-at-a-time remainder; for the handful of codes a front-coded suffix
  // holds that framing costs more than the decode.
  size_t DecodeInto(bytes_view input, byte_type* out) const noexcept {
    SDB_ASSERT(_enabled);
    const auto* len = _decoder.len;
    const auto* symbol = _decoder.symbol;
    const size_t count = input.size();
    size_t pos = 0;
    for (size_t i = 0; i != count;) {
      const size_t code = input[i++];
      if (code != FSST_ESC) [[likely]] {
        std::memcpy(out + pos, &symbol[code], sizeof(*symbol));
        pos += len[code];
      } else {
        SDB_ASSERT(i != count);
        out[pos++] = input[i++];
      }
    }
    return pos;
  }

 private:
  duckdb_fsst_decoder_t _decoder{};
  bool _enabled{false};
};

}  // namespace
namespace irs::term_dict {

class FieldWriter::Impl {
 public:
  Impl(PostingsWriter::ptr&& pw, IResourceManager& rm);

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }
  void SetOptions(const WriterOptions& options) noexcept { _options = options; }

  void prepare(const FlushState& state);
  void write(const BasicTermReader& reader);
  void end();

 private:
  // A term pended while the field's FSST layout is still undecided: its key,
  // its encoded record, and the term-start stream absolutes a restart entry
  // spells. Front coding needs one block of hindsight and gets none of this;
  // the store exists because the FSST table has to be trained before the
  // first block's bytes can exist, and it is replayed and dropped the moment
  // that decision is made.
  struct Entry {
    uint32_t key_offset;
    uint32_t key_size;
    uint32_t pay_offset;
    uint32_t pay_size;
    uint64_t doc_start;
    uint64_t pos_ptr;
    uint64_t pay_ptr;
    uint32_t pos_anchor;
  };

  bytes_view SampleKey(const Entry& entry) const noexcept {
    return {_sample_keys.data() + entry.key_offset, entry.key_size};
  }

  bytes_view BlockKey(uint32_t pos) const noexcept {
    const auto offset = _key_offsets[pos];
    const auto end = pos + 1 != _key_offsets.size()
                       ? _key_offsets[pos + 1]
                       : static_cast<uint32_t>(_keys.size());
    return {_keys.data() + offset, end - offset};
  }

  void BeginField(const FieldProperties& props);
  void EndField(field_id id, const FieldProperties& props, bytes_view min_term,
                bytes_view max_term, uint64_t total_doc_freq,
                uint64_t total_term_freq, uint64_t term_count);
  void Add(bytes_view term);
  bool BlockFull(size_t count, size_t bytes, size_t pay_bytes) const noexcept;
  void DecideLayout();
  void Replay();
  void CommitTerm(bytes_view term, uint32_t pay_offset, uint64_t doc_start,
                  uint64_t pos_ptr, uint64_t pay_ptr, uint32_t pos_anchor);
  void WriteSinglePayload(bstring& out);
  void WriteFieldMeta(field_id id, const FieldProperties& props,
                      FieldHeader& header,
                      const PostingsWriter::FieldStats& stats,
                      doc_id_t doc_count, uint64_t total_doc_freq,
                      uint64_t total_term_freq, uint64_t term_count);
  void FlushBlock(size_t records_end);
  void FlushTail();
  void AppendSeparator(bytes_view first_key);
  void ResetField();

  PostingsWriter::ptr _pw;
  IdxWriter* _idx{};
  IndexOutput* _blocks_out{};
  WriterOptions _options;

  bstring _sample_keys;
  PayloadArena _sample_pay;
  std::vector<Entry> _sample;
  bool _sampling{true};

  // The block being built. Keys are contiguous in `_keys`; records sit in
  // `_pay` and the restart absolutes in `_absolutes`, each already in the
  // bytes the payload area will spell, so a flush interleaves two spans per
  // restart group and copies nothing per entry.
  bstring _keys;
  std::vector<uint32_t> _key_offsets;
  PayloadArena _pay;
  PayloadArena _absolutes;
  std::vector<uint32_t> _restart_pay;
  std::vector<uint32_t> _restart_abs;
  // Key width shared by every entry of the block so far, zero once two
  // differed.
  uint32_t _block_width{};

  MemoryOutput _separators;
  std::vector<uint64_t> _block_offsets;
  std::vector<uint32_t> _block_entries;
  std::vector<uint32_t> _restart_str;
  std::vector<uint32_t> _lcps;
  std::vector<bytes_view> _suffixes;
  std::vector<bytes_view> _encoded;
  bstring _prev_separator;
  bstring _prev_block_last;
  bstring _scratch;

  FsstEncoder _fsst;
  TermRuns _runs;
  RecordColumns _cols;
  IndexFeatures _features{IndexFeatures::None};
  uint32_t _restart_interval{kRestartIntervalDefault};
  uint32_t _row_group_size{};
  uint32_t _segment_docs{};
  bool _row_grouped{false};
  uint32_t _block_count{};
  // Where the field's postings start in `.doc`, i.e. its first term's
  // `doc_start`; restart entries are written against it.
  uint64_t _doc_base{};
  // Term width shared by every block flushed so far, zero once a block came out
  // front-coded or a block's width differed. Non-zero at `EndField` means the
  // whole field is fixed-stride.
  uint32_t _field_width{};
  field_id _field_id{};
};

FieldWriter::Impl::Impl(PostingsWriter::ptr&& pw, IResourceManager& rm)
  : _pw{std::move(pw)}, _separators{rm} {
  SDB_ASSERT(_pw);
}

void FieldWriter::Impl::prepare(const FlushState& state) {
  SDB_ASSERT(_idx, "term_dict::FieldWriter requires SetIdxWriter first");
  SDB_ENSURE(_options.row_group_size != 0,
             "term_dict: row_group_size must be positive");

  _segment_docs = static_cast<uint32_t>(state.doc_count);
  _row_group_size = _options.row_group_size;
  _row_grouped = _segment_docs > _row_group_size;

  _blocks_out = &_idx->BlocksOut();
  _pw->Prepare(*_blocks_out, state);
  ResetField();
}

void FieldWriter::Impl::ResetField() {
  _sample_keys.clear();
  _sample_pay.Clear();
  _sample.clear();
  _sampling = true;
  _keys.clear();
  _key_offsets.clear();
  _pay.Clear();
  _absolutes.Clear();
  _restart_pay.clear();
  _restart_abs.clear();
  _block_width = 0;
  _separators.Reset();
  _block_offsets.clear();
  _block_entries.clear();
  _prev_separator.clear();
  _prev_block_last.clear();
  _fsst.Reset();
  _restart_interval = _options.restart_interval;
  _block_count = 0;
  _doc_base = 0;
  _field_width = 0;
}

void FieldWriter::Impl::write(const BasicTermReader& reader) {
  const auto props = reader.properties();
  const auto index_features = props.index_features;
  _field_id = reader.id();
  BeginField(props);
  _pw->SetTermPayloadWriter(reader.PayloadWriter());
  _pw->SetKnownDocsCount(reader.KnownDocsCount());

  uint64_t term_count = 0;
  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;

  const bool freq_exists =
    IndexFeatures::None != (index_features & IndexFeatures::Freq);

  auto terms = reader.iterator();
  SDB_ASSERT(terms);
  while (terms->next()) {
    auto postings = terms->postings(index_features);
    _pw->WriteTerm(*postings, _row_group_size, _runs);

    if (freq_exists) {
      sum_tfreq += _runs.freq;
    }

    if (_runs.docs_count != 0) {
      if (term_count == 0) {
        // The field's terms hold its `.doc` region in dict order, gapless, so
        // the first one starts it.
        _doc_base = _runs.doc_start;
      }
      sum_dfreq += _runs.docs_count;
      Add(terms->value());
      ++term_count;
    }
  }

  EndField(reader.id(), props, reader.min(), reader.max(), sum_dfreq, sum_tfreq,
           term_count);
}

void FieldWriter::Impl::BeginField(const FieldProperties& props) {
  SDB_ASSERT(_blocks_out);
  SDB_ASSERT(_sample.empty());
  SDB_ASSERT(_key_offsets.empty());
  ResetField();
  _features = props.index_features;
  const auto rg_count = static_cast<uint32_t>(std::max<uint64_t>(
    1, (uint64_t{_segment_docs} + _row_group_size - 1) / _row_group_size));
  _cols = RecordColumns{_features, rg_count, _row_group_size};
  _pw->BeginField(props);
}

void FieldWriter::Impl::Add(bytes_view term) {
  SDB_ASSERT(!_runs.runs.empty());
  SDB_ASSERT(_row_grouped || (_runs.runs.size() == 1 && _runs.runs[0].rg == 0));

  if (_sampling) {
    const auto pay_offset = static_cast<uint32_t>(_sample_pay.Size());
    EncodeRecord(_sample_pay.Open(), _cols, _row_grouped, _runs);
    const auto record_size = _sample_pay.Close();
    const auto key_offset = static_cast<uint32_t>(_sample_keys.size());
    _sample_keys.append(term.data(), term.size());
    _sample.emplace_back(key_offset, static_cast<uint32_t>(term.size()),
                         pay_offset, record_size, _runs.doc_start,
                         _runs.pos_ptr, _runs.pay_ptr, _runs.pos_anchor);
    if (_sample_keys.size() >= kFsstSampleBytes) {
      DecideLayout();
      Replay();
    }
    return;
  }

  // The record is complete right here: everything it spells is term-scoped, so
  // nothing about it waits for the block, and it is encoded where it will stay.
  auto pay_offset = static_cast<uint32_t>(_pay.Size());
  EncodeRecord(_pay.Open(), _cols, _row_grouped, _runs);
  _pay.Close();

  // An entry wider than a whole block gets a block of its own: the byte target
  // is a target, and a block that holds one entry needs no restart offsets to
  // stay inside their width. Flushing first is what makes it alone. A record
  // near the payload area's u16 restart-offset bound forces the same cut.
  if (!_key_offsets.empty() && (term.size() >= _options.block_byte_target ||
                                _pay.Size() >= kMaxAreaSize / 2)) {
    FlushBlock(pay_offset);
    // The block took everything before this record; it starts the next one.
    _pay.DropPrefix(pay_offset);
    pay_offset = 0;
  }

  CommitTerm(term, pay_offset, _runs.doc_start, _runs.pos_ptr, _runs.pay_ptr,
             _runs.pos_anchor);
}

// One term enters the block: the absolutes when its entry lands on a restart,
// then the key, then the cut when the block is full.
void FieldWriter::Impl::CommitTerm(bytes_view term, uint32_t pay_offset,
                                   uint64_t doc_start, uint64_t pos_ptr,
                                   uint64_t pay_ptr, uint32_t pos_anchor) {
  const auto pos = static_cast<uint32_t>(_key_offsets.size());
  if (pos % _restart_interval == 0) {
    const auto abs_offset = static_cast<uint32_t>(_absolutes.Size());
    _restart_abs.push_back(abs_offset);
    _restart_pay.push_back(abs_offset + pay_offset);
    WriteAbsolutes(_absolutes.Open(), _cols, _doc_base, doc_start, pos_ptr,
                   pos_anchor, pay_ptr);
    _absolutes.Close();
  }
  if (pos == 0) {
    _block_width = static_cast<uint32_t>(term.size());
  } else if (_block_width != term.size()) {
    _block_width = 0;
  }
  _key_offsets.push_back(static_cast<uint32_t>(_keys.size()));
  _keys.append(term.data(), term.size());

  if (BlockFull(_key_offsets.size(), _keys.size(), _pay.Size())) {
    FlushBlock(_pay.Size());
    _pay.Clear();
  }
}

bool FieldWriter::Impl::BlockFull(size_t count, size_t bytes,
                                  size_t pay_bytes) const noexcept {
  return bytes >= _options.block_byte_target || count >= kMaxBlockEntries ||
         pay_bytes >= kMaxAreaSize / 2;
}

void FieldWriter::Impl::DecideLayout() {
  SDB_ASSERT(_sampling);
  _sampling = false;
  _restart_interval = _options.restart_interval;
  if (!_options.fsst_enabled || _sample.size() < kFsstMinSamples) {
    return;
  }
  std::vector<bytes_view> samples;
  samples.reserve(_sample.size());
  bytes_view prev;
  for (const auto& entry : _sample) {
    const auto key = SampleKey(entry);
    const size_t lcp = CommonPrefix(prev, key);
    if (lcp < key.size()) {
      samples.emplace_back(key.data() + lcp, key.size() - lcp);
    }
    prev = key;
  }
  _fsst.Train(samples, _options);
  const auto& decision = _fsst.LastDecision();
  SDB_INFO(IRESEARCH, "term_dict field ", _field_id, " restart interval ",
           _restart_interval, ", fsst ", decision.enabled ? "on" : "off", ": ",
           decision.samples, " sampled suffixes, ", decision.raw_bytes,
           " raw bytes, ", decision.encoded_bytes, " encoded bytes, ",
           decision.table_bytes, " table bytes, ratio ", decision.RatioPct(),
           "%, mean suffix ", decision.MeanRawSuffix(), " bytes");
}

// The sampled terms flow through the same block path as every later one, so
// the cuts fall exactly where they would have fallen live; whatever block is
// filling when the store runs dry simply keeps filling. The one giant-entry
// rule the live path adds on top does not apply here, matching the cuts this
// prefix always had.
void FieldWriter::Impl::Replay() {
  SDB_ASSERT(!_sampling);
  for (const auto& entry : _sample) {
    const auto pay_offset = static_cast<uint32_t>(_pay.Size());
    _pay.Open().WriteData(_sample_pay.Data() + entry.pay_offset,
                          entry.pay_size);
    _pay.Close();
    CommitTerm(SampleKey(entry), pay_offset, entry.doc_start, entry.pos_ptr,
               entry.pay_ptr, entry.pos_anchor);
  }
  _sample.clear();
  _sample_keys.clear();
  _sample_pay.Clear();
}

void FieldWriter::Impl::FlushTail() {
  if (_key_offsets.empty()) {
    return;
  }
  FlushBlock(_pay.Size());
  _pay.Clear();
}

void FieldWriter::Impl::AppendSeparator(bytes_view first_key) {
  if (_block_count == 0) {
    return;
  }
  const size_t lcp = CommonPrefix(_prev_block_last, first_key);
  SDB_ASSERT(lcp < first_key.size());
  const bytes_view separator{first_key.data(), lcp + 1};
  const size_t shared = CommonPrefix(_prev_separator, separator);
  _separators.stream.WriteV32(static_cast<uint32_t>(shared));
  _separators.stream.WriteV32(static_cast<uint32_t>(separator.size() - shared));
  _separators.stream.WriteData(separator.data() + shared,
                               separator.size() - shared);
  _prev_separator.assign(separator);
}

// Everything of the block but its keys is already in payload-area byte order,
// so the flush costs the string area once and copies each area span once. The
// var-layout size and restart offsets fall out of one walk over the suffixes
// the lcp pass leaves behind; fixed stride is only taken when it does not
// cost more than that, so no shape can lose bytes to the layout.
void FieldWriter::Impl::FlushBlock(size_t records_end) {
  const auto entry_count = static_cast<uint32_t>(_key_offsets.size());
  SDB_ASSERT(entry_count != 0);
  const uint32_t restart_interval = _restart_interval;
  const uint32_t restart_count =
    (entry_count + restart_interval - 1) / restart_interval;
  SDB_ASSERT(restart_count <= kMaxRestarts);
  SDB_ASSERT(restart_count == _restart_pay.size());
  SDB_ASSERT(restart_count == _restart_abs.size());

  AppendSeparator(BlockKey(0));

  _lcps.resize(entry_count);
  _suffixes.clear();
  bytes_view prev;
  for (uint32_t pos = 0; pos != entry_count; ++pos) {
    const auto key = BlockKey(pos);
    if (pos % restart_interval == 0) {
      _lcps[pos] = 0;
    } else {
      const auto lcp = static_cast<uint32_t>(CommonPrefix(prev, key));
      SDB_ASSERT(lcp < key.size());
      _lcps[pos] = lcp;
      _suffixes.emplace_back(key.data() + lcp, key.size() - lcp);
    }
    prev = key;
  }
  _prev_block_last.assign(prev);

  const bool fsst = _fsst.Enabled();
  if (fsst) {
    SDB_ENSURE(_fsst.EncodeBatch(_suffixes, _scratch, _encoded),
               "term_dict: failed to encode ", _suffixes.size(),
               " term suffixes");
  }

  _restart_str.clear();
  size_t var_string = 0;
  {
    size_t suffix = 0;
    for (uint32_t pos = 0; pos != entry_count; ++pos) {
      if (pos % restart_interval == 0) {
        _restart_str.push_back(static_cast<uint32_t>(var_string));
        const auto len = static_cast<uint32_t>(BlockKey(pos).size());
        var_string += EntryHeaderSize(0, len) + len;
      } else {
        const auto len = static_cast<uint32_t>(
          (fsst ? _encoded[suffix] : _suffixes[suffix]).size());
        ++suffix;
        var_string += EntryHeaderSize(_lcps[pos], len) + len;
      }
    }
  }

  // A block of one entry is never fixed-stride: it has nothing to amortize
  // the shared prefix over, and an oversized solo term would store its whole
  // key as that prefix.
  const uint32_t width = entry_count >= 2 ? _block_width : 0;
  uint32_t prefix_len = 0;
  uint32_t stride = 0;
  if (width != 0) {
    prefix_len = static_cast<uint32_t>(
      CommonPrefix(BlockKey(0), BlockKey(entry_count - 1)));
    SDB_ASSERT(prefix_len < width);
    stride = width - prefix_len;
    // The block's shared prefix once, then one raw stride per entry, and a
    // restart table of payload offsets alone.
    const size_t fixed_bytes = V32Size(prefix_len) + prefix_len +
                               size_t{entry_count} * stride +
                               size_t{restart_count} * sizeof(uint16_t);
    const size_t var_bytes =
      var_string + size_t{restart_count} * kRestartEntrySize;
    if (fixed_bytes > var_bytes + var_bytes / kFixedStrideSlackDiv) {
      stride = 0;
    }
  }
  const bool fixed = stride != 0;
  if (fixed && (_block_count == 0 || _field_width == width)) {
    _field_width = width;
  } else {
    _field_width = 0;
  }

  const auto string_size =
    static_cast<uint32_t>(fixed ? size_t{entry_count} * stride : var_string);
  const auto payload_size =
    static_cast<uint32_t>(_absolutes.Size() + records_end);

  SDB_ENSURE(restart_count == 1 ||
               (string_size <= kMaxAreaSize && payload_size <= kMaxAreaSize),
             "term_dict: block area of ", std::max(string_size, payload_size),
             " bytes exceeds the ", kMaxAreaSize,
             " byte limit; lower block_byte_target");

  _block_offsets.push_back(_blocks_out->Position());
  _block_entries.push_back(entry_count);
  _blocks_out->WriteV32(entry_count);
  _blocks_out->WriteV32(restart_count);
  _blocks_out->WriteV32(string_size);
  _blocks_out->WriteV32(payload_size);
  _blocks_out->WriteV32(fixed ? stride + 1 : 0);
  if (fixed) {
    _blocks_out->WriteV32(prefix_len);
    _blocks_out->WriteData(BlockKey(0).data(), prefix_len);
  }
  // Fixed width so the reader indexes the table straight out of the mapped
  // block instead of decoding it into the iterator. A fixed-stride block needs
  // no string offsets: entry `i` starts at `i * stride`.
  for (uint32_t i = 0; i != restart_count; ++i) {
    if (!fixed) {
      _blocks_out->WriteU16(static_cast<uint16_t>(_restart_str[i]));
    }
    _blocks_out->WriteU16(static_cast<uint16_t>(_restart_pay[i]));
  }

  if (fixed) {
    for (uint32_t pos = 0; pos != entry_count; ++pos) {
      _blocks_out->WriteData(BlockKey(pos).data() + prefix_len, stride);
    }
  } else {
    size_t suffix = 0;
    for (uint32_t pos = 0; pos != entry_count; ++pos) {
      if (pos % restart_interval == 0) {
        const auto key = BlockKey(pos);
        WriteEntryHeader(*_blocks_out, 0, static_cast<uint32_t>(key.size()));
        _blocks_out->WriteData(key.data(), key.size());
      } else {
        const auto stored = fsst ? _encoded[suffix] : _suffixes[suffix];
        ++suffix;
        WriteEntryHeader(*_blocks_out, _lcps[pos],
                         static_cast<uint32_t>(stored.size()));
        _blocks_out->WriteData(stored.data(), stored.size());
      }
    }
  }

  for (uint32_t i = 0; i != restart_count; ++i) {
    const auto abs_begin = _restart_abs[i];
    const auto abs_end = i + 1 != restart_count
                           ? _restart_abs[i + 1]
                           : static_cast<uint32_t>(_absolutes.Size());
    _blocks_out->WriteData(_absolutes.Data() + abs_begin, abs_end - abs_begin);
    const auto rec_begin = _restart_pay[i] - abs_begin;
    const auto rec_end = i + 1 != restart_count
                           ? _restart_pay[i + 1] - abs_end
                           : static_cast<uint32_t>(records_end);
    _blocks_out->WriteData(_pay.Data() + rec_begin, rec_end - rec_begin);
  }

  ++_block_count;
  _keys.clear();
  _key_offsets.clear();
  _absolutes.Clear();
  _restart_pay.clear();
  _restart_abs.clear();
  _block_width = 0;
}

void FieldWriter::Impl::EndField(field_id id, const FieldProperties& props,
                                 bytes_view min_term, bytes_view max_term,
                                 uint64_t total_doc_freq,
                                 uint64_t total_term_freq,
                                 uint64_t term_count) {
  SDB_ASSERT(_blocks_out);

  if (!term_count) {
    ResetField();
    return;
  }

  const auto stats = _pw->EndField();
  const auto doc_count = stats.docs_count;
  SDB_ASSERT(doc_count != 0);

  FieldHeader header;
  header.restart_interval = _restart_interval;
  header.row_group_size = _row_group_size;
  header.segment_docs = _segment_docs;
  header.min_term.assign(min_term);
  header.max_term.assign(max_term);

  auto copy = [this](const byte_type* b, size_t len) {
    _blocks_out->WriteData(b, len);
    return true;
  };

  if (props.dictless) {
    // Nothing of the dictionary is stored: the field is its one term, whose
    // bytes the header already carries as `min_term`, and its records go in the
    // header beside them. No block, no separator, no term-meta section.
    SDB_ENSURE(term_count == 1, "term_dict: a single-term field holds ",
               term_count, " terms");
    header.layout = LayoutKind::Single;
    WriteSinglePayload(header.payload);
    WriteFieldMeta(id, props, header, stats, doc_count, total_doc_freq,
                   total_term_freq, term_count);
    return;
  }

  if (_sampling) {
    DecideLayout();
    Replay();
  }
  FlushTail();
  SDB_ASSERT(_sample.empty());
  SDB_ASSERT(_key_offsets.empty());

  if (_fsst.Enabled()) {
    header.flags |= kFlagFsst;
    header.fsst_table.assign(_fsst.Table());
  }
  header.block_count = _block_count;
  // Every block came out fixed-stride at one term width, so the field as a
  // whole is. Blocks carry their own stride; this is only what the field says
  // about itself, and what introspection reports.
  if (_field_width != 0) {
    header.layout = LayoutKind::FixedStride;
  }

  // Directory entry = offset delta plus the block's entry count, whose running
  // sum is the ordinal of the block's first term; the final entry is the end
  // sentinel and carries no count.
  header.separators_offset = _blocks_out->Position();
  SDB_ASSERT(_block_offsets.size() == _block_count);
  SDB_ASSERT(_block_entries.size() == _block_count);
  _block_offsets.push_back(header.separators_offset);
  uint64_t prev_offset = 0;
  for (size_t i = 0; i != _block_offsets.size(); ++i) {
    _blocks_out->WriteV64(_block_offsets[i] - prev_offset);
    prev_offset = _block_offsets[i];
    if (i != _block_count) {
      _blocks_out->WriteV32(_block_entries[i]);
    }
  }
  _separators.stream.Flush();
  _separators.file.Visit(copy);

  WriteFieldMeta(id, props, header, stats, doc_count, total_doc_freq,
                 total_term_freq, term_count);
}

// The one term's record, exactly as a block payload entry would spell it --
// absolutes first, since the header is its own restart. Such a field never
// leaves sampling, so the term sits in the sample store.
void FieldWriter::Impl::WriteSinglePayload(bstring& out) {
  SDB_ASSERT(_sampling);
  SDB_ASSERT(_sample.size() == 1);
  SDB_ASSERT(_pay.Size() == 0);
  const auto& entry = _sample.front();
  auto& stream = _pay.Open();
  WriteAbsolutes(stream, _cols, _doc_base, entry.doc_start, entry.pos_ptr,
                 entry.pos_anchor, entry.pay_ptr);
  stream.WriteData(_sample_pay.Data() + entry.pay_offset, entry.pay_size);
  _pay.Close();
  out.assign(_pay.Data(), _pay.Size());
  _pay.Clear();
}

void FieldWriter::Impl::WriteFieldMeta(
  field_id id, const FieldProperties& props, FieldHeader& header,
  const PostingsWriter::FieldStats& stats, doc_id_t doc_count,
  uint64_t total_doc_freq, uint64_t total_term_freq, uint64_t term_count) {
  TermDictMeta meta;
  meta.features = props.index_features;
  meta.term_count = term_count;
  meta.doc_count = doc_count;
  meta.total_doc_freq = total_doc_freq;
  meta.total_term_freq = total_term_freq;
  meta.has_wand = stats.has_wand;
  meta.body_offset = _blocks_out->Position();
  meta.norm = props.norm;

  header.doc_base = _doc_base;
  header.pay_base = stats.pay_base;
  WriteFieldHeader(*_blocks_out, header, props.index_features);
  _idx->AddTermDictEntry(id, std::move(meta));
  ResetField();
}

void FieldWriter::Impl::end() {
  _pw->End();
  _idx = nullptr;
  _blocks_out = nullptr;
  ResetField();
}

}  // namespace irs::term_dict
namespace {

using namespace irs;

// A loaded leaf block: the string area, the restart table and the payload
// area, all pointing either into the mapped `.idx` or into an owned buffer.
class BlockView {
 public:
  void Load(IndexInput& in, uint64_t offset, uint64_t size) {
    in.Seek(offset);
    const byte_type* data = in.ReadStable(size);
    if (!data) {
      _buf.resize(size);
      in.ReadData(_buf.data(), size);
      data = _buf.c_str();
    }
    Parse(data);
  }

  uint32_t EntryCount() const noexcept { return _entry_count; }
  uint32_t RestartCount() const noexcept { return _restart_count; }
  const byte_type* Strings() const noexcept { return _strings; }
  const byte_type* Payloads() const noexcept { return _payloads; }
  // Non-zero for a fixed-stride block: entry `i`'s key is the block prefix
  // followed by the `stride` bytes at `Strings() + i * stride`.
  uint32_t Stride() const noexcept { return _stride; }
  const byte_type* Prefix() const noexcept { return _prefix; }
  uint32_t PrefixLen() const noexcept { return _prefix_len; }
  uint32_t RestartString(uint32_t i) const noexcept {
    SDB_ASSERT(_stride == 0);
    return ReadRestart(i * kRestartEntrySize);
  }
  uint32_t RestartPayload(uint32_t i) const noexcept {
    return ReadRestart(i * _restart_size + _pay_offset);
  }

  // Full key stored at restart point `i`, always raw.
  bytes_view RestartKey(uint32_t i) const noexcept {
    const byte_type* p = _strings + RestartString(i);
    uint32_t lcp = 0;
    uint32_t size = 0;
    ReadEntryHeader(p, lcp, size);
    SDB_ASSERT(lcp == 0);
    return {p, size};
  }

 private:
  uint32_t ReadRestart(size_t offset) const noexcept {
    uint16_t value;
    std::memcpy(&value, _restarts + offset, sizeof(value));
    return value;
  }

  void Parse(const byte_type* p) {
    _entry_count = vread<uint32_t>(p);
    const uint32_t restart_count = vread<uint32_t>(p);
    const uint32_t string_size = vread<uint32_t>(p);
    [[maybe_unused]] const uint32_t payload_size = vread<uint32_t>(p);
    const uint32_t stride_tag = vread<uint32_t>(p);
    SDB_ENSURE(restart_count != 0 && restart_count <= kMaxRestarts,
               "term_dict: block declares ", restart_count, " restart points");
    _restart_count = restart_count;
    _stride = stride_tag == 0 ? 0 : stride_tag - 1;
    if (_stride != 0) {
      _prefix_len = vread<uint32_t>(p);
      _prefix = p;
      p += _prefix_len;
      _restart_size = sizeof(uint16_t);
      _pay_offset = 0;
    } else {
      _prefix = nullptr;
      _prefix_len = 0;
      _restart_size = kRestartEntrySize;
      _pay_offset = sizeof(uint16_t);
    }
    _restarts = p;
    _strings = p + restart_count * _restart_size;
    _payloads = _strings + string_size;
  }

  bstring _buf;
  const byte_type* _restarts{};
  const byte_type* _strings{};
  const byte_type* _payloads{};
  const byte_type* _prefix{};
  uint32_t _prefix_len{};
  uint32_t _stride{};
  uint32_t _restart_size{kRestartEntrySize};
  uint32_t _pay_offset{sizeof(uint16_t)};
  uint32_t _entry_count{};
  uint32_t _restart_count{};
};

// Everything a field needs at read time. The block directory, the separator
// array and the derived binary-search root are decoded at prepare, from the
// bytes just below the field header, so a first use never reads on its own.
struct FieldState {
  FieldMeta meta;
  FieldHeader header;
  FsstDecoder fsst;
  // The field's slice of the `.idx` body: `[body_start, body_offset)` holds the
  // leaf blocks, the term-meta section and the separators, and `body_offset` is
  // where the header that describes them begins.
  uint64_t body_start{};
  uint64_t body_offset{};
  uint64_t term_count{};
  uint64_t doc_count{};
  uint64_t total_term_freq{};
  bool has_wand{false};

  // `Single` only: the one term's posting lists, decoded once at prepare.
  // Its term is `header.min_term`, and `single_end` is where its `.doc`
  // bytes stop -- the parse chain's answer, kept for the merge transplant.
  TermCookie single;
  uint64_t single_end{0};

  duckdb::AllocatedData separators;
  duckdb::AllocatedData separator_index;
  duckdb::AllocatedData block_offsets;
  // Ordinal of each block's first term, `block_count + 1` entries with the
  // field's term count as the sentinel. Blocks are capped by bytes, so this
  // is what makes an equal-terms cut of the term space exact.
  duckdb::AllocatedData block_ordinals;
  uint64_t resident_bytes{0};

  uint32_t RestartInterval() const noexcept { return header.restart_interval; }
  uint32_t BlockCount() const noexcept { return header.block_count; }
  bool Single() const noexcept { return header.layout == LayoutKind::Single; }
  // A field of more than one row group: every term's records say which row
  // group each belongs to. Otherwise there is exactly one record per term, at
  // row group 0, and the format spends nothing saying so.
  bool Partitioned() const noexcept { return header.Partitioned(); }

  RowGroupLayout Layout() const noexcept { return HeaderLayout(header); }

  const uint64_t* BlockOffsets() const noexcept {
    return reinterpret_cast<const uint64_t*>(block_offsets.get());
  }

  uint64_t BlockOffset(uint32_t i) const noexcept { return BlockOffsets()[i]; }
  uint64_t BlockSize(uint32_t i) const noexcept {
    return BlockOffsets()[i + 1] - BlockOffsets()[i];
  }

  uint64_t BlockOrdinal(uint32_t i) const noexcept {
    return reinterpret_cast<const uint64_t*>(block_ordinals.get())[i];
  }

  bytes_view Separator(uint32_t i) const noexcept {
    const auto* index =
      reinterpret_cast<const uint32_t*>(separator_index.get());
    return {separators.get() + index[i], index[i + 1] - index[i]};
  }

  // Same answer as `FindBlock` for a target that is not below the one `from`
  // was chosen for: an exponential probe forward instead of a binary search
  // over the whole separator array, so a sorted probe stream pays a
  // comparison or two per block boundary rather than log(block_count) each.
  uint32_t FindBlockFrom(uint32_t from, bytes_view target) const noexcept {
    const uint32_t last = header.block_count - 1;
    if (from >= last || target < Separator(from)) {
      return from;
    }
    uint32_t lo = from + 1;
    uint32_t step = 1;
    while (lo + step <= last && Separator(lo + step - 1) <= target) {
      lo += step;
      step *= 2;
    }
    uint32_t hi = lo + step <= last ? lo + step : last;
    while (lo < hi) {
      const uint32_t mid = lo + (hi - lo + 1) / 2;
      if (Separator(mid - 1) <= target) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  // Index of the only block that may hold `target`.
  uint32_t FindBlock(bytes_view target) const noexcept {
    uint32_t lo = 0;
    uint32_t hi = header.block_count - 1;
    while (lo < hi) {
      const uint32_t mid = lo + (hi - lo + 1) / 2;
      if (Separator(mid - 1) <= target) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  // Longest prefix shared by every key of block `i`. The block's keys lie
  // between the two bounds below, so whatever those two agree on every key of
  // the block opens with. The first block has no separator beneath it and is
  // bounded by the field minimum instead.
  bytes_view BlockPrefix(uint32_t i) const noexcept {
    const auto lower = i == 0 ? bytes_view{header.min_term} : Separator(i - 1);
    const auto upper =
      i + 1 < header.block_count ? Separator(i) : bytes_view{header.max_term};
    return {lower.data(), CommonPrefix(lower, upper)};
  }
};

// One term's posting list inside `rg`, in that row group's local ids. The
// postings reader reads the term state during `Prepare` only, so the stack
// cookie holding the row group's meta outlives every use of it.
DocIterator::ptr RowGroupPostingsOf(PostingsReader& pr, const FieldState& field,
                                    const TermCookie& cookie,
                                    IndexFeatures features, uint32_t rg) {
  const auto* run = cookie.Find(rg);
  if (!run) {
    return {};
  }
  return pr.Iterator(field.meta.index_features, features,
                     {.meta = {cookie, *run}},
                     IteratorFieldOptions{field.has_wand});
}

// Copies `n` bytes while staying inside both ranges, so it needs no slack at
// either end. A libc `memmove` call for the handful of bytes a front-coded
// suffix holds costs more than the copy itself.
IRS_FORCE_INLINE void CopySuffix(byte_type* dst, const byte_type* src,
                                 size_t n) noexcept {
  if (n >= 32) [[unlikely]] {
    std::memcpy(dst, src, n);
  } else if (n >= 16) {
    std::memcpy(dst, src, 16);
    std::memcpy(dst + n - 16, src + n - 16, 16);
  } else if (n >= 8) {
    std::memcpy(dst, src, 8);
    std::memcpy(dst + n - 8, src + n - 8, 8);
  } else if (n >= 4) {
    std::memcpy(dst, src, 4);
    std::memcpy(dst + n - 4, src + n - 4, 4);
  } else if (n != 0) {
    dst[0] = src[0];
    dst[n >> 1] = src[n >> 1];
    dst[n - 1] = src[n - 1];
  }
}

// The reconstructed key. Its buffer is a high-water mark and its length an
// integer beside it, so a front-coded step neither truncates the string nor
// grows it: it writes the suffix delta over whatever the buffer held and moves
// the length. `std::basic_string`'s own size ops cost two out-of-line calls
// per entry, one of them branching on grow-versus-shrink, which for keys of
// varying length mispredicts on every second entry.
class KeyBuffer {
 public:
  bytes_view View() const noexcept { return {_buf.data(), _len}; }
  size_t size() const noexcept { return _len; }
  const byte_type* data() const noexcept { return _buf.data(); }
  byte_type operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _len);
    return _buf[i];
  }

  // Writable room for `len` bytes, keeping what the buffer already holds.
  byte_type* Grow(size_t len) {
    if (_buf.size() < len) [[unlikely]] {
      sdb::basics::StrResize(_buf, std::max(len, 2 * _buf.size()));
    }
    return _buf.data();
  }

  void SetSize(size_t len) noexcept {
    SDB_ASSERT(len <= _buf.size());
    _len = len;
  }

 private:
  bstring _buf;
  size_t _len{0};
};

// Cursor over the entries of one block. Keeps the reconstructed key and
// decodes the payload lazily, at most one restart group at a time.
class BlockCursor {
 public:
  explicit BlockCursor(const FieldState& field) noexcept
    : _field{&field},
      _cols{field.meta.index_features, field.header.RowGroupCount(),
            field.header.row_group_size},
      _doc_base{field.header.doc_base},
      _interval{field.RestartInterval()},
      _partitioned{field.Partitioned()},
      _fsst{field.fsst.Enabled()} {}

  void Load(IndexInput& in, uint32_t block) {
    _block.Load(in, _field->BlockOffset(block), _field->BlockSize(block));
    Rewind();
  }

  bool Next(KeyBuffer& key) {
    if (_next >= _entry_count) {
      return false;
    }
    if (Fixed()) {
      FixedEntry(key, _next);
      return true;
    }
    DecodeEntry(key);
    return true;
  }

  // Positions on the first entry whose key is not less than `target`.
  SeekResult ScanTo(bytes_view target, KeyBuffer& key) {
    if (Fixed()) {
      return FixedScanTo(target, key);
    }
    uint32_t lo = 0;
    uint32_t hi = _block.RestartCount() - 1;
    while (lo < hi) {
      const uint32_t mid = lo + (hi - lo + 1) / 2;
      if (_block.RestartKey(mid) <= target) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }

    // The cursor already sits inside the restart group that holds the answer,
    // so rewinding to its restart point would re-walk entries it has passed.
    if (Positioned() && _cur >= lo * _interval) {
      size_t matched = 0;
      const int cmp = CompareFull(key.View(), target, matched);
      if (cmp <= 0) {
        if (cmp != 0) {
          return ScanUp(target, key, matched);
        }
        _matched = matched;
        return SeekResult::Found;
      }
    }

    _str_ptr = _block.Strings() + _block.RestartString(lo);
    _next = lo * _interval;
    _to_restart = 0;
    DecodeEntry(key);
    return ScanForward(target, key);
  }

  // Same, resuming from the entry the cursor already sits on. Valid only
  // while that entry is not greater than `target`.
  SeekResult ScanForward(bytes_view target, KeyBuffer& key) {
    SDB_ASSERT(Positioned());
    if (Fixed()) {
      return FixedScanForward(target, key);
    }
    size_t matched = 0;
    const int cmp = CompareFull(key.View(), target, matched);
    if (cmp >= 0) {
      _matched = matched;
      return cmp == 0 ? SeekResult::Found : SeekResult::NotFound;
    }
    return ScanUp(target, key, matched);
  }

  // Front-coded scan that never compares a key from its first byte. Invariant
  // on entry and after every step: the cursor sits below `target` and shares
  // exactly `matched` leading bytes with it, so a front-coded entry's own
  // coding length classifies it against `target` without touching its suffix:
  //   lcp > matched -- agrees with the predecessor past the point where the
  //                    predecessor already fell below `target`, so it is below
  //                    `target` too (skip, zero comparison bytes);
  //   lcp < matched -- its first divergent byte sits above `target`'s (stop);
  //   lcp == matched - the only case that compares, and only the tail.
  // Restart entries carry a forced lcp of 0 rather than their true agreement
  // with the predecessor, so they fall back to one full comparison.
  SeekResult ScanUp(bytes_view target, KeyBuffer& key, size_t matched) {
    SDB_ASSERT(matched < target.size());
    while (_next < _entry_count) {
      const bool restart = _to_restart == 0;
      const byte_type* stored = _str_ptr;
      uint32_t lcp = 0;
      uint32_t size = 0;
      ReadEntryHeader(stored, lcp, size);

      // A skipped entry's key is never needed: it agrees with `target` on
      // exactly `matched` leading bytes like every other entry of the run, so
      // the entry the scan stops on -- whose own lcp is at most `matched` --
      // rebuilds its prefix from what `key` still holds below `matched`.
      // Decoding the suffix of a skipped entry (a string resize, an append and,
      // under FSST, a decompress) is the cost this scan exists to avoid.
      if (!restart && lcp > matched) {
        SkipEntry(stored, lcp, size);
        continue;
      }
      AppendEntry(key, stored, lcp, size);

      if (restart) {
        const int cmp = CompareFull(key.View(), target, matched);
        if (cmp >= 0) {
          _matched = matched;
          return cmp == 0 ? SeekResult::Found : SeekResult::NotFound;
        }
        continue;
      }
      if (lcp < matched) {
        // The predecessor agreed with `target` past `lcp` and this entry
        // diverges from the predecessor at `lcp`, so `lcp` is exactly this
        // key's agreement with `target`.
        _matched = lcp;
        return SeekResult::NotFound;
      }
      const size_t limit = std::min(key.size(), target.size());
      const size_t i =
        matched + CommonPrefix({key.data() + matched, limit - matched},
                               {target.data() + matched, limit - matched});
      if (i != limit) {
        if (key[i] > target[i]) {
          _matched = i;
          return SeekResult::NotFound;
        }
      } else if (key.size() == target.size()) {
        _matched = i;
        return SeekResult::Found;
      } else if (key.size() > target.size()) {
        _matched = i;
        return SeekResult::NotFound;
      }
      matched = i;
    }
    return SeekResult::End;
  }

  bool Positioned() const noexcept { return _cur != kNoEntry; }

  // Decodes the positioned entry's record into `cookie`: under `Full` the
  // stats, the term bound and every run; otherwise the stats alone, which the
  // walk over the entry decodes anyway. The payload walk to reach it advances
  // the restart group's chains without materializing anything. A cookie filled
  // with stats only is upgraded in place by the next `Full` caller -- the
  // entry's payload start and the chain before it are kept for exactly that.
  template<bool Full>
  void LoadPayload(TermCookie& cookie) {
    SDB_ASSERT(_cur != kNoEntry);
    constexpr ParseMode kMode = Full ? ParseMode::Full : ParseMode::Stats;
    if (_loaded != _cur) {
      const uint32_t interval = _interval;
      const uint32_t group = _cur / interval;
      uint32_t begin;
      if (_loaded != kNoEntry && _loaded < _cur &&
          _loaded / interval == group) {
        begin = _loaded + 1;
      } else {
        _pay_ptr = _block.Payloads() + _block.RestartPayload(group);
        _chain.clear();
        begin = group * interval;
      }
      for (uint32_t e = begin; e != _cur; ++e) {
        _pay_ptr += ParseEntry<ParseMode::Pass>(_pay_ptr, _cols, _partitioned,
                                                e % interval == 0, _doc_base,
                                                _chain, nullptr);
      }
      _entry_ptr = _pay_ptr;
      _entry_chain = _chain;
      _entry_restart = _cur % interval == 0;
      _pay_ptr += ParseEntry<kMode>(_pay_ptr, _cols, _partitioned,
                                    _entry_restart, _doc_base, _chain, &cookie);
      _loaded = _cur;
      _filled = &cookie;
      _filled_runs = Full;
      return;
    }
    if (_filled == &cookie && (!Full || _filled_runs)) {
      return;
    }
    EntryChain chain = _entry_chain;
    ParseEntry<kMode>(_entry_ptr, _cols, _partitioned, _entry_restart,
                      _doc_base, chain, &cookie);
    _filled = &cookie;
    _filled_runs = Full;
  }

  uint32_t Lcp() const noexcept { return _lcp; }
  bool Done() const noexcept { return _next >= _entry_count; }

  // Absolute `.doc` position after the decoded entry's last run. Terms hold
  // the field's `.doc` region gapless in dict order, so this is also where
  // the next term's bytes start -- the one extent the entry's own run offsets
  // cannot spell. Valid once `LoadPayload` decoded the positioned entry: the
  // chain is left exactly past it until the cursor moves.
  uint64_t DocEnd() const noexcept {
    SDB_ASSERT(_loaded == _cur);
    return _chain.doc_start;
  }

  // Every key byte the block stores, exactly as it stores it: front-coded
  // entries, each an lcp/length header followed by its suffix, or the flat
  // stride array of a fixed-stride block.
  bytes_view Strings() const noexcept {
    return {_block.Strings(),
            static_cast<size_t>(_block.Payloads() - _block.Strings())};
  }

  // The prefix a fixed-stride block strips from every stored key, empty for a
  // front-coded block.
  bytes_view Prefix() const noexcept { return {_prefix, _prefix_len}; }

  // Non-zero for a fixed-stride block, whose stored area holds no headers.
  uint32_t Stride() const noexcept { return _stride; }

  // Bytes the positioned key shares with the target of the seek that settled
  // on it, which every scan above computes on its way to the answer. Valid
  // only for a seek that settled inside the block, so anything but `End`.
  size_t Matched() const noexcept { return _matched; }

 private:
  bool Fixed() const noexcept { return _stride != 0; }

  // Where `target` sits relative to the block's shared prefix. `Inside` is the
  // only case the stride array can answer, and it hands back the part of the
  // target the array actually stores.
  enum class PrefixRel : uint8_t {
    Below,
    Inside,
    Above,
  };

  PrefixRel FixedPrefix(bytes_view target, bytes_view& tail) const noexcept {
    const size_t plen = _prefix_len;
    if (plen != 0) {
      const size_t n = std::min<size_t>(plen, target.size());
      const int cmp = n == 0 ? 0 : std::memcmp(_prefix, target.data(), n);
      if (cmp > 0 || (cmp == 0 && target.size() < plen)) {
        return PrefixRel::Below;
      }
      if (cmp < 0) {
        return PrefixRel::Above;
      }
    }
    tail = {target.data() + plen, target.size() - plen};
    return PrefixRel::Inside;
  }

  int FixedCmp(uint32_t i, bytes_view tail) const noexcept {
    const auto* stored = _str_ptr + size_t{i} * _stride;
    const size_t n = std::min<size_t>(_stride, tail.size());
    const int cmp = n == 0 ? 0 : std::memcmp(stored, tail.data(), n);
    if (cmp != 0) {
      return cmp;
    }
    if (_stride == tail.size()) {
      return 0;
    }
    return _stride < tail.size() ? -1 : 1;
  }

  // First entry of `[lo, hi)` that is not below `tail`, and how it compares --
  // the search already knows, so settling on it costs no second comparison.
  uint32_t FixedLowerBound(uint32_t lo, uint32_t hi, bytes_view tail,
                           int& cmp) const noexcept {
    cmp = 1;
    while (lo < hi) {
      const uint32_t mid = lo + (hi - lo) / 2;
      const int mid_cmp = FixedCmp(mid, tail);
      if (mid_cmp < 0) {
        lo = mid + 1;
      } else {
        hi = mid;
        cmp = mid_cmp;
      }
    }
    return lo;
  }

  // The block's shared prefix is written into the key once and then left
  // alone: within a block only the stride moves, so a whole-key copy per entry
  // and the entry-header varint the front-coded walk pays are both absent.
  void FixedEntry(KeyBuffer& key, uint32_t i) {
    const size_t size = size_t{_prefix_len} + _stride;
    auto* out = key.Grow(size);
    if (!_prefix_ready) {
      std::memcpy(out, _prefix, _prefix_len);
      _prefix_ready = true;
    }
    CopySuffix(out + _prefix_len, _str_ptr + size_t{i} * _stride, _stride);
    key.SetSize(size);
    _cur = i;
    _next = i + 1;
  }

  // Leaves the cursor on the block's last entry, which is what the front-coded
  // walk leaves behind when it runs off the end.
  SeekResult FixedEnd(KeyBuffer& key) {
    FixedEntry(key, _entry_count - 1);
    _next = _entry_count;
    return SeekResult::End;
  }

  SeekResult FixedSettle(KeyBuffer& key, uint32_t entry, int cmp) {
    if (entry >= _entry_count) {
      return FixedEnd(key);
    }
    FixedEntry(key, entry);
    return cmp == 0 ? SeekResult::Found : SeekResult::NotFound;
  }

  // Galloping lower bound from an entry already known to be below `tail`, so a
  // sorted probe stream pays a comparison or two per step rather than a whole
  // binary search of the block.
  SeekResult FixedGallop(uint32_t from, bytes_view tail, KeyBuffer& key) {
    uint32_t lo = from;
    uint32_t step = 1;
    while (lo + step < _entry_count && FixedCmp(lo + step, tail) < 0) {
      lo += step;
      step *= 2;
    }
    const uint32_t hi = std::min<uint32_t>(lo + step + 1, _entry_count);
    int cmp = 1;
    const uint32_t entry = FixedLowerBound(lo + 1, hi, tail, cmp);
    return FixedSettle(key, entry, cmp);
  }

  // The step a dense sorted probe stream takes: the answer is the very next
  // entry, so it is tried before any search is set up. This is what the
  // front-coded walk gets for free from its running `matched`.
  SeekResult FixedAdvance(bytes_view tail, KeyBuffer& key) {
    const uint32_t next = _cur + 1;
    if (next >= _entry_count) {
      return FixedEnd(key);
    }
    const int cmp = FixedCmp(next, tail);
    if (cmp >= 0) {
      return FixedSettle(key, next, cmp);
    }
    return FixedGallop(next, tail, key);
  }

  SeekResult FixedScanTo(bytes_view target, KeyBuffer& key) {
    bytes_view tail;
    switch (FixedPrefix(target, tail)) {
      case PrefixRel::Below:
        FixedEntry(key, 0);
        _matched = 0;
        return SeekResult::NotFound;
      case PrefixRel::Above:
        return FixedEnd(key);
      case PrefixRel::Inside:
        break;
    }
    // `Inside` means `target` opens with the block prefix, and so does every
    // key of the block; the stride comparisons below never look lower.
    _matched = _prefix_len;
    if (Positioned()) {
      const int cmp = FixedCmp(_cur, tail);
      if (cmp == 0) {
        return SeekResult::Found;
      }
      if (cmp < 0) {
        return FixedAdvance(tail, key);
      }
    }
    int cmp = 1;
    const uint32_t entry = FixedLowerBound(0, _entry_count, tail, cmp);
    return FixedSettle(key, entry, cmp);
  }

  // The positioned entry's key is already materialized whole, so one
  // comparison against it settles the block prefix and the entry together:
  // agreement below the prefix length is exactly FixedPrefix's Below (the key
  // is greater) or Above (it is smaller) verdict. That replaces the split
  // compare -- the prefix, then the stride -- the front-coded scan beside it
  // never pays.
  SeekResult FixedScanForward(bytes_view target, KeyBuffer& key) {
    size_t matched = 0;
    const int cmp = CompareFull(key.View(), target, matched);
    if (cmp >= 0) {
      _matched = matched < _prefix_len ? 0 : _prefix_len;
      return cmp == 0 ? SeekResult::Found : SeekResult::NotFound;
    }
    if (matched < _prefix_len) {
      return FixedEnd(key);
    }
    _matched = _prefix_len;
    return FixedAdvance(
      {target.data() + _prefix_len, target.size() - _prefix_len}, key);
  }

  void Rewind() noexcept {
    _entry_count = _block.EntryCount();
    _cur = kNoEntry;
    _next = 0;
    _to_restart = 0;
    _loaded = kNoEntry;
    _filled = nullptr;
    _filled_runs = false;
    _str_ptr = _block.Strings();
    _stride = _block.Stride();
    _prefix = _block.Prefix();
    _prefix_len = _block.PrefixLen();
    _prefix_ready = false;
    _matched = 0;
    // Every key of a fixed-stride block shares the block prefix, and nothing
    // below it is ever rewritten -- a sound lower bound for the automaton
    // walk's resume depth, and zero for a front-coded block whose first entry
    // is a restart point.
    _lcp = _prefix_len;
  }

  void DecodeEntry(KeyBuffer& key) {
    const byte_type* stored = _str_ptr;
    uint32_t lcp = 0;
    uint32_t size = 0;
    ReadEntryHeader(stored, lcp, size);
    AppendEntry(key, stored, lcp, size);
  }

  void SkipEntry(const byte_type* stored, uint32_t lcp,
                 uint32_t size) noexcept {
    SDB_ASSERT(_to_restart != 0);
    --_to_restart;
    _str_ptr = stored + size;
    _lcp = lcp;
    _cur = _next;
    ++_next;
  }

  // The key already holds the retained prefix -- `lcp` never exceeds its
  // length -- so only the suffix delta is written, in place, with one
  // uninitialized resize instead of a truncate plus an append.
  void AppendEntry(KeyBuffer& key, const byte_type* stored, uint32_t lcp,
                   uint32_t size) {
    SDB_ASSERT(lcp <= key.size());
    if (_fsst && _to_restart != 0) {
      --_to_restart;
      auto* out = key.Grow(size_t{lcp} + kFsstMaxSymbol * size);
      key.SetSize(lcp + _field->fsst.DecodeInto({stored, size}, out + lcp));
    } else {
      _to_restart = _to_restart == 0 ? _interval - 1 : _to_restart - 1;
      auto* out = key.Grow(size_t{lcp} + size);
      CopySuffix(out + lcp, stored, size);
      key.SetSize(size_t{lcp} + size);
    }
    _str_ptr = stored + size;
    _lcp = lcp;
    _cur = _next;
    ++_next;
  }

  const FieldState* _field;
  BlockView _block;
  const byte_type* _str_ptr{};
  const byte_type* _prefix{};
  // The next undecoded entry's payload and the chain state at it.
  const byte_type* _pay_ptr{};
  EntryChain _chain;
  // The decoded entry `_loaded`: its payload start and the chain before it,
  // so a second cookie decodes without re-walking the group.
  const byte_type* _entry_ptr{};
  EntryChain _entry_chain;
  bool _entry_restart{false};
  // Whether the cookie below holds the entry's runs or only its statistics.
  bool _filled_runs{false};
  // The cookie that already holds the entry `_loaded` decoded.
  const TermCookie* _filled{};
  size_t _matched{0};
  uint32_t _entry_count{0};
  uint32_t _lcp{0};
  uint32_t _cur{kNoEntry};
  uint32_t _next{0};
  uint32_t _to_restart{0};
  uint32_t _loaded{kNoEntry};
  uint32_t _stride{0};
  uint32_t _prefix_len{0};
  bool _prefix_ready{false};
  // Field constants LoadPayload spells on every entry it decodes, so they are
  // taken once with the interval and the FSST flag above them.
  const RecordColumns _cols;
  const uint64_t _doc_base;
  const uint32_t _interval;
  const bool _partitioned;
  const bool _fsst;
};

// Shared state of every iterator: the field, the postings reader and a
// private `.idx` stream.
class IteratorBase {
 protected:
  IteratorBase(const FieldState& field, PostingsReader& pr,
               IndexInput::ptr&& in)
    : _field{&field}, _pr{&pr}, _in{std::move(in)}, _cursor{field} {
    SDB_ASSERT(_in);
  }

  void LoadBlock(uint32_t block) {
    SDB_ASSERT(!_field->Single());
    _cursor.Load(*_in, block);
    _block = block;
    _loaded = true;
  }

  // A block covers the keys in [separator(block - 1), separator(block)).
  bool BlockCovers(bytes_view target) const noexcept {
    if (!_loaded || _block >= _field->BlockCount()) {
      return false;
    }
    if (_block + 1 < _field->BlockCount() &&
        _field->Separator(_block) <= target) {
      return false;
    }
    return _block == 0 || _field->Separator(_block - 1) <= target;
  }

  // Positions on the first entry whose key is not less than `target`, crossing
  // blocks as needed, and reports how far the landed key agrees with `target`.
  // False once the field is exhausted.
  bool SeekGeEntry(bytes_view target, KeyBuffer& key, size_t& matched) {
    matched = 0;
    if (bytes_view{_field->header.max_term} < target) {
      _block = _field->BlockCount();
      _loaded = true;
      return false;
    }
    // Two separator comparisons where the binary search would otherwise run:
    // the leapfrog's bound almost always stays inside the block it was
    // computed in.
    if (!BlockCovers(target)) {
      LoadBlock(_field->FindBlock(target));
    }
    if (_cursor.ScanTo(target, key) != SeekResult::End) {
      matched = _cursor.Matched();
      return true;
    }
    while (_block + 1 < _field->BlockCount()) {
      LoadBlock(_block + 1);
      if (_cursor.Next(key)) {
        return true;
      }
    }
    return false;
  }

  const FieldState* _field;
  PostingsReader* _pr;
  IndexInput::ptr _in;
  mutable BlockCursor _cursor;
  uint32_t _block{kNoEntry};
  bool _loaded{false};
};

class SeekIteratorImpl : public SeekTermIterator, protected IteratorBase {
 public:
  SeekIteratorImpl(const FieldState& field, PostingsReader& pr,
                   IndexInput::ptr&& in)
    : IteratorBase{field, pr, std::move(in)} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<TermMeta>::id()) {
      return &_meta.stats;
    }
    return type == irs::Type<TermAttr>::id() ? &_value : nullptr;
  }

  bytes_view value() const noexcept final { return _value.value; }

  bool next() override { return NextImpl<false>({}); }

  SeekResult seek_ge(bytes_view target) override {
    if (bytes_view{_field->header.max_term} < target) {
      _value.value = {};
      return SeekResult::End;
    }
    const auto res = SeekInBlock(target);
    if (res != SeekResult::End) {
      return res;
    }
    return next() ? SeekResult::NotFound : SeekResult::End;
  }

  bool seek(bytes_view target) override {
    if (bytes_view{_field->header.min_term} > target ||
        bytes_view{_field->header.max_term} < target) {
      return false;
    }
    return SeekResult::Found == SeekInBlock(target);
  }

  void read() final { _cursor.LoadPayload<false>(_meta); }

  DocIterator::ptr RowGroupPostings(IndexFeatures features,
                                    uint32_t rg) const final {
    _cursor.LoadPayload<true>(_meta);
    return RowGroupPostingsOf(*_pr, *_field, _meta, features, rg);
  }

  std::span<const TermRowGroup> RowGroups() const final {
    _cursor.LoadPayload<true>(_meta);
    return _meta.RowGroups();
  }

  DocIterator::ptr RowGroupPostings(IndexFeatures features, uint32_t rg,
                                    DocIterator::ptr&& reuse) const final {
    _cursor.LoadPayload<true>(_meta);
    const auto* run = _meta.Find(rg);
    if (!run) {
      return {};
    }
    return _pr->ReuseIterator(std::move(reuse), _field->meta.index_features,
                              features, {.meta = {_meta, *run}},
                              IteratorFieldOptions{_field->has_wand});
  }

  Extents TermExtents() const final {
    _cursor.LoadPayload<true>(_meta);
    return {.cookie = &_meta, .doc_end = _cursor.DocEnd()};
  }

  TermCookie cookie() const final {
    _cursor.LoadPayload<true>(_meta);
    return _meta;
  }

 protected:
  // The forward walk. `Bounded` folds a range's upper bound into it: the block
  // whose separator already sits at or past `hi` holds nothing the range wants
  // and is never read.
  template<bool Bounded>
  bool NextImpl(bytes_view hi) {
    if (!_loaded) {
      LoadBlock(0);
    } else if (_cursor.Done()) {
      if (_block + 1 >= _field->BlockCount() ||
          (Bounded && hi <= _field->Separator(_block))) {
        _value.value = {};
        return false;
      }
      LoadBlock(_block + 1);
    }
    if (!_cursor.Next(_key) || (Bounded && _key.View() >= hi)) {
      _value.value = {};
      return false;
    }
    _value.value = _key.View();
    return true;
  }

  // Exact seek of a probe that is never below the previous one. Monotonicity
  // pays twice: the block search gallops forward from the loaded block instead
  // of binary-searching the whole separator array, and the in-block scan can
  // resume on the cursor unconditionally -- `ScanForward` classifies a cursor
  // that overshot on its own, so the extra full comparison `SeekInBlock` needs
  // to decide between resuming and restarting is not paid at all.
  bool SeekAscending(bytes_view target) {
    SeekResult res;
    if (!_loaded) {
      if (bytes_view{_field->header.min_term} > target ||
          bytes_view{_field->header.max_term} < target) {
        _value.value = {};
        return false;
      }
      LoadBlock(_field->FindBlock(target));
      res = _cursor.ScanTo(target, _key);
    } else if (bytes_view{_field->header.max_term} < target) {
      _value.value = {};
      return false;
    } else {
      const uint32_t block = _field->FindBlockFrom(_block, target);
      if (block != _block) {
        LoadBlock(block);
        res = _cursor.ScanTo(target, _key);
      } else {
        res = _cursor.Positioned() ? _cursor.ScanForward(target, _key)
                                   : _cursor.ScanTo(target, _key);
      }
    }
    const bool found = res == SeekResult::Found;
    _value.value = found ? _key.View() : bytes_view{};
    return found;
  }

  mutable TermCookie _meta;
  TermAttr _value;
  KeyBuffer _key;

 private:
  SeekResult SeekInBlock(bytes_view target) {
    SeekResult res;
    if (BlockCovers(target)) {
      res = _cursor.Positioned() && _key.View() <= target
              ? _cursor.ScanForward(target, _key)
              : _cursor.ScanTo(target, _key);
    } else {
      LoadBlock(_field->FindBlock(target));
      res = _cursor.ScanTo(target, _key);
    }
    _value.value = res == SeekResult::End ? bytes_view{} : _key.View();
    return res;
  }
};

// One term range of a field: the walk starts at the first term not below `lo`
// and stops before `hi`, so several of them enumerate a dictionary between
// them and emit exactly what one full walk would.
class RangeIteratorImpl : public SeekIteratorImpl {
 public:
  RangeIteratorImpl(const FieldState& field, PostingsReader& pr,
                    IndexInput::ptr&& in, const TermRange& range)
    : SeekIteratorImpl{field, pr, std::move(in)},
      _lo{range.lo},
      _hi{range.hi} {}

  bool next() final {
    if (_done) {
      return false;
    }
    const bool found = [&] {
      if (_started) {
        return Step();
      }
      _started = true;
      if (_lo.empty()) {
        return Step();
      }
      // A field whose terms all sit below `lo` leaves the walk unpositioned,
      // so the end of a range is latched rather than recomputed.
      return SeekIteratorImpl::seek_ge(_lo) != SeekResult::End &&
             (_hi.empty() || _value.value < _hi);
    }();
    if (!found) {
      _done = true;
      _value.value = {};
    }
    return found;
  }

  // A probe below the range settles on its first term, one past it ends the
  // walk -- so a consumer that composes a range with a further predicate
  // (`FilteredSeekTermIterator`) gets the range's own bound, not the field's.
  SeekResult seek_ge(bytes_view target) final {
    if (_done) {
      return SeekResult::End;
    }
    _started = true;
    const auto from = std::max(target, _lo);
    if (SeekIteratorImpl::seek_ge(from) == SeekResult::End ||
        (!_hi.empty() && _value.value >= _hi)) {
      _done = true;
      _value.value = {};
      return SeekResult::End;
    }
    return _value.value == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return SeekResult::Found == seek_ge(target);
  }

 private:
  // The last range of a field runs to its end, and an unbounded walk is the
  // one that costs nothing per term.
  bool Step() {
    return _hi.empty() ? NextImpl<false>({}) : NextImpl<true>(_hi);
  }

  bytes_view _lo;
  bytes_view _hi;
  bool _started{false};
  bool _done{false};
};

// A sorted probe set resolved in one forward pass: the dictionary is walked
// once, and every probe resumes where the previous one settled.
class BatchIteratorImpl : public SeekIteratorImpl {
 public:
  BatchIteratorImpl(const FieldState& field, PostingsReader& pr,
                    IndexInput::ptr&& in, std::span<const bytes_view> terms)
    : SeekIteratorImpl{field, pr, std::move(in)}, _terms{terms} {}

  bool next() final {
    while (_next != _terms.size()) {
      const auto target = _terms[_next++];
      SDB_ASSERT(_next == 1 || _terms[_next - 2] < target);
      if (SeekAscending(target)) {
        return true;
      }
    }
    _value.value = {};
    return false;
  }

 private:
  std::span<const bytes_view> _terms;
  size_t _next{0};
};

// Exact single seeks only: never copies the term value.
class SingleIteratorImpl : public SeekTermIterator, private IteratorBase {
 public:
  SingleIteratorImpl(const FieldState& field, PostingsReader& pr,
                     IndexInput::ptr&& in)
    : IteratorBase{field, pr, std::move(in)} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<TermMeta>::id()) {
      return &_meta.stats;
    }
    return type == irs::Type<TermAttr>::id() ? &_value : nullptr;
  }

  bytes_view value() const noexcept final { return _value.value; }

  bool next() final { return false; }

  SeekResult seek_ge(bytes_view) final { throw NotSupported(); }

  bool seek(bytes_view target) final {
    if (bytes_view{_field->header.min_term} > target ||
        bytes_view{_field->header.max_term} < target) {
      _value = {};
      return false;
    }
    LoadBlock(_field->FindBlock(target));
    if (SeekResult::Found != _cursor.ScanTo(target, _key)) {
      _value = {};
      return false;
    }
    _cursor.LoadPayload<true>(_meta);
    _value.value = target;
    return true;
  }

  void read() final {}

  DocIterator::ptr RowGroupPostings(IndexFeatures features,
                                    uint32_t rg) const final {
    if (_meta.rgs.empty()) {
      return {};
    }
    return RowGroupPostingsOf(*_pr, *_field, _meta, features, rg);
  }

  TermCookie cookie() const final { return Cookie(); }

  std::span<const TermRowGroup> RowGroups() const final {
    return _meta.RowGroups();
  }

  const TermCookie& Cookie() const noexcept { return _meta; }

  const TermMeta& Meta() const noexcept { return _meta.stats; }

 private:
  mutable TermCookie _meta;
  TermAttr _value;
  KeyBuffer _key;
};

#ifdef SDB_DEV
// Leaf blocks the automaton walk has settled either way, so a dev-build test
// can pin which path a pattern takes. Debug-only: nothing in the system reads
// them.
std::atomic_size_t kDecidedBlocks{0};
std::atomic_size_t kWalkedBlocks{0};
#endif

// Automaton intersection by leapfrog: the acceptor state stack is kept over
// every way the key can change, so no prefix is ever stepped twice -- a
// front-coded step resumes at the lcp depth, a block change resumes at the
// prefix all the block's keys share, and a leapfrog seek resumes at the depth
// the landed key agrees with the bound it was sought with. A rejected key does
// not cost a scan to the next candidate either: the deepest state that still
// has a transition above the rejected byte yields a lower bound that is jumped
// to with a block-level seek. Whole blocks are skipped when the prefix all
// their keys share is already dead, and a whole block is decided in one pass
// over its stored bytes when the state that prefix reaches self-loops on every
// one of them.
//
// The acceptor is borrowed: it is immutable, it carries the tables a step
// reads, and the filter that compiled it outlives every walk it drives.
template<typename Acceptor>
class AutomatonIteratorImpl : public SeekTermIterator, private IteratorBase {
 public:
  using State = typename Acceptor::State;

  AutomatonIteratorImpl(const FieldState& field, PostingsReader& pr,
                        IndexInput::ptr&& in, const Acceptor& acceptor)
    : IteratorBase{field, pr, std::move(in)},
      _acceptor{acceptor},
      _start{acceptor.Start()},
      _fsst{field.fsst.Enabled()} {
    _payload.value = {reinterpret_cast<const byte_type*>(&_payload_value),
                      sizeof(_payload_value)};
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<TermMeta>::id()) {
      return &_meta.stats;
    }
    if (type == irs::Type<TermAttr>::id()) {
      return &_value;
    }
    return type == irs::Type<PayAttr>::id() ? &_payload : nullptr;
  }

  bytes_view value() const noexcept final { return _value.value; }

  bool next() final {
    if (_done) {
      return false;
    }
    if (!_loaded) {
      if (!NextBlock(0)) {
        return Stop();
      }
    } else if (!Advance()) {
      return Stop();
    }

    for (;;) {
      if (_batched) {
        // The block scan already decided every key of this block, so nothing
        // below the shared prefix is stepped and nothing can be rejected.
        _value.value = _key.View();
        return true;
      }
      const auto step = Step();
      if (step.accepted) {
        _value.value = _key.View();
        return true;
      }
      if (step.consumed) {
        if (!Advance()) {
          return Stop();
        }
        continue;
      }
      if (!Leapfrog(step.depth)) {
        return Stop();
      }
    }
  }

  // Same contract as the burst trie's automaton iterator, down to what an
  // empty probe reports on a fresh iterator: `next()` already leapfrogs, so
  // driving it forward is the whole implementation.
  SeekResult seek_ge(bytes_view target) final {
    if (!irs::seek(*this, target)) {
      return SeekResult::End;
    }
    return value() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return SeekResult::Found == seek_ge(target);
  }

  void read() final { _cursor.LoadPayload<false>(_meta); }

  DocIterator::ptr RowGroupPostings(IndexFeatures features,
                                    uint32_t rg) const final {
    _cursor.LoadPayload<true>(_meta);
    return RowGroupPostingsOf(*_pr, *_field, _meta, features, rg);
  }

  std::span<const TermRowGroup> RowGroups() const final {
    _cursor.LoadPayload<true>(_meta);
    return _meta.RowGroups();
  }

  TermCookie cookie() const final {
    _cursor.LoadPayload<true>(_meta);
    return _meta;
  }

 private:
  struct StepResult {
    bool accepted;
    // The whole key was consumed on a live but non-accepting state, so no
    // string above it shares this key as a prefix.
    bool consumed;
    size_t depth;
  };

  bool Stop() {
    _done = true;
    _value.value = {};
    return false;
  }

  // Next entry in key order, skipping blocks the acceptor rejects whole.
  bool Advance() {
    if (_cursor.Next(_key)) {
      _depth = std::min<size_t>(_depth, _cursor.Lcp());
      return true;
    }
    return NextBlock(_block + 1);
  }

  // What selecting a block settled.
  enum class BlockSelect : uint8_t {
    // The block is loaded and positioned; its keys are walked, or handed over
    // whole when the scan already decided them.
    Take,
    // Nothing in the block matches, and the reason bounds nothing above it.
    Skip,
    // The prefix every key of the block shares died, which bounds every key
    // below the block the acceptor could still admit.
    Dead,
  };

  // First entry at or above `block` that a selected block holds. A dead shared
  // prefix does not cost a step of the next block: it rejects every key that
  // opens with it, and those keys can fill an unbounded run of blocks.
  bool NextBlock(uint32_t block) {
    const auto count = _field->BlockCount();
    while (block < count) {
      const auto prefix = _field->BlockPrefix(block);
      size_t dead = 0;
      switch (SelectBlock(block, prefix, dead)) {
        case BlockSelect::Take:
          // The block's first key opens with the prefix `SelectBlock` stepped,
          // so its resume depth is that prefix rather than the entry's own lcp.
          if (_cursor.Next(_key)) {
            return true;
          }
          [[fallthrough]];
        case BlockSelect::Skip:
          ++block;
          break;
        case BlockSelect::Dead:
          block = SkipDead(block, prefix, dead);
          break;
      }
    }
    return false;
  }

  // Block to resume at once `block`'s shared prefix has died at `dead`. Every
  // key opening with that prefix is rejected, so the smallest key above them
  // the acceptor can still admit is the bound `Leapfrog` computes from any
  // rejected key -- and the separator array says which block holds it. A run
  // of dead blocks therefore costs one galloping separator search instead of
  // one prefix walk per block.
  uint32_t SkipDead(uint32_t block, bytes_view prefix, size_t dead) {
    for (size_t i = dead + 1; i-- != 0;) {
      uint32_t label = 0;
      if (!_acceptor.NextLabel(StateBefore(i), prefix[i], label)) {
        continue;
      }
      SDB_ASSERT(label > prefix[i] &&
                 label <= std::numeric_limits<byte_type>::max());
      _bound.assign(prefix.data(), i);
      _bound.push_back(static_cast<byte_type>(label));
      // The bound is above every key that opens with the shared prefix, and
      // the block's upper separator opens with it too, so the block found is
      // above this one -- except on the last block, where there is nothing
      // above and `FindBlockFrom` answers with the block it was given.
      const auto next = _field->FindBlockFrom(block, _bound);
      return next > block ? next : block + 1;
    }
    // No position of the prefix admits a larger byte, so no key above it can
    // match at all.
    return _field->BlockCount();
  }

  // Steps the prefix every key of `block` shares, which both decides the block
  // and leaves the walk resumable at that depth for the block's first key. An
  // acceptor whose run test costs a step per byte gains nothing from the block
  // scan and does not take it.
  BlockSelect SelectBlock(uint32_t block, bytes_view prefix, size_t& dead) {
    if (_states.size() < prefix.size()) {
      _states.resize(prefix.size());
    }
    auto state = _start;
    for (size_t i = 0; i != prefix.size(); ++i) {
      state = _acceptor.Step(state, prefix[i]);
      if (!_acceptor.Alive(state)) {
        dead = i;
        return BlockSelect::Dead;
      }
      _states[i] = state;
    }
    LoadBlock(block);
    _depth = prefix.size();
    auto verdict = BlockVerdict::Walk;
    if constexpr (Acceptor::kCheapRuns) {
      verdict = ScanBlock(state);
    }
#ifdef SDB_DEV
    (verdict == BlockVerdict::Walk ? kWalkedBlocks : kDecidedBlocks)
      .fetch_add(1, std::memory_order_relaxed);
#endif
    _batched = verdict == BlockVerdict::All;
    return verdict == BlockVerdict::None ? BlockSelect::Skip
                                         : BlockSelect::Take;
  }

  // What one pass over a block's stored bytes settles.
  enum class BlockVerdict : uint8_t {
    // Every key of the block ends on the same accepting state.
    All,
    // Every key of the block ends on the same state, and it does not accept.
    None,
    // The automaton moves somewhere inside the block; walk it key by key.
    Walk,
  };

  // Decides a whole block against `state`, the state its shared prefix
  // reaches, without reconstructing a single key.
  //
  // Every byte a key of the block holds above that prefix is a byte the block
  // stores: a front-coded entry copies the rest from its predecessor, and that
  // chain bottoms out at a restart entry, which stores its key whole. So a
  // state that self-loops on every stored byte is the state every key of the
  // block ends on, and `Accept` on it answers for all of them at once.
  //
  // The stored area holds more than the keys above the prefix -- restart
  // entries repeat the shared prefix, and a fixed-stride block keeps its own,
  // longer prefix aside -- but testing bytes the walk would have stepped
  // earlier can only decline a block, never admit one.
  BlockVerdict ScanBlock(State state) {
    const auto strings = _cursor.Strings();
    State moved{};
    if (_cursor.Stride() != 0) {
      // A fixed-stride block stores one flat array of equal-length keys, with
      // no entry headers to step over and no FSST: the whole block is one run.
      const auto prefix = _cursor.Prefix();
      if (_acceptor.StepRun(state, prefix.data(), prefix.size(), moved) !=
            prefix.size() ||
          _acceptor.StepRun(state, strings.data(), strings.size(), moved) !=
            strings.size()) {
        return BlockVerdict::Walk;
      }
    } else {
      const auto* p = strings.data();
      const auto* end = p + strings.size();
      const uint32_t interval = _field->RestartInterval();
      const uint8_t* codes = _fsst ? CodeStates(state) : nullptr;
      uint32_t to_restart = 0;
      while (p != end) {
        uint32_t lcp = 0;
        uint32_t size = 0;
        ReadEntryHeader(p, lcp, size);
        // A header is not a key byte, and under FSST neither is a suffix: only
        // a restart entry, which the writer leaves raw so a seek can compare
        // it, is stored as the bytes it stands for.
        const bool restart = to_restart == 0;
        to_restart = restart ? interval - 1 : to_restart - 1;
        if (codes && !restart) {
          uint8_t attention = 0;
          for (uint32_t i = 0; i != size; ++i) {
            attention |= codes[p[i]];
          }
          if (attention != 0 && !ProbeCodes(state, p, size)) {
            return BlockVerdict::Walk;
          }
        } else if (_acceptor.StepRun(state, p, size, moved) != size) {
          return BlockVerdict::Walk;
        }
        p += size;
      }
    }
    return _acceptor.Accept(state, _payload_value) ? BlockVerdict::All
                                                   : BlockVerdict::None;
  }

  // An FSST-compressed entry stores one byte per symbol, so its run is decided
  // by what the symbols expand to rather than by the acceptor's own bytes.
  // Every code starts unknown and is looked at only when a run actually holds
  // it: probing all 255 symbols up front costs more than the scan it serves
  // whenever the block is going to be declined on its first key.
  const uint8_t* CodeStates(State state) {
    if (!_codes_ready || _codes_state != state) {
      _codes.fill(kCodeUnknown);
      // The escape code stands for no symbol -- the byte it escapes is stored
      // beside it and the table says nothing about it -- so a run holding one
      // is always walked.
      _codes[FSST_ESC] = kCodeMoves;
      _codes_state = state;
      _codes_ready = true;
    }
    return _codes.data();
  }

  // Resolves the codes of a run the accumulator flagged, stopping at the first
  // one that moves the automaton.
  bool ProbeCodes(State state, const byte_type* p, size_t n) {
    State moved{};
    for (size_t i = 0; i != n; ++i) {
      const size_t code = p[i];
      if (_codes[code] == kCodeUnknown) {
        const auto symbol = _field->fsst.Symbol(code);
        _codes[code] = _acceptor.StepRun(state, symbol.data(), symbol.size(),
                                         moved) == symbol.size()
                         ? kCodeStays
                         : kCodeMoves;
      }
      if (_codes[code] != kCodeStays) {
        return false;
      }
    }
    return true;
  }

  const State& StateBefore(size_t depth) const noexcept {
    return depth == 0 ? _start : _states[depth - 1];
  }

  StepResult Step() {
    const auto size = _key.size();
    SDB_ASSERT(_depth <= size);
    if (_states.size() < size) {
      _states.resize(size);
    }
    for (size_t i = _depth; i != size; ++i) {
      auto state = _acceptor.Step(StateBefore(i), _key[i]);
      if (!_acceptor.Alive(state)) {
        _depth = i;
        return {.accepted = false, .consumed = false, .depth = i};
      }
      _states[i] = state;
    }
    _depth = size;
    if (!_acceptor.Accept(StateBefore(size), _payload_value)) {
      return {.accepted = false, .consumed = true, .depth = size};
    }
    return {.accepted = true, .consumed = false, .depth = 0};
  }

  // Jumps to the tightest lower bound above the rejected key: the deepest
  // prefix position that still admits a larger byte.
  bool Leapfrog(size_t depth) {
    SDB_ASSERT(depth < _key.size());
    for (size_t i = depth + 1; i-- != 0;) {
      uint32_t label = 0;
      if (!_acceptor.NextLabel(StateBefore(i), _key[i], label)) {
        continue;
      }
      // A label an acceptor cannot express as one byte would truncate into a
      // bound at or below the rejected key, and the walk would not advance.
      SDB_ASSERT(label > _key[i] &&
                 label <= std::numeric_limits<byte_type>::max());
      _bound.assign(_key.data(), i);
      _bound.push_back(static_cast<byte_type>(label));
      size_t matched = 0;
      if (!SeekGeEntry(_bound, _key, matched)) {
        return false;
      }
      // The bound repeats the rejected key below `i`, so agreement with it is
      // agreement with the states already stacked -- capped at `i`, past which
      // the bound holds the substituted label instead.
      _depth = std::min(matched, i);
      return true;
    }
    return false;
  }

  mutable TermCookie _meta;
  TermAttr _value;
  PayAttr _payload;
  KeyBuffer _key;
  bstring _bound;
  std::vector<State> _states;
  const Acceptor& _acceptor;
  State _start;
  // Zero, and it has to be: a run of codes is accumulated with `|=`, so only
  // "the automaton stays" may leave the accumulator clean.
  static constexpr uint8_t kCodeStays = 0;
  static constexpr uint8_t kCodeUnknown = 1;
  static constexpr uint8_t kCodeMoves = 2;

  // What each FSST code does to `_codes_state`, reset when that state changes.
  std::array<uint8_t, 256> _codes{};
  State _codes_state{};
  size_t _depth{0};
  typename Acceptor::PayloadType _payload_value{};
  bool _codes_ready{false};
  // The loaded block is decided: every key of it matches, so `next()` hands
  // them over without stepping one byte.
  bool _batched{false};
  const bool _fsst;
  bool _done{false};
};

}  // namespace
namespace {

using namespace irs;

// The whole read path of a field that has no dictionary: one term, whose bytes
// and whose posting lists both sit in the field header. Nothing is loaded, no
// block is read, and the `Var` paths stay free of a branch for it.
class SingleTermIterator : public SeekTermIterator {
 public:
  SingleTermIterator(const FieldState& field, PostingsReader& pr,
                     bool accepted) noexcept
    : _field{&field}, _pr{&pr}, _accepted{accepted} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<TermMeta>::id()) {
      return &_stats;
    }
    return type == irs::Type<TermAttr>::id() ? &_value : nullptr;
  }

  bytes_view value() const noexcept final { return _value.value; }

  bool next() final {
    if (_done || !_accepted) {
      _value.value = {};
      return false;
    }
    _done = true;
    Position();
    return true;
  }

  SeekResult seek_ge(bytes_view target) final {
    if (Term() < target) {
      _done = true;
      _value.value = {};
      return SeekResult::End;
    }
    _done = true;
    Position();
    return Term() == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    if (Term() != target) {
      _value.value = {};
      return false;
    }
    _done = true;
    Position();
    return true;
  }

  void read() final {}

  DocIterator::ptr RowGroupPostings(IndexFeatures features,
                                    uint32_t rg) const final {
    return RowGroupPostingsOf(*_pr, *_field, _field->single, features, rg);
  }

  TermCookie cookie() const final { return _field->single; }

  std::span<const TermRowGroup> RowGroups() const final {
    return _field->single.RowGroups();
  }

  Extents TermExtents() const final {
    return {.cookie = &_field->single, .doc_end = _field->single_end};
  }

 private:
  bytes_view Term() const noexcept { return _field->header.min_term; }

  void Position() noexcept {
    _value.value = Term();
    _stats = _field->single.stats;
  }

  const FieldState* _field;
  PostingsReader* _pr;
  TermAttr _value;
  TermMeta _stats;
  bool _accepted;
  bool _done{false};
};

class TermReaderImpl final : public TermReader, private util::Noncopyable {
 public:
  TermReaderImpl(PostingsReader& pr, const IndexInput& in, field_id id,
                 const TermDictMeta& meta, FieldHeader&& header,
                 uint64_t body_start)
    : _pr{&pr}, _in{&in} {
    _state.meta.id = id;
    _state.meta.norm = meta.norm;
    _state.meta.index_features = meta.features;
    _state.body_start = body_start;
    _state.body_offset = meta.body_offset;
    _state.term_count = meta.term_count;
    _state.doc_count = meta.doc_count;
    _state.total_term_freq = meta.total_term_freq;
    _state.has_wand = meta.has_wand;
    _state.header = std::move(header);
    if (_state.Single()) {
      SDB_ENSURE(_state.term_count == 1, "term_dict: single-term field ", id,
                 " declares ", _state.term_count, " terms");
      EntryChain chain;
      ParseEntry<ParseMode::Full>(
        _state.header.payload.c_str(),
        RecordColumns{meta.features, _state.header.RowGroupCount(),
                      _state.header.row_group_size},
        _state.Partitioned(), true, _state.header.doc_base, chain,
        &_state.single);
      _state.single_end = chain.doc_start;
    } else if ((_state.header.flags & kFlagFsst) != 0) {
      _state.fsst.Reset(_state.header.fsst_table);
      SDB_ENSURE(_state.fsst.Enabled(),
                 "term_dict: failed to import the FSST table of field ", id);
    }
    if (IndexFeatures::None != (meta.features & IndexFeatures::Freq)) {
      SDB_ENSURE(meta.total_term_freq <= std::numeric_limits<uint32_t>::max(),
                 "term_dict: total_term_freq ", meta.total_term_freq,
                 " exceeds uint32_t::max");
      _freq.value = static_cast<uint32_t>(meta.total_term_freq);
    }
  }

  const FieldMeta& meta() const noexcept final { return _state.meta; }
  size_t size() const noexcept final { return _state.term_count; }
  uint64_t docs_count() const noexcept final { return _state.doc_count; }
  bytes_view(min)() const noexcept final { return _state.header.min_term; }
  bytes_view(max)() const noexcept final { return _state.header.max_term; }
  bool has_scorer(uint8_t index) const noexcept final {
    return index == 0 && _state.has_wand;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (IndexFeatures::None !=
          (_state.meta.index_features & IndexFeatures::Freq) &&
        irs::Type<FreqAttr>::id() == type) {
      return &_freq;
    }
    return nullptr;
  }

  uint64_t ResidentBytes() const noexcept { return _state.resident_bytes; }

  void BuildRoot(IndexInput& in);

  SeekTermIterator::ptr iterator(SeekMode mode) const final {
    if (_state.Single()) {
      return memory::make_managed<SingleTermIterator>(_state, *_pr,
                                                      /*accepted=*/true);
    }
    if (mode == SeekMode::RandomOnly) {
      return memory::make_managed<SingleIteratorImpl>(_state, *_pr, Reopen());
    }
    return memory::make_managed<SeekIteratorImpl>(_state, *_pr, Reopen());
  }

  // Cuts at block boundaries: the separators already are those boundaries and
  // the root is resident from prepare, so this reads nothing. The directory's
  // ordinal column is what makes the shares equal in TERMS -- blocks are
  // capped by bytes, so their entry counts are not.
  size_t TermRanges(std::span<TermRange> out) const final {
    if (out.empty() || _state.term_count == 0) {
      return 0;
    }
    if (_state.Single()) {
      out.front() = {};
      return 1;
    }
    const uint32_t blocks = _state.BlockCount();
    const auto count = std::min<uint32_t>(out.size(), blocks);
    uint32_t block = 0;
    bytes_view lo;
    for (uint32_t i = 1; i < count; ++i) {
      const auto target = (_state.term_count * i) / count;
      // Every later cut needs a block of its own, so this one stops early
      // enough to leave them: the field always yields `count` ranges.
      const uint32_t last = blocks - (count - i);
      auto cut = block + 1;
      while (cut < last && _state.BlockOrdinal(cut) < target) {
        ++cut;
      }
      // Blocks are byte-capped, so a boundary lands beside the target rather
      // than on it -- take the nearer of the two it falls between.
      if (cut > block + 1 && _state.BlockOrdinal(cut) > target &&
          _state.BlockOrdinal(cut) - target >
            target - _state.BlockOrdinal(cut - 1)) {
        --cut;
      }
      const auto hi = _state.Separator(cut - 1);
      out[i - 1] = {lo, hi};
      lo = hi;
      block = cut;
    }
    out[count - 1] = {lo, {}};
    return count;
  }

  SeekTermIterator::ptr RangeIterator(const TermRange& range) const final {
    if (_state.Single()) {
      const bytes_view term{_state.header.min_term};
      const bool inside = (range.lo.empty() || range.lo <= term) &&
                          (range.hi.empty() || term < range.hi);
      return memory::make_managed<SingleTermIterator>(_state, *_pr, inside);
    }
    return memory::make_managed<RangeIteratorImpl>(_state, *_pr, Reopen(),
                                                   range);
  }

  SeekTermIterator::ptr BatchIterator(
    std::span<const bytes_view> terms) const final {
    if (_state.Single()) {
      return nullptr;
    }
    return memory::make_managed<BatchIteratorImpl>(_state, *_pr, Reopen(),
                                                   terms);
  }

  // The parametric backend: the acceptor steps `utils/levenshtein_utils`'s
  // tables directly, so no automaton is materialized.
  SeekTermIterator::ptr iterator(
    const LevenshteinAcceptor& acceptor) const final {
    if (_state.Single()) {
      return nullptr;
    }
    return memory::make_managed<AutomatonIteratorImpl<LevenshteinAcceptor>>(
      _state, *_pr, Reopen(), acceptor);
  }

  // The regexp backend: the acceptor's transition table is stepped directly, so
  // again nothing is materialized here. An acceptor that compiled to nothing
  // accepts nothing.
  SeekTermIterator::ptr iterator(const RegexpAcceptor& acceptor) const final {
    if (!acceptor.ok()) {
      return SeekTermIterator::empty();
    }
    if (_state.Single()) {
      return nullptr;
    }
    return memory::make_managed<AutomatonIteratorImpl<RegexpAcceptor>>(
      _state, *_pr, Reopen(), acceptor);
  }

  size_t RowGroupBitUnion(const cookie_provider& provider, uint32_t rg,
                          size_t* set) const final {
    TermMetaImpl meta;
    auto term_provider = [&]() mutable -> const TermMeta* {
      while (const auto* cookie = provider()) {
        if (const auto* run = cookie->Find(rg)) {
          meta = TermMetaImpl{*cookie, *run};
          return &meta;
        }
      }
      return nullptr;
    };
    return _pr->BitUnion(_state.meta.index_features, term_provider, set,
                         _state.has_wand);
  }

  RowGroupLayout RowGroups() const noexcept final { return _state.Layout(); }

  DocIterator::ptr RowGroupIterator(IndexFeatures features,
                                    std::span<const TermLeaf> terms,
                                    uint32_t rg, WandContext options,
                                    size_t min_match,
                                    ScoreMergeType type) const final {
    SDB_ASSERT(!terms.empty());
    SDB_ASSERT(1 <= min_match);
    SDB_ASSERT(min_match <= terms.size());

    // A term's run in this row group is a value inside its cookie; resolving
    // it against the term's anchors is what turns it into stream positions.
    absl::InlinedVector<PostingCookie, 4> cookies;
    cookies.reserve(terms.size());
    for (const auto& term : terms) {
      SDB_ASSERT(term.cookie);
      const auto* run = term.cookie->Find(rg);
      if (!run) {
        continue;
      }
      cookies.emplace_back(TermMetaImpl{*term.cookie, *run}, term.stats,
                           term.boost, term.field);
    }

    if (cookies.size() < min_match) {
      return {};
    }

    const IteratorFieldOptions field_options{options, _state.has_wand};
    return _pr->Iterator(_state.meta.index_features, features, cookies,
                         field_options, min_match, type);
  }

  std::unique_ptr<IndexInput> ReopenPayload() const final {
    return _pr->ReopenPayload();
  }

  std::unique_ptr<IndexInput> ReopenDoc() const final {
    return _pr->ReopenDoc();
  }

  std::unique_ptr<IndexInput> ReopenPos() const final {
    return _pr->ReopenPos();
  }

  uint64_t PayloadBase() const final { return _state.header.pay_base; }

  DictFieldStorage StorageInfo(bool walk) const final {
    const auto& header = _state.header;
    DictFieldStorage info;
    info.layout = LayoutName(header);
    info.body_offset = _state.body_offset;
    info.block_count = header.block_count;
    info.row_groups = header.RowGroupCount();
    info.terms = _state.term_count;
    info.docs = _state.doc_count;
    info.partitioned = _state.Partitioned();
    if (_state.Single()) {
      // Every plane is absent by construction; the one term's records live in
      // the header, which the pragma reports as the field's own size elsewhere.
      info.rg_lists = _state.single.rgs.size();
      info.max_rg_per_term = static_cast<uint32_t>(_state.single.rgs.size());
      info.walked = true;
      return info;
    }
    info.fsst = (header.flags & kFlagFsst) != 0;
    info.fsst_table_size = header.fsst_table.size();
    info.blocks_size = header.separators_offset - _state.body_start;
    info.separators_size = _state.body_offset - header.separators_offset;
    if (walk) {
      CountBlockLayouts(info);
      auto it = iterator(SeekMode::NORMAL);
      while (it->next()) {
        const auto cookie = it->cookie();
        const auto lists = static_cast<uint32_t>(cookie.rgs.size());
        info.rg_lists += lists;
        info.max_rg_per_term = std::max(info.max_rg_per_term, lists);
      }
      info.walked = true;
    }
    return info;
  }

 private:
  // Admission is per block, so the field header's one label cannot answer
  // "is the fast leaf engaged?" for a field of more than one key width --
  // every numeric field, whose precision ladder gives four term widths.
  void CountBlockLayouts(DictFieldStorage& info) const {
    auto in = Reopen();
    BlockView view;
    for (uint32_t i = 0, blocks = _state.BlockCount(); i != blocks; ++i) {
      view.Load(*in, _state.BlockOffset(i), _state.BlockSize(i));
      if (view.Stride() != 0) {
        ++info.fixed_blocks;
      } else {
        ++info.var_blocks;
      }
    }
  }

  static std::string_view LayoutName(const FieldHeader& header) noexcept {
    switch (header.layout) {
      case LayoutKind::Var:
        return "VAR";
      case LayoutKind::FixedStride:
        return "FIXED_STRIDE";
      case LayoutKind::Single:
        return "SINGLE";
    }
    return {};
  }

  IndexInput::ptr Reopen() const {
    auto in = _in->Reopen();
    if (!in) {
      SDB_ERROR(IRESEARCH, "Failed to reopen term dictionary input");
      throw IoError{"failed to reopen term dictionary input"};
    }
    return in;
  }

  PostingsReader* _pr;
  const IndexInput* _in;
  FieldState _state;
  FreqAttr _freq;
};

void TermReaderImpl::BuildRoot(IndexInput& in) {
  auto& field = _state;
  if (field.Single()) {
    return;
  }
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  const uint32_t blocks = field.BlockCount();
  SDB_ENSURE(blocks != 0, "term_dict: field ", field.meta.id,
             " has no leaf blocks");

  in.Seek(field.header.separators_offset);

  const size_t offsets_bytes = (blocks + 1) * sizeof(uint64_t);
  field.block_offsets = allocator.Allocate(offsets_bytes);
  field.block_ordinals = allocator.Allocate(offsets_bytes);
  auto* offsets = reinterpret_cast<uint64_t*>(field.block_offsets.get());
  auto* ordinals = reinterpret_cast<uint64_t*>(field.block_ordinals.get());
  uint64_t running = 0;
  uint64_t ordinal = 0;
  for (uint32_t i = 0; i != blocks + 1; ++i) {
    running += in.ReadV64();
    offsets[i] = running;
    ordinals[i] = ordinal;
    if (i != blocks) {
      ordinal += in.ReadV32();
    }
  }
  SDB_ENSURE(ordinal == field.term_count,
             "term_dict: block directory of field ", field.meta.id, " sums ",
             ordinal, " terms, the field declares ", field.term_count);

  bstring arena;
  bstring separator;
  std::vector<uint32_t> index;
  index.reserve(blocks);
  for (uint32_t i = 0; i + 1 != blocks; ++i) {
    const uint32_t shared = in.ReadV32();
    const uint32_t size = in.ReadV32();
    SDB_ENSURE(shared <= separator.size(),
               "term_dict: corrupted separator section of field ",
               field.meta.id);
    separator.resize(shared + size);
    in.ReadData(separator.data() + shared, size);
    index.push_back(static_cast<uint32_t>(arena.size()));
    arena.append(separator);
  }
  index.push_back(static_cast<uint32_t>(arena.size()));

  const size_t arena_bytes = std::max<size_t>(arena.size(), 1);
  field.separators = allocator.Allocate(arena_bytes);
  std::memcpy(field.separators.get(), arena.data(), arena.size());
  const size_t index_bytes = index.size() * sizeof(uint32_t);
  field.separator_index = allocator.Allocate(index_bytes);
  std::memcpy(field.separator_index.get(), index.data(), index_bytes);

  field.resident_bytes = 2 * offsets_bytes + arena_bytes + index_bytes;
}

}  // namespace
namespace irs::term_dict {

class FieldReader::Impl {
 public:
  explicit Impl(PostingsReader::ptr&& pr) : _pr{std::move(pr)} {
    SDB_ASSERT(_pr);
  }

  uint64_t CountMappedMemory() const {
    uint64_t bytes = 0;
    if (_pr) {
      bytes += _pr->CountMappedMemory();
    }
    if (_in) {
      bytes += _in->CountMappedMemory();
    }
    for (const auto& field : _fields) {
      bytes += field->ResidentBytes();
    }
    return bytes;
  }

  void prepare(const ReaderState& state);

  const TermReader* field(field_id id) const {
    auto it = _id_to_field.find(id);
    return it == _id_to_field.end() ? nullptr : it->second;
  }
  std::span<const field_id> field_ids() const noexcept { return _sorted_ids; }
  size_t size() const noexcept { return _id_to_field.size(); }
  RowGroupLayout RowGroups() const noexcept { return _row_groups; }

 private:
  RowGroupLayout _row_groups;
  std::vector<std::unique_ptr<TermReaderImpl>> _fields;
  sdb::containers::FlatHashMap<field_id, TermReader*> _id_to_field;
  std::vector<field_id> _sorted_ids;
  PostingsReader::ptr _pr;
  IndexInput::ptr _in;
};

void FieldReader::Impl::prepare(const ReaderState& state) {
  SDB_ASSERT(state.dir);
  SDB_ASSERT(state.meta);
  SDB_ASSERT(state.idx,
             "term_dict::FieldReader::prepare requires an IdxReader");

  auto entries = state.idx->TermDicts();
  _fields.reserve(entries.size());
  _id_to_field.reserve(entries.size());

  _in = state.idx->ReopenIn();
  if (!_in) {
    SDB_ENSURE(entries.empty(), "term_dict: TermDicts span has ",
               entries.size(), " entries but `.idx` body stream is null");
    return;
  }
  _in->Seek(state.idx->BodyStart());

  IndexFeatures features = IndexFeatures::None;
  for (const auto& [id, meta] : entries) {
    features = features | meta.features;
  }
  _pr->prepare(*_in, state, features);

  // Fields follow one another in the body in the order they were written, which
  // is ascending field id, so each field's planes start where the previous
  // field's header ended and the first one starts after the postings header.
  uint64_t body_start = _in->Position();

  _sorted_ids.reserve(entries.size());
  for (const auto& [id, meta] : entries) {
    _in->Seek(meta.body_offset);
    auto header = ReadFieldHeader(*_in, meta.features);
    auto layout = HeaderLayout(header);
    SDB_ASSERT(_row_groups.rows_per_group == 0 ||
                 _row_groups.rows_per_group == layout.rows_per_group,
               "term_dict: fields of one segment disagree on the row group "
               "grid");
    // The derived body start is what makes the plane sizes exact without any
    // stored offset, so the ordering it rests on is asserted, not assumed.
    SDB_ASSERT(header.layout == LayoutKind::Single ||
                 (body_start <= header.separators_offset &&
                  header.separators_offset <= meta.body_offset),
               "term_dict: field ", id, " body planes are out of order");
    _row_groups = layout;
    auto& field = _fields.emplace_back(std::make_unique<TermReaderImpl>(
      *_pr, *_in, id, meta, std::move(header), body_start));
    body_start = _in->Position();
    field->BuildRoot(*_in);
    auto [it, ok] = _id_to_field.emplace(id, field.get());
    SDB_ENSURE(ok, ".idx footer: duplicate term-dict field_id ", id);
    _sorted_ids.push_back(id);
  }
  SDB_ENSURE(std::is_sorted(_sorted_ids.begin(), _sorted_ids.end()),
             "term_dict: term-dict entries are not sorted by field_id");
}

FieldWriter::FieldWriter(PostingsWriter::ptr pw, bool /*compaction*/,
                         IResourceManager& rm, const DictOptions& dict)
  : _impl{std::make_unique<Impl>(std::move(pw), rm)} {
  WriterOptions options;
  options.row_group_size = dict.row_group_size;
  _impl->SetOptions(options);
}

FieldWriter::~FieldWriter() = default;

void FieldWriter::SetIdxWriter(IdxWriter& idx) noexcept {
  _impl->SetIdxWriter(idx);
}

void FieldWriter::SetOptions(const WriterOptions& options) noexcept {
  _impl->SetOptions(options);
}

void FieldWriter::prepare(const FlushState& state) { _impl->prepare(state); }

void FieldWriter::write(const BasicTermReader& reader) { _impl->write(reader); }

void FieldWriter::end() { _impl->end(); }

FieldReader::FieldReader(PostingsReader::ptr pr, IResourceManager& /*rm*/)
  : _impl{std::make_unique<Impl>(std::move(pr))} {}

FieldReader::~FieldReader() = default;

uint64_t FieldReader::CountMappedMemory() const {
  return _impl->CountMappedMemory();
}

void FieldReader::prepare(const ReaderState& state) { _impl->prepare(state); }

const TermReader* FieldReader::field(field_id id) const {
  return _impl->field(id);
}

std::span<const field_id> FieldReader::field_ids() const noexcept {
  return _impl->field_ids();
}

size_t FieldReader::size() const noexcept { return _impl->size(); }

RowGroupLayout FieldReader::RowGroups() const noexcept {
  return _impl->RowGroups();
}

#ifdef SDB_DEV
AutomatonBlocks AutomatonBlockCounts() noexcept {
  return {.decided = kDecidedBlocks.load(std::memory_order_relaxed),
          .walked = kWalkedBlocks.load(std::memory_order_relaxed)};
}
#endif

}  // namespace irs::term_dict
