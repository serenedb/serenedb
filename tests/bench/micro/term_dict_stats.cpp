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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <fsst.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/duckdb_engine.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "search/index_durability.h"

namespace {

namespace td = irs::term_dict;
using irs::bstring;
using irs::byte_type;
using irs::bytes_view;

constexpr uint32_t kRestarts[] = {8, 16, 32, 64, 128, 256, 512, 1024};
constexpr size_t kAnchorInterval = 16;
constexpr size_t kBucketBytes = 256;
constexpr size_t kBuckets = td::kPage / kBucketBytes;
constexpr size_t kEncodeChunk = 1 << 16;
constexpr uint64_t kSampleBytes = 32 << 10;
constexpr size_t kSampleRun = 16;
constexpr size_t kMinSamples = 64;
constexpr uint64_t kMinMeanSuffix = 2;
constexpr uint64_t kMaxMeanSuffix = 16;
constexpr uint64_t kMinRatioPct = 115;

struct FileClose {
  void operator()(std::FILE* file) const noexcept { std::fclose(file); }
};

using File = std::unique_ptr<std::FILE, FileClose>;

struct FsstDelete {
  void operator()(duckdb_fsst_encoder_t* encoder) const noexcept {
    duckdb_fsst_destroy(encoder);
  }
};

using Fsst = std::unique_ptr<duckdb_fsst_encoder_t, FsstDelete>;

uint64_t VSize(uint64_t value) noexcept {
  return std::max<uint64_t>(1, (std::bit_width(value) + 6) / 7);
}

uint64_t VarBytesWith(uint64_t count, uint64_t restarts, uint32_t max,
                      uint64_t suffixes, uint64_t records,
                      uint64_t head_skip) noexcept {
  return 2 + VSize(count) + VSize(restarts) + VSize(suffixes) +
         VSize(head_skip) + (td::kVarRestart + td::kHeadBytes) * restarts +
         td::kRecordOffset * td::RecordGroups(count) +
         count * (2 * uint64_t{td::ColumnWidth(max)} + 2) + suffixes + records +
         td::kSlack;
}

uint64_t VarBytes(uint64_t count, uint64_t restart, uint32_t max,
                  uint64_t suffixes, uint64_t records,
                  uint64_t head_skip) noexcept {
  return VarBytesWith(count, (count + restart - 1) / restart, max, suffixes,
                      records, head_skip);
}

size_t GuardedLcp(bytes_view lhs, bytes_view rhs) noexcept {
  size_t lcp = irs::CommonPrefixLength(lhs, rhs);
  while (lcp != 0 && lhs[lcp - 1] == FSST_ESC) {
    --lcp;
  }
  return lcp;
}

double Percentile(std::vector<double>& values, double p) {
  if (values.empty()) {
    return 0;
  }
  absl::c_sort(values);
  const auto at =
    static_cast<size_t>(p * static_cast<double>(values.size() - 1));
  return values[at];
}

struct Sections {
  uint64_t header = 0;
  uint64_t restart_offsets = 0;
  uint64_t restart_heads = 0;
  uint64_t record_offsets = 0;
  uint64_t lcps = 0;
  uint64_t lens = 0;
  uint64_t tails = 0;
  uint64_t befores = 0;
  uint64_t prefix = 0;
  uint64_t keys = 0;
  uint64_t suffixes = 0;
  uint64_t records = 0;
  uint64_t slack = 0;

  uint64_t Sum() const noexcept {
    return header + restart_offsets + restart_heads + record_offsets + lcps +
           lens + tails + befores + prefix + keys + suffixes + records + slack;
  }

  void Add(const Sections& other) noexcept {
    header += other.header;
    restart_offsets += other.restart_offsets;
    restart_heads += other.restart_heads;
    record_offsets += other.record_offsets;
    lcps += other.lcps;
    lens += other.lens;
    tails += other.tails;
    befores += other.befores;
    prefix += other.prefix;
    keys += other.keys;
    suffixes += other.suffixes;
    records += other.records;
    slack += other.slack;
  }
};

constexpr std::string_view kSectionNames[] = {
  "header",   "restart_offsets", "restart_heads", "record_offsets", "lcps",
  "lens",     "tails",           "befores",       "prefix",         "keys",
  "suffixes", "records",         "slack",
};

std::array<uint64_t, std::size(kSectionNames)> Values(const Sections& s) {
  return {
    s.header,   s.restart_offsets, s.restart_heads, s.record_offsets, s.lcps,
    s.lens,     s.tails,           s.befores,       s.prefix,         s.keys,
    s.suffixes, s.records,         s.slack};
}

constexpr std::string_view kKindNames[] = {"var", "fixed", "var_fsst"};

struct KindStats {
  uint64_t blocks = 0;
  uint64_t written = 0;
  uint64_t page_bytes = 0;
  uint64_t raw_fits = 0;
  Sections sections;
  std::array<uint64_t, kBuckets + 1> histogram{};
  std::vector<double> sizes;
};

struct Checks {
  uint64_t last_block = 0;
  uint64_t nonzero_tail = 0;
  uint64_t section_sum = 0;
  uint64_t tail_owner = 0;
  uint64_t term_count = 0;
  uint64_t errors = 0;
};

struct Meta {
  uint64_t doc_start;
  uint64_t pos_start;
  uint64_t pay_start;
  uint32_t docs;
  uint32_t freq;
  uint32_t doc_delta;
  uint32_t pos_extent;
  uint32_t pay_extent;
  uint16_t pos_offset;
  uint8_t inline_size;
};

Meta MetaOf(const irs::PostingMeta& meta) noexcept {
  return {
    .doc_start = meta.doc_start,
    .pos_start = meta.pos_start,
    .pay_start = meta.pay_start,
    .docs = meta.docs_count,
    .freq = meta.freq,
    .doc_delta = meta.doc_delta,
    .pos_extent = meta.pos_extent,
    .pay_extent = meta.pay_extent,
    .pos_offset = meta.pos_offset,
    .inline_size = meta.inline_size,
  };
}

struct Dict {
  const td::FieldHead* head = nullptr;
  const td::TermReaderImpl* reader = nullptr;
  std::vector<uint32_t> records;
  std::vector<Meta> metas;
  uint64_t blocks = 0;
  uint64_t suffix_bytes = 0;
};

struct Options {
  std::filesystem::path out;
  std::filesystem::path dump_keys;
  bool fsst = false;
};

struct Outputs {
  File blocks;
  File dicts;
  File fsst;
  File fixed;
  std::array<KindStats, std::size(kKindNames)> kinds;
  Checks checks;
};

class BlockBytes {
 public:
  explicit BlockBytes(const td::FieldReader& fields) noexcept
    : _fields{&fields} {}

  const byte_type* At(uint64_t offset, uint64_t size) {
    if (const auto* base = _fields->Base()) {
      return base + offset;
    }
    if (!_in) {
      _in = _fields->Input().Reopen();
    }
    _copy.resize(size);
    _in->ReadData(offset, _copy.data(), size);
    return _copy.data();
  }

 private:
  const td::FieldReader* _fields;
  irs::IndexInput::ptr _in;
  bstring _copy;
};

// Raw size a VarFsst block would have with its suffixes decoded.
uint64_t RawSize(const td::BlockView& view, uint64_t records,
                 const duckdb_fsst_decoder_t& fsst, bstring& buffer) {
  const auto* sp = view.suffixes;
  uint64_t suffixes = 0;
  uint32_t max = 0;
  for (uint32_t entry = 0; entry != view.count; ++entry) {
    const auto header = td::HeaderAt(view, entry);
    uint64_t len = header.len;
    if (entry % td::kRestartInterval != 0) {
      buffer.resize(size_t{header.len} * sizeof(uint64_t) + 16);
      len = td::DecodeInto(fsst, {sp, header.len}, buffer.data(),
                           buffer.size() - sizeof(uint64_t));
    }
    sp += header.len;
    suffixes += len;
    max = std::max({max, header.lcp, static_cast<uint32_t>(len)});
  }
  return VarBytes(view.count, td::kRestartInterval, max, suffixes, records,
                  view.head_skip);
}

void WalkBlocks(std::string_view segment, const td::FieldReader& fields,
                std::span<Dict> dicts, Outputs& out) {
  BlockBytes bytes{fields};
  bstring buffer;
  const uint32_t count = fields.BlockCount();
  size_t lo = 0;
  for (uint32_t block = 0; block != count; ++block) {
    const uint64_t offset = fields.PageOffset(block);
    const uint64_t end =
      block + 1 == count ? fields.InlineBegin() : fields.PageOffset(block + 1);
    const auto* p = bytes.At(offset, end - offset);
    while (lo != dicts.size() && dicts[lo].head->last_block < block) {
      ++lo;
    }
    const duckdb_fsst_decoder_t* fsst = nullptr;
    for (size_t d = lo;
         !fsst && d != dicts.size() && dicts[d].head->first_block <= block;
         ++d) {
      if (dicts[d].head->last_block >= block && dicts[d].reader) {
        fsst = dicts[d].reader->Fsst();
      }
    }
    const auto view = td::Parse(p, fsst);

    const byte_type* records_end = view.records;
    std::vector<irs::field_id> ids;
    uint32_t covering = 0;
    uint32_t tail = 0;
    for (size_t d = lo;
         d != dicts.size() && dicts[d].head->first_block <= block; ++d) {
      auto& dict = dicts[d];
      const auto& head = *dict.head;
      if (head.last_block < block) {
        continue;
      }
      ++covering;
      ++dict.blocks;
      if (ids.empty() || ids.back() != head.id) {
        ids.push_back(head.id);
      }
      const uint32_t first = head.first_block == block ? head.first_entry : 0;
      const uint32_t last =
        head.last_block == block ? head.last_entry : view.count - 1;
      const byte_type* rec = nullptr;
      irs::PostingMeta meta;
      uint64_t inline_end = 0;
      for (uint32_t entry = first; entry <= last; ++entry) {
        if (entry == first || entry % td::kRecordInterval == 0) {
          rec = entry == first && head.first_block == block
                  ? view.records + head.first_record
                  : td::GroupStart(view, entry / td::kRecordInterval);
          meta.clear();
          inline_end = 0;
        }
        const auto* before = rec;
        td::DecodeRecords(rec, 1, head.features, meta, inline_end);
        dict.records.push_back(static_cast<uint32_t>(rec - before));
        dict.metas.push_back(MetaOf(meta));
        if (!view.fixed) {
          dict.suffix_bytes += td::HeaderAt(view, entry).len;
        }
      }
      records_end = rec;
      tail = last;
    }
    if (covering == 0 || tail + 1 != view.count) {
      ++out.checks.tail_owner;
    }

    Sections s;
    const uint64_t groups = td::RecordGroups(view.count);
    if (view.fixed) {
      s.header = static_cast<uint64_t>(view.table - view.prefix - p);
      s.prefix = view.prefix;
      s.record_offsets = td::kRecordOffset * groups;
      s.restart_heads = td::kHeadBytes * groups;
      s.keys = uint64_t{view.count} * view.stride;
    } else {
      s.header = static_cast<uint64_t>(view.table - p);
      s.restart_offsets = td::kVarRestart * uint64_t{view.restarts};
      s.restart_heads = td::kHeadBytes * uint64_t{view.restarts};
      s.record_offsets = td::kRecordOffset * groups;
      s.lcps = uint64_t{view.count} * view.width;
      s.lens = uint64_t{view.count} * view.width;
      s.tails = view.count;
      s.befores = view.count;
      s.suffixes = static_cast<uint64_t>(view.records - view.suffixes);
    }
    s.records = static_cast<uint64_t>(records_end - view.records);
    s.slack = td::kSlack;
    const uint64_t written =
      static_cast<uint64_t>(records_end - p) + td::kSlack;
    if (s.Sum() != written) {
      ++out.checks.section_sum;
    }
    if (block + 1 == count && written != end - offset) {
      ++out.checks.last_block;
    }
    if (std::any_of(records_end, p + (end - offset),
                    [](byte_type b) { return b != 0; })) {
      ++out.checks.nonzero_tail;
    }

    const auto kind = static_cast<size_t>(*p);
    uint64_t raw = 0;
    bool raw_fits = false;
    if (view.coded) {
      raw = RawSize(view, s.records, *fsst, buffer);
      raw_fits = raw <= td::kPage;
    }
    auto& stats = out.kinds.at(kind);
    ++stats.blocks;
    stats.written += written;
    stats.page_bytes += end - offset;
    stats.raw_fits += raw_fits;
    stats.sections.Add(s);
    stats.sizes.push_back(static_cast<double>(written));
    ++stats.histogram[written > td::kPage ? kBuckets
                                          : (written - 1) / kBucketBytes];

    const auto values = Values(s);
    const uint64_t padding = end - offset - written;
    absl::FPrintF(out.blocks.get(), "%s,%u,%s,%u,%u,%s,%u,%s,%u,%u,%u,%d,%u\n",
                  segment, block, kKindNames[kind], view.count, covering,
                  absl::StrJoin(ids, ";"), view.width,
                  absl::StrJoin(values, ","), written, end - offset, padding,
                  static_cast<int>(raw_fits), raw);
  }
}

struct Key {
  uint32_t key;
  uint32_t lcp;
  uint32_t record;
  uint32_t coded_sample;
  uint32_t coded_full;
  uint32_t coded_key;
  uint32_t code_lcp;
  uint32_t coded_segment;
};

struct Table {
  Fsst encoder;
  uint64_t samples = 0;
  uint64_t raw = 0;
  uint64_t coded = 0;
  uint64_t bytes = 0;

  bool Gate() const noexcept {
    return samples >= kMinSamples && raw >= kMinMeanSuffix * samples &&
           raw <= kMaxMeanSuffix * samples &&
           raw * 100 >= coded * kMinRatioPct + bytes * 100;
  }
  double Ratio() const noexcept {
    return coded == 0 ? 1
                      : static_cast<double>(raw) / static_cast<double>(coded);
  }
};

// Encodes `inputs` with `encoder`, calling visit(i, codes) for each string.
template<typename Visit>
void Encode(duckdb_fsst_encoder_t* encoder, std::span<const bytes_view> inputs,
            Visit&& visit) {
  std::vector<size_t> lens;
  std::vector<unsigned char*> ptrs;
  std::vector<size_t> out_lens;
  std::vector<unsigned char*> out_ptrs;
  bstring out;
  for (size_t at = 0; at < inputs.size(); at += kEncodeChunk) {
    const size_t n = std::min(kEncodeChunk, inputs.size() - at);
    lens.resize(n);
    ptrs.resize(n);
    out_lens.resize(n);
    out_ptrs.resize(n);
    uint64_t total = 0;
    for (size_t i = 0; i != n; ++i) {
      const auto input = inputs[at + i];
      lens[i] = input.size();
      ptrs[i] = const_cast<unsigned char*>(input.data());
      total += input.size();
    }
    out.resize(7 + 2 * total + 16);
    const size_t done =
      duckdb_fsst_compress(encoder, n, lens.data(), ptrs.data(), out.size(),
                           out.data(), out_lens.data(), out_ptrs.data());
    SDB_ASSERT(done == n);
    for (size_t i = 0; i != done; ++i) {
      visit(at + i, bytes_view{out_ptrs[i], out_lens[i]});
    }
  }
}

Table Train(std::span<const bytes_view> suffixes) {
  Table table;
  if (suffixes.empty()) {
    return table;
  }
  std::vector<size_t> lens(suffixes.size());
  std::vector<unsigned char*> ptrs(suffixes.size());
  for (size_t i = 0; i != suffixes.size(); ++i) {
    lens[i] = suffixes[i].size();
    ptrs[i] = const_cast<unsigned char*>(suffixes[i].data());
    table.raw += suffixes[i].size();
  }
  table.samples = suffixes.size();
  table.encoder.reset(
    duckdb_fsst_create(lens.size(), lens.data(), ptrs.data(), 0));
  if (!table.encoder) {
    return table;
  }
  unsigned char header[FSST_MAXHEADER];
  table.bytes = duckdb_fsst_export(table.encoder.get(), header);
  Encode(table.encoder.get(), suffixes,
         [&](size_t, bytes_view codes) { table.coded += codes.size(); });
  return table;
}

uint64_t Bits(uint64_t value) noexcept {
  return static_cast<uint64_t>(std::bit_width(value));
}

uint64_t PackedBytes(uint64_t count, uint64_t bits) noexcept {
  return (count * bits + 7) / 8;
}

// Values stored frame-of-reference: a base and (value - base) in a fixed
// number of bits.
struct ForColumn {
  uint64_t n = 0;
  uint64_t min = std::numeric_limits<uint64_t>::max();
  uint64_t max = 0;

  void Add(uint64_t value) noexcept {
    ++n;
    min = std::min(min, value);
    max = std::max(max, value);
  }
  uint64_t Bytes() const noexcept {
    return n == 0 ? 0 : 1 + VSize(min) + PackedBytes(n, Bits(max - min));
  }
};

// Non-decreasing values: either frame-of-reference on the values (O(1)
// access) or on the gaps (access by a prefix sum).
struct MonotoneColumn {
  uint64_t n = 0;
  uint64_t first = 0;
  uint64_t last = 0;
  uint64_t min_gap = std::numeric_limits<uint64_t>::max();
  uint64_t max_gap = 0;

  void Add(uint64_t value) noexcept {
    if (n != 0) {
      const uint64_t gap = value - last;
      min_gap = std::min(min_gap, gap);
      max_gap = std::max(max_gap, gap);
    } else {
      first = value;
    }
    last = value;
    ++n;
  }
  uint64_t Bytes(bool gaps) const noexcept {
    if (n == 0) {
      return 0;
    }
    if (!gaps || n == 1) {
      return 1 + VSize(first) + PackedBytes(n, Bits(last - first));
    }
    return 2 + VSize(first) + VSize(min_gap) +
           PackedBytes(n - 1, Bits(max_gap - min_gap));
  }
};

enum class Records : uint8_t {
  Current,
  SoaFor,
  SoaGap,
};

constexpr std::string_view kRecordNames[] = {"current", "soa_for", "soa_gap"};

// PostingMeta of a block stored column-wise. Optional columns hold only the
// entries they apply to; an entry finds its slot by a rank over the kind and
// follows bitmaps.
struct SoaRecords {
  uint64_t count = 0;
  bool any_kind = false;
  bool any_inline = false;
  ForColumn docs;
  ForColumn single_doc;
  ForColumn freq;
  ForColumn inline_size;
  ForColumn pos_offset;
  ForColumn skip;
  ForColumn pos_extent;
  ForColumn pay_extent;
  MonotoneColumn doc_start;
  MonotoneColumn pos_start;
  MonotoneColumn pay_start;
  Meta prev{};

  void Add(const Meta& m, irs::IndexFeatures features) noexcept {
    const bool has_freq =
      irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);
    const bool has_pos =
      irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos);
    const bool has_offs =
      irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs);
    const bool single = m.docs == 1;
    const bool inlined = m.inline_size != 0;
    any_kind |= single || inlined;
    if (single) {
      single_doc.Add(m.doc_delta);
    } else {
      docs.Add(m.docs);
    }
    if (has_freq) {
      freq.Add(m.freq - m.docs);
    }
    if (inlined) {
      any_inline = true;
      inline_size.Add(m.inline_size);
    } else if (!single) {
      doc_start.Add(m.doc_start);
    }
    if (has_pos) {
      const bool follows =
        count != 0 && m.pos_offset == (uint64_t{prev.pos_offset} + prev.freq) %
                                        irs::PosGroup::kPositions;
      if (!follows) {
        pos_offset.Add(m.pos_offset);
      }
      if (count == 0 || m.pos_start != prev.pos_start) {
        pos_start.Add(m.pos_start);
        if (has_offs) {
          pay_start.Add(m.pay_start);
        }
      }
      if (irs::pos_limits::kBlockSize < m.freq) {
        pos_extent.Add(m.pos_extent);
        if (has_offs) {
          pay_extent.Add(m.pay_extent);
        }
      }
    }
    if (irs::doc_limits::kBlockSize < m.docs) {
      skip.Add(m.doc_delta);
    }
    prev = m;
    ++count;
  }

  uint64_t Bytes(bool gaps, irs::IndexFeatures features) const noexcept {
    const bool has_pos =
      irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos);
    return 1 + (any_kind ? PackedBytes(count, 2) : 0) +
           (has_pos ? PackedBytes(count, 1) : 0) + (any_inline ? 4 : 0) +
           docs.Bytes() + single_doc.Bytes() + freq.Bytes() +
           inline_size.Bytes() + pos_offset.Bytes() + skip.Bytes() +
           pos_extent.Bytes() + pay_extent.Bytes() + doc_start.Bytes(gaps) +
           pos_start.Bytes(gaps) + pay_start.Bytes(gaps);
  }
};

enum class Variant : uint8_t {
  Raw,
  Sample,
  Full,
  EncodedFirst,
  Estimate,
  Segment,
};

constexpr std::string_view kVariantNames[] = {
  "raw", "field_sample", "field_full", "encoded_first", "estimate", "segment",
};

struct Packed {
  uint64_t blocks = 0;
  uint64_t written = 0;
  uint64_t suffixes = 0;
  uint64_t records = 0;
  uint64_t overflow_blocks = 0;
  uint64_t overflow_bytes = 0;
  std::vector<double> ratios;
};

struct Lens {
  uint32_t lcp;
  uint32_t cut;
  uint32_t real;
};

Lens LensOf(const Key& key, Variant variant, bool restart, uint32_t prefix,
            double ratio) {
  if (variant == Variant::EncodedFirst) {
    const uint32_t whole = prefix + key.coded_key;
    return restart
             ? Lens{0, whole, whole}
             : Lens{key.code_lcp, whole - key.code_lcp, whole - key.code_lcp};
  }
  if (restart) {
    return {0, key.key, key.key};
  }
  const uint32_t raw = key.key - key.lcp;
  switch (variant) {
    case Variant::Raw:
      return {key.lcp, raw, raw};
    case Variant::Sample:
      return {key.lcp, key.coded_sample, key.coded_sample};
    case Variant::Full:
      return {key.lcp, key.coded_full, key.coded_full};
    case Variant::Estimate:
      return {
        key.lcp,
        static_cast<uint32_t>(std::ceil(static_cast<double>(raw) / ratio)),
        key.coded_sample};
    case Variant::Segment:
      return {key.lcp, key.coded_segment, key.coded_segment};
    case Variant::EncodedFirst:
      break;
  }
  return {key.lcp, raw, raw};
}

struct PackOptions {
  Variant variant = Variant::Raw;
  uint32_t restart = td::kRestartInterval;
  uint32_t prefix = 0;
  double ratio = 1;
  Records records = Records::Current;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  // Raw only: each key shares its prefix with its group's restart key instead
  // of its predecessor, so any key decodes from the head and its own suffix.
  bool head_relative = false;
  // Also restart at a key sharing at most one byte beyond the dictionary
  // prefix with its predecessor, once the group holds adaptive_min keys.
  uint32_t adaptive_min = 0;
  std::span<const bstring> terms;
};

Packed Pack(std::span<const Key> keys, std::span<const Meta> metas,
            const PackOptions& o) {
  struct Block {
    uint64_t count = 0;
    uint64_t cut = 0;
    uint64_t real = 0;
    uint32_t cut_max = 0;
    uint32_t real_max = 0;
    uint64_t records = 0;
    uint64_t raw = 0;
    uint64_t coded = 0;
    uint64_t restarts = 0;
    uint64_t since = 0;
    size_t head = 0;
    SoaRecords soa;
  };
  const auto variant = o.variant;
  const auto restart = o.restart;
  const auto prefix = o.prefix;
  const auto ratio = o.ratio;
  const bool soa = o.records != Records::Current;
  const bool gaps = o.records == Records::SoaGap;
  const auto size_of = [&](uint64_t count, uint64_t restarts, uint32_t max,
                           uint64_t suffixes, uint64_t records) {
    const uint64_t size =
      VarBytesWith(count, restarts, max, suffixes, records, 1);
    return soa ? size - td::kRecordOffset * td::RecordGroups(count) : size;
  };
  const auto lens_of = [&](size_t i, const Block& b, bool first) {
    const auto& key = keys[i];
    if (first || !o.head_relative) {
      return LensOf(key, variant, first, prefix, ratio);
    }
    const auto lcp = static_cast<uint32_t>(
      prefix + irs::CommonPrefixLength(bytes_view{o.terms[b.head]},
                                       bytes_view{o.terms[i]}));
    return Lens{lcp, key.key - lcp, key.key - lcp};
  };
  const auto restarts_at = [&](size_t i, const Block& b) {
    return b.count == 0 || b.since >= restart ||
           (o.adaptive_min != 0 && b.since >= o.adaptive_min &&
            keys[i].lcp <= prefix + 1);
  };
  Packed packed;
  Block block;
  SoaRecords trial;
  const auto close = [&] {
    const uint64_t size = size_of(block.count, block.restarts, block.real_max,
                                  block.real, block.records);
    ++packed.blocks;
    packed.written += size;
    packed.suffixes += block.real;
    packed.records += block.records;
    if (size > td::kPage && block.count > 1) {
      ++packed.overflow_blocks;
      packed.overflow_bytes += size - td::kPage;
    }
    if (block.coded != 0) {
      packed.ratios.push_back(static_cast<double>(block.raw) /
                              static_cast<double>(block.coded));
    }
    block = {};
  };
  const auto records_with = [&](size_t i, const Block& b, SoaRecords& next) {
    if (!soa) {
      return b.records + keys[i].record;
    }
    next = b.soa;
    next.Add(metas[i], o.features);
    return next.Bytes(gaps, o.features);
  };
  for (size_t i = 0; i != keys.size(); ++i) {
    const auto& key = keys[i];
    bool first = restarts_at(i, block);
    auto lens = lens_of(i, block, first);
    uint64_t records = records_with(i, block, trial);
    const uint64_t size =
      size_of(block.count + 1, block.restarts + (first ? 1 : 0),
              std::max({block.cut_max, lens.lcp, lens.cut}),
              block.cut + lens.cut, records);
    if (block.count != 0 && size > td::kPage) {
      close();
      first = true;
      lens = lens_of(i, block, first);
      records = records_with(i, block, trial);
    }
    if (first) {
      ++block.restarts;
      block.since = 0;
      block.head = i;
    }
    ++block.since;
    ++block.count;
    block.cut += lens.cut;
    block.real += lens.real;
    block.cut_max = std::max({block.cut_max, lens.lcp, lens.cut});
    block.real_max = std::max({block.real_max, lens.lcp, lens.real});
    block.records = records;
    if (soa) {
      block.soa = trial;
    }
    if (!first) {
      block.raw += key.key - key.lcp;
      block.coded += lens.real;
    }
  }
  if (block.count != 0) {
    close();
  }
  return packed;
}

enum class KeyModel : uint8_t {
  Stride,
  For,
  GapFor,
  Trie,
};

constexpr std::string_view kKeyModelNames[] = {"stride", "for", "gap_for",
                                               "trie"};

using U128 = unsigned __int128;

U128 ValueOf(bytes_view key) noexcept {
  U128 value = 0;
  for (const auto b : key) {
    value = value << 8 | b;
  }
  return value;
}

uint64_t Bits128(U128 value) noexcept {
  const auto hi = static_cast<uint64_t>(value >> 64);
  return hi != 0 ? 64 + Bits(hi) : Bits(static_cast<uint64_t>(value));
}

uint64_t VSize128(U128 value) noexcept {
  return std::max<uint64_t>(1, (Bits128(value) + 6) / 7);
}

struct FixedPacked {
  uint64_t blocks = 0;
  uint64_t written = 0;
  uint64_t keys = 0;
  uint64_t records = 0;
};

// Packs a fixed-width dictionary. `Stride` is today's FixedStride block; the
// others replace its prefix, stride bytes and heads:
// - For: the first key and (key - first) in bits(last - first) bits;
// - GapFor: the first key, (gap - min gap) bitpacked, and an absolute anchor
//   every kAnchorInterval keys for binary search;
// - Trie: a byte trie of the block's keys, one label byte per node plus 2 bits
//   of LOUDS structure per node.
FixedPacked PackFixed(std::span<const bstring> terms,
                      std::span<const Meta> metas,
                      std::span<const uint32_t> current, uint32_t width,
                      KeyModel model, Records records,
                      irs::IndexFeatures features) {
  struct Block {
    uint64_t count = 0;
    size_t first = 0;
    U128 vfirst = 0;
    U128 vlast = 0;
    U128 min_gap = ~U128{0};
    U128 max_gap = 0;
    uint64_t nodes = 0;
    uint64_t records = 0;
    SoaRecords soa;
  };
  const bool soa = records != Records::Current;
  const bool gaps = records == Records::SoaGap;
  const auto key_bytes = [&](const Block& b, size_t last) -> uint64_t {
    const uint64_t groups = td::RecordGroups(b.count);
    switch (model) {
      case KeyModel::Stride: {
        const uint64_t prefix = irs::CommonPrefixLength(
          bytes_view{terms[b.first]}, bytes_view{terms[last]});
        const uint64_t stride = width - prefix;
        return VSize(groups) + VSize(stride) + VSize(prefix) + prefix +
               td::kHeadBytes * groups + b.count * stride;
      }
      case KeyModel::For:
        return width + 1 + PackedBytes(b.count, Bits128(b.vlast - b.vfirst));
      case KeyModel::GapFor: {
        const uint64_t anchors =
          (b.count + kAnchorInterval - 1) / kAnchorInterval;
        const uint64_t range = (Bits128(b.vlast - b.vfirst) + 7) / 8;
        return width + 2 + (b.count > 1 ? VSize128(b.min_gap) : 0) +
               (b.count > 1
                  ? PackedBytes(b.count - 1, Bits128(b.max_gap - b.min_gap))
                  : 0) +
               anchors * range;
      }
      case KeyModel::Trie:
        return VSize(b.nodes) + b.nodes + PackedBytes(b.nodes, 2);
    }
    return 0;
  };
  const auto block_bytes = [&](const Block& b, size_t last) {
    const uint64_t offsets =
      soa ? 0 : td::kRecordOffset * td::RecordGroups(b.count);
    return 1 + VSize(b.count) + key_bytes(b, last) + offsets + b.records +
           td::kSlack;
  };
  const auto extend = [&](const Block& b, size_t i) {
    Block next = b;
    const U128 value = width <= 16 ? ValueOf(terms[i]) : 0;
    if (next.count == 0) {
      next.first = i;
      next.vfirst = value;
      next.nodes = width;
    } else {
      const U128 gap = value - next.vlast;
      next.min_gap = std::min(next.min_gap, gap);
      next.max_gap = std::max(next.max_gap, gap);
      next.nodes += width - irs::CommonPrefixLength(bytes_view{terms[i - 1]},
                                                    bytes_view{terms[i]});
    }
    next.vlast = value;
    ++next.count;
    if (soa) {
      next.soa.Add(metas[i], features);
      next.records = next.soa.Bytes(gaps, features);
    } else {
      next.records += current[i];
    }
    return next;
  };
  FixedPacked packed;
  Block block;
  size_t last = 0;
  const auto close = [&] {
    const uint64_t size = block_bytes(block, last);
    ++packed.blocks;
    packed.written += size;
    packed.keys += key_bytes(block, last);
    packed.records += block.records;
    block = {};
  };
  for (size_t i = 0; i != terms.size(); ++i) {
    auto next = extend(block, i);
    if (block.count != 0 && block_bytes(next, i) > td::kPage) {
      close();
      next = extend(block, i);
    }
    block = std::move(next);
    last = i;
  }
  if (block.count != 0) {
    close();
  }
  return packed;
}

std::vector<bstring> Terms(const td::TermReaderImpl& reader) {
  std::vector<bstring> terms;
  terms.reserve(reader.size());
  auto it = reader.iterator();
  while (it->next()) {
    terms.emplace_back(it->value());
  }
  return terms;
}

void DumpKeys(const std::filesystem::path& path,
              std::span<const bstring> terms) {
  File file{std::fopen(path.c_str(), "wb")};
  if (!file) {
    absl::FPrintF(stderr, "cannot write %s\n", path.string());
    return;
  }
  for (const auto& term : terms) {
    const auto len = static_cast<uint32_t>(term.size());
    std::fwrite(&len, sizeof(len), 1, file.get());
    std::fwrite(term.data(), 1, term.size(), file.get());
  }
}

std::vector<bytes_view> SampleSuffixes(std::span<const bstring> terms,
                                       uint64_t term_bytes) {
  std::vector<bytes_view> suffixes;
  const size_t n = terms.size();
  if (n == 0 || term_bytes == 0) {
    return suffixes;
  }
  const uint64_t runs =
    std::max<uint64_t>(1, kSampleBytes * n / term_bytes / kSampleRun);
  const size_t stride = std::max<size_t>(kSampleRun, n / runs);
  for (size_t i = 0; i < n; i += stride) {
    const size_t end = std::min(n, i + kSampleRun);
    for (size_t j = i + 1; j < end; ++j) {
      const bytes_view term = terms[j];
      const auto suffix =
        term.substr(irs::CommonPrefixLength(bytes_view{terms[j - 1]}, term));
      if (!suffix.empty()) {
        suffixes.push_back(suffix);
      }
    }
  }
  return suffixes;
}

void Simulate(std::string_view segment, const Dict& dict, const Table& shared,
              const Options& options, Outputs& out) {
  const auto& head = *dict.head;
  const auto& reader = *dict.reader;
  const auto terms = Terms(reader);
  if (terms.size() != dict.records.size()) {
    ++out.checks.term_count;
    return;
  }
  if (!options.dump_keys.empty()) {
    DumpKeys(
      options.dump_keys / absl::StrCat(segment, "-f", head.id, "-m",
                                       static_cast<int>(head.member), ".keys"),
      terms);
  }
  if (!options.fsst) {
    return;
  }
  const auto prefix = static_cast<uint32_t>(reader.KeyPrefix().size());
  uint64_t term_bytes = 0;
  std::vector<Key> keys(terms.size());
  std::vector<bytes_view> suffixes(terms.size());
  std::vector<bytes_view> whole(terms.size());
  for (size_t i = 0; i != terms.size(); ++i) {
    const bytes_view term = terms[i];
    term_bytes += term.size();
    const size_t lcp =
      i == 0 ? 0 : irs::CommonPrefixLength(bytes_view{terms[i - 1]}, term);
    keys[i] = {
      .key = static_cast<uint32_t>(prefix + term.size()),
      .lcp = static_cast<uint32_t>(i == 0 ? 0 : prefix + lcp),
      .record = dict.records[i],
      .coded_sample = 0,
      .coded_full = 0,
      .coded_key = 0,
      .code_lcp = 0,
      .coded_segment = static_cast<uint32_t>(term.size() - lcp),
    };
    suffixes[i] = term.substr(lcp);
    whole[i] = term;
  }

  const auto sample = SampleSuffixes(terms, term_bytes);
  auto sampled = Train(sample);
  std::vector<bytes_view> all;
  all.reserve(terms.size());
  for (size_t i = 1; i < suffixes.size(); ++i) {
    if (!suffixes[i].empty()) {
      all.push_back(suffixes[i]);
    }
  }
  auto full = Train(all);
  if (sampled.encoder) {
    Encode(sampled.encoder.get(), suffixes, [&](size_t i, bytes_view codes) {
      keys[i].coded_sample = static_cast<uint32_t>(codes.size());
    });
  }
  if (full.encoder) {
    Encode(full.encoder.get(), suffixes, [&](size_t i, bytes_view codes) {
      keys[i].coded_full = static_cast<uint32_t>(codes.size());
    });
    bstring previous;
    Encode(full.encoder.get(), whole, [&](size_t i, bytes_view codes) {
      keys[i].coded_key = static_cast<uint32_t>(codes.size());
      keys[i].code_lcp =
        i == 0 ? 0
               : prefix + static_cast<uint32_t>(GuardedLcp(previous, codes));
      previous.assign(codes);
    });
  }

  bool segment_used = false;
  if (shared.encoder) {
    uint64_t raw = 0;
    uint64_t coded = 0;
    Encode(shared.encoder.get(), sample, [&](size_t i, bytes_view codes) {
      raw += sample[i].size();
      coded += codes.size();
    });
    segment_used =
      sample.size() < kMinSamples || raw * 100 >= coded * kMinRatioPct;
    if (segment_used) {
      Encode(shared.encoder.get(), suffixes, [&](size_t i, bytes_view codes) {
        keys[i].coded_segment = static_cast<uint32_t>(codes.size());
      });
    }
  }

  const auto emit = [&](Variant variant, uint32_t restart, double ratio,
                        Records records, std::string_view layout = "",
                        bool head_relative = false, uint32_t adaptive_min = 0) {
    auto packed = Pack(keys, dict.metas,
                       {.variant = variant,
                        .restart = restart,
                        .prefix = prefix,
                        .ratio = ratio,
                        .records = records,
                        .features = head.features,
                        .head_relative = head_relative,
                        .adaptive_min = adaptive_min,
                        .terms = terms});
    const double p5 = Percentile(packed.ratios, 0.05);
    const double p50 = Percentile(packed.ratios, 0.5);
    const double p95 = Percentile(packed.ratios, 0.95);
    absl::FPrintF(
      out.fsst.get(),
      "%s,%u,%d,%u,%u,%u,%u,%u,%u,%u,%d,%.4f,%u,%u,%u,%.4f,%s,%u,%u,%u,%u,%u,%"
      "u,"
      "%.4f,%.4f,%.4f,%s,%u,%d\n",
      segment, head.id, static_cast<int>(head.member), terms.size(), term_bytes,
      prefix, dict.blocks, dict.suffix_bytes, sampled.samples, sampled.raw,
      static_cast<int>(sampled.Gate()), sampled.Ratio(), sampled.bytes,
      full.samples, full.raw, full.Ratio(),
      absl::StrCat(kVariantNames[static_cast<size_t>(variant)], layout),
      restart, packed.blocks, packed.written, packed.suffixes,
      packed.overflow_blocks, packed.overflow_bytes, p5, p50, p95,
      kRecordNames[static_cast<size_t>(records)], packed.records,
      static_cast<int>(segment_used));
  };
  for (const auto restart : {16u, 32u, 64u, 128u}) {
    emit(Variant::Raw, restart, 1, Records::Current, "+head_relative", true);
    emit(Variant::Raw, restart, 1, Records::Current, "+adaptive", false,
         restart / 4);
    if (sampled.encoder) {
      emit(Variant::Sample, restart, 1, Records::Current, "+adaptive", false,
           restart / 4);
    }
  }
  for (const auto restart : kRestarts) {
    emit(Variant::Raw, restart, 1, Records::Current);
    if (sampled.encoder) {
      emit(Variant::Sample, restart, 1, Records::Current);
    }
    if (full.encoder) {
      emit(Variant::Full, restart, 1, Records::Current);
    }
  }
  emit(Variant::Segment, td::kRestartInterval, 1, Records::Current);
  for (const auto records : {Records::SoaFor, Records::SoaGap}) {
    emit(Variant::Raw, td::kRestartInterval, 1, records);
    if (sampled.encoder) {
      emit(Variant::Sample, td::kRestartInterval, 1, records);
      emit(Variant::Sample, 32, 1, records);
    }
  }
  if (full.encoder) {
    emit(Variant::EncodedFirst, td::kRestartInterval, 1, Records::Current);
  }
  if (sampled.encoder) {
    emit(Variant::Estimate, td::kRestartInterval, sampled.Ratio(),
         Records::Current);
  }
}

void SimulateFixed(std::string_view segment, const Dict& dict,
                   const Options& options, Outputs& out) {
  const auto& head = *dict.head;
  const auto terms = Terms(*dict.reader);
  if (terms.size() != dict.records.size()) {
    ++out.checks.term_count;
    return;
  }
  if (!options.dump_keys.empty() && head.layout == irs::DictLayout::Numeric) {
    DumpKeys(
      options.dump_keys / absl::StrCat(segment, "-f", head.id, "-m",
                                       static_cast<int>(head.member), ".keys"),
      terms);
  }
  for (const auto model :
       {KeyModel::Stride, KeyModel::For, KeyModel::GapFor, KeyModel::Trie}) {
    if (head.width > 16 &&
        (model == KeyModel::For || model == KeyModel::GapFor)) {
      continue;
    }
    for (const auto records :
         {Records::Current, Records::SoaFor, Records::SoaGap}) {
      const auto packed = PackFixed(terms, dict.metas, dict.records, head.width,
                                    model, records, head.features);
      absl::FPrintF(out.fixed.get(), "%s,%u,%d,%d,%u,%u,%u,%s,%s,%u,%u,%u,%u\n",
                    segment, head.id, static_cast<int>(head.layout),
                    static_cast<int>(head.member), head.width, terms.size(),
                    dict.blocks, kKeyModelNames[static_cast<size_t>(model)],
                    kRecordNames[static_cast<size_t>(records)], packed.blocks,
                    packed.written, packed.keys, packed.records);
    }
  }
}

// The writer's segment table: trained on the first variable-width dictionary
// whose sample passes the gate.
Table SegmentTable(std::span<const Dict> dicts) {
  for (const auto& dict : dicts) {
    if (dict.head->width != 0) {
      continue;
    }
    const auto terms = Terms(*dict.reader);
    uint64_t term_bytes = 0;
    for (const auto& term : terms) {
      term_bytes += term.size();
    }
    auto table = Train(SampleSuffixes(terms, term_bytes));
    if (table.encoder && table.Gate()) {
      return table;
    }
  }
  return {};
}

void Segment(const irs::Directory& dir, const irs::SegmentMeta& meta,
             const Options& options, Outputs& out) {
  irs::IdxReader heads_idx{dir, meta.name, nullptr};
  const auto terms = heads_idx.TakeTerms();
  if (terms.fields.empty()) {
    return;
  }
  irs::IdxReader idx{dir, meta.name, nullptr};
  td::FieldReader fields{irs::IResourceManager::gNoop};
  fields.prepare(irs::ReaderState{.dir = &dir, .meta = &meta, .idx = &idx});

  std::vector<Dict> dicts;
  for (const auto& head : terms.fields) {
    if (head.term_count <= 1) {
      continue;
    }
    const irs::DictRef ref{.layout = head.layout, .member = head.member};
    for (const auto* reader : fields.fields(head.id)) {
      if (reader->meta().dict == ref) {
        dicts.push_back({
          .head = &head,
          .reader = static_cast<const td::TermReaderImpl*>(reader),
        });
      }
    }
  }
  WalkBlocks(meta.name, fields, dicts, out);
  const auto shared = options.fsst ? SegmentTable(dicts) : Table{};
  for (const auto& dict : dicts) {
    const auto& head = *dict.head;
    uint64_t records = 0;
    for (const auto record : dict.records) {
      records += record;
    }
    absl::FPrintF(out.dicts.get(), "%s,%u,%d,%d,%u,%u,%u,%u,%u,%u,%u\n",
                  meta.name, head.id, static_cast<int>(head.layout),
                  static_cast<int>(head.member), head.term_count, head.width,
                  dict.blocks, head.first_block, head.last_block,
                  dict.suffix_bytes, records);
    if (head.width == 0) {
      Simulate(meta.name, dict, shared, options, out);
    } else if (options.fsst) {
      SimulateFixed(meta.name, dict, options, out);
      if (head.layout != irs::DictLayout::Numeric) {
        Simulate(meta.name, dict, shared, options, out);
      }
    }
  }
}

std::vector<std::filesystem::path> IndexDirs(
  std::span<const std::filesystem::path> roots) {
  std::vector<std::filesystem::path> dirs;
  for (const auto& root : roots) {
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator{root}) {
      if (entry.is_regular_file() &&
          entry.path().filename().string().starts_with("segments_")) {
        dirs.push_back(entry.path().parent_path());
      }
    }
  }
  absl::c_sort(dirs);
  dirs.erase(std::unique(dirs.begin(), dirs.end()), dirs.end());
  return dirs;
}

void Summary(Outputs& out) {
  absl::PrintF(
    "kind,blocks,written,page_bytes,padding_pct,mean,p10,p50,p90,"
    "raw_fits\n");
  for (size_t kind = 0; kind != out.kinds.size(); ++kind) {
    auto& stats = out.kinds[kind];
    if (stats.blocks == 0) {
      continue;
    }
    absl::PrintF(
      "%s,%u,%u,%u,%.2f,%.0f,%.0f,%.0f,%.0f,%u\n", kKindNames[kind],
      stats.blocks, stats.written, stats.page_bytes,
      100.0 * static_cast<double>(stats.page_bytes - stats.written) /
        static_cast<double>(stats.page_bytes),
      static_cast<double>(stats.written) / static_cast<double>(stats.blocks),
      Percentile(stats.sizes, 0.1), Percentile(stats.sizes, 0.5),
      Percentile(stats.sizes, 0.9), stats.raw_fits);
  }
  absl::PrintF("\nkind,%s\n", absl::StrJoin(kSectionNames, ","));
  for (size_t kind = 0; kind != out.kinds.size(); ++kind) {
    const auto& stats = out.kinds[kind];
    if (stats.blocks == 0) {
      continue;
    }
    std::vector<std::string> shares;
    for (const auto value : Values(stats.sections)) {
      shares.push_back(
        absl::StrFormat("%.2f", 100.0 * static_cast<double>(value) /
                                  static_cast<double>(stats.written)));
    }
    absl::PrintF("%s,%s\n", kKindNames[kind], absl::StrJoin(shares, ","));
  }
  absl::PrintF("\nkind,written_by_256B_bucket(1-256..3841-4096),over_4096\n");
  for (size_t kind = 0; kind != out.kinds.size(); ++kind) {
    const auto& stats = out.kinds[kind];
    if (stats.blocks == 0) {
      continue;
    }
    absl::PrintF("%s,%s\n", kKindNames[kind],
                 absl::StrJoin(stats.histogram, ","));
  }
  const auto& c = out.checks;
  absl::PrintF(
    "\nchecks: last_block=%u nonzero_tail=%u section_sum=%u tail_owner=%u "
    "term_count=%u errors=%u\n",
    c.last_block, c.nonzero_tail, c.section_sum, c.tail_owner, c.term_count,
    c.errors);
}

File OpenCsv(const std::filesystem::path& path, std::string_view header) {
  File file{std::fopen(path.c_str(), "w")};
  if (file) {
    absl::FPrintF(file.get(), "%s\n", header);
  } else {
    absl::FPrintF(stderr, "cannot write %s\n", path.string());
  }
  return file;
}

int Main(int argc, char** argv) {
  Options options;
  std::vector<std::filesystem::path> roots;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg{argv[i]};
    if (arg == "--fsst") {
      options.fsst = true;
    } else if (arg == "--out" && i + 1 < argc) {
      options.out = argv[++i];
    } else if (arg == "--dump-keys" && i + 1 < argc) {
      options.dump_keys = argv[++i];
    } else {
      roots.emplace_back(arg);
    }
  }
  if (roots.empty() || options.out.empty()) {
    absl::FPrintF(stderr,
                  "usage: term_dict_stats --out <dir> [--fsst] "
                  "[--dump-keys <dir>] <index dir or root>...\n");
    return 1;
  }
  std::filesystem::create_directories(options.out);
  if (!options.dump_keys.empty()) {
    std::filesystem::create_directories(options.dump_keys);
  }
  Outputs out;
  out.blocks =
    OpenCsv(options.out / "blocks.csv",
            absl::StrCat("segment,block,kind,entries,dicts,fields,width,",
                         absl::StrJoin(kSectionNames, ","),
                         ",written,pages_bytes,padding,raw_fits,raw_size"));
  out.dicts = OpenCsv(options.out / "dicts.csv",
                      "segment,field,layout,member,terms,width,blocks,"
                      "first_block,last_block,suffix_bytes,record_bytes");
  out.fsst = OpenCsv(
    options.out / "fsst.csv",
    "segment,field,member,terms,term_bytes,prefix,actual_blocks,"
    "actual_suffix_bytes,sample_count,sample_bytes,sample_gate,sample_ratio,"
    "table_bytes,full_count,full_bytes,full_ratio,variant,restart,blocks,"
    "written,suffix_bytes,overflow_blocks,overflow_bytes,ratio_p5,ratio_p50,"
    "ratio_p95,records_model,record_bytes,segment_table_used");
  out.fixed = OpenCsv(options.out / "fixed.csv",
                      "segment,field,layout,member,width,terms,actual_blocks,"
                      "key_model,records_model,blocks,written,key_bytes,"
                      "record_bytes");
  if (!out.blocks || !out.dicts || !out.fsst || !out.fixed) {
    return 1;
  }

  irs::DuckDBEngine::Instance().Initialize();
  for (const auto& path : IndexDirs(roots)) {
    absl::FPrintF(stderr, "index %s\n", path.string());
    irs::MMapDirectory dir{path};
    irs::DirectoryReader reader{
      dir,
      irs::IndexReaderOptions{
        .db = &irs::DuckDBEngine::Instance().instance(),
        .index = false,
      },
      [](duckdb::BinaryDeserializer& in) {
        sdb::search::IndexStamp::Read(in);
      }};
    for (const auto& segment : reader) {
      try {
        irs::SegmentMeta meta;
        static_cast<irs::SegmentInfo&>(meta) = segment.Meta();
        Segment(dir, meta, options, out);
      } catch (const std::exception& e) {
        ++out.checks.errors;
        absl::FPrintF(stderr, "segment %s: %s\n", segment.Meta().name,
                      e.what());
      }
    }
  }
  Summary(out);
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);

}  // namespace
