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

#include <absl/base/internal/endian.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>
#include <vector>

#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/duckdb_engine.hpp"

namespace {

constexpr uint32_t kTerms = 32;
constexpr uint32_t kSyntheticBlocks = 1 << 15;
constexpr auto kFeatures = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
constexpr uint32_t kFields = 7;

struct Stats {
  uint32_t docs;
  uint32_t freq;
  uint64_t doc_start;
  uint64_t pos_start;
  uint32_t pos_offset;
  uint32_t doc_delta;
  uint32_t pos_extent;
  uint8_t inline_size;
};

std::vector<Stats> MakeBlock(std::mt19937_64& rng, uint64_t& doc_at,
                             uint64_t& pos_at, uint32_t& pos_index) {
  std::uniform_real_distribution<double> u{0, 1};
  std::geometric_distribution<uint32_t> extra{0.4};
  std::vector<Stats> terms(kTerms);
  for (auto& s : terms) {
    const double x = u(rng);
    s.docs = x < 0.5 ? 1
                     : (x < 0.95 ? 2 + static_cast<uint32_t>(u(rng) * 60)
                                 : 257 + static_cast<uint32_t>(u(rng) * 20000));
    s.freq = s.docs + extra(rng) * s.docs / 2;
    s.inline_size =
      s.docs >= 2 && s.docs <= 20 ? static_cast<uint8_t>(4 + s.docs) : 0;
    s.doc_delta = s.docs == 1 ? static_cast<uint32_t>(u(rng) * 5e6)
                              : (s.docs > 256 ? s.docs * 2 : 0);
    s.doc_start = doc_at;
    if (s.docs >= 2 && s.inline_size == 0) {
      doc_at += s.docs * 2 + 40;
    }
    s.pos_start = pos_at;
    s.pos_offset = pos_index;
    s.pos_extent = s.freq > irs::pos_limits::kBlockSize ? s.freq * 2 : 0;
    pos_index += s.freq;
    if (pos_index >= irs::PosGroup::kPositions) {
      pos_at += 3000 * (pos_index / irs::PosGroup::kPositions);
      pos_index %= irs::PosGroup::kPositions;
    }
  }
  return terms;
}

void EncodeVarint(const std::vector<Stats>& terms, irs::bstring& out) {
  irs::BytesOutput o{out};
  irs::PostingMeta last;
  for (const auto& s : terms) {
    const bool inlined = s.inline_size != 0;
    const bool single = s.docs == 1;
    const uint64_t next = uint64_t{last.pos_offset} + last.freq;
    const bool follows =
      last.docs_count != 0 &&
      s.pos_offset == next % irs::PosGroup::kPositions &&
      (next >= irs::PosGroup::kPositions || s.pos_start == last.pos_start);
    const uint64_t flags = uint64_t{follows} << 2 | uint64_t{s.freq == s.docs}
                                                      << 1;
    o.WriteV64(single ? uint64_t{s.doc_delta} << 3 | flags | 1
                      : uint64_t{s.docs} << 4 | uint64_t{inlined} << 3 | flags);
    if (s.freq != s.docs) {
      o.WriteV32(s.freq - s.docs - 1);
    }
    if (inlined) {
      o.WriteByte(s.inline_size);
    } else if (!single) {
      o.WriteV64(s.doc_start - last.doc_start);
    }
    if (!follows || next >= irs::PosGroup::kPositions) {
      o.WriteV64(s.pos_start - last.pos_start);
    }
    if (!follows) {
      o.WriteV32(s.pos_offset);
    }
    if (s.docs > irs::doc_limits::kBlockSize) {
      o.WriteV32(s.doc_delta);
    }
    if (s.freq > irs::pos_limits::kBlockSize) {
      o.WriteV32(s.pos_extent);
    }
    const auto doc_start = last.doc_start;
    last.docs_count = s.docs;
    last.freq = s.freq;
    last.doc_start = inlined || single ? doc_start : s.doc_start;
    last.pos_start = s.pos_start;
    last.pos_offset = static_cast<uint16_t>(s.pos_offset);
  }
}

void EncodeLegacy(const std::vector<Stats>& terms, irs::bstring& out) {
  irs::BytesOutput o{out};
  irs::PostingMeta last;
  for (const auto& s : terms) {
    const bool inlined = s.inline_size != 0;
    o.WriteV32(s.docs << 1 | static_cast<uint32_t>(inlined));
    o.WriteV32(s.freq - s.docs);
    if (inlined) {
      o.WriteByte(s.inline_size);
    } else {
      o.WriteV64(s.doc_start - last.doc_start);
    }
    const uint64_t pos_delta = s.pos_start - last.pos_start;
    o.WriteV64(pos_delta);
    o.WriteV32(pos_delta == 0 ? s.pos_offset - last.pos_offset : s.pos_offset);
    if (s.docs == 1 || s.docs > irs::doc_limits::kBlockSize) {
      o.WriteV32(s.doc_delta);
    }
    if (s.freq > irs::pos_limits::kBlockSize) {
      o.WriteV32(s.pos_extent);
    }
    const auto doc_start = last.doc_start;
    last.doc_start = inlined ? doc_start : s.doc_start;
    last.pos_start = s.pos_start;
    last.pos_offset = static_cast<uint16_t>(s.pos_offset);
  }
}

IRS_NO_INLINE size_t DecodeLegacy(const irs::byte_type* in,
                                  irs::IndexFeatures features,
                                  irs::PostingMeta& meta) {
  using irs::IndexFeatures;
  const auto* p = in;
  const auto head = irs::vread<uint32_t>(p);
  meta.docs_count = head >> 1;
  if (IndexFeatures::None != (features & IndexFeatures::Freq)) {
    meta.freq = meta.docs_count + irs::vread<uint32_t>(p);
  }
  if ((head & 1) != 0) {
    meta.inline_size = *p++;
  } else {
    meta.inline_size = 0;
    meta.doc_start += irs::vread<uint64_t>(p);
  }
  if (IndexFeatures::None != (features & IndexFeatures::Pos)) {
    const auto pos_delta = irs::vread<uint64_t>(p);
    meta.pos_start += pos_delta;
    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      meta.pay_start += irs::vread<uint64_t>(p);
    }
    const auto pos_offset = irs::vread<uint32_t>(p);
    meta.pos_offset = static_cast<uint16_t>(
      pos_delta == 0 ? meta.pos_offset + pos_offset : pos_offset);
  } else if (IndexFeatures::None != (features & IndexFeatures::Vec)) {
    meta.pay_start += irs::vread<uint64_t>(p);
    meta.pos_offset = *p++;
  }
  if (meta.docs_count == 1 || meta.docs_count > irs::doc_limits::kBlockSize) {
    meta.doc_delta = irs::vread<uint32_t>(p);
  }
  if (IndexFeatures::None != (features & IndexFeatures::Pos) &&
      meta.freq > irs::pos_limits::kBlockSize) {
    meta.pos_extent = irs::vread<uint32_t>(p);
    if (IndexFeatures::None != (features & IndexFeatures::Offs)) {
      meta.pay_extent = irs::vread<uint32_t>(p);
    }
  }
  return static_cast<size_t>(p - in);
}

struct Columns {
  uint64_t doc_base;
  uint64_t pos_base;
  uint8_t width[kFields];
  uint32_t offset[kFields];
};

uint64_t FieldValue(const Stats& s, uint32_t field, const Columns& c) {
  switch (field) {
    case 0:
      return s.docs;
    case 1:
      return s.freq - s.docs;
    case 2:
      return s.inline_size;
    case 3:
      return s.inline_size != 0 || s.docs == 1 ? 0 : s.doc_start - c.doc_base;
    case 4:
      return s.pos_start - c.pos_base;
    case 5:
      return s.pos_offset;
    default:
      return s.doc_delta;
  }
}

void EncodeColumns(const std::vector<Stats>& terms, irs::bstring& out) {
  Columns c{};
  c.doc_base = terms.front().doc_start;
  c.pos_base = terms.front().pos_start;
  uint32_t at = 0;
  for (uint32_t f = 0; f != kFields; ++f) {
    uint64_t max = 0;
    for (const auto& s : terms) {
      max = std::max(max, FieldValue(s, f, c));
    }
    c.width[f] = static_cast<uint8_t>(std::bit_width(max));
    c.offset[f] = at;
    at += (kTerms * c.width[f] + 7) / 8;
  }
  const size_t header = sizeof(Columns);
  out.resize(header + at + 8, 0);
  std::memcpy(out.data(), &c, sizeof(c));
  for (uint32_t f = 0; f != kFields; ++f) {
    for (uint32_t i = 0; i != kTerms; ++i) {
      const uint64_t v = FieldValue(terms[i], f, c);
      const uint64_t bit = uint64_t{i} * c.width[f];
      auto* p = out.data() + header + c.offset[f] + bit / 8;
      uint64_t word = absl::little_endian::Load64(p);
      word |= v << (bit % 8);
      absl::little_endian::Store64(p, word);
    }
  }
  out.resize(header + at + 8);
}

IRS_FORCE_INLINE uint64_t Column(const irs::byte_type* data, uint32_t offset,
                                 uint32_t width, uint32_t i) noexcept {
  const uint64_t bit = uint64_t{i} * width;
  const uint64_t word = absl::little_endian::Load64(data + offset + bit / 8);
  return width == 0 ? 0 : (word >> (bit % 8)) & ((uint64_t{1} << width) - 1);
}

IRS_FORCE_INLINE void DecodeColumn(const irs::byte_type* block, uint32_t i,
                                   irs::PostingMeta& meta) noexcept {
  Columns c;
  std::memcpy(&c, block, sizeof(c));
  const auto* data = block + sizeof(Columns);
  meta.docs_count =
    static_cast<uint32_t>(Column(data, c.offset[0], c.width[0], i));
  meta.freq = meta.docs_count +
              static_cast<uint32_t>(Column(data, c.offset[1], c.width[1], i));
  meta.inline_size =
    static_cast<uint8_t>(Column(data, c.offset[2], c.width[2], i));
  meta.doc_start = c.doc_base + Column(data, c.offset[3], c.width[3], i);
  meta.pos_start = c.pos_base + Column(data, c.offset[4], c.width[4], i);
  meta.pos_offset =
    static_cast<uint32_t>(Column(data, c.offset[5], c.width[5], i));
  meta.doc_delta =
    static_cast<uint32_t>(Column(data, c.offset[6], c.width[6], i));
}

constexpr uint32_t kDeltaFields = 6;

struct DeltaColumns {
  uint64_t doc_base;
  uint64_t pos_base;
  uint8_t width[kDeltaFields + 1];
  uint32_t offset[kDeltaFields + 1];
};

bool NeedsDocDelta(uint32_t docs) noexcept {
  return docs == 1 || docs > irs::doc_limits::kBlockSize;
}

void EncodeDeltaColumns(const std::vector<Stats>& terms, irs::bstring& out) {
  DeltaColumns c{};
  c.doc_base = terms.front().doc_start;
  c.pos_base = terms.front().pos_start;
  std::vector<uint64_t> values[kDeltaFields + 1];
  uint64_t doc_prev = c.doc_base;
  uint64_t pos_prev = c.pos_base;
  for (const auto& s : terms) {
    values[0].push_back(s.docs);
    values[1].push_back(s.freq - s.docs);
    values[2].push_back(s.inline_size);
    if (s.inline_size == 0 && s.docs != 1) {
      values[3].push_back(s.doc_start - doc_prev);
      doc_prev = s.doc_start;
    } else {
      values[3].push_back(0);
    }
    values[4].push_back(s.pos_start - pos_prev);
    pos_prev = s.pos_start;
    values[5].push_back(s.pos_offset);
    if (NeedsDocDelta(s.docs)) {
      values[6].push_back(s.doc_delta);
    }
  }
  uint32_t at = 0;
  for (uint32_t f = 0; f != kDeltaFields + 1; ++f) {
    uint64_t max = 0;
    for (const auto v : values[f]) {
      max = std::max(max, v);
    }
    c.width[f] = static_cast<uint8_t>(std::bit_width(max));
    c.offset[f] = at;
    at += (static_cast<uint32_t>(values[f].size()) * c.width[f] + 7) / 8;
  }
  const size_t header = sizeof(DeltaColumns);
  out.resize(header + at + 8, 0);
  std::memcpy(out.data(), &c, sizeof(c));
  for (uint32_t f = 0; f != kDeltaFields + 1; ++f) {
    for (size_t i = 0; i != values[f].size(); ++i) {
      const uint64_t bit = uint64_t{i} * c.width[f];
      auto* p = out.data() + header + c.offset[f] + bit / 8;
      uint64_t word = absl::little_endian::Load64(p);
      word |= values[f][i] << (bit % 8);
      absl::little_endian::Store64(p, word);
    }
  }
}

IRS_FORCE_INLINE void DecodeDeltaColumn(const irs::byte_type* block, uint32_t i,
                                        irs::PostingMeta& meta) noexcept {
  DeltaColumns c;
  std::memcpy(&c, block, sizeof(c));
  const auto* data = block + sizeof(DeltaColumns);
  uint64_t doc_sum = 0;
  uint64_t pos_sum = 0;
  uint32_t needs = 0;
  for (uint32_t j = 0; j != i; ++j) {
    doc_sum += Column(data, c.offset[3], c.width[3], j);
    pos_sum += Column(data, c.offset[4], c.width[4], j);
    needs += NeedsDocDelta(
      static_cast<uint32_t>(Column(data, c.offset[0], c.width[0], j)));
  }
  meta.docs_count =
    static_cast<uint32_t>(Column(data, c.offset[0], c.width[0], i));
  meta.freq = meta.docs_count +
              static_cast<uint32_t>(Column(data, c.offset[1], c.width[1], i));
  meta.inline_size =
    static_cast<uint8_t>(Column(data, c.offset[2], c.width[2], i));
  meta.doc_start =
    c.doc_base + doc_sum + Column(data, c.offset[3], c.width[3], i);
  meta.pos_start =
    c.pos_base + pos_sum + Column(data, c.offset[4], c.width[4], i);
  meta.pos_offset =
    static_cast<uint32_t>(Column(data, c.offset[5], c.width[5], i));
  meta.doc_delta =
    NeedsDocDelta(meta.docs_count)
      ? static_cast<uint32_t>(Column(data, c.offset[6], c.width[6], needs))
      : 0;
}

void DecodeDeltaAll(const irs::byte_type* block, irs::PostingMeta* metas) {
  DeltaColumns c;
  std::memcpy(&c, block, sizeof(c));
  const auto* data = block + sizeof(DeltaColumns);
  uint64_t doc = c.doc_base;
  uint64_t pos = c.pos_base;
  uint32_t needs = 0;
  for (uint32_t i = 0; i != kTerms; ++i) {
    auto& meta = metas[i];
    meta.docs_count =
      static_cast<uint32_t>(Column(data, c.offset[0], c.width[0], i));
    meta.freq = meta.docs_count +
                static_cast<uint32_t>(Column(data, c.offset[1], c.width[1], i));
    meta.inline_size =
      static_cast<uint8_t>(Column(data, c.offset[2], c.width[2], i));
    doc += Column(data, c.offset[3], c.width[3], i);
    meta.doc_start = doc;
    pos += Column(data, c.offset[4], c.width[4], i);
    meta.pos_start = pos;
    meta.pos_offset =
      static_cast<uint32_t>(Column(data, c.offset[5], c.width[5], i));
    if (NeedsDocDelta(meta.docs_count)) {
      meta.doc_delta =
        static_cast<uint32_t>(Column(data, c.offset[6], c.width[6], needs++));
    }
  }
}

std::vector<std::vector<Stats>> LoadIndex(const char* path, const char* field) {
  irs::MMapDirectory dir{path};
  irs::DirectoryReader reader{
    dir,
    irs::IndexReaderOptions{.db = &irs::DuckDBEngine::Instance().instance()}};
  std::vector<std::vector<Stats>> blocks;
  for (const auto& segment : reader) {
    for (const auto id : segment.field_ids()) {
      if (field != nullptr && id != std::strtoul(field, nullptr, 10)) {
        continue;
      }
      std::vector<Stats> block;
      for (auto it = segment.field(id)->iterator(); it->next();) {
        const auto& meta = it->cookie();
        block.push_back({.docs = meta.docs_count,
                         .freq = meta.freq,
                         .doc_start = meta.doc_start,
                         .pos_start = meta.pos_start,
                         .pos_offset = meta.pos_offset,
                         .doc_delta = meta.doc_delta,
                         .pos_extent = meta.pos_extent,
                         .inline_size = meta.inline_size});
        if (block.size() == kTerms) {
          blocks.push_back(std::move(block));
          block.clear();
        }
      }
    }
  }
  return blocks;
}

struct Fixture {
  std::vector<std::vector<Stats>> stats;
  std::vector<irs::bstring> varint;
  std::vector<irs::bstring> legacy;
  std::vector<irs::bstring> columns;
  std::vector<irs::bstring> delta_columns;
  std::vector<uint32_t> picks;
  size_t blocks = 0;

  Fixture() {
    std::mt19937_64 rng{42};
    if (const char* path = std::getenv("TERM_STATS_INDEX")) {
      stats = LoadIndex(path, std::getenv("TERM_STATS_FIELD"));
    } else {
      uint64_t doc_at = 0;
      uint64_t pos_at = 0;
      uint32_t pos_index = 0;
      for (uint32_t b = 0; b != kSyntheticBlocks; ++b) {
        stats.push_back(MakeBlock(rng, doc_at, pos_at, pos_index));
      }
    }
    blocks = stats.size();
    for (const auto& block : stats) {
      EncodeVarint(block, varint.emplace_back());
      EncodeLegacy(block, legacy.emplace_back());
      EncodeColumns(block, columns.emplace_back());
      EncodeDeltaColumns(block, delta_columns.emplace_back());
    }
    std::uniform_int_distribution<uint32_t> pick{0, kTerms - 1};
    picks.resize(blocks);
    for (auto& p : picks) {
      p = pick(rng);
    }
  }

  size_t Bytes(const std::vector<irs::bstring>& blocks) const {
    size_t bytes = 0;
    for (const auto& b : blocks) {
      bytes += b.size();
    }
    return bytes;
  }
};

Fixture& GetFixture() {
  static Fixture f;
  return f;
}

bool Same(const irs::PostingMeta& meta, const Stats& s) {
  return meta.docs_count == s.docs && meta.freq == s.freq &&
         meta.inline_size == s.inline_size &&
         (s.inline_size != 0 || s.docs == 1 || meta.doc_start == s.doc_start) &&
         meta.pos_start == s.pos_start && meta.pos_offset == s.pos_offset &&
         (s.docs == 1 || s.docs > irs::doc_limits::kBlockSize
            ? meta.doc_delta == s.doc_delta
            : true);
}

void BmVarintAll(benchmark::State& state) {
  auto& f = GetFixture();
  irs::PostingsReader reader;
  size_t b = 0;
  uint64_t sink = 0;
  for (auto _ : state) {
    irs::PostingMeta meta;
    const auto* p = f.varint[b].data();
    for (uint32_t i = 0; i != kTerms; ++i) {
      p += reader.decode(p, kFeatures, meta);
      sink += meta.doc_start + meta.pos_offset;
    }
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kTerms);
  state.counters["bytes_per_term"] =
    static_cast<double>(f.Bytes(f.varint)) / (f.blocks * kTerms);
}

void BmLegacyAll(benchmark::State& state) {
  auto& f = GetFixture();
  size_t b = 0;
  uint64_t sink = 0;
  for (auto _ : state) {
    irs::PostingMeta meta;
    const auto* p = f.legacy[b].data();
    for (uint32_t i = 0; i != kTerms; ++i) {
      p += DecodeLegacy(p, kFeatures, meta);
      sink += meta.doc_start + meta.pos_offset;
    }
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kTerms);
  state.counters["bytes_per_term"] =
    static_cast<double>(f.Bytes(f.legacy)) / (f.blocks * kTerms);
}

void BmLegacyOne(benchmark::State& state) {
  auto& f = GetFixture();
  size_t b = 0;
  uint64_t sink = 0;
  for (auto _ : state) {
    irs::PostingMeta meta;
    const auto* p = f.legacy[b].data();
    for (uint32_t i = 0; i <= f.picks[b]; ++i) {
      p += DecodeLegacy(p, kFeatures, meta);
    }
    sink += meta.doc_start + meta.pos_offset;
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
}

void BmColumnsAll(benchmark::State& state) {
  auto& f = GetFixture();
  size_t b = 0;
  uint64_t sink = 0;
  for (auto _ : state) {
    irs::PostingMeta meta;
    const auto* block = f.columns[b].data();
    for (uint32_t i = 0; i != kTerms; ++i) {
      DecodeColumn(block, i, meta);
      sink += meta.doc_start + meta.pos_offset;
    }
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kTerms);
  state.counters["bytes_per_term"] =
    static_cast<double>(f.Bytes(f.columns) -
                        f.blocks * (sizeof(Columns) + 8 - 16)) /
    (f.blocks * kTerms);
}

void BmVarintOne(benchmark::State& state) {
  auto& f = GetFixture();
  irs::PostingsReader reader;
  size_t b = 0;
  uint64_t sink = 0;
  for (auto _ : state) {
    irs::PostingMeta meta;
    const auto* p = f.varint[b].data();
    for (uint32_t i = 0; i <= f.picks[b]; ++i) {
      p += reader.decode(p, kFeatures, meta);
    }
    sink += meta.doc_start + meta.pos_offset;
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
}

void BmColumnsOne(benchmark::State& state) {
  auto& f = GetFixture();
  size_t b = 0;
  uint64_t sink = 0;
  for (auto _ : state) {
    irs::PostingMeta meta;
    DecodeColumn(f.columns[b].data(), f.picks[b], meta);
    sink += meta.doc_start + meta.pos_offset;
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
}

void BmCheck(benchmark::State& state) {
  auto& f = GetFixture();
  irs::PostingsReader reader;
  for (uint32_t b = 0; b < f.blocks; b += 101) {
    irs::PostingMeta varint;
    irs::PostingMeta legacy;
    irs::PostingMeta column;
    const auto* p = f.varint[b].data();
    const auto* q = f.legacy[b].data();
    irs::PostingMeta all[kTerms];
    DecodeDeltaAll(f.delta_columns[b].data(), all);
    for (uint32_t i = 0; i != kTerms; ++i) {
      p += reader.decode(p, kFeatures, varint);
      q += DecodeLegacy(q, kFeatures, legacy);
      DecodeColumn(f.columns[b].data(), i, column);
      irs::PostingMeta delta;
      DecodeDeltaColumn(f.delta_columns[b].data(), i, delta);
      if (!Same(varint, f.stats[b][i]) || !Same(legacy, f.stats[b][i]) ||
          !Same(column, f.stats[b][i]) || !Same(delta, f.stats[b][i]) ||
          !Same(all[i], f.stats[b][i])) {
        state.SkipWithError("mismatch");
        return;
      }
    }
  }
  for (auto _ : state) {
  }
}

void BmDeltaAll(benchmark::State& state) {
  auto& f = GetFixture();
  size_t b = 0;
  uint64_t sink = 0;
  irs::PostingMeta metas[kTerms];
  for (auto _ : state) {
    DecodeDeltaAll(f.delta_columns[b].data(), metas);
    sink += metas[kTerms - 1].doc_start + metas[3].pos_offset;
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kTerms);
  state.counters["bytes_per_term"] =
    static_cast<double>(f.Bytes(f.delta_columns) -
                        f.blocks * (sizeof(DeltaColumns) + 8 - 16)) /
    (f.blocks * kTerms);
}

void BmDeltaOne(benchmark::State& state) {
  auto& f = GetFixture();
  size_t b = 0;
  uint64_t sink = 0;
  for (auto _ : state) {
    irs::PostingMeta meta;
    DecodeDeltaColumn(f.delta_columns[b].data(), f.picks[b], meta);
    sink += meta.doc_start + meta.pos_offset;
    if (++b == f.blocks) {
      b = 0;
    }
  }
  benchmark::DoNotOptimize(sink);
}

BENCHMARK(BmCheck)->Iterations(1);
BENCHMARK(BmVarintAll);
BENCHMARK(BmLegacyAll);
BENCHMARK(BmColumnsAll);
BENCHMARK(BmDeltaAll);
BENCHMARK(BmVarintOne);
BENCHMARK(BmLegacyOne);
BENCHMARK(BmColumnsOne);
BENCHMARK(BmDeltaOne);

}  // namespace

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
