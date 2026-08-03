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

#include <absl/strings/ascii.h>

#include <cctype>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "iresearch/analysis/numeric_terms.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "iresearch/index/typed_terms.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

using namespace irs;

bool FillSplitBatch(std::string_view text, size_t& cursor, TokenLayout layout,
                    TokenBatch& out) {
  auto alnum = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c));
  };
  while (cursor < text.size()) {
    while (cursor < text.size() && !alnum(text[cursor])) {
      ++cursor;
    }
    if (cursor >= text.size()) {
      break;
    }
    const auto start = cursor;
    while (cursor < text.size() && alnum(text[cursor])) {
      ++cursor;
    }
    if (out.count == TokenBatch::kCapacity) {
      cursor = start;
      return true;
    }
    const auto i = out.count++;
    out.terms[i] = duckdb::string_t{text.data() + start,
                                    static_cast<uint32_t>(cursor - start)};
    if (layout == TokenLayout::TermsPosOffs) {
      out.offs_start[i] = static_cast<uint32_t>(start);
      out.offs_end[i] = static_cast<uint32_t>(cursor);
    }
  }
  return false;
}

bytes_view ToBytesView(std::string_view s) {
  return {reinterpret_cast<const byte_type*>(s.data()), s.size()};
}

// Scalar shim over the block entry: one value for one doc.
bool InvertKeyword(FieldInverter& field, doc_id_t doc, bytes_view value) {
  const duckdb::string_t v{reinterpret_cast<const char*>(value.data()),
                           static_cast<uint32_t>(value.size())};
  return field.InvertKeywordBlock({&v, 1}, {&doc, 1});
}

// MakeTermView bit-assembles string_t's layout; it must be byte-identical to
// duckdb's own ctor across the inline boundary, or a duckdb layout change has
// silently broken it (the static_asserts in term_view.hpp catch dimension
// changes; this catches field-offset changes).
TEST(TermViewTest, MakeTermViewMatchesDuckDbCtor) {
  for (const uint32_t size :
       {0u, 1u, 2u, 4u, 7u, 8u, 9u, 11u, 12u, 13u, 20u, 64u}) {
    std::string s(size, '\0');
    for (uint32_t i = 0; i < size; ++i) {
      s[i] = static_cast<char>('a' + (i % 26));
    }
    const auto ours = MakeTermView(s.data(), size);
    const duckdb::string_t theirs{s.data(), size};
    ASSERT_EQ(size, ours.GetSize()) << size;
    ASSERT_EQ(0, std::memcmp(&ours, &theirs, sizeof ours)) << "size=" << size;
  }
  // Empty terms legally carry a null data pointer; must not be dereferenced.
  const auto empty = MakeTermView(static_cast<const char*>(nullptr), 0);
  const duckdb::string_t empty_ref{static_cast<const char*>(nullptr), 0};
  ASSERT_EQ(0, empty.GetSize());
  ASSERT_EQ(0, std::memcmp(&empty, &empty_ref, sizeof empty));
}

// The bounded builder reads up to 12 bytes of the surrounding value buffer;
// it must produce the identical string_t for every (size, remaining-slack)
// combination, including tail tokens that force the exact-bounds fallback.
TEST(TermViewTest, MakeTermViewWithSlackMatchesDuckDbCtor) {
  std::string buf;
  for (uint32_t i = 0; i < 64; ++i) {
    buf += static_cast<char>('A' + (i % 26));
  }
  const char* end = buf.data() + buf.size();
  for (uint32_t size : {0u, 1u, 2u, 3u, 4u, 7u, 8u, 9u, 11u, 12u, 13u, 20u}) {
    for (uint32_t back : {0u, 1u, 4u, 11u, 12u, 13u, 40u}) {
      if (size + back > buf.size()) {
        continue;
      }
      const char* data = end - back - size;
      const auto ours = MakeTermView(data, size, end);
      const duckdb::string_t theirs{data, size};
      ASSERT_EQ(0, std::memcmp(&ours, &theirs, sizeof ours))
        << "size=" << size << " slack=" << back;
    }
  }
}

// MakeTermViewPadded's contract is >= kTermViewSlack readable bytes past the
// term (TokenSink::Reserve's padding); under it the masked build applies
// unconditionally and must still be byte-identical to duckdb's ctor.
TEST(TermViewTest, MakeTermViewPaddedMatchesDuckDbCtor) {
  std::string buf;
  for (uint32_t i = 0; i < 64 + kTermViewSlack; ++i) {
    buf += static_cast<char>('a' + (i % 26));
  }
  for (uint32_t size = 0; size <= 64; ++size) {
    for (uint32_t off : {0u, 1u, 5u, 13u}) {
      const char* data = buf.data() + off;
      const auto ours = MakeTermViewPadded(data, size);
      const duckdb::string_t theirs{data, size};
      ASSERT_EQ(0, std::memcmp(&ours, &theirs, sizeof ours))
        << "size=" << size << " off=" << off;
    }
  }
}

// FoldTermViewAscii folds the full 16-byte slot; every byte value at every
// position of every inline size must match absl's per-byte fold applied
// before the build, in both directions, with the surrounding bytes (and the
// length word) untouched.
TEST(TermViewTest, FoldTermViewAsciiMatchesAbslFold) {
  for (uint32_t size = 0; size <= duckdb::string_t::INLINE_LENGTH; ++size) {
    std::string s(size, 'q');
    std::string folded(size, '\0');
    const uint32_t at_max = size == 0 ? 0 : size - 1;
    for (uint32_t at = 0; at <= at_max; ++at) {
      for (int c = 0; c < 256; ++c) {
        if (size != 0) {
          s[at] = static_cast<char>(c);
        }
        const auto view = MakeTermView(s.data(), size);

        absl::ascii_internal::AsciiStrToLower(folded.data(), s.data(), size);
        duckdb::string_t lower_ref{folded.data(), size};
        auto ours = FoldTermViewAscii<true>(view);
        ASSERT_EQ(0, std::memcmp(&ours, &lower_ref, sizeof ours))
          << "size=" << size << " at=" << at << " c=" << c;

        absl::ascii_internal::AsciiStrToUpper(folded.data(), s.data(), size);
        duckdb::string_t upper_ref{folded.data(), size};
        ours = FoldTermViewAscii<false>(view);
        ASSERT_EQ(0, std::memcmp(&ours, &upper_ref, sizeof ours))
          << "size=" << size << " at=" << at << " c=" << c;

        if (size == 0) {
          break;
        }
      }
      s[at] = 'q';
    }
  }
}

TEST(InverterTermDictionaryTest, ResolveDedupIntern) {
  SimpleMemoryAccounter memory;
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  TermDictionary dict{arena, memory};

  const auto id0 = dict.Insert(MakeTermView(ToBytesView("quick")));
  const auto id1 = dict.Insert(MakeTermView(ToBytesView("brown")));
  const auto id2 = dict.Insert(MakeTermView(ToBytesView("quick")));
  ASSERT_EQ(id0, id2);
  ASSERT_NE(id0, id1);
  ASSERT_EQ(2, dict.Size());

  const std::string long_term(100000, 'x');
  const auto id3 = dict.Insert(MakeTermView(ToBytesView(long_term)));
  const auto& interned = dict.Entries()[id3].term;
  ASSERT_EQ(
    ToBytesView(long_term),
    irs::bytes_view(reinterpret_cast<const irs::byte_type*>(interned.GetData()),
                    interned.GetSize()));
  ASSERT_EQ(3, dict.Size());
}

TEST(InverterPostingLogTest, RunMerging) {
  SimpleMemoryAccounter memory;
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  PostingLog<TokenLayout::TermsPos> log{arena, memory};

  log.PushOne(1, 0, 1);
  log.PushOne(1, 1, 2);  // multi-value continuation: same run, same doc
  log.PushOne(2, 0, 1);  // consecutive doc: run extends
  log.PushOne(5, 1,
              1);  // gap of 2 (<= kMaxBridgedGap): bridged with zero slots
  log.PushOne(20, 0, 1);  // gap of 14 (> kMaxBridgedGap): new run

  const auto runs = log.Runs();
  ASSERT_EQ(2, runs.size());
  EXPECT_EQ(1, runs[0].first_doc);
  EXPECT_EQ(5, runs[0].ndocs);  // docs 1..5, 3 and 4 as zero-count slots
  EXPECT_EQ(20, runs[1].first_doc);
  EXPECT_EQ(1, runs[1].ndocs);

  ASSERT_EQ(6, log.DocTokens().Size());
  std::vector<uint32_t> doc_tokens;
  LogColumn::Cursor cursor{log.DocTokens()};
  for (;;) {
    const auto span = cursor.Next(std::numeric_limits<size_t>::max());
    if (span.empty()) {
      break;
    }
    doc_tokens.insert(doc_tokens.end(), span.begin(), span.end());
  }
  EXPECT_EQ(2, doc_tokens[0]);  // doc 1
  EXPECT_EQ(1, doc_tokens[1]);  // doc 2
  EXPECT_EQ(0, doc_tokens[2]);  // doc 3 (bridged)
  EXPECT_EQ(0, doc_tokens[3]);  // doc 4 (bridged)
  EXPECT_EQ(1, doc_tokens[4]);  // doc 5
  EXPECT_EQ(1, doc_tokens[5]);  // doc 20
  EXPECT_EQ(5, log.Size());
}

TEST(InverterPackedColumnTest, RoundtripAcrossSeals) {
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  PackedU32Column col{arena, IResourceManager::gNoop};
  std::vector<uint32_t> expect;
  uint32_t state = 12345;
  auto next = [&]() {
    state = state * 1664525u + 1013904223u;
    return state;
  };
  auto verify = [&]() {
    ASSERT_EQ(expect.size(), col.Size());
    PackedU32Column::Cursor cursor{col};
    size_t i = 0;
    for (;;) {
      const auto span = cursor.Next(97);
      if (span.empty()) {
        break;
      }
      for (const auto v : span) {
        ASSERT_EQ(expect[i], v) << "at " << i;
        ++i;
      }
    }
    ASSERT_EQ(expect.size(), i);
  };

  for (size_t i = 0; i < 10; ++i) {
    const auto v = next() & 0xF;
    col.Push(v);
    expect.push_back(v);
  }
  verify();

  uint32_t bulk[333];
  for (size_t r = 0; r < 5; ++r) {
    for (auto& v : bulk) {
      v = next() & 0xFFFF;
      expect.push_back(v + 7);
    }
    col.PushNAdd(bulk, 333, 7);
  }
  verify();

  for (size_t r = 0; r < 4; ++r) {
    for (auto& v : bulk) {
      v = next();
      expect.push_back(v);
    }
    col.PushN(bulk, 333);
  }
  for (size_t i = 0; i < 3000; ++i) {
    const auto v = next();
    col.Push(v);
    expect.push_back(v);
  }
  verify();

  col.Reset();
  expect.clear();
  for (size_t i = 0; i < 2050; ++i) {
    const auto v = next() & 0x7;
    col.Push(v);
    expect.push_back(v);
  }
  verify();
}

TEST(InverterPackedColumnTest, SequentialIdsSharedArena) {
  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  PackedU32Column ids{arena, IResourceManager::gNoop};
  PackedU32Column unused{arena, IResourceManager::gNoop};
  std::vector<uint32_t> expect;
  uint32_t buf[1024];
  uint32_t v = 0;
  for (const size_t n : {size_t{1024}, size_t{1024}, size_t{952}}) {
    for (size_t i = 0; i < n; ++i) {
      buf[i] = v;
      expect.push_back(v++);
    }
    ids.PushN(buf, n);
  }
  ASSERT_EQ(expect.size(), ids.Size());
  PackedU32Column::Cursor cursor{ids};
  size_t i = 0;
  for (;;) {
    const auto span = cursor.Next(std::numeric_limits<size_t>::max());
    if (span.empty()) {
      break;
    }
    for (const auto x : span) {
      ASSERT_EQ(expect[i], x) << "at " << i;
      ++i;
    }
  }
  ASSERT_EQ(expect.size(), i);
}

struct ExpectedPosting {
  doc_id_t doc;
  uint32_t freq;
  std::vector<uint32_t> positions;
  std::vector<std::pair<uint32_t, uint32_t>> offsets;
};

void AssertField(const FieldInverter& field, InverterMemory& mem,
                 const std::vector<std::string>& expected_terms,
                 const std::vector<std::vector<ExpectedPosting>>& expected) {
  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(field);
  ColumnarTermReader terms;
  terms.Reset(scattered);

  ASSERT_EQ(expected_terms.size(), scattered.TermCount());
  if (!expected_terms.empty()) {
    ASSERT_EQ(ToBytesView(expected_terms.front()), terms.min());
    ASSERT_EQ(ToBytesView(expected_terms.back()), terms.max());
  }

  auto it = terms.iterator();
  size_t term_idx = 0;
  while (it->next()) {
    ASSERT_LT(term_idx, expected_terms.size());
    EXPECT_EQ(ToBytesView(expected_terms[term_idx]), it->value());

    auto postings = it->postings(field.RequestedFeatures());
    auto* freq = irs::GetMutable<FreqAttr>(postings.get());
    auto* pos = irs::GetMutable<PosAttr>(postings.get());

    for (const auto& exp : expected[term_idx]) {
      ASSERT_EQ(exp.doc, postings->advance());
      if (freq) {
        EXPECT_EQ(exp.freq, freq->value);
      }
      if (pos && !exp.positions.empty()) {
        auto* offs = irs::GetMutable<OffsAttr>(pos);
        for (size_t p = 0; p < exp.positions.size(); ++p) {
          ASSERT_TRUE(pos->next());
          EXPECT_EQ(exp.positions[p], pos->value());
          if (offs && p < exp.offsets.size()) {
            EXPECT_EQ(exp.offsets[p].first, offs->start);
            EXPECT_EQ(exp.offsets[p].second, offs->end);
          }
        }
        ASSERT_FALSE(pos->next());
      }
    }
    ASSERT_TRUE(doc_limits::eof(postings->advance()));
    ++term_idx;
  }
  ASSERT_EQ(expected_terms.size(), term_idx);
}

TEST(InverterPipelineTest, KernelToPostings) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  const auto features =
    IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;
  auto* field = inv.Emplace(42, features);
  ASSERT_NE(nullptr, field);

  auto batch = std::make_unique<TokenBatch>();
  const std::vector<std::string> docs = {
    "quick brown fox",
    "the quick fox",
    "fox fox brown",
  };
  doc_id_t doc = doc_limits::min();
  for (const auto& text : docs) {
    bool more;
    size_t cursor = 0;
    do {
      batch->count = 0;
      more = FillSplitBatch(text, cursor, TokenLayout::TermsPosOffs, *batch);
      ASSERT_TRUE(
        ::tests::InvertTokens(*field, doc, *batch, /*ends_value=*/!more));
    } while (more);
    ++doc;
  }

  const auto d = doc_limits::min();
  AssertField(*field, mem, {"brown", "fox", "quick", "the"},
              {
                {{d, 1, {2}, {{6, 11}}}, {d + 2, 1, {3}, {{8, 13}}}},
                {{d, 1, {3}, {{12, 15}}},
                 {d + 1, 1, {3}, {{10, 13}}},
                 {d + 2, 2, {1, 2}, {{0, 3}, {4, 7}}}},
                {{d, 1, {1}, {{0, 5}}}, {d + 1, 1, {2}, {{4, 9}}}},
                {{d + 1, 1, {1}, {{0, 3}}}},
              });
}

TEST(InverterPipelineTest, KeywordMultiValue) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  const auto features = IndexFeatures::Freq | IndexFeatures::Pos;
  auto* field = inv.Emplace(7, features);

  const auto d = doc_limits::min();
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("red")));
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("green")));
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("red")));
  ASSERT_TRUE(InvertKeyword(*field, d + 1, ToBytesView("green")));

  AssertField(*field, mem, {"green", "red"},
              {
                {{d, 1, {2}, {}}, {d + 1, 1, {1}, {}}},
                {{d, 2, {1, 3}, {}}},
              });
}

// InvertBlock (block path) must produce exactly the scatter output of
// a per-value InvertKeyword loop over the same (doc, value) sequence: same
// terms, docs, positions and offsets. Spans >kCapacity docs to cross the
// AddBatch sub-chunk boundary.
void AssertSameScatter(const FieldInverter& a, const FieldInverter& b,
                       InverterMemory& mem) {
  ScatterScratch sca{mem.rm};
  ScatteredField sa{mem, sca};
  sa.Reset(a);
  ScatterScratch scb{mem.rm};
  ScatteredField sb{mem, scb};
  sb.Reset(b);

  ASSERT_EQ(sa.Layout(), sb.Layout());
  ASSERT_EQ(sa.TermCount(), sb.TermCount());
  const auto nterms = sa.TermCount();
  if (nterms == 0) {
    return;
  }
  for (size_t r = 0; r < nterms; ++r) {
    ASSERT_EQ(sa.TermAt(r), sb.TermAt(r));
    ASSERT_EQ(sa.TermBegin(r), sb.TermBegin(r));
    ASSERT_EQ(sa.TermEnd(r), sb.TermEnd(r));
  }
  const auto nocc = sa.TermEnd(nterms - 1);
  for (uint64_t k = 0; k < nocc; ++k) {
    ASSERT_EQ(sa.DocAt(k), sb.DocAt(k));
  }
  if (sa.Layout() != TokenLayout::Terms) {
    for (uint64_t k = 0; k < nocc; ++k) {
      ASSERT_EQ(sa.Pos()[k], sb.Pos()[k]);
    }
  }
  if (sa.Layout() == TokenLayout::TermsPosOffs) {
    for (uint64_t k = 0; k < nocc; ++k) {
      ASSERT_EQ(sa.OffsStart()[k], sb.OffsStart()[k]);
      ASSERT_EQ(sa.OffsEnd()[k], sb.OffsEnd()[k]);
    }
  }
}

TEST(InverterColumnKeywordTest, MatchesPerValue) {
  // 1500 distinct terms > kFusedProbeThreshold (1024) with repeats, over 2500
  // docs: streams across several block batches AND makes the adaptive resolve
  // switch fused->pipelined mid-column.
  constexpr size_t kDocs = 2500;
  std::vector<std::string> vals;
  vals.reserve(kDocs);
  for (size_t i = 0; i < kDocs; ++i) {
    vals.push_back("cat_" + std::to_string(i % 1500));
  }

  for (const auto features :
       {IndexFeatures::Freq, IndexFeatures::Freq | IndexFeatures::Pos,
        IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs}) {
    auto mem = InverterMemory::Default();
    FieldsInverter inv_ref{mem};
    FieldsInverter inv_col{mem};
    auto* ref = inv_ref.Emplace(1, features);
    auto* col = inv_col.Emplace(1, features);

    for (size_t i = 0; i < kDocs; ++i) {
      ASSERT_TRUE(InvertKeyword(
        *ref, doc_limits::min() + static_cast<doc_id_t>(i),
        bytes_view{reinterpret_cast<const byte_type*>(vals[i].data()),
                   vals[i].size()}));
    }

    std::vector<duckdb::string_t> terms;
    std::vector<doc_id_t> docs;
    for (size_t i = 0; i < kDocs; ++i) {
      terms.push_back(duckdb::string_t{vals[i].data(),
                                       static_cast<uint32_t>(vals[i].size())});
      docs.push_back(doc_limits::min() + static_cast<doc_id_t>(i));
    }
    ASSERT_TRUE(col->InvertKeywordBlock(terms, docs));

    ASSERT_EQ(ref->Dictionary().Size(), col->Dictionary().Size());
    ASSERT_EQ(ref->Log().Size(), col->Log().Size());
    AssertSameScatter(*ref, *col, mem);
  }
}

// The unique fast path must produce exactly the general path's output
// over the same 1-1 shaped stream (single token per doc-run), across all
// layouts and both position modes.
TEST(InverterOneToOneTest, MatchesGeneralPath) {
  constexpr size_t kDocs = 2500;
  std::vector<std::string> vals;
  vals.reserve(kDocs);
  for (size_t i = 0; i < kDocs; ++i) {
    vals.push_back("tok_" + std::to_string(i % 700));
  }

  for (const auto features :
       {IndexFeatures::Freq, IndexFeatures::Freq | IndexFeatures::Pos,
        IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs}) {
    for (const bool dense : {true, false}) {
      auto mem = InverterMemory::Default();
      FieldsInverter inv_fast{mem};
      FieldsInverter inv_gen{mem};
      auto* fast = inv_fast.Emplace(1, features);
      auto* gen = inv_gen.Emplace(1, features);
      fast->SetOneToOne(true);
      fast->SetDensePos(dense);
      gen->SetDensePos(dense);

      auto batch = std::make_unique<TokenBatch>();
      std::vector<DocRun> runs;
      size_t i = 0;
      while (i < kDocs) {
        const size_t n = std::min(TokenBatch::kCapacity, kDocs - i);
        batch->count = static_cast<uint32_t>(n);
        runs.clear();
        for (size_t j = 0; j < n; ++j) {
          const auto& v = vals[i + j];
          batch->terms[j] =
            duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())};
          if (!dense) {
            batch->pos[j] = 1;
          }
          batch->offs_start[j] = 0;
          batch->offs_end[j] = static_cast<uint32_t>(v.size());
          runs.push_back({doc_limits::min() + static_cast<doc_id_t>(i + j), 1});
        }
        ASSERT_TRUE(fast->InvertBlock(*batch, {{runs}}));
        ASSERT_TRUE(gen->InvertBlock(*batch, {{runs}}));
        i += n;
      }
      ASSERT_EQ(gen->Dictionary().Size(), fast->Dictionary().Size());
      ASSERT_EQ(gen->Log().Size(), fast->Log().Size());
      AssertSameScatter(*gen, *fast, mem);
    }
  }
}

// Append-only (unique-marked) interning must flush identically to the
// resolving dictionary over the same stream: duplicates become separate
// entries folded at flush, where the resolving dict dedups at intern time
// (inline capture, then log spill past kInlineOccs).
TEST(InverterUniqueTermsTest, MatchesResolvedDictionary) {
  constexpr size_t kDocs = 3000;
  std::vector<std::string> vals;
  vals.reserve(kDocs);
  for (size_t i = 0; i < kDocs; ++i) {
    char buf[16];
    std::snprintf(buf, sizeof buf, "pk_%08zu", i);
    vals.emplace_back(buf);
  }
  vals[100] = vals[99];
  vals[101] = vals[99];
  vals[102] = vals[99];
  vals[2000] = vals[1999];

  auto mem = InverterMemory::Default();
  FieldsInverter inv_ref{mem};
  FieldsInverter inv_uni{mem};
  auto* ref = inv_ref.Emplace(1, IndexFeatures::Freq);
  auto* uni = inv_uni.Emplace(1, IndexFeatures::Freq);

  std::vector<duckdb::string_t> terms;
  std::vector<doc_id_t> docs;
  for (size_t i = 0; i < kDocs; ++i) {
    terms.push_back(
      duckdb::string_t{vals[i].data(), static_cast<uint32_t>(vals[i].size())});
    docs.push_back(doc_limits::min() + static_cast<doc_id_t>(i));
  }
  ASSERT_TRUE(ref->InvertKeywordBlock(terms, docs));
  ASSERT_TRUE(uni->InvertUniqueKeywordBlock(
    std::span<const duckdb::string_t>{terms}, doc_limits::min()));

  ASSERT_EQ(kDocs, uni->Dictionary().Size());
  ASSERT_EQ(0, uni->Log().Size());
  AssertSameScatter(*ref, *uni, mem);
}

// The rank fast path skips the sort when the fill comes out key-sorted
// (PK-shaped columns): equal 8-byte prefix keys count as sorted, so the
// equal-key tie-break must still reorder them by term bytes.
TEST(InverterScatterTest, PresortedKeysTieBreak) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::Freq);

  const std::array<std::string_view, 4> vals{"aaaaaaaa2", "aaaaaaaa10",
                                             "aaaaaaaa1", "aaaaaaab"};
  doc_id_t doc = doc_limits::min();
  for (const auto v : vals) {
    ASSERT_TRUE(InvertKeyword(*field, doc++, ToBytesView(v)));
  }

  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(*field);
  ASSERT_EQ(4, scattered.TermCount());
  ASSERT_EQ(ToBytesView("aaaaaaaa1"), scattered.TermAt(0));
  ASSERT_EQ(ToBytesView("aaaaaaaa10"), scattered.TermAt(1));
  ASSERT_EQ(ToBytesView("aaaaaaaa2"), scattered.TermAt(2));
  ASSERT_EQ(ToBytesView("aaaaaaab"), scattered.TermAt(3));
}

TEST(InverterScatterTest, SharedPrefixRekeyOrder) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::Freq);

  const std::array<std::string_view, 6> vals{
    "http://example.com/products/42", "http://example.com/about",
    "http://example.com/products/7",  "http://example.com/",
    "http://example.com/zzz",         "http://example.com/products/41"};
  doc_id_t doc = doc_limits::min();
  for (const auto v : vals) {
    ASSERT_TRUE(InvertKeyword(*field, doc++, ToBytesView(v)));
  }

  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(*field);
  ASSERT_EQ(6, scattered.TermCount());
  ASSERT_EQ(ToBytesView("http://example.com/"), scattered.TermAt(0));
  ASSERT_EQ(ToBytesView("http://example.com/about"), scattered.TermAt(1));
  ASSERT_EQ(ToBytesView("http://example.com/products/41"), scattered.TermAt(2));
  ASSERT_EQ(ToBytesView("http://example.com/products/42"), scattered.TermAt(3));
  ASSERT_EQ(ToBytesView("http://example.com/products/7"), scattered.TermAt(4));
  ASSERT_EQ(ToBytesView("http://example.com/zzz"), scattered.TermAt(5));
}

TEST(InverterScatterTest, RekeyPrefixTermRanksFirst) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::Freq);

  const std::array<std::string_view, 3> vals{
    "aaaaaaaabbbbbbbbY", "aaaaaaaabbbbbbbb", "aaaaaaaabbbbbbbbX"};
  doc_id_t doc = doc_limits::min();
  for (const auto v : vals) {
    ASSERT_TRUE(InvertKeyword(*field, doc++, ToBytesView(v)));
  }

  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(*field);
  ASSERT_EQ(3, scattered.TermCount());
  ASSERT_EQ(ToBytesView("aaaaaaaabbbbbbbb"), scattered.TermAt(0));
  ASSERT_EQ(ToBytesView("aaaaaaaabbbbbbbbX"), scattered.TermAt(1));
  ASSERT_EQ(ToBytesView("aaaaaaaabbbbbbbbY"), scattered.TermAt(2));
}

TEST(InverterScatterTest, SortedEqualKeyOutOfOrderResorts) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::Freq);

  const std::array<std::string_view, 3> vals{"za_anchor", "zz_abcdefgh_2",
                                             "zz_abcdefgh_1"};
  doc_id_t doc = doc_limits::min();
  for (const auto v : vals) {
    ASSERT_TRUE(InvertKeyword(*field, doc++, ToBytesView(v)));
  }

  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(*field);
  ASSERT_EQ(3, scattered.TermCount());
  ASSERT_EQ(ToBytesView("za_anchor"), scattered.TermAt(0));
  ASSERT_EQ(ToBytesView("zz_abcdefgh_1"), scattered.TermAt(1));
  ASSERT_EQ(ToBytesView("zz_abcdefgh_2"), scattered.TermAt(2));
}

TEST(InverterScatterTest, UniqueDictNonAdjacentDupFolds) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::Freq);

  const std::array<std::string_view, 3> vals{"qqqqqqqq_a", "qqqqqqqq_b",
                                             "qqqqqqqq_a"};
  std::vector<duckdb::string_t> terms;
  for (const auto v : vals) {
    terms.push_back(
      duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())});
  }
  ASSERT_TRUE(field->InvertUniqueKeywordBlock(
    std::span<const duckdb::string_t>{terms}, doc_limits::min()));
  ASSERT_EQ(3, field->Dictionary().Size());

  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(*field);
  ASSERT_EQ(2, scattered.TermCount());
  ASSERT_EQ(ToBytesView("qqqqqqqq_a"), scattered.TermAt(0));
  ASSERT_EQ(ToBytesView("qqqqqqqq_b"), scattered.TermAt(1));
  ASSERT_EQ(0, scattered.TermBegin(0));
  ASSERT_EQ(2, scattered.TermEnd(0));
  ASSERT_EQ(3, scattered.TermEnd(1));
  ASSERT_TRUE(scattered.AllInline());
  ASSERT_EQ(doc_limits::min(), scattered.DocAt(0));
  ASSERT_EQ(doc_limits::min() + 2, scattered.DocAt(1));
  ASSERT_EQ(doc_limits::min() + 1, scattered.DocAt(2));
}

// A rejected batch must leave the log untouched (validation first,
// accounting last): rejecting mid-stream and continuing with valid input
// flushes byte-identically to a stream that never saw the reject. The
// dictionary MAY grow (resolve precedes validation by design); zero-occ
// leftovers are filtered at flush.
struct RejectFill {
  TokenBatch& batch;
  std::vector<DocRun>& runs;

  void operator()(
    doc_id_t doc, std::initializer_list<std::string_view> terms,
    std::initializer_list<uint32_t> pos = {},
    std::initializer_list<std::pair<uint32_t, uint32_t>> offs = {}) const {
    batch.count = 0;
    runs.clear();
    auto p = pos.begin();
    auto o = offs.begin();
    for (const auto t : terms) {
      const auto i = batch.count++;
      batch.terms[i] =
        duckdb::string_t{t.data(), static_cast<uint32_t>(t.size())};
      if (p != pos.end()) {
        batch.pos[i] = *p++;
      }
      if (o != offs.end()) {
        batch.offs_start[i] = o->first;
        batch.offs_end[i] = o->second;
        ++o;
      }
    }
    runs.push_back({doc, batch.count});
  }
};

TEST(InverterRejectTest, NonMonotonicPosLeavesLogUntouched) {
  const auto features = IndexFeatures::Freq | IndexFeatures::Pos;
  auto mem = InverterMemory::Default();
  FieldsInverter inv_a{mem};
  FieldsInverter inv_b{mem};
  auto* a = inv_a.Emplace(1, features);
  auto* b = inv_b.Emplace(1, features);
  a->SetDensePos(false);
  b->SetDensePos(false);

  auto batch = std::make_unique<TokenBatch>();
  std::vector<DocRun> runs;
  const RejectFill fill{*batch, runs};
  const auto doc0 = doc_limits::min();

  fill(doc0, {"alpha", "bravo"}, {1, 2});
  ASSERT_TRUE(a->InvertBlock(*batch, {{runs}}));
  fill(doc0, {"alpha", "bravo"}, {1, 2});
  ASSERT_TRUE(b->InvertBlock(*batch, {{runs}}));

  const auto log_size = a->Log().Size();
  const auto doc_slots = a->Log().DocTokens().Size();
  fill(doc0 + 1, {"charlie", "delta"}, {5, 3});
  ASSERT_FALSE(a->InvertBlock(*batch, {{runs}}));
  ASSERT_EQ(log_size, a->Log().Size());
  ASSERT_EQ(doc_slots, a->Log().DocTokens().Size());

  fill(doc0 + 2, {"echo"}, {1});
  ASSERT_TRUE(a->InvertBlock(*batch, {{runs}}));
  fill(doc0 + 2, {"echo"}, {1});
  ASSERT_TRUE(b->InvertBlock(*batch, {{runs}}));

  AssertSameScatter(*a, *b, mem);
}

TEST(InverterRejectTest, OffsetRegressionLeavesLogUntouched) {
  const auto features =
    IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;
  auto mem = InverterMemory::Default();
  FieldsInverter inv_a{mem};
  FieldsInverter inv_b{mem};
  auto* a = inv_a.Emplace(1, features);
  auto* b = inv_b.Emplace(1, features);

  auto batch = std::make_unique<TokenBatch>();
  std::vector<DocRun> runs;
  const RejectFill fill{*batch, runs};
  const auto doc0 = doc_limits::min();

  fill(doc0, {"alpha", "bravo"}, {}, {{0, 5}, {6, 11}});
  ASSERT_TRUE(a->InvertBlock(*batch, {{runs}}));
  fill(doc0, {"alpha", "bravo"}, {}, {{0, 5}, {6, 11}});
  ASSERT_TRUE(b->InvertBlock(*batch, {{runs}}));

  const auto log_size = a->Log().Size();
  const auto doc_slots = a->Log().DocTokens().Size();
  fill(doc0 + 1, {"charlie"}, {}, {{5, 3}});
  ASSERT_FALSE(a->InvertBlock(*batch, {{runs}}));
  ASSERT_EQ(log_size, a->Log().Size());
  ASSERT_EQ(doc_slots, a->Log().DocTokens().Size());

  fill(doc0 + 2, {"echo"}, {}, {{0, 4}});
  ASSERT_TRUE(a->InvertBlock(*batch, {{runs}}));
  fill(doc0 + 2, {"echo"}, {}, {{0, 4}});
  ASSERT_TRUE(b->InvertBlock(*batch, {{runs}}));

  AssertSameScatter(*a, *b, mem);
}

TEST(InverterRejectTest, OffsetWraparoundLeavesLogUntouched) {
  const auto features =
    IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;
  auto mem = InverterMemory::Default();
  FieldsInverter inv_a{mem};
  FieldsInverter inv_b{mem};
  auto* a = inv_a.Emplace(1, features);
  auto* b = inv_b.Emplace(1, features);

  auto batch = std::make_unique<TokenBatch>();
  std::vector<DocRun> runs;
  const RejectFill fill{*batch, runs};
  const auto doc0 = doc_limits::min();

  // First value advances the doc's offset base to 4e9; the second value's
  // rebased end (4e9 + 4e8) wraps u32 and must reject before any push.
  fill(doc0, {"alpha"}, {}, {{0, 4000000000u}});
  ASSERT_TRUE(a->InvertBlock(*batch, {{runs}}));
  fill(doc0, {"alpha"}, {}, {{0, 4000000000u}});
  ASSERT_TRUE(b->InvertBlock(*batch, {{runs}}));

  const auto log_size = a->Log().Size();
  const auto doc_slots = a->Log().DocTokens().Size();
  fill(doc0, {"bravo"}, {}, {{0, 400000000u}});
  ASSERT_FALSE(a->InvertBlock(*batch, {{runs}}));
  ASSERT_EQ(log_size, a->Log().Size());
  ASSERT_EQ(doc_slots, a->Log().DocTokens().Size());

  fill(doc0 + 1, {"echo"}, {}, {{0, 4}});
  ASSERT_TRUE(a->InvertBlock(*batch, {{runs}}));
  fill(doc0 + 1, {"echo"}, {}, {{0, 4}});
  ASSERT_TRUE(b->InvertBlock(*batch, {{runs}}));

  AssertSameScatter(*a, *b, mem);
}

// Terms layout (no positions): every term's first occurrence is captured
// inline in its dictionary entry and only occurrences 2..n reach the log.
// Exact expected postings anchor the scatter's inline-then-log emission.
TEST(InverterPipelineTest, TermsLayoutKeywordPostings) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(3, IndexFeatures::Freq);

  const auto d = doc_limits::min();
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("red")));
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("green")));
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("red")));
  ASSERT_TRUE(InvertKeyword(*field, d + 1, ToBytesView("green")));
  ASSERT_TRUE(InvertKeyword(*field, d + 2, ToBytesView("solo")));

  ASSERT_EQ(3, field->Dictionary().Size());
  ASSERT_EQ(5, field->Log().Size());

  AssertField(*field, mem, {"green", "red", "solo"},
              {
                {{d, 1, {}, {}}, {d + 1, 1, {}, {}}},
                {{d, 2, {}, {}}},
                {{d + 2, 1, {}, {}}},
              });
}

// Postings stay exact for repeated terms, including a single doc whose freq
// counts multiple occurrences.
TEST(InverterPipelineTest, TermsLayoutRepeatedKeywords) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(5, IndexFeatures::Freq);

  const auto d = doc_limits::min();
  // "trip": 3 occ. "quint": 5 occ. "rep": 4 occ in ONE doc.
  for (uint32_t k = 0; k < 5; ++k) {
    ASSERT_TRUE(InvertKeyword(*field, d + k, ToBytesView("quint")));
    if (k < 3) {
      ASSERT_TRUE(InvertKeyword(*field, d + k, ToBytesView("trip")));
    }
    if (k == 1) {
      for (int r = 0; r < 4; ++r) {
        ASSERT_TRUE(InvertKeyword(*field, d + k, ToBytesView("rep")));
      }
    }
  }

  ASSERT_EQ(3, field->Dictionary().Size());
  ASSERT_EQ(12, field->Log().Size());

  AssertField(*field, mem, {"quint", "rep", "trip"},
              {
                {{d, 1, {}, {}},
                 {d + 1, 1, {}, {}},
                 {d + 2, 1, {}, {}},
                 {d + 3, 1, {}, {}},
                 {d + 4, 1, {}, {}}},
                {{d + 1, 4, {}, {}}},
                {{d, 1, {}, {}}, {d + 1, 1, {}, {}}, {d + 2, 1, {}, {}}},
              });
}

// Distinct keyword values through the non-unique path: one occurrence each,
// all recorded in the log.
TEST(InverterPipelineTest, TermsLayoutDistinctKeywords) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(4, IndexFeatures::Freq);

  const auto d = doc_limits::min();
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("pk_b")));
  ASSERT_TRUE(InvertKeyword(*field, d + 1, ToBytesView("pk_a")));
  ASSERT_TRUE(InvertKeyword(*field, d + 2, ToBytesView("pk_c")));

  ASSERT_EQ(3, field->Dictionary().Size());
  ASSERT_EQ(3, field->Log().Size());

  AssertField(*field, mem, {"pk_a", "pk_b", "pk_c"},
              {
                {{d + 1, 1, {}, {}}},
                {{d, 1, {}, {}}},
                {{d + 2, 1, {}, {}}},
              });
}

TEST(InverterPipelineTest, MultiBatchLongValue) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::Freq | IndexFeatures::Pos);

  auto batch = std::make_unique<TokenBatch>();
  std::string text;
  constexpr size_t kTokens = 3000;
  for (size_t i = 0; i < kTokens; ++i) {
    text += "t" + std::to_string(i) + " ";
  }

  const auto d = doc_limits::min();
  bool more;
  size_t rounds = 0;
  size_t cursor = 0;
  do {
    batch->count = 0;
    more = FillSplitBatch(text, cursor, TokenLayout::TermsPos, *batch);
    ASSERT_TRUE(::tests::InvertTokens(*field, d, *batch, /*ends_value=*/!more));
    ++rounds;
  } while (more);
  ASSERT_GT(rounds, 1);
  ASSERT_EQ(kTokens, field->Stats().len);
  ASSERT_EQ(kTokens, field->Dictionary().Size());
  ASSERT_EQ(kTokens, field->Log().Size());

  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(*field);
  ASSERT_EQ(kTokens, scattered.TermCount());
  for (size_t r = 0; r < kTokens; ++r) {
    ASSERT_EQ(scattered.TermBegin(r) + 1, scattered.TermEnd(r));
  }
}

// A single value that spans several batches, WITH offsets: every occurrence's
// offset must equal its true byte range in the value (regression for the
// per-batch _offs double-shift on multi-batch continuations).
TEST(InverterPipelineTest, MultiBatchOffsets) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  const auto features =
    IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;
  auto* field = inv.Emplace(1, features);

  // "aaa " repeated: every token is the same term "aaa", occurrence k spans
  // bytes [4k, 4k + 3).
  constexpr size_t kOccurrences = 1500;
  std::string text;
  for (size_t i = 0; i < kOccurrences; ++i) {
    text += "aaa ";
  }

  auto batch = std::make_unique<TokenBatch>();
  const auto d = doc_limits::min();
  bool more;
  size_t rounds = 0;
  size_t cursor = 0;
  do {
    batch->count = 0;
    more = FillSplitBatch(text, cursor, TokenLayout::TermsPosOffs, *batch);
    ASSERT_TRUE(::tests::InvertTokens(*field, d, *batch, /*ends_value=*/!more));
    ++rounds;
  } while (more);
  ASSERT_GT(rounds, 1);  // the value really did span multiple batches

  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(*field);
  ColumnarTermReader terms;
  terms.Reset(scattered);

  auto it = terms.iterator();
  ASSERT_TRUE(it->next());
  EXPECT_EQ(ToBytesView("aaa"), it->value());
  auto postings = it->postings(features);
  auto* pos = irs::GetMutable<PosAttr>(postings.get());
  ASSERT_NE(nullptr, pos);
  ASSERT_EQ(d, postings->advance());
  auto* offs = irs::GetMutable<OffsAttr>(pos);
  ASSERT_NE(nullptr, offs);
  uint32_t k = 0;
  while (pos->next()) {
    EXPECT_EQ(4 * k, offs->start);
    EXPECT_EQ(4 * k + 3, offs->end);
    ++k;
  }
  EXPECT_EQ(kOccurrences, k);
  ASSERT_FALSE(it->next());
}

// A doc that receives an explicit-position value first (non-dense batch) then
// a dense value on the same field must keep the pos column in sync -- the dense
// tokens are written explicitly at running positions (regression for the
// dense-push-after-explicit-doc desync that was guarded only by SDB_ASSERT).
TEST(InverterPipelineTest, DenseAfterExplicitDoc) {
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  const auto features = IndexFeatures::Freq | IndexFeatures::Pos;
  auto* field = inv.Emplace(1, features);
  field->SetDensePos(false);

  const auto d = doc_limits::min();

  // Value 1: explicit positions (a non-dense batch), tokens "a","b" at pos 1,2.
  auto batch = std::make_unique<TokenBatch>();
  batch->count = 2;
  batch->terms[0] = duckdb::string_t{"a", 1};
  batch->pos[0] = 1;
  batch->terms[1] = duckdb::string_t{"b", 1};
  batch->pos[1] = 2;
  ASSERT_TRUE(::tests::InvertTokens(*field, d, *batch, /*ends_value=*/true));

  // Value 2: dense keyword on the same doc; must land at position 3.
  ASSERT_TRUE(InvertKeyword(*field, d, ToBytesView("c")));

  AssertField(*field, mem, {"a", "b", "c"},
              {
                {{d, 1, {1}, {}}},
                {{d, 1, {2}, {}}},
                {{d, 1, {3}, {}}},
              });
}

}  // namespace
namespace {

TEST(InverterEndToEndTest, InsertKeywordAndTokensThroughWriter) {
  auto codec = formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  MemoryDirectory dir;
  auto writer = IndexWriter::Make(dir, codec, kOmCreate,
                                  irs::tests::DefaultWriterOptions());
  ASSERT_NE(nullptr, writer);

  constexpr field_id kKeyword = 1;
  constexpr field_id kText = 2;
  constexpr auto kKeywordFeatures = IndexFeatures::Freq;
  constexpr auto kTextFeatures =
    IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;

  auto batch = std::make_unique<TokenBatch>();
  const std::vector<std::pair<std::string, std::string>> rows = {
    {"alpha", "quick brown fox"},
    {"beta", "the quick fox"},
    {"alpha", "fox fox brown"},
  };

  {
    auto trx = writer->GetBatch();
    for (const auto& [keyword, text] : rows) {
      auto doc = trx.Insert();
      const auto d = doc.DocId();
      const duckdb::string_t kw{keyword.data(),
                                static_cast<uint32_t>(keyword.size())};
      ASSERT_TRUE(
        doc.InsertKeywordBlock(kKeyword, kKeywordFeatures, {&kw, 1}, {&d, 1}));
      bool more;
      size_t cursor = 0;
      std::vector<DocRun> runs;
      do {
        batch->count = 0;
        runs.clear();
        more = FillSplitBatch(text, cursor, TokenLayout::TermsPosOffs, *batch);
        runs.push_back({d, batch->count});
        ASSERT_TRUE(
          doc.InsertBlock(kText, kTextFeatures, *batch, {{runs}, more}));
      } while (more);
    }
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(writer->RefreshCommit());

  auto reader = DirectoryReader(dir, codec, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(reader);
  ASSERT_EQ(3, reader->docs_count());
  const auto& segment = (*reader)[0];

  const auto* keyword_reader = segment.field(kKeyword);
  ASSERT_NE(nullptr, keyword_reader);
  {
    auto terms = keyword_reader->iterator(SeekMode::NORMAL);
    ASSERT_TRUE(terms->next());
    EXPECT_EQ(ToBytesView("alpha"), terms->value());
    auto docs = terms->postings(kKeywordFeatures);
    ASSERT_FALSE(irs::doc_limits::eof(docs->advance()));
    ASSERT_FALSE(irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(terms->next());
    EXPECT_EQ(ToBytesView("beta"), terms->value());
    ASSERT_FALSE(terms->next());
  }

  const auto* text_reader = segment.field(kText);
  ASSERT_NE(nullptr, text_reader);
  ASSERT_EQ(kTextFeatures,
            text_reader->meta().index_features &
              (IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs));
  {
    auto terms = text_reader->iterator(SeekMode::NORMAL);
    ASSERT_TRUE(terms->seek(ToBytesView("fox")));
    auto docs = terms->postings(kTextFeatures);
    auto* pos = irs::GetMutable<PosAttr>(docs.get());
    ASSERT_NE(nullptr, pos);

    ASSERT_FALSE(irs::doc_limits::eof(docs->advance()));
    EXPECT_EQ(1, docs->GetFreq());
    ASSERT_TRUE(pos->next());
    EXPECT_EQ(3, pos->value());

    ASSERT_FALSE(irs::doc_limits::eof(docs->advance()));
    ASSERT_FALSE(irs::doc_limits::eof(docs->advance()));
    EXPECT_EQ(2, docs->GetFreq());
    ASSERT_TRUE(pos->next());
    EXPECT_EQ(1, pos->value());
    auto* offs = irs::GetMutable<OffsAttr>(pos);
    ASSERT_NE(nullptr, offs);
    EXPECT_EQ(0, offs->start);
    EXPECT_EQ(3, offs->end);
    ASSERT_TRUE(pos->next());
    EXPECT_EQ(2, pos->value());
    ASSERT_FALSE(pos->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->advance()));
  }
}

}  // namespace
namespace {

// Deterministic pull-based analyzer for differential testing: whitespace
// split with offsets; a token "syn" additionally emits "SYN" at the same
// position (increment 0), exercising overlaps.
class WhitespaceSyn final : public analysis::TypedTokenizer<WhitespaceSyn> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "test_whitespace_syn";
  }

  irs::TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = true,
      .offsets = true,
    };
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    const std::string_view value{raw.GetData(), raw.GetSize()};
    uint32_t pos = 0;
    size_t cursor = 0;
    const auto emit = [&](std::string_view tok, uint32_t inc, uint32_t start,
                          uint32_t end) {
      pos += inc;
      ::tests::EmitCopy<Layout>(sink, ToBytesView(tok), pos, Offs{start, end});
    };
    for (;;) {
      while (cursor < value.size() && value[cursor] == ' ') {
        ++cursor;
      }
      if (cursor >= value.size()) {
        return true;
      }
      const size_t start = cursor;
      while (cursor < value.size() && value[cursor] != ' ') {
        ++cursor;
      }
      const std::string_view tok{value.data() + start, cursor - start};
      emit(tok, 1, static_cast<uint32_t>(start), static_cast<uint32_t>(cursor));
      if (tok == "syn") {
        emit("SYN", 0, static_cast<uint32_t>(start),
             static_cast<uint32_t>(cursor));
      }
    }
  }
};

struct DumpedPosting {
  doc_id_t doc;
  uint32_t freq;
  std::vector<uint32_t> positions;
  std::vector<std::pair<uint32_t, uint32_t>> offsets;
  bool operator==(const DumpedPosting&) const = default;
};
struct DumpedTerm {
  std::string term;
  std::vector<DumpedPosting> postings;
  bool operator==(const DumpedTerm&) const = default;
};

std::vector<DumpedTerm> DumpField(const FieldInverter& field,
                                  InverterMemory& mem) {
  ScatterScratch scratch{mem.rm};
  ScatteredField scattered{mem, scratch};
  scattered.Reset(field);
  ColumnarTermReader reader;
  reader.Reset(scattered);

  std::vector<DumpedTerm> out;
  auto it = reader.iterator();
  while (it->next()) {
    const auto tv = it->value();
    DumpedTerm dt;
    dt.term.assign(reinterpret_cast<const char*>(tv.data()), tv.size());
    auto postings = it->postings(field.RequestedFeatures());
    auto* pos = irs::GetMutable<PosAttr>(postings.get());
    while (!doc_limits::eof(postings->advance())) {
      DumpedPosting dp;
      dp.doc = postings->value();
      dp.freq = postings->GetFreq();
      if (pos) {
        auto* offs = irs::GetMutable<OffsAttr>(pos);
        while (pos->next()) {
          dp.positions.push_back(pos->value());
          if (offs) {
            dp.offsets.emplace_back(offs->start, offs->end);
          }
        }
      }
      dt.postings.push_back(std::move(dp));
    }
    out.push_back(std::move(dt));
  }
  return out;
}

// Synonym overlaps (inc 0), offsets, and multi-value position/offset
// continuation through the driver + InvertTokens, anchored to exact expected
// postings.
TEST(InverterDriverTest, SynonymOverlapsExactPostings) {
  const auto features =
    IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;

  // doc -> its values (multi-value on doc 2)
  const std::vector<std::vector<std::string>> docs = {
    {"the quick syn fox"},
    {"syn syn brown", "second value here"},
    {"lazy dog"},
  };

  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, features);

  WhitespaceSyn analyzer;
  field->SetDensePos(!analyzer.Traits().explicit_pos);
  const auto invert = [&](TokenBatch& batch, DocRuns runs) {
    ASSERT_TRUE(field->InvertBlock(batch, runs));
  };
  ::tests::FnTokenSink sink{TokenLayout::TermsPosOffs, invert};
  doc_id_t d = doc_limits::min();
  for (const auto& values : docs) {
    for (const auto& v : values) {
      sink.writer.BeginValue(d, static_cast<uint32_t>(v.size()));
      ASSERT_TRUE(analyzer.Fill(v, sink.writer, sink.layout));
      sink.writer.EndValue();
      sink.writer.Finish();
    }
    ++d;
  }

  const auto d0 = doc_limits::min();
  AssertField(
    *field, mem,
    {"SYN", "brown", "dog", "fox", "here", "lazy", "quick", "second", "syn",
     "the", "value"},
    {
      {{d0, 1, {3}, {{10, 13}}}, {d0 + 1, 2, {1, 2}, {{0, 3}, {4, 7}}}},
      {{d0 + 1, 1, {3}, {{8, 13}}}},
      {{d0 + 2, 1, {2}, {{5, 8}}}},
      {{d0, 1, {4}, {{14, 17}}}},
      {{d0 + 1, 1, {6}, {{26, 30}}}},
      {{d0 + 2, 1, {1}, {{0, 4}}}},
      {{d0, 1, {2}, {{4, 9}}}},
      {{d0 + 1, 1, {4}, {{13, 19}}}},
      {{d0, 1, {3}, {{10, 13}}}, {d0 + 1, 2, {1, 2}, {{0, 3}, {4, 7}}}},
      {{d0, 1, {1}, {{0, 3}}}},
      {{d0 + 1, 1, {5}, {{20, 25}}}},
    });
}

// Id-mode batches (terms resolved into the field's dictionary at emit, no
// term bytes in the batch) must produce postings identical to byte-mode
// batches of the same analyzer. Includes a value long enough to stream over
// several batches.
TEST(InverterColumnFillTest, MatchesPerValue) {
  const auto features =
    IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs;
  const auto layout = TokenLayout::TermsPosOffs;

  auto make_config = [] {
    analysis::TokenizerConfig cfg;
    cfg.config = analysis::DelimitedTokenizer::Options{.delimiter = ","};
    return cfg;
  };

  std::vector<std::string> docs = {"a,bb,ccc", "bb,a", "ddd,a,bb"};
  // > kCapacity tokens: forces the column fill to stream one value across
  // batches (open-value sentinel).
  std::string long_value;
  for (size_t i = 0; i < 1500; ++i) {
    long_value += "t" + std::to_string(i % 97) + ",";
  }
  long_value.pop_back();
  docs.push_back(std::move(long_value));

  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* by_value = inv.Emplace(1, features);
  auto* by_column = inv.Emplace(2, features);

  {
    auto analyzer = analysis::CreateTokenizer(make_config());
    ASSERT_NE(nullptr, analyzer);
    const auto invert = [&](TokenBatch& batch, DocRuns runs) {
      ASSERT_TRUE(by_value->InvertBlock(batch, runs));
    };
    ::tests::FnTokenSink sink{layout, invert};
    doc_id_t d = doc_limits::min();
    for (const auto& v : docs) {
      sink.writer.BeginValue(d, static_cast<uint32_t>(v.size()));
      ASSERT_TRUE(analyzer->Fill(v, sink.writer, sink.layout));
      sink.writer.EndValue();
      sink.writer.Finish();
      ++d;
    }
  }

  {
    auto analyzer = analysis::CreateTokenizer(make_config());
    ASSERT_NE(nullptr, analyzer);
    std::vector<duckdb::string_t> values;
    std::vector<doc_id_t> doc_ids;
    doc_id_t d = doc_limits::min();
    for (const auto& v : docs) {
      values.push_back(
        duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())});
      doc_ids.push_back(d++);
    }
    const auto invert = [&](TokenBatch& batch, DocRuns runs) {
      ASSERT_TRUE(by_column->InvertBlock(batch, runs));
    };
    ::tests::FnTokenSink sink{layout, invert};
    analyzer->Fill(values, doc_ids, sink.writer, sink.layout);
    sink.writer.Finish();
  }

  ASSERT_EQ(by_value->Dictionary().Size(), by_column->Dictionary().Size());
  ASSERT_EQ(by_value->Log().Size(), by_column->Log().Size());
  AssertSameScatter(*by_value, *by_column, mem);
}

TEST(InverterBlockTextTest, MatchesPerValueTokens) {
  const auto features = IndexFeatures::Freq | IndexFeatures::Pos;
  const auto layout = TokenLayout::TermsPos;

  auto make_config = [] {
    analysis::TokenizerConfig cfg;
    cfg.config = analysis::DelimitedTokenizer::Options{.delimiter = ","};
    return cfg;
  };

  std::vector<std::string> docs = {"a,bb,ccc", "", "bb,a"};
  std::string long_value;
  for (size_t i = 0; i < 1500; ++i) {
    long_value += "t" + std::to_string(i % 97) + ",";
  }
  long_value.pop_back();
  docs.push_back(std::move(long_value));
  docs.push_back("tail,a");

  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* per_value = inv.Emplace(1, features);
  auto* block = inv.Emplace(2, features);

  {
    auto analyzer = analysis::CreateTokenizer(make_config());
    const auto invert = [&](TokenBatch& batch, DocRuns runs) {
      ASSERT_TRUE(per_value->InvertBlock(batch, runs));
    };
    ::tests::FnTokenSink sink{layout, invert};
    doc_id_t d = doc_limits::min();
    for (const auto& v : docs) {
      sink.writer.BeginValue(d, static_cast<uint32_t>(v.size()));
      ASSERT_TRUE(analyzer->Fill(v, sink.writer, sink.layout));
      sink.writer.EndValue();
      sink.writer.Finish();
      ++d;
    }
  }

  {
    auto analyzer = analysis::CreateTokenizer(make_config());
    const auto invert = [&](TokenBatch& batch, DocRuns runs) {
      ASSERT_TRUE(block->InvertBlock(batch, runs));
    };
    ::tests::FnTokenSink sink{layout, invert};
    doc_id_t d = doc_limits::min();
    for (const auto& v : docs) {
      sink.writer.BeginValue(d, static_cast<uint32_t>(v.size()));
      ASSERT_TRUE(analyzer->Fill(v, sink.writer, sink.layout));
      sink.writer.EndValue();
      ++d;
    }
    sink.writer.Finish();
  }

  EXPECT_EQ(DumpField(*per_value, mem), DumpField(*block, mem));
}

template<typename T>
void NumericBlockMatchesPerValue(const std::vector<T>& vals) {
  const auto features = IndexFeatures::None;

  auto mem = InverterMemory::Default();
  FieldsInverter inv_ref{mem};
  FieldsInverter inv_blk{mem};
  auto* ref = inv_ref.Emplace(1, features);
  auto* blk = inv_blk.Emplace(1, features);

  {
    auto batch = std::make_unique<TokenBatch>();
    doc_id_t d = doc_limits::min();
    for (const auto v : vals) {
      batch->count = 0;
      AppendNumericTermsBlock(*batch, std::span<const T>{&v, 1});
      ASSERT_TRUE(::tests::InvertTokens(*ref, d, *batch, /*ends_value=*/true));
      ++d;
    }
  }

  {
    auto batch = std::make_unique<TokenBatch>();
    std::vector<DocRun> runs;
    auto flush = [&] {
      if (batch->count != 0 || !runs.empty()) {
        ASSERT_TRUE(blk->InvertBlock(*batch, {{runs}}));
        batch->count = 0;
        runs.clear();
      }
    };
    doc_id_t d = doc_limits::min();
    for (const auto v : vals) {
      if (batch->count + 4 >= TokenBatch::kCapacity) {
        flush();
      }
      AppendNumericTermsBlock(*batch, std::span<const T>{&v, 1});
      runs.push_back({d, NumericTermCount<T>()});
      ++d;
    }
    flush();
  }

  ASSERT_EQ(ref->Dictionary().Size(), blk->Dictionary().Size());
  ASSERT_EQ(ref->Log().Size(), blk->Log().Size());
  AssertSameScatter(*ref, *blk, mem);
}

TEST(InverterNumericBlockTest, MatchesPerValueStream) {
  {
    std::vector<int32_t> vals = {0,
                                 1,
                                 -1,
                                 42,
                                 65536,
                                 -65536,
                                 std::numeric_limits<int32_t>::min(),
                                 std::numeric_limits<int32_t>::max()};
    for (int32_t i = 0; i < 3000; ++i) {
      vals.push_back((i % 700) * 1000003);
    }
    NumericBlockMatchesPerValue(vals);
  }
  {
    std::vector<int64_t> vals = {0, -1, std::numeric_limits<int64_t>::min(),
                                 std::numeric_limits<int64_t>::max()};
    for (int64_t i = 0; i < 3000; ++i) {
      vals.push_back((i % 700) * 68719476737LL);
    }
    NumericBlockMatchesPerValue(vals);
  }
  {
    std::vector<double> vals = {0.0,
                                -0.0,
                                1.5,
                                -273.15,
                                std::numeric_limits<double>::infinity(),
                                -std::numeric_limits<double>::infinity()};
    for (int i = 0; i < 3000; ++i) {
      vals.push_back(static_cast<double>(i % 700) * 0.1);
    }
    NumericBlockMatchesPerValue(vals);
  }
}

TEST(InverterNumericBlockTest, BooleanMatchesPerValueStream) {
  const auto features = IndexFeatures::None;

  std::vector<bool> vals;
  for (int i = 0; i < 3000; ++i) {
    vals.push_back((i % 7) < 3);
  }

  auto mem = InverterMemory::Default();
  FieldsInverter inv_ref{mem};
  FieldsInverter inv_blk{mem};
  auto* ref = inv_ref.Emplace(1, features);
  auto* blk = inv_blk.Emplace(1, features);

  {
    auto batch = std::make_unique<TokenBatch>();
    doc_id_t d = doc_limits::min();
    for (const bool v : vals) {
      batch->count = 0;
      const auto term = BooleanTerm(v);
      batch->terms[batch->count++] =
        duckdb::string_t{term.data(), static_cast<uint32_t>(term.size())};
      ASSERT_TRUE(::tests::InvertTokens(*ref, d, *batch, /*ends_value=*/true));
      ++d;
    }
  }

  {
    std::vector<duckdb::string_t> terms;
    std::vector<doc_id_t> docs;
    doc_id_t d = doc_limits::min();
    for (const bool v : vals) {
      const auto term = BooleanTerm(v);
      terms.push_back(
        duckdb::string_t{term.data(), static_cast<uint32_t>(term.size())});
      docs.push_back(d++);
    }
    ASSERT_TRUE(blk->InvertKeywordBlock(terms, docs));
  }

  ASSERT_EQ(ref->Dictionary().Size(), blk->Dictionary().Size());
  ASSERT_EQ(ref->Log().Size(), blk->Log().Size());
  AssertSameScatter(*ref, *blk, mem);
}

TEST(InverterConstantBlockTest, MatchesPerValueKeyword) {
  const auto features = IndexFeatures::None;
  const auto term = ViewCast<byte_type>(kNullTerm);

  std::vector<doc_id_t> docs;
  doc_id_t d = doc_limits::min();
  for (int i = 0; i < 3000; ++i, ++d) {
    if (i % 3 == 0) {
      continue;
    }
    docs.push_back(d);
  }

  auto mem = InverterMemory::Default();
  FieldsInverter inv_ref{mem};
  FieldsInverter inv_blk{mem};
  auto* ref = inv_ref.Emplace(1, features);
  auto* blk = inv_blk.Emplace(1, features);

  for (const auto doc : docs) {
    ASSERT_TRUE(InvertKeyword(*ref, doc, term));
  }
  ASSERT_TRUE(blk->InvertConstantBlock(term, docs));

  ASSERT_EQ(ref->Dictionary().Size(), blk->Dictionary().Size());
  ASSERT_EQ(ref->Log().Size(), blk->Log().Size());
  AssertSameScatter(*ref, *blk, mem);
}

// Block pipeline (kernel source + in-place transform/filter stages, one
// virtual call per stage per batch) must match the legacy PipelineTokenizer
// pull chain (N virtual calls per token) over the same logical chain:
// split -> lowercase -> stopwords. Input is comma-separated ASCII so the
// delimiter(,) and split-by-non-alpha segmenters agree; includes a
// >kCapacity-token value so stages run across source resumptions.

}  // namespace
