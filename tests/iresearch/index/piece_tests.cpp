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

#include <array>
#include <duckdb/storage/storage_info.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/formats/column/norm_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/levenshtein_acceptor.hpp>
#include <iresearch/utils/levenshtein_utils.hpp>
#include <string>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "test_index_writer.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

using namespace irs;

struct ReplayToken {
  std::string term;
  uint32_t inc;
  uint32_t offs_start;
  uint32_t offs_end;
};

class ReplayStream final : public analysis::TypedTokenizer<ReplayStream> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "ReplayStream";
  }

  void Reset(const std::vector<ReplayToken>* tokens) noexcept {
    _tokens = tokens;
  }

  irs::TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = true,
      .offsets = true,
    };
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t, TokenSink& sink) {
    uint32_t pos = 0;
    for (const auto& t : *_tokens) {
      pos += t.inc;
      ::tests::EmitCopy<Layout>(
        sink,
        bytes_view{reinterpret_cast<const byte_type*>(t.term.data()),
                   t.term.size()},
        pos, Offs{t.offs_start, t.offs_end});
    }
    return true;
  }

 private:
  const std::vector<ReplayToken>* _tokens = nullptr;
};

struct ReplayField {
  field_id Id() const noexcept { return id; }
  IndexFeatures GetIndexFeatures() const noexcept { return features; }
  analysis::Tokenizer& GetTokens() const noexcept { return *stream; }
  std::string_view Value() const noexcept { return {}; }

  field_id id;
  IndexFeatures features;
  ReplayStream* stream;
};

constexpr field_id kText = 1;
constexpr field_id kKeyword = 2;
constexpr field_id kFreqOnly = 3;
constexpr auto kTextFeatures = IndexFeatures::Freq | IndexFeatures::Pos |
                               IndexFeatures::Offs | IndexFeatures::Norm;
constexpr auto kKeywordFeatures = IndexFeatures::Freq | IndexFeatures::Pos;
constexpr auto kFreqOnlyFeatures = IndexFeatures::Freq;

std::vector<ReplayToken> MakeDocTokens(uint32_t seed, size_t count) {
  std::vector<ReplayToken> tokens;
  tokens.reserve(count);
  uint32_t offs = 0;
  uint64_t state = seed * 2654435761u + 1;
  for (size_t i = 0; i < count; ++i) {
    state = state * 6364136223846793005ull + 1442695040888963407ull;
    const auto rank = static_cast<uint32_t>((state >> 33) % 300);
    std::string term = "t" + std::to_string(rank * rank % 997);
    const auto len = static_cast<uint32_t>(term.size());
    const uint32_t inc = (i != 0 && (state >> 21) % 16 == 0) ? 0 : 1;
    tokens.push_back({std::move(term), inc, offs, offs + len});
    offs += len + 1;
  }
  return tokens;
}

void InsertDocs(IndexWriter::Transaction& trx, uint32_t docs,
                uint32_t first = 0) {
  ReplayStream text;
  ReplayStream keyword;
  ReplayStream freq;
  for (uint32_t d = first; d < first + docs; ++d) {
    auto doc = trx.Insert();
    const auto text_tokens = MakeDocTokens(d, 3 + d % 40);
    text.Reset(&text_tokens);
    ASSERT_TRUE(
      ::tests::InsertField(doc, ReplayField{kText, kTextFeatures, &text}));
    const std::vector<ReplayToken> kw = {
      {"cat" + std::to_string(d % 17), 1, 0, 0}};
    keyword.Reset(&kw);
    ASSERT_TRUE(::tests::InsertField(
      doc, ReplayField{kKeyword, kKeywordFeatures, &keyword}));
    if (d % 4 != 0) {
      const std::vector<ReplayToken> fo = {
        {"bucket" + std::to_string(d % 5), 1, 0, 0},
        {"bucket" + std::to_string((d + 1) % 5), 1, 0, 0}};
      freq.Reset(&fo);
      ASSERT_TRUE(::tests::InsertField(
        doc, ReplayField{kFreqOnly, kFreqOnlyFeatures, &freq}));
    }
  }
}

struct Postings {
  std::vector<doc_id_t> docs;
  std::vector<uint32_t> freqs;
  std::vector<uint32_t> pos;
  std::vector<uint32_t> offs_start;
  std::vector<uint32_t> offs_len;

  bool operator==(const Postings&) const = default;
};

Postings ReadPostings(TermPostings& it, IndexFeatures features) {
  const bool has_freq = IndexFeatures::None != (features & IndexFeatures::Freq);
  const bool has_pos = IndexFeatures::None != (features & IndexFeatures::Pos);
  const bool has_offs = IndexFeatures::None != (features & IndexFeatures::Offs);
  Postings out;
  doc_id_t docs[doc_limits::kBlockSize];
  uint32_t freqs[doc_limits::kBlockSize];
  while (const auto n = it.NextDocs(docs, has_freq ? freqs : nullptr)) {
    out.docs.insert(out.docs.end(), docs, docs + n);
    if (!has_freq) {
      continue;
    }
    out.freqs.insert(out.freqs.end(), freqs, freqs + n);
    if (!has_pos) {
      continue;
    }
    uint64_t total = 0;
    for (uint32_t i = 0; i != n; ++i) {
      total += freqs[i];
    }
    const auto at = out.pos.size();
    out.pos.resize(at + total);
    if (has_offs) {
      out.offs_start.resize(at + total);
      out.offs_len.resize(at + total);
      it.NextPositions(out.pos.data() + at, out.offs_start.data() + at,
                       out.offs_len.data() + at, static_cast<uint32_t>(total));
    } else {
      it.NextPositions(out.pos.data() + at, nullptr, nullptr,
                       static_cast<uint32_t>(total));
    }
  }
  return out;
}

void ExpectSameField(const TermReader& expected, const TermReader& actual) {
  EXPECT_EQ(expected.size(), actual.size());
  EXPECT_EQ(expected.docs_count(), actual.docs_count());
  EXPECT_EQ(expected.min(), actual.min());
  EXPECT_EQ(expected.max(), actual.max());
  EXPECT_EQ(expected.meta().index_features, actual.meta().index_features);
  EXPECT_EQ(expected.meta().norm, actual.meta().norm);
  EXPECT_EQ(expected.meta().dict, actual.meta().dict);
  EXPECT_EQ(expected.key_width, actual.key_width);
  const auto* expected_freq = irs::get<FreqAttr>(expected);
  const auto* actual_freq = irs::get<FreqAttr>(actual);
  ASSERT_EQ(expected_freq == nullptr, actual_freq == nullptr);
  if (expected_freq) {
    EXPECT_EQ(expected_freq->value, actual_freq->value);
  }
  const auto features = expected.meta().index_features;
  auto e = expected.iterator();
  auto a = actual.iterator();
  while (e->next()) {
    ASSERT_TRUE(a->next());
    ASSERT_EQ(e->value(), a->value());
    EXPECT_EQ(e->cookie().docs_count, a->cookie().docs_count);
    EXPECT_EQ(e->cookie().freq, a->cookie().freq);
    EXPECT_EQ(ReadPostings(*e->postings(features), features),
              ReadPostings(*a->postings(features), features));
    EXPECT_EQ(expected.Lookup(e->value()).docs_count,
              actual.Lookup(e->value()).docs_count);
    std::vector<doc_id_t> expected_docs;
    std::vector<doc_id_t> actual_docs;
    expected.ReadDocs(e->value(), [&](doc_id_t doc) {
      expected_docs.push_back(doc);
      return true;
    });
    actual.ReadDocs(e->value(), [&](doc_id_t doc) {
      actual_docs.push_back(doc);
      return true;
    });
    EXPECT_EQ(expected_docs, actual_docs);
  }
  EXPECT_FALSE(a->next());
  EXPECT_EQ(
    0,
    actual.Lookup(ViewCast<byte_type>(std::string_view{"absent"})).docs_count);
}

void ExpectSameSegment(const SubReader& expected, const SubReader& actual) {
  EXPECT_EQ(expected.docs_count(), actual.docs_count());
  EXPECT_EQ(expected.live_docs_count(), actual.live_docs_count());
  const auto expected_ids = expected.field_ids();
  const auto actual_ids = actual.field_ids();
  ASSERT_EQ(std::vector<field_id>(expected_ids.begin(), expected_ids.end()),
            std::vector<field_id>(actual_ids.begin(), actual_ids.end()));
  for (const auto id : expected_ids) {
    const auto expected_fields = expected.fields(id);
    const auto actual_fields = actual.fields(id);
    ASSERT_EQ(expected_fields.size(), actual_fields.size());
    for (size_t i = 0; i != expected_fields.size(); ++i) {
      ExpectSameField(*expected_fields[i], *actual_fields[i]);
    }
  }
  const auto norm = expected.fields(kText).front()->meta().norm;
  ASSERT_TRUE(field_limits::valid(norm));
  auto expected_norms = expected.norms(norm);
  auto actual_norms = actual.norms(norm);
  ASSERT_NE(nullptr, expected_norms);
  ASSERT_NE(nullptr, actual_norms);
  for (doc_id_t doc = doc_limits::min();
       doc != doc_limits::min() + expected.docs_count(); ++doc) {
    EXPECT_EQ(expected_norms->Get(doc), actual_norms->Get(doc));
  }
}

TEST(PieceTest, WrittenPiecesReadLikeFlushedSegment) {
  constexpr uint32_t kDocs = 300;
  constexpr uint32_t kPieceDocs = 100;

  MemoryDirectory flushed_dir;
  auto flushed_writer = ::tests::IndexWriter::Make(
    flushed_dir, ::tests::kOmCreate, irs::tests::DefaultWriterOptions());
  {
    auto trx = flushed_writer->GetBatch();
    InsertDocs(trx.Native(), kDocs);
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(flushed_writer->RefreshCommit());
  const auto flushed = flushed_writer->GetSnapshot();
  ASSERT_EQ(1, flushed.size());

  MemoryDirectory piece_dir;
  auto piece_writer =
    IndexWriter::Make(piece_dir, irs::tests::DefaultWriterOptions());
  std::vector<SegmentReader> pieces;
  for (uint32_t first = 0; first != kDocs; first += kPieceDocs) {
    auto trx = piece_writer->GetBatch();
    trx.SetHot(0);
    InsertDocs(trx, kPieceDocs, first);
    auto piece = trx.SealPiece(first + 1);
    trx.Abort();
    ASSERT_NE(nullptr, piece);
    pieces.emplace_back(std::move(piece));
  }
  size_t files = 0;
  piece_dir.visit([&](std::string_view) {
    ++files;
    return true;
  });
  EXPECT_EQ(0, files);

  DirectoryReader source{piece_dir, irs::tests::DefaultReaderOptions(),
                         std::move(pieces), /*stats=*/false};
  MemoryDirectory written_dir;
  auto writer =
    IndexWriter::Make(written_dir, irs::tests::DefaultWriterOptions());
  auto written = GetBlocking(writer->WriteAsync(
    std::move(source), writer->NextSegmentId(), nullptr, [] { return true; },
    nullptr));
  ASSERT_NE(nullptr, written.reader);
  EXPECT_EQ(kDocs / kPieceDocs, written.maps.size());
  ExpectSameSegment(flushed[0], *written.reader);
}

TEST(PieceTest, MergedPiecesReadLikeFlushedSegment) {
  constexpr std::array<uint32_t, 5> kPieceDocs{1, 1, 37, 100, 161};
  constexpr uint32_t kDocs = 300;

  MemoryDirectory flushed_dir;
  auto flushed_writer = ::tests::IndexWriter::Make(
    flushed_dir, ::tests::kOmCreate, irs::tests::DefaultWriterOptions());
  {
    auto trx = flushed_writer->GetBatch();
    InsertDocs(trx.Native(), kDocs);
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(flushed_writer->RefreshCommit());
  const auto flushed = flushed_writer->GetSnapshot();
  ASSERT_EQ(1, flushed.size());

  MemoryDirectory piece_dir;
  auto piece_writer =
    IndexWriter::Make(piece_dir, irs::tests::DefaultWriterOptions());
  std::vector<SegmentReader> pieces;
  uint32_t first = 0;
  for (const auto docs : kPieceDocs) {
    auto trx = piece_writer->GetBatch();
    trx.SetHot(0);
    InsertDocs(trx, docs, first);
    auto piece = trx.SealPiece(first + 1);
    trx.Abort();
    ASSERT_NE(nullptr, piece);
    pieces.emplace_back(std::move(piece));
    first += docs;
  }
  ASSERT_EQ(kDocs, first);

  DirectoryReader source{piece_dir, irs::tests::DefaultReaderOptions(),
                         std::move(pieces), /*stats=*/false};
  MemoryDirectory merged_dir;
  auto writer =
    IndexWriter::Make(merged_dir, irs::tests::DefaultWriterOptions());
  const auto merged =
    writer->MergePieces(std::move(source), writer->NextSegmentId(), nullptr);
  ASSERT_NE(nullptr, merged.reader);
  EXPECT_EQ(kPieceDocs.size(), merged.maps.size());
  ExpectSameSegment(flushed[0], *merged.reader);
  size_t files = 0;
  merged_dir.visit([&](std::string_view) {
    ++files;
    return true;
  });
  EXPECT_EQ(0, files);
}

TEST(PieceTest, SealedPieceReadsLikeFlushedSegment) {
  constexpr uint32_t kDocs = 300;

  MemoryDirectory flushed_dir;
  auto flushed_writer = ::tests::IndexWriter::Make(
    flushed_dir, ::tests::kOmCreate, irs::tests::DefaultWriterOptions());
  {
    auto trx = flushed_writer->GetBatch();
    InsertDocs(trx.Native(), kDocs);
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(flushed_writer->RefreshCommit());
  const auto flushed = flushed_writer->GetSnapshot();
  ASSERT_EQ(1, flushed.size());
  const auto& expected = flushed[0];

  MemoryDirectory piece_dir;
  auto piece_writer =
    IndexWriter::Make(piece_dir, irs::tests::DefaultWriterOptions());
  std::shared_ptr<const SegmentReaderImpl> piece;
  {
    auto trx = piece_writer->GetBatch();
    trx.SetHot(0);
    InsertDocs(trx, kDocs);
    piece = trx.SealPiece(42);
    trx.Abort();
  }
  ASSERT_NE(nullptr, piece);
  EXPECT_EQ(42, piece->Id());
  EXPECT_EQ(expected.docs_count(), piece->docs_count());
  EXPECT_EQ(expected.live_docs_count(), piece->live_docs_count());

  const auto expected_ids = expected.field_ids();
  const auto actual_ids = piece->field_ids();
  ASSERT_EQ(std::vector<field_id>(expected_ids.begin(), expected_ids.end()),
            std::vector<field_id>(actual_ids.begin(), actual_ids.end()));
  for (const auto id : expected_ids) {
    const auto expected_fields = expected.fields(id);
    const auto actual_fields = piece->fields(id);
    ASSERT_EQ(expected_fields.size(), actual_fields.size());
    for (size_t i = 0; i != expected_fields.size(); ++i) {
      ExpectSameField(*expected_fields[i], *actual_fields[i]);
    }
  }

  const auto norm = expected.fields(kText).front()->meta().norm;
  ASSERT_TRUE(field_limits::valid(norm));
  auto expected_norms = expected.norms(norm);
  auto actual_norms = piece->norms(norm);
  ASSERT_NE(nullptr, expected_norms);
  ASSERT_NE(nullptr, actual_norms);
  for (doc_id_t doc = doc_limits::min(); doc != doc_limits::min() + kDocs;
       ++doc) {
    EXPECT_EQ(expected_norms->Get(doc), actual_norms->Get(doc));
  }
}

TEST(PieceTest, AbortedPieceRemovesItsFiles) {
  MemoryDirectory dir;
  auto writer = IndexWriter::Make(dir, irs::tests::DefaultWriterOptions());
  {
    auto trx = writer->GetBatch();
    trx.SetHot(0);
    const std::vector<ReplayToken> short_doc = {{"word", 1, 0, 4}};
    const std::vector<ReplayToken> long_doc = {{"word", 1, 0, 4},
                                               {"more", 1, 5, 9}};
    ReplayStream text;
    for (uint64_t d = 0; d <= DEFAULT_ROW_GROUP_SIZE; ++d) {
      text.Reset(d % 2 == 0 ? &short_doc : &long_doc);
      auto doc = trx.Insert();
      ASSERT_TRUE(
        ::tests::InsertField(doc, ReplayField{kText, kTextFeatures, &text}));
    }
    trx.Abort();
  }
  size_t files = 0;
  dir.visit([&](std::string_view) {
    ++files;
    return true;
  });
  EXPECT_EQ(0, files);
}

TEST(PieceTest, SealsSingleDocumentPieces) {
  MemoryDirectory dir;
  auto writer = IndexWriter::Make(dir, irs::tests::DefaultWriterOptions());
  for (uint64_t id = 1; id != 4; ++id) {
    auto trx = writer->GetBatch();
    trx.SetHot(0);
    InsertDocs(trx, 1);
    const auto piece = trx.SealPiece(id);
    trx.Abort();
    ASSERT_NE(nullptr, piece);
    EXPECT_EQ(id, piece->Id());
    EXPECT_EQ(1, piece->docs_count());
    const auto fields = piece->fields(kText);
    ASSERT_EQ(1, fields.size());
    const auto norm = fields.front()->meta().norm;
    auto norms = piece->norms(norm);
    ASSERT_NE(nullptr, norms);
    EXPECT_EQ(3, norms->Get(doc_limits::min()));
    const ReadContext context{*piece->GetColReader(), /*random_access=*/false};
    EXPECT_EQ(DEFAULT_BLOCK_ALLOC_SIZE, context.GetBlockAllocSize());
  }
}

TEST(PieceTest, FuzzyWalksPublishTheirDistanceFromTheStart) {
  constexpr uint32_t kDocs = 50;
  MemoryDirectory flushed_dir;
  auto flushed_writer = ::tests::IndexWriter::Make(
    flushed_dir, ::tests::kOmCreate, irs::tests::DefaultWriterOptions());
  {
    auto trx = flushed_writer->GetBatch();
    InsertDocs(trx.Native(), kDocs);
    ASSERT_TRUE(trx.Commit());
  }
  ASSERT_TRUE(flushed_writer->RefreshCommit());
  const auto flushed = flushed_writer->GetSnapshot();
  ASSERT_EQ(1, flushed.size());

  MemoryDirectory piece_dir;
  auto piece_writer =
    IndexWriter::Make(piece_dir, irs::tests::DefaultWriterOptions());
  auto trx = piece_writer->GetBatch();
  trx.SetHot(0);
  InsertDocs(trx, kDocs);
  const auto piece = trx.SealPiece(9);
  trx.Abort();
  ASSERT_NE(nullptr, piece);

  const auto description = MakeParametricDescription(1, false);
  const LevenshteinAcceptor fuzzy{
    description, kEmptyStringView<byte_type>,
    ViewCast<byte_type>(std::string_view{"cat1"})};
  const auto walk = [&](const TermReader& field) {
    std::vector<std::pair<std::string, byte_type>> terms;
    auto it = field.iterator(fuzzy);
    const auto* payload = irs::get<PayAttr>(*it);
    EXPECT_NE(nullptr, payload);
    if (!payload) {
      return terms;
    }
    EXPECT_EQ(1, payload->value.size());
    while (it->next()) {
      terms.emplace_back(ViewCast<char>(it->value()), payload->value[0]);
    }
    return terms;
  };
  const auto expected = walk(*flushed[0].fields(kKeyword).front());
  EXPECT_FALSE(expected.empty());
  EXPECT_EQ(expected, walk(*piece->fields(kKeyword).front()));
}

TEST(PieceTest, SeeksAndRangesFollowTheSortedTerms) {
  MemoryDirectory dir;
  auto writer = IndexWriter::Make(dir, irs::tests::DefaultWriterOptions());
  auto trx = writer->GetBatch();
  trx.SetHot(0);
  InsertDocs(trx, 50);
  const auto piece = trx.SealPiece(7);
  trx.Abort();
  ASSERT_NE(nullptr, piece);
  const auto fields = piece->fields(kKeyword);
  ASSERT_EQ(1, fields.size());
  const auto& field = *fields.front();
  std::vector<std::string> terms;
  for (auto it = field.iterator(); it->next();) {
    terms.emplace_back(ViewCast<char>(it->value()));
  }
  ASSERT_EQ(17, terms.size());
  ASSERT_TRUE(absl::c_is_sorted(terms));

  auto it = field.iterator();
  EXPECT_EQ(SeekResult::Found,
            it->seek_ge(ViewCast<byte_type>(std::string_view{terms[5]})));
  EXPECT_EQ(terms[5], ViewCast<char>(it->value()));
  EXPECT_TRUE(it->next());
  EXPECT_EQ(terms[6], ViewCast<char>(it->value()));
  const auto between = terms[8] + "0";
  EXPECT_EQ(SeekResult::NotFound,
            it->seek_ge(ViewCast<byte_type>(std::string_view{between})));
  EXPECT_EQ(terms[9], ViewCast<char>(it->value()));
  EXPECT_EQ(SeekResult::End,
            it->seek_ge(ViewCast<byte_type>(std::string_view{"zzz"})));
  EXPECT_FALSE(it->seek(ViewCast<byte_type>(std::string_view{between})));

  std::vector<std::string> visited;
  field.VisitRange(ViewCast<byte_type>(std::string_view{terms[2]}),
                   ViewCast<byte_type>(std::string_view{terms[6]}),
                   [&](bytes_view term, const PostingMeta& meta) {
                     EXPECT_NE(0, meta.docs_count);
                     visited.emplace_back(ViewCast<char>(term));
                   });
  EXPECT_EQ(std::vector<std::string>(terms.begin() + 2, terms.begin() + 6),
            visited);

  std::vector<size_t> found;
  const std::vector<std::string> probes{terms[1], "cat-none", terms[3]};
  field.LookupBatch(
    probes.size(),
    [&](size_t i) { return ViewCast<byte_type>(std::string_view{probes[i]}); },
    [&](size_t i, const PostingMeta& meta) {
      EXPECT_NE(0, meta.docs_count);
      found.push_back(i);
    });
  EXPECT_EQ((std::vector<size_t>{0, 2}), found);
}

}  // namespace
