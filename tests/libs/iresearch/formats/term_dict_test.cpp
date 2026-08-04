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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <jemalloc/jemalloc.h>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "basics/duckdb_engine.h"
#include "formats/formats_test_case_base.hpp"
#include "iresearch/formats/empty_term_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/index/term_dict.hpp"
#include "iresearch/formats/posting/wand_writer.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/term_acceptor.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/levenshtein_acceptor.hpp"
#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"
#include "re2/re2.h"
#include "tests_shared.hpp"

namespace {

constexpr std::string_view kCodec = "1_5simd";
constexpr irs::field_id kFieldId = 1;
constexpr std::string_view kSegment = "term_dict_segment";

using Docs = std::vector<std::pair<irs::doc_id_t, uint32_t>>;

irs::bstring MakeTerm(std::string_view str) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(str)};
}

Docs MakeDocs(size_t count, uint32_t step) {
  Docs docs;
  docs.reserve(count);
  irs::doc_id_t doc = irs::doc_limits::min();
  for (size_t i = 0; i != count; ++i) {
    docs.emplace_back(doc, std::max(1U, doc % 7));
    doc += step;
  }
  return docs;
}

// A field of `count` terms sharing long prefixes: exercises front coding,
// multiple blocks, separators and (past the sample threshold) FSST.
std::vector<irs::bstring> MakeTerms(size_t count, std::string_view prefix) {
  std::vector<irs::bstring> terms;
  terms.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    terms.emplace_back(
      MakeTerm(absl::StrCat(prefix, absl::Dec(i, absl::kZeroPad8))));
  }
  std::ranges::sort(terms);
  return terms;
}

// Syllable-built vocabulary: long distinct suffixes with repeating byte
// pairs, i.e. the shape the FSST gate is meant to accept.
std::vector<irs::bstring> MakeWordyTerms(size_t count) {
  static constexpr std::string_view kSyllables[] = {
    "ka",  "re",  "shi", "to",  "mu",  "na",  "ri",  "ze",  "bo",  "lu",
    "tan", "ver", "sil", "mor", "dan", "kul", "pes", "rin", "vol", "hem",
  };
  constexpr size_t kParts = std::size(kSyllables);
  std::vector<irs::bstring> terms;
  terms.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    std::string word = "document_field_";
    uint64_t x = i * 2654435761ULL + 12345;
    for (size_t j = 0; j != 8; ++j) {
      x = x * 6364136223846793005ULL + 1442695040888963407ULL;
      word += kSyllables[(x >> 33) % kParts];
    }
    terms.emplace_back(MakeTerm(word));
  }
  std::ranges::sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  return terms;
}

uint64_t IdxSize(const irs::Directory& dir) {
  uint64_t size = 0;
  EXPECT_TRUE(dir.length(size, absl::StrCat(kSegment, ".idx")));
  return size;
}

struct Corpus {
  std::vector<irs::bstring> terms;
  Docs docs;
  irs::IndexFeatures features{irs::IndexFeatures::None};
  size_t doc_count{128};
};

template<typename Writer>
void WriteField(irs::Directory& dir, const Corpus& corpus, Writer&& configure) {
  auto codec = irs::formats::Get(kCodec);
  ASSERT_NE(nullptr, codec);

  irs::FieldMeta meta;
  meta.id = kFieldId;
  meta.index_features = corpus.features;

  const size_t doc_count =
    corpus.docs.empty() ? corpus.doc_count : corpus.docs.back().first + 1;
  irs::FlushState state{
    .dir = &dir,
    .name = kSegment,
    .doc_count = doc_count,
    .index_features = corpus.features,
  };

  tests::FormatTestCase::Terms<std::vector<irs::bstring>::const_iterator> terms{
    corpus.terms.begin(), corpus.terms.end(), corpus.docs.begin(),
    corpus.docs.end()};
  tests::MockTermReader reader{
    terms, meta,
    corpus.terms.empty() ? irs::bytes_view{} : corpus.terms.front(),
    corpus.terms.empty() ? irs::bytes_view{} : corpus.terms.back()};

  irs::IdxWriter idx{dir, kSegment, ::sdb::DuckDBEngine::Instance().instance()};
  configure(idx, *codec, state, reader);
  idx.Commit();
}

void WriteV2(irs::Directory& dir, const Corpus& corpus,
             const irs::term_dict::WriterOptions& options) {
  WriteField(
    dir, corpus,
    [&](irs::IdxWriter& idx, const irs::Format& codec,
        const irs::FlushState& state, const irs::BasicTermReader& reader) {
      irs::term_dict::FieldWriter writer{
        codec.get_postings_writer(/*compaction=*/false,
                                  irs::IResourceManager::gNoop),
        /*compaction=*/false, irs::IResourceManager::gNoop};
      writer.SetIdxWriter(idx);
      writer.SetOptions(options);
      writer.prepare(state);
      writer.write(reader);
      writer.end();
    });
}

// Keeps the reader objects the returned TermReader points into alive.
template<typename Reader>
struct OpenedField {
  irs::SegmentMeta meta;
  std::unique_ptr<irs::IdxReader> idx;
  std::unique_ptr<Reader> fields;

  const irs::TermReader* Field() const { return fields->field(kFieldId); }
};

template<typename Reader>
OpenedField<Reader> Open(const irs::Directory& dir) {
  auto codec = irs::formats::Get(kCodec);
  OpenedField<Reader> opened;
  opened.meta.name = kSegment;
  opened.idx = std::make_unique<irs::IdxReader>(dir, kSegment);
  opened.fields = std::make_unique<Reader>(codec->get_postings_reader(),
                                           irs::IResourceManager::gNoop);
  opened.fields->prepare(irs::ReaderState{
    .dir = &dir, .meta = &opened.meta, .idx = opened.idx.get()});
  return opened;
}

std::vector<std::pair<irs::doc_id_t, uint32_t>> ReadPostings(
  irs::DocIterator& it, bool with_freq) {
  std::vector<std::pair<irs::doc_id_t, uint32_t>> out;
  irs::doc_id_t doc;
  while (!irs::doc_limits::eof(doc = it.advance())) {
    out.emplace_back(doc, with_freq ? it.GetFreq() : 0);
  }
  return out;
}

irs::bstring Between(irs::bytes_view lhs) {
  irs::bstring probe{lhs};
  probe.push_back(0);
  return probe;
}

// A corpus whose terms carry different posting lists, so one field mixes the
// shapes the rg-list has to encode: row groups with a single document, row
// groups long enough to own a skip list, terms confined to one row group, and
// terms whose postings straddle a boundary.
struct RgCorpus {
  std::vector<irs::bstring> terms;
  std::vector<Docs> docs;
  irs::IndexFeatures features{irs::IndexFeatures::None};
  size_t doc_count{0};
};

// The shared `Terms` mock hardcodes `IndexFeatures::None` for its postings, so
// the writer never sees positions through it.
class RgTermIterator final : public irs::SourceTermIterator {
 public:
  explicit RgTermIterator(const RgCorpus& corpus) : _corpus{&corpus} {}

  bool next() final {
    if (_next == _corpus->terms.size()) {
      return false;
    }
    _cur = _next++;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _corpus->terms[_cur]; }

  irs::DocIterator::ptr postings(irs::IndexFeatures) const final {
    return irs::memory::make_managed<tests::FormatTestCase::TestPostings>(
      _corpus->docs[_cur], _corpus->features);
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

 private:
  const RgCorpus* _corpus;
  size_t _cur{0};
  size_t _next{0};
};

template<typename Configure>
void WriteRgField(irs::Directory& dir, const RgCorpus& corpus,
                  Configure&& configure, bool dictless = false) {
  auto codec = irs::formats::Get(kCodec);
  ASSERT_NE(nullptr, codec);

  irs::FieldMeta meta;
  meta.id = kFieldId;
  meta.index_features = corpus.features;
  meta.dictless = dictless;

  irs::FlushState state{
    .dir = &dir,
    .name = kSegment,
    .doc_count = corpus.doc_count,
    .index_features = corpus.features,
  };

  RgTermIterator terms{corpus};
  tests::MockTermReader reader{terms, meta, corpus.terms.front(),
                               corpus.terms.back()};

  irs::IdxWriter idx{dir, kSegment, ::sdb::DuckDBEngine::Instance().instance()};
  configure(idx, *codec, state, reader);
  idx.Commit();
}

void WriteRg(irs::Directory& dir, const RgCorpus& corpus,
             uint32_t row_group_size, bool dictless = false) {
  WriteRgField(
    dir, corpus,
    [&](irs::IdxWriter& idx, const irs::Format& codec,
        const irs::FlushState& state, const irs::BasicTermReader& reader) {
      irs::term_dict::WriterOptions options;
      options.row_group_size = row_group_size;
      irs::term_dict::FieldWriter writer{
        codec.get_postings_writer(/*compaction=*/false,
                                  irs::IResourceManager::gNoop),
        /*compaction=*/false, irs::IResourceManager::gNoop};
      writer.SetIdxWriter(idx);
      writer.SetOptions(options);
      writer.prepare(state);
      writer.write(reader);
      writer.end();
    },
    dictless);
}

// One document of a term, addressed the way the scan will address it.
struct RgHit {
  uint32_t rg;
  irs::doc_id_t local;
  uint32_t freq;
  std::vector<uint32_t> positions;
  std::vector<std::pair<uint32_t, uint32_t>> offsets;

  bool operator==(const RgHit&) const = default;
};

void ReadHits(irs::DocIterator& it, irs::IndexFeatures features,
              const irs::RowGroupLayout& layout, uint32_t rg, bool decompose,
              std::vector<RgHit>& out) {
  const bool with_freq =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);
  auto* pos = irs::GetMutable<irs::PosAttr>(&it);
  const auto* offs = pos ? irs::get<irs::OffsAttr>(*pos) : nullptr;

  irs::doc_id_t doc;
  while (!irs::doc_limits::eof(doc = it.advance())) {
    auto& hit = out.emplace_back();
    if (decompose) {
      // A doc id of the unpartitioned index, split through the partitioned
      // index's directory. The reverse -- rebuilding a segment-wide id from
      // (rg, local) -- is what the id model forbids.
      hit.rg = (doc - irs::doc_limits::min()) / layout.rows_per_group;
      hit.local = doc - hit.rg * layout.rows_per_group;
    } else {
      hit.rg = rg;
      hit.local = doc;
    }
    hit.freq = with_freq ? it.GetFreq() : 0;
    if (!pos) {
      continue;
    }
    while (pos->next()) {
      hit.positions.push_back(pos->value());
      if (offs) {
        hit.offsets.emplace_back(offs->start, offs->end);
      }
    }
  }
}

// The expected hit stream of one term, computed from the corpus rather than
// read back from any index. `TestPostings` derives a document's positions from
// its id (`doc + 1 .. doc + freq`) and an offset from its position, and
// partitioning changes only the addressing -- so this is the whole expected
// answer, and it is the one oracle a bug in the row-group adapter cannot
// cancel out against.
std::vector<RgHit> ExpectedHits(const Docs& docs, irs::IndexFeatures features,
                                uint32_t row_group_size) {
  const bool with_freq =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);
  const bool with_pos =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos);
  const bool with_offs =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs);

  std::vector<RgHit> out;
  out.reserve(docs.size());
  for (const auto [doc, freq] : docs) {
    auto& hit = out.emplace_back();
    hit.rg = (doc - irs::doc_limits::min()) / row_group_size;
    hit.local = doc - hit.rg * row_group_size;
    hit.freq = with_freq ? freq : 0;
    if (!with_pos) {
      continue;
    }
    for (uint32_t p = doc + 1; p <= doc + freq; ++p) {
      hit.positions.push_back(p);
      if (with_offs) {
        hit.offsets.emplace_back(p, p + absl::StrCat(p).size());
      }
    }
  }
  return out;
}

}  // namespace

class TermDictTest : public ::testing::Test {};

TEST_F(TermDictTest, forward_iteration) {
  Corpus corpus;
  corpus.terms = MakeTerms(2000, "prefix_shared_");
  corpus.docs = MakeDocs(64, 3);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});

  auto opened = Open<irs::term_dict::FieldReader>(dir);
  ASSERT_EQ(1, opened.fields->size());
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);
  ASSERT_EQ(kFieldId, field->meta().id);
  ASSERT_EQ(corpus.terms.size(), field->size());
  ASSERT_EQ(corpus.terms.front(), (field->min)());
  ASSERT_EQ(corpus.terms.back(), (field->max)());
  ASSERT_EQ(corpus.docs.size(), field->docs_count());

  auto it = field->iterator(irs::SeekMode::NORMAL);
  size_t i = 0;
  while (it->next()) {
    ASSERT_LT(i, corpus.terms.size());
    ASSERT_EQ(corpus.terms[i], it->value());
    ++i;
  }
  ASSERT_EQ(corpus.terms.size(), i);
  ASSERT_FALSE(it->next());
}

TEST_F(TermDictTest, exact_seek_every_term) {
  Corpus corpus;
  corpus.terms = MakeTerms(5000, "term_with_a_long_shared_prefix_");
  corpus.docs = MakeDocs(300, 2);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  const auto expected = ReadPostings(
    *tests::FormatTestCase::Terms<std::vector<irs::bstring>::const_iterator>{
      corpus.terms.begin(), corpus.terms.end(), corpus.docs.begin(),
      corpus.docs.end()}
       .postings(corpus.features),
    true);
  ASSERT_EQ(corpus.docs.size(), expected.size());

  // A fresh single-seek iterator per term.
  for (const auto& term : corpus.terms) {
    auto it = field->iterator(irs::SeekMode::RandomOnly);
    ASSERT_TRUE(it->seek(term));
    ASSERT_EQ(term, it->value());
    ASSERT_FALSE(it->next());
    ASSERT_THROW(it->seek_ge(term), irs::NotSupported);
    const auto* meta = irs::get<irs::TermMeta>(*it);
    ASSERT_NE(nullptr, meta);
    ASSERT_EQ(corpus.docs.size(), meta->docs_count);
  }

  // One reusable iterator, forward then backward.
  {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    for (const auto& term : corpus.terms) {
      ASSERT_TRUE(it->seek(term));
      ASSERT_EQ(term, it->value());
    }
  }
  {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    for (auto term = corpus.terms.rbegin(); term != corpus.terms.rend();
         ++term) {
      ASSERT_TRUE(it->seek(*term));
      ASSERT_EQ(*term, it->value());
    }
  }

  // Postings survive the payload decode from any restart group.
  {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(it->seek(corpus.terms[corpus.terms.size() / 2]));
    auto postings = it->RowGroupPostings(corpus.features, 0);
    ASSERT_NE(nullptr, postings);
    ASSERT_EQ(expected, ReadPostings(*postings, true));
  }
}

namespace {

// jemalloc's own per-thread byte counter. Nothing inside the code under test
// can show that a seek allocates nothing; only a counter outside it can.
uint64_t ThreadAllocated() {
  uint64_t allocated = 0;
  size_t len = sizeof(allocated);
  EXPECT_EQ(0, mallctl("thread.allocated", &allocated, &len, nullptr, 0));
  return allocated;
}

}  // namespace

// The contract a point-lookup consumer relies on: one exact-seek cursor, then
// probes that cost no heap at all once its buffers have grown. A cursor per
// probe reopens the `.idx` stream and rebuilds the block cursor instead, which
// is why the PK delete filter holds one for the whole walk.
TEST_F(TermDictTest, point_seek_is_allocation_free) {
  Corpus corpus;
  corpus.terms = MakeTerms(5000, "term_with_a_long_shared_prefix_");
  corpus.docs = MakeDocs(300, 2);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  // Probe order matters: sequential probes never leave the loaded block, so
  // the block reload and the payload rewind would go unmeasured.
  std::vector<irs::bytes_view> probes{corpus.terms.begin(), corpus.terms.end()};
  std::shuffle(probes.begin(), probes.end(), std::mt19937{20260802});

  auto it = field->iterator(irs::SeekMode::RandomOnly);
  ASSERT_NE(nullptr, it);
  size_t hits = 0;
  // Two warm-up passes: the key buffer, the block buffer and the cookie's
  // row-group list all reach their final size inside the first.
  for (size_t pass = 0; pass != 2; ++pass) {
    for (const auto probe : probes) {
      hits += it->seek(probe) ? 1 : 0;
    }
  }
  ASSERT_EQ(2 * probes.size(), hits);

  const auto before = ThreadAllocated();
  for (const auto probe : probes) {
    hits += it->seek(probe) ? 1 : 0;
  }
  const auto after = ThreadAllocated();
  ASSERT_EQ(3 * probes.size(), hits);
  EXPECT_EQ(before, after) << (after - before) << " bytes over "
                           << probes.size() << " probes";
}

namespace {

// What a probe resolves to, deep enough that a batch pass that found the right
// term through the wrong block state still fails: the term, its stats and its
// whole posting list.
struct ProbeHit {
  irs::bstring term;
  uint32_t docs_count;
  uint32_t freq;
  std::vector<std::pair<irs::doc_id_t, uint32_t>> postings;

  bool operator==(const ProbeHit&) const = default;
};

void CollectHit(irs::SeekTermIterator& it, irs::IndexFeatures features,
                std::vector<ProbeHit>& out) {
  it.read();
  const auto* meta = irs::get<irs::TermMeta>(it);
  ASSERT_NE(nullptr, meta);
  auto postings = it.RowGroupPostings(features, 0);
  ASSERT_NE(nullptr, postings);
  out.emplace_back(
    irs::bstring{it.value()}, meta->docs_count, meta->freq,
    ReadPostings(*postings, irs::IndexFeatures::None !=
                              (features & irs::IndexFeatures::Freq)));
}

std::vector<ProbeHit> BatchProbe(const irs::TermReader& field,
                                 std::span<const irs::bytes_view> probes,
                                 irs::IndexFeatures features) {
  std::vector<ProbeHit> out;
  auto it = field.BatchIterator(probes);
  EXPECT_NE(nullptr, it);
  while (it->next()) {
    CollectHit(*it, features, out);
  }
  EXPECT_FALSE(it->next());
  return out;
}

std::vector<ProbeHit> LoopProbe(const irs::TermReader& field,
                                std::span<const irs::bytes_view> probes,
                                irs::IndexFeatures features) {
  std::vector<ProbeHit> out;
  auto it = field.iterator(irs::SeekMode::NORMAL);
  for (const auto probe : probes) {
    if (it->seek(probe)) {
      CollectHit(*it, features, out);
    }
  }
  return out;
}

// The oracle for every acceptor walk: the whole vocabulary, in dict order,
// filtered by testing one key at a time. It shares no code with the leapfrog --
// no block pruning, no front-coded stepping, no seek -- so a walk that skips a
// block or stops early cannot hide behind it.
std::vector<irs::bstring> FullScanAccepted(const irs::TermReader& field,
                                           const irs::TermPredicate& accepts) {
  std::vector<irs::bstring> out;
  auto it = field.iterator(irs::SeekMode::NORMAL);
  while (it->next()) {
    if (accepts.Accepts(it->value())) {
      out.emplace_back(it->value());
    }
  }
  return out;
}

irs::TermAcceptorSource::ptr WildcardSource(std::string_view pattern) {
  return irs::MakePatternSource(
    irs::bstring{irs::ViewCast<irs::byte_type>(pattern)},
    irs::PatternKind::Wildcard, irs::RegexpSyntax::Perl);
}

irs::TermAcceptorSource::ptr RegexpSource(
  std::string_view pattern,
  irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl) {
  return irs::MakePatternSource(
    irs::bstring{irs::ViewCast<irs::byte_type>(pattern)},
    irs::PatternKind::Regexp, syntax);
}

// The distance a matched key must report: computed from the two strings by the
// textbook algorithm over code points, independent of the parametric tables.
irs::byte_type ComputedDistance(std::string_view prefix, std::string_view term,
                                irs::bytes_view key) {
  const auto pfx = irs::ViewCast<irs::byte_type>(prefix);
  EXPECT_TRUE(key.starts_with(pfx));
  std::vector<uint32_t> lhs;
  std::vector<uint32_t> rhs;
  irs::utf8_utils::ToUTF32<false>(key.substr(pfx.size()),
                                  std::back_inserter(lhs));
  irs::utf8_utils::ToUTF32<false>(irs::ViewCast<irs::byte_type>(term),
                                  std::back_inserter(rhs));
  return static_cast<irs::byte_type>(
    irs::EditDistance(lhs.data(), lhs.size(), rhs.data(), rhs.size()));
}

// The same shape for a Levenshtein acceptor, whose language has no filter to
// build a predicate from.
std::vector<irs::bstring> FullScanAccepted(
  const irs::TermReader& field, const irs::LevenshteinAcceptor& acceptor) {
  std::vector<irs::bstring> out;
  auto it = field.iterator(irs::SeekMode::NORMAL);
  while (it->next()) {
    if (acceptor.Matches(it->value())) {
      out.emplace_back(it->value());
    }
  }
  return out;
}

std::vector<irs::bstring> WalkTerms(irs::SeekTermIterator::ptr it) {
  std::vector<irs::bstring> out;
  while (it->next()) {
    out.emplace_back(it->value());
  }
  return out;
}

// The acceptor oracle taken from the corpus itself rather than from a walk of
// the dictionary, so no read path of the dictionary is its own oracle.
std::vector<irs::bstring> CorpusAccepted(const std::vector<irs::bstring>& terms,
                                         const irs::TermPredicate& accepts) {
  std::vector<irs::bstring> out;
  for (const auto& term : terms) {
    if (accepts.Accepts(term)) {
      out.emplace_back(term);
    }
  }
  return out;
}

// `count` distinct keys of `width` bytes drawn from `alphabet`: the shape whose
// keys share little, so a block's stride is most of its key.
std::vector<irs::bstring> MakeWidthTerms(size_t count, size_t width,
                                         std::string_view alphabet,
                                         uint64_t seed,
                                         std::string_view lead = {}) {
  std::vector<irs::bstring> terms;
  terms.reserve(count);
  uint64_t x = seed;
  for (size_t i = 0; i != count; ++i) {
    std::string key{lead};
    for (size_t j = 0; j != width; ++j) {
      x = x * 6364136223846793005ULL + 1442695040888963407ULL;
      key += alphabet[(x >> 33) % alphabet.size()];
    }
    terms.emplace_back(MakeTerm(key));
  }
  std::ranges::sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  return terms;
}

// Every read path of a field against the term vector it was written from:
// forward walk, exact seek in both modes and both directions, `seek_ge` at each
// term and at a miss between neighbours, the sorted batch pass, and a wildcard
// walk. The oracle is always the vector, never another read path.
void AssertDictReads(const irs::TermReader& field,
                     const std::vector<irs::bstring>& terms,
                     irs::IndexFeatures features, bool partitioned = false) {
  ASSERT_EQ(terms.size(), field.size());
  ASSERT_EQ(irs::bytes_view{terms.front()}, (field.min)());
  ASSERT_EQ(irs::bytes_view{terms.back()}, (field.max)());

  {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    size_t i = 0;
    while (it->next()) {
      ASSERT_LT(i, terms.size());
      ASSERT_EQ(irs::bytes_view{terms[i]}, it->value());
      ++i;
    }
    ASSERT_EQ(terms.size(), i);
    ASSERT_FALSE(it->next());
  }
  {
    auto fwd = field.iterator(irs::SeekMode::NORMAL);
    for (const auto& term : terms) {
      ASSERT_TRUE(fwd->seek(term));
      ASSERT_EQ(irs::bytes_view{term}, fwd->value());
    }
    auto bwd = field.iterator(irs::SeekMode::NORMAL);
    for (auto term = terms.rbegin(); term != terms.rend(); ++term) {
      ASSERT_TRUE(bwd->seek(*term));
      ASSERT_EQ(irs::bytes_view{*term}, bwd->value());
    }
    for (size_t i = 0; i < terms.size(); i += 13) {
      auto one = field.iterator(irs::SeekMode::RandomOnly);
      ASSERT_TRUE(one->seek(terms[i]));
      ASSERT_FALSE(one->seek(Between(terms[i])));
    }
  }
  {
    auto it = field.iterator(irs::SeekMode::NORMAL);
    for (const auto& term : terms) {
      ASSERT_EQ(irs::SeekResult::Found, it->seek_ge(term));
      ASSERT_EQ(irs::bytes_view{term}, it->value());
    }
  }
  {
    auto shared = field.iterator(irs::SeekMode::NORMAL);
    for (size_t i = 0; i != terms.size(); ++i) {
      const auto probe = Between(terms[i]);
      if (i + 1 == terms.size()) {
        ASSERT_EQ(irs::SeekResult::End, shared->seek_ge(probe));
        break;
      }
      ASSERT_LT(irs::bytes_view{probe}, irs::bytes_view{terms[i + 1]});
      ASSERT_EQ(irs::SeekResult::NotFound, shared->seek_ge(probe));
      ASSERT_EQ(irs::bytes_view{terms[i + 1]}, shared->value());
      if (i % 29 == 0) {
        // A cold iterator takes the block search rather than the resume path.
        auto cold = field.iterator(irs::SeekMode::NORMAL);
        ASSERT_EQ(irs::SeekResult::NotFound, cold->seek_ge(probe));
        ASSERT_EQ(irs::bytes_view{terms[i + 1]}, cold->value());
      }
    }
  }
  {
    std::vector<irs::bytes_view> probes;
    for (size_t i = 0; i < terms.size(); i += 3) {
      probes.emplace_back(terms[i]);
      probes.emplace_back(terms[i + 1 < terms.size() ? i + 1 : i]);
    }
    probes.erase(std::unique(probes.begin(), probes.end()), probes.end());
    if (partitioned) {
      // A partitioned field has no whole-term posting list, so the batch pass
      // is compared on the terms it settles on.
      std::vector<irs::bstring> expected;
      auto loop = field.iterator(irs::SeekMode::NORMAL);
      for (const auto probe : probes) {
        if (loop->seek(probe)) {
          expected.emplace_back(loop->value());
        }
      }
      ASSERT_EQ(probes.size(), expected.size());
      ASSERT_EQ(expected, WalkTerms(field.BatchIterator(probes)));
    } else {
      const auto expected = LoopProbe(field, probes, features);
      ASSERT_EQ(probes.size(), expected.size());
      ASSERT_EQ(expected, BatchProbe(field, probes, features));
    }
  }
  for (std::string_view pattern : {"%", "a%", "%a", "%a%", "_%", "%__"}) {
    const auto source = WildcardSource(pattern);
    ASSERT_EQ(CorpusAccepted(terms, *source->Predicate()),
              WalkTerms(source->Iterator(field)))
      << "pattern: " << pattern;
  }
}

}  // namespace

// The batch pass must answer exactly what a loop of seeks answers -- which is
// what the `TermReader::BatchIterator` base-class default literally is, so the
// loop is an oracle that shares no code with the forward pass under test.
TEST_F(TermDictTest, batch_seek_matches_loop_seek) {
  Corpus corpus;
  corpus.terms = MakeTerms(5000, "term_with_a_long_shared_prefix_");
  corpus.terms.emplace_back(MakeTerm("zzz_after_everything"));
  std::ranges::sort(corpus.terms);
  corpus.docs = MakeDocs(300, 2);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory new_dir;
  WriteV2(new_dir, corpus, {});
  auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);
  const auto* new_field = new_opened.Field();
  ASSERT_NE(nullptr, new_field);

  // Probe sets, each sorted and unique as the contract requires: every term;
  // a sparse subset that crosses blocks between probes; hits interleaved with
  // misses that sit between two neighbouring terms (the case that leaves the
  // cursor above the next probe); all misses; the out-of-range ends; the
  // degenerate sizes.
  std::vector<std::vector<irs::bstring>> sets;
  sets.emplace_back(corpus.terms);
  {
    std::vector<irs::bstring> sparse;
    for (size_t i = 0; i < corpus.terms.size(); i += 37) {
      sparse.emplace_back(corpus.terms[i]);
    }
    sets.emplace_back(std::move(sparse));
  }
  {
    std::vector<irs::bstring> mixed;
    for (size_t i = 0; i < corpus.terms.size(); i += 3) {
      mixed.emplace_back(corpus.terms[i]);
      mixed.emplace_back(Between(corpus.terms[i]));
    }
    std::ranges::sort(mixed);
    sets.emplace_back(std::move(mixed));
  }
  {
    std::vector<irs::bstring> misses;
    for (size_t i = 0; i < corpus.terms.size(); i += 11) {
      misses.emplace_back(Between(corpus.terms[i]));
    }
    sets.emplace_back(std::move(misses));
  }
  sets.emplace_back(std::vector<irs::bstring>{
    MakeTerm("aaa_before_everything"), corpus.terms.front(),
    corpus.terms.back(), MakeTerm("zzzz_after_everything")});
  sets.emplace_back(std::vector<irs::bstring>{corpus.terms[17]});
  sets.emplace_back(std::vector<irs::bstring>{MakeTerm("nothing_here")});
  sets.emplace_back(std::vector<irs::bstring>{});

  for (const auto& set : sets) {
    std::vector<irs::bytes_view> probes;
    probes.reserve(set.size());
    for (const auto& term : set) {
      probes.emplace_back(term);
    }

    const auto expected = LoopProbe(*new_field, probes, corpus.features);
    ASSERT_EQ(expected, BatchProbe(*new_field, probes, corpus.features));
  }

  // A dictless field has one block and no separators, so the batch pass takes
  // a different branch of the block search.
  {
    Corpus tiny;
    tiny.terms.emplace_back(MakeTerm("false"));
    tiny.terms.emplace_back(MakeTerm("true"));
    tiny.docs = MakeDocs(8, 1);
    tiny.features = irs::IndexFeatures::Freq;

    irs::MemoryDirectory tiny_new;
    WriteV2(tiny_new, tiny, {});
    auto tiny_new_opened = Open<irs::term_dict::FieldReader>(tiny_new);

    const auto miss = MakeTerm("nope");
    const std::vector<irs::bytes_view> probes{irs::bytes_view{tiny.terms[0]},
                                              irs::bytes_view{miss},
                                              irs::bytes_view{tiny.terms[1]}};
    const auto expected =
      LoopProbe(*tiny_new_opened.Field(), probes, tiny.features);
    ASSERT_EQ(2, expected.size());
    ASSERT_EQ(expected,
              BatchProbe(*tiny_new_opened.Field(), probes, tiny.features));
  }
}

TEST_F(TermDictTest, seek_missing) {
  Corpus corpus;
  corpus.terms = MakeTerms(500, "abc_");
  corpus.docs = MakeDocs(8, 1);

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();

  for (auto mode : {irs::SeekMode::NORMAL, irs::SeekMode::RandomOnly}) {
    auto it = field->iterator(mode);
    ASSERT_FALSE(it->seek(MakeTerm("")));
    ASSERT_FALSE(it->seek(MakeTerm("aaa")));
    ASSERT_FALSE(it->seek(MakeTerm("zzz")));
    ASSERT_FALSE(it->seek(Between(corpus.terms.front())));
  }
}

TEST_F(TermDictTest, seek_ge) {
  Corpus corpus;
  corpus.terms = MakeTerms(3000, "seek_ge_prefix_");
  corpus.docs = MakeDocs(4, 1);

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();

  {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_EQ(irs::SeekResult::NotFound, it->seek_ge(MakeTerm("a")));
    ASSERT_EQ(corpus.terms.front(), it->value());
  }
  {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_EQ(irs::SeekResult::End, it->seek_ge(MakeTerm("zzzz")));
  }
  for (size_t i = 0; i + 1 < corpus.terms.size(); i += 37) {
    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_EQ(irs::SeekResult::Found, it->seek_ge(corpus.terms[i]));
    ASSERT_EQ(corpus.terms[i], it->value());
    // A probe strictly between term i and term i+1 lands on term i+1.
    ASSERT_EQ(irs::SeekResult::NotFound, it->seek_ge(Between(corpus.terms[i])));
    ASSERT_EQ(corpus.terms[i + 1], it->value());
    // ... and iteration continues from there.
    if (i + 2 < corpus.terms.size()) {
      ASSERT_TRUE(it->next());
      ASSERT_EQ(corpus.terms[i + 2], it->value());
    }
  }
}

TEST_F(TermDictTest, dictless_field) {
  Corpus corpus;
  corpus.terms = {MakeTerm("false"), MakeTerm("true")};
  corpus.docs = MakeDocs(16, 1);

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);
  ASSERT_EQ(2, field->size());

  auto it = field->iterator(irs::SeekMode::NORMAL);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(corpus.terms[0], it->value());
  ASSERT_TRUE(it->next());
  ASSERT_EQ(corpus.terms[1], it->value());
  ASSERT_FALSE(it->next());

  auto single = field->iterator(irs::SeekMode::RandomOnly);
  ASSERT_TRUE(single->seek(corpus.terms[1]));
  ASSERT_FALSE(single->seek(MakeTerm("maybe")));
}

TEST_F(TermDictTest, empty_field) {
  Corpus corpus;
  corpus.docs = MakeDocs(4, 1);

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  ASSERT_EQ(0, opened.fields->size());
  ASSERT_EQ(nullptr, opened.Field());
}

TEST_F(TermDictTest, tiny_blocks_and_no_fsst) {
  Corpus corpus;
  corpus.terms = MakeTerms(300, "tiny_");
  corpus.docs = MakeDocs(200, 1);
  corpus.features = irs::IndexFeatures::Freq;

  irs::term_dict::WriterOptions options;
  options.block_byte_target = 48;
  options.restart_interval = 2;
  options.fsst_enabled = false;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, options);
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  auto it = field->iterator(irs::SeekMode::NORMAL);
  size_t i = 0;
  while (it->next()) {
    ASSERT_EQ(corpus.terms[i], it->value());
    ++i;
  }
  ASSERT_EQ(corpus.terms.size(), i);

  for (const auto& term : corpus.terms) {
    auto seek = field->iterator(irs::SeekMode::RandomOnly);
    ASSERT_TRUE(seek->seek(term));
  }
}

// Everything the dictionary reports about a field, against the corpus it was
// handed rather than against another index: term set and order, `docs_count`,
// min/max, per-term meta, the doc/freq streams and `read_documents`. Positions
// and offsets belong to `row_group_partitioned_postings`: the shared `Terms`
// mock this corpus goes through hardcodes `IndexFeatures::None` on its
// postings, so no position is ever handed to the writer here whatever the
// field's declared features say.
TEST_F(TermDictTest, postings_match_corpus) {
  for (auto features : {irs::IndexFeatures::None, irs::IndexFeatures::Freq,
                        irs::IndexFeatures::Freq | irs::IndexFeatures::Pos,
                        irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                          irs::IndexFeatures::Offs}) {
    Corpus corpus;
    corpus.terms = MakeTerms(4000, "differential_");
    corpus.docs = MakeDocs(500, 3);
    corpus.features = features;
    corpus.doc_count = 2048;

    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, {});

    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);

    const bool with_freq =
      irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);

    ASSERT_EQ(corpus.terms.size(), field->size());
    ASSERT_EQ(corpus.docs.size(), field->docs_count());
    ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, (field->min)());
    ASSERT_EQ(irs::bytes_view{corpus.terms.back()}, (field->max)());

    uint32_t expected_freq = 0;
    std::vector<std::pair<irs::doc_id_t, uint32_t>> expected_docs;
    expected_docs.reserve(corpus.docs.size());
    for (const auto [doc, freq] : corpus.docs) {
      expected_freq += freq;
      expected_docs.emplace_back(doc, with_freq ? freq : 0);
    }

    auto it = field->iterator(irs::SeekMode::NORMAL);
    const auto* meta = irs::get<irs::TermMeta>(*it);
    ASSERT_NE(nullptr, meta);
    size_t count = 0;
    while (it->next()) {
      ASSERT_EQ(irs::bytes_view{corpus.terms[count]}, it->value());
      it->read();
      ASSERT_EQ(corpus.docs.size(), meta->docs_count);
      ASSERT_EQ(with_freq ? expected_freq : 0, meta->freq);

      auto postings = it->RowGroupPostings(features, 0);
      ASSERT_NE(nullptr, postings);
      ASSERT_EQ(expected_docs, ReadPostings(*postings, with_freq));
      ++count;
    }
    ASSERT_EQ(corpus.terms.size(), count);

    for (size_t i = 0; i < corpus.terms.size(); i += 101) {
      std::vector<irs::doc_id_t> docs;
      tests::ReadDocuments(*field, corpus.terms[i], [&](irs::doc_id_t d) {
        docs.push_back(d);
        return true;
      });
      ASSERT_EQ(corpus.docs.size(), docs.size());
      for (size_t d = 0; d != docs.size(); ++d) {
        ASSERT_EQ(corpus.docs[d].first, docs[d]);
      }
      ASSERT_EQ(corpus.docs.size(),
                tests::TermStats(*field, corpus.terms[i]).docs_count);
    }
  }
}

TEST_F(TermDictTest, automaton_matches_full_scan) {
  Corpus corpus;
  corpus.terms.emplace_back(MakeTerm("alpha"));
  corpus.terms.emplace_back(MakeTerm("alphabet"));
  corpus.terms.emplace_back(MakeTerm("alphabetical"));
  corpus.terms.emplace_back(MakeTerm("beta"));
  corpus.terms.emplace_back(MakeTerm("betamax"));
  for (const auto& term : MakeTerms(3000, "gamma_")) {
    corpus.terms.emplace_back(term);
  }
  for (const auto& term : MakeTerms(3000, "delta_")) {
    corpus.terms.emplace_back(term);
  }
  std::ranges::sort(corpus.terms);
  corpus.docs = MakeDocs(32, 1);
  corpus.features = irs::IndexFeatures::Freq;

  // Restart density changes which entries carry a forced lcp of 0, which is
  // exactly what the leapfrog's front-coding deductions hinge on.
  for (uint32_t interval : {2u, 4u, 8u, 16u, 32u}) {
    irs::term_dict::WriterOptions options;
    options.restart_interval = interval;
    irs::MemoryDirectory new_dir;
    WriteV2(new_dir, corpus, options);
    auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);
    const auto* new_field = new_opened.Field();

    for (std::string_view pattern :
         {"alpha%", "%beta%", "gamma_0000%", "delta_00000_1", "%max", "a_pha",
          "%", "z%", "%_0000000_", "gamma%9", "_lpha%", "delta_%_5", "aa%",
          "gamma_00000%0", "%a", "betamax"}) {
      const auto source = WildcardSource(pattern);
      const auto expected = FullScanAccepted(*new_field, *source->Predicate());
      const auto actual = WalkTerms(source->Iterator(*new_field));
      ASSERT_EQ(expected.size(), actual.size())
        << "pattern: " << pattern << " interval: " << interval;
      ASSERT_EQ(expected, actual)
        << "pattern: " << pattern << " interval: " << interval;
    }

    // A Levenshtein acceptor drives the leapfrog differently than a wildcard
    // one: its live alphabet is the target's own code points rather than a byte
    // range. The accepted set is gated by the full scan and the reported edit
    // distance against `EditDistance`, which computes it from the two strings
    // directly.
    auto collect_lev = [](irs::SeekTermIterator::ptr it) {
      std::vector<std::pair<irs::bstring, irs::byte_type>> out;
      const auto* payload = irs::get<irs::PayAttr>(*it);
      EXPECT_NE(nullptr, payload);
      while (it->next()) {
        EXPECT_FALSE(payload->value.empty());
        out.emplace_back(it->value(), payload->value.front());
      }
      return out;
    };

    for (std::string_view prefix : {"", "gamma_"}) {
      for (std::string_view term :
           {"alpha", "betamax", "gamma_00001234", "zzz", "00001234", "0000"}) {
        for (irs::byte_type distance : {1, 2}) {
          const auto description =
            irs::MakeParametricDescription(distance, false);
          const auto pfx = irs::ViewCast<irs::byte_type>(prefix);
          const auto tgt = irs::ViewCast<irs::byte_type>(term);
          const irs::LevenshteinAcceptor parametric{description, pfx, tgt};

          const auto scanned = FullScanAccepted(*new_field, parametric);
          auto direct_it = new_field->iterator(parametric);
          ASSERT_NE(nullptr, direct_it);
          const auto direct = collect_lev(std::move(direct_it));

          std::vector<irs::bstring> walked;
          walked.reserve(direct.size());
          for (const auto& [key, dist] : direct) {
            walked.emplace_back(key);
            ASSERT_EQ(ComputedDistance(prefix, term, key), dist)
              << "prefix: " << prefix << " term: " << term
              << " distance: " << int{distance} << " interval: " << interval;
          }
          ASSERT_EQ(scanned, walked)
            << "prefix: " << prefix << " term: " << term
            << " distance: " << int{distance} << " interval: " << interval;
        }
      }
    }

    // seek_ge on an acceptor iterator must land on the first accepted term at
    // or after the probe, for probes that are terms, gaps and out-of-range --
    // an empty probe included, which every term satisfies. The full scan says
    // which term that is. (The leapfrog answers the empty probe differently,
    // with the `irs::seek`-driven form's `Found` on an empty value; that is
    // pinned where it lives, in `regexp_matches_full_scan`.)
    {
      const auto source = WildcardSource("gamma%");
      const auto accepted = FullScanAccepted(*new_field, *source->Predicate());
      for (std::string_view probe : {"", "alpha", "gamma_00000000",
                                     "gamma_00001500", "gamma_9", "zzz"}) {
        auto new_it = source->Iterator(*new_field);
        const auto target = irs::ViewCast<irs::byte_type>(probe);
        const auto actual = new_it->seek_ge(target);
        const auto first =
          std::ranges::lower_bound(accepted, irs::bstring{target});
        if (first == accepted.end()) {
          ASSERT_EQ(irs::SeekResult::End, actual) << "probe: " << probe;
          continue;
        }
        ASSERT_NE(irs::SeekResult::End, actual) << "probe: " << probe;
        ASSERT_EQ(*first == irs::bstring{target} ? irs::SeekResult::Found
                                                 : irs::SeekResult::NotFound,
                  actual)
          << "probe: " << probe;
        ASSERT_EQ(irs::bytes_view{*first}, new_it->value())
          << "probe: " << probe;
      }
    }
  }
}

// The prefix-range walk replaces stepping the automaton with a residual check,
// so it stands or falls on the bytes the automaton's UTF-8 model rejects: a
// dictionary key is arbitrary bytes, and `%` matches a code point sequence,
// not a byte sequence. This corpus is the set of ways that distinction shows.
// Bytes a lead byte claims under the model `%` and `_` walk -- the loose one,
// which is what `MakeAny` builds and what the RE2 wildcard dialect spells out:
// lead ranges with unconstrained continuations, so overlongs and surrogates are
// admitted and `C0`/`C1`/`F5`..`FF` are not. Zero when the key does not start
// with a code point the model takes.
size_t LooseCodePointLength(irs::bytes_view key) noexcept {
  if (key.empty()) {
    return 0;
  }
  const uint32_t lead = key.front();
  size_t extra = 0;
  if (lead <= 0x7F) {
    return 1;
  } else if (lead >= 0xC2 && lead <= 0xDF) {
    extra = 1;
  } else if (lead >= 0xE0 && lead <= 0xEF) {
    extra = 2;
  } else if (lead >= 0xF0 && lead <= 0xF4) {
    extra = 3;
  } else {
    return 0;
  }
  if (key.size() <= extra) {
    return 0;
  }
  for (size_t i = 1; i <= extra; ++i) {
    if (key[i] < 0x80 || key[i] > 0xBF) {
      return 0;
    }
  }
  return extra + 1;
}

// The wildcard language, computed directly from the pattern: the oracle both
// backends are held to.
bool LooseWildcardMatch(irs::bytes_view pattern, irs::bytes_view term) {
  if (pattern.empty()) {
    return term.empty();
  }
  switch (pattern.front()) {
    case '\\':
      if (pattern.size() == 1) {
        // A trailing escape has nothing to escape and is dropped.
        return term.empty();
      }
      return !term.empty() && term.front() == pattern[1] &&
             LooseWildcardMatch(pattern.substr(2), term.substr(1));
    case '%': {
      if (LooseWildcardMatch(pattern.substr(1), term)) {
        return true;
      }
      const auto length = LooseCodePointLength(term);
      return length != 0 && LooseWildcardMatch(pattern, term.substr(length));
    }
    case '_': {
      const auto length = LooseCodePointLength(term);
      return length != 0 &&
             LooseWildcardMatch(pattern.substr(1), term.substr(length));
    }
    default:
      return !term.empty() && term.front() == pattern.front() &&
             LooseWildcardMatch(pattern.substr(1), term.substr(1));
  }
}

// The wildcard driver against that oracle, over a corpus of ill-formed keys.
// This is the statement that the RE2 wildcard dialect -- byte classes in
// Latin-1, so a literal byte is itself and a pattern of arbitrary bytes is
// expressible at all -- decides exactly the model's language, which is the one
// the compiled automaton used to decide.
TEST_F(TermDictTest, wildcard_parity_non_utf8) {
  static constexpr std::string_view kTails[] = {
    "",
    "z",
    "zz",
    "\xc3\xa9",
    "\xe2\x82\xac",
    "\xf0\x9f\x98\x80",
    "\xc3\xa9z",
    "\x80",
    "\xbf",
    "\xc0\x80",
    "\xc1\xbf",
    "\xc3",
    "\xe2\x82",
    "\xe2\x82\x41",
    "\xf0\x9f\x98",
    "\xf5\x80\x80\x80",
    "\xff",
    "\xfe\x80",
    "z\x80z",
    "\xc3\xa9\xf0\x9f\x98\x80",
    "\xed\xa0\x80",
    "\xe0\x80\x80",
    "\xf4\x90\x80\x80",
  };

  Corpus corpus;
  for (std::string_view prefix : {"alpha", "alphabet", "beta"}) {
    for (const auto tail : kTails) {
      corpus.terms.emplace_back(MakeTerm(absl::StrCat(prefix, tail)));
    }
  }
  std::ranges::sort(corpus.terms);
  corpus.terms.erase(std::unique(corpus.terms.begin(), corpus.terms.end()),
                     corpus.terms.end());
  corpus.docs = MakeDocs(8, 1);
  corpus.features = irs::IndexFeatures::Freq;

  size_t nonempty = 0;
  for (uint32_t interval : {2u, 16u}) {
    irs::term_dict::WriterOptions options;
    options.restart_interval = interval;
    irs::MemoryDirectory new_dir;
    WriteV2(new_dir, corpus, options);
    auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);
    const auto* new_field = new_opened.Field();
    ASSERT_NE(nullptr, new_field);

    for (std::string_view pattern :
         {"%", "a%", "alpha%", "alphabet%", "beta%", "b%", "%z", "alpha_",
          "alpha%z", "zzz%", "alpha__", "%_", "alpha%_z", "al_ha%",
          // The head-and-tail shape and its boundaries: a tail that cannot
          // overlap the head, a one-byte pair, a tail that is a bare lead
          // byte, and an escaped `%` that belongs to the head.
          "alpha%alpha", "a%a", "alpha%\xc3", "alpha\\%%z", "alpha%zz"}) {
      const auto bytes = irs::ViewCast<irs::byte_type>(pattern);

      std::vector<irs::bstring> expected;
      for (const auto& term : corpus.terms) {
        if (LooseWildcardMatch(bytes, term)) {
          expected.emplace_back(term);
        }
      }

      const irs::RegexpAcceptor re2{irs::RegexpAcceptor::WildcardTag{}, bytes};
      ASSERT_TRUE(re2.ok()) << "pattern: " << pattern;

      ASSERT_EQ(expected, WalkTerms(new_field->iterator(re2)))
        << "re2, pattern: " << pattern << " interval: " << interval;
      // And the same through the source the filters use, whose prefix shortcut
      // answers the `literal%` shapes without a walk at all.
      const auto source = WildcardSource(pattern);
      ASSERT_EQ(expected, WalkTerms(source->Iterator(*new_field)))
        << "source, pattern: " << pattern << " interval: " << interval;
      ASSERT_EQ(expected, FullScanAccepted(*new_field, *source->Predicate()))
        << "predicate, pattern: " << pattern << " interval: " << interval;
      nonempty += !expected.empty();
    }
  }

  // A gate that would otherwise pass by matching nothing everywhere.
  ASSERT_GT(nonempty, 20);
}

TEST_F(TermDictTest, levenshtein_parity_utf8) {
  static constexpr std::string_view kParts[] = {
    "\xd0\xbf\xd1\x80\xd0\xb8",  // при
    "\xd0\xb2\xd0\xb5\xd1\x82",  // вет
    "\xe6\x97\xa5\xe6\x9c\xac",  // 日本
    "\xe8\xaa\x9e",              // 語
    "\xf0\x9f\x99\x82",          // U+1F642
    "\xf0\x9f\x99\x83",          // U+1F643
    "ab",
    "\xc3\xa9",  // é
  };

  Corpus corpus;
  for (size_t i = 0; i != 600; ++i) {
    std::string term;
    uint64_t x = i * 2654435761ULL + 7;
    for (size_t j = 0; j != 3; ++j) {
      x = x * 6364136223846793005ULL + 1442695040888963407ULL;
      term += kParts[(x >> 33) % std::size(kParts)];
    }
    corpus.terms.emplace_back(MakeTerm(term));
  }
  std::ranges::sort(corpus.terms);
  corpus.terms.erase(std::unique(corpus.terms.begin(), corpus.terms.end()),
                     corpus.terms.end());
  corpus.docs = MakeDocs(8, 1);
  corpus.features = irs::IndexFeatures::Freq;

  for (uint32_t interval : {2u, 16u}) {
    irs::term_dict::WriterOptions options;
    options.restart_interval = interval;
    irs::MemoryDirectory new_dir;
    WriteV2(new_dir, corpus, options);
    auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);
    const auto* new_field = new_opened.Field();

    auto collect = [](irs::SeekTermIterator::ptr it) {
      std::vector<std::pair<irs::bstring, irs::byte_type>> out;
      const auto* payload = irs::get<irs::PayAttr>(*it);
      EXPECT_NE(nullptr, payload);
      while (it->next()) {
        EXPECT_FALSE(payload->value.empty());
        out.emplace_back(it->value(), payload->value.front());
      }
      return out;
    };

    for (std::string_view prefix : {"", "\xd0\xbf\xd1\x80\xd0\xb8"}) {
      for (std::string_view term :
           {"\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82",
            "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e",
            "\xf0\x9f\x99\x82\xf0\x9f\x99\x83", "ab\xc3\xa9",
            "\xd0\xb2\xd0\xb5\xd1\x82"
            "ab"}) {
        for (irs::byte_type distance : {1, 2}) {
          const auto description =
            irs::MakeParametricDescription(distance, false);
          const auto pfx = irs::ViewCast<irs::byte_type>(prefix);
          const auto tgt = irs::ViewCast<irs::byte_type>(term);
          const irs::LevenshteinAcceptor parametric{description, pfx, tgt};

          const auto scanned = FullScanAccepted(*new_field, parametric);
          auto direct_it = new_field->iterator(parametric);
          ASSERT_NE(nullptr, direct_it);
          const auto direct = collect(std::move(direct_it));

          std::vector<irs::bstring> walked;
          walked.reserve(direct.size());
          for (const auto& [key, dist] : direct) {
            walked.emplace_back(key);
            ASSERT_EQ(ComputedDistance(prefix, term, key), dist)
              << "prefix bytes: " << prefix.size()
              << " term bytes: " << term.size()
              << " distance: " << int{distance};
          }
          ASSERT_FALSE(scanned.empty())
            << "prefix bytes: " << prefix.size()
            << " term bytes: " << term.size() << " distance: " << int{distance};
          ASSERT_EQ(scanned, walked)
            << "prefix bytes: " << prefix.size()
            << " term bytes: " << term.size() << " distance: " << int{distance}
            << " interval: " << interval;
        }
      }
    }
  }
}

// What RE2's own engine matches whole, over the same corpus: the oracle for the
// backend, sharing neither its walk nor its tree rewrite.
std::vector<irs::bstring> NativeMatches(
  std::string_view pattern, const std::vector<irs::bstring>& terms) {
  RE2 native{std::string{pattern}};
  EXPECT_TRUE(native.ok()) << pattern;
  std::vector<irs::bstring> out;
  for (const auto& term : terms) {
    const std::string key{reinterpret_cast<const char*>(term.data()),
                          term.size()};
    if (RE2::FullMatch(key, native)) {
      out.emplace_back(term);
    }
  }
  return out;
}

// Three ways of deciding the same language: `RE2::FullMatch` whole-term over
// the corpus (the oracle), the leapfrog over RE2's lazy DFA, and the source the
// filters build -- walked, and applied one key at a time.
//
// The corpus is ASCII and valid UTF-8 on purpose -- see
// `regexp_non_utf8_divergence` for where the two backends genuinely part ways.
TEST_F(TermDictTest, regexp_matches_full_scan) {
  Corpus corpus;
  for (std::string_view word :
       {"alpha", "alphabet", "alphabetical", "beta", "betamax", "gamma",
        "Gamma", "delta1", "delta22", "delta333", "x", "xy", "xyz"}) {
    corpus.terms.emplace_back(MakeTerm(word));
  }
  for (const auto& term : MakeTerms(2000, "gamma_")) {
    corpus.terms.emplace_back(term);
  }
  for (const auto& term : MakeTerms(2000, "delta_")) {
    corpus.terms.emplace_back(term);
  }
  for (std::string_view part :
       {"\xd0\xbf\xd1\x80\xd0\xb8", "\xe6\x97\xa5", "\xf0\x9f\x99\x82"}) {
    corpus.terms.emplace_back(MakeTerm(std::string{part} + "tail"));
    corpus.terms.emplace_back(MakeTerm("head" + std::string{part}));
  }
  std::ranges::sort(corpus.terms);
  corpus.terms.erase(std::unique(corpus.terms.begin(), corpus.terms.end()),
                     corpus.terms.end());
  corpus.docs = MakeDocs(16, 1);
  corpus.features = irs::IndexFeatures::Freq;

  static constexpr std::string_view kPatterns[] = {
    "alpha.*",
    "alphabet",
    ".*beta.*",
    "gamma_0000.*",
    "delta_00000.1",
    ".*max",
    "a.pha",
    ".*",
    "z.*",
    "gamma.*9",
    "[gd]....?_00000.0",
    "delta[0-9]+",
    "delta[0-9]{2,3}",
    "(alpha|beta)(bet|max)?",
    "x(y(z)?)?",
    "gamma_(0|1)+",
    "[[:alpha:]]+",
    "\\w+",
    "\\d+",
    "[^a-z]+.*",
    "^alpha.*$",
    "\\balpha\\b",
    "(?i:gamma)",
    "\\p{Cyrillic}+tail",
    "head\\p{Han}",
    ".{4}",
    "delta_[0-9]*5",
    "nomatchatall.*",
  };

  auto collect = [](irs::SeekTermIterator::ptr it) {
    std::vector<irs::bstring> out;
    if (!it) {
      return out;
    }
    while (it->next()) {
      out.emplace_back(it->value());
    }
    return out;
  };

  size_t nonempty = 0;
  for (uint32_t interval : {2u, 16u}) {
    irs::term_dict::WriterOptions options;
    options.restart_interval = interval;
    irs::MemoryDirectory new_dir;
    WriteV2(new_dir, corpus, options);
    auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);
    const auto* new_field = new_opened.Field();

    for (const auto pattern : kPatterns) {
      const auto bytes = irs::ViewCast<irs::byte_type>(pattern);
      const irs::RegexpAcceptor re2{bytes};
      ASSERT_TRUE(re2.ok()) << "pattern: " << pattern;

      // RE2's own engine, run whole-term over the same corpus: an oracle that
      // shares neither the walk nor the tree rewrite.
      const auto expected = NativeMatches(pattern, corpus.terms);
      auto re2_it = new_field->iterator(re2);
      ASSERT_NE(nullptr, re2_it) << "pattern: " << pattern;
      const auto v2_re2 = collect(std::move(re2_it));

      ASSERT_EQ(expected, v2_re2)
        << "re2, pattern: " << pattern << " interval: " << interval;
      // And through the source the filters build, which walks the same acceptor
      // and must also answer it one key at a time.
      const auto source = RegexpSource(pattern);
      ASSERT_EQ(expected, collect(source->Iterator(*new_field)))
        << "source, pattern: " << pattern << " interval: " << interval;
      ASSERT_EQ(expected, FullScanAccepted(*new_field, *source->Predicate()))
        << "predicate, pattern: " << pattern << " interval: " << interval;
      nonempty += !expected.empty();
    }

    // The empty-width operators the tree rewrite reinterprets: RE2's own engine
    // is not the oracle for the pattern itself, so each is paired with the
    // pattern its whole-term reading is equivalent to, which RE2 does answer.
    static constexpr std::pair<std::string_view, std::string_view> kRewritten[]{
      {"^alpha.*$", "alpha.*"},
      {"\\balpha\\b", "alpha"},
      {"al$pha", "alpha"},
      {"alpha\\bbet", "alphabet"}};

    for (const auto& [pattern, equivalent] : kRewritten) {
      const auto bytes = irs::ViewCast<irs::byte_type>(pattern);
      const irs::RegexpAcceptor re2{bytes};
      ASSERT_TRUE(re2.ok()) << "pattern: " << pattern;
      const auto expected = NativeMatches(equivalent, corpus.terms);
      ASSERT_FALSE(expected.empty()) << "pattern: " << pattern;
      ASSERT_EQ(expected, collect(new_field->iterator(re2)))
        << "rewritten, pattern: " << pattern << " interval: " << interval;
    }

    // ... and that the rewrite is what makes those agree: RE2's own engine
    // reads a mid-pattern `$` and an interior `\b` as unsatisfiable, so a
    // backend that handed the parsed tree to RE2 unchanged would match nothing.
    for (std::string_view pattern : {"al$pha", "alpha\\bbet"}) {
      ASSERT_TRUE(NativeMatches(pattern, corpus.terms).empty())
        << "pattern: " << pattern;
      const irs::RegexpAcceptor re2{irs::ViewCast<irs::byte_type>(pattern)};
      ASSERT_FALSE(collect(new_field->iterator(re2)).empty())
        << "pattern: " << pattern;
    }

    // The same iterator must also answer `seek_ge` the way the leapfrog does.
    {
      const auto bytes =
        irs::ViewCast<irs::byte_type>(std::string_view{"gamma_.*"});
      const irs::RegexpAcceptor re2{bytes};
      const auto accepted = NativeMatches("gamma_.*", corpus.terms);
      for (std::string_view probe : {"", "alpha", "gamma_00000000",
                                     "gamma_00001500", "gamma_9", "zzz"}) {
        auto new_it = new_field->iterator(re2);
        const auto target = irs::ViewCast<irs::byte_type>(probe);
        const auto got = new_it->seek_ge(target);
        const auto first =
          std::ranges::lower_bound(accepted, irs::bstring{target});
        if (first == accepted.end()) {
          ASSERT_EQ(irs::SeekResult::End, got) << "probe: " << probe;
          continue;
        }
        // See the note on the empty probe in `automaton_matches_full_scan`.
        if (target.empty()) {
          ASSERT_EQ(irs::SeekResult::Found, got);
          ASSERT_TRUE(new_it->value().empty());
          continue;
        }
        ASSERT_EQ(*first == irs::bstring{target} ? irs::SeekResult::Found
                                                 : irs::SeekResult::NotFound,
                  got)
          << "probe: " << probe;
        ASSERT_EQ(irs::bytes_view{*first}, new_it->value())
          << "probe: " << probe;
      }
    }
  }

  // A gate that would otherwise pass by matching nothing everywhere.
  ASSERT_GT(nonempty, std::size(kPatterns));

  // A pattern RE2 cannot parse accepts nothing.
  {
    const irs::RegexpAcceptor bad{
      irs::ViewCast<irs::byte_type>(std::string_view{"a(b"})};
    ASSERT_FALSE(bad.ok());
    irs::MemoryDirectory new_dir;
    WriteV2(new_dir, corpus, {});
    auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);
    auto it = new_opened.Field()->iterator(bad);
    ASSERT_NE(nullptr, it);
    ASSERT_FALSE(it->next());
  }
}

// Which path a walk takes over a leaf block: one pass over the block's stored
// bytes when the state its shared prefix reaches self-loops on every one of
// them, otherwise a step per byte of every key. The counts that say so are
// dev-build counters.
#ifdef SDB_DEV
TEST_F(TermDictTest, automaton_decides_whole_blocks) {
  Corpus corpus;
  corpus.terms = MakeWordyTerms(20000);
  corpus.docs = MakeDocs(16, 1);
  corpus.features = irs::IndexFeatures::Freq;

  struct Walk {
    std::vector<irs::bstring> matched;
    size_t decided;
    size_t walked;
  };

  for (const bool fsst : {true, false}) {
    irs::term_dict::WriterOptions options;
    options.fsst_enabled = fsst;
    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, options);
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    // Every block, the first one included, is settled in one pass or not at
    // all: the field minimum bounds block 0 the way a separator bounds the
    // rest.
    const size_t blocks = field->StorageInfo(/*walk=*/false).block_count;
    ASSERT_LT(1, blocks);

    auto walk = [&](std::string_view pattern) {
      const irs::RegexpAcceptor re2{irs::ViewCast<irs::byte_type>(pattern)};
      EXPECT_TRUE(re2.ok()) << pattern;
      const auto before = irs::term_dict::AutomatonBlockCounts();
      Walk out;
      auto it = field->iterator(re2);
      EXPECT_NE(nullptr, it) << pattern;
      while (it->next()) {
        out.matched.emplace_back(it->value());
      }
      const auto after = irs::term_dict::AutomatonBlockCounts();
      out.decided = after.decided - before.decided;
      out.walked = after.walked - before.walked;
      return out;
    };

    // A literal prefix leaves the automaton in one accepting state that every
    // byte of the corpus self-loops on, so no key of any block is stepped.
    const auto prefix = walk("document_field_.*");
    EXPECT_EQ(0, prefix.walked) << "fsst: " << fsst;
    EXPECT_EQ(blocks, prefix.decided) << "fsst: " << fsst;
    EXPECT_EQ(corpus.terms, prefix.matched) << "fsst: " << fsst;

    // The same prefix with a tail: the residual automaton moves on a byte the
    // corpus holds, so blocks fall back to the per-key walk and answer the
    // same language.
    const auto tail = walk("document_field_.*ka");
    EXPECT_LT(0, tail.walked) << "fsst: " << fsst;
    EXPECT_EQ(NativeMatches("document_field_.*ka", corpus.terms), tail.matched)
      << "fsst: " << fsst;

    // A literal the corpus does not hold anywhere: the state does not accept,
    // and every block is rejected whole rather than walked.
    const auto absent = walk("document_field_.*qq.*");
    EXPECT_EQ(0, absent.walked) << "fsst: " << fsst;
    EXPECT_EQ(blocks, absent.decided) << "fsst: " << fsst;
    EXPECT_TRUE(absent.matched.empty()) << "fsst: " << fsst;
  }
}

// The byte the automaton moves on is rare and never lands on a restart entry,
// which is the only shape in which the FSST leg of the block scan decides
// anything: every other entry stores codes rather than bytes, so a block that
// holds the byte is admitted or declined by the code table alone.
TEST_F(TermDictTest, automaton_block_scan_reads_fsst_codes) {
  static constexpr std::string_view kSyllables[] = {
    "ka", "re",  "shi", "to",  "mu",  "na",  "ri",  "bo",
    "lu", "tan", "ver", "sil", "mor", "dan", "kul", "pes",
  };
  Corpus corpus;
  for (size_t i = 0; i != 40000; ++i) {
    std::string word = "document_field_";
    uint64_t x = i * 2654435761ULL + 12345;
    for (size_t j = 0; j != 8; ++j) {
      x = x * 6364136223846793005ULL + 1442695040888963407ULL;
      word += kSyllables[(x >> 33) % std::size(kSyllables)];
    }
    // One key in a hundred carries the marker, so a leaf block holds a handful
    // of them and a restart entry -- one entry in sixteen -- almost never is
    // one of them.
    if (i % 100 == 37) {
      word += 'z';
    }
    corpus.terms.emplace_back(MakeTerm(word));
  }
  std::ranges::sort(corpus.terms);
  corpus.terms.erase(std::unique(corpus.terms.begin(), corpus.terms.end()),
                     corpus.terms.end());
  corpus.docs = MakeDocs(16, 1);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);
  ASSERT_TRUE(field->StorageInfo(/*walk=*/false).fsst);

  static constexpr std::string_view kPattern = "document_field_.*z";
  const irs::RegexpAcceptor re2{irs::ViewCast<irs::byte_type>(kPattern)};
  ASSERT_TRUE(re2.ok());
  const auto before = irs::term_dict::AutomatonBlockCounts();
  std::vector<irs::bstring> matched;
  auto it = field->iterator(re2);
  ASSERT_NE(nullptr, it);
  while (it->next()) {
    matched.emplace_back(it->value());
  }
  const auto after = irs::term_dict::AutomatonBlockCounts();

  // Both verdicts occur: a block without the marker is declined whole, one
  // with it is walked, and the walk finds every marked key.
  EXPECT_LT(0, after.decided - before.decided);
  EXPECT_LT(0, after.walked - before.walked);
  EXPECT_EQ(NativeMatches(kPattern, corpus.terms), matched);
  EXPECT_LT(300, matched.size());
}
#endif

// The model both backends must agree on: a run of well-formed UTF-8 code
// points, surrogates included (a partial rune range covers them and both models
// encode them as three bytes), overlongs and anything above U+10FFFF excluded.
bool StrictCodePoints(irs::bytes_view key) {
  static constexpr std::pair<irs::byte_type, irs::byte_type> kFirst[]{
    {0x80, 0xBF}, {0xA0, 0xBF}, {0x90, 0xBF}, {0x80, 0x8F}};
  for (size_t i = 0; i != key.size();) {
    const uint32_t lead = key[i++];
    size_t extra = 0;
    size_t rule = 0;
    if (lead < 0x80) {
      continue;
    } else if (lead >= 0xC2 && lead <= 0xDF) {
      extra = 1;
    } else if (lead >= 0xE0 && lead <= 0xEF) {
      extra = 2;
      rule = lead == 0xE0 ? 1 : 0;
    } else if (lead >= 0xF0 && lead <= 0xF4) {
      extra = 3;
      rule = lead == 0xF0 ? 2 : (lead == 0xF4 ? 3 : 0);
    } else {
      return false;
    }
    if (key.size() - i < extra) {
      return false;
    }
    for (size_t c = 0; c != extra; ++c) {
      const auto [lo, hi] = c == 0 ? kFirst[rule] : kFirst[0];
      if (key[i] < lo || key[i] > hi) {
        return false;
      }
      ++i;
    }
  }
  return true;
}

// Where the two backends used to differ, now pinned equal in both directions.
// `.` and a negated class are a *code point* in both models, and iresearch's is
// the strict one: a partial character class expands range by range into
// well-formed UTF-8 (surrogates included, overlongs and above-U+10FFFF not),
// while RE2's compiler widens the single range `[0x80, Runemax]` to loose byte
// ranges. `NarrowCharClass` splits that range so RE2 stays on its strict path,
// which makes RE2 *reject* the ill-formed sequences it used to accept.
//
// The oracle is computed from the model rather than taken from RE2's own
// engine: after the narrowing the backend deliberately no longer agrees with
// `RE2::FullMatch` on these keys, which is the whole point.
TEST_F(TermDictTest, regexp_parity_non_utf8) {
  static constexpr std::string_view kTails[] = {
    "z",
    "\xc3\xa9",          // é, well formed
    "\x80",              // lone continuation
    "\xc0\xaf",          // overlong 2-byte
    "\xc1\x81",          // overlong 2-byte
    "\xe0\x80\x80",      // overlong 3-byte
    "\xed\xa0\x80",      // surrogate D800
    "\xf4\x90\x80\x80",  // above U+10FFFF
    "\xf5\x80\x80\x80",  // invalid lead
    "\xff",              // invalid byte
  };

  Corpus corpus;
  for (const auto tail : kTails) {
    corpus.terms.emplace_back(MakeTerm("key" + std::string{tail}));
  }
  std::ranges::sort(corpus.terms);
  corpus.docs = MakeDocs(4, 1);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory new_dir;
  WriteV2(new_dir, corpus, {});
  auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);

  auto collect = [](irs::SeekTermIterator::ptr it) {
    std::vector<irs::bstring> out;
    while (it->next()) {
      out.emplace_back(it->value());
    }
    return out;
  };

  // Every tail above is either one well-formed code point or ill-formed, so
  // each of the three patterns admits exactly the well-formed ones.
  std::vector<irs::bstring> expected;
  for (const auto& term : corpus.terms) {
    if (StrictCodePoints(irs::bytes_view{term}.substr(3))) {
      expected.emplace_back(term);
    }
  }
  ASSERT_EQ(3, expected.size());

  for (std::string_view pattern : {"key.", "key.*", "key[^a]*"}) {
    const auto bytes = irs::ViewCast<irs::byte_type>(pattern);
    const irs::RegexpAcceptor re2{bytes};
    ASSERT_TRUE(re2.ok());

    const auto dfa = collect(new_opened.Field()->iterator(re2));
    ASSERT_EQ(expected, dfa) << "pattern: " << pattern;

    const auto source = RegexpSource(pattern);
    ASSERT_EQ(expected,
              FullScanAccepted(*new_opened.Field(), *source->Predicate()))
      << "predicate, pattern: " << pattern;

    // The narrowing is what makes the walk and the model agree: RE2's own
    // engine still admits the overlong and above-U+10FFFF sequences the model
    // rejects.
    ASSERT_GT(NativeMatches(pattern, corpus.terms).size(), dfa.size())
      << "pattern: " << pattern;
  }
}

// `BitUnion` over a set of terms, against the union computed from the corpus:
// every term of this corpus carries the same doc list, so the union is exactly
// that list.
TEST_F(TermDictTest, bit_union_matches_corpus) {
  Corpus corpus;
  corpus.terms = MakeTerms(1000, "bit_union_");
  corpus.docs = MakeDocs(200, 5);
  corpus.features = irs::IndexFeatures::Freq;
  corpus.doc_count = 2048;

  irs::MemoryDirectory new_dir;
  WriteV2(new_dir, corpus, {});

  auto new_opened = Open<irs::term_dict::FieldReader>(new_dir);

  auto bits = [&corpus](const irs::TermReader& field) {
    std::vector<irs::TermCookie> cookies;
    for (size_t i = 0; i < corpus.terms.size(); i += 97) {
      auto it = field.iterator(irs::SeekMode::RandomOnly);
      EXPECT_TRUE(it->seek(corpus.terms[i]));
      cookies.emplace_back(it->cookie());
    }
    size_t next = 0;
    auto provider = [&]() -> const irs::TermCookie* {
      return next == cookies.size() ? nullptr : &cookies[next++];
    };
    std::vector<size_t> set(256, 0);
    const size_t bits_set = field.RowGroupBitUnion(provider, 0, set.data());
    return std::pair{bits_set, set};
  };

  constexpr auto kBits = irs::BitsRequired<size_t>();
  std::vector<size_t> expected_set(256, 0);
  size_t expected_count = 0;
  for (size_t i = 0; i < corpus.terms.size(); i += 97) {
    expected_count += corpus.docs.size();
  }
  for (const auto [doc, freq] : corpus.docs) {
    irs::SetBit(expected_set[doc / kBits], doc % kBits);
  }
  const auto actual = bits(*new_opened.Field());
  ASSERT_EQ(expected_count, actual.first);
  ASSERT_EQ(expected_set, actual.second);
}

// FSST must be picked up on a suffix-heavy vocabulary, shrink `.idx`, and
// round-trip byte-identically.
TEST_F(TermDictTest, fsst_round_trip_and_size) {
  Corpus corpus;
  corpus.terms = MakeWordyTerms(40000);
  corpus.docs = MakeDocs(16, 1);
  corpus.features = irs::IndexFeatures::Freq;

  size_t key_bytes = 0;
  for (const auto& term : corpus.terms) {
    key_bytes += term.size();
  }
  ASSERT_GT(key_bytes, 256 * 1024);

  irs::term_dict::WriterOptions raw;
  raw.fsst_enabled = false;

  irs::MemoryDirectory fsst_dir;
  irs::MemoryDirectory raw_dir;
  WriteV2(fsst_dir, corpus, {});
  WriteV2(raw_dir, corpus, raw);

  ASSERT_LT(IdxSize(fsst_dir), IdxSize(raw_dir));

  for (auto* dir : {&fsst_dir, &raw_dir}) {
    auto opened = Open<irs::term_dict::FieldReader>(*dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(corpus.terms.size(), field->size());

    auto it = field->iterator(irs::SeekMode::NORMAL);
    size_t i = 0;
    while (it->next()) {
      ASSERT_EQ(corpus.terms[i], it->value());
      ++i;
    }
    ASSERT_EQ(corpus.terms.size(), i);

    for (size_t j = 0; j < corpus.terms.size(); j += 13) {
      auto seek = field->iterator(irs::SeekMode::RandomOnly);
      ASSERT_TRUE(seek->seek(corpus.terms[j]));
      ASSERT_EQ(corpus.terms[j], seek->value());
    }
  }
}

TEST_F(TermDictTest, root_resident_at_open) {
  Corpus corpus;
  corpus.terms = MakeWordyTerms(20000);
  corpus.docs = MakeDocs(8, 1);

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});

  auto codec = irs::formats::Get(kCodec);
  irs::SegmentMeta meta;
  meta.name = kSegment;
  irs::IdxReader idx{dir, kSegment};
  irs::term_dict::FieldReader fields{codec->get_postings_reader(),
                                     irs::IResourceManager::gNoop};
  fields.prepare(irs::ReaderState{.dir = &dir, .meta = &meta, .idx = &idx});
  const uint64_t at_open = fields.CountMappedMemory();
  ASSERT_GT(at_open, 0);

  auto it = fields.field(kFieldId)->iterator(irs::SeekMode::RandomOnly);
  ASSERT_TRUE(it->seek(corpus.terms.front()));
  std::vector<irs::TermRange> ranges(8);
  ASSERT_LT(1, fields.field(kFieldId)->TermRanges(ranges));
  ASSERT_EQ(at_open, fields.CountMappedMemory());
}

// The scan-facing contract is per-(term, row group). A segment smaller than the
// row group holds one, so this pins the degenerate case -- one row group, local
// ids equal to the segment's doc ids -- through the API shape.
void AssertRowGroupContract() {
  Corpus corpus;
  corpus.terms = MakeTerms(2000, "row_group_");
  corpus.docs = MakeDocs(120, 3);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  size_t checked = 0;
  auto it = field->iterator(irs::SeekMode::NORMAL);
  while (it->next()) {
    // The term meta is decoded lazily, so the row-group list is only populated
    // once the entry has been read -- the same contract as `TermMeta` itself.
    it->read();
    auto cookie = it->cookie();
    ASSERT_FALSE(cookie.rgs.empty());

    const auto row_groups = cookie.RowGroups();
    ASSERT_EQ(1, row_groups.size());
    ASSERT_EQ(0, row_groups.front().rg);
    ASSERT_EQ(corpus.docs.size(), row_groups.front().docs_count);
    ASSERT_EQ(cookie.stats.docs_count, row_groups.front().docs_count);

    if (++checked % 251 != 0) {
      continue;
    }
    // The whole-term iterator and the row-group iterator must agree, and both
    // must agree with the input, since one row group covers everything.
    const irs::TermLeaf leaf{.cookie = &cookie, .field = field->meta()};
    auto whole = field->RowGroupIterator(corpus.features, leaf, 0);
    auto local =
      field->RowGroupIterator(corpus.features, leaf, row_groups.front().rg);
    ASSERT_NE(nullptr, whole);
    ASSERT_NE(nullptr, local);
    const auto expected = ReadPostings(*whole, true);
    ASSERT_EQ(corpus.docs.size(), expected.size());
    ASSERT_EQ(expected, ReadPostings(*local, true));
    ASSERT_EQ(corpus.docs.front().first, expected.front().first);
  }
  ASSERT_EQ(corpus.terms.size(), checked);
}

TEST_F(TermDictTest, row_group_contract) { AssertRowGroupContract(); }

namespace {

constexpr irs::doc_id_t kRgSegmentDocs = 4300;
constexpr uint32_t kRgSize = 1000;

Docs Range(irs::doc_id_t begin, irs::doc_id_t end, uint32_t step) {
  Docs docs;
  for (irs::doc_id_t doc = begin; doc <= end; doc += step) {
    docs.emplace_back(doc, std::max(1U, doc % 5));
  }
  return docs;
}

RgCorpus MakeRgCorpus(irs::IndexFeatures features) {
  std::vector<std::pair<std::string, Docs>> entries;
  // Every row group long enough to own a skip list. Stops one short of the
  // segment on purpose: the last document is then owned by `single_last`
  // alone, so the field's distinct document count depends on a term whose only
  // row group is an inlined singleton -- the one shape in which the field
  // writer, not the postings writer, is what counts the document.
  entries.emplace_back("dense", Range(1, kRgSegmentDocs - 1, 3));
  // A single document, in the first and in the short last row group.
  entries.emplace_back("single_first", Range(1, 1, 1));
  entries.emplace_back("single_last", Range(kRgSegmentDocs, kRgSegmentDocs, 1));
  // Straddles two boundaries, with the run on either side shorter than a
  // postings block.
  entries.emplace_back("straddle", Docs{{998, 1},
                                        {999, 2},
                                        {1000, 3},
                                        {1001, 4},
                                        {1002, 1},
                                        {2000, 2},
                                        {2001, 3}});
  // One or two documents per row group.
  entries.emplace_back("sparse", Range(1, kRgSegmentDocs, 700));
  // Exactly the skip-list threshold on either side of it.
  Docs edge = Range(1, 129, 1);
  for (const auto& doc : Range(1001, 1128, 1)) {
    edge.emplace_back(doc);
  }
  entries.emplace_back("block_edge", std::move(edge));
  // Confined to a single row group that is neither the first nor the last.
  entries.emplace_back("one_rg_only", Range(2001, 2100, 1));

  // A term confined to one row group carries that row group's whole record in
  // the block payload entry, delta-chained against the previous such term of
  // the same restart group. These are enough of them, in sorted order and each
  // with more than one document, that the chain crosses many restart points --
  // which is what a reset mismatch between the writer and the reader shows up
  // as, and nothing else in this corpus reaches.
  for (uint32_t i = 0; i != 200; ++i) {
    const irs::doc_id_t base = 1 + (i % 5) * kRgSize + (i % 7);
    entries.emplace_back(absl::StrCat("rgonly_", absl::Dec(i, absl::kZeroPad4)),
                         Range(base, base + 2 * (1 + i % 4), 1 + i % 4));
  }

  for (uint32_t i = 0; i != 600; ++i) {
    entries.emplace_back(absl::StrCat("filler_", absl::Dec(i, absl::kZeroPad4)),
                         Range(1 + i % 37, kRgSegmentDocs - 1, 11 + i % 97));
  }
  std::ranges::sort(entries, {}, &std::pair<std::string, Docs>::first);

  RgCorpus corpus;
  corpus.features = features;
  corpus.doc_count = kRgSegmentDocs;
  corpus.terms.reserve(entries.size());
  corpus.docs.reserve(entries.size());
  for (auto& [term, docs] : entries) {
    corpus.terms.emplace_back(MakeTerm(term));
    corpus.docs.emplace_back(std::move(docs));
  }
  return corpus;
}

}  // namespace

// The writer emits one posting list per (term, row group) with row-group-local
// ids. Three legs, term by term, with frequencies, positions and offsets: the
// hit stream computed straight from the corpus (the independent oracle -- an
// adapter bug corrupts an index, never the corpus); the same corpus written
// with a row group spanning the segment, its segment-wide stream decomposed
// through the partitioned index's directory; and per-row-group iteration over
// the partitioned index.
TEST_F(TermDictTest, row_group_partitioned_postings) {
  for (auto features : {irs::IndexFeatures::None, irs::IndexFeatures::Freq,
                        irs::IndexFeatures::Freq | irs::IndexFeatures::Pos,
                        irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                          irs::IndexFeatures::Offs}) {
    const auto corpus = MakeRgCorpus(features);

    irs::MemoryDirectory whole_dir;
    irs::MemoryDirectory split_dir;
    WriteRg(whole_dir, corpus, irs::term_dict::kRowGroupSizeUnbounded);
    WriteRg(split_dir, corpus, kRgSize);

    auto whole_opened = Open<irs::term_dict::FieldReader>(whole_dir);
    auto split_opened = Open<irs::term_dict::FieldReader>(split_dir);
    const auto* whole = whole_opened.Field();
    const auto* split = split_opened.Field();
    ASSERT_NE(nullptr, whole);
    ASSERT_NE(nullptr, split);

    // The field's distinct document count, from the corpus.
    std::vector<bool> seen(kRgSegmentDocs + irs::doc_limits::min(), false);
    for (const auto& docs : corpus.docs) {
      for (const auto [doc, freq] : docs) {
        seen[doc] = true;
      }
    }
    const auto distinct = static_cast<uint64_t>(std::ranges::count(seen, true));

    ASSERT_EQ(corpus.terms.size(), split->size());
    ASSERT_EQ(distinct, split->docs_count());
    ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, (split->min)());
    ASSERT_EQ(irs::bytes_view{corpus.terms.back()}, (split->max)());
    ASSERT_EQ(corpus.terms.size(), whole->size());
    ASSERT_EQ(distinct, whole->docs_count());

    const auto layout = split->RowGroups();
    ASSERT_EQ(kRgSize, layout.rows_per_group);
    ASSERT_EQ(kRgSegmentDocs, layout.segment_docs);
    ASSERT_EQ(5, layout.count);
    ASSERT_EQ(irs::doc_limits::min(), layout.Base(0));
    ASSERT_EQ(kRgSize, layout.Rows(0));
    ASSERT_EQ(kRgSegmentDocs - 4 * kRgSize, layout.Rows(layout.count - 1));

    // The unpartitioned index has no directory.
    ASSERT_EQ(1, whole->RowGroups().count);

    auto whole_hits = [&](const irs::TermReader& field,
                          const irs::TermCookie& cookie,
                          std::vector<RgHit>& out) {
      const irs::TermLeaf leaf{.cookie = &cookie, .field = field.meta()};
      auto postings = field.RowGroupIterator(features, leaf, 0);
      ASSERT_NE(nullptr, postings);
      ReadHits(*postings, features, layout, 0, /*decompose=*/true, out);
    };

    const bool with_freq =
      irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);
    auto whole_it = whole->iterator(irs::SeekMode::NORMAL);
    auto split_it = split->iterator(irs::SeekMode::NORMAL);
    size_t checked = 0;
    while (split_it->next()) {
      ASSERT_TRUE(whole_it->next());
      const auto& term_docs = corpus.docs[checked];
      ASSERT_EQ(irs::bytes_view{corpus.terms[checked]}, split_it->value());
      ASSERT_EQ(irs::bytes_view{corpus.terms[checked]}, whole_it->value());
      whole_it->read();
      split_it->read();

      const auto* whole_meta = irs::get<irs::TermMeta>(*whole_it);
      const auto* split_meta = irs::get<irs::TermMeta>(*split_it);
      ASSERT_NE(nullptr, whole_meta);
      ASSERT_NE(nullptr, split_meta);
      uint32_t term_freq = 0;
      for (const auto [doc, freq] : term_docs) {
        term_freq += freq;
      }
      ASSERT_EQ(term_docs.size(), split_meta->docs_count);
      ASSERT_EQ(with_freq ? term_freq : 0, split_meta->freq);
      ASSERT_EQ(term_docs.size(), whole_meta->docs_count);
      ASSERT_EQ(with_freq ? term_freq : 0, whole_meta->freq);

      auto whole_cookie = whole_it->cookie();
      auto split_cookie = split_it->cookie();
      ASSERT_FALSE(whole_cookie.rgs.empty());
      ASSERT_FALSE(split_cookie.rgs.empty());
      ASSERT_EQ(1, whole_cookie.RowGroups().size());

      const auto groups = split_cookie.RowGroups();
      ASSERT_FALSE(groups.empty());
      uint32_t summed = 0;
      for (size_t i = 0; i != groups.size(); ++i) {
        ASSERT_LT(groups[i].rg, layout.count);
        ASSERT_NE(0, groups[i].docs_count);
        if (i != 0) {
          ASSERT_LT(groups[i - 1].rg, groups[i].rg);
        }
        summed += groups[i].docs_count;
      }
      ASSERT_EQ(split_meta->docs_count, summed);

      const auto expected = ExpectedHits(term_docs, features, kRgSize);
      ASSERT_EQ(term_docs.size(), expected.size());
      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq)) {
        ASSERT_NE(0, expected.front().freq);
      }
      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos)) {
        ASSERT_EQ(expected.front().freq, expected.front().positions.size());
      }
      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs)) {
        ASSERT_EQ(expected.front().freq, expected.front().offsets.size());
      }

      std::vector<RgHit> degenerate;
      ASSERT_NO_FATAL_FAILURE(whole_hits(*whole, whole_cookie, degenerate));
      ASSERT_EQ(expected, degenerate)
        << "term: " << ::testing::PrintToString(split_it->value());

      std::vector<RgHit> actual;
      {
        const irs::TermLeaf leaf{.cookie = &split_cookie,
                                 .field = split->meta()};
        for (const auto& group : groups) {
          auto postings = split->RowGroupIterator(features, leaf, group.rg);
          ASSERT_NE(nullptr, postings);
          const size_t before = actual.size();
          ReadHits(*postings, features, layout, group.rg, /*decompose=*/false,
                   actual);
          ASSERT_EQ(group.docs_count, actual.size() - before);
        }
      }

      ASSERT_EQ(expected, actual)
        << "term: " << ::testing::PrintToString(split_it->value());
      ++checked;
    }
    ASSERT_FALSE(split_it->next());
    ASSERT_FALSE(whole_it->next());
    ASSERT_EQ(corpus.terms.size(), checked);
  }
}

// A vocabulary small enough for the block-less layout goes through the same
// writer, so it picks up the row-group dimension without a special case. This
// is also the shape of the boolean/null synthetic fields and, with
// `IndexFeatures::None`, of unquantized IVF cluster postings.
// A two-term vocabulary: one leaf block, no separators, and the row-group
// dimension has to come out of the same writer with no special case.
TEST_F(TermDictTest, row_group_one_block) {
  RgCorpus corpus;
  corpus.features = irs::IndexFeatures::Freq;
  corpus.doc_count = kRgSegmentDocs;
  corpus.terms = {MakeTerm("false"), MakeTerm("true")};
  corpus.docs = {Range(1, kRgSegmentDocs, 2), Range(2, kRgSegmentDocs, 2)};

  irs::MemoryDirectory dir;
  WriteRg(dir, corpus, kRgSize);
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);
  ASSERT_EQ(2, field->size());

  const auto layout = field->RowGroups();
  ASSERT_EQ(5, layout.count);
  ASSERT_EQ(kRgSize, layout.rows_per_group);

  auto it = field->iterator(irs::SeekMode::NORMAL);
  size_t term = 0;
  while (it->next()) {
    ASSERT_EQ(corpus.terms[term], it->value());
    it->read();
    auto cookie = it->cookie();
    const auto groups = cookie.RowGroups();
    ASSERT_EQ(layout.count, groups.size());

    const irs::TermLeaf leaf{.cookie = &cookie, .field = field->meta()};
    size_t doc = 0;
    for (const auto& group : groups) {
      auto postings = field->RowGroupIterator(corpus.features, leaf, group.rg);
      ASSERT_NE(nullptr, postings);
      irs::doc_id_t local;
      while (!irs::doc_limits::eof(local = postings->advance())) {
        ASSERT_LT(doc, corpus.docs[term].size());
        const auto expected = corpus.docs[term][doc].first;
        ASSERT_EQ(group.rg,
                  (expected - irs::doc_limits::min()) / layout.rows_per_group);
        ASSERT_EQ(expected - group.rg * layout.rows_per_group, local);
        ASSERT_EQ(corpus.docs[term][doc].second, postings->GetFreq());
        ++doc;
      }
    }
    ASSERT_EQ(corpus.docs[term].size(), doc);
    ++term;
  }
  ASSERT_EQ(corpus.terms.size(), term);
}

// A batch pass over a partitioned field has to produce the same rg-lists the
// per-term seek does -- the block payload entry it decodes is delta-chained
// against the previous entry of the restart group, so a cursor that arrived at
// a term differently is exactly what would corrupt them.
TEST_F(TermDictTest, batch_seek_row_groups) {
  const auto corpus = MakeRgCorpus(irs::IndexFeatures::Freq);

  irs::MemoryDirectory split_dir;
  WriteRg(split_dir, corpus, kRgSize);
  auto split_opened = Open<irs::term_dict::FieldReader>(split_dir);
  const auto* split = split_opened.Field();
  ASSERT_NE(nullptr, split);

  const auto layout = split->RowGroups();
  ASSERT_EQ(kRgSize, layout.rows_per_group);

  // Every term plus a miss between each pair, so the pass is driven through
  // both outcomes at every block and restart boundary.
  std::vector<irs::bstring> owned;
  owned.reserve(2 * corpus.terms.size());
  for (const auto& term : corpus.terms) {
    owned.emplace_back(term);
    owned.emplace_back(Between(term));
  }
  std::ranges::sort(owned);
  std::vector<irs::bytes_view> probes;
  probes.reserve(owned.size());
  for (const auto& term : owned) {
    probes.emplace_back(term);
  }

  // Row-group hits, addressed the way the scan addresses them, taken through
  // the batch cookie; against the hit stream computed from the corpus.
  auto batch = split->BatchIterator(probes);
  ASSERT_NE(nullptr, batch);
  size_t checked = 0;
  while (batch->next()) {
    const auto& term_docs = corpus.docs[checked];
    ASSERT_EQ(irs::bytes_view{corpus.terms[checked]}, batch->value());
    batch->read();

    const auto* batch_meta = irs::get<irs::TermMeta>(*batch);
    ASSERT_NE(nullptr, batch_meta);
    uint32_t term_freq = 0;
    for (const auto [doc, freq] : term_docs) {
      term_freq += freq;
    }
    ASSERT_EQ(term_docs.size(), batch_meta->docs_count);
    ASSERT_EQ(term_freq, batch_meta->freq);

    const auto expected = ExpectedHits(term_docs, corpus.features, kRgSize);

    auto cookie = batch->cookie();
    ASSERT_FALSE(cookie.rgs.empty());
    std::vector<RgHit> actual;
    for (const auto& group : cookie.RowGroups()) {
      const irs::TermLeaf leaf{.cookie = &cookie, .field = split->meta()};
      auto postings = split->RowGroupIterator(corpus.features, leaf, group.rg);
      ASSERT_NE(nullptr, postings);
      ReadHits(*postings, corpus.features, layout, group.rg,
               /*decompose=*/false, actual);
    }
    ASSERT_EQ(expected, actual) << "term: " << checked;
    ++checked;
  }
  ASSERT_EQ(corpus.terms.size(), checked);
}

// The exact cut of the corpus above, so a change of the partitioning rule is
// visible as such rather than only as a mismatch against another index.
TEST_F(TermDictTest, row_group_shape) {
  const auto corpus = MakeRgCorpus(irs::IndexFeatures::Freq);

  irs::MemoryDirectory dir;
  WriteRg(dir, corpus, kRgSize);
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  // (row group, documents there), so a shape reads as one literal.
  using Groups = std::vector<std::pair<uint32_t, uint32_t>>;
  auto shape = [&](std::string_view term) {
    Groups out;
    auto it = field->iterator(irs::SeekMode::RandomOnly);
    EXPECT_TRUE(it->seek(MakeTerm(term)));
    auto cookie = it->cookie();
    for (const auto& group : cookie.RowGroups()) {
      out.emplace_back(group.rg, group.docs_count);
    }
    return out;
  };
  ASSERT_EQ((Groups{{0, 3}, {1, 3}, {2, 1}}), shape("straddle"));
  ASSERT_EQ((Groups{{0, 1}}), shape("single_first"));
  ASSERT_EQ((Groups{{4, 1}}), shape("single_last"));
  ASSERT_EQ((Groups{{2, 100}}), shape("one_rg_only"));
  ASSERT_EQ((Groups{{0, 129}, {1, 128}}), shape("block_edge"));
  ASSERT_EQ((Groups{{0, 2}, {1, 1}, {2, 2}, {3, 1}, {4, 1}}), shape("sparse"));

  // Every full row group of `dense` owns a skip list; the short last one does
  // not, which is the pair of postings shapes the rg-list has to distinguish.
  const auto dense = shape("dense");
  ASSERT_EQ(5, dense.size());
  uint32_t total = 0;
  for (uint32_t rg = 0; rg != dense.size(); ++rg) {
    ASSERT_EQ(rg, dense[rg].first);
    if (rg + 1 != dense.size()) {
      ASSERT_LT(irs::doc_limits::kBlockSize, dense[rg].second);
    } else {
      ASSERT_GT(irs::doc_limits::kBlockSize, dense[rg].second);
    }
    total += dense[rg].second;
  }
  ASSERT_EQ((kRgSegmentDocs + 1) / 3, total);
}

// What a query pays in heap per term for its run list. A term of one row group
// -- every term of an unpartitioned field and most of a partitioned one -- pays
// nothing; beyond that a run costs its own width and nothing more, which is
// what makes the width worth minimizing.
TEST_F(TermDictTest, term_run_list_heap_per_term) {
  const auto corpus = MakeRgCorpus(irs::IndexFeatures::Freq);

  irs::MemoryDirectory dir;
  WriteRg(dir, corpus, kRgSize);
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  constexpr size_t kRounds = 1000;
  auto bytes_per_term = [&](std::string_view term, size_t runs) {
    auto it = field->iterator(irs::SeekMode::RandomOnly);
    EXPECT_TRUE(it->seek(MakeTerm(term)));
    size_t seen = 0;
    // The iterator's own list reaches its capacity on the first cookie; only
    // the copy a query keeps is measured.
    for (size_t i = 0; i != 2; ++i) {
      seen += it->cookie().rgs.size();
    }
    EXPECT_EQ(2 * runs, seen);
    seen = 0;
    const auto before = ThreadAllocated();
    for (size_t i = 0; i != kRounds; ++i) {
      seen += it->cookie().rgs.size();
    }
    const auto after = ThreadAllocated();
    EXPECT_EQ(kRounds * runs, seen);
    return (after - before) / kRounds;
  };

  EXPECT_EQ(0, bytes_per_term("one_rg_only", 1));
  EXPECT_EQ(0, bytes_per_term("single_first", 1));
  // Five runs: one allocation, and it is the runs themselves.
  EXPECT_EQ(5 * sizeof(irs::TermRowGroup), bytes_per_term("sparse", 5));
  EXPECT_EQ(5 * sizeof(irs::TermRowGroup), bytes_per_term("dense", 5));
}

// A term absent from a row group has no posting list there.
TEST_F(TermDictTest, row_group_absent) {
  RgCorpus corpus;
  corpus.features = irs::IndexFeatures::Freq;
  corpus.doc_count = kRgSegmentDocs;
  corpus.terms = {MakeTerm("aaa"), MakeTerm("bbb"), MakeTerm("ccc")};
  corpus.docs = {Range(1, 100, 1), Range(2001, 2100, 1),
                 Range(1, kRgSegmentDocs, 1)};

  irs::MemoryDirectory dir;
  WriteRg(dir, corpus, kRgSize);
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  auto it = field->iterator(irs::SeekMode::RandomOnly);
  ASSERT_TRUE(it->seek(corpus.terms[1]));
  auto cookie = it->cookie();
  const auto groups = cookie.RowGroups();
  ASSERT_EQ(1, groups.size());
  ASSERT_EQ(2, groups.front().rg);
  ASSERT_EQ(100, groups.front().docs_count);

  const irs::TermLeaf leaf{.cookie = &cookie, .field = field->meta()};
  ASSERT_EQ(nullptr, field->RowGroupIterator(corpus.features, leaf, 0));
  ASSERT_EQ(nullptr, field->RowGroupIterator(corpus.features, leaf, 4));
  ASSERT_NE(nullptr, field->RowGroupIterator(corpus.features, leaf, 2));
}

// The row group a term's postings are cut at is a writer setting, not a corpus
// property: the same corpus at several row group sizes must agree with itself.
TEST_F(TermDictTest, row_group_sizes_agree) {
  const auto corpus = MakeRgCorpus(irs::IndexFeatures::Freq);

  for (uint32_t rgs : {128u, 129u, 1024u, 4096u, 5000u}) {
    irs::MemoryDirectory split_dir;
    WriteRg(split_dir, corpus, rgs);
    auto split_opened = Open<irs::term_dict::FieldReader>(split_dir);
    const auto* split = split_opened.Field();
    ASSERT_NE(nullptr, split);

    const auto layout = split->RowGroups();
    ASSERT_EQ(rgs, layout.rows_per_group);
    ASSERT_EQ((kRgSegmentDocs + rgs - 1) / rgs, layout.count);

    auto split_it = split->iterator(irs::SeekMode::NORMAL);
    size_t checked = 0;
    while (split_it->next()) {
      ASSERT_EQ(irs::bytes_view{corpus.terms[checked]}, split_it->value());
      split_it->read();
      auto split_cookie = split_it->cookie();

      const auto expected =
        ExpectedHits(corpus.docs[checked], corpus.features, rgs);
      ++checked;

      std::vector<RgHit> actual;
      const irs::TermLeaf split_leaf{.cookie = &split_cookie,
                                     .field = split->meta()};
      for (const auto& group : split_cookie.RowGroups()) {
        auto postings =
          split->RowGroupIterator(corpus.features, split_leaf, group.rg);
        ASSERT_NE(nullptr, postings);
        ReadHits(*postings, corpus.features, layout, group.rg,
                 /*decompose=*/false, actual);
      }
      ASSERT_EQ(expected, actual) << "row group size: " << rgs;
    }
    ASSERT_EQ(corpus.terms.size(), checked);
  }
}

// The multi-term query's unscored leg unions its terms into one bitset instead
// of one iterator each, so that union has to exist per row group and in the row
// group's local id space. Oracle: the same union computed from the corpus, and
// the unpartitioned index's segment-wide union of the same terms.
TEST_F(TermDictTest, row_group_bit_union) {
  const auto corpus = MakeRgCorpus(irs::IndexFeatures::Freq);

  irs::MemoryDirectory whole_dir;
  irs::MemoryDirectory split_dir;
  WriteRg(whole_dir, corpus, irs::term_dict::kRowGroupSizeUnbounded);
  WriteRg(split_dir, corpus, kRgSize);

  auto whole_opened = Open<irs::term_dict::FieldReader>(whole_dir);
  auto split_opened = Open<irs::term_dict::FieldReader>(split_dir);
  const auto* whole = whole_opened.Field();
  const auto* split = split_opened.Field();
  ASSERT_NE(nullptr, whole);
  ASSERT_NE(nullptr, split);

  const auto layout = split->RowGroups();
  ASSERT_EQ(kRgSize, layout.rows_per_group);

  // Terms whose postings between them cover every shape the rg-list encodes.
  constexpr size_t kStep = 53;
  auto seek_cookies = [&](const irs::TermReader& field) {
    std::vector<irs::TermCookie> cookies;
    for (size_t i = 0; i < corpus.terms.size(); i += kStep) {
      auto it = field.iterator(irs::SeekMode::RandomOnly);
      EXPECT_TRUE(it->seek(corpus.terms[i]));
      cookies.emplace_back(it->cookie());
    }
    return cookies;
  };
  auto make_provider = [](std::vector<irs::TermCookie>& cookies, size_t& next) {
    return [&]() -> const irs::TermCookie* {
      return next == cookies.size() ? nullptr : &cookies[next++];
    };
  };

  constexpr size_t kBits = irs::BitsRequired<size_t>();
  auto whole_cookies = seek_cookies(*whole);
  size_t whole_next = 0;
  auto whole_provider = make_provider(whole_cookies, whole_next);
  std::vector<size_t> whole_set(
    irs::bitset::bits_to_words(kRgSegmentDocs + irs::doc_limits::min()), 0);
  const size_t whole_count =
    whole->RowGroupBitUnion(whole_provider, 0, whole_set.data());
  ASSERT_NE(0, whole_count);

  // The same union straight from the corpus, so the comparison below does not
  // rest on one index alone.
  std::vector<size_t> corpus_set(whole_set.size(), 0);
  size_t corpus_count = 0;
  for (size_t i = 0; i < corpus.terms.size(); i += kStep) {
    corpus_count += corpus.docs[i].size();
    for (const auto [doc, freq] : corpus.docs[i]) {
      irs::SetBit(corpus_set[doc / kBits], doc % kBits);
    }
  }
  ASSERT_EQ(corpus_count, whole_count);
  ASSERT_EQ(corpus_set, whole_set);

  auto split_cookies = seek_cookies(*split);
  size_t split_total = 0;
  size_t set_bits = 0;
  for (uint32_t rg = 0; rg != layout.count; ++rg) {
    const size_t bits = layout.Rows(rg) + irs::doc_limits::min();
    std::vector<size_t> set(irs::bitset::bits_to_words(bits), 0);
    size_t next = 0;
    auto provider = make_provider(split_cookies, next);
    split_total += split->RowGroupBitUnion(provider, rg, set.data());

    for (irs::doc_id_t local = 0; local != bits; ++local) {
      const bool actual = irs::CheckBit(set[local / kBits], local % kBits);
      if (local < irs::doc_limits::min()) {
        ASSERT_FALSE(actual) << "rg " << rg;
        continue;
      }
      // The one legal direction: the oracle's segment-wide id split by the
      // directory, never a local id put back together.
      const irs::doc_id_t doc = local + rg * layout.rows_per_group;
      const bool expected = irs::CheckBit(whole_set[doc / kBits], doc % kBits);
      ASSERT_EQ(expected, actual) << "rg " << rg << " local " << local;
      set_bits += actual;
    }
  }
  ASSERT_EQ(whole_count, split_total);
  // A union of several thousand postings: the comparison above cannot have
  // passed by comparing two empty bitsets.
  ASSERT_LT(1000, set_bits);
}

// Terms of several kilobytes each: nothing in the dictionary caps a term
// length, so a term wider than a whole leaf block gets a block of its own and
// every read path -- iteration, exact seek, seek_ge, an automaton walk, the
// batch pass -- serves it like any other.
TEST_F(TermDictTest, long_terms) {
  // 60 KiB is past the u16 restart-offset width a multi-restart block is bound
  // by, so these terms are only readable at all if the writer gives each one a
  // block whose single restart point sits at offset zero.
  // The order matters: `long_c` is short enough not to fill a block on its own,
  // so it is still pending when `long_d` arrives, and at restart interval 1 a
  // block holding both would need a second restart offset past the u16 width.
  constexpr size_t kSizes[]{4096, 60000, 2000, 200000};

  Corpus corpus;
  for (size_t i = 0; i != std::size(kSizes); ++i) {
    std::string term{"long_"};
    term += static_cast<char>('a' + i);
    term.append(kSizes[i] - term.size(), 'x');
    corpus.terms.emplace_back(MakeTerm(term));
  }
  // Short terms around them, so a long one is neither the first nor the last
  // entry of the field and the separators have to bound it on both sides. There
  // are enough of them ahead of the long ones to push the field past the FSST
  // sampling window first, so the long terms arrive on the steady-state add
  // path rather than through the final drain -- the two are separate flush
  // decisions and both have to give an oversized entry a block of its own.
  for (const auto& term : MakeTerms(30000, "aaa_")) {
    corpus.terms.emplace_back(term);
  }
  for (const auto& term : MakeTerms(200, "zzz_")) {
    corpus.terms.emplace_back(term);
  }
  std::ranges::sort(corpus.terms);
  corpus.docs = MakeDocs(8, 1);
  corpus.features = irs::IndexFeatures::Freq;

  for (uint32_t interval : {1u, 4u, 16u}) {
    irs::term_dict::WriterOptions options;
    options.restart_interval = interval;
    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, options);
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    ASSERT_EQ(corpus.terms.size(), field->size());
    ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, (field->min)());
    ASSERT_EQ(irs::bytes_view{corpus.terms.back()}, (field->max)());

    // Iteration
    {
      auto it = field->iterator(irs::SeekMode::NORMAL);
      size_t i = 0;
      while (it->next()) {
        ASSERT_EQ(irs::bytes_view{corpus.terms[i]}, it->value())
          << "interval: " << interval << " term: " << i;
        ++i;
      }
      ASSERT_EQ(corpus.terms.size(), i);
    }

    // Exact seek, seek_ge and the posting list of every long term
    for (size_t i = 0; i != std::size(kSizes); ++i) {
      const auto& term =
        corpus.terms[std::ranges::lower_bound(
                       corpus.terms, MakeTerm(absl::StrCat(
                                       "long_", std::string(1, 'a' + i)))) -
                     corpus.terms.begin()];
      ASSERT_EQ(kSizes[i], term.size());
      for (auto mode : {irs::SeekMode::NORMAL, irs::SeekMode::RandomOnly}) {
        auto it = field->iterator(mode);
        ASSERT_TRUE(it->seek(term)) << "interval: " << interval;
        it->read();
        auto docs = it->RowGroupPostings(corpus.features, 0);
        ASSERT_NE(nullptr, docs);
        ASSERT_EQ(corpus.docs.size(), ReadPostings(*docs, true).size());
      }
      {
        auto it = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_EQ(irs::SeekResult::Found, it->seek_ge(term));
        ASSERT_EQ(irs::bytes_view{term}, it->value());
      }
      // A probe one byte longer than a multi-KB term lands on its successor.
      {
        auto it = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_EQ(irs::SeekResult::NotFound, it->seek_ge(Between(term)));
      }
    }

    // Automaton walk over patterns that reach the long terms and past them
    for (std::string_view pattern : {"long_%", "long_axxx%", "%xxx", "aaa_%",
                                     "%", "long_bxx%x", "zzz_00000%"}) {
      const auto source = WildcardSource(pattern);
      ASSERT_EQ(FullScanAccepted(*field, *source->Predicate()),
                WalkTerms(source->Iterator(*field)))
        << "pattern: " << pattern << " interval: " << interval;
    }

    // The sorted batch pass
    {
      std::vector<irs::bytes_view> probes;
      for (const auto& term : corpus.terms) {
        probes.emplace_back(term);
      }
      const auto expected = LoopProbe(*field, probes, corpus.features);
      ASSERT_EQ(corpus.terms.size(), expected.size());
      ASSERT_EQ(expected, BatchProbe(*field, probes, corpus.features));
    }
  }
}

// A dictless field stores no dictionary at all: no block, no separator, no
// term-meta section. Its one term's bytes are the field's min/max and its
// posting lists ride in the field header, so every read path is served without
// touching the body of the `.idx`.
TEST_F(TermDictTest, dictless) {
  for (const bool partitioned : {false, true}) {
    for (const auto term : {std::string_view{"true"}, std::string_view{}}) {
      RgCorpus corpus;
      corpus.features = irs::IndexFeatures::Freq;
      corpus.doc_count = kRgSegmentDocs;
      corpus.terms = {MakeTerm(term)};
      corpus.docs = {Range(1, kRgSegmentDocs, 2)};

      const uint32_t rgs =
        partitioned ? kRgSize : irs::term_dict::kRowGroupSizeUnbounded;
      irs::MemoryDirectory dir;
      WriteRg(dir, corpus, rgs, /*dictless=*/true);
      auto opened = Open<irs::term_dict::FieldReader>(dir);
      const auto* field = opened.Field();
      ASSERT_NE(nullptr, field);
      ASSERT_EQ(1, field->size());
      ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, (field->min)());
      ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, (field->max)());
      ASSERT_EQ(corpus.docs.front().size(), field->docs_count());

      const auto layout = field->RowGroups();
      ASSERT_EQ(partitioned ? (kRgSegmentDocs + kRgSize - 1) / kRgSize : 1,
                layout.count);

      const auto expected =
        ExpectedHits(corpus.docs.front(), corpus.features, rgs);
      ASSERT_EQ(corpus.docs.front().size(), expected.size());

      // Iteration yields the one term, and its cookie carries every record.
      {
        auto it = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, it->value());
        it->read();
        const auto* meta = irs::get<irs::TermMeta>(*it);
        ASSERT_NE(nullptr, meta);
        ASSERT_EQ(corpus.docs.front().size(), meta->docs_count);
        auto cookie = it->cookie();
        ASSERT_EQ(partitioned ? layout.count : 1, cookie.rgs.size());

        std::vector<RgHit> actual;
        const irs::TermLeaf leaf{.cookie = &cookie, .field = field->meta()};
        for (const auto& group : cookie.RowGroups()) {
          auto postings =
            field->RowGroupIterator(corpus.features, leaf, group.rg);
          ASSERT_NE(nullptr, postings);
          ReadHits(*postings, corpus.features, layout, group.rg,
                   /*decompose=*/false, actual);
        }
        ASSERT_EQ(expected, actual);
        ASSERT_FALSE(it->next());
      }

      // Exact seek, both modes, hit and miss.
      for (auto mode : {irs::SeekMode::NORMAL, irs::SeekMode::RandomOnly}) {
        auto it = field->iterator(mode);
        ASSERT_TRUE(it->seek(corpus.terms.front()));
        ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, it->value());
        ASSERT_FALSE(field->iterator(mode)->seek(MakeTerm("zzz")));
      }

      // seek_ge below, at and above the term.
      {
        auto it = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_EQ(irs::SeekResult::Found, it->seek_ge(corpus.terms.front()));
        auto above = field->iterator(irs::SeekMode::NORMAL);
        ASSERT_EQ(irs::SeekResult::End, above->seek_ge(MakeTerm("zzz")));
        if (!term.empty()) {
          auto below = field->iterator(irs::SeekMode::NORMAL);
          ASSERT_EQ(irs::SeekResult::NotFound, below->seek_ge(MakeTerm("a")));
          ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, below->value());
        }
      }

      // The term's statistics and the automaton walk, against the full scan.
      ASSERT_EQ(corpus.docs.front().size(),
                tests::TermStats(*field, corpus.terms.front()).docs_count);
      ASSERT_EQ(0, tests::TermStats(*field, MakeTerm("zzz")).docs_count);
      for (std::string_view pattern : {"%", "true", "tr%", "zzz", "%e"}) {
        const auto source = WildcardSource(pattern);
        ASSERT_EQ(FullScanAccepted(*field, *source->Predicate()),
                  WalkTerms(source->Iterator(*field)))
          << "pattern: " << pattern;
      }

      // Both take the whole-term posting list, which only a field of one row
      // group has. A dictless field has no forward batch pass at all -- there
      // is no block to walk -- so it reports none and its probes are seeked one
      // at a time, which is what `ByTerms` then does.
      if (!partitioned) {
        const irs::bytes_view probes[]{irs::bytes_view{corpus.terms.front()}};
        ASSERT_EQ(nullptr, field->BatchIterator(probes));
        const auto hits = LoopProbe(*field, probes, corpus.features);
        ASSERT_EQ(1, hits.size());
        ASSERT_EQ(corpus.docs.front().size(), hits.front().docs_count);

        irs::ByTerms by_terms;
        auto& terms = by_terms.mutable_options()->terms;
        terms.emplace(MakeTerm("aaa_absent"), 1.f);
        terms.emplace(irs::bstring{corpus.terms.front()}, 2.f);
        terms.emplace(MakeTerm("zzz_absent"), 3.f);
        auto it = by_terms.CompileTermIterator(*field);
        ASSERT_NE(nullptr, it);
        ASSERT_TRUE(it->next());
        ASSERT_EQ(irs::bytes_view{corpus.terms.front()}, it->value());
        ASSERT_FALSE(it->next());

        std::vector<irs::doc_id_t> read;
        tests::ReadDocuments(*field, corpus.terms.front(),
                             [&](irs::doc_id_t doc) {
                               read.emplace_back(doc);
                               return true;
                             });
        ASSERT_EQ(corpus.docs.front().size(), read.size());
      }
    }
  }
}

// The layout is chosen by the field's own property, never by what its
// statistics happen to be: the same one-term corpus written without the
// property keeps a full dictionary, and the storage decomposition says so plane
// by plane.
TEST_F(TermDictTest, dictless_is_a_property_not_a_statistic) {
  for (const bool partitioned : {false, true}) {
    RgCorpus corpus;
    corpus.features = irs::IndexFeatures::Freq;
    corpus.doc_count = kRgSegmentDocs;
    corpus.terms = {MakeTerm("true")};
    corpus.docs = {Range(1, kRgSegmentDocs, 2)};

    const uint32_t rgs =
      partitioned ? kRgSize : irs::term_dict::kRowGroupSizeUnbounded;
    const uint32_t rg_count =
      partitioned ? (kRgSegmentDocs + kRgSize - 1) / kRgSize : 1;

    irs::MemoryDirectory plain;
    WriteRg(plain, corpus, rgs, /*dictless=*/false);
    auto opened_plain = Open<irs::term_dict::FieldReader>(plain);
    const auto* plain_field = opened_plain.Field();
    ASSERT_NE(nullptr, plain_field);
    const auto plain_info = plain_field->StorageInfo(/*walk=*/true);
    ASSERT_EQ("VAR", plain_info.layout);
    ASSERT_EQ(1, plain_info.block_count);
    ASSERT_NE(0, plain_info.blocks_size);
    ASSERT_NE(0, plain_info.separators_size);
    ASSERT_EQ(1, plain_info.terms);
    ASSERT_EQ(rg_count, plain_info.row_groups);
    ASSERT_EQ(rg_count, plain_info.rg_lists);
    ASSERT_EQ(rg_count, plain_info.max_rg_per_term);
    ASSERT_EQ(partitioned, plain_info.partitioned);

    irs::MemoryDirectory dictless;
    WriteRg(dictless, corpus, rgs, /*dictless=*/true);
    auto opened_dictless = Open<irs::term_dict::FieldReader>(dictless);
    const auto* dictless_field = opened_dictless.Field();
    ASSERT_NE(nullptr, dictless_field);
    const auto info = dictless_field->StorageInfo(/*walk=*/false);
    ASSERT_EQ("SINGLE", info.layout);
    // Zero dictionary content: the field is its term.
    ASSERT_EQ(0, info.block_count);
    ASSERT_EQ(0, info.blocks_size);
    ASSERT_EQ(0, info.separators_size);
    ASSERT_EQ(0, info.fsst_table_size);
    ASSERT_FALSE(info.fsst);
    // The rg-list statistics need no walk here -- there is one term.
    ASSERT_TRUE(info.walked);
    ASSERT_EQ(1, info.terms);
    ASSERT_EQ(rg_count, info.row_groups);
    ASSERT_EQ(rg_count, info.rg_lists);
    ASSERT_EQ(rg_count, info.max_rg_per_term);
    ASSERT_EQ(partitioned, info.partitioned);

    // Same answers from both, which is what "the layout is invisible" means.
    ASSERT_EQ(plain_field->docs_count(), dictless_field->docs_count());
    ASSERT_EQ((plain_field->min)(), (dictless_field->min)());
    ASSERT_EQ((plain_field->max)(), (dictless_field->max)());
    ASSERT_EQ(
      tests::TermStats(*plain_field, corpus.terms.front()).docs_count,
      tests::TermStats(*dictless_field, corpus.terms.front()).docs_count);
    for (uint32_t rg = 0; rg != rg_count; ++rg) {
      std::vector<irs::doc_id_t> want;
      tests::RowGroupReadDocuments(*plain_field, corpus.terms.front(), rg,
                                   [&](irs::doc_id_t doc) {
                                     want.emplace_back(doc);
                                     return true;
                                   });
      std::vector<irs::doc_id_t> got;
      tests::RowGroupReadDocuments(*dictless_field, corpus.terms.front(), rg,
                                   [&](irs::doc_id_t doc) {
                                     got.emplace_back(doc);
                                     return true;
                                   });
      ASSERT_EQ(want, got) << "rg: " << rg;
      ASSERT_FALSE(want.empty());
    }
  }
}

// The walked rg-list statistics of a many-term field, against a full scan of
// the dictionary as the oracle.
TEST_F(TermDictTest, storage_info_rg_lists) {
  RgCorpus corpus;
  corpus.features = irs::IndexFeatures::Freq;
  corpus.doc_count = kRgSegmentDocs;
  // Terms alternate between one document and a stride that spans every row
  // group, so the rg-list lengths are not all equal. Zero-padded names keep the
  // corpus sorted, which the writer requires.
  for (uint32_t i = 0; i != 64; ++i) {
    corpus.terms.emplace_back(
      MakeTerm(absl::StrCat("term_", absl::Dec(i, absl::kZeroPad4))));
    corpus.docs.emplace_back(i % 2 == 0
                               ? Range(1 + i, 2 + i, 1)
                               : Range(1 + (i % 5), kRgSegmentDocs, 7));
  }

  irs::MemoryDirectory dir;
  WriteRg(dir, corpus, kRgSize);
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  uint64_t want_lists = 0;
  uint32_t want_max = 0;
  for (const auto& docs : corpus.docs) {
    std::set<uint32_t> groups;
    for (const auto& [doc, freq] : docs) {
      groups.insert((doc - irs::doc_limits::min()) / kRgSize);
    }
    want_lists += groups.size();
    want_max = std::max(want_max, static_cast<uint32_t>(groups.size()));
  }

  const auto info = field->StorageInfo(/*walk=*/true);
  // Every term is `term_` plus four digits, so the field is uniform width and
  // its one block is fixed stride.
  ASSERT_EQ("FIXED_STRIDE", info.layout);
  ASSERT_TRUE(info.walked);
  ASSERT_EQ(corpus.terms.size(), info.terms);
  ASSERT_EQ(want_lists, info.rg_lists);
  ASSERT_EQ(want_max, info.max_rg_per_term);

  // Without the walk the field reports no rg-list statistics at all, rather
  // than reporting wrong ones.
  const auto unwalked = field->StorageInfo(/*walk=*/false);
  ASSERT_FALSE(unwalked.walked);
  ASSERT_EQ(0, unwalked.rg_lists);
  ASSERT_EQ(0, unwalked.max_rg_per_term);
  ASSERT_EQ(info.blocks_size, unwalked.blocks_size);
  ASSERT_EQ(info.separators_size, unwalked.separators_size);
}

// The fixed-stride leaf: a sorted array of one raw key suffix per entry behind
// the block's shared prefix, with no front-coding varint anywhere. Selection is
// per block: a short uniform width takes the layout unconditionally, a wider
// one only where it does not cost more bytes than front coding. So a field of
// uniform-width terms is fixed stride end to end, a field of several width
// groups is fixed stride inside each group and front coded across the seams,
// and only a wide shape front coding compresses better stays `Var`.
TEST_F(TermDictTest, fixed_stride) {
  // Wide alphabet, long keys: neighbouring terms share almost nothing, so front
  // coding has nothing to remove and pays an entry header per term.
  static constexpr std::string_view kWide =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-";
  const auto features = irs::IndexFeatures::Freq;

  {
    Corpus corpus;
    corpus.terms = MakeWidthTerms(6000, 45, kWide, 7);
    corpus.docs = MakeDocs(64, 3);
    corpus.features = features;

    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, {});
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    ASSERT_EQ("FIXED_STRIDE", field->StorageInfo(/*walk=*/false).layout);
    ASSERT_LT(1, field->StorageInfo(/*walk=*/false).block_count);
    AssertDictReads(*field, corpus.terms, features);
  }

  // Few enough terms that one block spans the whole alphabet: the block's
  // shared prefix is empty and the stride is the whole key.
  {
    Corpus corpus;
    corpus.terms = MakeWidthTerms(150, 45, kWide, 19);
    corpus.docs = MakeDocs(64, 3);
    corpus.features = features;

    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, {});
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    ASSERT_EQ("FIXED_STRIDE", field->StorageInfo(/*walk=*/false).layout);
    AssertDictReads(*field, corpus.terms, features);
  }

  // A narrow alphabet over a short key: neighbours share most of it and what
  // is left compresses, so front coding plus FSST is the smaller encoding and
  // the byte comparison refuses the stride array.
  {
    Corpus corpus;
    corpus.terms = MakeWidthTerms(20000, 10, "0123456789", 31);
    corpus.docs = MakeDocs(64, 3);
    corpus.features = features;

    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, {});
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    const auto info = field->StorageInfo(/*walk=*/true);
    ASSERT_EQ("VAR", info.layout);
    ASSERT_LT(1, info.block_count);
    ASSERT_LT(info.fixed_blocks, info.var_blocks);
    AssertDictReads(*field, corpus.terms, features);
  }

  // The same shape above the width bound is where the byte comparison still
  // decides, and it refuses: a long shared lead plus a narrow alphabet leaves
  // front coding a few compressible bytes per entry against a whole stride.
  {
    Corpus corpus;
    corpus.terms = MakeWidthTerms(20000, 10, "0123456789", 31,
                                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    corpus.docs = MakeDocs(64, 3);
    corpus.features = features;

    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, {});
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    const auto info = field->StorageInfo(/*walk=*/true);
    ASSERT_EQ("VAR", info.layout);
    ASSERT_LT(1, info.block_count);
    ASSERT_LT(info.fixed_blocks, info.var_blocks);
    AssertDictReads(*field, corpus.terms, features);
  }

  // Three width groups, each many blocks wide: the blocks inside a group are
  // fixed stride, the blocks that straddle a group boundary are not, and a
  // reader crosses both seams in either direction. `Var` at the field level is
  // exactly that mixture -- the two counters are what tells them apart, and
  // they are the only way to see that a numeric field, whose precision ladder
  // is this shape, has its fast leaf engaged.
  {
    Corpus corpus;
    for (const auto& group : {MakeWidthTerms(1500, 40, kWide, 11, "a"),
                              MakeWidthTerms(1500, 48, kWide, 13, "b"),
                              MakeWidthTerms(1500, 56, kWide, 17, "c")}) {
      corpus.terms.insert(corpus.terms.end(), group.begin(), group.end());
    }
    std::ranges::sort(corpus.terms);
    corpus.docs = MakeDocs(64, 3);
    corpus.features = features;

    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, {});
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    const auto info = field->StorageInfo(/*walk=*/true);
    ASSERT_EQ("VAR", info.layout);
    ASSERT_LT(3, info.block_count);
    // Two seams, so at most one straddling block each; everything else is one
    // width and therefore fixed.
    ASSERT_GE(2, info.var_blocks);
    ASSERT_LT(3, info.fixed_blocks);
    AssertDictReads(*field, corpus.terms, features);
  }

  // Restart density and block size drive the payload restart table, which is
  // the one offset array a fixed-stride block still carries.
  for (uint32_t interval : {1u, 2u, 4u, 16u}) {
    for (uint32_t target : {256u, 4096u}) {
      Corpus corpus;
      corpus.terms = MakeWidthTerms(2000, 45, kWide, 23);
      corpus.docs = MakeDocs(40, 5);
      corpus.features = features;

      irs::term_dict::WriterOptions options;
      options.restart_interval = interval;
      options.block_byte_target = target;

      irs::MemoryDirectory dir;
      WriteV2(dir, corpus, options);
      auto opened = Open<irs::term_dict::FieldReader>(dir);
      const auto* field = opened.Field();
      ASSERT_NE(nullptr, field);
      ASSERT_EQ("FIXED_STRIDE", field->StorageInfo(/*walk=*/false).layout)
        << "interval: " << interval << " target: " << target;
      AssertDictReads(*field, corpus.terms, features);
    }
  }

  // A block of a single entry is never fixed stride: its shared prefix would be
  // its whole key. The two-term field is the smallest one that can be.
  {
    Corpus corpus;
    corpus.terms.emplace_back(MakeTerm("aa"));
    corpus.terms.emplace_back(MakeTerm("ab"));
    corpus.docs = MakeDocs(8, 1);
    corpus.features = features;

    irs::MemoryDirectory dir;
    WriteV2(dir, corpus, {});
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    AssertDictReads(*field, corpus.terms, features);
  }
}

// The same leaf under row-group partitioning: the layout changes what a key
// costs to read and nothing about what a term's posting lists are.
TEST_F(TermDictTest, fixed_stride_row_groups) {
  static constexpr std::string_view kWide =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-";
  RgCorpus corpus;
  corpus.features = irs::IndexFeatures::Freq;
  corpus.doc_count = kRgSegmentDocs;
  corpus.terms = MakeWidthTerms(600, 45, kWide, 29);
  ASSERT_LT(64, corpus.terms.size());
  for (size_t i = 0; i != corpus.terms.size(); ++i) {
    corpus.docs.emplace_back(i % 3 == 0
                               ? Range(1 + i, 2 + i, 1)
                               : Range(1 + (i % 5), kRgSegmentDocs, 11));
  }

  irs::MemoryDirectory dir;
  WriteRg(dir, corpus, kRgSize);
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);
  ASSERT_EQ("FIXED_STRIDE", field->StorageInfo(/*walk=*/false).layout);

  const auto layout = field->RowGroups();
  ASSERT_LT(1, layout.count);
  AssertDictReads(*field, corpus.terms, corpus.features, /*partitioned=*/true);

  for (size_t i = 0; i != corpus.terms.size(); ++i) {
    const auto expected =
      ExpectedHits(corpus.docs[i], corpus.features, kRgSize);
    std::vector<RgHit> got;
    auto it = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(it->seek(corpus.terms[i]));
    for (uint32_t rg = 0; rg != layout.count; ++rg) {
      auto postings = it->RowGroupPostings(corpus.features, rg);
      if (!postings) {
        continue;
      }
      ReadHits(*postings, corpus.features, layout, rg, /*decompose=*/false,
               got);
    }
    ASSERT_EQ(expected, got) << "term " << i;
  }
}

namespace {

struct MaxFreqScorer : irs::ScorerBase<void> {
  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::Freq;
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext&) const final {
    return irs::ScoreFunction::Default();
  }

  irs::WandWriter::ptr prepare_wand_writer(size_t max_levels) const final {
    return std::make_unique<irs::FreqNormWriter<irs::kWandTagMaxFreq>>(
      max_levels);
  }

  irs::WandSource::ptr prepare_wand_source() const final {
    return std::make_unique<irs::FreqNormSource<irs::kWandTagFreq>>();
  }
};

void WriteRgScored(irs::Directory& dir, const RgCorpus& corpus,
                   uint32_t row_group_size, const irs::Scorer& scorer) {
  auto codec = irs::formats::Get(kCodec);
  ASSERT_NE(nullptr, codec);

  irs::FieldMeta meta;
  meta.id = kFieldId;
  meta.index_features = corpus.features;

  irs::FlushState state{
    .dir = &dir,
    .norms = &irs::SubReader::empty(),
    .name = kSegment,
    .scorer = &scorer,
    .doc_count = corpus.doc_count,
    .index_features = corpus.features,
  };

  RgTermIterator terms{corpus};
  tests::MockTermReader reader{terms, meta, corpus.terms.front(),
                               corpus.terms.back()};

  irs::IdxWriter idx{dir, kSegment, ::sdb::DuckDBEngine::Instance().instance()};
  irs::term_dict::WriterOptions options;
  options.row_group_size = row_group_size;
  irs::term_dict::FieldWriter writer{
    codec->get_postings_writer(/*compaction=*/false,
                               irs::IResourceManager::gNoop),
    /*compaction=*/false, irs::IResourceManager::gNoop};
  writer.SetIdxWriter(idx);
  writer.SetOptions(options);
  writer.prepare(state);
  writer.write(reader);
  writer.end();
  idx.Commit();
}

}  // namespace

// A scored field's records over a partitioned corpus: the same terms written
// whole and split have the same term-wide statistics, and the split ones cut
// into runs -- including terms whose runs are a mix of inlined df == 1 lists
// and real ones, which is where the run-cut logic is hardest. The record itself
// carries no term-level score bound: the bounds a scored walk reads sit at the
// head of every run in `.doc`.
TEST_F(TermDictTest, multi_run_scored_records) {
  const auto corpus = MakeRgCorpus(irs::IndexFeatures::Freq);
  const MaxFreqScorer scorer;

  irs::MemoryDirectory whole_dir;
  irs::MemoryDirectory split_dir;
  WriteRgScored(whole_dir, corpus, irs::term_dict::kRowGroupSizeUnbounded,
                scorer);
  WriteRgScored(split_dir, corpus, kRgSize, scorer);

  auto whole_opened = Open<irs::term_dict::FieldReader>(whole_dir);
  auto split_opened = Open<irs::term_dict::FieldReader>(split_dir);
  const auto* whole = whole_opened.Field();
  const auto* split = split_opened.Field();
  ASSERT_NE(nullptr, whole);
  ASSERT_NE(nullptr, split);
  ASSERT_TRUE(whole->has_scorer(0));
  ASSERT_TRUE(split->has_scorer(0));

  auto whole_it = whole->iterator(irs::SeekMode::NORMAL);
  auto split_it = split->iterator(irs::SeekMode::NORMAL);
  size_t checked = 0;
  size_t multi = 0;
  size_t multi_with_singleton = 0;
  while (split_it->next()) {
    ASSERT_TRUE(whole_it->next());
    const auto whole_cookie = whole_it->cookie();
    const auto split_cookie = split_it->cookie();
    ASSERT_EQ(1, whole_cookie.rgs.size());
    // Computed from the corpus, not from the other index: both sides owe the
    // same counts however the terms are cut.
    uint32_t expected_freq = 0;
    for (const auto [doc, freq] : corpus.docs[checked]) {
      expected_freq += freq;
    }
    const auto expected_docs =
      static_cast<uint32_t>(corpus.docs[checked].size());
    ASSERT_EQ(expected_docs, whole_cookie.stats.docs_count);
    ASSERT_EQ(expected_freq, whole_cookie.stats.freq);
    ASSERT_EQ(expected_docs, split_cookie.stats.docs_count)
      << "term " << checked;
    ASSERT_EQ(expected_freq, split_cookie.stats.freq) << "term " << checked;
    uint32_t run_docs = 0;
    uint32_t run_freq = 0;
    for (const auto& group : split_cookie.rgs) {
      run_docs += group.docs_count;
      run_freq += group.freq;
    }
    ASSERT_EQ(expected_docs, run_docs) << "term " << checked;
    ASSERT_EQ(expected_freq, run_freq) << "term " << checked;
    if (split_cookie.rgs.size() > 1) {
      ++multi;
      for (const auto& group : split_cookie.rgs) {
        if (group.docs_count == 1) {
          ++multi_with_singleton;
          break;
        }
      }
    }
    ++checked;
  }
  ASSERT_FALSE(whole_it->next());
  ASSERT_EQ(corpus.terms.size(), checked);
  ASSERT_LT(0, multi);
  ASSERT_LT(0, multi_with_singleton);
}

namespace {

// Terms whose byte widths differ by an order of magnitude between the halves
// of the dictionary: a byte-balanced cut lands nowhere near the middle TERM,
// so a split that is right on this corpus is one that counted terms.
std::vector<irs::bstring> MakeLopsidedTerms(size_t count) {
  std::vector<irs::bstring> terms;
  terms.reserve(count);
  const std::string fat(200, 'x');
  for (size_t i = 0; i != count; ++i) {
    terms.emplace_back(MakeTerm(absl::StrFormat(
      "term_%06d_%s", i, i < count / 2 ? std::string_view{fat} : "")));
  }
  std::ranges::sort(terms);
  return terms;
}

std::vector<irs::bstring> WalkRange(const irs::TermReader& field,
                                    const irs::TermRange& range) {
  std::vector<irs::bstring> out;
  auto it = field.RangeIterator(range);
  EXPECT_NE(nullptr, it);
  while (it->next()) {
    out.emplace_back(it->value());
  }
  EXPECT_FALSE(it->next());
  return out;
}

// Every range's terms, in range order -- and the invariants that make the
// ranges a partition: they abut, the first starts before the field and the
// last runs past it.
std::vector<std::vector<irs::bstring>> WalkRanges(
  const irs::TermReader& field, std::span<const irs::TermRange> ranges) {
  std::vector<std::vector<irs::bstring>> out;
  out.reserve(ranges.size());
  for (size_t i = 0; i != ranges.size(); ++i) {
    EXPECT_EQ(i == 0, ranges[i].lo.empty());
    EXPECT_EQ(i + 1 == ranges.size(), ranges[i].hi.empty());
    if (i != 0) {
      EXPECT_EQ(ranges[i - 1].hi, ranges[i].lo);
    }
    out.push_back(WalkRange(field, ranges[i]));
  }
  return out;
}

}  // namespace

// The ranges of a field partition its terms: whatever the requested share
// count, concatenating the range walks reproduces the one full walk exactly,
// in order, with nothing dropped and nothing repeated.
TEST_F(TermDictTest, term_ranges_partition_the_walk) {
  Corpus corpus;
  corpus.terms = MakeLopsidedTerms(4000);
  corpus.docs = MakeDocs(32, 1);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);
  const auto blocks = field->StorageInfo(/*walk=*/false).block_count;
  ASSERT_LT(8, blocks);

  // As many ranges as blocks: each one is exactly a block, which is what the
  // per-block term counts below are read off.
  std::vector<irs::TermRange> per_block(blocks);
  ASSERT_EQ(blocks, field->TermRanges(per_block));
  const auto block_terms = WalkRanges(*field, per_block);
  size_t widest_block = 0;
  size_t total = 0;
  for (const auto& terms : block_terms) {
    ASSERT_FALSE(terms.empty());
    widest_block = std::max(widest_block, terms.size());
    total += terms.size();
  }
  ASSERT_EQ(corpus.terms.size(), total);

  for (const size_t shares :
       {size_t{1}, size_t{2}, size_t{3}, size_t{7}, size_t{64},
        size_t{blocks} + 1, size_t{blocks} * 4}) {
    std::vector<irs::TermRange> ranges(shares);
    const auto count = field->TermRanges(ranges);
    ASSERT_EQ(std::min<size_t>(shares, blocks), count);
    ranges.resize(count);

    std::vector<irs::bstring> walked;
    size_t smallest = corpus.terms.size();
    size_t largest = 0;
    for (const auto& terms : WalkRanges(*field, ranges)) {
      ASSERT_FALSE(terms.empty());
      smallest = std::min(smallest, terms.size());
      largest = std::max(largest, terms.size());
      walked.insert(walked.end(), terms.begin(), terms.end());
    }
    ASSERT_EQ(corpus.terms, walked) << "shares " << shares;

    // Balanced by TERMS: a block is the granularity of a cut, so a share can
    // miss the equal split by at most the widest block -- which a cut placed
    // by bytes would not respect on this corpus.
    const auto fair = corpus.terms.size() / count;
    EXPECT_LE(largest, fair + widest_block) << "shares " << shares;
    EXPECT_GE(smallest + widest_block, fair) << "shares " << shares;
  }
}

// The range bounds are honoured verbatim, not rounded to the block boundaries
// they usually are: a walk of an arbitrary [lo, hi) is exactly the terms of
// the full walk inside it.
TEST_F(TermDictTest, term_range_bounds_are_exact) {
  Corpus corpus;
  corpus.terms = MakeTerms(2000, "bounded_");
  corpus.docs = MakeDocs(16, 1);

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});
  auto opened = Open<irs::term_dict::FieldReader>(dir);
  const auto* field = opened.Field();
  ASSERT_NE(nullptr, field);

  const auto& terms = corpus.terms;
  const std::pair<size_t, size_t> spans[] = {
    {0, terms.size()}, {0, 1},      {1, 2},
    {7, 999},          {999, 1000}, {terms.size() - 1, terms.size()},
  };
  for (const auto [from, to] : spans) {
    const irs::TermRange range{
      .lo = from == 0 ? irs::bytes_view{} : irs::bytes_view{terms[from]},
      .hi = to == terms.size() ? irs::bytes_view{} : irs::bytes_view{terms[to]},
    };
    const std::vector<irs::bstring> expected{terms.begin() + from,
                                             terms.begin() + to};
    EXPECT_EQ(expected, WalkRange(*field, range)) << from << ".." << to;
  }

  // Ranges that hold nothing: one past the field's last term, and one whose
  // bounds meet.
  EXPECT_TRUE(WalkRange(*field, {MakeTerm("zzzz"), {}}).empty());
  EXPECT_TRUE(WalkRange(*field, {terms[10], terms[10]}).empty());
  EXPECT_TRUE(WalkRange(*field, {{}, terms.front()}).empty());

  // Bounds that fall between terms cut where the terms do.
  const auto between_lo = Between(terms[10]);
  const auto between_hi = Between(terms[20]);
  const std::vector<irs::bstring> expected{terms.begin() + 11,
                                           terms.begin() + 21};
  EXPECT_EQ(expected, WalkRange(*field, {between_lo, between_hi}));
}

// A `Single` field has one term and no block directory, so it is one range by
// construction -- the whole field, exactly what its own walk yields.
TEST_F(TermDictTest, term_ranges_single_field) {
  for (const auto term : {std::string_view{"true"}, std::string_view{}}) {
    RgCorpus corpus;
    corpus.features = irs::IndexFeatures::Freq;
    corpus.doc_count = kRgSegmentDocs;
    corpus.terms = {MakeTerm(term)};
    corpus.docs = {Range(1, kRgSegmentDocs, 2)};

    irs::MemoryDirectory dir;
    WriteRg(dir, corpus, kRgSize, /*dictless=*/true);
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    ASSERT_EQ("SINGLE", field->StorageInfo(/*walk=*/false).layout);

    std::vector<irs::TermRange> ranges(8);
    ASSERT_EQ(1, field->TermRanges(ranges));
    EXPECT_TRUE(ranges.front().lo.empty());
    EXPECT_TRUE(ranges.front().hi.empty());
    EXPECT_EQ(corpus.terms, WalkRange(*field, ranges.front()));

    // And it still answers a range that excludes it with nothing.
    EXPECT_TRUE(WalkRange(*field, {MakeTerm("zzz"), {}}).empty());
  }
}

// A field with no terms has no ranges to claim.
TEST_F(TermDictTest, term_ranges_of_an_empty_reader) {
  const irs::EmptyTermReader empty{42};
  std::vector<irs::TermRange> ranges(4);
  EXPECT_EQ(0, empty.TermRanges(ranges));
}

// The field's block directory, separators and separator index are built on
// first use and published under a flag the readers test outside the lock. Six
// entry points reach that publication, and the row-group scheduler drives all
// six from N workers against one reader, so a field's first use is a genuine
// N-way race. Each round opens a fresh reader, so the race is re-run from
// scratch every time rather than once.
TEST_F(TermDictTest, concurrent_first_use) {
  Corpus corpus;
  corpus.terms = MakeWordyTerms(20000);
  corpus.docs = MakeDocs(64, 1);
  corpus.features = irs::IndexFeatures::Freq;

  irs::MemoryDirectory dir;
  WriteV2(dir, corpus, {});

  // Expectations from a reader of its own, single-threaded: no racing walk is
  // ever its own oracle.
  const irs::bstring& first = corpus.terms.front();
  const irs::bstring& last = corpus.terms.back();
  const irs::bstring& middle = corpus.terms[corpus.terms.size() / 2];
  const std::vector<irs::bytes_view> probes{first, middle, last};
  uint32_t expected_docs = 0;
  size_t expected_ranges = 0;
  size_t expected_read_documents = 0;
  {
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);
    expected_docs = tests::TermStats(*field, middle).docs_count;
    ASSERT_NE(0, expected_docs);
    std::vector<irs::TermRange> ranges(8);
    expected_ranges = field->TermRanges(ranges);
    ASSERT_LT(1, expected_ranges);
    tests::RowGroupReadDocuments(*field, middle, 0, [&](irs::doc_id_t) {
      return ++expected_read_documents, true;
    });
    ASSERT_EQ(expected_docs, expected_read_documents);
  }

  constexpr size_t kThreads = 8;
  constexpr size_t kRounds = 64;
  for (size_t round = 0; round != kRounds; ++round) {
    auto opened = Open<irs::term_dict::FieldReader>(dir);
    const auto* field = opened.Field();
    ASSERT_NE(nullptr, field);

    // Spin the workers up to the line first, so they enter the six entry
    // points together rather than in creation order.
    std::atomic_size_t at_line{0};
    std::atomic_bool go{false};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (size_t i = 0; i != kThreads; ++i) {
      workers.emplace_back([&, i] {
        at_line.fetch_add(1, std::memory_order_relaxed);
        while (!go.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        switch (i) {
          case 0: {
            auto it = field->iterator(irs::SeekMode::NORMAL);
            ASSERT_NE(nullptr, it);
            EXPECT_TRUE(it->seek(middle));
            EXPECT_EQ(irs::bytes_view{middle}, it->value());
            break;
          }
          case 1: {
            std::vector<irs::TermRange> ranges(8);
            EXPECT_EQ(expected_ranges, field->TermRanges(ranges));
            break;
          }
          case 2: {
            std::vector<irs::TermRange> ranges(8);
            const auto count = field->TermRanges(ranges);
            ASSERT_NE(0, count);
            auto it = field->RangeIterator(ranges.front());
            ASSERT_NE(nullptr, it);
            EXPECT_TRUE(it->next());
            EXPECT_EQ(irs::bytes_view{first}, it->value());
            break;
          }
          case 3: {
            auto it = field->BatchIterator(probes);
            ASSERT_NE(nullptr, it);
            EXPECT_EQ(probes.size(), WalkTerms(std::move(it)).size());
            break;
          }
          case 4: {
            EXPECT_EQ(expected_docs,
                      tests::TermStats(*field, middle).docs_count);
            break;
          }
          case 5: {
            size_t seen = 0;
            tests::RowGroupReadDocuments(
              *field, middle, 0, [&](irs::doc_id_t) { return ++seen, true; });
            EXPECT_EQ(expected_read_documents, seen);
            break;
          }
          case 6: {
            auto it = field->iterator(irs::SeekMode::RandomOnly);
            ASSERT_NE(nullptr, it);
            EXPECT_TRUE(it->seek(last));
            EXPECT_EQ(irs::bytes_view{last}, it->value());
            break;
          }
          default: {
            auto it = field->iterator(irs::SeekMode::NORMAL);
            ASSERT_NE(nullptr, it);
            EXPECT_TRUE(it->next());
            EXPECT_EQ(irs::bytes_view{first}, it->value());
            break;
          }
        }
      });
    }
    while (at_line.load(std::memory_order_relaxed) != kThreads) {
      std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);
    for (auto& worker : workers) {
      worker.join();
    }
    ASSERT_FALSE(::testing::Test::HasFailure()) << "round " << round;
  }
}
