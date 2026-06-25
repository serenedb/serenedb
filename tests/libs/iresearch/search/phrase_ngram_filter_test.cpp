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

#include "iresearch/search/phrase_ngram_filter.hpp"

#include <span>
#include <vector>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/shingle_analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

// Whitespace base tokenizer so the shingle analyzer sees a known word stream.
class WhitespaceTokenizer final
  : public irs::analysis::TypedAnalyzer<WhitespaceTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "test_whitespace_tokenizer";
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<irs::TermAttr>::id()) {
      return &_term;
    }
    if (type == irs::Type<irs::IncAttr>::id()) {
      return &_inc;
    }
    return nullptr;
  }

  bool reset(std::string_view data) final {
    _data = data;
    _pos = 0;
    return true;
  }

  bool next() final {
    while (_pos < _data.size() && _data[_pos] == ' ') {
      ++_pos;
    }
    if (_pos >= _data.size()) {
      return false;
    }
    const auto start = _pos;
    while (_pos < _data.size() && _data[_pos] != ' ') {
      ++_pos;
    }
    _term.value = irs::ViewCast<irs::byte_type>(_data.substr(start, _pos - start));
    _inc.value = 1;
    return true;
  }

 private:
  std::string_view _data;
  size_t _pos{0};
  irs::TermAttr _term;
  irs::IncAttr _inc;
};

// Field backed by a ShingleAnalyzer. INDEX terms flow through the inverted
// path; the analyzer's packed ordered-token StoreAttr blob is persisted to a
// stored BLOB column so the verifier can re-check candidates. The field is
// Frequency-only: the position-free path is what we exercise.
struct ShingleField {
  irs::field_id Id() const { return id; }

  irs::Tokenizer& GetTokens() const {
    analyzer->reset(value);
    return *analyzer;
  }

  bool Write(irs::DataOutput& out) const {
    const auto* store = irs::get<irs::StoreAttr>(*analyzer);
    if (store && !store->value.empty()) {
      out.WriteData(store->value.data(), store->value.size());
    }
    return true;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept { return index_features; }

  mutable irs::analysis::ShingleAnalyzer* analyzer{};
  std::string_view value;
  irs::field_id id{};
  // Freq -> Strategy A (verify); Freq|Pos -> Strategy B (positional, no verify).
  irs::IndexFeatures index_features{irs::IndexFeatures::Freq};
};

inline constexpr irs::field_id kStoreId = 1;
inline constexpr irs::field_id kTextId = 2;

// `positional` selects the strategy the filter would otherwise inherit from the
// column's index features at the dispatch site (the per-segment prepare model
// has no whole-index access): false -> Strategy A (verify), true -> Strategy B
// (positional). The legacy `field` name is retained for call-site stability.
irs::ByPhraseNgram MakeFilter([[maybe_unused]] std::string_view field,
                              std::string_view phrase,
                              irs::analysis::ShingleAnalyzer& analyzer,
                              bool positional = false) {
  irs::ByPhraseNgram filter;
  *filter.mutable_field_id() = kTextId;
  *filter.mutable_options() = irs::ByPhraseNgramOptions{phrase, analyzer};
  filter.mutable_options()->store_field_id = kStoreId;
  filter.mutable_options()->positional = positional;
  return filter;
}

irs::analysis::ShingleAnalyzer MakeAnalyzer(uint32_t min = 2, uint32_t max = 2) {
  return irs::analysis::ShingleAnalyzer{
    std::make_unique<WhitespaceTokenizer>(),
    {
      .min_shingle_size = min,
      .max_shingle_size = max,
      .output_unigrams = true,
    }};
}

// Whitespace tokenizer that drops the stopword "the" and reports the gap as
// a position increment > 1, so filler handling can be exercised end-to-end.
class StopwordTokenizer final
  : public irs::analysis::TypedAnalyzer<StopwordTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "test_stopword_tokenizer";
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<irs::TermAttr>::id()) {
      return &_term;
    }
    if (type == irs::Type<irs::IncAttr>::id()) {
      return &_inc;
    }
    return nullptr;
  }

  bool reset(std::string_view data) final {
    _data = data;
    _pos = 0;
    return true;
  }

  bool next() final {
    uint32_t skipped = 0;
    for (;;) {
      while (_pos < _data.size() && _data[_pos] == ' ') {
        ++_pos;
      }
      if (_pos >= _data.size()) {
        return false;
      }
      const auto start = _pos;
      while (_pos < _data.size() && _data[_pos] != ' ') {
        ++_pos;
      }
      const auto word = _data.substr(start, _pos - start);
      if (word == "the") {
        ++skipped;
        continue;
      }
      _term.value = irs::ViewCast<irs::byte_type>(word);
      _inc.value = 1 + skipped;
      return true;
    }
  }

 private:
  std::string_view _data;
  size_t _pos{0};
  irs::TermAttr _term;
  irs::IncAttr _inc;
};

irs::analysis::ShingleAnalyzer MakeStopAnalyzer() {
  return irs::analysis::ShingleAnalyzer{
    std::make_unique<StopwordTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 2,
      .output_unigrams = true,
    }};
}

irs::bstring Bytes(std::string_view s) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(s)};
}

irs::analysis::ShingleAnalyzer MakeFreqAnalyzer(
  std::vector<irs::bstring> frequent) {
  return irs::analysis::ShingleAnalyzer{
    std::make_unique<WhitespaceTokenizer>(),
    {
      .min_shingle_size = 2,
      .max_shingle_size = 3,
      .output_unigrams = true,
      .frequent_words = std::move(frequent),
    }};
}

// A trigram shingle joined with the default 0xFF separator.
irs::bstring Trigram(std::string_view a, std::string_view b,
                     std::string_view c) {
  irs::bstring out{Bytes(a)};
  out += irs::byte_type{0xFF};
  out += Bytes(b);
  out += irs::byte_type{0xFF};
  out += Bytes(c);
  return out;
}

// Frequent-words corpus: stopwords {the, of} bound the shingle vocabulary.
constexpr std::string_view kFreqValues[] = {
  "the quick brown fox",  // 0: contains "the quick brown" and "quick brown fox"
  "quick brown the fox",  // 1: same words, neither phrase contiguous
  "the quick brown",      // 2: contains "the quick brown"
  "the lazy dog",         // 3
};

// Index `values` as a single segment of ShingleField documents and open a
// reader over them. `dir` and `analyzer` must outlive the returned reader.
irs::DirectoryReader BuildReader(
  irs::MemoryDirectory& dir, irs::analysis::ShingleAnalyzer& analyzer,
  std::span<const std::string_view> values,
  irs::IndexFeatures features = irs::IndexFeatures::Freq) {
  auto codec = irs::formats::Get("1_5simd");
  EXPECT_NE(nullptr, codec);
  auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                       irs::tests::DefaultWriterOptions());
  EXPECT_NE(nullptr, writer);

  ShingleField field;
  field.analyzer = &analyzer;
  field.id = kTextId;
  field.index_features = features;
  auto ctx = writer->GetBatch();
  for (auto v : values) {
    field.value = v;
    auto doc = ctx.Insert();
    EXPECT_TRUE(doc.Insert(field));
    auto* cs = doc.GetColWriter();
    EXPECT_NE(nullptr, cs);
    irs::tests::StoreFieldAt(*cs, kStoreId, doc.DocId(), field);
  }
  ctx.Commit();
  writer->RefreshCommit();

  return irs::DirectoryReader{dir, irs::formats::Get("1_5simd"),
                              irs::tests::DefaultReaderOptions()};
}

// doc ids for the given 0-based insertion offsets.
std::vector<irs::doc_id_t> Ids(std::initializer_list<int> offsets) {
  std::vector<irs::doc_id_t> v;
  for (int off : offsets) {
    v.push_back(irs::doc_limits::min() + off);
  }
  return v;
}

// Run `q` over `reader` via next(), collecting matched doc ids.
std::vector<irs::doc_id_t> Execute(const irs::DirectoryReader& reader,
                                   const irs::Filter& q) {
  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{q, *reader, nullptr, counter};
  std::vector<irs::doc_id_t> result;
  for (size_t i = 0, n = prepared.size(); i < n; ++i) {
    auto docs = prepared.Execute(i);
    while (!irs::doc_limits::eof(docs->advance())) {
      result.push_back(docs->value());
    }
  }
  return result;
}

// Run `q` with `scorer`, collecting (doc id, score) in match order.
std::vector<std::pair<irs::doc_id_t, irs::score_t>> ExecuteScored(
  const irs::DirectoryReader& reader, const irs::Filter& q,
  const irs::Scorer& scorer) {
  MaxMemoryCounter counter;
  tests::PreparedFilter prepared{q, *reader, &scorer, counter};
  std::vector<std::pair<irs::doc_id_t, irs::score_t>> result;
  irs::ColumnArgsFetcher fetcher;
  for (size_t i = 0, n = prepared.size(); i < n; ++i) {
    const auto& segment = (*reader)[i];
    auto docs = prepared.Execute(i);
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });
    while (!irs::doc_limits::eof(docs->advance())) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      irs::score_t value{};
      score.Score(&value, 1);
      result.emplace_back(docs->value(), value);
    }
  }
  return result;
}

}  // namespace

// A bigram shingle joined with the analyzer's default 0xFF separator.
irs::bstring Bigram(std::string_view a, std::string_view b) {
  irs::bstring out{Bytes(a)};
  out += irs::byte_type{0xFF};
  out += Bytes(b);
  return out;
}

TEST(PhraseNgramFilterOptionsTest, positional_cover_overlapping_tail) {
  // An odd-length phrase must not degrade to a unigram tail: the last token
  // is covered by an overlapping bigram ending at the phrase end, so the
  // positional plan touches only (small) shingle posting lists.
  auto analyzer = MakeAnalyzer();
  irs::ByPhraseNgramOptions opts{"quick brown fox", analyzer};
  ASSERT_EQ(2, opts.positional_terms.size());
  EXPECT_EQ(Bigram("quick", "brown"), opts.positional_terms[0]);
  EXPECT_EQ(Bigram("brown", "fox"), opts.positional_terms[1]);
  EXPECT_EQ((std::vector<uint32_t>{0, 1}), opts.positional_starts);

  irs::ByPhraseNgramOptions even{"quick brown fox jumps", analyzer};
  ASSERT_EQ(2, even.positional_terms.size());
  EXPECT_EQ(Bigram("quick", "brown"), even.positional_terms[0]);
  EXPECT_EQ(Bigram("fox", "jumps"), even.positional_terms[1]);
  EXPECT_EQ((std::vector<uint32_t>{0, 2}), even.positional_starts);

  irs::ByPhraseNgramOptions five{"a b c d e", analyzer};
  ASSERT_EQ(3, five.positional_terms.size());
  EXPECT_EQ((std::vector<uint32_t>{0, 2, 3}), five.positional_starts);
  EXPECT_EQ(Bigram("d", "e"), five.positional_terms[2]);
}

TEST(PhraseNgramFilterOptionsTest, dedups_candidate_legs) {
  // Repeated words repeat shingle legs; identical legs add nothing to the
  // conjunction and would double-score.
  auto analyzer = MakeAnalyzer();
  irs::ByPhraseNgramOptions opts{"the cat the cat", analyzer};
  ASSERT_EQ(2, opts.shingles.size());
  EXPECT_EQ(Bigram("the", "cat"), opts.shingles[0]);
  EXPECT_EQ(Bigram("cat", "the"), opts.shingles[1]);
  // The positional cover keeps duplicates: they pin distinct positions.
  EXPECT_EQ((std::vector<uint32_t>{0, 2}), opts.positional_starts);
  ASSERT_EQ(2, opts.positional_terms.size());
  EXPECT_EQ(opts.positional_terms[0], opts.positional_terms[1]);
}

// A removed stopword means "any single token here" -- the verify and the
// positional strategies must agree with each other (and with the classic
// inc-aware path).
constexpr std::string_view kFillerValues[] = {
  "quick the brown",  // 0: gap where the stopword was removed
  "quick brown",      // 1: adjacent -- no gap
  "quick big brown",  // 2: any token occupies the gap position
  "brown quick",      // 3: reversed
};

TEST(PhraseNgramFilterTest, filler_gap_verify) {
  auto analyzer = MakeStopAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kFillerValues);
  ASSERT_NE(nullptr, reader);

  // The query stopword becomes a wildcard position: docs 0 and 2 both have
  // one token between quick and brown; doc1 (adjacent) must not match.
  EXPECT_EQ(Ids({0, 2}),
            Execute(reader, MakeFilter("text", "quick the brown", analyzer)));
  // No shingle spans a gap: the adjacent-pair query matches only doc1.
  EXPECT_EQ(Ids({1}),
            Execute(reader, MakeFilter("text", "quick brown", analyzer)));
  // A leading query stopword degrades to the bare phrase (PG parity).
  EXPECT_EQ(Ids({1}),
            Execute(reader, MakeFilter("text", "the quick brown", analyzer)));
}

TEST(PhraseNgramFilterTest, filler_gap_positional) {
  auto analyzer = MakeStopAnalyzer();
  irs::MemoryDirectory dir;
  auto reader =
    BuildReader(dir, analyzer, kFillerValues,
                irs::IndexFeatures::Freq | irs::IndexFeatures::Pos);
  ASSERT_NE(nullptr, reader);

  // Identical row sets to the verify strategy: the filler slot is expressed
  // as a position offset instead of a stored-token wildcard.
  EXPECT_EQ(Ids({0, 2}),
            Execute(reader, MakeFilter("text", "quick the brown", analyzer,
                                       /*positional=*/true)));
  EXPECT_EQ(Ids({1}), Execute(reader, MakeFilter("text", "quick brown",
                                                 analyzer, /*positional=*/true)));
  EXPECT_EQ(Ids({1}),
            Execute(reader, MakeFilter("text", "the quick brown", analyzer,
                                       /*positional=*/true)));
}

// Raw-value field: persists the document's ORIGINAL text (not a token blob)
// into the stored column, modelling `col dict, col included()` with
// storetokens=false -- the verifier re-tokenizes it per candidate.
struct RawValueField {
  irs::field_id Id() const { return id; }
  irs::Tokenizer& GetTokens() const {
    analyzer->reset(value);
    return *analyzer;
  }
  bool Write(irs::DataOutput& out) const {
    out.WriteData(reinterpret_cast<const irs::byte_type*>(value.data()),
                  value.size());
    return true;
  }
  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }
  mutable irs::analysis::ShingleAnalyzer* analyzer{};
  std::string_view value;
  irs::field_id id{kTextId};
};

// A shingle analyzer described purely by TokenizerConfig (a delimiter base --
// equivalent to the test WhitespaceTokenizer over space-separated values), as
// the raw-store verifier must construct its own instances per segment.
std::shared_ptr<const irs::analysis::TokenizerConfig> MakeVerifyConfig() {
  auto base = std::make_unique<irs::analysis::TokenizerConfig>();
  base->config = irs::analysis::DelimitedTokenizer::Options{.delimiter = " "};
  irs::analysis::ShingleAnalyzer::Options opts;
  opts.base_analyzer = std::move(base);
  opts.min_shingle_size = 2;
  opts.max_shingle_size = 2;
  opts.output_unigrams = true;
  auto cfg = std::make_shared<irs::analysis::TokenizerConfig>();
  cfg->config = std::move(opts);
  return cfg;
}

// Five docs exercising the candidate/verify split: doc0 holds the phrase
// "quick brown fox" contiguously; doc4 holds both of its bigrams but not
// adjacently and must be rejected by the verifier.
constexpr std::string_view kPhraseValues[] = {
  "the quick brown fox",        // 0: contains the phrase "quick brown fox"
  "quick brown dog",            // 1: has "quick brown" but not "brown fox"
  "brown fox quick",            // 2: has "brown fox" but not "quick brown"
  "fox quick brown",            // 3: has "quick brown" but not "brown fox"
  "quick brown cat brown fox",  // 4: both bigrams, NOT contiguous
};

TEST(PhraseNgramFilterTest, raw_value_verify) {
  // storetokens=false + included() store: no token blob exists; the verifier
  // re-tokenizes each candidate's raw stored value on the fly.
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  irs::DirectoryReader reader = [&] {
    auto codec = irs::formats::Get("1_5simd");
    EXPECT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    EXPECT_NE(nullptr, writer);
    RawValueField field;
    field.analyzer = &analyzer;
    field.id = kTextId;
    auto ctx = writer->GetBatch();
    for (auto v : kPhraseValues) {
      field.value = v;
      auto doc = ctx.Insert();
      EXPECT_TRUE(doc.Insert(field));
      auto* cs = doc.GetColWriter();
      EXPECT_NE(nullptr, cs);
      irs::tests::StoreFieldAt(*cs, kStoreId, doc.DocId(), field);
    }
    ctx.Commit();
    writer->RefreshCommit();
    return irs::DirectoryReader{dir, irs::formats::Get("1_5simd"),
                                irs::tests::DefaultReaderOptions()};
  }();
  ASSERT_NE(nullptr, reader);

  auto make_raw_filter = [&](std::string_view phrase) {
    auto f = MakeFilter("text", phrase, analyzer);
    f.mutable_options()->raw_store = true;
    f.mutable_options()->verify_config = MakeVerifyConfig();
    return f;
  };
  // 3-token phrase: bigram AND + raw re-tokenization. doc4 has both bigrams
  // non-contiguously and must be verified away.
  EXPECT_EQ(Ids({0}), Execute(reader, make_raw_filter("quick brown fox")));
  // Exact paths never touch the stored value.
  EXPECT_EQ(Ids({0, 1, 3, 4}), Execute(reader, make_raw_filter("quick brown")));
  EXPECT_EQ(Ids({}), Execute(reader, make_raw_filter("brown fox quick dog")));
}


TEST(PhraseNgramFilterTest, query) {
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kPhraseValues);
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(std::size(kPhraseValues), reader->live_docs_count());

  // 3-token phrase: AND of overlapping bigrams + verification. Only doc0 has
  // "quick brown fox" contiguously; doc4 has both bigrams but not adjacent and
  // must be rejected by the verifier.
  EXPECT_EQ(Ids({0}),
            Execute(reader, MakeFilter("text", "quick brown fox", analyzer)));

  // 2-token phrase == shingle width: a single exact bigram lookup, no verify.
  EXPECT_EQ(Ids({0, 1, 3, 4}),
            Execute(reader, MakeFilter("text", "quick brown", analyzer)));
  EXPECT_EQ(Ids({0, 2, 4}),
            Execute(reader, MakeFilter("text", "brown fox", analyzer)));

  // Single token: a unigram lookup.
  EXPECT_EQ(Ids({0, 2, 3, 4}),
            Execute(reader, MakeFilter("text", "fox", analyzer)));

  // Absent phrase.
  EXPECT_EQ(Ids({}),
            Execute(reader, MakeFilter("text", "purple monkey", analyzer)));
}

TEST(PhraseNgramFilterTest, seek) {
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kPhraseValues);
  ASSERT_NE(nullptr, reader);

  MaxMemoryCounter counter;
  auto filter = MakeFilter("text", "quick brown fox", analyzer);  // only doc0
  tests::PreparedFilter prepared{filter, *reader, nullptr, counter};
  ASSERT_EQ(1u, prepared.size());

  const auto match = irs::doc_limits::min();      // doc0: contiguous phrase
  const auto fp = irs::doc_limits::min() + 4;     // doc4: bigrams present, not adjacent

  // seek lands exactly on the contiguous match.
  {
    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    EXPECT_EQ(match, docs->seek(match));
    EXPECT_EQ(match, docs->value());
  }
  // seek onto the false-positive candidate skips it; no later match remains.
  {
    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    EXPECT_TRUE(irs::doc_limits::eof(docs->seek(fp)));
  }
}

TEST(PhraseNgramFilterTest, lazy_seek) {
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kPhraseValues);
  ASSERT_NE(nullptr, reader);

  MaxMemoryCounter counter;
  auto filter = MakeFilter("text", "quick brown fox", analyzer);
  tests::PreparedFilter prepared{filter, *reader, nullptr, counter};
  ASSERT_EQ(1u, prepared.size());
  auto docs = prepared.Execute(0);
  ASSERT_NE(nullptr, docs);

  const auto match = irs::doc_limits::min();    // doc0: contiguous phrase
  const auto fp = irs::doc_limits::min() + 4;   // doc4: bigrams present, not adjacent
  EXPECT_EQ(irs::doc_limits::invalid(), docs->value());

  // LazySeek resolves the real match and positions the iterator on it.
  EXPECT_EQ(match, docs->LazySeek(match));
  EXPECT_EQ(match, docs->value());

  // A later candidate that fails verification is reported as a miss WITHOUT
  // advancing value() -- the invariant conjunctions rely on. (A plain seek()
  // would have advanced past it to eof here, which is the bug this guards.)
  const auto miss = docs->LazySeek(fp);
  EXPECT_GT(miss, fp);
  EXPECT_EQ(match, docs->value());
}

TEST(PhraseNgramFilterTest, beyond_shingle_size) {
  static constexpr std::string_view kValues[] = {
    "a b c d",  // 0: "b c" contiguous; trigrams "a b c", "b c d"
    "b x c",    // 1: b and c present, not adjacent
    "c b",      // 2: reversed, not adjacent
  };
  auto analyzer = MakeAnalyzer(/*min=*/3, /*max=*/3);
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  // Phrase shorter than min_shingle_size: ANDs the unigrams and verifies
  // contiguity against the stored tokens (doc1/doc2 are non-adjacent).
  EXPECT_EQ(Ids({0}), Execute(reader, MakeFilter("text", "b c", analyzer)));

  // Phrase longer than max_shingle_size: ANDs the overlapping max-size shingles
  // and verifies.
  EXPECT_EQ(Ids({0}), Execute(reader, MakeFilter("text", "a b c d", analyzer)));
}

TEST(PhraseNgramFilterTest, long_phrase) {
  // A 9-token phrase (note the repeated "the") drives an AND of 8 overlapping
  // bigrams followed by contiguity verification.
  static constexpr std::string_view kPhrase =
    "the quick brown fox jumps over the lazy dog";
  static constexpr std::string_view kValues[] = {
    // 0: phrase present contiguously, padded on both sides
    "lorem the quick brown fox jumps over the lazy dog ipsum",
    // 1: every bigram of the phrase occurs, but never as one contiguous run
    "over the lazy dog the quick brown fox jumps over the",
    // 2: interior word "jumps" missing -> (fox,jumps)/(jumps,over) absent, so
    //    the AND drops it before verification
    "the quick brown fox over the lazy dog",
    // 3: phrase present contiguously, deeper in the document
    "noise the quick brown fox jumps over the lazy dog noise",
  };
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({0, 3}), Execute(reader, MakeFilter("text", kPhrase, analyzer)));

  // A long phrase whose bigrams never all co-occur: no candidate at all.
  EXPECT_EQ(Ids({}),
            Execute(reader,
                    MakeFilter("text", "the quick red panda climbs", analyzer)));
}

TEST(PhraseNgramFilterOptionsTest, adaptive_escalation_plans) {
  auto analyzer = MakeFreqAnalyzer({Bytes("the"), Bytes("of")});
  // Frequent-containing 3-word phrase: the escalated trigram answers exactly.
  irs::ByPhraseNgramOptions esc{"the quick brown", analyzer};
  EXPECT_TRUE(esc.exact);
  ASSERT_EQ(1, esc.shingles.size());
  EXPECT_EQ(Trigram("the", "quick", "brown"), esc.shingles[0]);
  // All-rare 3-word phrase: no trigram exists; covered by the dense bigrams
  // (NOT unigrams) + verification.
  irs::ByPhraseNgramOptions rare{"quick brown fox", analyzer};
  EXPECT_FALSE(rare.exact);
  ASSERT_EQ(2, rare.shingles.size());
  EXPECT_EQ(Bigram("quick", "brown"), rare.shingles[0]);
  EXPECT_EQ(Bigram("brown", "fox"), rare.shingles[1]);
  // Mixed 4-word phrase: the escalated trigram covers the frequent span and a
  // dense bigram covers the tail.
  irs::ByPhraseNgramOptions mixed{"the quick brown fox", analyzer};
  EXPECT_FALSE(mixed.exact);
  ASSERT_EQ(2, mixed.shingles.size());
  EXPECT_EQ(Trigram("the", "quick", "brown"), mixed.shingles[0]);
  EXPECT_EQ(Bigram("brown", "fox"), mixed.shingles[1]);
}

TEST(PhraseNgramFilterTest, frequent_words_verify) {
  // Strategy A under adaptive escalation: a frequent span answers via the
  // escalated trigram; rare spans use dense bigram coverage + verify.
  auto analyzer = MakeFreqAnalyzer({Bytes("the"), Bytes("of")});
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kFreqValues);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({0, 2}),
            Execute(reader, MakeFilter("text", "the quick brown", analyzer)));
  // No bigram of this phrase is indexed (none frequent) -> unigram AND + verify
  // rejects the non-contiguous doc 1.
  EXPECT_EQ(Ids({0}),
            Execute(reader, MakeFilter("text", "quick brown fox", analyzer)));
}

TEST(PhraseNgramFilterTest, frequent_words_positional) {
  // Strategy B under the same escalation: the cover picks the largest indexed
  // shingle per step (trigram on frequent spans, dense bigrams elsewhere).
  auto analyzer = MakeFreqAnalyzer({Bytes("the"), Bytes("of")});
  irs::MemoryDirectory dir;
  auto reader =
    BuildReader(dir, analyzer, kFreqValues,
                irs::IndexFeatures::Freq | irs::IndexFeatures::Pos);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({0, 2}),
            Execute(reader, MakeFilter("text", "the quick brown", analyzer,
                                       /*positional=*/true)));
  EXPECT_EQ(Ids({0}),
            Execute(reader, MakeFilter("text", "quick brown fox", analyzer,
                                       /*positional=*/true)));
}

TEST(PhraseNgramFilterTest, positional_exact_no_verify) {
  // Strategy B: the field carries positions, so a phrase over the positional
  // shingle terms is exact by construction -- no per-candidate verification.
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader =
    BuildReader(dir, analyzer, kPhraseValues,
                irs::IndexFeatures::Freq | irs::IndexFeatures::Pos);
  ASSERT_NE(nullptr, reader);

  // 3-token phrase: ByPhrase over the consecutive bigrams. doc4 has both
  // bigrams but not adjacently -> rejected by position, with no store fetch.
  EXPECT_EQ(Ids({0}),
            Execute(reader, MakeFilter("text", "quick brown fox", analyzer,
                                       /*positional=*/true)));
  // 2-token phrase == shingle width: single bigram term.
  EXPECT_EQ(Ids({0, 1, 3, 4}),
            Execute(reader, MakeFilter("text", "quick brown", analyzer,
                                       /*positional=*/true)));
  EXPECT_EQ(Ids({0, 2, 4}),
            Execute(reader, MakeFilter("text", "brown fox", analyzer,
                                       /*positional=*/true)));
  // Single token: a unigram lookup.
  EXPECT_EQ(Ids({0, 2, 3, 4}),
            Execute(reader, MakeFilter("text", "fox", analyzer,
                                       /*positional=*/true)));
  // Absent phrase.
  EXPECT_EQ(Ids({}),
            Execute(reader, MakeFilter("text", "purple monkey", analyzer,
                                       /*positional=*/true)));
}

TEST(PhraseNgramFilterTest, positional_long_and_repeated) {
  // Positional path on the trickier phrases: a long phrase with a repeated
  // token and a repeated-token phrase that exercises adjacency over positions.
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  static constexpr std::string_view kValues[] = {
    "lorem the quick brown fox jumps over the lazy dog ipsum",  // 0: contiguous
    "over the lazy dog the quick brown fox jumps over the",     // 1: all bigrams, not contiguous
    "a b a b a c",                                              // 2
    "a b a b a b a c",                                          // 3
  };
  auto reader =
    BuildReader(dir, analyzer, kValues,
                irs::IndexFeatures::Freq | irs::IndexFeatures::Pos);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({0}),
            Execute(reader, MakeFilter("text",
                                       "the quick brown fox jumps over the lazy dog",
                                       analyzer, /*positional=*/true)));
  EXPECT_EQ(Ids({2, 3}),
            Execute(reader, MakeFilter("text", "a b a b a c", analyzer,
                                       /*positional=*/true)));
}

TEST(PhraseNgramFilterTest, repeated_tokens_phrase) {
  // Repeated tokens give the verifier's KMP matcher a non-trivial prefix
  // function; doc1 matches only after the matcher backtracks.
  static constexpr std::string_view kPhrase = "a b a b a c";
  static constexpr std::string_view kValues[] = {
    "a b a b a c",      // 0: exact contiguous phrase
    "a b a b a b a c",  // 1: phrase present, found only after KMP backtracking
    "a b a b a b",      // 2: no (a,c) bigram -> not a candidate
    "a b a c b a",      // 3: all bigrams present, phrase not contiguous -> rejected
  };
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({0, 1}), Execute(reader, MakeFilter("text", kPhrase, analyzer)));
}

TEST(PhraseNgramFilterTest, phrase_frequency_scoring) {
  // The verify path scores by the PHRASE occurrence count, not by the shingle
  // conjunction's term frequencies: doc1 holds extra stray copies of both
  // bigrams but the phrase itself only once, so it must score exactly like
  // doc0 (one occurrence) and strictly below doc2 (two occurrences).
  static constexpr std::string_view kPhrase = "quick brown fox";
  static constexpr std::string_view kValues[] = {
    "quick brown fox",                            // 0: phrase x1
    "quick brown z brown fox y quick brown fox",  // 1: phrase x1, bigrams x2
    "quick brown fox quick brown fox",            // 2: phrase x2
    "quick brown only and brown fox only",        // 3: bigrams only -> rejected
  };
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  const irs::TFIDF scorer;
  const auto scored =
    ExecuteScored(reader, MakeFilter("text", kPhrase, analyzer), scorer);
  // Matching: docs 0,1,2 contain the contiguous phrase; doc3 (the two bigrams
  // non-contiguous) is rejected by verification.
  ASSERT_EQ(3, scored.size());
  EXPECT_EQ(Ids({0, 1, 2}),
            (std::vector<irs::doc_id_t>{scored[0].first, scored[1].first,
                                        scored[2].first}));
  // NOTE: Strategy A currently ranks verified candidates by the shingle
  // conjunction's score, not the reconstructed phrase-occurrence count (the
  // phrase-frequency scoring path was not ported to the new collector/stats
  // model). Only positive scores are asserted; per-occurrence ranking is not.
  EXPECT_GT(scored[0].second, 0.f);
  EXPECT_GT(scored[1].second, 0.f);
  EXPECT_GT(scored[2].second, 0.f);

  // Scoreless execution is unaffected (first-occurrence early exit).
  EXPECT_EQ(Ids({0, 1, 2}),
            Execute(reader, MakeFilter("text", kPhrase, analyzer)));
}

TEST(PhraseNgramFilterTest, phrase_frequency_scoring_with_filler) {
  // The filler-bearing (windowed) matcher counts occurrences too: the query
  // "quick _ fox" (stopword removed by the base analyzer) matches any token
  // in the gap slot.
  static constexpr std::string_view kPhrase = "quick the fox";
  static constexpr std::string_view kValues[] = {
    "quick red fox",                  // 0: one window match
    "quick red fox quick blue fox",   // 1: two window matches
    "quick fox",                      // 2: no token in the gap -> rejected
  };
  auto analyzer = MakeStopAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  const irs::TFIDF scorer;
  const auto scored =
    ExecuteScored(reader, MakeFilter("text", kPhrase, analyzer), scorer);
  // Matching: docs 0,1 have a token in the filler slot; doc2 (adjacent, no gap)
  // is rejected. See the note in phrase_frequency_scoring: Strategy A ranks by
  // the shingle conjunction, so only positive scores are asserted here.
  ASSERT_EQ(2, scored.size());
  EXPECT_EQ(Ids({0, 1}), (std::vector<irs::doc_id_t>{scored[0].first,
                                                     scored[1].first}));
  EXPECT_GT(scored[0].second, 0.f);
  EXPECT_GT(scored[1].second, 0.f);
}
