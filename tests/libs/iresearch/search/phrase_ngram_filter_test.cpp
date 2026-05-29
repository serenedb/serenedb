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

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/shingle_analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
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
  std::string_view Name() const { return field_name; }

  irs::Tokenizer& GetTokens() const {
    analyzer->reset(value);
    return *analyzer;
  }

  bool Write(irs::DataOutput& out) const {
    const auto* store = irs::get<irs::StoreAttr>(*analyzer);
    if (store && !store->value.empty()) {
      out.WriteBytes(store->value.data(), store->value.size());
    }
    return true;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept { return index_features; }

  mutable irs::analysis::ShingleAnalyzer* analyzer{};
  std::string_view value;
  std::string_view field_name{"text"};
  // Freq -> Strategy A (verify); Freq|Pos -> Strategy B (positional, no verify).
  irs::IndexFeatures index_features{irs::IndexFeatures::Freq};
};

inline constexpr irs::field_id kStoreId = 1;

irs::ByPhraseNgram MakeFilter(std::string_view field, std::string_view phrase,
                              irs::analysis::ShingleAnalyzer& analyzer) {
  irs::ByPhraseNgram filter;
  *filter.mutable_field() = field;
  *filter.mutable_options() = irs::ByPhraseNgramOptions{phrase, analyzer};
  filter.mutable_options()->store_field_id = kStoreId;
  // Strategy (verify vs positional) is detected from the index at Prepare time.
  return filter;
}

irs::analysis::ShingleAnalyzer MakeAnalyzer(uint32_t min = 2, uint32_t max = 2) {
  return irs::analysis::ShingleAnalyzer{{
    .base_analyzer = std::make_unique<WhitespaceTokenizer>(),
    .min_shingle_size = min,
    .max_shingle_size = max,
    .output_unigrams = true,
  }};
}

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
  field.index_features = features;
  auto ctx = writer->GetBatch();
  for (auto v : values) {
    field.value = v;
    auto doc = ctx.Insert();
    EXPECT_TRUE(doc.Insert(field));
    auto* cs = doc.Columnstore();
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
  auto prepared = q.prepare({.index = *reader, .memory = counter});
  EXPECT_NE(nullptr, prepared);
  std::vector<irs::doc_id_t> result;
  if (prepared != nullptr) {
    for (const auto& segment : *reader) {
      auto docs = prepared->execute({.segment = segment});
      while (docs->next()) {
        result.push_back(docs->value());
      }
    }
  }
  return result;
}

}  // namespace

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
  auto prepared = filter.prepare({.index = *reader, .memory = counter});
  ASSERT_NE(nullptr, prepared);
  const auto& segment = *(*reader).begin();

  const auto match = irs::doc_limits::min();      // doc0: contiguous phrase
  const auto fp = irs::doc_limits::min() + 4;     // doc4: bigrams present, not adjacent

  // seek lands exactly on the contiguous match.
  {
    auto docs = prepared->execute({.segment = segment});
    ASSERT_NE(nullptr, docs);
    EXPECT_EQ(match, docs->seek(match));
    EXPECT_EQ(match, docs->value());
  }
  // seek onto the false-positive candidate skips it; no later match remains.
  {
    auto docs = prepared->execute({.segment = segment});
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
  auto prepared = filter.prepare({.index = *reader, .memory = counter});
  ASSERT_NE(nullptr, prepared);
  auto docs = prepared->execute({.segment = *(*reader).begin()});
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
            Execute(reader, MakeFilter("text", "quick brown fox", analyzer)));
  // 2-token phrase == shingle width: single bigram term.
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
                                       analyzer)));
  EXPECT_EQ(Ids({2, 3}),
            Execute(reader, MakeFilter("text", "a b a b a c", analyzer)));
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
