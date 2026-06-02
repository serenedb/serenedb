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

#include "iresearch/search/regexp_ngram_filter.hpp"

#include <span>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

// Whitespace base tokenizer so the wildcard analyzer sees a known word stream;
// the words double as the regex-verification terms.
class WhitespaceTokenizer final
  : public irs::analysis::TypedAnalyzer<WhitespaceTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "test_regex_whitespace";
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
    _term.value =
      irs::ViewCast<irs::byte_type>(_data.substr(start, _pos - start));
    _inc.value = 1;
    return true;
  }

 private:
  std::string_view _data;
  size_t _pos{0};
  irs::TermAttr _term;
  irs::IncAttr _inc;
};

// Field backed by a WildcardAnalyzer: char n-grams are indexed; the original
// tokens are packed into StoreAttr and persisted to a BLOB column for the
// regex verifier. Frequency-only -- the n-gram-AND + verify path.
struct WildcardField {
  std::string_view Name() const { return "text"; }

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

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }

  mutable irs::analysis::WildcardAnalyzer* analyzer{};
  std::string_view value;
};

inline constexpr irs::field_id kStoreId = 1;

irs::analysis::WildcardAnalyzer MakeAnalyzer() {
  return irs::analysis::WildcardAnalyzer{
    std::make_unique<WhitespaceTokenizer>(), /*ngram_size=*/3};
}

irs::ByRegexpNgram MakeFilter(std::string_view regex,
                              irs::analysis::WildcardAnalyzer& analyzer) {
  irs::ByRegexpNgram filter;
  *filter.mutable_field() = "text";
  *filter.mutable_options() = irs::ByRegexpNgramOptions{regex, analyzer};
  filter.mutable_options()->store_field_id = kStoreId;
  return filter;
}

irs::DirectoryReader BuildReader(irs::MemoryDirectory& dir,
                                 irs::analysis::WildcardAnalyzer& analyzer,
                                 std::span<const std::string_view> values) {
  auto codec = irs::formats::Get("1_5simd");
  EXPECT_NE(nullptr, codec);
  auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                       irs::tests::DefaultWriterOptions());
  EXPECT_NE(nullptr, writer);

  WildcardField field;
  field.analyzer = &analyzer;
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

std::vector<irs::doc_id_t> Ids(std::initializer_list<int> offsets) {
  std::vector<irs::doc_id_t> v;
  for (int off : offsets) {
    v.push_back(irs::doc_limits::min() + off);
  }
  return v;
}

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

// Lower-cased corpus: RE2's Prefilter lower-cases its atoms, so the n-gram
// column must be case-folded for the prefilter to avoid false negatives.
constexpr std::string_view kValues[] = {
  "the quick brown fox",  // 0
  "quick brown dog",      // 1
  "lazy fox runs",        // 2
  "quack browns foxy",    // 3
};

}  // namespace

TEST(RegexpNgramFilterOptionsTest, default_and_equality) {
  irs::ByRegexpNgramOptions a;
  irs::ByRegexpNgramOptions b;
  EXPECT_EQ(nullptr, a.matcher);
  EXPECT_TRUE(a == b);

  auto analyzer = MakeAnalyzer();
  irs::ByRegexpNgramOptions x{"quick", analyzer};
  irs::ByRegexpNgramOptions y{"quick", analyzer};
  irs::ByRegexpNgramOptions z{"brown", analyzer};
  EXPECT_TRUE(x == y);
  EXPECT_FALSE(x == z);
  EXPECT_NE(nullptr, x.matcher);
}

TEST(RegexpNgramFilterTest, literal) {
  // A literal >= n-gram size: AND of its char n-grams + RE2 verify.
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({0, 1}), Execute(reader, MakeFilter("quick", analyzer)));
  // "quack" must NOT match the "quick" n-grams.
  EXPECT_EQ(Ids({3}), Execute(reader, MakeFilter("quack", analyzer)));
}

TEST(RegexpNgramFilterTest, regex_metachars) {
  // Patterns with metacharacters, verified per token via RE2 PartialMatch.
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  // Whole-term match: "bro.n" matches brown (0,1) but NOT browns (3); the
  // literal run "bro" drives the n-gram prefilter.
  EXPECT_EQ(Ids({0, 1}), Execute(reader, MakeFilter("bro.n", analyzer)));
  // "f.x" has no >=3 literal run -> prefilter is match-all -> scan + verify;
  // matches fox (0,2) but NOT foxy (3).
  EXPECT_EQ(Ids({0, 2}), Execute(reader, MakeFilter("f.x", analyzer)));
}

TEST(RegexpNgramFilterTest, alternation) {
  // An OR regex -> OR of the alternatives' n-gram conjunctions.
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({0, 1, 2}),
            Execute(reader, MakeFilter("quick|lazy", analyzer)));
}

TEST(RegexpNgramFilterTest, anchored) {
  // Anchors apply per token (the verifier matches each stored token).
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  // Whole-token "fox": matches fox (0,2) but not foxy (3).
  EXPECT_EQ(Ids({0, 2}), Execute(reader, MakeFilter("^fox$", analyzer)));
}

TEST(RegexpNgramFilterTest, absent) {
  // A literal whose n-grams are absent -> no candidate, no match.
  auto analyzer = MakeAnalyzer();
  irs::MemoryDirectory dir;
  auto reader = BuildReader(dir, analyzer, kValues);
  ASSERT_NE(nullptr, reader);

  EXPECT_EQ(Ids({}), Execute(reader, MakeFilter("xyzzy", analyzer)));
}
