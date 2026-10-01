////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/wildcard_tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"
#include "token_sink_utils.hpp"

namespace {

struct WildcardField final {
  irs::field_id Id() const { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return *analyzer; }

  std::string_view Value() const noexcept { return value; }

  bool Write(irs::DataOutput& out) const {
    irs::ValueAnalyzer value_analyzer;
    irs::ValueTokens tokens;
    if (value_analyzer.Analyze(*analyzer, tests::ToStringT(value), tokens) &&
        !tokens.store().empty()) {
      out.WriteData(tokens.store().data(), tokens.store().size());
    }
    return true;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  mutable irs::analysis::WildcardTokenizer* analyzer{};
  std::string_view value;
  irs::field_id id{};
};

inline constexpr irs::field_id kStoreId = 1;

inline constexpr irs::field_id kTextId = 2;
inline constexpr irs::field_id kFieldId = 3;
inline constexpr irs::field_id kOtherId = 4;

// Build a ByWildcardNGram for the given field and SQL LIKE pattern.
// `store_field_id` is wired to kStoreId so the filter's per-doc point
// access lands on the cs column written below in the `query` test.
irs::ByWildcardNGram MakeFilter(irs::field_id field, std::string_view pattern,
                                irs::analysis::WildcardTokenizer& analyzer,
                                bool has_positions = true) {
  irs::ByWildcardNGram filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() =
    irs::ByWildcardNGramOptions{pattern, analyzer, has_positions};
  filter.mutable_options()->store_field_id = kStoreId;
  return filter;
}

irs::ByWildcardNGramOptions MakeRegexpOptions(
  std::string_view pattern, irs::analysis::WildcardTokenizer& analyzer,
  bool has_positions = true,
  irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl) {
  return {
    irs::ViewCast<irs::byte_type>(pattern),
    syntax,
    analyzer,
    has_positions,
  };
}

irs::ByWildcardNGram MakeRegexpFilter(
  irs::field_id field, std::string_view pattern,
  irs::analysis::WildcardTokenizer& analyzer, bool has_positions = true,
  irs::RegexpSyntax syntax = irs::RegexpSyntax::Perl) {
  irs::ByWildcardNGram filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() =
    MakeRegexpOptions(pattern, analyzer, has_positions, syntax);
  filter.mutable_options()->store_field_id = kStoreId;
  return filter;
}

}  // namespace

// ---------------------------------------------------------------------------
// ByWildcardNGramOptions unit tests
// ---------------------------------------------------------------------------

TEST(WildcardNGramFilterOptionsTest, default_ctor) {
  irs::ByWildcardNGramOptions opts;
  EXPECT_TRUE(opts.pattern.empty());
  EXPECT_EQ(irs::GramQuery{}, opts.query);
  EXPECT_TRUE(opts.grams.empty());
  EXPECT_FALSE(opts.has_pos);
  EXPECT_EQ(nullptr, opts.matcher);
}

TEST(WildcardNGramFilterOptionsTest, like_is_a_regexp) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  EXPECT_TRUE(irs::ByWildcardNGramOptions("a\\%b%c_", analyzer, true) ==
              MakeRegexpOptions(R"((?s)\Aa%b.*c.\z)", analyzer));

  const irs::ByWildcardNGramOptions prefix{"abc%", analyzer, false};
  EXPECT_EQ(R"("\x1fabc")", irs::ToString(prefix.query));
  EXPECT_NE(nullptr, prefix.matcher);
  EXPECT_EQ(prefix.query, MakeRegexpOptions("abc.*", analyzer, false).query);
}

TEST(WildcardNGramFilterOptionsTest, short_pieces) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  const auto part = [&](std::string_view like) {
    const irs::ByWildcardNGramOptions opts{like, analyzer, false};
    EXPECT_EQ(nullptr, opts.matcher) << like;
    EXPECT_EQ(1U, opts.grams.size()) << like;
    EXPECT_EQ(1U, opts.grams.front().size()) << like;
    return opts.grams.front().begin()->part;
  };
  EXPECT_EQ(
    irs::ByPhraseOptions::PhrasePart{irs::ByPrefixOptions{
      {.term =
         irs::bstring{irs::ViewCast<irs::byte_type>(std::string_view{"\x1F"
                                                                     "a"})}}}},
    part("a%"));
  EXPECT_EQ(irs::ByPhraseOptions::PhrasePart{irs::ByTermOptions{
              .term = irs::bstring{irs::ViewCast<irs::byte_type>(
                std::string_view{"a\x1F"})}}},
            part("%a"));
  EXPECT_EQ(
    irs::ByPhraseOptions::PhrasePart{irs::ByPrefixOptions{
      {.term =
         irs::bstring{irs::ViewCast<irs::byte_type>(std::string_view{"a"})}}}},
    part("%a%"));
  EXPECT_EQ(irs::ByPhraseOptions::PhrasePart{irs::ByPrefixOptions{}},
            part("%"));
  EXPECT_EQ(irs::ByPhraseOptions::PhrasePart{irs::ByTermOptions{
              .term = irs::bstring{irs::ViewCast<irs::byte_type>(
                std::string_view{"\x1F\x1F"})}}},
            part(""));

  const irs::ByWildcardNGramOptions any{"_%", analyzer, true};
  EXPECT_EQ(irs::GramQuery{}, any.query);
  EXPECT_NE(nullptr, any.matcher);

  EXPECT_NE(nullptr,
            irs::ByWildcardNGramOptions("bc%", analyzer, false).matcher);
  EXPECT_EQ(nullptr,
            irs::ByWildcardNGramOptions("bc%", analyzer, true).matcher);
}

TEST(WildcardNGramFilterOptionsTest, equality_empty) {
  irs::ByWildcardNGramOptions a;
  irs::ByWildcardNGramOptions b;
  EXPECT_TRUE(a == b);
}

TEST(WildcardNGramFilterOptionsTest, equality_with_matcher) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  irs::ByWildcardNGramOptions a{"foo%bar", analyzer, true};
  irs::ByWildcardNGramOptions b{"foo%bar", analyzer, true};
  EXPECT_TRUE(a == b);

  irs::ByWildcardNGramOptions c{"foo%baz", analyzer, true};
  EXPECT_FALSE(a == c);
}

TEST(WildcardNGramFilterOptionsTest, equality_different_has_pos) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  irs::ByWildcardNGramOptions a{"foo_bar", analyzer, true};
  irs::ByWildcardNGramOptions b{"foo_bar", analyzer, false};
  EXPECT_FALSE(a == b);
}

TEST(WildcardNGramFilterOptionsTest, one_null_matcher) {
  // One options has a matcher (because of '_'), the other doesn't
  // (pure prefix) -- they must not be equal.
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  irs::ByWildcardNGramOptions with_matcher{"a_c", analyzer, true};
  irs::ByWildcardNGramOptions no_matcher{"abc%", analyzer, true};

  EXPECT_NE(with_matcher.matcher, nullptr);
  EXPECT_EQ(no_matcher.matcher, nullptr);
  EXPECT_FALSE(with_matcher == no_matcher);
}

TEST(WildcardNGramFilterOptionsTest, pattern_past_default_budget_is_verified) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};
  constexpr size_t kUnits = 600'000;
  std::string pattern = "abc";
  pattern.append(kUnits, '_');
  pattern += "xyz";

  irs::ByWildcardNGramOptions opts{pattern, analyzer, true};
  ASSERT_NE(nullptr, opts.matcher);

  re2::RE2::Options small;
  small.set_log_errors(false);
  small.set_max_mem(int64_t{64} << 20);
  EXPECT_FALSE(
    re2::RE2(irs::ViewCast<char>(irs::bytes_view{opts.pattern}), small).ok());

  std::string text = "abc";
  for (size_t i = 0; i != kUnits; ++i) {
    text += "\xD0\xB6";
  }
  text += "xyz";
  EXPECT_TRUE(re2::RE2::FullMatch(text, *opts.matcher));
  text.erase(3, 2);
  EXPECT_FALSE(re2::RE2::FullMatch(text, *opts.matcher));
}

// ---------------------------------------------------------------------------
// ByWildcardNGram unit tests
// ---------------------------------------------------------------------------

TEST(WildcardNGramFilterTest, ctor) {
  irs::ByWildcardNGram q;
  EXPECT_EQ(irs::Type<irs::ByWildcardNGram>::id(), q.type());
  EXPECT_EQ(irs::ByWildcardNGramOptions{}, q.options());
  EXPECT_EQ(irs::field_limits::invalid(), q.field_id());
  EXPECT_EQ(irs::kNoBoost, q.GetBoost());
}

TEST(WildcardNGramFilterTest, equal) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  auto q = MakeFilter(kFieldId, "foo_bar", analyzer);
  auto q_same = MakeFilter(kFieldId, "foo_bar", analyzer);
  auto q_diff_field = MakeFilter(kOtherId, "foo_bar", analyzer);
  auto q_diff_pattern = MakeFilter(kFieldId, "foo_baz", analyzer);

  EXPECT_EQ(q, q_same);
  EXPECT_NE(q, q_diff_field);
  EXPECT_NE(q, q_diff_pattern);
}

// ---------------------------------------------------------------------------
// Integration tests: build an in-memory index and run queries
// ---------------------------------------------------------------------------

TEST(WildcardNGramFilterTest, query) {
  // Documents indexed under field "text" (1-indexed doc_ids):
  //  doc 1: "foobar"
  //  doc 2: "foobaz"
  //  doc 3: "xyz123"
  //  doc 4: "hello"
  //  doc 5: "world"
  static constexpr irs::field_id kField = kTextId;
  static constexpr std::string_view kValues[] = {
    "foobar", "foobaz", "xyz123", "hello", "world",
  };
  static constexpr irs::doc_id_t kBase = irs::doc_limits::min();

  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  irs::MemoryDirectory dir;

  {
    auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    WildcardField field;
    field.id = kField;
    field.analyzer = &analyzer;

    auto ctx = writer->GetBatch();
    for (auto v : kValues) {
      field.value = v;
      auto doc = ctx.Insert();
      ASSERT_TRUE(tests::InsertField(doc, field));
      auto* cs = doc.GetColWriter();
      ASSERT_NE(nullptr, cs);
      irs::tests::StoreFieldAt(*cs, kStoreId, doc.DocId(), field);
    }
    ctx.Commit();
    writer->RefreshCommit();
  }

  irs::DirectoryReader reader{dir, irs::tests::DefaultReaderOptions()};
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(std::size(kValues), reader->live_docs_count());

  MaxMemoryCounter counter;

  // Execute a filter and return matched doc_ids across all segments.
  auto execute = [&](const irs::ByWildcardNGram& q) {
    tests::PreparedFilter prepared{q, *reader, nullptr, counter};
    counter.Reset();

    std::vector<irs::doc_id_t> result;
    for (size_t i = 0, n = prepared.size(); i < n; ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->Next())) {
        result.push_back(docs->Value());
      }
    }
    return result;
  };

  auto ids = [](std::initializer_list<int> offsets) {
    std::vector<irs::doc_id_t> v;
    for (int off : offsets) {
      v.push_back(kBase + off);
    }
    return v;
  };

  EXPECT_EQ(ids({0, 1, 2, 3, 4}), execute(MakeFilter(kField, "%", analyzer)));

  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "foo%", analyzer)));
  EXPECT_EQ(ids({2}), execute(MakeFilter(kField, "xyz%", analyzer)));
  EXPECT_EQ(ids({3}), execute(MakeFilter(kField, "hel%", analyzer)));

  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "%bar", analyzer)));
  EXPECT_EQ(ids({1}), execute(MakeFilter(kField, "%baz", analyzer)));
  EXPECT_EQ(ids({2}), execute(MakeFilter(kField, "%123", analyzer)));

  EXPECT_EQ(ids({3}), execute(MakeFilter(kField, "hello", analyzer)));
  EXPECT_EQ(ids({4}), execute(MakeFilter(kField, "world", analyzer)));
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "foobar", analyzer)));

  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "foo_ar", analyzer)));
  EXPECT_EQ(ids({1}), execute(MakeFilter(kField, "foo_az", analyzer)));
  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "foo_a_", analyzer)));
  EXPECT_EQ(ids({3}), execute(MakeFilter(kField, "_ello", analyzer)));
  EXPECT_EQ(ids({4}), execute(MakeFilter(kField, "wor__", analyzer)));

  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "f%r", analyzer)));
  EXPECT_EQ(ids({1}), execute(MakeFilter(kField, "f%z", analyzer)));

  EXPECT_EQ(ids({}), execute(MakeFilter(kField, "nope%", analyzer)));
  EXPECT_EQ(ids({}), execute(MakeFilter(kField, "%qqq%", analyzer)));
  EXPECT_EQ(ids({}), execute(MakeFilter(kField, "fo_x%", analyzer)));

  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "f%", analyzer)));
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "%r", analyzer)));
  EXPECT_EQ(ids({0, 1, 3, 4}), execute(MakeFilter(kField, "%o%", analyzer)));
  EXPECT_EQ(ids({3, 4}), execute(MakeFilter(kField, "%l_%", analyzer)));
  EXPECT_EQ(ids({}), execute(MakeFilter(kField, "", analyzer)));

  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "foo%", analyzer, false)));
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "foo_ar", analyzer, false)));
  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "f%", analyzer, false)));
  EXPECT_EQ(ids({0, 1, 3, 4}),
            execute(MakeFilter(kField, "%o%", analyzer, false)));

  {
    tests::sort::Boost sort;
    const auto scored = [&](const irs::ByWildcardNGram& q) {
      tests::PreparedFilter prepared{q, *reader, &sort, counter};
      counter.Reset();
      std::vector<irs::doc_id_t> result;
      for (size_t i = 0, n = prepared.size(); i < n; ++i) {
        auto docs = prepared.Execute(i);
        while (!irs::doc_limits::eof(docs->Next())) {
          result.push_back(docs->Value());
        }
      }
      return result;
    };

    auto present = MakeFilter(kField, "fooba_", analyzer);
    ASSERT_NE(nullptr, present.options().matcher);
    EXPECT_EQ(ids({0, 1}), scored(present));

    auto absent = MakeFilter(kField, "fooba_", analyzer);
    absent.mutable_options()->store_field_id = kOtherId;
    EXPECT_EQ(ids({}), scored(absent));
  }
}

TEST(WildcardNGramFilterOptionsTest, grams_follow_literals) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  const auto opts = MakeRegexpOptions("foo.*bar", analyzer);
  EXPECT_EQ(R"(And("\x1ffoo", "bar\x1f"))", irs::ToString(opts.query));
  ASSERT_EQ(2U, opts.grams.size());
  EXPECT_EQ(2U, opts.grams[0].size());
  EXPECT_EQ(2U, opts.grams[1].size());
  ASSERT_NE(nullptr, opts.matcher);
  EXPECT_TRUE(re2::RE2::FullMatch("foo-bar", *opts.matcher));
  EXPECT_FALSE(re2::RE2::FullMatch("foo", *opts.matcher));

  const auto exact = MakeRegexpOptions("(?s)foo.*", analyzer);
  EXPECT_EQ(R"("\x1ffoo")", irs::ToString(exact.query));
  EXPECT_EQ(nullptr, exact.matcher);
  EXPECT_NE(nullptr, MakeRegexpOptions("(?s)foo.*", analyzer, false).matcher);
}

TEST(WildcardNGramFilterOptionsTest, invalid_pattern_matches_nothing) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  for (const std::string_view pattern : {"(", "foo\\", "a{1001}"}) {
    const auto opts = MakeRegexpOptions(pattern, analyzer);
    EXPECT_EQ(irs::GramQuery::Kind::None, opts.query.kind) << pattern;
    EXPECT_EQ(nullptr, opts.matcher) << pattern;
  }
}

TEST(WildcardNGramFilterOptionsTest, regexp_equality_is_by_pattern) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  EXPECT_TRUE(MakeRegexpOptions(".*abc.*x", analyzer) ==
              MakeRegexpOptions(".*abc.*x", analyzer));

  const auto x = MakeRegexpOptions(".*abc.*x", analyzer);
  const auto y = MakeRegexpOptions(".*abc.*y", analyzer);
  EXPECT_EQ(x.query, y.query);
  EXPECT_FALSE(x == y);

  EXPECT_FALSE(
    MakeRegexpOptions("abc", analyzer) ==
    MakeRegexpOptions("abc", analyzer, true, irs::RegexpSyntax::PosixEre));
  EXPECT_FALSE(MakeRegexpOptions("abc", analyzer, true) ==
               MakeRegexpOptions("abc", analyzer, false));
}

TEST(WildcardNGramFilterTest, equal_regexp) {
  irs::analysis::WildcardTokenizer analyzer{nullptr, 3};

  auto q = MakeRegexpFilter(kFieldId, "foo.*bar", analyzer);
  auto q_same = MakeRegexpFilter(kFieldId, "foo.*bar", analyzer);
  auto q_diff_field = MakeRegexpFilter(kOtherId, "foo.*bar", analyzer);
  auto q_diff_pattern = MakeRegexpFilter(kFieldId, "foo.*baz", analyzer);

  EXPECT_EQ(q, q_same);
  EXPECT_NE(q, q_diff_field);
  EXPECT_NE(q, q_diff_pattern);
}

TEST(WildcardNGramFilterTest, query_matches_re2) {
  static constexpr irs::field_id kField = kTextId;
  static constexpr std::string_view kValues[]{
    "foobar",
    "foobaz",
    "xyz123",
    "hello",
    "world",
    "FOOBAR",
    "abc",
    "xabcx",
    "alpha",
    "ab",
    "ABC",
    "abc\nd",
    "\xD1\x81\xD0\xBE\xD0\xB1\xD0\xB0\xD0\xBA\xD0\xB0",
    "a\x1F"
    "b",
    "",
    "\xE0\x80\x80"
    "abc",
  };
  static constexpr std::string_view kPatterns[]{
    "abc",
    "^abc$",
    "abc.*",
    "(?s)abc.*",
    "(?s).*abc",
    "(?s).*abc.*",
    "(?s)a.*",
    "(?s).*a",
    "(?s).*b.*",
    "(?s).*",
    "(?s)..*",
    "a.*",
    ".*a.*",
    ".*abc",
    ".*abc.*",
    "ab",
    "ab.*",
    ".*a",
    "x.*x",
    "(?i)abc",
    "(?i)foo.*",
    "alpha",
    "foo.*ba[rz]",
    "foo(bar|baz)",
    ".*(abc|xyz).*",
    ".*(abc|x).*",
    "[a-z]+[0-9]+",
    "abc+",
    "a.c",
    "(abc)?",
    "a*",
    ".*",
    "",
    "hel+o",
    "w.r.d",
    "[^x]*",
    "\\bfoo\\w+",
    ".*\xD1\x81\xD0\xBE\xD0\xB1\xD0\xB0\xD0\xBA.*",
    "a\\x1fb",
    "[\\s\\S]*abc",
    "\\C*abc",
    "\\C\\C\\Cabc",
    "(",
    "zzz",
  };
  static constexpr irs::doc_id_t kBase = irs::doc_limits::min();

  for (const auto n : {size_t{2}, size_t{3}}) {
    irs::analysis::WildcardTokenizer analyzer{nullptr, n};
    irs::MemoryDirectory dir;
    {
      auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate,
                                           irs::tests::DefaultWriterOptions());
      ASSERT_NE(nullptr, writer);

      WildcardField field;
      field.id = kField;
      field.analyzer = &analyzer;

      const auto insert = [&](std::span<const std::string_view> values) {
        auto ctx = writer->GetBatch();
        for (auto v : values) {
          field.value = v;
          auto doc = ctx.Insert();
          ASSERT_TRUE(tests::InsertField(doc, field));
          auto* cs = doc.GetColWriter();
          ASSERT_NE(nullptr, cs);
          irs::tests::StoreFieldAt(*cs, kStoreId, doc.DocId(), field);
        }
        ctx.Commit();
        writer->RefreshCommit();
      };
      insert(std::span{kValues}.first(8));
      insert(std::span{kValues}.subspan(8));
    }

    irs::DirectoryReader reader{dir, irs::tests::DefaultReaderOptions()};
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(2U, reader.size());
    ASSERT_EQ(std::size(kValues), reader->live_docs_count());

    MaxMemoryCounter counter;
    const auto execute = [&](const irs::Filter& q, const irs::Scorer* sort) {
      tests::PreparedFilter prepared{q, *reader, sort, counter};
      counter.Reset();
      std::vector<irs::doc_id_t> result;
      irs::doc_id_t offset = 0;
      for (size_t i = 0, size = prepared.size(); i < size; ++i) {
        auto docs = prepared.Execute(i);
        while (!irs::doc_limits::eof(docs->Next())) {
          result.push_back(offset + docs->Value());
        }
        offset += static_cast<irs::doc_id_t>(reader[i].docs_count());
      }
      return result;
    };

    const auto check = [&](std::string_view pattern, irs::RegexpSyntax syntax) {
      const re2::RE2 re{pattern, irs::RegexpOptions(syntax)};
      const auto marked = [&](irs::doc_id_t doc) {
        return kValues[doc - kBase].find('\x1F') != std::string_view::npos;
      };
      for (const bool has_pos : {true, false}) {
        const auto q =
          MakeRegexpFilter(kField, pattern, analyzer, has_pos, syntax);
        const bool decided = !q.options().matcher;
        std::vector<irs::doc_id_t> expected;
        for (size_t i = 0; i != std::size(kValues); ++i) {
          const auto doc = kBase + static_cast<irs::doc_id_t>(i);
          if (re.ok() && !(decided && marked(doc)) &&
              re2::RE2::FullMatch(kValues[i], re)) {
            expected.push_back(doc);
          }
        }
        auto actual = execute(q, nullptr);
        if (decided) {
          std::erase_if(actual, marked);
        }
        EXPECT_EQ(expected, actual)
          << "pattern: " << pattern << ", n: " << n << ", pos: " << has_pos
          << ", query: " << irs::ToString(q.options().query);
      }
    };

    for (const auto pattern : kPatterns) {
      check(pattern, irs::RegexpSyntax::Perl);
    }
    for (const std::string_view pattern : {"foo.*", "foo(bar|baz)", "w.r.d"}) {
      check(pattern, irs::RegexpSyntax::PosixEre);
    }

    tests::sort::Boost sort;
    EXPECT_EQ(std::vector<irs::doc_id_t>{kBase},
              execute(MakeRegexpFilter(kField, "foo.*r", analyzer), &sort));

    auto absent = MakeRegexpFilter(kField, "foo.*", analyzer);
    absent.mutable_options()->store_field_id = kOtherId;
    EXPECT_TRUE(execute(absent, nullptr).empty());
  }
}
