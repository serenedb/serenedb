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

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "insert_field.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/type_limits.hpp"
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

  mutable irs::analysis::WildcardAnalyzer* analyzer{};
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
                                irs::analysis::WildcardAnalyzer& analyzer,
                                bool has_positions = true) {
  irs::ByWildcardNGram filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() =
    irs::ByWildcardNGramOptions{pattern, analyzer, has_positions};
  filter.mutable_options()->store_field_id = kStoreId;
  return filter;
}

}  // namespace

// ---------------------------------------------------------------------------
// ByWildcardNGramOptions unit tests
// ---------------------------------------------------------------------------

TEST(WildcardNGramFilterOptionsTest, default_ctor) {
  irs::ByWildcardNGramOptions opts;
  EXPECT_TRUE(opts.parts.empty());
  EXPECT_TRUE(opts.token.empty());
  EXPECT_TRUE(opts.has_pos);
  EXPECT_EQ(nullptr, opts.matcher);
}

TEST(WildcardNGramFilterOptionsTest, equality_empty) {
  irs::ByWildcardNGramOptions a;
  irs::ByWildcardNGramOptions b;
  EXPECT_TRUE(a == b);
}

TEST(WildcardNGramFilterOptionsTest, equality_with_matcher) {
  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

  // A middle "%" causes needs_matcher=true, so BuildLikeMatcher is called.
  irs::ByWildcardNGramOptions a{"foo%bar", analyzer, true};
  irs::ByWildcardNGramOptions b{"foo%bar", analyzer, true};
  EXPECT_TRUE(a == b);

  irs::ByWildcardNGramOptions c{"foo%baz", analyzer, true};
  EXPECT_FALSE(a == c);
}

TEST(WildcardNGramFilterOptionsTest, equality_different_has_pos) {
  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

  irs::ByWildcardNGramOptions a{"foo_bar", analyzer, true};
  irs::ByWildcardNGramOptions b{"foo_bar", analyzer, false};
  EXPECT_FALSE(a == b);
}

TEST(WildcardNGramFilterOptionsTest, one_null_matcher) {
  // One options has a matcher (because of '_'), the other doesn't
  // (pure prefix) -- they must not be equal.
  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

  irs::ByWildcardNGramOptions with_matcher{"a_c", analyzer, true};
  irs::ByWildcardNGramOptions no_matcher{"abc%", analyzer, true};

  EXPECT_NE(with_matcher.matcher, nullptr);
  EXPECT_EQ(no_matcher.matcher, nullptr);
  EXPECT_FALSE(with_matcher == no_matcher);
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
  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

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

  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

  irs::MemoryDirectory dir;

  {
    auto codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
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

  irs::DirectoryReader reader{dir, irs::formats::Get("1_5simd"),
                              irs::tests::DefaultReaderOptions()};
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
      while (!irs::doc_limits::eof(docs->Advance())) {
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

  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "foo%", analyzer, false)));
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "foo_ar", analyzer, false)));
}
