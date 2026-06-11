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

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

// Field type backed by WildcardAnalyzer.
//
// GetTokens() calls analyzer.reset(value) which:
//   1. Tokenizes the value via the base analyzer (StringTokenizer by default)
//   2. Packs the resulting terms into _store.value in the format expected by
//      WildcardIterator: [vint(size)][0xFF][term_bytes][0xFF] per term
//
// Write() then persists _store.value verbatim into the stored column so that
// WildcardIterator can post-filter documents with the RE2 matcher.
struct WildcardField final {
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

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  mutable irs::analysis::WildcardAnalyzer* analyzer{};
  std::string_view value;
  irs::field_id id{};
};

// Per-file cs column id for the analyzer's packed-terms STORE bytes.
// In production this id comes from the catalog via
// `InvertedIndexColumnInfo::tokenizer_column`; in tests we pick a fixed
// constant so writer and filter agree without any name->id mapping.
inline constexpr irs::field_id kStoreId = 1;

// Stable field ids for unit tests below.
inline constexpr irs::field_id kTextId = 2;
inline constexpr irs::field_id kFieldId = 3;
inline constexpr irs::field_id kOtherId = 4;

// Build a ByWildcardNgram for the given field and SQL LIKE pattern.
// `store_field_id` is wired to kStoreId so the filter's per-doc point
// access lands on the cs column written below in the `query` test.
irs::ByWildcardNgram MakeFilter(irs::field_id field, std::string_view pattern,
                                irs::analysis::WildcardAnalyzer& analyzer,
                                bool has_positions = true) {
  irs::ByWildcardNgram filter;
  *filter.mutable_field_id() = field;
  *filter.mutable_options() =
    irs::ByWildcardNgramOptions{pattern, analyzer, has_positions};
  filter.mutable_options()->store_field_id = kStoreId;
  return filter;
}

}  // namespace

// ---------------------------------------------------------------------------
// ByWildcardNgramOptions unit tests
// ---------------------------------------------------------------------------

TEST(WildcardNgramFilterOptionsTest, default_ctor) {
  irs::ByWildcardNgramOptions opts;
  EXPECT_TRUE(opts.parts.empty());
  EXPECT_TRUE(opts.token.empty());
  EXPECT_TRUE(opts.has_pos);
  EXPECT_EQ(nullptr, opts.matcher);
}

TEST(WildcardNgramFilterOptionsTest, equality_empty) {
  irs::ByWildcardNgramOptions a;
  irs::ByWildcardNgramOptions b;
  EXPECT_TRUE(a == b);
}

TEST(WildcardNgramFilterOptionsTest, equality_with_matcher) {
  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

  // A middle "%" causes needs_matcher=true, so BuildLikeMatcher is called.
  irs::ByWildcardNgramOptions a{"foo%bar", analyzer, true};
  irs::ByWildcardNgramOptions b{"foo%bar", analyzer, true};
  EXPECT_TRUE(a == b);

  irs::ByWildcardNgramOptions c{"foo%baz", analyzer, true};
  EXPECT_FALSE(a == c);
}

TEST(WildcardNgramFilterOptionsTest, equality_different_has_pos) {
  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

  irs::ByWildcardNgramOptions a{"foo_bar", analyzer, true};
  irs::ByWildcardNgramOptions b{"foo_bar", analyzer, false};
  EXPECT_FALSE(a == b);
}

TEST(WildcardNgramFilterOptionsTest, one_null_matcher) {
  // One options has a matcher (because of '_'), the other doesn't
  // (pure prefix) -- they must not be equal.
  irs::analysis::WildcardAnalyzer analyzer{nullptr, 3};

  irs::ByWildcardNgramOptions with_matcher{"a_c", analyzer, true};
  irs::ByWildcardNgramOptions no_matcher{"abc%", analyzer, true};

  EXPECT_NE(with_matcher.matcher, nullptr);
  EXPECT_EQ(no_matcher.matcher, nullptr);
  EXPECT_FALSE(with_matcher == no_matcher);
}

// ---------------------------------------------------------------------------
// ByWildcardNgram unit tests
// ---------------------------------------------------------------------------

TEST(WildcardNgramFilterTest, ctor) {
  irs::ByWildcardNgram q;
  EXPECT_EQ(irs::Type<irs::ByWildcardNgram>::id(), q.type());
  EXPECT_EQ(irs::ByWildcardNgramOptions{}, q.options());
  EXPECT_EQ(irs::field_limits::invalid(), q.field_id());
  EXPECT_EQ(irs::kNoBoost, q.Boost());
}

TEST(WildcardNgramFilterTest, equal) {
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

TEST(WildcardNgramFilterTest, query) {
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

  // Index all documents. INDEX goes through the inverted path; STORE is
  // now an explicit BLOB write to cs column kStoreId so WildcardIterator
  // can post-filter via its store_field_id point cursor.
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
      ASSERT_TRUE(doc.Insert(field));
      // Stored bytes -> cs BLOB column kStoreId; filter reads via the
      // same id (set in MakeFilter / mutable_options()->store_field_id).
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
  auto execute = [&](const irs::ByWildcardNgram& q) {
    auto prepared = q.prepare({.index = *reader, .memory = counter});
    EXPECT_NE(nullptr, prepared);
    counter.Reset();

    std::vector<irs::doc_id_t> result;
    for (const auto& segment : *reader) {
      auto docs = prepared->execute({.segment = segment});
      while (docs->next()) {
        result.push_back(docs->value());
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

  // "%" -- matches every document (pure wildcard, no literals).
  EXPECT_EQ(ids({0, 1, 2, 3, 4}), execute(MakeFilter(kField, "%", analyzer)));

  // Pure prefix (% only at the end) -- no RE2 matcher needed.
  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "foo%", analyzer)));
  EXPECT_EQ(ids({2}), execute(MakeFilter(kField, "xyz%", analyzer)));
  EXPECT_EQ(ids({3}), execute(MakeFilter(kField, "hel%", analyzer)));

  // Pure suffix (% only at position 0) -- no RE2 matcher needed.
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "%bar", analyzer)));
  EXPECT_EQ(ids({1}), execute(MakeFilter(kField, "%baz", analyzer)));
  EXPECT_EQ(ids({2}), execute(MakeFilter(kField, "%123", analyzer)));

  // Exact match -- no wildcards.
  EXPECT_EQ(ids({3}), execute(MakeFilter(kField, "hello", analyzer)));
  EXPECT_EQ(ids({4}), execute(MakeFilter(kField, "world", analyzer)));
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "foobar", analyzer)));

  // Single-char wildcard "_" -- RE2 matcher is built.
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "foo_ar", analyzer)));
  EXPECT_EQ(ids({1}), execute(MakeFilter(kField, "foo_az", analyzer)));
  EXPECT_EQ(ids({0, 1}),
            execute(MakeFilter(kField, "foo_a_", analyzer)));  // foobar, foobaz
  EXPECT_EQ(ids({3}), execute(MakeFilter(kField, "_ello", analyzer)));
  EXPECT_EQ(ids({4}), execute(MakeFilter(kField, "wor__", analyzer)));

  // Middle "%" -- RE2 matcher is built.
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "f%r", analyzer)));
  EXPECT_EQ(ids({1}), execute(MakeFilter(kField, "f%z", analyzer)));

  // No match.
  EXPECT_EQ(ids({}), execute(MakeFilter(kField, "nope%", analyzer)));
  EXPECT_EQ(ids({}), execute(MakeFilter(kField, "%qqq%", analyzer)));
  EXPECT_EQ(ids({}), execute(MakeFilter(kField, "fo_x%", analyzer)));

  // With has_positions=false the RE2 matcher is always built, even for
  // patterns that would otherwise not need one (e.g. pure prefix).
  EXPECT_EQ(ids({0, 1}), execute(MakeFilter(kField, "foo%", analyzer,
                                            /*has_positions=*/false)));
  EXPECT_EQ(ids({0}), execute(MakeFilter(kField, "foo_ar", analyzer,
                                         /*has_positions=*/false)));
}
