////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "basics/duckdb_engine.h"
#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "tests_shared.hpp"

namespace {

// Stable field ids for the simple_sequential / AdventureWorks test data,
// sourced from `tests::FieldIdFor` so the shared JSON factories and these
// tests agree on the id-per-name.
[[maybe_unused]] inline constexpr irs::field_id kPrefixId =
  tests::FieldIdFor("prefix");
[[maybe_unused]] inline constexpr irs::field_id kSameId =
  tests::FieldIdFor("same");
[[maybe_unused]] inline constexpr irs::field_id kDuplicatedId =
  tests::FieldIdFor("duplicated");
[[maybe_unused]] inline constexpr irs::field_id kNameId =
  tests::FieldIdFor("name");
[[maybe_unused]] inline constexpr irs::field_id kNameSchemaId =
  tests::FieldIdFor("Name");
[[maybe_unused]] inline constexpr irs::field_id kInvalidId =
  tests::FieldIdFor("same1");
[[maybe_unused]] inline constexpr irs::field_id kEmptyFieldId =
  irs::field_limits::invalid();

irs::ByPrefix MakeFilter(irs::field_id field, const std::string_view term,
                         size_t scored_terms_limit = 1024) {
  irs::ByPrefix q;
  *q.mutable_field_id() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  q.mutable_options()->scored_terms_limit = scored_terms_limit;
  return q;
}

class PrefixFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  void ByPrefixOrder() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // empty query
    CheckQuery(irs::ByPrefix(), Docs{}, Costs{0}, rdr);

    // empty prefix test collector call count for field/term/finish
    {
      Docs docs{1, 4, 9, 16, 21, 24, 26, 29, 31, 32};
      Costs costs{docs.size()};
      size_t finish_count = 0;
      uint64_t finish_docs_with_field = 0;
      uint64_t finish_docs_with_term = 0;

      std::array<irs::Scorer::ptr, 1> order{
        std::make_unique<tests::sort::CustomSort>()};

      auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

      scorer.collectors_collect = [&](irs::byte_type*,
                                      const irs::FieldCollector* field,
                                      const irs::TermCollector* term) -> void {
        ++finish_count;
        ASSERT_NE(nullptr, field);
        ASSERT_NE(nullptr, term);
        finish_docs_with_field += field->docs_with_field;
        finish_docs_with_term += term->docs_with_term;
      };
      CheckQuery(MakeFilter(kPrefixId, ""), order, docs, rdr);
      ASSERT_EQ(9, finish_count);
      ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
      ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
    }

    // empty prefix
    {
      Docs docs{31, 32, 1, 4, 9, 16, 21, 24, 26, 29};
      Costs costs{docs.size()};

      irs::Scorer::ptr scorer{std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter(kPrefixId, ""), std::span{&scorer, 1}, docs, rdr);
    }

    // empty prefix + scored_terms_limit
    {
      // They are all in the lazy bitset iterator
      Docs docs{1, 4, 9, 16, 21, 24, 26, 29, 31, 32};
      Costs costs{docs.size()};

      irs::Scorer::ptr scorer{std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter(kPrefixId, "", 1), std::span{&scorer, 1}, docs,
                 rdr);
    }

    // prefix
    {
      Docs docs{31, 32, 1, 4, 16, 21, 26, 29};
      Costs costs{docs.size()};

      std::array<irs::Scorer::ptr, 1> order{
        std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter(kPrefixId, "a"), order, docs, rdr);
    }

    // prefix
    {
      Docs docs{31, 32, 1, 4, 16, 21, 26, 29};
      Costs costs{docs.size()};

      std::array<irs::Scorer::ptr, 2> order{
        std::make_unique<tests::sort::FrequencySort>(),
        std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter(kPrefixId, "a"), order, docs, rdr);
    }
  }

  void ByPrefixSequential(bool wand) {
    irs::BM25 bm25;
    irs::Scorer* score = &bm25;
    irs::IndexWriterOptions opts;
    opts.reader_options.db = &::sdb::DuckDBEngine::Instance().instance();
    if (codec()->type()().name().starts_with("1_5simd") && wand) {
      opts.reader_options.scorer = score;
    }
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::NormStringJsonFieldFactory);
      add_segment(gen, irs::kOmCreate, opts);
    }

    auto rdr = open_reader(opts.reader_options);

    // empty query
    CheckQuery(irs::ByPrefix(), Docs{}, Costs{0}, rdr);

    // empty field
    CheckQuery(MakeFilter(kEmptyFieldId, "xyz"), Docs{}, Costs{0}, rdr);

    // invalid field
    CheckQuery(MakeFilter(kInvalidId, "xyz"), Docs{}, Costs{0}, rdr);

    // invalid prefix
    CheckQuery(MakeFilter(kSameId, "xyz_invalid"), Docs{}, Costs{0}, rdr);

    // valid prefix
    {
      Docs result;
      for (size_t i = 0; i < 32; ++i) {
        result.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
      }

      Costs costs{result.size()};

      CheckQuery(MakeFilter(kSameId, "xyz"), result, costs, rdr);
    }

    // empty prefix : get all fields
    {
      Docs docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter(kDuplicatedId, ""), docs, costs, rdr);
    }

    // single digit prefix
    {
      Docs docs{1, 5, 11, 21, 27, 31};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter(kDuplicatedId, "a"), docs, costs, rdr);
    }

    CheckQuery(MakeFilter(kNameId, "!"), Docs{28}, Costs{1}, rdr);
    CheckQuery(MakeFilter(kPrefixId, "b"), Docs{9, 24}, Costs{2}, rdr);

    // multiple digit prefix
    {
      Docs docs{2, 3, 8, 14, 17, 19, 24};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter(kDuplicatedId, "vcz"), docs, costs, rdr);
    }

    {
      Docs docs{1, 4, 21, 26, 31, 32};
      Costs costs{docs.size()};
      CheckQuery(MakeFilter(kPrefixId, "abc"), docs, costs, rdr);
    }

    {
      Docs docs{1, 4, 21, 26, 31, 32};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter(kPrefixId, "abc"), docs, costs, rdr);
    }

    // whole word
    CheckQuery(MakeFilter(kPrefixId, "bateradsfsfasdf"), Docs{24}, Costs{1},
               rdr);
  }

  void ByPrefixSchemas() {
    // write segments
    {
      auto writer = open_writer(irs::kOmCreate);

      std::vector<tests::DocGeneratorBase::ptr> gens;
      gens.emplace_back(new tests::JsonDocGenerator(
        resource("AdventureWorks2014.json"), &tests::GenericJsonFieldFactory));
      gens.emplace_back(
        new tests::JsonDocGenerator(resource("AdventureWorks2014Edges.json"),
                                    &tests::GenericJsonFieldFactory));
      gens.emplace_back(new tests::JsonDocGenerator(
        resource("Northwnd.json"), &tests::GenericJsonFieldFactory));
      gens.emplace_back(new tests::JsonDocGenerator(
        resource("NorthwndEdges.json"), &tests::GenericJsonFieldFactory));
      add_segments(*writer, gens);
    }

    auto rdr = open_reader();

    CheckQuery(MakeFilter(kNameSchemaId, "Addr"), Docs{1, 2, 77, 78}, rdr);
  }
};

TEST(by_prefix_test, options) {
  irs::ByPrefixOptions opts;
  ASSERT_TRUE(opts.term.empty());
  ASSERT_EQ(1024, opts.scored_terms_limit);
}

TEST(by_prefix_test, ctor) {
  irs::ByPrefix q;
  ASSERT_EQ(irs::Type<irs::ByPrefix>::id(), q.type());
  ASSERT_EQ(irs::ByPrefixOptions{}, q.options());
  ASSERT_EQ(irs::field_limits::invalid(), q.field_id());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_prefix_test, equal) {
  constexpr irs::field_id kField = 1;
  constexpr irs::field_id kField1 = 2;
  {
    irs::ByPrefix q = MakeFilter(kField, "term");

    ASSERT_EQ(q, MakeFilter(kField, "term"));
    ASSERT_NE(q, MakeFilter(kField1, "term"));
    ASSERT_NE(q, MakeFilter(kField, "term", 100));
  }

  {
    irs::ByPrefix q = MakeFilter(kField, "term", 100);

    ASSERT_EQ(q, MakeFilter(kField, "term", 100));
    ASSERT_NE(q, MakeFilter(kField1, "term", 100));
    ASSERT_NE(q, MakeFilter(kField, "term"));
  }
}

TEST(by_prefix_test, boost) {
  constexpr irs::field_id kField = 1;
  MaxMemoryCounter counter;

  // no boost
  {
    irs::ByPrefix q = MakeFilter(kField, "term");

    auto prepared = q.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // with boost
  {
    irs::score_t boost = 1.5f;
    irs::ByPrefix q = MakeFilter(kField, "term");
    q.boost(boost);

    auto prepared = q.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    ASSERT_EQ(boost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(PrefixFilterTestCase, by_prefix) {
  ByPrefixOrder();
  ByPrefixSequential(false);
  ByPrefixSequential(true);
  ByPrefixSchemas();
}

TEST_P(PrefixFilterTestCase, visit) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  const irs::field_id field = kPrefixId;
  const irs::bytes_view term =
    irs::ViewCast<irs::byte_type>(std::string_view("ab"));

  tests::EmptyFilterVisitor visitor;
  // read segment
  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];
  // get term dictionary for field
  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);
  irs::ByPrefix::visit(segment, *reader, term, visitor);
  ASSERT_EQ(1, visitor.prepare_calls_counter());
  ASSERT_EQ(6, visitor.visit_calls_counter());
  ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
              {"abc", irs::kNoBoost},
              {"abcd", irs::kNoBoost},
              {"abcde", irs::kNoBoost},
              {"abcdrer", irs::kNoBoost},
              {"abcy", irs::kNoBoost},
              {"abde", irs::kNoBoost}}),
            visitor.term_refs<char>());

  visitor.reset();
}

TEST_P(PrefixFilterTestCase, by_prefix_order_partial_field_stats) {
  // segment 1 carries the "prefix" field
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  // segment 2 has no "prefix" field at all
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (name == "name" && data.is_string()) {
          auto f = std::make_shared<tests::StringField>(name, data.str);
          f->id = tests::FieldIdForRuntime(name);
          doc.insert(std::move(f));
        }
      });
    add_segment(gen, irs::kOmAppend);
  }

  auto rdr = open_reader();
  ASSERT_EQ(2, rdr.size());

  // only the segments that have the field contribute to docs_with_field
  uint64_t expected_docs_with_field = 0;
  size_t segments_with_field = 0;
  for (const auto& segment : rdr) {
    if (const auto* field = segment.field(kPrefixId)) {
      expected_docs_with_field += field->docs_count();
      ++segments_with_field;
    }
  }
  ASSERT_EQ(1u, segments_with_field);  // field present in a single segment
  ASSERT_GT(expected_docs_with_field, 0u);

  const irs::FieldCollector* shared_field = nullptr;
  size_t finish_count = 0;

  std::array<irs::Scorer::ptr, 1> order{
    std::make_unique<tests::sort::CustomSort>()};
  auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());
  scorer.collectors_collect = [&](irs::byte_type*,
                                  const irs::FieldCollector* field,
                                  const irs::TermCollector* term) -> void {
    ++finish_count;
    ASSERT_NE(nullptr, field);
    ASSERT_NE(nullptr, term);
    if (shared_field == nullptr) {
      shared_field = field;
    } else {
      ASSERT_EQ(shared_field, field);  // same collector reused for every term
    }
    ASSERT_EQ(expected_docs_with_field, field->docs_with_field);
  };

  auto q = MakeFilter(kPrefixId, "").prepare({.index = rdr, .scorer = &scorer});
  ASSERT_NE(nullptr, q);

  ASSERT_GT(finish_count, 1u);       // multiple scored terms
  ASSERT_NE(nullptr, shared_field);  // field stats were collected
}

TEST_P(PrefixFilterTestCase, by_prefix_order_multiple_terms_score) {
  // single field "name", three terms with frequencies 2 / 1 / 3
  {
    auto writer = open_writer(irs::kOmCreate);
    {
      auto ctx = writer->GetBatch();
      for (const auto* value : {"aa", "aa", "ab", "ac", "ac", "ac"}) {
        auto doc = ctx.Insert();
        tests::StringField field{"name", value};
        field.id = kNameId;
        doc.Insert(field);
      }
    }
    writer->RefreshCommit();
  }

  auto rdr = open_reader();
  ASSERT_EQ(1, rdr.size());

  // prefix "a" matches all three terms; each doc carries exactly one term, so
  // FrequencySort scores it 1 / docs_with_term:
  //   doc 1,2 -> "aa" (freq 2) -> 0.5
  //   doc 3   -> "ab" (freq 1) -> 1.0
  //   doc 4,5,6 -> "ac" (freq 3) -> 1/3
  ScoredDocs expected{
    {1, {0.5f}},    {2, {0.5f}},    {3, {1.f}},
    {4, {1.f / 3}}, {5, {1.f / 3}}, {6, {1.f / 3}},
  };

  irs::Scorer::ptr scorer{std::make_unique<tests::sort::FrequencySort>()};
  CheckQuery(MakeFilter(kNameId, "a"), std::span{&scorer, 1}, expected, rdr);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(prefix_filter_test, PrefixFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         PrefixFilterTestCase::to_string);

}  // namespace
