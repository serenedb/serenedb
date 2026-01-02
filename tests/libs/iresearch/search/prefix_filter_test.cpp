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

#include <iresearch/index/index_features.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/filter_visitor.hpp>
#include <iresearch/search/prefix_filter.hpp>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

irs::ByPrefix MakeFilter(const std::string_view& field,
                         const std::string_view term,
                         size_t scored_terms_limit = 1024) {
  irs::ByPrefix q;
  *q.mutable_field() = field;
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

      size_t collect_field_count = 0;
      size_t collect_term_count = 0;
      size_t finish_count = 0;

      std::array<irs::Scorer::ptr, 1> order{
        std::make_unique<tests::sort::CustomSort>()};

      auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

      scorer.collector_collect_field = [&collect_field_count](
                                         const irs::SubReader&,
                                         const irs::TermReader&) -> void {
        ++collect_field_count;
      };
      scorer.collector_collect_term =
        [&collect_term_count](const irs::SubReader&, const irs::TermReader&,
                              const irs::AttributeProvider&) -> void {
        ++collect_term_count;
      };
      scorer.collectors_collect =
        [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                        const irs::TermCollector*) -> void { ++finish_count; };
      scorer.prepare_field_collector = [&scorer]() -> irs::FieldCollector::ptr {
        return std::make_unique<tests::sort::CustomSort::FieldCollector>(
          scorer);
      };
      scorer.prepare_term_collector = [&scorer]() -> irs::TermCollector::ptr {
        return std::make_unique<tests::sort::CustomSort::TermCollector>(scorer);
      };
      CheckQuery(MakeFilter("prefix", ""), order, docs, rdr);
      ASSERT_EQ(9, collect_field_count);  // 9 fields (1 per term since treated
                                          // as a disjunction) in 1 segment
      ASSERT_EQ(9, collect_term_count);   // 9 different terms
      ASSERT_EQ(9, finish_count);         // 9 unque terms
    }

    // empty prefix
    {
      Docs docs{31, 32, 1, 4, 9, 16, 21, 24, 26, 29};
      Costs costs{docs.size()};

      irs::Scorer::ptr scorer{std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter("prefix", ""), std::span{&scorer, 1}, docs, rdr);
    }

    // empty prefix + scored_terms_limit
    {
      // They are all in the lazy bitset iterator
      Docs docs{1, 4, 9, 16, 21, 24, 26, 29, 31, 32};
      Costs costs{docs.size()};

      irs::Scorer::ptr scorer{std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter("prefix", "", 1), std::span{&scorer, 1}, docs, rdr);
    }

    // prefix
    {
      Docs docs{31, 32, 1, 4, 16, 21, 26, 29};
      Costs costs{docs.size()};

      std::array<irs::Scorer::ptr, 1> order{
        std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter("prefix", "a"), order, docs, rdr);
    }

    // prefix
    {
      Docs docs{31, 32, 1, 4, 16, 21, 26, 29};
      Costs costs{docs.size()};

      std::array<irs::Scorer::ptr, 2> order{
        std::make_unique<tests::sort::FrequencySort>(),
        std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(MakeFilter("prefix", "a"), order, docs, rdr);
    }
  }

  void ByPrefixSequential(bool wand) {
    irs::BM25 bm25;
    irs::Scorer* score = &bm25;
    irs::IndexWriterOptions opts;
    opts.features = [](irs::IndexFeatures id) {
      if (irs::IndexFeatures::Norm == id) {
        return std::pair{
          irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
          &irs::Norm::MakeWriter};
      }
      return std::pair{
        irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
        irs::FeatureWriterFactory{}};
    };
    if (codec()->type()().name().starts_with("1_5avx") && wand) {
      opts.reader_options.scorers = {&score, 1};
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
    CheckQuery(MakeFilter("", "xyz"), Docs{}, Costs{0}, rdr);

    // invalid field
    CheckQuery(MakeFilter("same1", "xyz"), Docs{}, Costs{0}, rdr);

    // invalid prefix
    CheckQuery(MakeFilter("same", "xyz_invalid"), Docs{}, Costs{0}, rdr);

    // valid prefix
    {
      Docs result;
      for (size_t i = 0; i < 32; ++i) {
        result.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
      }

      Costs costs{result.size()};

      CheckQuery(MakeFilter("same", "xyz"), result, costs, rdr);
    }

    // empty prefix : get all fields
    {
      Docs docs{1, 2, 3, 5, 8, 11, 14, 17, 19, 21, 24, 27, 31};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter("duplicated", ""), docs, costs, rdr);
    }

    // single digit prefix
    {
      Docs docs{1, 5, 11, 21, 27, 31};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter("duplicated", "a"), docs, costs, rdr);
    }

    CheckQuery(MakeFilter("name", "!"), Docs{28}, Costs{1}, rdr);
    CheckQuery(MakeFilter("prefix", "b"), Docs{9, 24}, Costs{2}, rdr);

    // multiple digit prefix
    {
      Docs docs{2, 3, 8, 14, 17, 19, 24};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter("duplicated", "vcz"), docs, costs, rdr);
    }

    {
      Docs docs{1, 4, 21, 26, 31, 32};
      Costs costs{docs.size()};
      CheckQuery(MakeFilter("prefix", "abc"), docs, costs, rdr);
    }

    {
      Docs docs{1, 4, 21, 26, 31, 32};
      Costs costs{docs.size()};

      CheckQuery(MakeFilter("prefix", "abc"), docs, costs, rdr);
    }

    // whole word
    CheckQuery(MakeFilter("prefix", "bateradsfsfasdf"), Docs{24}, Costs{1},
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

    CheckQuery(MakeFilter("Name", "Addr"), Docs{1, 2, 77, 78}, rdr);
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
  ASSERT_EQ("", q.field());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_prefix_test, equal) {
  {
    irs::ByPrefix q = MakeFilter("field", "term");

    ASSERT_EQ(q, MakeFilter("field", "term"));
    ASSERT_NE(q, MakeFilter("field1", "term"));
    ASSERT_NE(q, MakeFilter("field", "term", 100));
  }

  {
    irs::ByPrefix q = MakeFilter("field", "term", 100);

    ASSERT_EQ(q, MakeFilter("field", "term", 100));
    ASSERT_NE(q, MakeFilter("field1", "term", 100));
    ASSERT_NE(q, MakeFilter("field", "term"));
  }
}

TEST(by_prefix_test, boost) {
  MaxMemoryCounter counter;

  // no boost
  {
    irs::ByPrefix q = MakeFilter("field", "term");

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
    irs::ByPrefix q = MakeFilter("field", "term");
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

  const std::string_view field = "prefix";
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

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  prefix_filter_test, PrefixFilterTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5avx"},
                                       tests::FormatInfo{"1_5simd"})),
  PrefixFilterTestCase::to_string);

}  // namespace
