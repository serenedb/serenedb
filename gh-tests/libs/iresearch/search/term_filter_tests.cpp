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

#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/term_query.hpp>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

irs::ByTerm MakeFilter(const std::string_view& field,
                       const std::string_view term) {
  irs::ByTerm q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

class TermFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  void ByTermSequentialCost() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    // read segment
    auto rdr = open_reader();

    CheckQuery(irs::ByTerm(), Docs{}, Costs{0}, rdr);

    // empty term
    CheckQuery(MakeFilter("name", ""), Docs{}, Costs{0}, rdr);

    // empty field
    CheckQuery(MakeFilter("", "xyz"), Docs{}, Costs{0}, rdr);

    // search : invalid field
    CheckQuery(MakeFilter("invalid_field", "A"), Docs{}, Costs{0}, rdr);

    // search : single term
    CheckQuery(MakeFilter("name", "A"), Docs{1}, Costs{1}, rdr);

    MaxMemoryCounter counter;
    {
      irs::ByTerm q = MakeFilter("name", "A");

      auto prepared = q.prepare({
        .index = rdr,
        .memory = counter,
      });
      auto sub = rdr.begin();
      auto docs0 = prepared->execute({.segment = *sub});
      auto* doc = irs::get<irs::DocAttr>(*docs0);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs0->value(), doc->value);
      auto docs1 = prepared->execute({.segment = *sub});
      ASSERT_TRUE(docs0->next());
      ASSERT_EQ(docs0->value(), docs1->seek(docs0->value()));
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // search : all terms
    CheckQuery(
      MakeFilter("same", "xyz"),
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      Costs{32}, rdr);

    // search : empty result
    CheckQuery(MakeFilter("same", "invalid_term"), Docs{}, Costs{0}, rdr);
  }

  void ByTermSequentialBoost() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    // read segment
    auto rdr = open_reader();

    // create filter
    irs::ByTerm filter = MakeFilter("name", "A");
    filter.boost(0.f);

    // create order

    auto scorer = tests::sort::Boost{};
    auto pord = irs::Scorers::Prepare(scorer);

    MaxMemoryCounter counter;

    // without boost
    {
      auto prep = filter.prepare({
        .index = rdr,
        .memory = counter,
        .scorers = pord,
      });
      auto docs = prep->execute({.segment = *(rdr.begin()), .scorers = pord});
      auto* doc = irs::get<irs::DocAttr>(*docs);
      ASSERT_TRUE(bool(doc));
      ASSERT_EQ(docs->value(), doc->value);

      auto* scr = irs::get<irs::ScoreAttr>(*docs);
      ASSERT_FALSE(!scr);

      // first hit
      {
        ASSERT_TRUE(docs->next());
        irs::score_t score_value{};
        (*scr)(&score_value);
        ASSERT_EQ(irs::score_t(0), score_value);
        ASSERT_EQ(docs->value(), doc->value);
      }

      ASSERT_FALSE(docs->next());
      ASSERT_EQ(docs->value(), doc->value);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // with boost
    {
      const irs::score_t value = 5;
      filter.boost(value);

      auto prep = filter.prepare({
        .index = rdr,
        .memory = counter,
        .scorers = pord,
      });
      auto docs = prep->execute({.segment = *(rdr.begin()), .scorers = pord});

      auto* scr = irs::get<irs::ScoreAttr>(*docs);
      ASSERT_FALSE(!scr);

      // first hit
      {
        ASSERT_TRUE(docs->next());
        irs::score_t score_value{};
        (*scr)(&score_value);
        ASSERT_EQ(irs::score_t(value), score_value);
      }

      ASSERT_FALSE(docs->next());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }

  void ByTermSequentialNumeric() {
    // add segment
    {
      tests::JsonDocGenerator gen(
        resource("simple_sequential.json"),
        [](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
          if (data.is_string()) {
            doc.insert(std::make_shared<tests::StringField>(name, data.str));
          } else if (data.is_null()) {
            doc.insert(std::make_shared<tests::BinaryField>());
            auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
            field.Name(name);
            field.value(
              irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
          } else if (data.is_bool() && data.b) {
            doc.insert(std::make_shared<tests::BinaryField>());
            auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
            field.Name(name);
            field.value(irs::ViewCast<irs::byte_type>(
              irs::BooleanTokenizer::value_true()));
          } else if (data.is_bool() && !data.b) {
            doc.insert(std::make_shared<tests::BinaryField>());
            auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
            field.Name(name);
            field.value(irs::ViewCast<irs::byte_type>(
              irs::BooleanTokenizer::value_true()));
          } else if (data.is_number()) {
            const double d_value = data.as_number<double_t>();
            {
              // 'value' can be interpreted as a double
              doc.insert(std::make_shared<tests::DoubleField>());
              auto& field = (doc.indexed.end() - 1).as<tests::DoubleField>();
              field.Name(name);
              field.value(d_value);
            }

            const float f_value = data.as_number<float_t>();
            {
              // 'value' can be interpreted as a float
              doc.insert(std::make_shared<tests::FloatField>());
              auto& field = (doc.indexed.end() - 1).as<tests::FloatField>();
              field.Name(name);
              field.value(f_value);
            }

            const auto value = std::ceil(d_value);
            {
              doc.insert(std::make_shared<tests::LongField>());
              auto& field = (doc.indexed.end() - 1).as<tests::LongField>();
              field.Name(name);
              field.value(static_cast<int64_t>(value));
            }

            {
              doc.insert(std::make_shared<tests::IntField>());
              auto& field = (doc.indexed.end() - 1).as<tests::IntField>();
              field.Name(name);
              field.value(static_cast<int32_t>(value));
            }
          }
        });
      add_segment(gen);
    }

    auto rdr = open_reader();

    MaxMemoryCounter counter;

    // long (20)
    {
      irs::NumericTokenizer stream;
      stream.reset(INT64_C(20));
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("seq", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{21};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // int (21)
    {
      irs::NumericTokenizer stream;
      stream.reset(INT32_C(21));
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("seq", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{22};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        auto* doc = irs::get<irs::DocAttr>(*docs);
        ASSERT_TRUE(bool(doc));
        ASSERT_EQ(docs->value(), doc->value);
        for (; docs->next();) {
          actual.push_back(docs->value());
          ASSERT_EQ(docs->value(), doc->value);
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // double (90.564)
    {
      irs::NumericTokenizer stream;
      stream.reset((double_t)90.564);
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("value", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{13};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        auto* doc = irs::get<irs::DocAttr>(*docs);
        ASSERT_TRUE(bool(doc));
        ASSERT_EQ(docs->value(), doc->value);
        for (; docs->next();) {
          actual.push_back(docs->value());
          ASSERT_EQ(docs->value(), doc->value);
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // float (90.564)
    {
      irs::NumericTokenizer stream;
      stream.reset((float_t)90.564f);
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("value", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{13};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        auto* doc = irs::get<irs::DocAttr>(*docs);
        ASSERT_TRUE(bool(doc));
        ASSERT_EQ(docs->value(), doc->value);
        for (; docs->next();) {
          actual.push_back(docs->value());
          ASSERT_EQ(docs->value(), doc->value);
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // double (100)
    {
      irs::NumericTokenizer stream;
      stream.reset((double_t)100.);
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("value", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{1, 5, 7, 9, 10};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        auto* doc = irs::get<irs::DocAttr>(*docs);
        ASSERT_TRUE(bool(doc));
        ASSERT_EQ(docs->value(), doc->value);
        for (; docs->next();) {
          actual.push_back(docs->value());
          ASSERT_EQ(docs->value(), doc->value);
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // float_t(100)
    {
      irs::NumericTokenizer stream;
      stream.reset((float_t)100.f);
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("value", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{1, 5, 7, 9, 10};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        auto* doc = irs::get<irs::DocAttr>(*docs);
        ASSERT_TRUE(bool(doc));
        ASSERT_EQ(docs->value(), doc->value);
        for (; docs->next();) {
          actual.push_back(docs->value());
          ASSERT_EQ(docs->value(), doc->value);
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // int(100)
    {
      irs::NumericTokenizer stream;
      stream.reset(100);
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("value", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{1, 5, 7, 9, 10};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        auto* doc = irs::get<irs::DocAttr>(*docs);
        ASSERT_TRUE(bool(doc));
        ASSERT_EQ(docs->value(), doc->value);
        for (; docs->next();) {
          actual.push_back(docs->value());
          ASSERT_EQ(docs->value(), doc->value);
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // long(100)
    {
      irs::NumericTokenizer stream;
      stream.reset(INT64_C(100));
      auto* term = irs::get<irs::TermAttr>(stream);
      ASSERT_TRUE(stream.next());

      irs::ByTerm query = MakeFilter("value", irs::ViewCast<char>(term->value));

      auto prepared = query.prepare({
        .index = rdr,
        .memory = counter,
      });

      std::vector<irs::doc_id_t> expected{1, 5, 7, 9, 10};
      std::vector<irs::doc_id_t> actual;

      for (const auto& sub : rdr) {
        auto docs = prepared->execute({.segment = sub});
        auto* doc = irs::get<irs::DocAttr>(*docs);
        ASSERT_TRUE(bool(doc));
        ASSERT_EQ(docs->value(), doc->value);
        for (; docs->next();) {
          actual.push_back(docs->value());
          ASSERT_EQ(docs->value(), doc->value);
        }
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }

  void ByTermSequentialOrder() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    // read segment
    auto rdr = open_reader();

    MaxMemoryCounter counter;

    {
      // create filter
      irs::ByTerm filter = MakeFilter("prefix", "abcy");

      // create order
      size_t collect_field_count = 0;
      size_t collect_term_count = 0;
      size_t finish_count = 0;

      tests::sort::CustomSort scorer;

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

      std::set<irs::doc_id_t> expected{31, 32};
      auto pord = irs::Scorers::Prepare(scorer);
      auto prep = filter.prepare({
        .index = rdr,
        .memory = counter,
        .scorers = pord,
      });
      auto docs = prep->execute({.segment = *(rdr.begin()), .scorers = pord});

      auto* scr = irs::get<irs::ScoreAttr>(*docs);
      ASSERT_FALSE(!scr);

      while (docs->next()) {
        irs::score_t score_value{};
        (*scr)(&score_value);
        IRS_IGNORE(score_value);
        ASSERT_EQ(1, expected.erase(docs->value()));
      }

      ASSERT_TRUE(expected.empty());
      ASSERT_EQ(1, collect_field_count);  // 1 field in 1 segment
      ASSERT_EQ(1, collect_term_count);   // 1 term in 1 field in 1 segment
      ASSERT_EQ(1, finish_count);         // 1 unique term
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }

  void ByTermSequential() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // empty query
    CheckQuery(irs::ByTerm(), Docs{}, rdr);

    // empty term
    CheckQuery(MakeFilter("name", ""), Docs{}, rdr);

    // empty field
    CheckQuery(MakeFilter("", "xyz"), Docs{}, rdr);

    // search : invalid field
    CheckQuery(MakeFilter("invalid_field", "A"), Docs{}, rdr);

    // search : single term
    CheckQuery(MakeFilter("name", "A"), Docs{1}, rdr);

    // search : all terms
    CheckQuery(
      MakeFilter("same", "xyz"),
      Docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
      rdr);

    // search : empty result
    CheckQuery(MakeFilter("same", "invalid_term"), Docs{}, rdr);
  }

  void ByTermSchemas() {
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
      gens.emplace_back(new tests::JsonDocGenerator(
        resource("Northwnd.json"), &tests::GenericJsonFieldFactory));
      gens.emplace_back(new tests::JsonDocGenerator(
        resource("NorthwndEdges.json"), &tests::GenericJsonFieldFactory));
      add_segments(*writer, gens);
    }

    auto rdr = open_reader();
    CheckQuery(MakeFilter("Fields", "FirstName"), Docs{28, 167, 194}, rdr);

    // address to the [SDD-179]
    CheckQuery(MakeFilter("Name", "Product"), Docs{32}, rdr);
  }
};

TEST_P(TermFilterTestCase, by_term) {
  ByTermSequential();
  ByTermSchemas();
}

TEST_P(TermFilterTestCase, by_term_numeric) { ByTermSequentialNumeric(); }

TEST_P(TermFilterTestCase, by_term_order) { ByTermSequentialOrder(); }

TEST_P(TermFilterTestCase, by_term_boost) { ByTermSequentialBoost(); }

TEST_P(TermFilterTestCase, by_term_cost) { ByTermSequentialCost(); }

TEST_P(TermFilterTestCase, visit) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  const std::string_view field = "prefix";
  const auto term = irs::ViewCast<irs::byte_type>(std::string_view("abc"));

  tests::EmptyFilterVisitor visitor;

  // read segment
  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];

  // get term dictionary for field
  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);
  irs::ByTerm::visit(segment, *reader, term, visitor);
  ASSERT_EQ(1, visitor.prepare_calls_counter());
  ASSERT_EQ(1, visitor.visit_calls_counter());
  ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
              {"abc", irs::kNoBoost}}),
            visitor.term_refs<char>());
  visitor.reset();
}

TEST(by_prefix_test, options_simple) {
  irs::ByTermOptions opts;
  ASSERT_TRUE(opts.term.empty());
}

TEST(by_term_test, ctor) {
  irs::ByTerm q;
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), q.type());
  ASSERT_EQ(irs::ByTermOptions{}, q.options());
  ASSERT_EQ("", q.field());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_term_test, equal) {
  irs::ByTerm q = MakeFilter("field", "term");
  ASSERT_EQ(q, MakeFilter("field", "term"));
  ASSERT_NE(q, MakeFilter("field1", "term"));
}

TEST(by_term_test, boost) {
  MaxMemoryCounter counter;

  // no boost
  {
    irs::ByTerm q = MakeFilter("field", "term");

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
    irs::ByTerm q = MakeFilter("field", "term");
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

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(term_filter_test, TermFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5avx")),
                         TermFilterTestCase::to_string);

}  // namespace
