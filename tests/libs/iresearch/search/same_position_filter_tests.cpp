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

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/search/same_position_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kIdId = 1;

void StoreId(irs::IndexWriter::Document& doc, const tests::Document& src) {
  auto* cs = doc.Columnstore();
  if (cs == nullptr) {
    return;
  }
  const auto* field =
    dynamic_cast<const tests::LongField*>(src.indexed.get("_id"));
  if (field == nullptr) {
    return;
  }
  irs::tests::StoreFieldAt(*cs, kIdId, doc.DocId(), *field);
}

}  // namespace

class SamePositionFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  void SubObjectsOrdered() {
    // add segment
    {
      tests::JsonDocGenerator gen(
        resource("phrase_sequential.json"),
        [](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
          if (data.is_string()) {  // field
            doc.insert(
              std::make_shared<tests::TextField<std::string>>(name, data.str),
              true, false);
          } else if (data.is_number()) {  // seq
            const auto value = std::to_string(data.as_number<int64_t>());
            doc.insert(std::make_shared<tests::StringField>(name, value), false,
                       true);
          }
        });
      add_segment(gen);
      gen.reset();
      add_segment(gen, irs::kOmAppend);
    }

    // read segment
    auto index = open_reader();

    MaxMemoryCounter counter;

    // collector count (no branches)
    {
      irs::BySamePosition filter;
      size_t finish_count = 0;

      tests::sort::CustomSort scorer;

      scorer.collectors_collect =
        [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                        const irs::TermCollector*) -> void { ++finish_count; };

      auto prepared = filter.prepare({.index = index, .scorer = &scorer});
      ASSERT_EQ(0, finish_count);
    }

    // collector count (single term)
    {
      irs::BySamePosition filter;
      filter.mutable_options()->terms.emplace_back(
        "phrase", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
      size_t finish_count = 0;
      uint64_t finish_docs_with_field = 0;
      uint64_t finish_docs_with_term = 0;

      tests::sort::CustomSort scorer;

      scorer.collectors_collect = [&](irs::byte_type*,
                                      const irs::FieldCollector* field,
                                      const irs::TermCollector* term) -> void {
        ++finish_count;
        ASSERT_NE(nullptr, field);
        ASSERT_NE(nullptr, term);
        finish_docs_with_field += field->docs_with_field;
        finish_docs_with_term += term->docs_with_term;
      };

      auto prepared = filter.prepare({
        .index = index,
        .memory = counter,
        .scorer = &scorer,
      });
      ASSERT_EQ(1, finish_count);
      ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
      ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // collector count (multiple terms)
    {
      irs::BySamePosition filter;
      filter.mutable_options()->terms.emplace_back(
        "phrase", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
      filter.mutable_options()->terms.emplace_back(
        "phrase", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
      size_t finish_count = 0;
      uint64_t finish_docs_with_field = 0;
      uint64_t finish_docs_with_term = 0;

      tests::sort::CustomSort scorer;

      scorer.collectors_collect = [&](irs::byte_type*,
                                      const irs::FieldCollector* field,
                                      const irs::TermCollector* term) -> void {
        ++finish_count;
        ASSERT_NE(nullptr, field);
        ASSERT_NE(nullptr, term);
        finish_docs_with_field += field->docs_with_field;
        finish_docs_with_term += term->docs_with_term;
      };

      auto prepared = filter.prepare({.index = index, .scorer = &scorer});
      ASSERT_EQ(2, finish_count);
      ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
      ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
    }
  }

  void SubObjectsUnordered() {
    // add segment
    tests::JsonDocGenerator gen(
      resource("same_position.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        typedef tests::TextField<std::string> TextField;
        if (data.is_string()) {
          // a || b || c
          doc.indexed.push_back(std::make_shared<TextField>(name, data.str));
        } else if (data.is_number()) {
          // _id
          const auto l_value = data.as_number<int64_t>();

          // 'value' can be interpreted as a double
          doc.insert(std::make_shared<tests::LongField>());
          auto& field = (doc.indexed.end() - 1).as<tests::LongField>();
          field.Name(name);
          field.value(l_value);
        }
      });
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                &StoreId);

    // read segment
    auto index = open_reader(irs::tests::DefaultReaderOptions());
    ASSERT_EQ(1, index.size());
    auto& segment = *(index.begin());

    irs::BytesViewInput in;
    const auto* column = segment.Column(kIdId);
    ASSERT_NE(nullptr, column);

    // empty query
    {
      irs::BySamePosition q;
      auto prepared = q.prepare({.index = index});
      auto docs = prepared->execute({.segment = segment});
      ASSERT_FALSE(docs->next());
    }

    // { a: 100 } - equal to 'by_term'
    {
      irs::BySamePosition query;
      query.mutable_options()->terms.emplace_back(
        "a", irs::ViewCast<irs::byte_type>(std::string_view("100")));

      irs::ByTerm expected_query;
      *expected_query.mutable_field() = "a";
      expected_query.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("100"));

      auto prepared = query.prepare({.index = index});
      auto expected_prepared = expected_query.prepare({.index = index});

      auto docs = prepared->execute({.segment = segment});
      auto expected_docs = prepared->execute({.segment = segment});

      ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
      while (expected_docs->next()) {
        ASSERT_TRUE(docs->next());
        ASSERT_EQ(expected_docs->value(), docs->value());
      }
      ASSERT_FALSE(docs->next());
      ASSERT_EQ(irs::doc_limits::eof(), docs->value());
    }

    // check document with first position
    // { a: 300, b:90, c:9 }
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "a", irs::ViewCast<irs::byte_type>(std::string_view("300")));
      q.mutable_options()->terms.emplace_back(
        "b", irs::ViewCast<irs::byte_type>(std::string_view("90")));
      q.mutable_options()->terms.emplace_back(
        "c", irs::ViewCast<irs::byte_type>(std::string_view("9")));
      auto prepared = q.prepare({.index = index});
      auto docs = prepared->execute({.segment = segment});
      ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(1, docs->value());
    }

    // { a: 100, b:30, c:6 }
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "a", irs::ViewCast<irs::byte_type>(std::string_view("100")));
      q.mutable_options()->terms.emplace_back(
        "b", irs::ViewCast<irs::byte_type>(std::string_view("30")));
      q.mutable_options()->terms.emplace_back(
        "c", irs::ViewCast<irs::byte_type>(std::string_view("6")));

      auto prepared = q.prepare({.index = index});

      // next
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs = prepared->execute({.segment = segment});
        ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(6, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(27, irs::ReadZV64(in));
        ASSERT_FALSE(docs->next());
        ASSERT_EQ(irs::doc_limits::eof(), docs->value());
      }

      // seek
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs = prepared->execute({.segment = segment});
        ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
        ASSERT_EQ((irs::doc_limits::min)() + 6,
                  docs->seek((irs::doc_limits::min)()));
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(6, irs::ReadZV64(in));
        ASSERT_EQ((irs::doc_limits::min)() + 27, docs->seek(27));
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(27, irs::ReadZV64(in));
        ASSERT_EQ((irs::doc_limits::min)() + 27,
                  docs->seek(8));  // seek backwards
        ASSERT_EQ((irs::doc_limits::min)() + 27,
                  docs->seek(27));  // seek to same position
        ASSERT_FALSE(docs->next());
        ASSERT_EQ(irs::doc_limits::eof(), docs->value());
      }
    }

    // { c: 8, b:80, a:700 }
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "c", irs::ViewCast<irs::byte_type>(std::string_view("8")));
      q.mutable_options()->terms.emplace_back(
        "b", irs::ViewCast<irs::byte_type>(std::string_view("80")));
      q.mutable_options()->terms.emplace_back(
        "a", irs::ViewCast<irs::byte_type>(std::string_view("700")));

      auto prepared = q.prepare({.index = index});

      // next
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs = prepared->execute({.segment = segment});
        ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(14, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(91, irs::ReadZV64(in));
        ASSERT_FALSE(docs->next());
        ASSERT_EQ(irs::doc_limits::eof(), docs->value());
      }

      // seek
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs = prepared->execute({.segment = segment});
        ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
        ASSERT_EQ((irs::doc_limits::min)() + 91, docs->seek(27));
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(91, irs::ReadZV64(in));
        ASSERT_EQ((irs::doc_limits::min)() + 91,
                  docs->seek(8));  // seek backwards
        ASSERT_EQ((irs::doc_limits::min)() + 91,
                  docs->seek(27));  // seek to same position
        ASSERT_FALSE(docs->next());
        ASSERT_EQ(irs::doc_limits::eof(), docs->value());
      }
    }

    // { a: 700, b:*, c: 7 }
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "a", irs::ViewCast<irs::byte_type>(std::string_view("700")));
      q.mutable_options()->terms.emplace_back(
        "c", irs::ViewCast<irs::byte_type>(std::string_view("7")));

      auto prepared = q.prepare({.index = index});

      // next
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs = prepared->execute({.segment = segment});
        ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(1, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(6, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(11, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(17, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(18, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(23, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(24, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(28, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(38, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(51, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(66, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(79, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(89, irs::ReadZV64(in));
        ASSERT_FALSE(docs->next());
        ASSERT_EQ(irs::doc_limits::eof(), docs->value());
      }

      // seek + next
      {
        irs::tests::BlobPointReader values{segment, *column};

        auto docs = prepared->execute({.segment = segment});
        ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(1, irs::ReadZV64(in));
        ASSERT_EQ((irs::doc_limits::min)() + 28,
                  docs->seek((irs::doc_limits::min)() + 28));
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(28, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(38, irs::ReadZV64(in));
        ASSERT_EQ((irs::doc_limits::min)() + 51, docs->seek(45));
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(51, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(66, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(79, irs::ReadZV64(in));
        ASSERT_TRUE(docs->next());
        in.reset(values.Get(docs->value()));
        ASSERT_EQ(89, irs::ReadZV64(in));
        ASSERT_FALSE(docs->next());
        ASSERT_EQ(irs::doc_limits::eof(), docs->value());
      }

      // seek to the end
      {
        auto docs = prepared->execute({.segment = segment});
        ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
        ASSERT_EQ(irs::doc_limits::eof(), docs->seek(irs::doc_limits::eof()));
        ASSERT_FALSE(docs->next());
        ASSERT_EQ(irs::doc_limits::eof(), docs->value());
      }
    }
  }
};

TEST_P(SamePositionFilterTestCase, by_same_position) {
  SubObjectsOrdered();
  SubObjectsUnordered();
}

TEST(by_same_position_test, options) {
  irs::BySamePositionOptions opts;
  ASSERT_TRUE(opts.terms.empty());
}

TEST(by_same_position_test, ctor) {
  irs::BySamePosition q;
  ASSERT_EQ(irs::Type<irs::BySamePosition>::id(), q.type());
  ASSERT_EQ(irs::BySamePositionOptions{}, q.options());
  ASSERT_EQ(irs::kNoBoost, q.Boost());

  static_assert((irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) ==
                irs::BySamePosition::kRequiredFeatures);
}

TEST(by_same_position_test, boost) {
  // no boost
  {
    (void)1;  // format work-around
    // no branches
    {
      irs::BySamePosition q;

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }

    // single term
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "field", irs::ViewCast<irs::byte_type>(std::string_view("quick")));

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }

    // multiple terms
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "field", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
      q.mutable_options()->terms.emplace_back(
        "field", irs::ViewCast<irs::byte_type>(std::string_view("brown")));

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }
  }

  // with boost
  {
    irs::score_t boost = 1.5f;

    // no terms, return empty query
    {
      irs::BySamePosition q;
      q.boost(boost);

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }

    // single term
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "field", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
      q.boost(boost);

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(boost, prepared->Boost());
    }

    // single multiple terms
    {
      irs::BySamePosition q;
      q.mutable_options()->terms.emplace_back(
        "field", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
      q.mutable_options()->terms.emplace_back(
        "field", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
      q.boost(boost);

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(boost, prepared->Boost());
    }
  }
}

TEST(by_same_position_test, equal) {
  ASSERT_EQ(irs::BySamePosition(), irs::BySamePosition());

  {
    irs::BySamePosition q0;
    q0.mutable_options()->terms.emplace_back(
      "speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q0.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));

    irs::BySamePosition q1;
    q1.mutable_options()->terms.emplace_back(
      "speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q1.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    ASSERT_EQ(q0, q1);
  }

  {
    irs::BySamePosition q0;
    q0.mutable_options()->terms.emplace_back(
      "speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q0.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    q0.mutable_options()->terms.emplace_back(
      "name", irs::ViewCast<irs::byte_type>(std::string_view("fox")));

    irs::BySamePosition q1;
    q1.mutable_options()->terms.emplace_back(
      "speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q1.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    q1.mutable_options()->terms.emplace_back(
      "name", irs::ViewCast<irs::byte_type>(std::string_view("squirrel")));
    ASSERT_NE(q0, q1);
  }

  {
    irs::BySamePosition q0;
    q0.mutable_options()->terms.emplace_back(
      "Speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q0.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    q0.mutable_options()->terms.emplace_back(
      "name", irs::ViewCast<irs::byte_type>(std::string_view("fox")));

    irs::BySamePosition q1;
    q1.mutable_options()->terms.emplace_back(
      "speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q1.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    q1.mutable_options()->terms.emplace_back(
      "name", irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    ASSERT_NE(q0, q1);
  }

  {
    irs::BySamePosition q0;
    q0.mutable_options()->terms.emplace_back(
      "speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q0.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));

    irs::BySamePosition q1;
    q1.mutable_options()->terms.emplace_back(
      "speed", irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    q1.mutable_options()->terms.emplace_back(
      "color", irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    q1.mutable_options()->terms.emplace_back(
      "name", irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    ASSERT_NE(q0, q1);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(same_position_filter_test, SamePositionFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         SamePositionFilterTestCase::to_string);
