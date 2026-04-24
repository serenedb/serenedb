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
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/term_query.hpp"
#include "tests_shared.hpp"

namespace tests {

void AnalyzedJsonFieldFactory(tests::Document& doc, const std::string& name,
                              const tests::JsonDocGenerator::JsonValue& data) {
  typedef TextField<std::string> TextField;

  class StringField : public tests::StringField {
   public:
    StringField(const std::string& name, const std::string_view& value)
      : tests::StringField(name, value, irs::IndexFeatures::Freq) {}
  };

  if (data.is_string()) {
    // analyzed field
    doc.indexed.push_back(
      std::make_shared<TextField>(std::string(name.data()) + "_anl", data.str));

    // not analyzed field
    doc.insert(std::make_shared<StringField>(name, data.str));
  }
}

}  // namespace tests

class PhraseFilterTestCase : public tests::FilterTestCaseBase {};

TEST_P(PhraseFilterTestCase, sequential_one_term) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  // read segment
  auto rdr = open_reader();

  // empty field
  {
    irs::ByPhrase q;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // empty phrase
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // equals to term_filter "fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // prefix_filter "fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Y", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fo%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByWildcardOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Y", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "%ox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByWildcardOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("%ox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f%x"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByWildcardOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("_ox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f_x"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByWildcardOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("f_x"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fo_"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByWildcardOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fo_"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByWildcardOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // levenshtein_filter "fox" max_distance = 0
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 0;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // levenshtein_filter "fol"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fol"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByTermsOptions "fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByTermsOptions "fox|that"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // by_range_filter_options "[x0, x0]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X0",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x0]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "[x0, x0)"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x0)"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // by_range_filter_options "[x0, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X0",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X1",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X2",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X3",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X5",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X1",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X2",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X3",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X5",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "[x0, x2)"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X0",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X1",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X3",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x2)"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X1",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X3",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fox" on field without positions
  // which is ok for single word phrases
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fo*" on field without positions
  // which is ok for the first word in phrase
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase";
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fo%" on field without positions
  // which is ok for first word in phrase
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "f_x%" on field without positions
  // which is ok for first word in phrase
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("f_x%"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fxo" on field without positions
  // which is ok for single word phrases
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.with_transpositions = true;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fxo"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search ByRangeOptions "[x0, x1]" on field without positions
  // which is ok for first word in phrase
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X0",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X1",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // term_filter "fox" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // prefix_filter "fo*" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Y", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fo%" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Y", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f%x" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("f%x"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f%x" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>(
      std::numeric_limits<size_t>::max());
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fkx"));

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search ByRangeOptions "[x0, x1]" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>(
      std::numeric_limits<size_t>::max());
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.get()));
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X0",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X1",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X3",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sequential_three_terms) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  // read segment
  auto rdr = open_reader();

  // "quick brown fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui* brown fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% brown fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "q%ck brown fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("q%ck"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown fox" simple term max_distance = 0
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 0;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quck brown fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quck"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] x0 x2
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x2"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick bro* fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick bro% fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("bro%"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick b%w_ fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("b%w_"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brkln fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("brkln"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "x1 [x0, x1] x2"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x2"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick brown fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown fo%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown f_x"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("f_x"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown fxo"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.with_transpositions = true;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fxo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "x1 x0 [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qui* bro* fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% bro% fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("bro%"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% b%o__ fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("b%o__"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui bro fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt1 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt1.max_distance = 2;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    auto& lt2 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("brow"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] [x0, x1] x2"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt1 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt1.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt1.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt1.range.min_type = irs::BoundType::Inclusive;
    rt1.range.max_type = irs::BoundType::Inclusive;
    auto& rt2 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt2.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt2.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt2.range.min_type = irs::BoundType::Inclusive;
    rt2.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x2"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qui* brown fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% brown fo%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "q_i% brown f%x"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("q_i%"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("f%x"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] x0 [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt1 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt1.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt1.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt1.range.min_type = irs::BoundType::Inclusive;
    rt1.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    auto& rt2 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt2.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt2.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt2.range.min_type = irs::BoundType::Inclusive;
    rt2.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qoick br__nn fix"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt1 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt1.max_distance = 1;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qoick"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("br__n"));
    auto& lt2 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fix"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick bro* fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick bro% fo%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("bro%"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick b_o% f_%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("b_o%"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("f_%"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "x1 [x0, x1] [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    auto& rt1 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt1.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt1.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt1.range.min_type = irs::BoundType::Inclusive;
    rt1.range.max_type = irs::BoundType::Inclusive;
    auto& rt2 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt2.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt2.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt2.range.min_type = irs::BoundType::Inclusive;
    rt2.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qui* bro* fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt3 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    pt3.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Y", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% bro% fo%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt3 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%"));
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("bro%"));
    wt3.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));

    size_t collect_field_count = 0;
    size_t collect_term_count = 0;
    size_t finish_count = 0;

    tests::sort::CustomSort sort;

    sort.collector_collect_field = [&collect_field_count](
                                     const irs::SubReader&,
                                     const irs::TermReader&) -> void {
      ++collect_field_count;
    };
    sort.collector_collect_term =
      [&collect_term_count](const irs::SubReader&, const irs::TermReader&,
                            const irs::AttributeProvider&) -> void {
      ++collect_term_count;
    };
    sort.collectors_collect =
      [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                      const irs::TermCollector*) -> void { ++finish_count; };
    sort.prepare_field_collector = [&sort]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(sort);
    };
    sort.prepare_term_collector = [&sort]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(sort);
    };

    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    ASSERT_EQ(1, collect_field_count);  // 1 field in 1 segment
    ASSERT_EQ(6, collect_term_count);   // 6 different terms
    ASSERT_EQ(6, finish_count);         // 6 sub-terms in phrase

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    // no order passed - no frequency
    {
      auto docs = prepared->execute({.segment = *sub});
      ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
      ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    }

    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Y", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "q%ic_ br_wn _%x"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt3 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("q%ic_"));
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("br_wn"));
    wt3.term = irs::ViewCast<irs::byte_type>(std::string_view("_%x"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick|quilt|hhh brown|brother fox"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& st1 = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    st1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("quilt")));
    st1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("hhh")));
    auto& st2 = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st2.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    st2.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("brother")));
    auto& st3 = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st3.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] [x0, x1] [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt1 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    auto& rt2 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    auto& rt3 = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt1.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt1.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt1.range.min_type = irs::BoundType::Inclusive;
    rt1.range.max_type = irs::BoundType::Inclusive;
    rt2.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt2.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt2.range.min_type = irs::BoundType::Inclusive;
    rt2.range.max_type = irs::BoundType::Inclusive;
    rt3.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt3.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt3.range.min_type = irs::BoundType::Inclusive;
    rt3.range.max_type = irs::BoundType::Inclusive;

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick brown fox" with order
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    size_t collect_field_count = 0;
    size_t collect_term_count = 0;
    size_t finish_count = 0;

    tests::sort::CustomSort sort;

    sort.collector_collect_field = [&collect_field_count](
                                     const irs::SubReader&,
                                     const irs::TermReader&) -> void {
      ++collect_field_count;
    };
    sort.collector_collect_term =
      [&collect_term_count](const irs::SubReader&, const irs::TermReader&,
                            const irs::AttributeProvider&) -> void {
      ++collect_term_count;
    };
    sort.collectors_collect =
      [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                      const irs::TermCollector*) -> void { ++finish_count; };
    sort.prepare_field_collector = [&sort]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(sort);
    };
    sort.prepare_term_collector = [&sort]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(sort);
    };
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    ASSERT_EQ(1, collect_field_count);  // 1 field in 1 segment
    ASSERT_EQ(3, collect_term_count);   // 3 different terms
    ASSERT_EQ(3, finish_count);         // 3 sub-terms in phrase
    auto sub = rdr.begin();

    // no order passed - no frequency
    {
      auto docs = prepared->execute({.segment = *sub});
      ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
      ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    }

    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    it = docs.get();

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }
}

TEST_P(PhraseFilterTestCase, sequential_several_terms) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  // read segment
  auto rdr = open_reader();

  // "fox ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "f_x ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("f_x"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fpx ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fpx"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui%ck"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%ck"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... qui*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "f%x ... qui%ck"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("f%x"));
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%ck"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fx ... quik"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt1 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    auto& lt2 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>(1);
    lt1.max_distance = 1;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fx"));
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("quik"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fx ... quik"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt1 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    auto& lt2 = q.mutable_options()->push_back<irs::ByEditDistanceOptions>(1);
    lt1.max_distance = 1;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fx"));
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("quik"));

    auto scorer = irs::scorers::Get(
      "bm25", irs::Type<irs::text_format::Json>::get(), "{ \"b\" : 0 }");

    auto prepared = q.prepare({.index = rdr, .scorer = scorer.get()});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_FLOAT_EQ((0.5f + 0.75f) / 2, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_FLOAT_EQ(boost->value[0],
                    irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ((0.5f + 0.75f) / 2, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_FLOAT_EQ(boost->value[0],
                    irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // =============================
  // "fo* ... qui*" with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    auto scorer = irs::scorers::Get(
      "bm25", irs::Type<irs::text_format::Json>::get(), "{ \"b\" : 0 }");

    auto prepared = q.prepare({.index = rdr, .scorer = scorer.get()});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // =============================
  // jumps ... (jumps|hotdog|the) with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pos0 = q.mutable_options()->push_back<irs::ByTermsOptions>();
    pos0.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("jumps")));
    auto& pos1 = q.mutable_options()->push_back<irs::ByTermsOptions>(1);
    pos1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("jumps")),
                       0.25f);
    pos1.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("hotdog")), 0.5f);
    pos1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("the")),
                       0.75f);

    auto scorer = irs::scorers::Get(
      "bm25", irs::Type<irs::text_format::Json>::get(), "{ \"b\" : 0 }");

    auto prepared = q.prepare({.index = rdr, .scorer = scorer.get()});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.75f) / 2, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ(((1.f + 0.25f) / 2 + (1.f + 0.5f) / 2) / 2,
                    boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("O", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("P", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(3, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Q", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("R", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // ByTermsOptions "fox|that" with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    auto scorer = irs::scorers::Get(
      "bm25", irs::Type<irs::text_format::Json>::get(), "{ \"b\" : 0 }");

    auto prepared = q.prepare({.index = rdr, .scorer = scorer.get()});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByTermsOptions "fox|that" with scorer and boost
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")),
                     0.5f);
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    auto scorer = irs::scorers::Get(
      "bm25", irs::Type<irs::text_format::Json>::get(), "{ \"b\" : 0 }");

    auto prepared = q.prepare({.index = rdr, .scorer = scorer.get()});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(irs::kNoBoost, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // test disjunctions (unary, basic, small, disjunction)
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("%las"));
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("%nd"));
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("go"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("like"));

    auto scorer = irs::scorers::Get(
      "bm25", irs::Type<irs::text_format::Json>::get(), "{ \"b\" : 0 }");

    auto prepared = q.prepare({.index = rdr, .scorer = scorer.get()});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = scorer.get(),

    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Z", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // =============================

  // "fox ... quick" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox quick"
  // const_max and zero offset
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(0).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox* quick*"
  // const_max and zero offset
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(0);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... quick" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "f_x ... quick" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("f_x"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui*" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui%k" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%k"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... qui*" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo% ... qui%" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui%"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo% ... quik" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>(1);
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quik"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(10).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... qui*"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(10);
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... qu_ck"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>(10);
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("qu_ck"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... quc"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>(10);
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quc"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "eye ... eye"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("eye"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("eye"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in the past we are looking forward"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("as"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("in"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("the"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("past"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("we"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("are"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("looking"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("forward"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in % past we ___ looking forward"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("ass"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("in"));
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("%"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("past"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("we"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("___"));
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("looking")));
    st.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("searching")));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in the past we are looking forward" with order
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("as"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("in"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("the"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("past"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("we"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("are"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("looking"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("forward"));

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto score = it->PrepareScore({
      .scorer = &sort,
      .segment = &*sub,
    });

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    irs::score_t score_value{};
    score.Score(&score_value, 1);
    ASSERT_EQ(docs->value(), score_value);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in the p_st we are look* forward" with order
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("as"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("in"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("the"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("p_st"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("we"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("are"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("look"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("forward"));

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto score = docs->PrepareScore({
      .scorer = &sort,
      .segment = &*sub,
    });

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    irs::score_t score_value{};
    score.Score(&score_value, 1);
    ASSERT_EQ(docs->value(), score_value);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // fox quick
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    // Check repeatable seek to the same document given frequency of the phrase
    // within the document = 2
    auto v = docs->value();
    ASSERT_EQ(v, docs->seek(docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // fox quick with order
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // wildcard_filter "zo\\_%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("zo\\_%"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("PHW0",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "\\_oo"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("\\_oo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("PHW1",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "z\\_o"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("z\\_o"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("PHW2",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "elephant giraff\\_%"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("giraff\\_%"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("PHW3",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "elephant \\_iraffe"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("\\_iraffe"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("PHW4",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "elephant gira\\_fe"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("gira\\_fe"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("PHW5",
              irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, interval_several_terms) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_interval.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  // read segment
  auto rdr = open_reader();

  // "fox ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... quick ... brown"
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(4, 5).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("E", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "pox ... quick ... brown" check for proper accounting of interval
  // adjustments
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("pox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_FALSE(docs->next());
  }

  // mix interval and single
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("jumps"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("second"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field() = "phrase_anl";
      ph.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fox"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off).term =
        irs::ViewCast<irs::byte_type>(std::string_view("second"));
    };
    add_phrase(0);
    add_phrase(1);
    add_phrase(2);

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };
    auto sub = rdr.begin();
    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    tests::sort::FrequencyScore freq_score;
    auto disj_prepared = disjunction.prepare({
      .index = rdr,
      .scorer = &freq_score,
    });
    auto disj_docs = disj_prepared->execute({
      .segment = *sub,
      .scorer = &freq_score,

    });
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    it = docs.get();

    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("C", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("D", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("E", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
    ASSERT_FALSE(disj_docs->next());
  }

  // mix interval and single
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("long"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("road"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("to"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("teppereri"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
      irs::ViewCast<irs::byte_type>(std::string_view("yes"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("to"));

    auto prepared = q.prepare({.index = rdr});

    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q.mutable_options()->push_back<irs::ByPrefixOptions>(3, 4);
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("B", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("E", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // fixed interval ordered
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field() = "phrase_anl";
      ph.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fox"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off1).term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    };
    add_phrase(1, 1);
    add_phrase(1, 2);
    add_phrase(1, 3);
    add_phrase(2, 1);
    add_phrase(2, 2);
    add_phrase(2, 3);
    add_phrase(3, 1);
    add_phrase(3, 2);
    add_phrase(3, 3);

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto* freq_seek = irs::get<irs::FreqBlockAttr>(*docs_seek);
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    auto disj_prepared = disjunction.prepare({
      .index = rdr,
      .scorer = &freq_score,
    });
    auto disj_docs = disj_prepared->execute({
      .segment = *sub,
      .scorer = &freq_score,

    });
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("E", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(1, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(6, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(11, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(2, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
    ASSERT_FALSE(disj_docs->next());
  }

  // variadic interval ordered
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    q.mutable_options()->push_back<irs::ByPrefixOptions>(4, 5).term =
      irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByPrefixOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("bro"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field() = "phrase_anl";
      ph.mutable_options()->push_back<irs::ByPrefixOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fo"));
      ph.mutable_options()->push_back<irs::ByPrefixOptions>(off1).term =
        irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      ph.mutable_options()->push_back<irs::ByPrefixOptions>(off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    };
    add_phrase(3, 1);
    add_phrase(3, 2);
    add_phrase(4, 1);
    add_phrase(4, 2);

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto* freq_seek = irs::get<irs::FreqBlockAttr>(*docs_seek);
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    auto disj_prepared = disjunction.prepare({
      .index = rdr,
      .scorer = &freq_score,
    });
    auto disj_docs = disj_prepared->execute({
      .segment = *sub,
      .scorer = &freq_score,

    });
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("E", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(1, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("F", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(5, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("H", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
    ASSERT_FALSE(disj_docs->next());
  }

  // fixed interval ordered last only repeated
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("zoo"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 4).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field() = "phrase_anl";
      ph.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("zoo"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off1).term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    };
    add_phrase(0, 0);
    add_phrase(0, 1);
    add_phrase(0, 2);
    add_phrase(0, 3);

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto docs = prepared->execute({.segment = *sub});
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto* freq_seek = irs::get<irs::FreqBlockAttr>(*docs_seek);
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    auto disj_prepared = disjunction.prepare({
      .index = rdr,
      .scorer = &freq_score,
    });
    auto disj_docs = disj_prepared->execute({
      .segment = *sub,
      .scorer = &freq_score,

    });
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(disj_docs->next());
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_FALSE(disj_docs->next());
  }
}

TEST(by_phrase_test, options) {
  irs::ByPhraseOptions opts;
  ASSERT_TRUE(opts.simple());
  ASSERT_TRUE(opts.empty());
  ASSERT_EQ(0, opts.size());
  ASSERT_EQ(opts.begin(), opts.end());
}

TEST(by_phrase_test, options_clear) {
  irs::ByPhraseOptions opts;
  ASSERT_TRUE(opts.simple());
  ASSERT_TRUE(opts.empty());
  ASSERT_EQ(0, opts.size());
  opts.push_back<irs::ByTermOptions>();
  ASSERT_EQ(1, opts.size());
  ASSERT_FALSE(opts.empty());
  ASSERT_TRUE(opts.simple());
  opts.push_back<irs::ByTermOptions>();
  ASSERT_EQ(2, opts.size());
  ASSERT_FALSE(opts.empty());
  ASSERT_TRUE(opts.simple());
  opts.push_back<irs::ByPrefixOptions>();
  ASSERT_EQ(3, opts.size());
  ASSERT_FALSE(opts.empty());
  ASSERT_FALSE(opts.simple());
  opts.clear();
  ASSERT_TRUE(opts.simple());
  ASSERT_TRUE(opts.empty());
  ASSERT_EQ(0, opts.size());
}

TEST(by_phrase_test, ctor) {
  irs::ByPhrase q;
  ASSERT_EQ(irs::Type<irs::ByPhrase>::id(), q.type());
  ASSERT_EQ("", q.field());
  ASSERT_EQ(irs::ByPhraseOptions{}, q.options());
  ASSERT_EQ(irs::kNoBoost, q.Boost());

  static_assert((irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) ==
                irs::FixedPhraseQuery::kRequiredFeatures);
  static_assert((irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) ==
                irs::VariadicPhraseQuery::kRequiredFeatures);
}

TEST(by_phrase_test, boost) {
  {
    irs::ByPhrase q;
    *q.mutable_field() = "field";

    auto prepared = q.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }

  // single term
  {
    irs::ByPhrase q;
    *q.mutable_field() = "field";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    auto prepared = q.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }

  // multiple terms
  {
    irs::ByPhrase q;
    *q.mutable_field() = "field";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    auto prepared = q.prepare({.index = irs::SubReader::empty()});
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }

  // with boost
  {
    MaxMemoryCounter counter;
    irs::score_t boost = 1.5f;

    // no terms, return empty query
    {
      irs::ByPhrase q;
      *q.mutable_field() = "field";
      q.boost(boost);

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }

    // single term
    {
      irs::ByPhrase q;
      *q.mutable_field() = "field";
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
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

    // single multiple terms
    {
      irs::ByPhrase q;
      *q.mutable_field() = "field";
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
      q.boost(boost);

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(boost, prepared->Boost());
    }

    // prefix, wildcard, levenshtein, set, range
    {
      irs::ByPhrase q;
      *q.mutable_field() = "field";
      auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt.term = irs::ViewCast<irs::byte_type>(std::string_view("qu__k"));
      auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
      lt.max_distance = 1;
      lt.term = irs::ViewCast<irs::byte_type>(std::string_view("brwn"));
      q.boost(boost);
      auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
      st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
      st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("dob")));
      auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
      rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("forward"));
      rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("forward"));
      rt.range.min_type = irs::BoundType::Inclusive;
      rt.range.max_type = irs::BoundType::Inclusive;

      auto prepared = q.prepare({.index = irs::SubReader::empty()});
      ASSERT_EQ(boost, prepared->Boost());
    }
  }
}

TEST(by_phrase_test, push_back) {
  irs::ByPhraseOptions q;

  // push_back
  {
    q.push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    ASSERT_FALSE(q.empty());
    ASSERT_EQ(3, q.size());

    // check elements via positions
    {
      auto it = q.begin();
      ASSERT_NE(it, q.end());
      const auto& st1 = std::get<irs::ByTermOptions>(it->part);
      ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("quick")),
                st1.term);
      ++it;
      ASSERT_NE(it, q.end());
      const auto& st2 = std::get<irs::ByTermOptions>(it->part);
      ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("brown")),
                st2.term);
      ++it;
      ASSERT_NE(it, q.end());
      const auto& st3 = std::get<irs::ByTermOptions>(it->part);
      ASSERT_EQ(irs::ViewCast<irs::byte_type>(std::string_view("fox")),
                st3.term);
      ++it;
      ASSERT_EQ(it, q.end());
    }

    // push term
    {
      irs::ByTermOptions st1;
      st1.term = irs::ViewCast<irs::byte_type>(std::string_view("squirrel"));
      q.push_back(st1);
      const auto& st2 = std::get<irs::ByTermOptions>((--q.end())->part);
      ASSERT_EQ(st1, st2);

      irs::ByPrefixOptions pt1;
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("cat"));
      q.push_back(pt1);
      const auto& pt2 = std::get<irs::ByPrefixOptions>((--q.end())->part);
      ASSERT_EQ(pt1, pt2);

      irs::ByWildcardOptions wt1;
      wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("dog"));
      q.push_back(wt1);
      const auto& wt2 = std::get<irs::ByWildcardOptions>((--q.end())->part);
      ASSERT_EQ(wt1, wt2);

      irs::ByEditDistanceOptions lt1;
      lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("whale"));
      q.push_back(lt1);
      const auto& lt2 = std::get<irs::ByEditDistanceOptions>((--q.end())->part);
      ASSERT_EQ(lt1, lt2);

      irs::ByTermsOptions ct1;
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("bird")));
      q.push_back(ct1);
      const auto& ct2 = std::get<irs::ByTermsOptions>((--q.end())->part);
      ASSERT_EQ(ct1, ct2);

      irs::ByRangeOptions rt1;
      rt1.range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.min_type = irs::BoundType::Inclusive;
      rt1.range.max_type = irs::BoundType::Inclusive;
      q.push_back(rt1);
      const auto& rt2 = std::get<irs::ByRangeOptions>((--q.end())->part);
      ASSERT_EQ(rt1, rt2);
    }
    ASSERT_EQ(9, q.size());
  }
}

TEST(by_phrase_test, equal) {
  ASSERT_EQ(irs::ByPhrase(), irs::ByPhrase());

  {
    irs::ByPhrase q0;
    *q0.mutable_field() = "name";
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::ByPhrase q1;
    *q1.mutable_field() = "name";
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_EQ(q0, q1);
  }

  {
    irs::ByPhrase q0;
    {
      *q0.mutable_field() = "name";
      auto& pt1 = q0.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& ct1 = q0.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q0.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("br_wn"));
      auto& lt1 = q0.mutable_options()->push_back<irs::ByEditDistanceOptions>();
      lt1.max_distance = 2;
      lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
      auto& rt1 = q0.mutable_options()->push_back<irs::ByRangeOptions>();
      rt1.range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.min_type = irs::BoundType::Inclusive;
      rt1.range.max_type = irs::BoundType::Inclusive;
    }

    irs::ByPhrase q1;
    {
      *q1.mutable_field() = "name";
      auto& pt1 = q1.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& ct1 = q1.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q1.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("br_wn"));
      auto& lt1 = q1.mutable_options()->push_back<irs::ByEditDistanceOptions>();
      lt1.max_distance = 2;
      lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
      auto& rt1 = q1.mutable_options()->push_back<irs::ByRangeOptions>();
      rt1.range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.min_type = irs::BoundType::Inclusive;
      rt1.range.max_type = irs::BoundType::Inclusive;
    }

    ASSERT_EQ(q0, q1);
  }

  {
    irs::ByPhrase q0;
    *q0.mutable_field() = "name";
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("squirrel"));

    irs::ByPhrase q1;
    *q1.mutable_field() = "name";
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_NE(q0, q1);
  }

  {
    irs::ByPhrase q0;
    *q0.mutable_field() = "name1";
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::ByPhrase q1;
    *q1.mutable_field() = "name";
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_NE(q0, q1);
  }

  {
    irs::ByPhrase q0;
    *q0.mutable_field() = "name";
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    irs::ByPhrase q1;
    *q1.mutable_field() = "name";
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_NE(q0, q1);
  }

  {
    irs::ByPhrase q0;
    {
      *q0.mutable_field() = "name";
      auto& pt1 = q0.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("quil"));
      auto& ct1 = q0.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q0.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("br_wn"));
      auto& lt1 = q0.mutable_options()->push_back<irs::ByEditDistanceOptions>();
      lt1.max_distance = 2;
      lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
      auto& rt1 = q0.mutable_options()->push_back<irs::ByRangeOptions>();
      rt1.range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.min_type = irs::BoundType::Inclusive;
      rt1.range.max_type = irs::BoundType::Inclusive;
    }

    irs::ByPhrase q1;
    {
      *q1.mutable_field() = "name";
      auto& pt1 = q1.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& ct1 = q1.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q1.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("br_wn"));
      auto& lt1 = q1.mutable_options()->push_back<irs::ByEditDistanceOptions>();
      lt1.max_distance = 2;
      lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
      auto& rt1 = q1.mutable_options()->push_back<irs::ByRangeOptions>();
      rt1.range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
      rt1.range.min_type = irs::BoundType::Inclusive;
      rt1.range.max_type = irs::BoundType::Inclusive;
    }

    ASSERT_NE(q0, q1);
  }
}

TEST(by_phrase_test, copy_move) {
  {
    irs::ByTermOptions st;
    st.term = irs::ViewCast<irs::byte_type>(std::string_view("very"));
    irs::ByPrefixOptions pt;
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    irs::ByTermsOptions ct;
    ct.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("light")));
    ct.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("dark")));
    irs::ByWildcardOptions wt;
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("br_wn"));
    irs::ByEditDistanceOptions lt;
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    irs::ByRangeOptions rt;
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    irs::ByPhrase q0;
    *q0.mutable_field() = "name";
    q0.mutable_options()->push_back(st);
    q0.mutable_options()->push_back(pt);
    q0.mutable_options()->push_back(ct);
    q0.mutable_options()->push_back(wt);
    q0.mutable_options()->push_back(lt);
    q0.mutable_options()->push_back(rt);
    q0.mutable_options()->push_back(std::move(st));
    q0.mutable_options()->push_back(std::move(pt));
    q0.mutable_options()->push_back(std::move(ct));
    q0.mutable_options()->push_back(std::move(wt));
    q0.mutable_options()->push_back(std::move(lt));
    q0.mutable_options()->push_back(std::move(rt));

    irs::ByPhrase q1 = q0;
    ASSERT_EQ(q0, q1);
    irs::ByPhrase q2 = q0;
    irs::ByPhrase q3 = std::move(q2);
    ASSERT_EQ(q0, q3);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(phrase_filter_test, PhraseFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         PhraseFilterTestCase::to_string);

TEST_P(PhraseFilterTestCase, sloppy_phrase_one_extra_word) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick brown fox" slop=1
  // A,G,I: distance=0. S: distance=1.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_skip_word) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick fox" slop=1
  // A,G,I,T: distance=1. N: distance=0 (best combo).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_reversal) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "fox brown" slop=2
  // A,G,I,S: distance=2 (reversal). L,T: distance=0 (correct order).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(2);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox brown" slop=1: reversal costs 2, only L,T match (distance=0).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_no_match) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "brown dog" slop=1: only A has both, distance=5. No match.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_larger_slop) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick moved" slop=3
  // I: d=2. S,T,U,X: d=3. W: d=1.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(3);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick moved" slop=2: only I (d=2) and W (d=1).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(2);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_two_term_reversal) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "fox quick" slop=2
  // L: d=1 (forward). N: d=2 (best combo).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->set_slop(2);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_prefix) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "qui* fox" slop=1: qui* matches quick, quilt.
  // A,G,I,T,V: d=1. N: d=0. S: d=1 (via quilt).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_wildcard) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "fo% bro%" slop=2: fo% matches fox/forward, bro% matches brown/brother.
  // A,G,I,S,V,W,X,Y: d=2 (reversal). L: d=0. T,U: d<=2.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt1 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo%"));
    auto& wt2 = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2.term = irs::ViewCast<irs::byte_type>(std::string_view("bro%"));
    q.mutable_options()->set_slop(2);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("W", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("Y", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_slop0_regression_two_terms) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick brown" slop=0: same as normal phrase, verifies set_slop(0)
  // doesn't break anything. Expected: A, G, I, U.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(0);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_three_terms_gap_first_pair) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick fox moved" slop=1: only I matches (d=1).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_three_terms_gap_distributed) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick fox moved" slop=2
  // I: d=1. S: d=2. T: d=2.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(2);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_boundary_distance_equals_slop) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "brown dog" slop=5: A has distance=5 == slop. Match.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q.mutable_options()->set_slop(5);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "brown dog" slop=4: distance=5 > slop. No match.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q.mutable_options()->set_slop(4);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_single_term_with_slop) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "fox" slop=5: single term, Prepare returns term query.
  // Expected: A, G, I, K, L, N, S, T, V.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(5);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("K", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_empty_field_and_phrase) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty field + slop
  {
    irs::ByPhrase q;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(5);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // empty phrase + slop
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->set_slop(5);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_term_not_in_index) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick zzzznotexist" slop=100: nonexistent term. Empty result.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("zzzznotexist"));
    q.mutable_options()->set_slop(100);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_repeated_term) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "fox fox" slop=0: two adjacent foxes. Only N.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(0);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox fox" slop=4: only N has multiple foxes (after duplicate fix).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(4);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_three_term_partial_reversal) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "brown quick fox" slop=2: no match (min distance=3 for all docs).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(2);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "brown quick fox" slop=4
  // A,G,I: d=3. L: d=3.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(4);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_three_term_full_reversal) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "fox brown quick" slop=4
  // L: d=0. A,G,I: d=4. T: d=4.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->set_slop(4);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_many_occurrences) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick quick" slop=0: adjacent quick-quick. Only N.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->set_slop(0);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_large_slop) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick fox" slop=100: matches all docs with both terms.
  // Expected: A, G, I, L, N, S, T.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(100);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("L", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST(SlopDistanceTest, compute_slop_distance) {
  {  // exact adjacent: 0
    irs::PosAttr::value_t p[] = {1, 2};
    ASSERT_EQ(0, irs::ComputeSlopDistance(p, 2));
  }
  {  // exact adjacent 3 terms: 0
    irs::PosAttr::value_t p[] = {1, 2, 3};
    ASSERT_EQ(0, irs::ComputeSlopDistance(p, 3));
  }
  {  // forward gap 1: 1
    irs::PosAttr::value_t p[] = {1, 3};
    ASSERT_EQ(1, irs::ComputeSlopDistance(p, 2));
  }
  {  // forward gap 5: 5
    irs::PosAttr::value_t p[] = {2, 8};
    ASSERT_EQ(5, irs::ComputeSlopDistance(p, 2));
  }
  {  // swap adjacent: 2
    irs::PosAttr::value_t p[] = {2, 1};
    ASSERT_EQ(2, irs::ComputeSlopDistance(p, 2));
  }
  {  // swap with gap: 4
    irs::PosAttr::value_t p[] = {4, 1};
    ASSERT_EQ(4, irs::ComputeSlopDistance(p, 2));
  }
  {  // same position: 0
    irs::PosAttr::value_t p[] = {3, 3};
    ASSERT_EQ(0, irs::ComputeSlopDistance(p, 2));
  }
  {  // 3 terms, gap first pair: 1
    irs::PosAttr::value_t p[] = {1, 3, 4};
    ASSERT_EQ(1, irs::ComputeSlopDistance(p, 3));
  }
  {  // 3 terms, gap second pair: 1
    irs::PosAttr::value_t p[] = {1, 2, 4};
    ASSERT_EQ(1, irs::ComputeSlopDistance(p, 3));
  }
  {  // 3 terms, distributed: 2
    irs::PosAttr::value_t p[] = {1, 3, 5};
    ASSERT_EQ(2, irs::ComputeSlopDistance(p, 3));
  }
  {  // 3 terms, full reversal: 4
    irs::PosAttr::value_t p[] = {3, 2, 1};
    ASSERT_EQ(4, irs::ComputeSlopDistance(p, 3));
  }
  {  // 3 terms, partial reversal: 3
    irs::PosAttr::value_t p[] = {2, 1, 3};
    ASSERT_EQ(3, irs::ComputeSlopDistance(p, 3));
  }
  {  // 4 terms, exact: 0
    irs::PosAttr::value_t p[] = {5, 6, 7, 8};
    ASSERT_EQ(0, irs::ComputeSlopDistance(p, 4));
  }
  {  // 4 terms, mixed: 6
    irs::PosAttr::value_t p[] = {1, 4, 3, 6};
    ASSERT_EQ(6, irs::ComputeSlopDistance(p, 4));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_scoring) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick fox" slop=3 with scorer.
  // A,G,I,T: freq=1. L: freq=1 (reversal d=3). N: freq=7, boost=1.0.
  // S: freq=1.
  {
    tests::sort::CustomSort sort;
    sort.scorer_score = [](const irs::ScoreOperator*, irs::score_t* score,
                           size_t) { *score = 1.f; };
    sort.prepare_field_collector = [&sort]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(sort);
    };
    sort.prepare_term_collector = [&sort]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(sort);
    };

    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(3);

    auto prepared = q.prepare({
      .index = rdr,
      .scorer = &sort,
    });
    auto sub = rdr.begin();

    auto docs = prepared->execute({
      .segment = *sub,
      .scorer = &sort,
    });
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost_attr = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost_attr);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    // L: reversal, freq=1
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    // N: freq=7, best_distance=0, boost=1.0
    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(7, freq->value[0]);
    ASSERT_FLOAT_EQ(1.f, boost_attr->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(docs->next());
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_seek_interleave) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick brown" slop=1: A, G, I, S, U. Test seek + next interleaving.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    auto docs_seek = prepared->execute({.segment = *sub});

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    auto sought = docs_seek->seek(docs->value());
    ASSERT_EQ(docs->value(), sought);

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("U", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_levenshtein) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // levenshtein("qoick",1) + "fox" slop=1: matches quick only.
  // A,G,I,T: d=1. N: d=0 (best combo).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& lt = q.mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("qoick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_no_match) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "bro% dog" slop=1: only A has dog, distance=5. No match.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("bro%"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_mixed) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // prefix("qui") + wildcard("f%x") slop=1
  // A,G,I,T,V: d=1. N: d=0. S: d=1 (via quilt).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("f%x"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("G", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("N", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("T", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("V", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_four_terms) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick brown fox jumps" slop=0: exact. Only A.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("jumps"));
    q.mutable_options()->set_slop(0);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("A", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick brown fox moved" slop=1: I (d=0), S (d=1).
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("I", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("S", irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_range) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // range [x0, x1] + "x2" slop=1: X3 and X4.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();
    auto column = sub->column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X3",
              irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("X4",
              irs::ToString<std::string_view>(actual_value->value.data()));

    ASSERT_FALSE(docs->next());
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_count) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick brown" slop=1: A,G,I,S,U -> count=5.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});
    auto sub = rdr.begin();

    auto docs = prepared->execute({.segment = *sub});
    ASSERT_EQ(5, docs->count());
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_two_segments) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "quick brown fox" slop=1: A,G,I,S per segment -> total=4.
  {
    irs::ByPhrase q;
    *q.mutable_field() = "phrase_anl";
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    auto prepared = q.prepare({.index = rdr});

    uint32_t total = 0;
    for (auto sub = rdr.begin(); sub != rdr.end(); ++sub) {
      auto column = sub->column("name");
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::PayAttr>(*values);
      ASSERT_NE(nullptr, actual_value);

      auto docs = prepared->execute({.segment = *sub});

      while (docs->next()) {
        ASSERT_EQ(docs->value(), values->seek(docs->value()));
        auto name = irs::ToString<std::string_view>(actual_value->value.data());
        ASSERT_TRUE(name == "A" || name == "G" || name == "I" || name == "S")
          << "unexpected doc: " << name;
        ++total;
      }
    }
    ASSERT_EQ(4, total);
  }
}
