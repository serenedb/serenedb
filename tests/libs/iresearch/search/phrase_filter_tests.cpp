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
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/term_query.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kName = tests::FieldIdFor("name");
inline constexpr irs::field_id kPhraseAnl = tests::FieldIdFor("phrase_anl");
inline constexpr irs::field_id kPhrase = tests::FieldIdFor("phrase");

auto StoreName() {
  return [](irs::IndexWriter::Document& doc, const tests::Document& src) {
    const auto* name =
      dynamic_cast<const tests::StringField*>(src.stored.get_by_id(kName));
    if (name) {
      irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(), *name);
    }
  };
}

irs::Filter::ptr Lower(std::unique_ptr<irs::ByPhrase> q,
                       const irs::Scorer* scorer = nullptr) {
  irs::Filter::ptr f = std::move(q);
  irs::Optimize(f, {.scored = scorer != nullptr});
  return f;
}

}  // namespace
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
    // analyzed field -- id derived per source JSON field name so different
    // sources (e.g. "name" vs "phrase") don't collide on the same writer slot.
    const std::string anl_name = std::string(name.data()) + "_anl";
    auto analyzed = std::make_shared<TextField>(anl_name, data.str);
    analyzed->id = tests::FieldIdFor(anl_name);
    doc.indexed.push_back(std::move(analyzed));

    // not analyzed field -- id derived from the raw source field name.
    auto stringField = std::make_shared<StringField>(name, data.str);
    stringField->id = tests::FieldIdFor(name);
    doc.insert(std::move(stringField));
  }
}

}  // namespace tests

class PhraseFilterTestCase : public tests::FilterTestCaseBase {};

TEST_P(PhraseFilterTestCase, sequential_one_term) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  // read segment
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // empty field
  {
    irs::ByPhrase q;

    tests::PreparedFilter prepared{q, rdr};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // empty phrase
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;

    tests::PreparedFilter prepared{q, rdr};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // equals to term_filter "fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // prefix_filter "fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fo%"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "%ox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("%ox"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f%x"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("_ox"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f_x"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("f_x"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fo_"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("fo_"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("fox"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // levenshtein_filter "fox" max_distance = 0
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 0;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // levenshtein_filter "fol"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fol"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByTermsOptions "fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByTermsOptions "fox|that"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // by_range_filter_options "[x0, x0]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x0]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "[x0, x0)"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x0)"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // by_range_filter_options "[x0, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X2", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X5", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X2", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X5", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "[x0, x2)"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByRangeOptions "(x0, x2)"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Exclusive;
    rt.range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fox" on field without positions
  // which is ok for single word phrases
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhrase;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{q, rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fo*" on field without positions
  // which is ok for the first word in phrase
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhrase;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fo%" on field without positions
  // which is ok for first word in phrase
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhrase;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "f_x%" on field without positions
  // which is ok for first word in phrase
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhrase;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f_x%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search "fxo" on field without positions
  // which is ok for single word phrases
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhrase;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.with_transpositions = true;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fxo"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search ByRangeOptions "[x0, x1]" on field without positions
  // which is ok for first word in phrase
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhrase;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // term_filter "fox" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{q, rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr, dynamic_cast<const irs::TermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // prefix_filter "fo*" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "fo%" with phrase offset
  // which does not matter
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f%x" with phrase offset
  // which does not matter
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f%x"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "f%x" with phrase offset
  // which does not matter
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>(
      std::numeric_limits<size_t>::max());
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fkx"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // search ByRangeOptions "[x0, x1]" with phrase offset
  // which does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>(
      std::numeric_limits<size_t>::max());
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, rdr};
    // check single word phrase optimization
    ASSERT_NE(nullptr,
              dynamic_cast<const irs::MultiTermQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sequential_three_terms) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  // read segment
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick brown fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui* brown fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% brown fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "q%ck brown fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("q%ck"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown fox" simple term max_distance = 0
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 0;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quck brown fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quck"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] x0 x2
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x2"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick bro* fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick bro% fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("bro%"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick b%w_ fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("b%w_"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brkln fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("brkln"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "x1 [x0, x1] x2"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x2"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick brown fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown fo%"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown f_x"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f_x"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick brown fxo"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.with_transpositions = true;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fxo"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "x1 x0 [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qui* bro* fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% bro% fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%"))};
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("bro%"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% b%o__ fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%"))};
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("b%o__"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui bro fox"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt1 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt1.max_distance = 2;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    auto& lt2 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("brow"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] [x0, x1] x2"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qui* brown fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% brown fo%"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "q_i% brown f%x"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("q_i%"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f%x"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] x0 [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qoick br__nn fix"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt1 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt1.max_distance = 1;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qoick"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("br__n"))};
    auto& lt2 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fix"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick bro* fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick bro% fo%"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("bro%"))};
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick b_o% f_%"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("b_o%"))};
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f_%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "x1 [x0, x1] [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "qui* bro* fo*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt3 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("bro"));
    pt3.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "qui% bro% fo%"
  {
    auto make_q = [] {
      auto q = std::make_unique<irs::ByPhrase>();
      *q->mutable_field_id() = kPhraseAnl;
      auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
      auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
      auto& wt3 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("qui%"))};
      wt2 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("bro%"))};
      wt3 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};
      return q;
    };
    size_t finish_count = 0;
    uint64_t finish_docs_with_field = 0;
    uint64_t finish_docs_with_term = 0;

    tests::sort::CustomSort sort;

    sort.collectors_collect = [&](irs::byte_type*,
                                  const irs::FieldCollector* field,
                                  const irs::TermCollector* term) -> void {
      ++finish_count;
      ASSERT_NE(nullptr, field);
      ASSERT_NE(nullptr, term);
      finish_docs_with_field += field->docs_with_field;
      finish_docs_with_term += term->docs_with_term;
    };

    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    tests::PreparedFilter prepared{*Lower(make_q(), &sort), rdr, &sort};
    ASSERT_EQ(6, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // no order passed - no frequency
    {
      tests::PreparedFilter unscored{*Lower(make_q()), rdr};
      auto docs = unscored.Execute(0);
      ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
      ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    }

    auto docs = prepared.Execute(0);
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "q%ic_ br_wn _%x"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt3 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("q%ic_"))};
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("br_wn"))};
    wt3 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("_%x"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "quick|quilt|hhh brown|brother fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "[x0, x1] [x0, x1] [x1, x2]"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick brown fox" with order
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    size_t finish_count = 0;
    uint64_t finish_docs_with_field = 0;
    uint64_t finish_docs_with_term = 0;

    tests::sort::CustomSort sort;

    sort.collectors_collect = [&](irs::byte_type*,
                                  const irs::FieldCollector* field,
                                  const irs::TermCollector* term) -> void {
      ++finish_count;
      ASSERT_NE(nullptr, field);
      ASSERT_NE(nullptr, term);
      finish_docs_with_field += field->docs_with_field;
      finish_docs_with_term += term->docs_with_term;
    };
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    tests::PreparedFilter prepared{q, rdr, &sort};
    ASSERT_EQ(3, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
    auto sub = rdr.begin();

    // no order passed - no frequency
    {
      tests::PreparedFilter unscored{q, rdr};
      auto docs = unscored.Execute(0);
      ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
      ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    }

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    it = docs.get();

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }
}

TEST_P(PhraseFilterTestCase, sequential_several_terms) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  // read segment
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "fox ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "f_x ... quick"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f_x"))};
    q->mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fpx ... quick"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fpx"));
    q->mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui%ck"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%ck"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... qui*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "f%x ... qui%ck"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f%x"))};
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%ck"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fx ... quik"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt1 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    auto& lt2 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>(1);
    lt1.max_distance = 1;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fx"));
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("quik"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::get<irs::FreqBlockAttr>(*docs));
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fx ... quik"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt1 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    auto& lt2 = q->mutable_options()->push_back<irs::ByEditDistanceOptions>(1);
    lt1.max_distance = 1;
    lt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fx"));
    lt2.max_distance = 1;
    lt2.term = irs::ViewCast<irs::byte_type>(std::string_view("quik"));

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    tests::PreparedFilter prepared{*Lower(std::move(q), scorer.get()), rdr,
                                   scorer.get()};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_FLOAT_EQ((0.5f + 0.75f) / 2, boost->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_FLOAT_EQ(boost->value[0],
                    irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ((0.5f + 0.75f) / 2, boost->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_FLOAT_EQ(boost->value[0],
                    irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // =============================
  // "fo* ... qui*" with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    tests::PreparedFilter prepared{q, rdr, scorer.get()};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // =============================
  // jumps ... (jumps|hotdog|the) with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    tests::PreparedFilter prepared{q, rdr, scorer.get()};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.75f) / 2, boost->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ(((1.f + 0.25f) / 2 + (1.f + 0.5f) / 2) / 2,
                    boost->value[0]);
    ASSERT_EQ(
      "O", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(
      "P", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(3, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(
      "Q", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(
      "R", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // ByTermsOptions "fox|that" with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    tests::PreparedFilter prepared{q, rdr, scorer.get()};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // ByTermsOptions "fox|that" with scorer and boost
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")),
                     0.5f);
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    tests::PreparedFilter prepared{q, rdr, scorer.get()};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(irs::kNoBoost, boost->value[0]);
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_EQ(boost->value[0],
              irs::get<irs::BoostBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // test disjunctions (unary, basic, small, disjunction)
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    auto& pt1 = q->mutable_options()->push_back<irs::ByPrefixOptions>();
    auto& pt2 = q->mutable_options()->push_back<irs::ByPrefixOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("%las"))};
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("%nd"))};
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("go"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("like"));

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    tests::PreparedFilter prepared{*Lower(std::move(q), scorer.get()), rdr,
                                   scorer.get()};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "Z", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // =============================

  // "fox ... quick" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox quick"
  // const_max and zero offset
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(0).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox* quick*"
  // const_max and zero offset
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(0);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... quick" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "f_x ... quick" with phrase offset
  // which is does not matter
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("f_x"))};
    q->mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui*" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... qui%k" with phrase offset
  // which is does not matter
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()
      ->push_back<irs::ByTermOptions>(std::numeric_limits<size_t>::max())
      .term = irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%k"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo* ... qui*" with phrase offset
  // which is does not matter
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt1 = q.mutable_options()->push_back<irs::ByPrefixOptions>(
      std::numeric_limits<size_t>::max());
    auto& pt2 = q.mutable_options()->push_back<irs::ByPrefixOptions>(1);
    pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    pt2.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo% ... qui%" with phrase offset
  // which is does not matter
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>(1);
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qui%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fo% ... quik" with phrase offset
  // which is does not matter
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>(
      std::numeric_limits<size_t>::max());
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>(1);
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quik"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(10).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... qui*"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>(10);
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));

    tests::PreparedFilter prepared{q, rdr};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... qu_ck"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>(10);
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("qu_ck"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox ... ... ... ... ... ... ... ... ... ... quc"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>(10);
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("quc"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "eye ... eye"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("eye"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("eye"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in the past we are looking forward"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in % past we ___ looking forward"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("ass"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("in"));
    auto& wt1 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt1 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("%"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("past"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("we"));
    auto& wt2 = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt2 = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("___"))};
    auto& st = q->mutable_options()->push_back<irs::ByTermsOptions>();
    st.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("looking")));
    st.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("searching")));
    auto& pt = q->mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in the past we are looking forward" with order
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr, &sort};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto score = it->PrepareScore({
      .scorer = &sort,
      .segment = &*sub,
    });

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    irs::score_t score_value{};
    score.Score(&score_value, 1);
    ASSERT_EQ(docs->value(), score_value);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "as in the p_st we are look* forward" with order
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("as"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("in"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("the"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("p_st"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("we"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("are"));
    auto& pt = q->mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("look"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("forward"));

    tests::sort::CustomSort sort;
    irs::DocIterator* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->value();
    };

    tests::PreparedFilter prepared{*Lower(std::move(q), &sort), rdr, &sort};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto score = docs->PrepareScore({
      .scorer = &sort,
      .segment = &*sub,
    });

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    irs::score_t score_value{};
    score.Score(&score_value, 1);
    ASSERT_EQ(docs->value(), score_value);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // fox quick
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    // Check repeatable seek to the same document given frequency of the phrase
    // within the document = 2
    auto v = docs->value();
    ASSERT_EQ(v, docs->seek(docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // fox quick with order
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr, &sort};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    it = docs.get();
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // wildcard_filter "zo\\_%"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("zo\\_%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ("PHW0", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "\\_oo"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("\\_oo"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ("PHW1", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "z\\_o"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("z\\_o"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ("PHW2", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "elephant giraff\\_%"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("giraff\\_%"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ("PHW3", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "elephant \\_iraffe"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("\\_iraffe"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ("PHW4", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // wildcard_filter "elephant gira\\_fe"
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    auto& wt = q->mutable_options()->push_back<irs::ByWildcardOptions>();
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("gira\\_fe"))};

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ("PHW5", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, interval_several_terms) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_interval.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  // read segment
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "fox ... quick"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "fox ... quick ... brown"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(4, 5).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  // "pox ... quick ... brown" check for proper accounting of interval
  // adjustments
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("pox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    tests::PreparedFilter prepared{q, rdr};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
  }

  // mix interval and single
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("second"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field_id() = kPhraseAnl;
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
    tests::PreparedFilter prepared{q, rdr, &sort};
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    auto disj_docs = disj_prepared.Execute(0);
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    auto docs = prepared.Execute(0);
    it = docs.get();

    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::get<irs::BoostBlockAttr>(*docs));
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0],
              irs::get<irs::FreqBlockAttr>(*docs_seek)->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->advance()));
  }

  // mix interval and single
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
  }

  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    auto& wt = q.mutable_options()->push_back<irs::ByPrefixOptions>(3, 4);
    wt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // fixed interval ordered
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr, &sort};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto* freq_seek = irs::get<irs::FreqBlockAttr>(*docs_seek);
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    auto disj_docs = disj_prepared.Execute(0);
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(1, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(6, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(11, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(2, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->advance()));
  }

  // variadic interval ordered
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    q.mutable_options()->push_back<irs::ByPrefixOptions>(4, 5).term =
      irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByPrefixOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("bro"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr, &sort};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto* freq_seek = irs::get<irs::FreqBlockAttr>(*docs_seek);
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    auto disj_docs = disj_prepared.Execute(0);
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(1, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(5, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->advance()));
  }

  // fixed interval ordered last only repeated
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("zoo"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 4).term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::Or disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto& ph = disjunction.add<irs::ByPhrase>();
      *ph.mutable_field_id() = kPhraseAnl;
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

    tests::PreparedFilter prepared{q, rdr, &sort};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    auto docs = prepared.Execute(0);
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->value()));
    auto* freq_seek = irs::get<irs::FreqBlockAttr>(*docs_seek);
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    auto disj_docs = disj_prepared.Execute(0);
    auto disj_score = disj_docs->PrepareScore({
      .scorer = &freq_score,
      .segment = &*sub,
    });
    irs::score_t score_val;

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(docs->value(), docs_seek->seek(docs->value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->advance()));
    ASSERT_EQ(docs->value(), disj_docs->value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->advance()));
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
  ASSERT_EQ(irs::field_limits::invalid(), q.field_id());
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
    *q.mutable_field_id() = 1;

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  // single term
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = 1;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  // multiple terms
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = 1;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  // with boost
  {
    MaxMemoryCounter counter;
    irs::score_t boost = 1.5f;

    // no terms, return empty query
    {
      irs::ByPhrase q;
      *q.mutable_field_id() = 1;
      q.boost(boost);

      tests::PreparedFilter prepared{q, irs::SubReader::empty()};
      ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
    }

    // single term
    {
      irs::ByPhrase q;
      *q.mutable_field_id() = 1;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      q.boost(boost);

      tests::PreparedFilter prepared{q, irs::SubReader::empty(), nullptr,
                                     counter};
      ASSERT_EQ(boost, prepared.Query(0)->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // single multiple terms
    {
      irs::ByPhrase q;
      *q.mutable_field_id() = 1;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
      q.boost(boost);

      tests::PreparedFilter prepared{q, irs::SubReader::empty()};
      ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
    }

    // prefix, wildcard, levenshtein, set, range
    {
      irs::ByPhrase q;
      *q.mutable_field_id() = 1;
      auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& wt = q.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("qu__k"))};
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

      tests::PreparedFilter prepared{q, irs::SubReader::empty()};
      ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
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
      wt1 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("dog"))};
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
    *q0.mutable_field_id() = 1;
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::ByPhrase q1;
    *q1.mutable_field_id() = 1;
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_EQ(q0, q1);
  }

  {
    irs::ByPhrase q0;
    {
      *q0.mutable_field_id() = 1;
      auto& pt1 = q0.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& ct1 = q0.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q0.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("br_wn"))};
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
      *q1.mutable_field_id() = 1;
      auto& pt1 = q1.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& ct1 = q1.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q1.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("br_wn"))};
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
    *q0.mutable_field_id() = 1;
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("squirrel"));

    irs::ByPhrase q1;
    *q1.mutable_field_id() = 1;
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_NE(q0, q1);
  }

  {
    irs::ByPhrase q0;
    *q0.mutable_field_id() = 2;
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));

    irs::ByPhrase q1;
    *q1.mutable_field_id() = 1;
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_NE(q0, q1);
  }

  {
    irs::ByPhrase q0;
    *q0.mutable_field_id() = 1;
    q0.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));

    irs::ByPhrase q1;
    *q1.mutable_field_id() = 1;
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q1.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    ASSERT_NE(q0, q1);
  }

  {
    irs::ByPhrase q0;
    {
      *q0.mutable_field_id() = 1;
      auto& pt1 = q0.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("quil"));
      auto& ct1 = q0.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q0.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("br_wn"))};
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
      *q1.mutable_field_id() = 1;
      auto& pt1 = q1.mutable_options()->push_back<irs::ByPrefixOptions>();
      pt1.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      auto& ct1 = q1.mutable_options()->push_back<irs::ByTermsOptions>();
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("light")));
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("dark")));
      auto& wt1 = q1.mutable_options()->push_back<irs::ByWildcardOptions>();
      wt1 = irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("br_wn"))};
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
    wt = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("br_wn"))};
    irs::ByEditDistanceOptions lt;
    lt.max_distance = 2;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    irs::ByRangeOptions rt;
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("elephant"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;

    irs::ByPhrase q0;
    *q0.mutable_field_id() = 1;
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

// Style note: other TEST_Ps in this file assert exact document names
// (A, G, K, ...) per document.  These two cases compare large result
// sets across two syntax modes, so we collect doc_ids into a vector -
// the per-name boilerplate would be much longer without adding signal.

TEST_P(PhraseFilterTestCase, regexp_part_syntax) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  auto execute = [&](std::unique_ptr<irs::ByPhrase> q) {
    std::vector<irs::doc_id_t> out;
    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    for (size_t i = 0, end = prepared.size(); i < end; ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->advance())) {
        out.push_back(docs->value());
      }
    }
    return out;
  };

  // "quick [br]+own" in Perl equals plain phrase "quick brown"
  {
    auto ref = std::make_unique<irs::ByPhrase>();
    *ref->mutable_field_id() = kPhraseAnl;
    ref->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    ref->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    auto expected = execute(std::move(ref));
    ASSERT_FALSE(expected.empty());

    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q->mutable_options()->push_back<irs::ByRegexpOptions>() =
      irs::ByRegexpOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("[br]+own"))};
    ASSERT_EQ(expected, execute(std::move(q)));
  }

  // "quick \w+" in POSIX matches nothing - \w+ is a parse error
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q->mutable_options()->push_back<irs::ByRegexpOptions>() =
      irs::ByRegexpOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("\\w+")),
        irs::RegexpSyntax::PosixEre};

    ASSERT_TRUE(execute(std::move(q)).empty());
  }

  // sanity: same "quick \w+" in Perl matches something
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q->mutable_options()->push_back<irs::ByRegexpOptions>() =
      irs::ByRegexpOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("\\w+"))};

    ASSERT_FALSE(execute(std::move(q)).empty());
  }
}

// Regression: NOT(ByPhrase) used to trip the `target >= value()`
// assertion in PostingIteratorBase::LazySeek. Not::prepare wraps the
// phrase as the excl side of an AndQuery over All; Exclusion::converge
// then re-reads value() from the PhraseIterator (and its inner
// Conjunction) between LazySeek calls. The bail-out paths in those
// iterators previously returned the advanced position without
// updating _doc, so the next LazySeek seeded a target that was
// behind some leaf's current position.
TEST_P(PhraseFilterTestCase, sequential_negation_regression) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  auto make_phrase = [] {
    auto phrase = std::make_unique<irs::ByPhrase>();
    *phrase->mutable_field_id() = kPhraseAnl;
    phrase->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    phrase->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    return phrase;
  };

  auto make_not_phrase = [&] {
    auto not_phrase = std::make_unique<irs::Exclusion>();
    not_phrase->exclude(make_phrase());
    irs::Filter::ptr f = std::move(not_phrase);
    irs::Optimize(f, {});
    return f;
  };

  auto collect = [&](const irs::Filter& filter) {
    std::vector<irs::doc_id_t> out;
    tests::PreparedFilter prepared{filter, rdr};
    for (size_t i = 0, end = prepared.size(); i < end; ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->advance())) {
        out.push_back(docs->value());
      }
    }
    return out;
  };

  const auto phrase_hits = collect(*Lower(make_phrase()));
  ASSERT_FALSE(phrase_hits.empty());

  const auto not_hits = collect(*make_not_phrase());

  // Complement must be non-empty and disjoint from the positive hits.
  ASSERT_FALSE(not_hits.empty());
  for (auto doc : phrase_hits) {
    EXPECT_EQ(not_hits.end(), std::find(not_hits.begin(), not_hits.end(), doc));
  }

  // Drive seek() across positive hits too -- that's the path that
  // surfaced the crash from the SQL side via Exclusion::converge.
  tests::PreparedFilter prepared{*make_not_phrase(), rdr};
  for (size_t i = 0, end = prepared.size(); i < end; ++i) {
    auto docs = prepared.Execute(i);
    for (auto doc : phrase_hits) {
      const auto target = doc + 1;
      const auto landed = docs->seek(target);
      EXPECT_GE(landed, target);
      if (irs::doc_limits::eof(landed)) {
        break;
      }
    }
  }
}

TEST(by_phrase_test, equal_regexp_part_syntax_differs) {
  // ByRegexpOptions::syntax must propagate through variant equality
  auto make = [](irs::RegexpSyntax syntax) {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    auto& r = q.mutable_options()->push_back<irs::ByRegexpOptions>();
    r.pattern = irs::ViewCast<irs::byte_type>(std::string_view("br.wn"));
    r.syntax = syntax;
    return q;
  };

  ASSERT_EQ(make(irs::RegexpSyntax::Perl), make(irs::RegexpSyntax::Perl));
  ASSERT_NE(make(irs::RegexpSyntax::Perl), make(irs::RegexpSyntax::PosixEre));
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(phrase_filter_test, PhraseFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         PhraseFilterTestCase::to_string);

TEST_P(PhraseFilterTestCase, sloppy_phrase_two_terms) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick fox" slop=1: A,G,I,T: d=1. N: d=0.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick fox" slop=100: all docs with both terms.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(100);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "L", "N", "S", "T"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick moved" slop=3: I d=2, S,T,U,X d=3, W d=1.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(3);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"I", "S", "T", "U", "W", "X"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick moved" slop=2: I d=2, W d=1.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(2);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "brown dog" slop=1: A has distance=5. No match.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "brown dog" slop=5: A matches (d=5 == slop).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q.mutable_options()->set_slop(5);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "brown dog" slop=4: A has d=5 > slop. No match.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q.mutable_options()->set_slop(4);

    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox fox" slop=0: only N has adjacent foxes.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(0);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox fox" slop=4: still only N (other docs have at most one fox).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(4);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick quick" slop=0: only N has adjacent quicks.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->set_slop(0);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_reversal) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "fox brown" slop=2: A,G,I,S d=2 (reversal). L,T d=0.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(2);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "L", "S", "T"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox brown" slop=1: reversal costs 2. Only L,T (d=0).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox quick" slop=2: L d=1 (forward), N d=2.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->set_slop(2);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_three_terms) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick brown fox" slop=1: A,G,I d=0. S d=1.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "S"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick fox moved" slop=1: only I (d=1).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick fox moved" slop=2: I d=1, S,T d=2.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(2);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"I", "S", "T"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "brown quick fox" slop=2: no match (min d=3 for all docs).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(2);

    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "brown quick fox" slop=4: A,G,I,L d=3.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(4);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "L"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fox brown quick" slop=4: L d=0. A,G,I,T d=4.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->set_slop(4);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "L", "T"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_four_terms) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick brown fox jumps" slop=0: exact. Only A.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("jumps"));
    q.mutable_options()->set_slop(0);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "quick brown fox moved" slop=1: I d=0, S d=1.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_prefix_and_wildcard) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "qui* fox" slop=1: qui* matches quick, quilt.
  // A,G,I,T,V: d=1. N: d=0. S: d=1 (via quilt).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "N", "S", "T", "V"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "fo% bro%" slop=2: fo% -> fox/forward, bro% -> brown/brother.
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("fo%"))};
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("bro%"))};
    q->mutable_options()->set_slop(2);

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected :
         {"A", "G", "I", "L", "S", "T", "U", "V", "W", "X", "Y"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // levenshtein("qoick",1) + "fox" slop=1: matches quick.
  // A,G,I,T: d=1. N: d=0.
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& lt = q->mutable_options()->push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("qoick"));
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q->mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "N", "T"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // prefix("qui") + wildcard("f%x") slop=1.
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& pt = q->mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("f%x"))};
    q->mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "N", "S", "T", "V"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // "bro% dog" slop=1: only A has dog, d=5. No match.
  {
    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    q->mutable_options()->push_back<irs::ByWildcardOptions>() =
      irs::ByWildcardOptions{
        irs::ViewCast<irs::byte_type>(std::string_view("bro%"))};
    q->mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    q->mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{*Lower(std::move(q)), rdr};
    auto docs = prepared.Execute(0);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // range [x0, x1] + "x2" slop=1: X3, X4.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& rt = q.mutable_options()->push_back<irs::ByRangeOptions>();
    rt.range.min = irs::ViewCast<irs::byte_type>(std::string_view("x0"));
    rt.range.max = irs::ViewCast<irs::byte_type>(std::string_view("x1"));
    rt.range.min_type = irs::BoundType::Inclusive;
    rt.range.max_type = irs::BoundType::Inclusive;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("x2"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_edge_cases) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // slop=0 falls through to exact path. "quick brown" -> A, G, I, U.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(0);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "U"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // Single term + slop: Prepare returns a plain term query.
  // "fox" slop=5 -> A, G, I, K, L, N, S, T, V.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(5);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    for (auto expected : {"A", "G", "I", "K", "L", "N", "S", "T", "V"}) {
      ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // Single terms-set part + slop: one slot makes slop meaningless; must
  // match exactly what the same query matches at slop 0.
  {
    const auto collect = [&](irs::PosAttr::value_t slop) {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      auto& st = q.mutable_options()->push_back<irs::ByTermsOptions>();
      st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
      st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));
      q.mutable_options()->set_slop(slop);

      tests::PreparedFilter prepared{q, rdr};
      std::vector<irs::doc_id_t> out;
      auto docs = prepared.Execute(0);
      while (!irs::doc_limits::eof(docs->advance())) {
        out.push_back(docs->value());
      }
      return out;
    };

    const auto plain = collect(0);
    ASSERT_FALSE(plain.empty());
    ASSERT_EQ(plain, collect(5));
  }

  // Empty field + slop.
  {
    irs::ByPhrase q;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(5);

    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // Empty phrase + slop.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->set_slop(5);

    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // Term not in index + large slop.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("zzzznotexist"));
    q.mutable_options()->set_slop(100);

    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_explicit_gap) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick __ moved" -- push_back(term, offs=1) sets offs_min == offs_max
  // == 2, so expected_step between slots is 2.

  // slop=0: only W has moved exactly two positions after quick.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(/*offs=*/1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(0);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }

  // slop=1: + I (delta=3, cost=1).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>(/*offs=*/1).term =
      irs::ViewCast<irs::byte_type>(std::string_view("moved"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_scoring) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick fox" slop=3 with scorer.
  // A,G,I,T: freq=1. L: freq=1 (reversal d=3). N: freq=7, boost=1.0.
  // S: freq=1.
  {
    tests::sort::CustomSort sort;
    sort.scorer_score = [](const irs::ScoreOperator*, irs::score_t* score,
                           size_t) { *score = 1.f; };

    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(3);

    tests::PreparedFilter prepared{q, rdr, &sort};

    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost_attr = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost_attr);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    // L: reversal, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    // N: freq=8, best_distance=0, boost=1.0
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(8, freq->value[0]);
    ASSERT_FLOAT_EQ(1.f, boost_attr->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_seek_interleave) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick brown" slop=1: A, G, I, S, U. Test seek + next interleaving.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    auto docs_seek = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    auto sought = docs_seek->seek(docs->value());
    ASSERT_EQ(docs->value(), sought);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));

    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->seek(irs::doc_limits::eof())));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_count) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick brown" slop=1: A,G,I,S,U -> count=5.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};

    auto docs = prepared.Execute(0);
    ASSERT_EQ(5, docs->count());
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_two_segments) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick brown fox" slop=1: A,G,I,S per segment -> total=4.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("brown"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};

    uint32_t total = 0;
    for (auto sub = rdr.begin(); sub != rdr.end(); ++sub) {
      const auto* column = sub->Column(kName);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{*sub, *column};

      auto docs = prepared.Execute(0);

      while (!irs::doc_limits::eof(docs->advance())) {
        auto name =
          irs::tests::ReadStoredStr<std::string_view>(values, docs->value());
        ASSERT_TRUE(name == "A" || name == "G" || name == "I" || name == "S")
          << "unexpected doc: " << name;
        ++total;
      }
    }
    ASSERT_EQ(4, total);
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_scoring) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "qui* fox" slop=1 with scorer: qui* matches quick, quilt.
  // A,G,I,T,V: freq=1. N: freq>1 (multiple combos). S: freq=2 (quick+quilt).
  {
    tests::sort::CustomSort sort;
    sort.scorer_score = [](const irs::ScoreOperator*, irs::score_t* score,
                           size_t) { *score = 1.f; };

    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr, &sort};
    auto sub = rdr.begin();

    auto docs = prepared.Execute(0);
    auto* freq = irs::get<irs::FreqBlockAttr>(*docs);
    ASSERT_TRUE(freq);
    auto* boost_attr = irs::get<irs::BoostBlockAttr>(*docs);
    ASSERT_TRUE(boost_attr);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // A: qui*=1(quick), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(1, freq->value[0]);

    // G: qui*=5(quick), fox=7, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(1, freq->value[0]);

    // I: qui*=1(quick), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(1, freq->value[0]);

    // N: multiple combos, best d=0, boost=1.0
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_GT(freq->value[0], 1);
    ASSERT_FLOAT_EQ(1.f, boost_attr->value[0]);

    // S: qui*=[1,2](quick,quilt), fox=[4].
    // quilt=2,fox=4 d=1. quick=1,fox=4 d=2>1. freq=1.
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(1, freq->value[0]);

    // T: qui*=1(quick), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(1, freq->value[0]);

    // V: qui*=1(quilt), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_execute_with_offsets) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "quick fox" slop=1 with offsets.
  // A: expected offsets start=0 (quick), end=15 (fox).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("quick"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto* phrase_query =
      dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0));
    ASSERT_NE(nullptr, phrase_query);

    auto sub = rdr.begin();
    auto docs = phrase_query->ExecuteWithOffsets(*sub);
    ASSERT_NE(nullptr, docs);

    auto* pos = irs::GetMutable<irs::PosAttr>(docs.get());
    ASSERT_NE(nullptr, pos);
    auto* offs = irs::get<irs::OffsAttr>(*pos);
    ASSERT_NE(nullptr, offs);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // A
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_TRUE(pos->next());
    ASSERT_EQ(0, offs->start);
    ASSERT_EQ(15, offs->end);
    ASSERT_FALSE(pos->next());  // exhaust, resets for next doc

    // G
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    // I
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    // N: freq=2. Tuples: (6,8) cost=1 leftmost=6, (7,8) cost=0 leftmost=7.
    // Sorted by leftmost ascending: (6,8) first, (7,8) second.
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_TRUE(pos->next());  // first match
    ASSERT_GT(offs->end, offs->start);
    ASSERT_TRUE(pos->next());  // second match
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());  // exhausted

    // T
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_execute_with_offsets) {
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // "qui* fox" slop=1 with offsets via ExecuteWithOffsets.
  // Variadic phrase: qui* matches quick, quilt.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("qui"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->set_slop(1);

    tests::PreparedFilter prepared{q, rdr};
    auto* phrase_query =
      dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0));
    ASSERT_NE(nullptr, phrase_query);

    auto sub = rdr.begin();
    auto docs = phrase_query->ExecuteWithOffsets(*sub);
    ASSERT_NE(nullptr, docs);

    auto* pos = irs::GetMutable<irs::PosAttr>(docs.get());
    ASSERT_NE(nullptr, pos);
    auto* offs = irs::get<irs::OffsAttr>(*pos);
    ASSERT_NE(nullptr, offs);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // A: "quick brown fox ..." -> start=0 (quick), end=15 (fox)
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_TRUE(pos->next());
    ASSERT_EQ(0, offs->start);
    ASSERT_EQ(15, offs->end);
    ASSERT_FALSE(pos->next());

    // G
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->value()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    // I
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    // N: freq=2 (tuples (6,8) cost=1, (7,8) cost=0; sorted by leftmost)
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(pos->next());  // first match
    ASSERT_GT(offs->end, offs->start);
    ASSERT_TRUE(pos->next());  // second match
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());  // exhausted

    // S
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    // T
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    // V
    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(pos->next());
    ASSERT_GT(offs->end, offs->start);
    ASSERT_FALSE(pos->next());

    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->value()));
  }
}

namespace tests {

// Field whose token stream places two distinct terms ("foo" and "bar") at the
// SAME position, via the real solr_synonyms analyzer (group "foo,bar":
// indexing "foo" emits foo@P inc=1 and bar@P inc=0). Mirrors tests::TextField.
class OverlapField : public tests::FieldBase {
 public:
  OverlapField(std::string_view name, irs::field_id field_id)
    : _stream(irs::analysis::SolrSynonymsTokenizer::Make(
        irs::analysis::SolrSynonymsTokenizer::Options{
          .synonyms_text = "foo,bar",
        })) {
    this->Name(std::string(name));
    this->id = field_id;
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  irs::Tokenizer& GetTokens() const final {
    _stream->reset(_value);  // "foo" -> foo@P (inc 1), bar@P (inc 0)
    return *_stream;
  }

 private:
  bool Write(irs::DataOutput&) const final { return false; }

  std::string _value{"foo"};
  irs::analysis::Analyzer::ptr _stream;
};

class OverlapDocGenerator : public tests::DocGeneratorBase {
 public:
  explicit OverlapDocGenerator(irs::field_id field_id) : _field_id(field_id) {}

  const tests::Document* next() final {
    if (_done) {
      return nullptr;
    }
    _done = true;
    _doc.indexed.clear();
    _doc.stored.clear();
    _doc.insert(std::make_shared<OverlapField>("phrase_anl", _field_id),
                /*indexed=*/true, /*stored=*/false);
    return &_doc;
  }

  void reset() final { _done = false; }

 private:
  irs::field_id _field_id;
  tests::Document _doc;
  bool _done{false};
};

// Same as OverlapField, but also indexes offsets so the doc can be
// queried via ExecuteWithOffsets.
class OverlapFieldWithOffsets : public OverlapField {
 public:
  OverlapFieldWithOffsets(std::string_view name, irs::field_id field_id)
    : OverlapField(name, field_id) {
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs;
  }
};

class OverlapDocGeneratorWithOffsets : public tests::DocGeneratorBase {
 public:
  explicit OverlapDocGeneratorWithOffsets(irs::field_id field_id)
    : _field_id(field_id) {}

  const tests::Document* next() final {
    if (_done) {
      return nullptr;
    }
    _done = true;
    _doc.indexed.clear();
    _doc.stored.clear();
    _doc.insert(
      std::make_shared<OverlapFieldWithOffsets>("phrase_anl", _field_id),
      /*indexed=*/true, /*stored=*/false);
    return &_doc;
  }

  void reset() final { _done = false; }

 private:
  irs::field_id _field_id;
  tests::Document _doc;
  bool _done{false};
};

}  // namespace tests
namespace {

// Returns the number of docs matched by phrase {t0, t1} at the given slop.
size_t MatchCount(const irs::IndexReader& rdr, irs::field_id field,
                  std::string_view t0, std::string_view t1,
                  irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = field;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t0);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t1);
  q.mutable_options()->set_slop(slop);

  tests::PreparedFilter prepared{q, rdr};
  size_t count = 0;
  for (auto sub = rdr.begin(); sub != rdr.end(); ++sub) {
    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->advance())) {
      ++count;
    }
  }
  return count;
}

}  // namespace

TEST_P(PhraseFilterTestCase, sloppy_phrase_overlap_same_position) {
  {
    tests::OverlapDocGenerator gen(kPhraseAnl);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // Contract that predates the same-position fix (must keep passing).

  // slop 0: same position is not adjacency -> no match (matches ES).
  EXPECT_EQ(0u, MatchCount(rdr, kPhraseAnl, "foo", "bar", 0));
  EXPECT_EQ(0u, MatchCount(rdr, kPhraseAnl, "bar", "foo", 0));

  // case B: a single occurrence cannot fill two slots, at any slop (ES: 0).
  EXPECT_EQ(0u, MatchCount(rdr, kPhraseAnl, "foo", "foo", 0));
  EXPECT_EQ(0u, MatchCount(rdr, kPhraseAnl, "foo", "foo", 5));

  // case A, the same-position fix: distinct terms sharing a position match at
  // slop >= 1 (ES). slop 0 excludes them because same position costs 1.
  EXPECT_EQ(1u, MatchCount(rdr, kPhraseAnl, "foo", "bar", 1));
  EXPECT_EQ(1u, MatchCount(rdr, kPhraseAnl, "foo", "bar", 5));
  EXPECT_EQ(1u, MatchCount(rdr, kPhraseAnl, "bar", "foo", 1));
}

// Offsets path of the same-position fix: the matcher (Run with term
// groups) accepts foo@P/bar@P at slop >= 1, and the highlight
// enumeration must agree. Before the fix the enumeration pass enforced
// strict position uniqueness unconditionally, dropped the tuple and
// tripped the enumerated.size() == _phrase_freq assert in BuildMatches.
TEST_P(PhraseFilterTestCase, sloppy_phrase_overlap_same_position_with_offsets) {
  {
    tests::OverlapDocGeneratorWithOffsets gen(kPhraseAnl);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  irs::ByPhrase q;
  *q.mutable_field_id() = kPhraseAnl;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view("foo"));
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(std::string_view("bar"));
  q.mutable_options()->set_slop(1);

  tests::PreparedFilter prepared{q, rdr};
  auto* phrase_query =
    dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0));
  ASSERT_NE(nullptr, phrase_query);

  auto sub = rdr.begin();
  auto docs = phrase_query->ExecuteWithOffsets(*sub);
  ASSERT_NE(nullptr, docs);
  auto* pos = irs::GetMutable<irs::PosAttr>(docs.get());
  ASSERT_NE(nullptr, pos);
  auto* offs = irs::get<irs::OffsAttr>(*pos);
  ASSERT_NE(nullptr, offs);

  ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
  ASSERT_TRUE(pos->next())
    << "matcher freq says a match exists, offsets enumeration dropped it";
  // foo@P and bar@P both originate from the single source token "foo",
  // so the single match spans exactly that token.
  EXPECT_LT(offs->start, offs->end);
  EXPECT_FALSE(pos->next());
  ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
}

namespace tests {

// Explicit token stream over a fixed (term, increment, offsets) table;
// each corpus below owns its table.
class FixedTokensStream final
  : public irs::analysis::TypedAnalyzer<FixedTokensStream>,
    private irs::util::Noncopyable {
 public:
  struct Token {
    std::string_view term;
    uint32_t inc;
    uint32_t start;
    uint32_t end;
  };

  static constexpr std::string_view type_name() noexcept {
    return "fixed_tokens_stream";
  }

  explicit FixedTokensStream(std::span<const Token> tokens) noexcept
    : _tokens(tokens) {}

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  bool next() final {
    if (_i >= _tokens.size()) {
      return false;
    }
    const auto& t = _tokens[_i++];
    std::get<irs::IncAttr>(_attrs).value = t.inc;
    std::get<irs::TermAttr>(_attrs).value =
      irs::ViewCast<irs::byte_type>(t.term);
    auto& offs = std::get<irs::OffsAttr>(_attrs);
    offs.start = t.start;
    offs.end = t.end;
    return true;
  }

  bool reset(std::string_view) final {
    _i = 0;
    return true;
  }

 private:
  using Attributes = std::tuple<irs::IncAttr, irs::OffsAttr, irs::TermAttr>;

  std::span<const Token> _tokens;
  Attributes _attrs;
  size_t _i{0};
};

class RepeatOverlapField : public tests::FieldBase {
 public:
  RepeatOverlapField(std::string_view name, irs::field_id field_id) {
    this->Name(std::string(name));
    this->id = field_id;
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  irs::Tokenizer& GetTokens() const final {
    _stream.reset({});
    return _stream;
  }

 private:
  bool Write(irs::DataOutput&) const final { return false; }

  static constexpr std::array<FixedTokensStream::Token, 3> kTokens{{
    {"foo", 1, 0, 3},
    {"bar", 1, 4, 9},
    {"foo", 0, 4, 9},
  }};
  mutable FixedTokensStream _stream{kTokens};
};

class RepeatOverlapDocGenerator : public tests::DocGeneratorBase {
 public:
  explicit RepeatOverlapDocGenerator(irs::field_id field_id)
    : _field_id(field_id) {}

  const tests::Document* next() final {
    if (_done) {
      return nullptr;
    }
    _done = true;
    _doc.indexed.clear();
    _doc.stored.clear();
    _doc.insert(std::make_shared<RepeatOverlapField>("phrase_anl", _field_id),
                /*indexed=*/true, /*stored=*/false);
    return &_doc;
  }

  void reset() final { _done = false; }

 private:
  irs::field_id _field_id;
  tests::Document _doc;
  bool _done{false};
};

// Same corpus with offsets indexed, for the ExecuteWithOffsets path.
class RepeatOverlapFieldWithOffsets : public RepeatOverlapField {
 public:
  RepeatOverlapFieldWithOffsets(std::string_view name, irs::field_id field_id)
    : RepeatOverlapField(name, field_id) {
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs;
  }
};

class RepeatOverlapDocGeneratorWithOffsets : public tests::DocGeneratorBase {
 public:
  explicit RepeatOverlapDocGeneratorWithOffsets(irs::field_id field_id)
    : _field_id(field_id) {}

  const tests::Document* next() final {
    if (_done) {
      return nullptr;
    }
    _done = true;
    _doc.indexed.clear();
    _doc.stored.clear();
    _doc.insert(
      std::make_shared<RepeatOverlapFieldWithOffsets>("phrase_anl", _field_id),
      /*indexed=*/true, /*stored=*/false);
    return &_doc;
  }

  void reset() final { _done = false; }

 private:
  irs::field_id _field_id;
  tests::Document _doc;
  bool _done{false};
};

}  // namespace tests
namespace {

// Returns the number of docs matched by phrase {t0, t1, t2} at the given slop.
size_t MatchCount3(const irs::IndexReader& rdr, irs::field_id field,
                   std::string_view t0, std::string_view t1,
                   std::string_view t2, irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = field;
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t0);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t1);
  q.mutable_options()->push_back<irs::ByTermOptions>().term =
    irs::ViewCast<irs::byte_type>(t2);
  q.mutable_options()->set_slop(slop);

  tests::PreparedFilter prepared{q, rdr};
  size_t count = 0;
  for (auto sub = rdr.begin(); sub != rdr.end(); ++sub) {
    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->advance())) {
      ++count;
    }
  }
  return count;
}

}  // namespace

// ES-verified (exp 1 of ES_verify_slop_uniqueness): phrase "foo bar foo"
// against foo@1 + {bar,foo}@2. The two foo slots form one group and take
// positions 1 and 2; bar shares position 2 with the second foo legally
// (different group). Before the per-group check the global rule barred
// every tuple.
TEST_P(PhraseFilterTestCase, sloppy_phrase_repeat_same_position) {
  {
    tests::RepeatOverlapDocGenerator gen(kPhraseAnl);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // Corpus-shape controls; if the one-way synonym mapping did not apply,
  // these fail first and point at the analyzer, not the matcher.
  EXPECT_EQ(1u, MatchCount(rdr, kPhraseAnl, "foo", "bar", 0));
  EXPECT_EQ(1u, MatchCount(rdr, kPhraseAnl, "foo", "foo", 0));

  // The pinned divergence: 0 at slop 0 (delta-0 costs 1), 1 doc from
  // slop 1 on (tuple foo@1, bar@2, foo@2 of cost 1).
  EXPECT_EQ(0u, MatchCount3(rdr, kPhraseAnl, "foo", "bar", "foo", 0));
  EXPECT_EQ(1u, MatchCount3(rdr, kPhraseAnl, "foo", "bar", "foo", 1));
  EXPECT_EQ(1u, MatchCount3(rdr, kPhraseAnl, "foo", "bar", "foo", 2));
}

// Offsets path of the same fix: Run's collector must emit exactly freq
// tuples under the per-group rule (BuildMatches asserts equality). One
// tuple at slop 1 (cost 1), a second from slop 3 on (reversed foo pair,
// cost 3).
TEST_P(PhraseFilterTestCase, sloppy_phrase_repeat_same_position_with_offsets) {
  {
    tests::RepeatOverlapDocGeneratorWithOffsets gen(kPhraseAnl);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  const auto run = [&](irs::PosAttr::value_t slop, size_t want_matches) {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("foo"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("bar"));
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("foo"));
    q.mutable_options()->set_slop(slop);

    tests::PreparedFilter prepared{q, rdr};
    auto* phrase_query =
      dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0));
    ASSERT_NE(nullptr, phrase_query);

    auto sub = rdr.begin();
    auto docs = phrase_query->ExecuteWithOffsets(*sub);
    ASSERT_NE(nullptr, docs);
    auto* pos = irs::GetMutable<irs::PosAttr>(docs.get());
    ASSERT_NE(nullptr, pos);
    auto* offs = irs::get<irs::OffsAttr>(*pos);
    ASSERT_NE(nullptr, offs);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    size_t matches = 0;
    while (pos->next()) {
      EXPECT_EQ(0u, offs->start);
      EXPECT_EQ(9u, offs->end);
      ++matches;
    }
    EXPECT_EQ(want_matches, matches);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
  };

  run(1, 1);
  run(3, 2);
}

namespace tests {

// "foo" indexed under synonym foo,fooa -> foo@0 (inc 1) + fooa@0 (inc 0).
// A prefix slot "fo*" matches both.
class OverlapPrefixField : public tests::FieldBase {
 public:
  OverlapPrefixField(std::string_view name, irs::field_id field_id)
    : _stream(irs::analysis::SolrSynonymsTokenizer::Make(
        irs::analysis::SolrSynonymsTokenizer::Options{
          .synonyms_text = "foo,fooa",
        })) {
    this->Name(std::string(name));
    this->id = field_id;
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  irs::Tokenizer& GetTokens() const final {
    _stream->reset(_value);
    return *_stream;
  }

 private:
  bool Write(irs::DataOutput&) const final { return false; }

  std::string _value{"foo"};
  irs::analysis::Analyzer::ptr _stream;
};

class OverlapPrefixDocGenerator : public tests::DocGeneratorBase {
 public:
  explicit OverlapPrefixDocGenerator(irs::field_id field_id)
    : _field_id(field_id) {}

  const tests::Document* next() final {
    if (_done) {
      return nullptr;
    }
    _done = true;
    _doc.indexed.clear();
    _doc.stored.clear();
    _doc.insert(std::make_shared<OverlapPrefixField>("phrase_anl", _field_id),
                /*indexed=*/true, /*stored=*/false);
    return &_doc;
  }

  void reset() final { _done = false; }

 private:
  irs::field_id _field_id;
  tests::Document _doc;
  bool _done{false};
};

}  // namespace tests
namespace {

// Count docs matched by a two-slot prefix phrase {p0*, p1*} at the given slop.
size_t PrefixPhraseMatchCount(const irs::IndexReader& rdr, irs::field_id field,
                              std::string_view p0, std::string_view p1,
                              irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = field;
  q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
    irs::ViewCast<irs::byte_type>(p0);
  q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
    irs::ViewCast<irs::byte_type>(p1);
  q.mutable_options()->set_slop(slop);

  tests::PreparedFilter prepared{q, rdr};
  size_t count = 0;
  for (auto sub = rdr.begin(); sub != rdr.end(); ++sub) {
    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->advance())) {
      ++count;
    }
  }
  return count;
}

}  // namespace

TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_overlap_same_position) {
  {
    tests::OverlapPrefixDocGenerator gen(kPhraseAnl);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // Positive control: a SINGLE "fo*" slot must find the doc (proves the prefix
  // expansion is wired to this field; foo/fooa are present).
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fo"));
    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    EXPECT_TRUE(!irs::doc_limits::eof(docs->advance()))
      << "single fo* slot should match foo/fooa";
  }

  for (const irs::PosAttr::value_t slop : {0u, 1u, 2u, 5u}) {
    const size_t n = PrefixPhraseMatchCount(rdr, kPhraseAnl, "fo", "fo", slop);
    EXPECT_EQ(0u, n) << "fo* fo* slop=" << slop
                     << " (ES-verified: identical per-segment expansions "
                        "form one component, strict)";
  }
}

namespace tests {

// aa@1 and cc@1 share position 1 (cc rides at increment 0); ee@2 follows.
// Offsets mimic a source text "aa cc ee".
class VariadicOverlapField : public tests::FieldBase {
 public:
  VariadicOverlapField(std::string_view name, irs::field_id field_id) {
    this->Name(std::string(name));
    this->id = field_id;
    index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                     irs::IndexFeatures::Offs;
  }

  irs::Tokenizer& GetTokens() const final {
    _stream.reset({});
    return _stream;
  }

 private:
  bool Write(irs::DataOutput&) const final { return false; }

  static constexpr std::array<FixedTokensStream::Token, 3> kTokens{{
    {"aa", 1, 0, 2},
    {"cc", 0, 3, 5},
    {"ee", 1, 6, 8},
  }};
  mutable FixedTokensStream _stream{kTokens};
};

class VariadicOverlapDocGenerator : public tests::DocGeneratorBase {
 public:
  explicit VariadicOverlapDocGenerator(irs::field_id field_id)
    : _field_id(field_id) {}

  const tests::Document* next() final {
    if (_done) {
      return nullptr;
    }
    _done = true;
    _doc.indexed.clear();
    _doc.stored.clear();
    _doc.insert(std::make_shared<VariadicOverlapField>("phrase_anl", _field_id),
                /*indexed=*/true, /*stored=*/false);
    return &_doc;
  }

  void reset() final { _done = false; }

 private:
  irs::field_id _field_id;
  tests::Document _doc;
  bool _done{false};
};

}  // namespace tests
namespace {

// Count docs matched by a variadic phrase (one ByTermsOptions set per slot).
size_t TermsPhraseMatchCount(
  const irs::IndexReader& rdr, irs::field_id field,
  const std::vector<std::vector<std::string_view>>& slots,
  irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = field;
  for (const auto& slot : slots) {
    auto& part = q.mutable_options()->push_back<irs::ByTermsOptions>();
    for (const auto t : slot) {
      part.terms.emplace(irs::ViewCast<irs::byte_type>(t));
    }
  }
  q.mutable_options()->set_slop(slop);

  tests::PreparedFilter prepared{q, rdr};
  size_t count = 0;
  for (auto sub = rdr.begin(); sub != rdr.end(); ++sub) {
    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->advance())) {
      ++count;
    }
  }
  return count;
}

}  // namespace

// ES-verified (exp 2 of ES_verify_slop_uniqueness): variadic slots with
// disjoint term sets may share a position at cost 1; identical or
// intersecting sets form one component and stay strict. Group identity is
// the QUERY set, so a shared term absent from the index still connects
// its slots (a3.1).
TEST_P(PhraseFilterTestCase, sloppy_phrase_variadic_disjoint_same_position) {
  {
    tests::VariadicOverlapDocGenerator gen(kPhraseAnl);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // Corpus-shape controls: aa@1 and cc@1 share a position, ee@2 follows.
  EXPECT_EQ(1u, TermsPhraseMatchCount(rdr, kPhraseAnl, {{"aa"}, {"ee"}}, 0));
  EXPECT_EQ(1u, TermsPhraseMatchCount(rdr, kPhraseAnl, {{"cc"}, {"ee"}}, 0));

  // The pinned divergence (a2): disjoint sets, shared position, cost 1.
  EXPECT_EQ(0u, TermsPhraseMatchCount(rdr, kPhraseAnl, {{"aa"}, {"cc"}}, 0));
  EXPECT_EQ(1u, TermsPhraseMatchCount(rdr, kPhraseAnl, {{"aa"}, {"cc"}}, 1));
  // Absent terms widen the query sets without changing the expansions;
  // the sets stay disjoint.
  EXPECT_EQ(1u, TermsPhraseMatchCount(rdr, kPhraseAnl,
                                      {{"aa", "zz"}, {"cc", "qq"}}, 1));

  // Identical sets: one component, one shared position, no legal tuple
  // (ES ctrl B).
  for (const uint32_t slop : {0u, 1u, 2u, 5u}) {
    EXPECT_EQ(0u, TermsPhraseMatchCount(rdr, kPhraseAnl,
                                        {{"aa", "cc"}, {"aa", "cc"}}, slop))
      << "slop=" << slop;
  }

  // (a3.1): the shared term is NOT in the index, yet the query-level sets
  // intersect - one component, strict.
  for (const uint32_t slop : {0u, 1u, 2u, 5u}) {
    EXPECT_EQ(0u, TermsPhraseMatchCount(
                    rdr, kPhraseAnl, {{"aa", "ghost"}, {"ghost", "cc"}}, slop))
      << "slop=" << slop;
  }

  // n == 3, all sets disjoint: tuple (aa@1, cc@1, ee@2) costs 1.
  EXPECT_EQ(
    0u, TermsPhraseMatchCount(rdr, kPhraseAnl, {{"aa"}, {"cc"}, {"ee"}}, 0));
  EXPECT_EQ(
    1u, TermsPhraseMatchCount(rdr, kPhraseAnl, {{"aa"}, {"cc"}, {"ee"}}, 1));

  // n == 3 with a repeated set: the group-0 slots cannot both sit on the
  // single aa position.
  for (const uint32_t slop : {0u, 1u, 5u}) {
    EXPECT_EQ(0u, TermsPhraseMatchCount(rdr, kPhraseAnl,
                                        {{"aa"}, {"cc"}, {"aa"}}, slop))
      << "slop=" << slop;
  }
}

// Offsets path: enumeration must agree with freq under group-scoped
// uniqueness on both the n == 2 join and the n >= 3 Run + BuildMatches
// paths (the latter asserts enumerated == freq).
TEST_P(PhraseFilterTestCase,
       sloppy_phrase_variadic_disjoint_same_position_with_offsets) {
  {
    tests::VariadicOverlapDocGenerator gen(kPhraseAnl);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  const auto run = [&](const std::vector<std::vector<std::string_view>>& slots,
                       irs::PosAttr::value_t slop, size_t want_matches,
                       uint32_t want_start, uint32_t want_end) {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    for (const auto& slot : slots) {
      auto& part = q.mutable_options()->push_back<irs::ByTermsOptions>();
      for (const auto t : slot) {
        part.terms.emplace(irs::ViewCast<irs::byte_type>(t));
      }
    }
    q.mutable_options()->set_slop(slop);

    tests::PreparedFilter prepared{q, rdr};
    auto* phrase_query =
      dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0));
    ASSERT_NE(nullptr, phrase_query);

    auto sub = rdr.begin();
    auto docs = phrase_query->ExecuteWithOffsets(*sub);
    ASSERT_NE(nullptr, docs);
    auto* pos = irs::GetMutable<irs::PosAttr>(docs.get());
    ASSERT_NE(nullptr, pos);
    auto* offs = irs::get<irs::OffsAttr>(*pos);
    ASSERT_NE(nullptr, offs);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->advance()));
    size_t matches = 0;
    while (pos->next()) {
      EXPECT_EQ(want_start, offs->start);
      EXPECT_EQ(want_end, offs->end);
      ++matches;
    }
    EXPECT_EQ(want_matches, matches);
    ASSERT_FALSE(!irs::doc_limits::eof(docs->advance()));
  };

  // n == 2 join path: single (aa@1, cc@1) pair; positions tie, phrase
  // slot 0 supplies both ends -> the aa token's offsets.
  run({{"aa"}, {"cc"}}, 1, 1, 0, 2);
  // n == 3 Run + BuildMatches path: leftmost aa@1, rightmost ee@2.
  run({{"aa"}, {"cc"}, {"ee"}}, 1, 1, 0, 8);
}
