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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/offsets/make.hpp"
#include "iresearch/search/offsets/root.hpp"
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

// Every occurrence the offsets plan reports for one document.
std::vector<irs::offsets::Range> ReadOffsets(irs::offsets::Root& offs,
                                             irs::doc_id_t doc) {
  std::vector<irs::offsets::Range> out;
  std::array<irs::offsets::Range, 16> buf;
  for (;;) {
    const auto n = offs.Run(doc, buf);
    out.insert(out.end(), buf.begin(), buf.begin() + n);
    if (n != buf.size()) {
      return out;
    }
  }
}

struct BlockAttrs {
  const irs::FreqBlockAttr* freq = nullptr;
  const irs::BoostBlockAttr* boost = nullptr;
};

// Records the block attributes a plan publishes when it prepares its score,
// and scores exactly as the scorer it wraps. `Target` picks which record the
// next prepare fills, so two plans of one query can be read apart.
class CapturingScorer final : public irs::Scorer {
 public:
  CapturingScorer(const irs::Scorer& impl, BlockAttrs& attrs) noexcept
    : _impl{impl}, _attrs{&attrs} {}

  void Target(BlockAttrs& attrs) noexcept { _attrs = &attrs; }

  void collect(irs::byte_type* stats, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final {
    _impl.collect(stats, field, term);
  }

  irs::IndexFeatures GetIndexFeatures() const final {
    return _impl.GetIndexFeatures();
  }

  irs::ScoreFunction PrepareScorer(const irs::ScoreContext& ctx) const final {
    _attrs->freq = irs::get<irs::FreqBlockAttr>(ctx.doc_attrs);
    _attrs->boost = irs::get<irs::BoostBlockAttr>(ctx.doc_attrs);
    return _impl.PrepareScorer(ctx);
  }

  irs::ScoreBoundWriter::ptr PrepareScoreBoundWriter(
    size_t max_levels) const final {
    return _impl.PrepareScoreBoundWriter(max_levels);
  }

  irs::ScoreBoundSource::ptr PrepareScoreBoundSource() const final {
    return _impl.PrepareScoreBoundSource();
  }

  bool Compatible(const irs::ScorerOptions& persisted) const noexcept final {
    return _impl.Compatible(persisted);
  }

  size_t stats_size() const final { return _impl.stats_size(); }

  irs::TypeInfo::type_id type() const noexcept final { return _impl.type(); }

 private:
  const irs::Scorer& _impl;
  BlockAttrs* _attrs;
};

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
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
  }

  // empty phrase
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;

    tests::PreparedFilter prepared{q, rdr};

    auto docs = prepared.Execute(0);
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
  }

  // TermSetOptions "fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::TermSetOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
  }

  // TermSetOptions "fox|that"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::TermSetOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X2", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X5", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X2", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X5", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
  }

  // search "fo*" on field without positions
  // which is ok for the first word in phrase
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhrase;
    auto& pt = q.mutable_options()->push_back<irs::ByPrefixOptions>();
    pt.term = irs::ViewCast<irs::byte_type>(std::string_view("fo"));

    tests::PreparedFilter prepared{q, rdr};
    // One part is no phrase: it is planned as whatever that part is, which is
    // a set of terms where the dictionary answered with several and the term
    // itself where it answered with one.
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0)));
    ASSERT_EQ(nullptr,
              dynamic_cast<const irs::VariadicPhraseQuery*>(prepared.Query(0)));
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X0", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X1", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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

    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{*Lower(make_q(), &sort), rdr, &capture};
    ASSERT_EQ(6, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    {
      tests::PreparedFilter unscored{*Lower(make_q()), rdr};
      ASSERT_NE(nullptr, unscored.Execute(0));
    }

    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    it = docs.get();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "X", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "Y", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
  }

  // "quick|quilt|hhh brown|brother fox"
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st1 = q.mutable_options()->push_back<irs::TermSetOptions>();
    st1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("quick")));
    st1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("quilt")));
    st1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("hhh")));
    auto& st2 = q.mutable_options()->push_back<irs::TermSetOptions>();
    st2.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("brown")));
    st2.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("brother")));
    auto& st3 = q.mutable_options()->push_back<irs::TermSetOptions>();
    st3.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));

    tests::PreparedFilter prepared{q, rdr};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto docs = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};
    ASSERT_EQ(3, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
    auto sub = rdr.begin();

    {
      tests::PreparedFilter unscored{q, rdr};
      ASSERT_NE(nullptr, unscored.Execute(0));
    }

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));
    it = docs.get();

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{*scorer, attrs};
    tests::PreparedFilter prepared{*Lower(std::move(q), scorer.get()), rdr,
                                   &capture};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    // Each slot found one term, so what that term is worth weighs every match
    // of the phrase the same: it is a factor of the query's own boost rather
    // than something reported per document.
    ASSERT_FALSE(attrs.boost);
    ASSERT_FLOAT_EQ((0.5f + 0.75f) / 2, prepared.Query(0)->Boost());
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{*scorer, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
  }

  // =============================
  // jumps ... (jumps|hotdog|the) with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& pos0 = q.mutable_options()->push_back<irs::TermSetOptions>();
    pos0.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("jumps")));
    auto& pos1 = q.mutable_options()->push_back<irs::TermSetOptions>(1);
    pos1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("jumps")),
                       0.25f);
    pos1.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("hotdog")), 0.5f);
    pos1.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("the")),
                       0.75f);

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{*scorer, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    const auto* boost = attrs.boost;
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.75f) / 2, boost->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ(((1.f + 0.25f) / 2 + (1.f + 0.5f) / 2) / 2,
                    boost->value[0]);
    ASSERT_EQ(
      "O", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(
      "P", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(3, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(
      "Q", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_FLOAT_EQ((1.f + 0.25f) / 2, boost->value[0]);
    ASSERT_EQ(
      "R", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
  }

  // TermSetOptions "fox|that" with scorer
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::TermSetOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{*scorer, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
  }

  // TermSetOptions "fox|that" with scorer and boost
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    auto& st = q.mutable_options()->push_back<irs::TermSetOptions>();
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")),
                     0.5f);
    st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));

    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{*scorer, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    const auto* boost = attrs.boost;
    ASSERT_TRUE(boost);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(irs::kNoBoost, boost->value[0]);
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(0.5f, boost->value[0]);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_EQ(boost->value[0], seek_attrs.boost->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{*scorer, attrs};
    tests::PreparedFilter prepared{*Lower(std::move(q), scorer.get()), rdr,
                                   &capture};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "Z", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    auto& st = q->mutable_options()->push_back<irs::TermSetOptions>();
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    it = docs.get();
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    irs::score_t score_value{};
    score.Score(&score_value, 1);
    ASSERT_EQ(docs->Value(), score_value);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{*Lower(std::move(q), &sort), rdr, &capture};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    it = docs.get();
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    irs::score_t score_value{};
    score.Score(&score_value, 1);
    ASSERT_EQ(docs->Value(), score_value);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    // Check repeatable seek to the same document given frequency of the phrase
    // within the document = 2
    auto v = docs->Value();
    ASSERT_EQ(v, docs->Seek(docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};

    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    it = docs.get();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ("PHW0", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ("PHW1", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ("PHW2", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ("PHW3", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ("PHW4", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ("PHW5", irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
  }

  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("fox"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("second"));

    irs::BooleanFilter disjunction;
    auto add_phrase = [&](size_t off) {
      auto phrase = std::make_unique<irs::ByPhrase>();
      auto& ph = *phrase;
      *ph.mutable_field_id() = kPhraseAnl;
      ph.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fox"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off).term =
        irs::ViewCast<irs::byte_type>(std::string_view("second"));
      disjunction.Add(std::move(phrase), irs::Occur::Should);
    };
    add_phrase(0);
    add_phrase(1);
    add_phrase(2);
    disjunction.SetMinShouldMatch(1);

    tests::sort::CustomSort sort;
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };
    auto sub = rdr.begin();
    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    irs::ColumnArgsFetcher disj_fetcher;
    auto disj_docs = disj_prepared.ExecuteScored(0, disj_fetcher);
    auto disj_score = disj_docs->PrepareScore();
    irs::score_t score_val;

    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    it = docs.get();

    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(2, freq->value[0]);
    ASSERT_EQ(
      "C", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);
    ASSERT_EQ(
      "D", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(4, freq->value[0]);
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->Advance()));
  }

  {
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    auto check_freq = [&](const irs::ByPhrase& q, std::string_view name,
                          uint32_t expected) {
      tests::sort::FrequencyScore scorer;
      BlockAttrs attrs;
      BlockAttrs seek_attrs;
      CapturingScorer capture{scorer, attrs};
      tests::PreparedFilter prepared{q, rdr, &capture};

      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(0, fetcher);
      auto score = docs->PrepareScore();
      const auto* freq = attrs.freq;
      ASSERT_TRUE(freq);
      ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
      capture.Target(seek_attrs);
      irs::ColumnArgsFetcher seek_fetcher;
      auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
      auto seek_score = docs_seek->PrepareScore();
      ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      ASSERT_EQ(expected, freq->value[0]);
      irs::score_t score_val;
      score.Score(&score_val, 1);
      ASSERT_DOUBLE_EQ(expected, score_val);
      ASSERT_EQ(name, irs::tests::ReadStoredStr<std::string_view>(
                        values, docs->Value()));
      ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
      docs_seek->FetchScoreArgs(0);
      ASSERT_EQ(freq->value[0], seek_attrs.freq->value[0]);
      ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
    };

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("konstantin"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("vedernikoff"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("is"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("intern"));
      check_freq(q, "L", 2);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("alfa"));
      q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("mike"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("oscar"));
      check_freq(q, "M", 2);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByPrefixOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("alf"));
      q.mutable_options()->push_back<irs::ByPrefixOptions>(2, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("mik"));
      q.mutable_options()->push_back<irs::ByPrefixOptions>(1, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("osc"));
      check_freq(q, "M", 2);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quebec"));
      q.mutable_options()->push_back<irs::ByPrefixOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("mike"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("sierra"));
      check_freq(q, "P", 2);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quebec"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("mikeone"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("sierra"));
      check_freq(q, "P", 1);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quebec"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("miketwo"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("sierra"));
      check_freq(q, "P", 1);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("tangoq"));
      q.mutable_options()->push_back<irs::ByPrefixOptions>(0, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("zeta"));
      check_freq(q, "Q", 2);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("romeor"));
      q.mutable_options()->push_back<irs::ByPrefixOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("zeta"));
      q.mutable_options()->push_back<irs::ByTermOptions>(2, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("sierrar"));
      check_freq(q, "R", 1);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("romeor"));
      q.mutable_options()->push_back<irs::ByPrefixOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("zeta"));
      q.mutable_options()->push_back<irs::ByTermOptions>(2, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("sierrar"));

      tests::PreparedFilter prepared{q, rdr};
      auto docs = prepared.Execute(0);
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ("R", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs->Value()));
      ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
    }

    {
      auto q = std::make_unique<irs::ByPhrase>();
      *q->mutable_field_id() = kPhraseAnl;
      q->mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("delta"));
      q->mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("golf"));
      auto& lt =
        q->mutable_options()->push_back<irs::ByEditDistanceOptions>(1, 3);
      lt.max_distance = 1;
      lt.term = irs::ViewCast<irs::byte_type>(std::string_view("hotel"));

      tests::sort::FrequencyScore scorer;
      BlockAttrs attrs;
      CapturingScorer capture{scorer, attrs};
      tests::PreparedFilter prepared{*Lower(std::move(q), &scorer), rdr,
                                     &capture};
      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(0, fetcher);
      auto score = docs->PrepareScore();
      const auto* freq = attrs.freq;
      ASSERT_TRUE(freq);
      const auto* boost = attrs.boost;
      ASSERT_TRUE(boost);
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      docs->FetchScoreArgs(0);
      ASSERT_EQ(2, freq->value[0]);
      ASSERT_DOUBLE_EQ(irs::kNoBoost, boost->value[0]);
      ASSERT_EQ("S", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs->Value()));
      ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("papa"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
        irs::ViewCast<irs::byte_type>(std::string_view("romeo"));
      q.mutable_options()->push_back<irs::ByTermOptions>(3, 4).term =
        irs::ViewCast<irs::byte_type>(std::string_view("tango"));
      check_freq(q, "N", 2);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("uniform"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("victor"));
      q.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
        irs::ViewCast<irs::byte_type>(std::string_view("xray"));
      q.mutable_options()->push_back<irs::ByTermOptions>(3, 5).term =
        irs::ViewCast<irs::byte_type>(std::string_view("zulu"));
      check_freq(q, "O", 2);
    }

    {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("uniform"));
      q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("victor"));
      q.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
        irs::ViewCast<irs::byte_type>(std::string_view("xray"));
      q.mutable_options()->push_back<irs::ByTermOptions>(3, 5).term =
        irs::ViewCast<irs::byte_type>(std::string_view("zulu"));

      tests::PreparedFilter prepared{q, rdr};
      auto docs = prepared.Execute(0);
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ("O", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs->Value()));
      ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
    }
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    auto docs_seek = prepared.Execute(0);
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "B", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    irs::BooleanFilter disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto phrase = std::make_unique<irs::ByPhrase>();
      auto& ph = *phrase;
      *ph.mutable_field_id() = kPhraseAnl;
      ph.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fox"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off1).term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
      disjunction.Add(std::move(phrase), irs::Occur::Should);
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
    disjunction.SetMinShouldMatch(1);

    tests::sort::CustomSort sort;
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));
    const auto* freq_seek = seek_attrs.freq;
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    irs::ColumnArgsFetcher disj_fetcher;
    auto disj_docs = disj_prepared.ExecuteScored(0, disj_fetcher);
    auto disj_score = disj_docs->PrepareScore();
    irs::score_t score_val;

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(1, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(6, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(11, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(2, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->Advance()));
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

    irs::BooleanFilter disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto phrase = std::make_unique<irs::ByPhrase>();
      auto& ph = *phrase;
      *ph.mutable_field_id() = kPhraseAnl;
      ph.mutable_options()->push_back<irs::ByPrefixOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fo"));
      ph.mutable_options()->push_back<irs::ByPrefixOptions>(off1).term =
        irs::ViewCast<irs::byte_type>(std::string_view("qui"));
      ph.mutable_options()->push_back<irs::ByPrefixOptions>(off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("bro"));
      disjunction.Add(std::move(phrase), irs::Occur::Should);
    };
    add_phrase(3, 1);
    add_phrase(3, 2);
    add_phrase(4, 1);
    add_phrase(4, 2);
    disjunction.SetMinShouldMatch(1);

    tests::sort::CustomSort sort;
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));
    const auto* freq_seek = seek_attrs.freq;
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    irs::ColumnArgsFetcher disj_fetcher;
    auto disj_docs = disj_prepared.ExecuteScored(0, disj_fetcher);
    auto disj_score = disj_docs->PrepareScore();
    irs::score_t score_val;

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "E", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(1, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "F", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(5, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "H", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->Advance()));
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

    irs::BooleanFilter disjunction;
    auto add_phrase = [&](size_t off1, size_t off2) {
      auto phrase = std::make_unique<irs::ByPhrase>();
      auto& ph = *phrase;
      *ph.mutable_field_id() = kPhraseAnl;
      ph.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("zoo"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off1).term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      ph.mutable_options()->push_back<irs::ByTermOptions>(off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
      disjunction.Add(std::move(phrase), irs::Occur::Should);
    };
    add_phrase(0, 0);
    add_phrase(0, 1);
    add_phrase(0, 2);
    add_phrase(0, 3);
    disjunction.SetMinShouldMatch(1);

    tests::sort::CustomSort sort;
    tests::LeadCursor* it = nullptr;
    sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                            size_t n) {
      ASSERT_NE(nullptr, it);
      *score = it->Value();
    };

    BlockAttrs attrs;
    BlockAttrs seek_attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};
    auto sub = rdr.begin();
    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};
    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    it = docs.get();
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    capture.Target(seek_attrs);
    irs::ColumnArgsFetcher seek_fetcher;
    auto docs_seek = prepared.ExecuteScored(0, seek_fetcher);
    auto seek_score = docs_seek->PrepareScore();
    ASSERT_FALSE(irs::doc_limits::valid(docs_seek->Value()));
    const auto* freq_seek = seek_attrs.freq;
    ASSERT_TRUE(freq_seek);

    tests::sort::FrequencyScore freq_score;
    tests::PreparedFilter disj_prepared{disjunction, rdr, &freq_score};
    irs::ColumnArgsFetcher disj_fetcher;
    auto disj_docs = disj_prepared.ExecuteScored(0, disj_fetcher);
    auto disj_score = disj_docs->PrepareScore();
    irs::score_t score_val;

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "K", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(docs->Value(), docs_seek->Seek(docs->Value()));
    docs_seek->FetchScoreArgs(0);
    ASSERT_EQ(3, freq_seek->value[0]);
    ASSERT_TRUE(!irs::doc_limits::eof(disj_docs->Advance()));
    ASSERT_EQ(docs->Value(), disj_docs->Value());
    disj_docs->FetchScoreArgs(0);
    disj_score.Score(&score_val, 1);
    ASSERT_DOUBLE_EQ(score_val, freq_seek->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_FALSE(!irs::doc_limits::eof(disj_docs->Advance()));
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
  ASSERT_EQ(irs::kNoBoost, q.GetBoost());

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
      q.SetBoost(boost);

      tests::PreparedFilter prepared{q, irs::SubReader::empty()};
      ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
    }

    // single term
    {
      irs::ByPhrase q;
      *q.mutable_field_id() = 1;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      q.SetBoost(boost);
      ASSERT_EQ(boost, q.GetBoost());

      // a segment without the field matches nothing, and nothing carries no
      // boost -- so the boost is only observable where the field exists
      tests::PreparedFilter prepared{q, irs::SubReader::empty(), nullptr,
                                     counter};
      ASSERT_TRUE(irs::QueryBuilder::IsEmpty(*prepared.Query(0)));
      ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    counter.Reset();

    // single multiple terms
    {
      irs::ByPhrase q;
      *q.mutable_field_id() = 1;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
      q.SetBoost(boost);

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
      q.SetBoost(boost);
      auto& st = q.mutable_options()->push_back<irs::TermSetOptions>();
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

      irs::TermSetOptions ct1;
      ct1.terms.emplace(
        irs::ViewCast<irs::byte_type>(std::string_view("bird")));
      q.push_back(ct1);
      const auto& ct2 = std::get<irs::TermSetOptions>((--q.end())->part);
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
      auto& ct1 = q0.mutable_options()->push_back<irs::TermSetOptions>();
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
      auto& ct1 = q1.mutable_options()->push_back<irs::TermSetOptions>();
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
      auto& ct1 = q0.mutable_options()->push_back<irs::TermSetOptions>();
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
      auto& ct1 = q1.mutable_options()->push_back<irs::TermSetOptions>();
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
    irs::TermSetOptions ct;
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
      while (!irs::doc_limits::eof(docs->Advance())) {
        out.push_back(docs->Value());
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
    // Excluding needs something to exclude from: a node holding nothing but a
    // negation has no include side and so matches nothing.
    auto not_phrase = std::make_unique<irs::BooleanFilter>();
    not_phrase->Add(std::make_unique<irs::All>(), irs::Occur::Must);
    not_phrase->Add(make_phrase(), irs::Occur::MustNot);
    irs::Filter::ptr f = std::move(not_phrase);
    irs::Optimize(f, {});
    return f;
  };

  auto collect = [&](const irs::Filter& filter) {
    std::vector<irs::doc_id_t> out;
    tests::PreparedFilter prepared{filter, rdr};
    for (size_t i = 0, end = prepared.size(); i < end; ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->Advance())) {
        out.push_back(docs->Value());
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
      const auto landed = docs->Seek(target);
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "L", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X3", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "X4", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ(expected, irs::tests::ReadStoredStr<std::string_view>(
                            values, docs->Value()));
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
  }

  // Single terms-set part + slop: one slot makes slop meaningless; must
  // match exactly what the same query matches at slop 0.
  {
    const auto collect = [&](irs::PosAttr::value_t slop) {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      auto& st = q.mutable_options()->push_back<irs::TermSetOptions>();
      st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("fox")));
      st.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view("that")));
      q.mutable_options()->set_slop(slop);

      tests::PreparedFilter prepared{q, rdr};
      std::vector<irs::doc_id_t> out;
      auto docs = prepared.Execute(0);
      while (!irs::doc_limits::eof(docs->Advance())) {
        out.push_back(docs->Value());
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
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
  }

  // Empty phrase + slop.
  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->set_slop(5);

    tests::PreparedFilter prepared{q, rdr};
    auto docs = prepared.Execute(0);
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
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
    // Unstarted, as any stream is before its first `Advance`.
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "W", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
  // A,G,I,T: freq=1. L: freq=1 (reversal d=3). N: freq=7.
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

    BlockAttrs attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};

    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    // L: reversal, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    // N: freq=8
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(8, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    ASSERT_FALSE(irs::doc_limits::valid(docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    auto docs_seek = prepared.Execute(0);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    auto sought = docs_seek->Seek(docs->Value());
    ASSERT_EQ(docs->Value(), sought);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "U", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));

    ASSERT_TRUE(irs::doc_limits::eof(docs_seek->Seek(irs::doc_limits::eof())));

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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

    const auto* query = prepared.Query(0);
    ASSERT_NE(nullptr, query);
    auto count = query->PlanCount({});
    ASSERT_NE(nullptr, count);
    ASSERT_EQ(5, count->Run());
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

      while (!irs::doc_limits::eof(docs->Advance())) {
        auto name =
          irs::tests::ReadStoredStr<std::string_view>(values, docs->Value());
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

    BlockAttrs attrs;
    CapturingScorer capture{sort, attrs};
    tests::PreparedFilter prepared{q, rdr, &capture};
    auto sub = rdr.begin();

    irs::ColumnArgsFetcher fetcher;
    auto docs = prepared.ExecuteScored(0, fetcher);
    auto score = docs->PrepareScore();
    const auto* freq = attrs.freq;
    ASSERT_TRUE(freq);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // A: qui*=1(quick), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(1, freq->value[0]);

    // G: qui*=5(quick), fox=7, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(1, freq->value[0]);

    // I: qui*=1(quick), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "I", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(1, freq->value[0]);

    // N: multiple combos, best d=0, boost=1.0
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_GT(freq->value[0], 1);

    // S: qui*=[1,2](quick,quilt), fox=[4].
    // quilt=2,fox=4 d=1. quick=1,fox=4 d=2>1. freq=1.
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(1, freq->value[0]);

    // T: qui*=1(quick), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "T", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(1, freq->value[0]);

    // V: qui*=1(quilt), fox=3, d=1, freq=1
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    docs->FetchScoreArgs(0);
    ASSERT_EQ(
      "V", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_EQ(1, freq->value[0]);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
  }
}

TEST_P(PhraseFilterTestCase, interval_combinations) {
  {
    tests::JsonDocGenerator gen(resource("phrase_interval.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());
  auto sub = rdr.begin();
  const auto* column = sub->Column(kName);
  ASSERT_NE(nullptr, column);
  irs::tests::BlobPointReader values{*sub, *column};

  enum class Part {
    kTerm,
    kPrefix,
    kEditDistance,
  };
  enum class Entry {
    kMatch,
    kScore,
    kOffsets,
  };

  struct Case {
    std::string_view label;
    Part part;
    bool intervals;
    Entry entry;
    uint32_t freq;
    bool boost;
  };

  static constexpr Case kCases[] = {
    {"fixed match", Part::kTerm, false, Entry::kMatch, 0, false},
    {"fixed score", Part::kTerm, false, Entry::kScore, 1, false},
    {"fixed offsets", Part::kTerm, false, Entry::kOffsets, 0, false},
    {"fixed interval match", Part::kTerm, true, Entry::kMatch, 0, false},
    {"fixed interval score", Part::kTerm, true, Entry::kScore, 2, false},
    {"fixed interval offsets", Part::kTerm, true, Entry::kOffsets, 0, false},
    {"variadic match", Part::kPrefix, false, Entry::kMatch, 0, false},
    {"variadic score", Part::kPrefix, false, Entry::kScore, 1, false},
    {"variadic offsets", Part::kPrefix, false, Entry::kOffsets, 0, false},
    {"variadic interval match", Part::kPrefix, true, Entry::kMatch, 0, false},
    {"variadic interval score", Part::kPrefix, true, Entry::kScore, 2, false},
    {"variadic interval offsets", Part::kPrefix, true, Entry::kOffsets, 0,
     false},
    {"editdist match", Part::kEditDistance, false, Entry::kMatch, 0, false},
    {"editdist score", Part::kEditDistance, false, Entry::kScore, 1, true},
    {"editdist offsets", Part::kEditDistance, false, Entry::kOffsets, 0, false},
    {"editdist interval match", Part::kEditDistance, true, Entry::kMatch, 0,
     false},
    {"editdist interval score", Part::kEditDistance, true, Entry::kScore, 2,
     true},
    {"editdist interval offsets", Part::kEditDistance, true, Entry::kOffsets, 0,
     false},
  };

  for (const auto& c : kCases) {
    SCOPED_TRACE(c.label);

    auto q = std::make_unique<irs::ByPhrase>();
    *q->mutable_field_id() = kPhraseAnl;
    auto& opts = *q->mutable_options();
    const size_t max1 = c.intervals ? 3 : 2;
    const size_t min2 = c.intervals ? 1 : 2;
    const size_t max2 = c.intervals ? 3 : 2;
    switch (c.part) {
      case Part::kTerm:
        opts.push_back<irs::ByTermOptions>().term =
          irs::ViewCast<irs::byte_type>(std::string_view("delta"));
        opts.push_back<irs::ByTermOptions>(2, max1).term =
          irs::ViewCast<irs::byte_type>(std::string_view("golf"));
        opts.push_back<irs::ByTermOptions>(min2, max2).term =
          irs::ViewCast<irs::byte_type>(std::string_view("hotel"));
        break;
      case Part::kPrefix:
        opts.push_back<irs::ByPrefixOptions>().term =
          irs::ViewCast<irs::byte_type>(std::string_view("del"));
        opts.push_back<irs::ByPrefixOptions>(2, max1).term =
          irs::ViewCast<irs::byte_type>(std::string_view("gol"));
        opts.push_back<irs::ByPrefixOptions>(min2, max2).term =
          irs::ViewCast<irs::byte_type>(std::string_view("hot"));
        break;
      case Part::kEditDistance: {
        opts.push_back<irs::ByTermOptions>().term =
          irs::ViewCast<irs::byte_type>(std::string_view("delta"));
        opts.push_back<irs::ByTermOptions>(2, max1).term =
          irs::ViewCast<irs::byte_type>(std::string_view("golf"));
        auto& lt = opts.push_back<irs::ByEditDistanceOptions>(min2, max2);
        lt.max_distance = 1;
        lt.term = irs::ViewCast<irs::byte_type>(std::string_view("hotel"));
      } break;
    }

    tests::sort::FrequencyScore scorer;
    const bool scored = c.entry == Entry::kScore;
    auto lowered = Lower(std::move(q), scored ? &scorer : nullptr);
    BlockAttrs attrs;
    CapturingScorer capture{scorer, attrs};
    tests::PreparedFilter prepared{*lowered, rdr, scored ? &capture : nullptr};
    const auto* query = prepared.Query(0);
    ASSERT_NE(nullptr, query);

    if (c.entry == Entry::kOffsets) {
      irs::offsets::Root::ptr offs;
      if (const auto* fixed =
            dynamic_cast<const irs::FixedPhraseQuery*>(query)) {
        offs = irs::offsets::Make(*fixed);
      } else {
        const auto* variadic =
          dynamic_cast<const irs::VariadicPhraseQuery*>(query);
        ASSERT_NE(nullptr, variadic);
        offs = irs::offsets::Make(*variadic);
      }
      ASSERT_NE(nullptr, offs);

      auto docs = prepared.Execute(0);
      ASSERT_NE(nullptr, docs);
      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ("S", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs->Value()));

      std::array<irs::offsets::Range, 8> ranges;
      const auto n = offs->Run(docs->Value(), ranges);
      ASSERT_GE(n, 1u);
      ASSERT_EQ(4, ranges[0].start);
      ASSERT_EQ(29, ranges[0].end);

      ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
      continue;
    }

    if (scored) {
      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(0, fetcher);
      ASSERT_NE(nullptr, docs);
      auto score = docs->PrepareScore();

      const auto* freq = attrs.freq;
      const auto* boost = attrs.boost;
      ASSERT_NE(nullptr, freq);
      ASSERT_EQ(c.boost, boost != nullptr);

      ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
      ASSERT_EQ("S", irs::tests::ReadStoredStr<std::string_view>(
                       values, docs->Value()));

      docs->FetchScoreArgs(0);
      fetcher.Fetch(docs->Value());
      ASSERT_EQ(c.freq, freq->value[0]);
      if (c.boost) {
        ASSERT_DOUBLE_EQ(irs::kNoBoost, boost->value[0]);
      }

      ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
      continue;
    }

    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "S", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
  }
}

TEST_P(PhraseFilterTestCase, interval_execute_with_offsets) {
  {
    tests::JsonDocGenerator gen(resource("phrase_interval.json"),
                                &tests::AnalyzedJsonFieldFactory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("alfa"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("mike"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 3).term =
      irs::ViewCast<irs::byte_type>(std::string_view("oscar"));

    tests::PreparedFilter prepared{q, rdr};
    auto* phrase_query =
      dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0));
    ASSERT_NE(nullptr, phrase_query);

    auto sub = rdr.begin();
    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    auto offs = irs::offsets::Make(*phrase_query);
    ASSERT_NE(nullptr, offs);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "M", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    const auto ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_EQ(4, ranges[0].start);
    ASSERT_EQ(28, ranges[0].end);
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
  }

  {
    irs::ByPhrase q;
    *q.mutable_field_id() = kPhraseAnl;
    q.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("uniform"));
    q.mutable_options()->push_back<irs::ByTermOptions>(1, 2).term =
      irs::ViewCast<irs::byte_type>(std::string_view("victor"));
    q.mutable_options()->push_back<irs::ByTermOptions>(2, 4).term =
      irs::ViewCast<irs::byte_type>(std::string_view("xray"));
    q.mutable_options()->push_back<irs::ByTermOptions>(3, 5).term =
      irs::ViewCast<irs::byte_type>(std::string_view("zulu"));

    tests::PreparedFilter prepared{q, rdr};
    auto* phrase_query =
      dynamic_cast<const irs::FixedPhraseQuery*>(prepared.Query(0));
    ASSERT_NE(nullptr, phrase_query);

    auto sub = rdr.begin();
    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    auto offs = irs::offsets::Make(*phrase_query);
    ASSERT_NE(nullptr, offs);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "O", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    const auto ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_EQ(0, ranges[0].start);
    ASSERT_EQ(57, ranges[0].end);
    ASSERT_TRUE(irs::doc_limits::eof(docs->Advance()));
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
    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    auto offs = irs::offsets::Make(*phrase_query);
    ASSERT_NE(nullptr, offs);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // A
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    auto ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_EQ(0, ranges[0].start);
    ASSERT_EQ(15, ranges[0].end);

    // G
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    // I
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    // N: freq=2. Tuples: (6,8) cost=1 leftmost=6, (7,8) cost=0 leftmost=7.
    // Sorted by leftmost ascending: (6,8) first, (7,8) second.
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "N", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(2, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);
    ASSERT_GT(ranges[1].end, ranges[1].start);

    // T
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    auto offs = irs::offsets::Make(*phrase_query);
    ASSERT_NE(nullptr, offs);

    const auto* column = sub->Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{*sub, *column};

    // A: "quick brown fox ..." -> start=0 (quick), end=15 (fox)
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "A", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    auto ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_EQ(0, ranges[0].start);
    ASSERT_EQ(15, ranges[0].end);

    // G
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(
      "G", irs::tests::ReadStoredStr<std::string_view>(values, docs->Value()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    // I
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    // N: freq=2 (tuples (6,8) cost=1, (7,8) cost=0; sorted by leftmost)
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(2, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);
    ASSERT_GT(ranges[1].end, ranges[1].start);

    // S
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    // T
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    // V
    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    ranges = ReadOffsets(*offs, docs->Value());
    ASSERT_EQ(1, ranges.size());
    ASSERT_GT(ranges[0].end, ranges[0].start);

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_TRUE(irs::doc_limits::eof(docs->Value()));
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
    while (!irs::doc_limits::eof(docs->Advance())) {
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

  auto docs = prepared.Execute(0);
  ASSERT_NE(nullptr, docs);
  auto offs = irs::offsets::Make(*phrase_query);
  ASSERT_NE(nullptr, offs);

  ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
  const auto ranges = ReadOffsets(*offs, docs->Value());
  ASSERT_EQ(1, ranges.size())
    << "matcher freq says a match exists, offsets enumeration dropped it";
  // foo@P and bar@P both originate from the single source token "foo",
  // so the single match spans exactly that token.
  EXPECT_LT(ranges[0].start, ranges[0].end);
  ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
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
    while (!irs::doc_limits::eof(docs->Advance())) {
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

    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    auto offs = irs::offsets::Make(*phrase_query);
    ASSERT_NE(nullptr, offs);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    const auto ranges = ReadOffsets(*offs, docs->Value());
    for (const auto& range : ranges) {
      EXPECT_EQ(0u, range.start);
      EXPECT_EQ(9u, range.end);
    }
    EXPECT_EQ(want_matches, ranges.size());
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
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
    while (!irs::doc_limits::eof(docs->Advance())) {
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
    EXPECT_TRUE(!irs::doc_limits::eof(docs->Advance()))
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

// Count docs matched by a variadic phrase (one TermSetOptions set per slot).
size_t TermsPhraseMatchCount(
  const irs::IndexReader& rdr, irs::field_id field,
  const std::vector<std::vector<std::string_view>>& slots,
  irs::PosAttr::value_t slop) {
  irs::ByPhrase q;
  *q.mutable_field_id() = field;
  for (const auto& slot : slots) {
    auto& part = q.mutable_options()->push_back<irs::TermSetOptions>();
    for (const auto t : slot) {
      part.terms.emplace(irs::ViewCast<irs::byte_type>(t));
    }
  }
  q.mutable_options()->set_slop(slop);

  tests::PreparedFilter prepared{q, rdr};
  size_t count = 0;
  for (auto sub = rdr.begin(); sub != rdr.end(); ++sub) {
    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->Advance())) {
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
      auto& part = q.mutable_options()->push_back<irs::TermSetOptions>();
      for (const auto t : slot) {
        part.terms.emplace(irs::ViewCast<irs::byte_type>(t));
      }
    }
    q.mutable_options()->set_slop(slop);

    tests::PreparedFilter prepared{q, rdr};
    // A slot the dictionary answered with one term is that term, so a phrase
    // of such slots is planned fixed however it was spelled. Either way the
    // slop budget is spent by the same matcher over the same groups.
    const auto* query = prepared.Query(0);
    const auto* variadic = dynamic_cast<const irs::VariadicPhraseQuery*>(query);
    const auto* fixed = dynamic_cast<const irs::FixedPhraseQuery*>(query);
    ASSERT_TRUE(variadic != nullptr || fixed != nullptr);

    auto docs = prepared.Execute(0);
    ASSERT_NE(nullptr, docs);
    auto offs =
      variadic ? irs::offsets::Make(*variadic) : irs::offsets::Make(*fixed);
    ASSERT_NE(nullptr, offs);

    ASSERT_TRUE(!irs::doc_limits::eof(docs->Advance()));
    const auto ranges = ReadOffsets(*offs, docs->Value());
    for (const auto& range : ranges) {
      EXPECT_EQ(want_start, range.start);
      EXPECT_EQ(want_end, range.end);
    }
    EXPECT_EQ(want_matches, ranges.size());
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  };

  // n == 2 join path: single (aa@1, cc@1) pair; positions tie, phrase
  // slot 0 supplies both ends -> the aa token's offsets.
  run({{"aa"}, {"cc"}}, 1, 1, 0, 2);
  // n == 3 Run + BuildMatches path: leftmost aa@1, rightmost ee@2.
  run({{"aa"}, {"cc"}, {"ee"}}, 1, 1, 0, 8);
}
