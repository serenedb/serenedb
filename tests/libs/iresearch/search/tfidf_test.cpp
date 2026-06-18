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

#include "formats/column/test_cs_helpers.hpp"
#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_existence_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

template<typename F>
auto PrepareOptimized(const F& filter, const irs::PrepareContext& ctx) {
  irs::Filter::ptr f = std::make_unique<F>(filter);
  irs::Optimize(f);
  return f->prepare(ctx);
}

// Stable per-name field ids, sourced from `tests::FieldIdFor` so the
// canonical JSON factories and these tests agree on the id-per-name.
[[maybe_unused]] inline constexpr irs::field_id kSeq = tests::FieldIdFor("seq");
[[maybe_unused]] inline constexpr irs::field_id kName =
  tests::FieldIdFor("name");
[[maybe_unused]] inline constexpr irs::field_id kField =
  tests::FieldIdFor("field");
[[maybe_unused]] inline constexpr irs::field_id kPhraseAnl =
  tests::FieldIdFor("phrase_anl");
[[maybe_unused]] inline constexpr irs::field_id kPrefix =
  tests::FieldIdFor("prefix");

auto StoreSeq() {
  return [](irs::IndexWriter::Document& doc, const tests::Document& src) {
    const auto* seq =
      dynamic_cast<const tests::StringField*>(src.stored.get_by_id(kSeq));
    if (seq) {
      irs::tests::StoreFieldAt(*doc.GetColWriter(), kSeq, doc.DocId(), *seq);
    }
  };
}

auto StoreName() {
  return [](irs::IndexWriter::Document& doc, const tests::Document& src) {
    const auto* name =
      dynamic_cast<const tests::StringField*>(src.stored.get_by_id(kName));
    if (name) {
      irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(), *name);
    }
  };
}

// Freq | Term
// -----------
// 4    | 0
// 3    | 1
// 10   | 2
// 7    | 3
// 5    | 4
// 4    | 5
// 3    | 6
// 7    | 7
// 2    | 8
// 7    | 9

// Stats
// ---------------------------------------------
// TotalFreq = 52
// DocsCount = 8
// AverageDocLength (TotalFreq/DocsCount) = 6.5

class TfidfTestCase : public IndexTestBase {
 protected:
  void TestQueryNorms();
};

void TfidfTestCase::TestQueryNorms() {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          auto field = std::make_shared<StringField>(
            name, data.str,
            irs::IndexFeatures::Freq | irs::IndexFeatures::Norm);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), true, false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          auto field = std::make_shared<StringField>(
            name, value, irs::IndexFeatures::Freq | irs::IndexFeatures::Norm);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), false, true);
        }
      });

    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreSeq());
  }

  auto scorer = irs::TFIDF{true};
  irs::ColumnArgsFetcher fetcher;

  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  auto& segment = *(reader.begin());
  const auto* column = segment.Column(kSeq);
  ASSERT_NE(nullptr, column);

  MaxMemoryCounter counter;

  // by_range multiple
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    constexpr uint64_t kExpected[]{
      7, 0, 3, 1, 5,
    };

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });

    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(std::size(kExpected), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_range multiple (3 values)
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    constexpr uint64_t kExpected[]{
      0, 7, 5, 2, 3, 1,
    };

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });

    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    std::vector<float_t> scores;
    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);
      scores.emplace_back(score_value);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(std::size(kExpected), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(TfidfTestCase, consts) {
  static_assert("tfidf" == irs::Type<irs::TFIDF>::name());
}

TEST_P(TfidfTestCase, test_load) {
  auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{});

  ASSERT_NE(nullptr, scorer);
}

TEST_P(TfidfTestCase, make_from_bool) {
  // `withNorms` argument
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{.with_norms = true});
    ASSERT_NE(nullptr, scorer);
    auto& tfidf = dynamic_cast<irs::TFIDF&>(*scorer);
    ASSERT_EQ(true, tfidf.normalize());
    ASSERT_EQ(irs::TFIDF::BOOST_AS_SCORE(), tfidf.use_boost_as_score());
  }
}

TEST_P(TfidfTestCase, make_from_array) {
  // default args
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::Type<irs::TFIDF>::id(), scorer->type());
    auto& tfidf = dynamic_cast<irs::TFIDF&>(*scorer);
    ASSERT_EQ(irs::TFIDF::WITH_NORMS(), tfidf.normalize());
    ASSERT_EQ(irs::TFIDF::BOOST_AS_SCORE(), tfidf.use_boost_as_score());
  }

  // `withNorms` argument
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{.with_norms = true});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::Type<irs::TFIDF>::id(), scorer->type());
    auto& tfidf = dynamic_cast<irs::TFIDF&>(*scorer);
    ASSERT_EQ(true, tfidf.normalize());
    ASSERT_EQ(irs::TFIDF::BOOST_AS_SCORE(), tfidf.use_boost_as_score());
  }
}

TEST_P(TfidfTestCase, test_normalize_features) {
  // default norms
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::IndexFeatures::Freq, scorer->GetIndexFeatures());
  }

  // with norms
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{.with_norms = true});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::IndexFeatures::Freq | irs::IndexFeatures::Norm,
              scorer->GetIndexFeatures());
  }

  // without norms
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{.with_norms = false});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::IndexFeatures::Freq, scorer->GetIndexFeatures());
  }
}

TEST_P(TfidfTestCase, test_phrase) {
  auto analyzed_json_field_factory =
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      typedef TextField<std::string> TextField;

      class StringField : public tests::StringField {
       public:
        StringField(const std::string& name, const std::string_view& value)
          : tests::StringField(name, value) {
          this->index_features = irs::IndexFeatures::Freq;
        }
      };

      if (data.is_string()) {
        // analyzed field
        const auto anl_name = std::string(name.c_str()) + "_anl";
        auto analyzed = std::make_shared<TextField>(anl_name, data.str);
        analyzed->id = tests::FieldIdFor(anl_name);
        doc.indexed.push_back(std::move(analyzed));

        // not analyzed field
        auto field = std::make_shared<StringField>(name, data.str);
        field->id = tests::FieldIdFor(name);
        doc.insert(std::move(field));
      }
    };

  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                analyzed_json_field_factory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto scorer = irs::TFIDF{false, true};

  // read segment
  auto index = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, index->size());
  auto& segment = *(index.begin());

  MaxMemoryCounter counter;
  irs::ColumnArgsFetcher fetcher;

  // "jumps high" with order
  {
    irs::ByPhrase filter;
    *filter.mutable_field_id() = kPhraseAnl;
    auto& phrase = *filter.mutable_options();
    phrase.push_back(irs::ByTermOptions{}).term =
      irs::ViewCast<irs::byte_type>(std::string_view("jumps"));
    phrase.push_back(irs::ByTermOptions{}).term =
      irs::ViewCast<irs::byte_type>(std::string_view("high"));

    std::multimap<irs::score_t, std::string, std::greater<>> sorted;

    std::vector<std::string> expected{
      "O",  // jumps high jumps high hotdog
      "P",  // jumps high jumps left jumps right jumps down jumps back
      "Q",  // jumps high jumps left jumps right jumps down walks back
      "R"   // jumps high jumps left jumps right walks down walks back
    };

    auto prepared_filter = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorer = &scorer,
    });

    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    const auto* column = segment.Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);

      irs::BytesViewInput in;
      in.reset(values.Get(docs->value()));
      sorted.emplace(score_value, irs::ReadString<std::string>(in));
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // "cookies ca* p_e bisKuit meringue|marshmallows" with order
  {
    irs::ByPhrase filter;
    *filter.mutable_field_id() = kPhraseAnl;
    auto& phrase = *filter.mutable_options();
    phrase.push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("cookies"));
    phrase.push_back<irs::ByPrefixOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("ca"));
    phrase.push_back<irs::ByWildcardOptions>() = irs::ByWildcardOptions{
      irs::ViewCast<irs::byte_type>(std::string_view("p_e"))};
    auto& lt = phrase.push_back<irs::ByEditDistanceOptions>();
    lt.max_distance = 1;
    lt.term = irs::ViewCast<irs::byte_type>(std::string_view("biscuit"));
    auto& ct = phrase.push_back<irs::ByTermsOptions>();
    ct.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("meringue")));
    ct.terms.emplace(
      irs::ViewCast<irs::byte_type>(std::string_view("marshmallows")));

    std::multimap<irs::score_t, std::string, std::greater<>> sorted;

    std::vector<std::string> expected{
      "SPWLC0",  // cookies cake pie biscuit meringue cookies cake pie biscuit
                 // marshmallows paste bread
      "SPWLC1",  // cookies cake pie biskuit marshmallows cookies pie meringue
      "SPWLC2",  // cookies cake pie biscwit meringue pie biscuit paste
      "SPWLC3"   // cookies cake pie biscuet marshmallows cake meringue
    };

    auto prepared_filter = PrepareOptimized(filter, {
                                                      .index = *index,
                                                      .memory = counter,
                                                      .scorer = &scorer,
                                                    });

    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    const auto* column = segment.Column(kName);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      irs::score_t score_value{};
      score.Score(&score_value, 1);

      irs::BytesViewInput in;
      in.reset(values.Get(docs->value()));
      sorted.emplace(score_value, irs::ReadString<std::string>(in));
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(TfidfTestCase, test_query) {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          auto field = std::make_shared<StringField>(name, data.str);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), true, false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          auto field = std::make_shared<StringField>(name, value);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), false, true);
        }
      });
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreSeq());
  }

  auto scorer = irs::TFIDF{false, true};

  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  auto& segment = *(reader.begin());
  const auto* column = segment.Column(kSeq);
  ASSERT_NE(nullptr, column);

  MaxMemoryCounter counter;
  irs::ColumnArgsFetcher fetcher;

  // by_term
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByTerm filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("7"));

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{0, 1, 5, 7};

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });

    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      in.reset(values.Get(docs->value()));

      irs::score_t score_value{};
      score.Score(&score_value, 1);

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by term multi-segment, same term (same score for all docs)
  {
    irs::tests::BlobPointReader values{segment, *column};

    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          auto field = std::make_shared<StringField>(name, data.str);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), true, false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          auto field = std::make_shared<StringField>(name, value);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), false, true);
        }
      });
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto store_seq = StoreSeq();
    const Document* doc;

    // add first segment (even 'seq')
    {
      gen.reset();
      while ((doc = gen.next())) {
        auto ctx = writer->GetBatch();
        {
          auto d = ctx.Insert();
          ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
          store_seq(d, *doc);
        }
        ctx.Commit();
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    // add second segment (odd 'seq')
    {
      gen.reset();
      gen.next();  // skip 1 doc
      while ((doc = gen.next())) {
        auto ctx = writer->GetBatch();
        {
          auto d = ctx.Insert();
          ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
          store_seq(d, *doc);
        }
        ctx.Commit();
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    irs::ByTerm filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{
      0, 2,  // segment 0
      5      // segment 1
    };

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });

    for (auto& segment : reader) {
      const auto* column = segment.Column(kSeq);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      fetcher.Clear();
      auto docs = prepared_filter->execute({
        .segment = segment,
        .scorer = &scorer,
      });
      auto score = docs->PrepareScore({
        .scorer = &scorer,
        .segment = &segment,
        .fetcher = &fetcher,
      });

      for (irs::score_t score_value{}; docs->next();) {
        fetcher.Fetch(docs->value());
        docs->FetchScoreArgs(0);
        in.reset(values.Get(docs->value()));

        auto str_seq = irs::ReadString<std::string>(in);
        auto seq = strtoull(str_seq.c_str(), nullptr, 10);
        score.Score(&score_value, 1);
        sorted.emplace(score_value, seq);
      }
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_term disjunction multi-segment, different terms (same score for all
  // docs)
  {
    irs::tests::BlobPointReader values{segment, *column};

    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          auto field = std::make_shared<StringField>(name, data.str);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), true, false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          auto field = std::make_shared<StringField>(name, value);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), false, true);
        }
      });
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto store_seq = StoreSeq();
    const Document* doc;

    // add first segment (even 'seq')
    {
      gen.reset();
      while ((doc = gen.next())) {
        auto ctx = writer->GetBatch();
        {
          auto d = ctx.Insert();
          ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
          store_seq(d, *doc);
        }
        ctx.Commit();
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    // add second segment (odd 'seq')
    {
      gen.reset();
      gen.next();  // skip 1 doc
      while ((doc = gen.next())) {
        auto ctx = writer->GetBatch();
        {
          auto d = ctx.Insert();
          ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
          store_seq(d, *doc);
        }
        ctx.Commit();
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    irs::Or filter;
    {
      // doc 0, 2, 5
      auto& sub = filter.add<irs::ByTerm>();
      *sub.mutable_field_id() = kField;
      sub.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("6"));
    }
    {
      // doc 3, 7
      auto& sub = filter.add<irs::ByTerm>();
      *sub.mutable_field_id() = kField;
      sub.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("8"));
    }

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{
      3, 7,    // same value in 2 documents
      0, 2, 5  // same value in 3 documents
    };

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });

    for (auto& segment : reader) {
      const auto* column = segment.Column(kSeq);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      fetcher.Clear();
      auto docs = prepared_filter->execute({
        .segment = segment,
        .scorer = &scorer,

      });
      auto score = docs->PrepareScore({
        .scorer = &scorer,
        .segment = &segment,
        .fetcher = &fetcher,
      });

      while (docs->next()) {
        fetcher.Fetch(docs->value());
        docs->FetchScoreArgs(0);
        in.reset(values.Get(docs->value()));

        irs::score_t score_value{};
        score.Score(&score_value, 1);

        auto str_seq = irs::ReadString<std::string>(in);
        auto seq = strtoull(str_seq.c_str(), nullptr, 10);
        sorted.emplace(score_value, seq);
      }
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_prefix empty multi-segment, different terms (same score for all docs)
  {
    irs::tests::BlobPointReader values{segment, *column};

    tests::JsonDocGenerator gen(
      resource("simple_sequential.json"),
      [](tests::Document& doc, const std::string& name,
         const JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          auto field = std::make_shared<StringField>(name, data.str);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), true, false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          auto field = std::make_shared<StringField>(name, value);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), false, true);
        }
      });
    auto writer =
      open_writer(irs::kOmCreate, irs::tests::DefaultWriterOptions());
    auto store_seq = StoreSeq();
    const Document* doc;

    // add first segment (even 'seq')
    {
      gen.reset();
      while ((doc = gen.next())) {
        auto ctx = writer->GetBatch();
        {
          auto d = ctx.Insert();
          ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
          store_seq(d, *doc);
        }
        ctx.Commit();
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    // add second segment (odd 'seq')
    {
      gen.reset();
      gen.next();  // skip 1 doc
      while ((doc = gen.next())) {
        auto ctx = writer->GetBatch();
        {
          auto d = ctx.Insert();
          ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
          store_seq(d, *doc);
        }
        ctx.Commit();
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    irs::ByPrefix filter;
    *filter.mutable_field_id() = kPrefix;
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view(""));

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{
      0,  8,  20, 28,  // segment 0
      3,  15, 23, 25,  // segment 1
      30, 31,  // same value in segment 0 and segment 1 (smaller idf() ->
               // smaller tfidf() + reverse)
    };

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });

    for (auto& segment : reader) {
      const auto* column = segment.Column(kSeq);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
      fetcher.Clear();
      auto docs = prepared_filter->execute({
        .segment = segment,
        .scorer = &scorer,

      });
      auto score = docs->PrepareScore({
        .scorer = &scorer,
        .segment = &segment,
        .fetcher = &fetcher,
      });

      while (docs->next()) {
        fetcher.Fetch(docs->value());
        docs->FetchScoreArgs(0);
        in.reset(values.Get(docs->value()));

        irs::score_t score_value{};
        score.Score(&score_value, 1);

        auto str_seq = irs::ReadString<std::string>(in);
        auto seq = strtoull(str_seq.c_str(), nullptr, 10);
        sorted.emplace(score_value, seq);
      }
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_range single
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{0, 1, 5, 7};

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    for (irs::score_t score_value{}; docs->next();) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      score.Score(&score_value, 1);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_range single + scored_terms_limit(0)
  // by_range single + scored_terms_limit(1)
  for (size_t limit = 0; limit != 2; ++limit) {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("9"));
    filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    filter.mutable_options()->scored_terms_limit = limit;

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{3, 7};

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    for (irs::score_t score_value{}; docs->next();) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      score.Score(&score_value, 1);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_range multiple
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{7, 0, 1, 3, 5};

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    for (irs::score_t score_value{}; docs->next();) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      score.Score(&score_value, 1);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_range multiple (3 values)
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field_id() = kField;
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{0, 7, 5, 1, 3, 2};

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    for (irs::score_t score_value{}; docs->next();) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      score.Score(&score_value, 1);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(expected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_phrase
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByPhrase filter;
    *filter.mutable_field_id() = kField;
    auto& phrase = *filter.mutable_options();
    phrase.push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("7"));

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<std::pair<float_t, uint64_t>> expected = {
      {-1, 0},
      {-1, 1},
      {-1, 5},
      {-1, 7},
    };

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    for (irs::score_t score_value{}; docs->next();) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      auto seq = strtoull(str_seq.c_str(), nullptr, 10);
      score.Score(&score_value, 1);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      auto& expected_entry = expected[i++];
      ASSERT_EQ(expected_entry.second, entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // all
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::All filter;
    filter.boost(1.5f);

    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      ASSERT_EQ(doc, docs->value());

      irs::score_t score_value{};
      score.Score(&score_value, 1);
      ASSERT_FALSE(values.IsNull(docs->value()));
      ++doc;
      ASSERT_EQ(1.5f, score_value);
    }
    ASSERT_EQ(irs::doc_limits::eof(), docs->value());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // all
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::All filter;
    filter.boost(0.f);

    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      ASSERT_EQ(doc, docs->value());

      irs::score_t score_value{};
      score.Score(&score_value, 1);
      ASSERT_FALSE(values.IsNull(docs->value()));
      ++doc;
      ASSERT_EQ(0.f, score_value);
    }
    ASSERT_EQ(irs::doc_limits::eof(), docs->value());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // column existence
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByColumnExistence filter;
    *filter.mutable_id() = kSeq;

    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });
    ASSERT_TRUE(score.IsDefault());

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      ASSERT_EQ(doc, docs->value());

      irs::score_t score_value{};
      score.Score(&score_value, 1);
      ASSERT_FALSE(values.IsNull(docs->value()));
      ++doc;
      ASSERT_EQ(0.f, score_value);
    }
    ASSERT_EQ(irs::doc_limits::eof(), docs->value());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // column existence
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByColumnExistence filter;
    *filter.mutable_id() = kSeq;
    filter.boost(0.f);

    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });
    ASSERT_TRUE(score.IsDefault());

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      ASSERT_EQ(doc, docs->value());

      irs::score_t score_value{};
      score.Score(&score_value, 1);
      ASSERT_FALSE(values.IsNull(docs->value()));
      ++doc;
      ASSERT_EQ(0.f, score_value);
    }
    ASSERT_EQ(irs::doc_limits::eof(), docs->value());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(TfidfTestCase, test_make) {
  // default values
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{});
    ASSERT_NE(nullptr, scorer);
    auto& scr = dynamic_cast<irs::TFIDF&>(*scorer);
    ASSERT_FALSE(scr.normalize());
    ASSERT_EQ(irs::TFIDF::BOOST_AS_SCORE(), scr.use_boost_as_score());
  }

  // with_norms=true
  {
    auto scorer = irs::TFIDF::Make(irs::TFIDF::Options{.with_norms = true});
    ASSERT_NE(nullptr, scorer);
    auto& scr = dynamic_cast<irs::TFIDF&>(*scorer);
    ASSERT_EQ(true, scr.normalize());
    ASSERT_EQ(irs::TFIDF::BOOST_AS_SCORE(), scr.use_boost_as_score());
  }
}

TEST_P(TfidfTestCase, test_order) {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          auto field = std::make_shared<StringField>(name, data.str);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), true, false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          auto field = std::make_shared<StringField>(name, value);
          field->id = tests::FieldIdFor(name);
          doc.insert(std::move(field), false, true);
        }
      });
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreSeq());
  }

  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  auto& segment = *(reader.begin());

  irs::ByTerm query;
  *query.mutable_field_id() = kField;

  auto scorer = irs::TFIDF{false, true};

  uint64_t seq = 0;
  const auto* column = segment.Column(kSeq);
  ASSERT_NE(nullptr, column);

  MaxMemoryCounter counter;
  irs::ColumnArgsFetcher fetcher;

  {
    irs::tests::BlobPointReader values{segment, *column};

    query.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("7"));

    std::multimap<irs::score_t, uint64_t, std::greater<>> sorted;
    std::vector<uint64_t> expected{0, 1, 5, 7};

    irs::BytesViewInput in;
    auto prepared = query.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });
    fetcher.Clear();
    auto docs = prepared->execute({
      .segment = segment,
      .scorer = &scorer,
    });
    auto score = docs->PrepareScore({
      .scorer = &scorer,
      .segment = &segment,
      .fetcher = &fetcher,
    });

    for (irs::score_t score_value{}; docs->next();) {
      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
      in.reset(values.Get(docs->value()));

      auto str_seq = irs::ReadString<std::string>(in);
      seq = strtoull(str_seq.c_str(), nullptr, 10);

      score.Score(&score_value, 1);
      sorted.emplace(score_value, seq);
    }

    ASSERT_EQ(expected.size(), sorted.size());
    const bool eq =
      std::equal(sorted.begin(), sorted.end(), expected.begin(),
                 [](const auto& lhs, auto rhs) { return lhs.second == rhs; });
    ASSERT_TRUE(eq);
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

TEST_P(TfidfTestCase, test_query_norms) { TestQueryNorms(); }

INSTANTIATE_TEST_SUITE_P(tfidf_test, TfidfTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         TfidfTestCase::to_string);

}  // namespace
