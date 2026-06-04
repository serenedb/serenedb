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
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/column_existence_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

inline constexpr irs::field_id kSeq = 1;
inline constexpr irs::field_id kName = 2;

auto StoreSeq() {
  return [](irs::IndexWriter::Document& doc, const tests::Document& src) {
    const auto* seq =
      dynamic_cast<const tests::StringField*>(src.stored.get("seq"));
    if (seq) {
      irs::tests::StoreFieldAt(*doc.Columnstore(), kSeq, doc.DocId(), *seq);
    }
  };
}

auto StoreName() {
  return [](irs::IndexWriter::Document& doc, const tests::Document& src) {
    const auto* name =
      dynamic_cast<const tests::StringField*>(src.stored.get("name"));
    if (name) {
      irs::tests::StoreFieldAt(*doc.Columnstore(), kName, doc.DocId(), *name);
    }
  };
}

/////////////////
// Freq | Term //
/////////////////
// 4    | 0    //
// 3    | 1    //
// 10   | 2    //
// 7    | 3    //
// 5    | 4    //
// 4    | 5    //
// 3    | 6    //
// 7    | 7    //
// 2    | 8    //
// 7    | 9    //
/////////////////

//////////////////////////////////////////////////
// Stats                                        //
//////////////////////////////////////////////////
// TotalFreq = 52                               //
// DocsCount = 8                                //
// AverageDocLength (TotalFreq/DocsCount) = 6.5 //
//////////////////////////////////////////////////

class Bm25TestCase : public IndexTestBase {
 protected:
  void TestQueryNorms();
};

void Bm25TestCase::TestQueryNorms() {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          doc.insert(std::make_shared<StringField>(name, data.str,
                                                   irs::IndexFeatures::Norm),
                     true, false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          doc.insert(std::make_shared<StringField>(name, value,
                                                   irs::IndexFeatures::Norm),
                     false, true);
        }
      });

    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreSeq());
  }

  auto scorer = irs::BM25{irs::BM25::K(), irs::BM25::B(), true};

  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  auto& segment = *(reader.begin());
  const auto* column = segment.Column(kSeq);
  ASSERT_NE(nullptr, column);

  MaxMemoryCounter counter;
  irs::ColumnArgsFetcher fetcher;

  // by_range multiple
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field() = "field";
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{7, 3, 0, 1, 5};

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

    ASSERT_EQ(kExpected.size(), sorted.size());
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
    *filter.mutable_field() = "field";
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    const auto expected = std::array{0, 7, 5, 3, 2, 1};

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

TEST_P(Bm25TestCase, consts) {
  static_assert("bm25" == irs::Type<irs::BM25>::name());
}

TEST_P(Bm25TestCase, test_load) {
  auto scorer = irs::BM25::Make(irs::BM25::Options{});
  ASSERT_NE(nullptr, scorer);
}

TEST_P(Bm25TestCase, make_from_array) {
  // default args
  {
    auto scorer = irs::BM25::Make(irs::BM25::Options{});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::Type<irs::BM25>::id(), scorer->type());
    auto& bm25 = dynamic_cast<irs::BM25&>(*scorer);
    ASSERT_EQ(irs::BM25::K(), bm25.k());
    ASSERT_EQ(irs::BM25::B(), bm25.b());
    ASSERT_EQ(irs::BM25::BOOST_AS_SCORE(), bm25.use_boost_as_score());
  }

  // custom `k` and `b`
  {
    auto scorer = irs::BM25::Make(irs::BM25::Options{.k1 = 1.5f, .b = 1.7f});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::Type<irs::BM25>::id(), scorer->type());
    auto& bm25 = dynamic_cast<irs::BM25&>(*scorer);
    ASSERT_EQ(1.5f, bm25.k());
    ASSERT_EQ(1.7f, bm25.b());
    ASSERT_EQ(irs::BM25::BOOST_AS_SCORE(), bm25.use_boost_as_score());
  }
}

TEST_P(Bm25TestCase, test_normalize_features) {
  // default norms
  {
    auto scorer = irs::BM25::Make(irs::BM25::Options{});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::IndexFeatures::Freq | irs::IndexFeatures::Norm,
              scorer->GetIndexFeatures());
  }

  // without norms (bm15) -- b = 0 selects the BM15 codepath.
  {
    auto scorer = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});
    ASSERT_NE(nullptr, scorer);
    ASSERT_EQ(irs::IndexFeatures::Freq, scorer->GetIndexFeatures());
  }
}

TEST_P(Bm25TestCase, test_phrase) {
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
        doc.indexed.push_back(std::make_shared<TextField>(
          std::string(name.c_str()) + "_anl", data.str));

        // not analyzed field
        doc.insert(std::make_shared<StringField>(name, data.str));
      }
    };

  // add segment
  {
    tests::JsonDocGenerator gen(resource("phrase_sequential.json"),
                                analyzed_json_field_factory);
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreName());
  }

  auto impl = irs::BM25::Make(irs::BM25::Options{.b = 0.0f});

  // read segment
  auto index = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, index->size());
  auto& segment = *(index.begin());

  MaxMemoryCounter counter;
  irs::ColumnArgsFetcher fetcher;

  // "jumps high" with order
  {
    irs::ByPhrase filter;
    *filter.mutable_field() = "phrase_anl";
    filter.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("jumps"));
    filter.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("high"));

    std::multimap<irs::score_t, std::string, std::greater<>> sorted;

    constexpr std::array<std::string_view, 4> kExpected{
      "O",   // jumps high jumps high hotdog
      "P",   // jumps high jumps left jumps right jumps down jumps back
      "Q",   // jumps high jumps left jumps right jumps down walks back
      "R"};  // jumps high jumps left jumps right walks down walks back

    auto prepared_filter = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorer = impl.get(),
    });

    fetcher.Clear();

    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = impl.get(),
    });
    auto score = docs->PrepareScore({
      .scorer = impl.get(),
      .segment = &segment,
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

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // "cookies ca* p_e bisKuit meringue|marshmallows" with order
  {
    irs::ByPhrase filter;
    *filter.mutable_field() = "phrase_anl";
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

    constexpr std::array<std::string_view, 4> kExpected{
      "SPWLC0",   // cookies cake pie biscuit meringue cookies cake pie biscuit
                  // marshmallows paste bread
      "SPWLC1",   // cookies cake pie biskuit marshmallows cookies pie meringue
      "SPWLC2",   // cookies cake pie biscwit meringue pie biscuit paste
      "SPWLC3"};  // cookies cake pie biscuet marshmallows cake meringue

    auto prepared_filter = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorer = impl.get(),
    });

    fetcher.Clear();

    auto docs = prepared_filter->execute({
      .segment = segment,
      .scorer = impl.get(),
    });
    auto score = docs->PrepareScore({
      .scorer = impl.get(),
      .segment = &segment,
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

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(Bm25TestCase, test_query) {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          doc.insert(std::make_shared<StringField>(name, data.str), true,
                     false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          doc.insert(std::make_shared<StringField>(name, value), false, true);
        }
      });
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreSeq());
  }

  irs::BM25 scorer{irs::BM25::K(), irs::BM25::B(), true};

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
    *filter.mutable_field() = "field";
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("7"));

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{0, 1, 5, 7};

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

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
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
          doc.insert(std::make_shared<StringField>(name, data.str), true,
                     false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          doc.insert(std::make_shared<StringField>(name, value), false, true);
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
        auto d = ctx.Insert();
        ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
        store_seq(d, *doc);
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
        auto d = ctx.Insert();
        ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
        store_seq(d, *doc);
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    irs::ByTerm filter;
    *filter.mutable_field() = "field";
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{
      0, 2,  // segment 0
      5      // segment 1
    };

    irs::BytesViewInput in;
    auto prepared_filter = filter.prepare({
      .index = reader,
      .memory = counter,
      .scorer = &scorer,
    });

    irs::ColumnArgsFetcher fetcher;
    for (auto& segment : reader) {
      fetcher.Clear();
      const auto* column = segment.Column(kSeq);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
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
    }

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
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
          doc.insert(std::make_shared<StringField>(name, data.str), true,
                     false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          doc.insert(std::make_shared<StringField>(name, value), false, true);
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
        auto d = ctx.Insert();
        ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
        store_seq(d, *doc);
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
        auto d = ctx.Insert();
        ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
        store_seq(d, *doc);
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
      *sub.mutable_field() = "field";
      sub.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("6"));
    }
    {
      // doc 3, 7
      auto& sub = filter.add<irs::ByTerm>();
      *sub.mutable_field() = "field";
      sub.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("8"));
    }

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{
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
      fetcher.Clear();
      const auto* column = segment.Column(kSeq);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
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
    }

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
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
          doc.insert(std::make_shared<StringField>(name, data.str), true,
                     false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          doc.insert(std::make_shared<StringField>(name, value), false, true);
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
        auto d = ctx.Insert();
        ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
        store_seq(d, *doc);
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
        auto d = ctx.Insert();
        ASSERT_TRUE(d.Insert(doc->indexed.begin(), doc->indexed.end()));
        store_seq(d, *doc);
        gen.next();  // skip 1 doc
      }
      writer->RefreshCommit();
      AssertSnapshotEquality(*writer);
    }

    auto reader =
      irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
    irs::ByPrefix filter;
    *filter.mutable_field() = "prefix";
    filter.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view(""));

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{
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

    irs::ColumnArgsFetcher fetcher;
    for (auto& segment : reader) {
      fetcher.Clear();
      const auto* column = segment.Column(kSeq);
      ASSERT_NE(nullptr, column);
      irs::tests::BlobPointReader values{segment, *column};
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
    }

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_range single
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field() = "field";
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{0, 1, 5, 7};

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

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
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
    *filter.mutable_field() = "field";
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("9"));
    filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    filter.mutable_options()->scored_terms_limit = limit;

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{3, 7};

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

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by_range multiple
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByRange filter;
    *filter.mutable_field() = "field";
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{7, 3, 0, 1, 5};

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

    ASSERT_EQ(kExpected.size(), sorted.size());
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
    *filter.mutable_field() = "field";
    filter.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("6"));
    filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    filter.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("8"));
    filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    constexpr std::array kExpected{7, 0, 5, 3, 2, 1};

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

    ASSERT_EQ(kExpected.size(), sorted.size());
    size_t i = 0;

    for (auto& entry : sorted) {
      ASSERT_EQ(kExpected[i++], entry.second);
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // by phrase
  {
    irs::tests::BlobPointReader values{segment, *column};

    irs::ByPhrase filter;
    *filter.mutable_field() = "field";
    filter.mutable_options()->push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(std::string_view("7"));

    std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
    std::vector<std::pair<float_t, uint32_t>> expected = {
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
    });

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      fetcher.Fetch(docs->value());
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
    });

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      ASSERT_EQ(doc, docs->value());

      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
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
    });
    ASSERT_TRUE(score.IsDefault());

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      ASSERT_EQ(doc, docs->value());

      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
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
    });
    ASSERT_TRUE(score.IsDefault());

    irs::doc_id_t doc = irs::doc_limits::min();
    while (docs->next()) {
      ASSERT_EQ(doc, docs->value());

      fetcher.Fetch(docs->value());
      docs->FetchScoreArgs(0);
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

TEST_P(Bm25TestCase, test_make) {
  // default values
  {
    auto scorer = irs::BM25::Make(irs::BM25::Options{});
    ASSERT_NE(nullptr, scorer);
    auto& scr = dynamic_cast<irs::BM25&>(*scorer);
    ASSERT_EQ(0.75f, scr.b());
    ASSERT_EQ(1.2f, scr.k());
    ASSERT_FALSE(scr.IsBM11());
    ASSERT_FALSE(scr.IsBM15());
  }

  // custom values
  {
    auto scorer =
      irs::BM25::Make(irs::BM25::Options{.k1 = 78.9f, .b = 123.456f});
    ASSERT_NE(nullptr, scorer);
    auto& scr = dynamic_cast<irs::BM25&>(*scorer);
    ASSERT_EQ(123.456f, scr.b());
    ASSERT_EQ(78.9f, scr.k());
    ASSERT_FALSE(scr.IsBM11());
    ASSERT_FALSE(scr.IsBM15());
  }

  // bm11 -- b = 1 selects the BM11 codepath.
  {
    auto scorer = irs::BM25::Make(irs::BM25::Options{.k1 = 78.9f, .b = 1.0f});
    ASSERT_NE(nullptr, scorer);
    auto& scr = dynamic_cast<irs::BM25&>(*scorer);
    ASSERT_EQ(1.f, scr.b());
    ASSERT_EQ(78.9f, scr.k());
    ASSERT_TRUE(scr.IsBM11());
    ASSERT_FALSE(scr.IsBM15());
  }

  // bm15 -- b = 0 selects the BM15 codepath.
  {
    auto scorer = irs::BM25::Make(irs::BM25::Options{.k1 = 78.9f, .b = 0.0f});
    ASSERT_NE(nullptr, scorer);
    auto& scr = dynamic_cast<irs::BM25&>(*scorer);
    ASSERT_EQ(0.f, scr.b());
    ASSERT_EQ(78.9f, scr.k());
    ASSERT_FALSE(scr.IsBM11());
    ASSERT_TRUE(scr.IsBM15());
  }
}

TEST_P(Bm25TestCase, test_order) {
  {
    tests::JsonDocGenerator gen(
      resource("simple_sequential_order.json"),
      [](tests::Document& doc, const std::string& name,
         const tests::JsonDocGenerator::JsonValue& data) {
        if (data.is_string()) {  // field
          doc.insert(std::make_shared<StringField>(name, data.str), true,
                     false);
        } else if (data.is_number()) {  // seq
          const auto value = std::to_string(data.as_number<int64_t>());
          doc.insert(std::make_shared<StringField>(name, value), false, true);
        }
      });
    add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                StoreSeq());
  }

  auto reader =
    irs::DirectoryReader(dir(), codec(), irs::tests::DefaultReaderOptions());
  auto& segment = *(reader.begin());

  MaxMemoryCounter counter;
  irs::ColumnArgsFetcher fetcher;

  irs::ByTerm query;
  *query.mutable_field() = "field";

  irs::BM25 sort25;
  irs::BM25 sort15{irs::BM25::K(), 0.f};
  irs::BM25 sort11{irs::BM25::K(), 1.f};
  irs::BM25 sort1{0.f, 0.1337f};
  for (irs::score_t boost : {0.f, 0.5f, irs::kNoBoost}) {
    for (auto& sort : {sort25, sort15, sort11, sort1}) {
      uint64_t seq = 0;
      const auto* column = segment.Column(kSeq);
      ASSERT_NE(nullptr, column);

      {
        irs::tests::BlobPointReader values{segment, *column};

        query.mutable_options()->term =
          irs::ViewCast<irs::byte_type>(std::string_view("7"));

        std::multimap<irs::score_t, uint32_t, std::greater<>> sorted;
        constexpr std::array kExpected{0, 1, 5, 7};

        irs::BytesViewInput in;
        auto prepared = query.prepare({
          .index = reader,
          .memory = counter,
          .scorer = &sort,
          .boost = boost,
        });
        fetcher.Clear();
        auto docs = prepared->execute({
          .segment = segment,
          .scorer = &sort,
        });
        auto score = docs->PrepareScore({
          .scorer = &sort,
          .segment = &segment,
          .fetcher = &fetcher,
        });

        for (; docs->next();) {
          fetcher.Fetch(docs->value());
          irs::score_t score_value{};
          score.Score(&score_value, 1);

          in.reset(values.Get(docs->value()));

          auto str_seq = irs::ReadString<std::string>(in);
          seq = strtoull(str_seq.c_str(), nullptr, 10);
          sorted.emplace(score_value, seq);
        }

        ASSERT_EQ(kExpected.size(), sorted.size());
        const bool eq = std::equal(
          sorted.begin(), sorted.end(), kExpected.begin(),
          [](const auto& lhs, uint64_t rhs) { return lhs.second == rhs; });
        EXPECT_TRUE(eq);
      }
      EXPECT_EQ(counter.current, 0);
      EXPECT_GT(counter.max, 0);
      counter.Reset();
    }
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

TEST_P(Bm25TestCase, test_query_norms) { TestQueryNorms(); }

INSTANTIATE_TEST_SUITE_P(bm25_test, Bm25TestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         Bm25TestCase::to_string);

}  // namespace
