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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>
#include <stdexcept>

#include "gtest/gtest.h"

using WordnetSynonymsTokenizer = irs::analysis::WordnetSynonymsTokenizer;

TEST(wordnet_synonyms_tests, consts) {
  static_assert("wordnet_synonyms" ==
                irs::Type<WordnetSynonymsTokenizer>::name());
}

TEST(wordnet_synonyms_tests, test_masking) {
  {
    std::string data0 = "come";
    WordnetSynonymsTokenizer::SynonymsMap mapping;

    WordnetSynonymsTokenizer stream(std::move(mapping));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    ASSERT_FALSE(stream.reset(data0));
    ASSERT_FALSE(stream.next());
  }

  {
    std::string data0 = "come";
    std::string data1 = "advance";
    WordnetSynonymsTokenizer::SynonymsGroups group{"100000002"};
    WordnetSynonymsTokenizer::SynonymsMap mapping{{"come", {"100000002"}}};

    WordnetSynonymsTokenizer stream(std::move(mapping));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_FALSE(stream.reset(data1));
    ASSERT_FALSE(stream.next());
  }

  {
    std::string data0 = "come";
    std::string data1 = "advance";
    std::string data2 = "approach";
    WordnetSynonymsTokenizer::SynonymsMap mapping{
      {"come", {"100000002"}},
      {"advance", {"100000002"}},
      {"approach", {"100000002"}},
    };

    WordnetSynonymsTokenizer stream(std::move(mapping));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data2));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(8, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }
}

TEST(wordnet_synonyms_tests, test_homonyms) {
  std::string data0 = "word0";
  std::string data1 = "word1";
  WordnetSynonymsTokenizer::SynonymsMap mapping{
    {data0, {"100000002", "100000003"}},
    {data1, {"100000002"}},
  };

  WordnetSynonymsTokenizer stream(std::move(mapping));
  ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);
  auto* inc = irs::get<irs::IncAttr>(stream);

  ASSERT_TRUE(stream.reset(data0));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000003", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());

  ASSERT_TRUE(stream.reset(data1));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());
}

TEST(wordnet_synonyms_tests, test_homonyms_early_reset) {
  std::string data0 = "word0";
  std::string data1 = "word1";
  WordnetSynonymsTokenizer::SynonymsMap mapping{
    {data0, {"100000002", "100000003"}},
    {data1, {"100000002"}},
  };

  WordnetSynonymsTokenizer stream(std::move(mapping));
  ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);
  auto* inc = irs::get<irs::IncAttr>(stream);

  ASSERT_TRUE(stream.reset(data0));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_TRUE(stream.next());

  ASSERT_TRUE(stream.reset(data1));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());
}

TEST(wordnet_synonyms_tests, test_homonyms_double_reset) {
  std::string data0 = "word0";
  std::string data1 = "word1";
  WordnetSynonymsTokenizer::SynonymsMap mapping{
    {data0, {"100000002", "100000003"}},
    {data1, {"100000002"}},
  };

  WordnetSynonymsTokenizer stream(std::move(mapping));
  ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

  auto* offset = irs::get<irs::OffsAttr>(stream);
  auto* term = irs::get<irs::TermAttr>(stream);
  auto* inc = irs::get<irs::IncAttr>(stream);

  ASSERT_TRUE(stream.reset(data0));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_TRUE(stream.next());

  ASSERT_TRUE(stream.reset(data0));
  ASSERT_TRUE(stream.reset(data0));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(0, offset->start);
  ASSERT_EQ(5, offset->end);
  ASSERT_EQ(1, inc->value);
  ASSERT_EQ("100000003", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());
}

TEST(wordnet_synonyms_tests, parsing_one_line) {
  {
    std::string_view data0("s(100000002,1,'come',v,1,0).");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result);
    const auto input = *result;
    WordnetSynonymsTokenizer::SynonymsMap expected{{"come", {"100000002"}}};
    ASSERT_EQ(expected, input);
  }
}

TEST(wordnet_synonyms_tests, parsing_empty) {
  {
    std::string_view data0("");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result);
    const auto input = *result;

    WordnetSynonymsTokenizer::SynonymsMap expected{};
    ASSERT_EQ(expected, input);
  }
}

TEST(wordnet_synonyms_tests, parsing_some_lines) {
  std::string_view data0(
    "s(100000002,1,'come',v,1,0).\ns(100000002,2,'advance',v,1,0).\n\ns("
    "100000002,3,'approach',v,1,0).\ns(100000003,1,'release',v,1,0).");
  auto result = WordnetSynonymsTokenizer::Parse(data0);
  ASSERT_TRUE(result);
  const auto input = *result;

  WordnetSynonymsTokenizer::SynonymsMap expected{
    {"come", {"100000002"}},
    {"advance", {"100000002"}},
    {"approach", {"100000002"}},
    {"release", {"100000003"}},
  };
  ASSERT_EQ(expected, input);
}

TEST(wordnet_synonyms_tests, parsing_short_version) {
  std::string_view data0("s(301380267,1,'aerial',s).");
  auto result = WordnetSynonymsTokenizer::Parse(data0);
  ASSERT_TRUE(result);
  const auto input = *result;

  WordnetSynonymsTokenizer::SynonymsMap expected{
    {"aerial", {"301380267"}},
  };
  ASSERT_EQ(expected, input);
}

TEST(wordnet_synonyms_tests, parsing_homonym_diffrent_synset_order) {
  std::string_view data0(
    "s(100000001,1,'word0',v,1,0).\ns(100000002,2,'word0',v,1,0).\n\ns("
    "100000004,3,'word0',v,1,0).\ns(100000003,1,'word0',v,1,0).");
  auto result = WordnetSynonymsTokenizer::Parse(data0);
  ASSERT_TRUE(result);
  const auto input = *result;
  WordnetSynonymsTokenizer::SynonymsMap expected{
    {"word0", {"100000001", "100000002", "100000003", "100000004"}},
  };
  ASSERT_EQ(expected, input);
}

TEST(wordnet_synonyms_tests, parsing_broken_short_line) {
  for (std::string_view data0 : {std::string("a"), std::string("go")}) {
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }
}

TEST(wordnet_synonyms_tests, parsing_broken_synonym) {
  for (std::string_view data0 : {std::string("s(100000002,1,come,v,1,0)."),
                                 std::string("s(100000002,1,'come,v,1,0)."),
                                 std::string("s(100000002,1,come',v,1,0)."),
                                 std::string("s(100000002,1,'',v,1,0)."),
                                 std::string("s(100000002,1,,v,1,0)."),
                                 std::string("s(100000002,1, ,v,1,0)."),
                                 std::string("s(100000002,1,a,v,1,0).")}) {
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }
}

TEST(wordnet_synonyms_tests, parsing_broken_second_line) {
  std::string_view data0("s(100000002,1,'come',v,1,0).\nasd");
  auto result = WordnetSynonymsTokenizer::Parse(data0);
  ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
  ASSERT_EQ(result.error().errorMessage(), "Failed parse line 2");
}

TEST(wordnet_synonyms_tests, parsing_broken_line_more_param) {
  std::string_view data0("s(100000002,1,'come',v,1,0,2).\n");
  auto result = WordnetSynonymsTokenizer::Parse(data0);
  ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
  ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
}

TEST(wordnet_synonyms_tests, parsing_broken_line_less_param) {
  std::string_view data0("s(100000002,1,'come').\n");
  auto result = WordnetSynonymsTokenizer::Parse(data0);
  ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
  ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
}

TEST(wordnet_synonyms_tests, parsing_broken_order_synsets) {
  std::string_view data0(
    "s(100000002,1,'word1',v,1,0).\ns(100000003,1,'word2',v,1,0).\ns(100000002,"
    "1,'word1',v,1,0).\n");
  auto result = WordnetSynonymsTokenizer::Parse(data0);
  ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
  ASSERT_EQ(result.error().errorMessage(),
            "Duplicate for word1: synset 100000002");
}
