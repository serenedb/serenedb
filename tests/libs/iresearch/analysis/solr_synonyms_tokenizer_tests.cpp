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

#include "gtest/gtest.h"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"

using SolrSynonymsTokenizer = irs::analysis::SolrSynonymsTokenizer;

TEST(solr_synonyms_tests, consts) {
  static_assert("solr_synonyms" == irs::Type<SolrSynonymsTokenizer>::name());
}

TEST(solr_synonyms_tests, test_masking) {
  // test no synonyms
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    SolrSynonymsTokenizer::SynonymsMap mask;
    SolrSynonymsTokenizer stream(std::move(mask));
    ASSERT_EQ(irs::Type<SolrSynonymsTokenizer>::id(), stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test with synonyms
  {
    std::string_view data0("foo");
    std::string_view data1("bar");
    std::string_view data2("xyz");

    const std::vector<std::string_view> synonyms{"foo", "bar"};
    SolrSynonymsTokenizer::SynonymsMap mask = {
      {"foo", &synonyms},
      {"bar", &synonyms},
    };
    SolrSynonymsTokenizer stream(std::move(mask));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("foo", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
    ASSERT_EQ(0, inc->value);

    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("foo", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
    ASSERT_EQ(0, inc->value);

    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data2));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("xyz", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);

    ASSERT_FALSE(stream.next());
  }
}

TEST(solr_synonyms_tests, parsing) {
  {
    std::string_view data0("foo,bar\n");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("foo,bar=>foo");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("foo,bar=>foo,bar\n");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("foo,bar\n\n#some comment\naaa, bbb, cc");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}},
        SolrSynonymsTokenizer::SynonymsLine{{}, {"aaa", "bbb", "cc"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines[0].out}, {"bar", &synonyms_lines[0].out},
        {"aaa", &synonyms_lines[1].out}, {"bbb", &synonyms_lines[1].out},
        {"cc", &synonyms_lines[1].out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0(
      "foo,bar=>foo,bar\n\n#some comment\naaa, bbb, cc => aaa, bbb, cc");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
        SolrSynonymsTokenizer::SynonymsLine{{"aaa", "bbb", "cc"},
                                            {"aaa", "bbb", "cc"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines[0].out}, {"bar", &synonyms_lines[0].out},
        {"aaa", &synonyms_lines[1].out}, {"bbb", &synonyms_lines[1].out},
        {"cc", &synonyms_lines[1].out},
      };

      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SolrSynonymsTokenizer::SynonymsMap expected = {};

      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("aaa, bbb, cc => => aaa, bbb, cc");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(),
              "More than one explicit mapping specified on the line 1");
  }

  {
    std::string_view data0("aaa,");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("aaa=>");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("aaa,=>aaa");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("aaa,bbb=>aaa,");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }
  {
    std::string_view data0("\n#aa\naaa,,bbb=>aaa,bbb");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 3");
  }

  {
    std::string_view data0("foo,bar,foo");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }

  {
    std::string_view data0("foo,bar,foo=>foo,bar");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }

  {
    std::string_view data0("foo,bar,foo=>foo,bar");
    auto result = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }
}
