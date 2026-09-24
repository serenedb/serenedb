////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/html_strip_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "token_sink_utils.hpp"

namespace {

struct Tok {
  std::string term;
  uint32_t start;
  uint32_t end;

  bool operator==(const Tok&) const = default;
};

using Toks = std::vector<Tok>;
using Terms = std::vector<std::string>;

Toks Strip(std::string_view value) {
  auto stream = irs::analysis::HtmlStripTokenizer::Make({});
  EXPECT_NE(nullptr, stream);
  const auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  Toks out;
  uint32_t pos = 0;
  for (const auto& t : *tokens) {
    EXPECT_EQ(++pos, t.pos);
    EXPECT_EQ(value.substr(t.offs_start, t.offs_end - t.offs_start), t.term);
    out.push_back({t.term, t.offs_start, t.offs_end});
  }
  return out;
}

Terms StripTerms(std::string_view value) {
  Terms out;
  for (auto& t : Strip(value)) {
    out.push_back(std::move(t.term));
  }
  return out;
}

Toks Decode(std::string_view value,
            irs::analysis::HtmlStripTokenizer::Options opts = {}) {
  auto stream = irs::analysis::HtmlStripTokenizer::Make(opts);
  EXPECT_NE(nullptr, stream);
  const auto tokens = tests::Analyze(*stream, value);
  EXPECT_TRUE(tokens.has_value());
  Toks out;
  uint32_t pos = 0;
  for (const auto& t : *tokens) {
    EXPECT_EQ(++pos, t.pos);
    EXPECT_LE(t.offs_end, value.size());
    out.push_back({t.term, t.offs_start, t.offs_end});
  }
  return out;
}

Toks Join(std::string_view value) {
  return Decode(value, {.join_inline_tags = true});
}

}  // namespace

TEST(html_strip_tokenizer_test, consts) {
  static_assert("strip_html" ==
                irs::Type<irs::analysis::HtmlStripTokenizer>::name());
}

TEST(html_strip_tokenizer_test, traits) {
  auto stream = irs::analysis::HtmlStripTokenizer::Make({});
  ASSERT_NE(nullptr, stream);
  ASSERT_EQ(irs::Type<irs::analysis::HtmlStripTokenizer>::id(), stream->type());
  ASSERT_TRUE(stream->Traits().offsets);
  ASSERT_FALSE(stream->Traits().unique);
}

TEST(html_strip_tokenizer_test, text_runs_keep_offsets) {
  EXPECT_EQ((Toks{{"Hello", 3, 8}, {"world", 12, 17}}),
            Strip("<p>Hello <b>world</b></p>"));
  EXPECT_EQ((Toks{{"Hel", 0, 3}, {"lo", 6, 8}}), Strip("Hel<b>lo</b>"));
  EXPECT_EQ((Toks{{"plain text", 1, 11}}), Strip(" plain text\t\n"));
  EXPECT_EQ((Toks{{"\xE6\x97\xA5\xE6\x9C\xAC", 3, 9}}),
            Strip("<p>\xE6\x97\xA5\xE6\x9C\xAC</p>"));
  EXPECT_TRUE(Strip("").empty());
  EXPECT_TRUE(Strip("<p> \n </p><br/>").empty());
}

TEST(html_strip_tokenizer_test, literal_angle_brackets) {
  EXPECT_EQ(Terms{"a < b and c > d"}, StripTerms("a < b and c > d"));
  EXPECT_EQ(Terms{"1<2"}, StripTerms("1<2"));
  EXPECT_EQ(Terms{"x<"}, StripTerms("x<"));
  EXPECT_EQ(Terms{"<"}, StripTerms("<"));
  EXPECT_EQ((Terms{"a", "b"}), StripTerms("a<br>b"));
  EXPECT_EQ((Terms{"a", "b"}), StripTerms("a</ >b"));
}

TEST(html_strip_tokenizer_test, comments) {
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 11, 12}}), Strip("a<!-- x -->b"));
  EXPECT_EQ(Terms{"x"}, StripTerms("<!-->x"));
  EXPECT_EQ(Terms{"x"}, StripTerms("<!--->x"));
  EXPECT_EQ((Terms{"a", "b"}), StripTerms("a<!-- <p>not a tag</p> -->b"));
  EXPECT_EQ(Terms{"a"}, StripTerms("a<!-- never closed <p>b"));
}

TEST(html_strip_tokenizer_test, cdata_content_is_text) {
  EXPECT_EQ((Toks{{"x", 0, 1}, {"a<b", 10, 13}, {"y", 16, 17}}),
            Strip("x<![CDATA[a<b]]>y"));
  EXPECT_EQ(Terms{"raw <data>"}, StripTerms("<![CDATA[raw <data>]]>"));
  EXPECT_EQ(Terms{"open"}, StripTerms("<![CDATA[open"));
}

TEST(html_strip_tokenizer_test, script_and_style_drop_content) {
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 31, 32}}),
            Strip("a<script>if (x < y) {}</script>b"));
  EXPECT_EQ(Terms{"Text"},
            StripTerms("<style>p {}</style><p>Text</p><script>x()</script>"));
  EXPECT_EQ(Terms{"z"},
            StripTerms("<SCRIPT type=\"x\">var a = '</p>';</ScRiPt >z"));
  EXPECT_EQ(Terms{"y"}, StripTerms("<script><![CDATA[</script>]]></script>y"));
  EXPECT_EQ((Terms{"x", "y"}), StripTerms("<scripts>x</scripts>y"));
  EXPECT_EQ(Terms{"x"}, StripTerms("<script/>x"));
  EXPECT_EQ((Terms{"inner", "after"}),
            StripTerms("<script:x>inner</script:x>after"));
  EXPECT_TRUE(StripTerms("<script>never closed</p>").empty());
}

TEST(html_strip_tokenizer_test, tags_and_malformed_markup) {
  EXPECT_EQ(Terms{"a"}, StripTerms("a<b c=\"d"));
  EXPECT_EQ(Terms{"x"}, StripTerms("<!DOCTYPE html><?xml version=\"1.0\"?>x"));
}

TEST(html_strip_tokenizer_test, quoted_attribute_values) {
  EXPECT_EQ((Toks{{"link", 15, 19}}), Strip("<a title=\"1>2\">link</a>"));
  EXPECT_EQ((Toks{{"x", 17, 18}}), Strip("<img alt='a > b'>x"));
  EXPECT_EQ((Toks{{"t", 16, 17}}), Strip("<a href = \"x>y\">t"));
  EXPECT_EQ((Toks{{"y", 10, 11}}), Strip("<a href=x>y</a>"));
  EXPECT_EQ((Toks{{"z", 7, 8}}), Strip("<p x\"y>z"));
  EXPECT_EQ((Toks{{"b", 19, 20}}), Strip("<a x=\"1\" y='>' z=2>b"));
  EXPECT_EQ(Terms{"z"}, StripTerms("<script data-x=\"a>b\">var y;</script>z"));
  EXPECT_TRUE(StripTerms("<a title=\"never closed>text").empty());
}

TEST(html_strip_tokenizer_test, character_references_decode) {
  EXPECT_EQ((Toks{{"Fish", 0, 4}, {"&", 5, 10}, {"chips", 11, 16}}),
            Decode("Fish &amp; chips"));
  EXPECT_EQ((Toks{{"caf\xC3\xA9", 0, 11}, {"au lait", 12, 19}}),
            Decode("caf&eacute; au lait"));
  EXPECT_EQ((Toks{{"x", 0, 1}, {"tom&jerry", 2, 15}, {"y", 16, 17}}),
            Decode("x tom&amp;jerry y"));
  EXPECT_EQ((Toks{{"<tag>", 0, 11}}), Decode("&lt;tag&gt;"));
  EXPECT_EQ((Toks{{"ABC", 0, 17}}), Decode("&#65;&#x42;&#X43;"));
  EXPECT_EQ((Toks{{"A", 0, 15}}), Decode("&#000000000065;"));
  EXPECT_EQ((Toks{{"\xC3\x89\xC3\xA9", 0, 16}}), Decode("&Eacute;&eacute;"));
  EXPECT_EQ((Toks{{"\xCF\x91", 0, 10}}), Decode("&thetasym;"));
  EXPECT_EQ((Toks{{"\xE2\x80\x93", 0, 6}}), Decode("&#150;"));
  EXPECT_EQ((Toks{{"\xE2\x82\xAC", 0, 6}}), Decode("&#x80;"));
  EXPECT_EQ((Toks{{"\xF0\x9F\x98\x80", 0, 9}}), Decode("&#x1F600;"));
  EXPECT_EQ((Toks{{"\xEF\xBF\xBD", 0, 4}}), Decode("&#0;"));
  EXPECT_EQ((Toks{{"\xEF\xBF\xBD", 0, 8}}), Decode("&#xD800;"));
  EXPECT_EQ((Toks{{"\xEF\xBF\xBD", 0, 10}}), Decode("&#x110000;"));
  EXPECT_EQ((Toks{{"\xEF\xBF\xBD", 0, 14}}), Decode("&#99999999999;"));
  EXPECT_EQ((Toks{{"&", 0, 5}}), Decode("&AMP;"));
  EXPECT_EQ((Toks{{"\xE2\x9C\x93", 0, 7}}), Decode("&check;"));
  EXPECT_EQ((Toks{{"\xE2\x89\x82\xCC\xB8", 0, 15}}), Decode("&NotEqualTilde;"));
}

TEST(html_strip_tokenizer_test, references_without_semicolon) {
  EXPECT_EQ((Toks{{"&", 0, 4}, {"chips", 5, 10}}), Decode("&amp chips"));
  EXPECT_EQ((Toks{{"\xC2\xACit;", 0, 7}}), Decode("&notit;"));
  EXPECT_EQ((Toks{{"Ax", 0, 5}}), Decode("&#65x"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 6, 7}}), Decode("a&nbspb"));
}

TEST(html_strip_tokenizer_test, space_references_separate) {
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 7, 8}}), Decode("a&nbsp;b"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 11, 12}}), Decode("a&#32;&#x9;b"));
  EXPECT_EQ((Toks{{"x", 0, 1}, {"y", 15, 16}}), Decode("x&ensp;&thinsp;y"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 13, 14}}), Decode("a&ThickSpace;b"));
  EXPECT_EQ((Toks{{"Text", 12, 16}}), Decode("&nbsp;&nbsp;Text"));
  EXPECT_TRUE(Decode("&nbsp;&#x3000;&NewLine;").empty());
}

TEST(html_strip_tokenizer_test, invalid_references_stay_text) {
  EXPECT_EQ((Toks{{"&unknown; & &#; &#x;", 0, 20}}),
            Strip("&unknown; & &#; &#x;"));
  EXPECT_EQ((Toks{{"&thetasyms;", 0, 11}}), Strip("&thetasyms;"));
  EXPECT_EQ((Toks{{"&amp;", 9, 14}}), Strip("<![CDATA[&amp;]]>"));
}

TEST(html_strip_tokenizer_test, join_inline_tags) {
  EXPECT_EQ((Toks{{"Hel", 0, 3}, {"lo", 6, 8}}), Decode("Hel<b>lo</b>"));
  EXPECT_EQ((Toks{{"Hello", 0, 8}, {"world", 13, 18}}),
            Join("Hel<b>lo</b> world"));
  EXPECT_EQ((Toks{{"Some", 3, 7}, {"body", 12, 16}, {"text", 22, 26}}),
            Join("<p>Some <em>body</em> text</p>"));
  EXPECT_EQ((Toks{{"abc", 0, 17}}), Join("a<b><i>b</i></b>c"));
  EXPECT_EQ((Toks{{"supercali", 0, 14}}), Join("super<wbr>cali"));
  EXPECT_EQ((Toks{{"xy", 0, 5}}), Join("x<B>y</B>"));
  EXPECT_EQ((Toks{{"caf\xC3\xA9", 0, 14}}), Join("caf<b>&eacute;</b>"));
  EXPECT_EQ((Toks{{"ab", 0, 5}, {"cd", 11, 17}}), Join("a<b>b&nbsp;c</b>d"));
  EXPECT_EQ((Toks{{"Bold", 3, 7}, {"para", 14, 18}}),
            Join("<b>Bold</b><p>para"));
  EXPECT_EQ((Toks{{"ab", 0, 20}}), Join("a<span class=\"x>y\">b"));
  EXPECT_EQ((Toks{{"a", 0, 1}}), Join("a<b c=\"d"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 4, 5}}), Join("a<p>b"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 5, 6}}), Join("a<br>b"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 7, 8}}), Join("a<bdox>b"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 9, 10}}), Join("a<span-x>b"));
  EXPECT_EQ((Toks{{"a", 0, 1}, {"b", 9, 10}}), Join("a<!--x-->b"));
}

TEST(html_strip_tokenizer_test, pipeline_offsets_point_into_markup) {
  std::vector<irs::analysis::Tokenizer::ptr> stages;
  stages.emplace_back(irs::analysis::HtmlStripTokenizer::Make({}));
  stages.emplace_back(irs::analysis::DelimitedTokenizer::Make(
    irs::analysis::DelimitedTokenizer::Options{.delimiter = " "}));
  irs::analysis::PipelineTokenizer pipe(std::move(stages));
  ASSERT_TRUE(pipe.Traits().offsets);
  const std::string data = "<p>Hello <b>big world</b></p>";
  const auto tokens = tests::Analyze(pipe, data);
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{
    {"Hello", 1, 3, 8}, {"big", 2, 12, 15}, {"world", 3, 16, 21}};
  ASSERT_EQ(expected, *tokens);
}

TEST(html_strip_tokenizer_test, pipeline_offsets_cover_decoded_words) {
  std::vector<irs::analysis::Tokenizer::ptr> stages;
  stages.emplace_back(
    irs::analysis::HtmlStripTokenizer::Make({.join_inline_tags = true}));
  stages.emplace_back(irs::analysis::DelimitedTokenizer::Make(
    irs::analysis::DelimitedTokenizer::Options{.delimiter = " "}));
  irs::analysis::PipelineTokenizer pipe(std::move(stages));
  const std::string data = "<p>Fish &amp; chips, caf&eacute; Hel<b>lo</b></p>";
  const auto tokens = tests::Analyze(pipe, data);
  ASSERT_TRUE(tokens.has_value());
  const std::vector<tests::AnalyzerToken> expected{{"Fish", 1, 3, 7},
                                                   {"&", 2, 8, 13},
                                                   {"chips,", 3, 14, 20},
                                                   {"caf\xC3\xA9", 4, 21, 32},
                                                   {"Hello", 5, 33, 41}};
  ASSERT_EQ(expected, *tokens);
}
