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

#include "basics/exceptions.h"
#include "gtest/gtest.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"

using SolrSynonymsTokenizer = irs::analysis::SolrSynonymsTokenizer;

namespace {

std::shared_ptr<const SolrSynonymsTokenizer::State> StateFromMap(
  SolrSynonymsTokenizer::SynonymsMap mask) {
  auto state = std::make_shared<SolrSynonymsTokenizer::State>();
  state->synonyms = std::move(mask);
  return state;
}

}  // namespace

TEST(solr_synonyms_tests, consts) {
  static_assert("solr_synonyms" == irs::Type<SolrSynonymsTokenizer>::name());
}

TEST(solr_synonyms_tests, test_masking) {
  // test no synonyms
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    SolrSynonymsTokenizer::SynonymsMap mask;
    SolrSynonymsTokenizer stream(StateFromMap(std::move(mask)));
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
    SolrSynonymsTokenizer stream(StateFromMap(std::move(mask)));

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
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0("foo,bar=>foo");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0("foo,bar=>foo,bar\n");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0("foo,bar\n\n#some comment\naaa, bbb, cc");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}},
        SolrSynonymsTokenizer::SynonymsLine{{}, {"aaa", "bbb", "cc"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines[0].out}, {"bar", &synonyms_lines[0].out},
        {"aaa", &synonyms_lines[1].out}, {"bbb", &synonyms_lines[1].out},
        {"cc", &synonyms_lines[1].out},
      };
      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0(
      "foo,bar=>foo,bar\n\n#some comment\naaa, bbb, cc => aaa, bbb, cc");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
        SolrSynonymsTokenizer::SynonymsLine{{"aaa", "bbb", "cc"},
                                            {"aaa", "bbb", "cc"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);

      SolrSynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines[0].out}, {"bar", &synonyms_lines[0].out},
        {"aaa", &synonyms_lines[1].out}, {"bbb", &synonyms_lines[1].out},
        {"cc", &synonyms_lines[1].out},
      };

      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0("");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);

      SolrSynonymsTokenizer::SynonymsMap expected = {};

      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0("aaa, bbb, cc => => aaa, bbb, cc");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::basics::Exception";
    } catch (const sdb::basics::Exception& e) {
      EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: More than one "
                "explicit mapping specified on the line 1");
    }
  }

  {
    std::string_view data0("aaa,");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::basics::Exception";
    } catch (const sdb::basics::Exception& e) {
      EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }

  {
    std::string_view data0("aaa=>");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::basics::Exception";
    } catch (const sdb::basics::Exception& e) {
      EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }

  {
    std::string_view data0("aaa,=>aaa");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::basics::Exception";
    } catch (const sdb::basics::Exception& e) {
      EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }

  {
    std::string_view data0("aaa,bbb=>aaa,");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::basics::Exception";
    } catch (const sdb::basics::Exception& e) {
      EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }
  {
    std::string_view data0("\n#aa\naaa,,bbb=>aaa,bbb");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::basics::Exception";
    } catch (const sdb::basics::Exception& e) {
      EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 3");
    }
  }

  {
    std::string_view data0("foo,bar,foo");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }

  {
    std::string_view data0("foo,bar,foo=>foo,bar");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }

  {
    std::string_view data0("foo,bar,foo=>foo,bar");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      SolrSynonymsTokenizer::SynonymsLines expected{
        SolrSynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }
}

TEST(solr_synonyms_tests, make_state_owning_storage) {
  auto state =
    SolrSynonymsTokenizer::MakeState("ipod, i-pod, i pod\nfoo => bar");
  ASSERT_NE(nullptr, state);
  SolrSynonymsTokenizer stream{std::move(state)};

  auto* term = irs::get<irs::TermAttr>(stream);
  auto* inc = irs::get<irs::IncAttr>(stream);

  // Bidirectional line: "ipod" expands to all three variants.
  ASSERT_TRUE(stream.reset("ipod"));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ(1, inc->value);
  std::vector<std::string> emitted;
  emitted.emplace_back(irs::ViewCast<char>(term->value));
  while (stream.next()) {
    ASSERT_EQ(0, inc->value);
    emitted.emplace_back(irs::ViewCast<char>(term->value));
  }
  std::sort(emitted.begin(), emitted.end());
  ASSERT_EQ((std::vector<std::string>{"i pod", "i-pod", "ipod"}), emitted);

  // One-way mapping: "foo" -> "bar".
  ASSERT_TRUE(stream.reset("foo"));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());

  // Unknown input passes through unchanged.
  ASSERT_TRUE(stream.reset("baz"));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("baz", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());
}

TEST(solr_synonyms_tests, make_state_invalid_input) {
  try {
    SolrSynonymsTokenizer::MakeState("foo,bar=>=>baz");
    FAIL() << "expected sdb::basics::Exception";
  } catch (const sdb::basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
  }
}

TEST(solr_synonyms_tests, factory_make_json) {
  auto analyzer = SolrSynonymsTokenizer::Make(SolrSynonymsTokenizer::Options{
    .synonyms_text = "ipod, i-pod, i pod",
  });
  ASSERT_NE(nullptr, analyzer);

  auto* term = irs::get<irs::TermAttr>(*analyzer);
  ASSERT_TRUE(analyzer->reset("ipod"));
  std::vector<std::string> emitted;
  while (analyzer->next()) {
    emitted.emplace_back(irs::ViewCast<char>(term->value));
  }
  std::sort(emitted.begin(), emitted.end());
  ASSERT_EQ((std::vector<std::string>{"i pod", "i-pod", "ipod"}), emitted);
}

TEST(solr_synonyms_tests, factory_make_default_options) {
  // Ported from the legacy `factory_make_json_missing_field` test, which
  // fed `{}` to the JSON parser and asserted nullptr. The direct-Options
  // API treats a missing `synonyms_text` as an empty string, which is a
  // valid (but empty) synonyms map -- the analyzer is non-null and emits
  // the input verbatim (no synonym substitution).
  auto analyzer = SolrSynonymsTokenizer::Make(SolrSynonymsTokenizer::Options{});
  ASSERT_NE(nullptr, analyzer);

  auto* term = irs::get<irs::TermAttr>(*analyzer);
  ASSERT_TRUE(analyzer->reset("anything"));
  ASSERT_TRUE(analyzer->next());
  ASSERT_EQ("anything", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(analyzer->next());
}
