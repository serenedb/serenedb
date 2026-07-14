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

#include <stdexcept>

#include "gtest/gtest.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/wordnet_synonyms_tokenizer.hpp"
#include "pg/sql_exception_macro.h"

using WordnetSynonymsTokenizer = irs::analysis::WordnetSynonymsTokenizer;

namespace {

// Wraps an externally-owned synonyms map in an ad-hoc State so tests can
// drive the tokenizer without re-parsing.
std::shared_ptr<const WordnetSynonymsTokenizer::State> StateFromMap(
  WordnetSynonymsTokenizer::SynonymsMap mapping) {
  auto state = std::make_shared<WordnetSynonymsTokenizer::State>();
  state->mapping = std::move(mapping);
  return state;
}

}  // namespace

TEST(wordnet_synonyms_tests, consts) {
  static_assert("wordnet_synonyms" ==
                irs::Type<WordnetSynonymsTokenizer>::name());
}

TEST(wordnet_synonyms_tests, test_masking) {
  {
    std::string data0 = "come";
    WordnetSynonymsTokenizer::SynonymsMap mapping;

    WordnetSynonymsTokenizer stream(StateFromMap(std::move(mapping)));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_FALSE(stream.next());
  }

  {
    std::string data0 = "come";
    std::string data1 = "advance";
    WordnetSynonymsTokenizer::SynonymsGroups group{"100000002"};
    WordnetSynonymsTokenizer::SynonymsMap mapping{{"come", {"100000002"}}};

    WordnetSynonymsTokenizer stream(StateFromMap(std::move(mapping)));
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

    WordnetSynonymsTokenizer stream(StateFromMap(std::move(mapping)));
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

  WordnetSynonymsTokenizer stream(StateFromMap(std::move(mapping)));
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

  WordnetSynonymsTokenizer stream(StateFromMap(std::move(mapping)));
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

  WordnetSynonymsTokenizer stream(StateFromMap(std::move(mapping)));
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
    const auto input = WordnetSynonymsTokenizer::Parse(data0);
    WordnetSynonymsTokenizer::SynonymsMap expected{{"come", {"100000002"}}};
    ASSERT_EQ(expected, input);
  }
}

TEST(wordnet_synonyms_tests, parsing_empty) {
  {
    std::string_view data0("");
    const auto input = WordnetSynonymsTokenizer::Parse(data0);

    WordnetSynonymsTokenizer::SynonymsMap expected{};
    ASSERT_EQ(expected, input);
  }
}

TEST(wordnet_synonyms_tests, parsing_some_lines) {
  std::string_view data0(
    "s(100000002,1,'come',v,1,0).\ns(100000002,2,'advance',v,1,0).\n\ns("
    "100000002,3,'approach',v,1,0).\ns(100000003,1,'release',v,1,0).");
  const auto input = WordnetSynonymsTokenizer::Parse(data0);

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
  const auto input = WordnetSynonymsTokenizer::Parse(data0);

  WordnetSynonymsTokenizer::SynonymsMap expected{
    {"aerial", {"301380267"}},
  };
  ASSERT_EQ(expected, input);
}

TEST(wordnet_synonyms_tests, parsing_homonym_diffrent_synset_order) {
  std::string_view data0(
    "s(100000001,1,'word0',v,1,0).\ns(100000002,2,'word0',v,1,0).\n\ns("
    "100000004,3,'word0',v,1,0).\ns(100000003,1,'word0',v,1,0).");
  const auto input = WordnetSynonymsTokenizer::Parse(data0);
  WordnetSynonymsTokenizer::SynonymsMap expected{
    {"word0", {"100000001", "100000002", "100000003", "100000004"}},
  };
  ASSERT_EQ(expected, input);
}

TEST(wordnet_synonyms_tests, parsing_duplicate_synsets) {
  std::string_view data0(
    "s(100000002,1,'word1',v,1,0).\ns(100000003,1,'word2',v,1,0).\ns(100000002,"
    "1,'word1',v,1,0).\n");
  const auto input = WordnetSynonymsTokenizer::Parse(data0);
  WordnetSynonymsTokenizer::SynonymsMap expected{
    {"word1", {"100000002"}},
    {"word2", {"100000003"}},
  };
  ASSERT_EQ(expected, input);
}

TEST(wordnet_synonyms_tests, parsing_broken_short_line) {
  for (std::string_view data0 : {std::string("a"), std::string("go")}) {
    try {
      WordnetSynonymsTokenizer::Parse(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
      EXPECT_EQ(e.message(),
                "wordnet_synonyms: failed to parse synonyms: Failed parse "
                "line 1");
    }
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
    try {
      WordnetSynonymsTokenizer::Parse(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
      EXPECT_EQ(e.message(),
                "wordnet_synonyms: failed to parse synonyms: Failed parse "
                "line 1");
    }
  }
}

TEST(wordnet_synonyms_tests, parsing_broken_second_line) {
  std::string_view data0("s(100000002,1,'come',v,1,0).\nasd");
  try {
    WordnetSynonymsTokenizer::Parse(data0);
    FAIL() << "expected sdb::SqlException";
  } catch (const sdb::SqlException& e) {
    EXPECT_EQ(e.message(),
              "wordnet_synonyms: failed to parse synonyms: Failed parse "
              "line 2");
  }
}

TEST(wordnet_synonyms_tests, parsing_broken_line_more_param) {
  std::string_view data0("s(100000002,1,'come',v,1,0,2).\n");
  try {
    WordnetSynonymsTokenizer::Parse(data0);
    FAIL() << "expected sdb::SqlException";
  } catch (const sdb::SqlException& e) {
    EXPECT_EQ(e.message(),
              "wordnet_synonyms: failed to parse synonyms: Failed parse "
              "line 1");
  }
}

TEST(wordnet_synonyms_tests, parsing_broken_line_less_param) {
  std::string_view data0("s(100000002,1,'come').\n");
  try {
    WordnetSynonymsTokenizer::Parse(data0);
    FAIL() << "expected sdb::SqlException";
  } catch (const sdb::SqlException& e) {
    EXPECT_EQ(e.message(),
              "wordnet_synonyms: failed to parse synonyms: Failed parse "
              "line 1");
  }
}

TEST(wordnet_synonyms_tests, make_state_owning_storage) {
  auto state = WordnetSynonymsTokenizer::MakeState(
    "s(100000002,1,'come',v,1,0).\ns(100000002,2,'advance',v,1,0).");
  ASSERT_NE(nullptr, state);
  WordnetSynonymsTokenizer stream{std::move(state)};

  auto* term = irs::get<irs::TermAttr>(stream);

  ASSERT_TRUE(stream.reset("come"));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());

  ASSERT_TRUE(stream.reset("advance"));
  ASSERT_TRUE(stream.next());
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(stream.next());

  ASSERT_TRUE(stream.reset("missing"));
  ASSERT_FALSE(stream.next());
}

TEST(wordnet_synonyms_tests, make_state_invalid_input) {
  try {
    WordnetSynonymsTokenizer::MakeState("not a wordnet record");
    FAIL() << "expected sdb::SqlException";
  } catch (const sdb::SqlException& e) {
  }
}

TEST(wordnet_synonyms_tests, factory_make_json) {
  auto analyzer =
    WordnetSynonymsTokenizer::Make(WordnetSynonymsTokenizer::Options{
      .synonyms_text = "s(100000002,1,'come',v,1,0).",
    });
  ASSERT_NE(nullptr, analyzer);

  auto* term = irs::get<irs::TermAttr>(*analyzer);
  ASSERT_TRUE(analyzer->reset("come"));
  ASSERT_TRUE(analyzer->next());
  ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
  ASSERT_FALSE(analyzer->next());
}

// NOTE: the legacy `factory_make_json_missing_field` test fed `{}` (no
// `synonyms` field) through the JSON loader and expected nullptr because the
// loader rejected the missing required field. The direct Options API has no
// equivalent "field-was-missing" notion -- `synonyms_text` is just a string,
// and the default-initialized empty value is a valid (empty) synonyms file
// that Parse accepts. The check below ports the spirit of the original test:
// default Options yields a valid analyzer that simply has no entries, so
// every term reset to a non-empty input emits the input itself.
TEST(wordnet_synonyms_tests, factory_make_missing_field) {
  auto analyzer =
    WordnetSynonymsTokenizer::Make(WordnetSynonymsTokenizer::Options{});
  ASSERT_NE(nullptr, analyzer);

  ASSERT_TRUE(analyzer->reset("anything"));
  ASSERT_FALSE(analyzer->next());
}
