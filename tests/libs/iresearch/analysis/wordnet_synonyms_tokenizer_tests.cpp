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
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/analysis/wordnet_synonyms_tokenizer.hpp"
#include "pg/sql_exception_macro.h"
#include "token_sink_utils.hpp"

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

    const auto tokens = tests::Analyze(stream, data0);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_TRUE(tokens->empty());
  }

  {
    std::string data0 = "come";
    std::string data1 = "advance";
    WordnetSynonymsTokenizer::SynonymsGroups group{"100000002"};
    WordnetSynonymsTokenizer::SynonymsMap mapping{{"come", {"100000002"}}};

    WordnetSynonymsTokenizer stream(StateFromMap(std::move(mapping)));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    {
      const auto tokens = tests::Analyze(stream, data0);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 4}), (*tokens)[0]);
    }

    {
      const auto tokens = tests::Analyze(stream, data1);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_TRUE(tokens->empty());
    }
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

    {
      const auto tokens = tests::Analyze(stream, data0);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 4}), (*tokens)[0]);
    }

    {
      const auto tokens = tests::Analyze(stream, data1);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 7}), (*tokens)[0]);
    }

    {
      const auto tokens = tests::Analyze(stream, data2);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 8}), (*tokens)[0]);
    }
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

  {
    const auto tokens = tests::Analyze(stream, data0);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(2, tokens->size());
    ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 5}), (*tokens)[0]);
    ASSERT_EQ((tests::AnalyzerToken{"100000003", 2, 0, 5}), (*tokens)[1]);
  }

  {
    const auto tokens = tests::Analyze(stream, data1);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 5}), (*tokens)[0]);
  }
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

  {
    const auto tokens = tests::Analyze(stream, data0);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(2, tokens->size());
    ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 5}), (*tokens)[0]);
  }

  {
    const auto tokens = tests::Analyze(stream, data1);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(1, tokens->size());
    ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 5}), (*tokens)[0]);
  }
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

  {
    const auto tokens = tests::Analyze(stream, data0);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(2, tokens->size());
    ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 5}), (*tokens)[0]);
  }

  {
    const auto tokens = tests::Analyze(stream, data0);
    ASSERT_TRUE(tokens.has_value());
    ASSERT_EQ(2, tokens->size());
    ASSERT_EQ((tests::AnalyzerToken{"100000002", 1, 0, 5}), (*tokens)[0]);
    ASSERT_EQ((tests::AnalyzerToken{"100000003", 2, 0, 5}), (*tokens)[1]);
  }
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

  {
    const auto terms = tests::AnalyzeTerms(stream, "come");
    ASSERT_TRUE(terms.has_value());
    ASSERT_EQ((std::vector<std::string>{"100000002"}), *terms);
  }

  {
    const auto terms = tests::AnalyzeTerms(stream, "advance");
    ASSERT_TRUE(terms.has_value());
    ASSERT_EQ((std::vector<std::string>{"100000002"}), *terms);
  }

  {
    const auto terms = tests::AnalyzeTerms(stream, "missing");
    ASSERT_TRUE(terms.has_value());
    ASSERT_TRUE(terms->empty());
  }
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

  const auto terms = tests::AnalyzeTerms(*analyzer, "come");
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ((std::vector<std::string>{"100000002"}), *terms);
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

  const auto terms = tests::AnalyzeTerms(*analyzer, "anything");
  ASSERT_TRUE(terms.has_value());
  ASSERT_TRUE(terms->empty());
}

TEST(wordnet_synonyms_tokenizer_tests, native_fills_match_pull) {
  irs::analysis::WordnetSynonymsTokenizer::Options opts;
  opts.synonyms_text =
    "s(100000001,1,'angry',a,1,0).\n"
    "s(100000001,2,'furious',a,1,0).\n"
    "s(100000001,3,'mad',a,1,0).\n"
    "s(100000002,1,'happy',a,1,0).\n"
    "s(100000002,2,'glad',a,1,0).\n";
  auto value_stream = irs::analysis::WordnetSynonymsTokenizer::Make(
    irs::analysis::WordnetSynonymsTokenizer::Options{opts});
  auto column_stream = irs::analysis::WordnetSynonymsTokenizer::Make(
    irs::analysis::WordnetSynonymsTokenizer::Options{opts});

  const std::vector<std::string> values = {"angry", "happy", "unknown", ""};

  std::vector<std::vector<std::string>> expected;
  for (const auto& v : values) {
    SCOPED_TRACE(v);
    auto terms = tests::AnalyzeTerms(*value_stream, v);
    ASSERT_TRUE(terms.has_value());
    expected.push_back(std::move(*terms));
  }

  std::vector<duckdb::string_t> vals;
  std::vector<irs::doc_id_t> docs;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
    docs.push_back(static_cast<irs::doc_id_t>(i + 1));
  }

  tests::OneBatchSink sink{irs::TokenLayout::Terms};
  column_stream->Fill(vals, docs, sink.writer, sink.layout);
  ASSERT_FALSE(sink.flushed());
  auto& batch = sink.writer.buf;
  const auto runs = sink.writer.Runs();
  ASSERT_TRUE(batch.dense_pos);

  ASSERT_EQ(values.size(), runs.size());
  uint32_t token_idx = 0;
  for (size_t v = 0; v < values.size(); ++v) {
    SCOPED_TRACE(values[v]);
    ASSERT_EQ(docs[v], runs[v].doc);
    ASSERT_EQ(expected[v].size(), runs[v].ntokens);
    for (const auto& e : expected[v]) {
      const auto& t = batch.terms[token_idx];
      ASSERT_EQ(e, (std::string{t.GetData(), t.GetSize()}));
      ++token_idx;
    }
  }
  ASSERT_EQ(batch.count, token_idx);
}
