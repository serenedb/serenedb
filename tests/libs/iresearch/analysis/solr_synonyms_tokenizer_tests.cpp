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
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "pg/sql_exception_macro.h"
#include "test_resources.hpp"
#include "token_sink_utils.hpp"

using SolrSynonymsTokenizer = irs::analysis::SolrSynonymsTokenizer;

namespace {

using SynonymsEntries = std::initializer_list<
  std::pair<std::string_view, const SolrSynonymsTokenizer::SynonymsList*>>;

duckdb::shared_ptr<const SolrSynonymsTokenizer::State> StateFromMap(
  SynonymsEntries entries) {
  auto state = duckdb::make_shared_ptr<SolrSynonymsTokenizer::State>();
  for (const auto& [word, terms] : entries) {
    state->synonyms[word] = terms;
  }
  return state;
}

SolrSynonymsTokenizer::SynonymsMap MakeMap(SynonymsEntries entries) {
  SolrSynonymsTokenizer::SynonymsMap map;
  for (const auto& [word, terms] : entries) {
    map[word] = terms;
  }
  return map;
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
    SolrSynonymsTokenizer stream(StateFromMap({}));
    ASSERT_EQ(irs::Type<SolrSynonymsTokenizer>::id(), stream.type());

    {
      const auto tokens = tests::Analyze(stream, data0);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"abc", 1, 0, 3}), (*tokens)[0]);
    }

    {
      const auto tokens = tests::Analyze(stream, data1);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"ghi", 1, 0, 3}), (*tokens)[0]);
    }
  }

  // test with synonyms
  {
    std::string_view data0("foo");
    std::string_view data1("bar");
    std::string_view data2("xyz");

    const SolrSynonymsTokenizer::SynonymsList synonyms{"foo", "bar"};
    SolrSynonymsTokenizer stream(StateFromMap({
      {"foo", &synonyms},
      {"bar", &synonyms},
    }));

    {
      const auto tokens = tests::Analyze(stream, data0);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(2, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"foo", 1, 0, 3}), (*tokens)[0]);
      ASSERT_EQ((tests::AnalyzerToken{"bar", 1, 0, 3}), (*tokens)[1]);
    }

    {
      const auto tokens = tests::Analyze(stream, data1);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(2, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"foo", 1, 0, 3}), (*tokens)[0]);
      ASSERT_EQ((tests::AnalyzerToken{"bar", 1, 0, 3}), (*tokens)[1]);
    }

    {
      const auto tokens = tests::Analyze(stream, data2);
      ASSERT_TRUE(tokens.has_value());
      ASSERT_EQ(1, tokens->size());
      ASSERT_EQ((tests::AnalyzerToken{"xyz", 1, 0, 3}), (*tokens)[0]);
    }
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

      const auto expected = MakeMap({{"foo", &synonyms_lines.back().out},
                                     {"bar", &synonyms_lines.back().out}});
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

      const auto expected = MakeMap({{"foo", &synonyms_lines.back().out},
                                     {"bar", &synonyms_lines.back().out}});
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

      const auto expected = MakeMap({{"foo", &synonyms_lines.back().out},
                                     {"bar", &synonyms_lines.back().out}});
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

      const auto expected = MakeMap({{"foo", &synonyms_lines[0].out},
                                     {"bar", &synonyms_lines[0].out},
                                     {"aaa", &synonyms_lines[1].out},
                                     {"bbb", &synonyms_lines[1].out},
                                     {"cc", &synonyms_lines[1].out}});
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

      const auto expected = MakeMap({{"foo", &synonyms_lines[0].out},
                                     {"bar", &synonyms_lines[0].out},
                                     {"aaa", &synonyms_lines[1].out},
                                     {"bbb", &synonyms_lines[1].out},
                                     {"cc", &synonyms_lines[1].out}});

      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0("");
    const auto synonyms_lines =
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
    {
      const auto actual = SolrSynonymsTokenizer::Parse(synonyms_lines);

      const SolrSynonymsTokenizer::SynonymsMap expected;

      ASSERT_EQ(expected, actual);
    }
  }

  {
    std::string_view data0("aaa, bbb, cc => => aaa, bbb, cc");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: More than one "
                "explicit mapping specified on the line 1");
    }
  }

  {
    std::string_view data0("aaa,");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }

  {
    std::string_view data0("aaa=>");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }

  {
    std::string_view data0("aaa,=>aaa");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }

  {
    std::string_view data0("aaa,bbb=>aaa,");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
      EXPECT_EQ(e.message(),
                "solr_synonyms: failed to parse synonyms: Failed parse line 1");
    }
  }
  {
    std::string_view data0("\n#aa\naaa,,bbb=>aaa,bbb");
    try {
      SolrSynonymsTokenizer::ParseSynonymsLines(data0);
      FAIL() << "expected sdb::SqlException";
    } catch (const sdb::SqlException& e) {
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

  // Bidirectional line: "ipod" expands to all three variants.
  {
    const auto tokens = tests::Analyze(stream, "ipod");
    ASSERT_TRUE(tokens.has_value());
    ASSERT_FALSE(tokens->empty());
    std::vector<std::string> emitted;
    for (const auto& t : *tokens) {
      ASSERT_EQ(1, t.pos);
      emitted.push_back(t.term);
    }
    std::sort(emitted.begin(), emitted.end());
    ASSERT_EQ((std::vector<std::string>{"i pod", "i-pod", "ipod"}), emitted);
  }

  // One-way mapping: "foo" -> "bar".
  {
    const auto terms = tests::AnalyzeTerms(stream, "foo");
    ASSERT_TRUE(terms.has_value());
    ASSERT_EQ((std::vector<std::string>{"bar"}), *terms);
  }

  // Unknown input passes through unchanged.
  {
    const auto terms = tests::AnalyzeTerms(stream, "baz");
    ASSERT_TRUE(terms.has_value());
    ASSERT_EQ((std::vector<std::string>{"baz"}), *terms);
  }
}

TEST(solr_synonyms_tests, make_state_invalid_input) {
  try {
    SolrSynonymsTokenizer::MakeState("foo,bar=>=>baz");
    FAIL() << "expected sdb::SqlException";
  } catch (const sdb::SqlException& e) {
  }
}

TEST(solr_synonyms_tests, factory_make_json) {
  auto analyzer = SolrSynonymsTokenizer::Make(
    SolrSynonymsTokenizer::Options{
      .synonyms_text = "ipod, i-pod, i pod",
    },
    tests::Cache());
  ASSERT_NE(nullptr, analyzer);

  auto terms = tests::AnalyzeTerms(*analyzer, "ipod");
  ASSERT_TRUE(terms.has_value());
  std::sort(terms->begin(), terms->end());
  ASSERT_EQ((std::vector<std::string>{"i pod", "i-pod", "ipod"}), *terms);
}

TEST(solr_synonyms_tests, factory_make_default_options) {
  // Ported from the legacy `factory_make_json_missing_field` test, which
  // fed `{}` to the JSON parser and asserted nullptr. The direct-Options
  // API treats a missing `synonyms_text` as an empty string, which is a
  // valid (but empty) synonyms map -- the analyzer is non-null and emits
  // the input verbatim (no synonym substitution).
  auto analyzer = SolrSynonymsTokenizer::Make(SolrSynonymsTokenizer::Options{},
                                              tests::Cache());
  ASSERT_NE(nullptr, analyzer);

  const auto terms = tests::AnalyzeTerms(*analyzer, "anything");
  ASSERT_TRUE(terms.has_value());
  ASSERT_EQ((std::vector<std::string>{"anything"}), *terms);
}

TEST(solr_synonyms_tokenizer_tests, native_fills_match_pull) {
  irs::analysis::SolrSynonymsTokenizer::Options opts;
  opts.synonyms_text =
    "i-pod, i pod, ipod\n"
    "sea biscuit, sea biscit => seabiscuit\n"
    "gb => gib, gigabyte\n";
  auto value_stream = irs::analysis::SolrSynonymsTokenizer::Make(
    irs::analysis::SolrSynonymsTokenizer::Options{opts}, tests::Cache());
  auto column_stream = irs::analysis::SolrSynonymsTokenizer::Make(
    irs::analysis::SolrSynonymsTokenizer::Options{opts}, tests::Cache());

  const std::vector<std::string> values = {"i-pod", "ipod", "gb",
                                           "unknown-word", ""};

  std::vector<std::vector<tests::AnalyzerToken>> expected;
  for (const auto& v : values) {
    SCOPED_TRACE(v);
    auto tokens = tests::Analyze(*value_stream, v);
    ASSERT_TRUE(tokens.has_value());
    expected.push_back(std::move(*tokens));
  }

  std::vector<duckdb::string_t> vals;
  for (size_t i = 0; i < values.size(); ++i) {
    vals.emplace_back(values[i].data(),
                      static_cast<uint32_t>(values[i].size()));
  }

  size_t flushes = 0;
  const auto check = [&](irs::TokenBatch& batch, irs::DocRuns runs) {
    ++flushes;
    ASSERT_EQ(values.size(), runs.size());
    uint32_t token_idx = 0;
    for (size_t v = 0; v < values.size(); ++v) {
      SCOPED_TRACE(values[v]);
      ASSERT_EQ(v + 1, runs[v].doc);
      ASSERT_EQ(expected[v].size(), runs[v].ntokens);
      for (const auto& e : expected[v]) {
        const auto& t = batch.terms[token_idx];
        ASSERT_EQ(e.term, (std::string{t.GetData(), t.GetSize()}));
        ASSERT_EQ(e.pos, batch.pos[token_idx]);
        ++token_idx;
      }
    }
    ASSERT_EQ(batch.count, token_idx);
  };
  tests::FnTokenSink sink{irs::TokenLayout::TermsPos, check};
  tests::FillColumn(*column_stream, vals, 1, sink.writer, sink.layout);
  sink.writer.Finish();
  ASSERT_EQ(1, flushes);
}

TEST(solr_synonyms_tests, parsing_crlf_lines) {
  std::string_view data0("foo,bar\r\nbaz,qux\r\n");
  const auto synonyms_lines = SolrSynonymsTokenizer::ParseSynonymsLines(data0);
  SolrSynonymsTokenizer::SynonymsLines expected{
    SolrSynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}},
    SolrSynonymsTokenizer::SynonymsLine{{}, {"baz", "qux"}}};
  ASSERT_EQ(expected, synonyms_lines);
}

TEST(solr_synonyms_tests, state_is_shared_between_instances) {
  duckdb::DuckDB db{nullptr};
  auto& cache = db.instance->GetSharedObjectCache();
  const auto make = [&](std::string text) {
    SolrSynonymsTokenizer::Options opts;
    opts.synonyms_text = std::move(text);
    return SolrSynonymsTokenizer::Make(std::move(opts), cache);
  };
  ASSERT_EQ(0u, cache.GetEntryCount());
  auto a = make("fast,quick,speedy");
  auto b = make("fast,quick,speedy");
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  EXPECT_EQ(1u, cache.GetEntryCount());
  EXPECT_EQ(tests::AnalyzeTerms(*a, "quick"), tests::AnalyzeTerms(*b, "quick"));
  EXPECT_EQ(tests::AnalyzeTerms(*a, "other"), tests::AnalyzeTerms(*b, "other"));
  auto c = make("big,large");
  ASSERT_TRUE(c);
  EXPECT_EQ(2u, cache.GetEntryCount());
  a.reset();
  b.reset();
  c.reset();
  EXPECT_EQ(0u, cache.GetEntryCount());
}
