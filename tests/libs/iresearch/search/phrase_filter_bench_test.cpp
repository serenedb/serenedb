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

#include <filesystem>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/term_query.hpp"
#include "tests_shared.hpp"

struct Config {
  inline static const std::string repeat_pattern_json =
    "interval_bench_repeat.json";
  inline static const std::string freqs_discrete_json =
    "interval_bench_freqs_discrete.json";
  inline static const std::string freqs_equal_json =
    "interval_bench_freqs_equal.json";
  inline static const std::string big_data_repeat_pattern_json =
    "interval_bench_big_data_repeat.json";
  inline static const std::string big_data_freqs_discrete_json =
    "interval_bench_big_data_freqs_discrete.json";
  inline static const std::string big_data_freqs_equal_json =
    "interval_bench_big_data_freqs_equal.json";

  inline static const std::string time_report_dir =
    "phrase_filter_bench_test_report";
  inline static const std::string golden_dir =
    "phrase_filter_bench_test_golden";
};

static const auto tab = '\t';
static const auto double_tab = "\t\t";

template<typename Func>
void timeEstimate(const std::string& test_name, Func func, std::ostream& out,
                  std::ostream& report) {
  auto start = std::chrono::high_resolution_clock::now();
  func(test_name, out);
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
    std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  report << double_tab << "\"time\": " << duration.count();
}

class PhraseFilterBenchTestCase : public tests::FilterTestCaseBase {
  static inline constexpr irs::field_id kName = tests::FieldIdFor("name");
  static inline constexpr irs::field_id kPhraseAnl =
    tests::FieldIdFor("phrase_anl");
  auto StoreName() {
    return [](irs::IndexWriter::Document& doc, const tests::Document& src) {
      const auto* name =
        dynamic_cast<const tests::StringField*>(src.stored.get_by_id(kName));
      if (name) {
        irs::tests::StoreFieldAt(*doc.GetColWriter(), kName, doc.DocId(),
                                 *name);
      }
    };
  }

  static void AnalyzedJsonFieldFactory(
    tests::Document& doc, const std::string& name,
    const tests::JsonDocGenerator::JsonValue& data) {
    typedef tests::TextField<std::string> TextField;

    class StringField : public tests::StringField {
     public:
      StringField(const std::string& name, const std::string_view& value)
        : tests::StringField(name, value, irs::IndexFeatures::Freq) {}
    };

    if (data.is_string()) {
      // analyzed field -- id derived per source JSON field name so different
      // sources (e.g. "name" vs "phrase") don't collide on the same writer
      // slot.
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

 protected:
  void SetUp() final {
    FilterTestCaseBase::SetUp();
    for (int i = 1; i < gArgc; ++i) {
      const std::string arg = gArgv[i];
      if (arg == "--report") {
        _need_time_report = true;
      }
    }
  }

 public:
  void estimateTimePhraseFilter(const std::string& input) {
    {
      tests::JsonDocGenerator gen(resource(input), &AnalyzedJsonFieldFactory);
      add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
                  StoreName());
    }

    auto rdr = open_reader(irs::tests::DefaultReaderOptions());

    auto search_test = [&rdr](size_t off1, size_t off2,
                              const std::string& test_name, std::ostream& out) {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fox"));
      q.mutable_options()->push_back<irs::ByTermOptions>(off1, off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      q.mutable_options()->push_back<irs::ByTermOptions>(off1, off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
      q.mutable_options()->push_back<irs::ByTermOptions>(off1, off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("jumps"));

      tests::PreparedFilter prepared{q, rdr};

      auto docs = prepared.Execute(0);
      docs->advance();
      while (!irs::doc_limits::eof(docs->value())) {
        out << docs->value() << '\n';
        docs->advance();
      }
    };

    auto freqs_test = [&rdr](size_t off1, size_t off2,
                             const std::string& test_name, std::ostream& out) {
      irs::ByPhrase q;
      *q.mutable_field_id() = kPhraseAnl;
      q.mutable_options()->push_back<irs::ByTermOptions>().term =
        irs::ViewCast<irs::byte_type>(std::string_view("fox"));
      q.mutable_options()->push_back<irs::ByTermOptions>(off1, off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("quick"));
      q.mutable_options()->push_back<irs::ByTermOptions>(off1, off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("brown"));
      q.mutable_options()->push_back<irs::ByTermOptions>(off1, off2).term =
        irs::ViewCast<irs::byte_type>(std::string_view("jumps"));

      tests::sort::CustomSort sort;
      irs::DocIterator* it = nullptr;
      sort.scorer_score = [&](const irs::ScoreOperator*, irs::score_t* score,
                              size_t n) { *score = it->value(); };

      tests::PreparedFilter prepared{q, rdr, &sort};
      auto docs = prepared.Execute(0);
      auto docs_seek = prepared.Execute(0);
      tests::sort::FrequencyScore freq_score;
      auto* freq_seek = irs::get<irs::FreqBlockAttr>(*docs_seek);
      docs->advance();
      while (!irs::doc_limits::eof(docs->value())) {
        docs_seek->seek(docs->value());
        docs_seek->FetchScoreArgs(0);
        out << freq_seek->value[0] << '\n';
        docs->advance();
      }
    };

    auto params = GetParam();
    std::string test_name =
      input + "__" +
      PhraseFilterBenchTestCase::to_string(
        ::testing::TestParamInfo<decltype(params)>(params, 0));

    std::unique_ptr<std::ostream> out;
    if (_update_golden) {
      out = std::make_unique<std::ofstream>(resource(Config::golden_dir) /
                                            (input + ".golden"));
    } else {
      out = std::make_unique<std::ostringstream>();
    }

    std::unique_ptr<std::ostream> report;
    if (_need_time_report) {
      const auto report_path = resource(Config::time_report_dir);
      if (!std::filesystem::exists(report_path)) {
        ASSERT_TRUE(std::filesystem::create_directory(report_path));
      }
      report =
        std::make_unique<std::ofstream>(report_path / (test_name + ".json"));
    } else {
      report = std::make_unique<std::ostream>(nullptr);
    }
    *report << "[\n";

    auto log_test = [](std::ostream& out, std::ostream& report,
                       const std::string& subtest_name) {
      report << tab << "{\n";
      report << double_tab << "\"subtest_name\": \"" << subtest_name << "\",\n";
      out << "Results of test '" << subtest_name << "'\n";
    };

    auto search_test1 = [search_test](const std::string& test_name,
                                      std::ostream& out) {
      search_test(1, 3, test_name, out);
    };
    log_test(*out, *report, "single_search1");
    timeEstimate("single_search1", search_test1, *out, *report);
    *report << '\n' << tab << "},\n";

    auto search_test2 = [search_test](const std::string& test_name,
                                      std::ostream& out) {
      search_test(1, 10, test_name, out);
    };
    log_test(*out, *report, "single_search2");
    timeEstimate("single_search2", search_test2, *out, *report);
    *report << '\n' << tab << "},\n";

    auto freqs_test1 = [freqs_test](const std::string& test_name,
                                    std::ostream& out) {
      freqs_test(1, 3, test_name, out);
    };
    log_test(*out, *report, "freqs_search1");
    timeEstimate("freqs_search1", freqs_test1, *out, *report);
    *report << '\n' << tab << "},\n";

    auto freqs_test2 = [freqs_test](const std::string& test_name,
                                    std::ostream& out) {
      freqs_test(1, 10, test_name, out);
    };
    log_test(*out, *report, "freqs_search2");
    timeEstimate("freqs_search2", freqs_test2, *out, *report);
    *report << '\n' << tab << "}\n]\n";

    auto test_output = static_cast<std::ostringstream&>(*out).str();
    std::ifstream golden(resource(Config::golden_dir) / (input + ".golden"));
    ASSERT_TRUE(golden.is_open());

    std::string golden_content{std::istreambuf_iterator<char>(golden),
                               std::istreambuf_iterator<char>()};
    ASSERT_EQ(golden_content, test_output);
  }

 private:
  bool _update_golden{false};
  bool _need_time_report{false};
};

TEST_P(PhraseFilterBenchTestCase, repeated_phrase) {
  estimateTimePhraseFilter(Config::repeat_pattern_json);
}

TEST_P(PhraseFilterBenchTestCase, freqs_equal) {
  estimateTimePhraseFilter(Config::freqs_equal_json);
}

TEST_P(PhraseFilterBenchTestCase, discrete_freqs) {
  estimateTimePhraseFilter(Config::freqs_discrete_json);
}

TEST_P(PhraseFilterBenchTestCase, big_data_repeated_phrase) {
  estimateTimePhraseFilter(Config::big_data_repeat_pattern_json);
}

TEST_P(PhraseFilterBenchTestCase, big_data_freqs_equal) {
  estimateTimePhraseFilter(Config::big_data_freqs_equal_json);
}

TEST_P(PhraseFilterBenchTestCase, big_data_discrete_freqs) {
  estimateTimePhraseFilter(Config::big_data_freqs_discrete_json);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(phrase_filter_bench_test, PhraseFilterBenchTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         PhraseFilterBenchTestCase::to_string);
