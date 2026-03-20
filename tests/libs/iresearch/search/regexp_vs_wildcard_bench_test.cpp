////////////////////////////////////////////////////////////////////////////////
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

#include <chrono>
#include <iomanip>
#include <iostream>

#include "filter_test_case_base.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "tests_shared.hpp"

namespace {

struct RunResult {
  std::chrono::nanoseconds prepare{0};
  std::chrono::nanoseconds execute{0};
  size_t doc_count{0};
};

constexpr int kWarmupRuns = 1;
constexpr int kMeasureRuns = 3;

RunResult Average(const RunResult* results, int count) {
  RunResult avg;
  for (int i = 0; i < count; ++i) {
    avg.prepare += results[i].prepare;
    avg.execute += results[i].execute;
    avg.doc_count = results[i].doc_count;
  }
  avg.prepare /= count;
  avg.execute /= count;
  return avg;
}

}  // namespace

class RegexpRe2VsWildcardBenchTestCase : public tests::FilterTestCaseBase {
 protected:
  RunResult RunFilter(const irs::Filter& filter, const irs::IndexReader& rdr) {
    RunResult result;

    auto t0 = std::chrono::high_resolution_clock::now();
    auto q = filter.prepare({.index = rdr});
    auto t1 = std::chrono::high_resolution_clock::now();
    result.prepare = t1 - t0;

    EXPECT_NE(nullptr, q);
    if (!q) {
      return result;
    }

    auto t2 = std::chrono::high_resolution_clock::now();
    for (const auto& sub : rdr) {
      auto docs = q->execute({.segment = sub});
      EXPECT_NE(nullptr, docs);
      if (!docs) {
        continue;
      }
      while (docs->next()) {
        ++result.doc_count;
      }
    }
    auto t3 = std::chrono::high_resolution_clock::now();
    result.execute = t3 - t2;

    return result;
  }

  RunResult RunAveraged(const irs::Filter& filter,
                        const irs::IndexReader& rdr) {
    // Warmup
    for (int i = 0; i < kWarmupRuns; ++i) {
      RunFilter(filter, rdr);
    }

    // Measure
    RunResult runs[kMeasureRuns];
    for (int i = 0; i < kMeasureRuns; ++i) {
      runs[i] = RunFilter(filter, rdr);
    }

    return Average(runs, kMeasureRuns);
  }

  void BenchPair(const irs::IndexReader& rdr, std::string_view label,
                 std::string_view wildcard_pattern,
                 std::string_view regexp_pattern,
                 std::string_view field = "body_anl") {
    irs::ByWildcard wq;
    *wq.mutable_field() = field;
    wq.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(wildcard_pattern);

    irs::ByRegexpRe2 rq;
    *rq.mutable_field() = field;
    rq.mutable_options()->pattern =
      irs::ViewCast<irs::byte_type>(regexp_pattern);

    auto wt = RunAveraged(wq, rdr);
    auto rt = RunAveraged(rq, rdr);

    ASSERT_EQ(wt.doc_count, rt.doc_count)
      << "Doc count mismatch: wildcard vs regexp_re2 for \"" << label << "\"";

    auto us = [](std::chrono::nanoseconds ns) -> double {
      return std::chrono::duration<double, std::micro>(ns).count();
    };

    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  " << label << "\n"
              << "  docs matched: " << wt.doc_count
              << "  (avg of " << kMeasureRuns << " runs)\n"
              << "  wildcard    \"" << wildcard_pattern << "\"\n"
              << "    prepare: " << us(wt.prepare) << " us\n"
              << "    execute: " << us(wt.execute) << " us\n"
              << "  regexp_re2  \"" << regexp_pattern << "\"\n"
              << "    prepare: " << us(rt.prepare) << " us\n"
              << "    execute: " << us(rt.execute) << " us\n"
              << std::endl;
  }
};

TEST_P(RegexpRe2VsWildcardBenchTestCase, europarl_big) {
  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.big.txt"), doc);
    add_segment(gen);
  }

  auto rdr = open_reader();
  ASSERT_NE(nullptr, rdr);

  std::cout << "\n"
            << " Wildcard vs Regexp_RE2\n"
            << " Dataset: europarl.subset.big.txt\n"
            << " Directory: mmap\n"
            << " Field: body_anl\n"
            << " Warmup: " << kWarmupRuns
            << ", Measure: " << kMeasureRuns << " (averaged)\n"
            << std::endl;

  BenchPair(rdr, "prefix: comm%  vs  comm.*", "comm%", "comm.*");

  BenchPair(rdr, "suffix: %tion  vs  .*tion", "%tion", ".*tion");

  BenchPair(rdr, "infix: %ment%  vs  .*ment.*", "%ment%", ".*ment.*");

  BenchPair(rdr, "single char: c_t  vs  c.t", "c_t", "c.t");

  BenchPair(rdr, "complex: %un_t%  vs  .*un.t.*", "%un_t%", ".*un.t.*");

  BenchPair(rdr, "prefix long: legislat%  vs  legislat.*", "legislat%",
            "legislat.*");

  BenchPair(rdr, "match all: %  vs  .*", "%", ".*");
}

INSTANTIATE_TEST_SUITE_P(
  regexp_re2_vs_wildcard_bench, RegexpRe2VsWildcardBenchTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MmapDirectory>),
    ::testing::Values(tests::FormatInfo{"1_5simd"})),
  RegexpRe2VsWildcardBenchTestCase::to_string);
