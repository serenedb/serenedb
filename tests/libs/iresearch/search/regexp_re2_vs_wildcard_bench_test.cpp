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

#include <chrono>
#include <iomanip>
#include <iostream>

#include "filter_test_case_base.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/regexp_filter_re2.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "tests_shared.hpp"

namespace {

struct RunResult {
  std::chrono::nanoseconds prepare{0};
  std::chrono::nanoseconds execute{0};
  size_t doc_count{0};
};

}  // namespace

class RegexpRe2VsWildcardBenchTestCase : public tests::FilterTestCaseBase {
 protected:
  RunResult RunFilter(const irs::Filter& filter, const irs::IndexReader& rdr) {
    RunResult result;

    // PREPARE: parse pattern, build automaton, prepare query
    auto t0 = std::chrono::high_resolution_clock::now();
    auto q = filter.prepare({.index = rdr});
    auto t1 = std::chrono::high_resolution_clock::now();
    result.prepare = t1 - t0;

    EXPECT_NE(nullptr, q);
    if (!q)
      return result;

    // EXECUTE: intersect automaton with term dictionary, iterate docs
    auto t2 = std::chrono::high_resolution_clock::now();
    for (const auto& sub : rdr) {
      auto docs = q->execute({.segment = sub});
      EXPECT_NE(nullptr, docs);
      if (!docs)
        continue;
      while (docs->next()) {
        ++result.doc_count;
      }
    }
    auto t3 = std::chrono::high_resolution_clock::now();
    result.execute = t3 - t2;

    return result;
  }

  // Three-way comparison: wildcard vs regexp (custom parser) vs regexp_re2.
  void BenchTriple(const irs::IndexReader& rdr, std::string_view label,
                   std::string_view wildcard_pattern,
                   std::string_view regexp_pattern,
                   std::string_view field = "body_anl") {
    // Build filters
    irs::ByWildcard wq;
    *wq.mutable_field() = field;
    wq.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(wildcard_pattern);

    irs::ByRegexp rq;
    *rq.mutable_field() = field;
    rq.mutable_options()->pattern =
      irs::ViewCast<irs::byte_type>(regexp_pattern);

    irs::ByRegexpRe2 rq2;
    *rq2.mutable_field() = field;
    rq2.mutable_options()->pattern =
      irs::ViewCast<irs::byte_type>(regexp_pattern);

    // Wildcard: warmup + measure
    RunFilter(wq, rdr);
    auto wt = RunFilter(wq, rdr);

    // Regexp (custom parser): warmup + measure
    RunFilter(rq, rdr);
    auto rt = RunFilter(rq, rdr);

    // Regexp RE2 (google re2 parser): warmup + measure
    RunFilter(rq2, rdr);
    auto rt2 = RunFilter(rq2, rdr);

    // Sanity: all three must return the same number of documents
    ASSERT_EQ(wt.doc_count, rt.doc_count)
      << "Doc count mismatch: wildcard vs regexp for \"" << label << "\"";
    ASSERT_EQ(wt.doc_count, rt2.doc_count)
      << "Doc count mismatch: wildcard vs regexp_re2 for \"" << label << "\"";

    // Print results
    auto us = [](std::chrono::nanoseconds ns) -> double {
      return std::chrono::duration<double, std::micro>(ns).count();
    };

    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  " << label << "  " << "\n"
              << "  docs matched: " << wt.doc_count << "\n"
              << "  wildcard    \"" << wildcard_pattern << "\"" << "\n"
              << "    prepare: " << us(wt.prepare) << " us" << "\n"
              << "    execute: " << us(wt.execute) << " us" << "\n"
              << "  regexp      \"" << regexp_pattern << "\"" << "\n"
              << "    prepare: " << us(rt.prepare) << " us" << "\n"
              << "    execute: " << us(rt.execute) << " us" << "\n"
              << "  regexp_re2  \"" << regexp_pattern << "\"" << "\n"
              << "    prepare: " << us(rt2.prepare) << " us" << "\n"
              << "    execute: " << us(rt2.execute) << " us" << "\n"
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
            << " Wildcard vs Regexp vs Regexp_RE2" << "\n"
            << " Dataset: europarl.subset.big.txt" << "\n"
            << " Directory: mmap" << "\n"
            << " Field: body_anl" << "\n"
            << std::endl;

  BenchTriple(rdr, "prefix: comm%  vs  comm.*", "comm%", "comm.*");

  BenchTriple(rdr, "suffix: %tion  vs  .*tion", "%tion", ".*tion");

  BenchTriple(rdr, "infix: %ment%  vs  .*ment.*", "%ment%", ".*ment.*");

  BenchTriple(rdr, "single char: c_t  vs  c.t", "c_t", "c.t");

  BenchTriple(rdr, "complex: %un_t%  vs  .*un.t.*", "%un_t%", ".*un.t.*");

  BenchTriple(rdr, "prefix long: legislat%  vs  legislat.*", "legislat%",
              "legislat.*");

  BenchTriple(rdr, "match all: %  vs  .*", "%", ".*");
}

INSTANTIATE_TEST_SUITE_P(
  regexp_re2_vs_wildcard_bench, RegexpRe2VsWildcardBenchTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MmapDirectory>),
    ::testing::Values(tests::FormatInfo{"1_5simd"})),
  RegexpRe2VsWildcardBenchTestCase::to_string);

// launching: ./bin/iresearch-tests
// options:
//         ires_output_path: ./bin
//         ires_resource_dir: /home/andrei/serenedb/resources/tests/iresearch
// Note: Google Test filter = *regexp_re2_vs_wildcard_bench*
// [==========] Running 1 test from 1 test suite.
// [----------] Global test environment set-up.
// [----------] 1 test from
// regexp_re2_vs_wildcard_bench/RegexpRe2VsWildcardBenchTestCase [ RUN      ]
// regexp_re2_vs_wildcard_bench/RegexpRe2VsWildcardBenchTestCase.europarl_big/mmap___1_5simd

//  Wildcard vs Regexp vs Regexp_RE2
//  Dataset: europarl.subset.big.txt
//  Directory: mmap
//  Field: body_anl

//   prefix: comm%  vs  comm.*
//   docs matched: 1628
//   wildcard    "comm%"
//     prepare: 119.9 us
//     execute: 102.0 us
//   regexp      "comm.*"
//     prepare: 110.6 us
//     execute: 109.3 us
//   regexp_re2  "comm.*"
//     prepare: 108.8 us
//     execute: 99.5 us

//   suffix: %tion  vs  .*tion
//   docs matched: 2045
//   wildcard    "%tion"
//     prepare: 28060.3 us
//     execute: 427.1 us
//   regexp      ".*tion"
//     prepare: 28428.1 us
//     execute: 429.9 us
//   regexp_re2  ".*tion"
//     prepare: 29196.9 us
//     execute: 439.6 us

//   infix: %ment%  vs  .*ment.*
//   docs matched: 4895
//   wildcard    "%ment%"
//     prepare: 29674.7 us
//     execute: 954.7 us
//   regexp      ".*ment.*"
//     prepare: 30113.3 us
//     execute: 948.0 us
//   regexp_re2  ".*ment.*"
//     prepare: 32185.1 us
//     execute: 998.3 us

//   single char: c_t  vs  c.t
//   docs matched: 117
//   wildcard    "c_t"
//     prepare: 187.7 us
//     execute: 15.0 us
//   regexp      "c.t"
//     prepare: 178.0 us
//     execute: 13.4 us
//   regexp_re2  "c.t"
//     prepare: 876.7 us
//     execute: 15.6 us

//   complex: %un_t%  vs  .*un.t.*
//   docs matched: 1078
//   wildcard    "%un_t%"
//     prepare: 26785.2 us
//     execute: 170.6 us
//   regexp      ".*un.t.*"
//     prepare: 27193.7 us
//     execute: 170.6 us
//   regexp_re2  ".*un.t.*"
//     prepare: 30548.6 us
//     execute: 187.5 us

//   prefix long: legislat%  vs  legislat.*
//   docs matched: 235
//   wildcard    "legislat%"
//     prepare: 44.4 us
//     execute: 23.9 us
//   regexp      "legislat.*"
//     prepare: 42.1 us
//     execute: 21.8 us
//   regexp_re2  "legislat.*"
//     prepare: 40.8 us
//     execute: 21.5 us

//   match all: %  vs  .*
//   docs matched: 10250
//   wildcard    "%"
//     prepare: 77682.9 us
//     execute: 37711.1 us
//   regexp      ".*"
//     prepare: 76737.0 us
//     execute: 37717.3 us
//   regexp_re2  ".*"
//     prepare: 77922.5 us
//     execute: 37828.6 us

// [       OK ]
// regexp_re2_vs_wildcard_bench/RegexpRe2VsWildcardBenchTestCase.europarl_big/mmap___1_5simd
// (76984 ms)
// [----------] 1 test from
// regexp_re2_vs_wildcard_bench/RegexpRe2VsWildcardBenchTestCase (76984 ms
// total)

// [----------] Global test environment tear-down
// [==========] 1 test from 1 test suite ran. (76984 ms total)
// [  PASSED  ] 1 test.
