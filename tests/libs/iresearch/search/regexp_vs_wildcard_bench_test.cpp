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
#include "iresearch/search/wildcard_filter.hpp"
#include "tests_shared.hpp"

namespace {

// Result of a single filter run: timings + matched document count.
struct RunResult {
  std::chrono::nanoseconds prepare{0};
  std::chrono::nanoseconds execute{0};
  size_t doc_count{0};
};

}  // namespace

class RegexpVsWildcardBenchTestCase : public tests::FilterTestCaseBase {
 protected:
  // Runs a filter through prepare + execute phases, measuring each separately.
  RunResult RunFilter(const irs::Filter& filter, const irs::IndexReader& rdr) {
    RunResult result;

    // PREPARE phase: build automaton + prepared query
    auto t0 = std::chrono::high_resolution_clock::now();
    auto q = filter.prepare({.index = rdr});
    auto t1 = std::chrono::high_resolution_clock::now();
    result.prepare = t1 - t0;

    EXPECT_NE(nullptr, q);
    if (!q)
      return result;

    // EXECUTE phase: iterate over matching documents
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

  // Benchmark a pair of equivalent wildcard / regexp patterns on the same
  // index. Each filter is run twice: first run is warmup (primes the page
  // cache), second run is the actual measurement.
  void BenchPair(const irs::IndexReader& rdr, std::string_view label,
                 std::string_view wildcard_pattern,
                 std::string_view regexp_pattern,
                 std::string_view field = "body_anl") {
    //   Build wildcard filter
    irs::ByWildcard wq;
    *wq.mutable_field() = field;
    wq.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(wildcard_pattern);

    //   Build regexp filter
    irs::ByRegexp rq;
    *rq.mutable_field() = field;
    rq.mutable_options()->pattern =
      irs::ViewCast<irs::byte_type>(regexp_pattern);

    //   Wildcard: warmup then measure
    RunFilter(wq, rdr);            // warmup: primes mmap page cache
    auto wt = RunFilter(wq, rdr);  // real measurement

    // Regexp: warmup then measure
    RunFilter(rq, rdr);            // warmup: primes mmap page cache
    auto rt = RunFilter(rq, rdr);  // real measurement

    // Sanity check: both filters must return the same number of docs
    ASSERT_EQ(wt.doc_count, rt.doc_count)
      << "Document count mismatch for pattern pair: wildcard=\""
      << wildcard_pattern << "\" regexp=\"" << regexp_pattern << "\"";

    //   Print results
    auto us = [](std::chrono::nanoseconds ns) -> double {
      return std::chrono::duration<double, std::micro>(ns).count();
    };

    std::cout << std::fixed << std::setprecision(1);
    std::cout << "  " << label << "  " << "\n"
              << "  docs matched: " << wt.doc_count << "\n"
              << "  wildcard \"" << wildcard_pattern << "\"" << "\n"
              << "    prepare: " << us(wt.prepare) << " us" << "\n"
              << "    execute: " << us(wt.execute) << " us" << "\n"
              << "  regexp    \"" << regexp_pattern << "\"" << "\n"
              << "    prepare: " << us(rt.prepare) << " us" << "\n"
              << "    execute: " << us(rt.execute) << " us" << "\n"
              << std::endl;
  }
};

TEST_P(RegexpVsWildcardBenchTestCase, europarl_big) {
  // Build index from europarl.subset.big.txt
  {
    tests::EuroparlDocTemplate doc;
    tests::DelimDocGenerator gen(resource("europarl.subset.big.txt"), doc);
    add_segment(gen);
  }

  auto rdr = open_reader();
  ASSERT_NE(nullptr, rdr);

  std::cout << "\n"
            << " Regexp vs Wildcard benchmark" << "\n"
            << " Dataset: europarl.subset.big.txt" << "\n"
            << " Directory: mmap" << "\n"
            << " Field: body_anl" << "\n"
            << std::endl;

  // Equivalent pattern pairs: wildcard syntax / regexp syntax
  //
  // Wildcard:  % = any string, _ = single char
  // Regexp:    .* = any string, . = single char

  BenchPair(rdr, "prefix: comm%  vs  comm.*", "comm%", "comm.*");

  BenchPair(rdr, "suffix: %tion  vs  .*tion", "%tion", ".*tion");

  BenchPair(rdr, "infix: %ment%  vs  .*ment.*", "%ment%", ".*ment.*");

  BenchPair(rdr, "single char: c_t  vs  c.t", "c_t", "c.t");

  BenchPair(rdr, "complex: %un_t%  vs  .*un.t.*", "%un_t%", ".*un.t.*");

  BenchPair(rdr, "prefix long: legislat%  vs  legislat.*", "legislat%",
            "legislat.*");

  BenchPair(rdr, "match all: %  vs  .*", "%", ".*");
}

// Only mmap directory — production configuration.
INSTANTIATE_TEST_SUITE_P(
  regexp_vs_wildcard_bench, RegexpVsWildcardBenchTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::Directory<&tests::MmapDirectory>),
    ::testing::Values(tests::FormatInfo{"1_5simd"})),
  RegexpVsWildcardBenchTestCase::to_string);

// launching: ./bin/iresearch-tests
// options:
//         ires_output_path: ./bin
//         ires_resource_dir: /home/andrei/serenedb/resources/tests/iresearch
// Note: Google Test filter = *regexp_vs_wildcard_bench*
// [==========] Running 1 test from 1 test suite.
// [----------] Global test environment set-up.
// [----------] 1 test from
// regexp_vs_wildcard_bench/RegexpVsWildcardBenchTestCase [ RUN      ]
// regexp_vs_wildcard_bench/RegexpVsWildcardBenchTestCase.europarl_big/mmap___1_5simd

//  Regexp vs Wildcard benchmark
//  Dataset: europarl.subset.big.txt
//  Directory: mmap
//  Field: body_anl

//   prefix: comm%  vs  comm.*
//   docs matched: 1628
//   wildcard "comm%"
//     prepare: 124.2 us
//     execute: 105.2 us
//   regexp    "comm.*"
//     prepare: 111.7 us
//     execute: 99.2 us

//   suffix: %tion  vs  .*tion
//   docs matched: 2045
//   wildcard "%tion"
//     prepare: 28459.9 us
//     execute: 417.2 us
//   regexp    ".*tion"
//     prepare: 27307.8 us
//     execute: 397.4 us

//   infix: %ment%  vs  .*ment.*
//   docs matched: 4895
//   wildcard "%ment%"
//     prepare: 29793.1 us
//     execute: 954.7 us
//   regexp    ".*ment.*"
//     prepare: 29715.5 us
//     execute: 982.1 us

//   single char: c_t  vs  c.t
//   docs matched: 117
//   wildcard "c_t"
//     prepare: 184.7 us
//     execute: 15.1 us
//   regexp    "c.t"
//     prepare: 178.2 us
//     execute: 13.6 us

//   complex: %un_t%  vs  .*un.t.*
//   docs matched: 1078
//   wildcard "%un_t%"
//     prepare: 26829.0 us
//     execute: 180.8 us
//   regexp    ".*un.t.*"
//     prepare: 26676.2 us
//     execute: 188.6 us

//   prefix long: legislat%  vs  legislat.*
//   docs matched: 235
//   wildcard "legislat%"
//     prepare: 44.3 us
//     execute: 23.3 us
//   regexp    "legislat.*"
//     prepare: 41.6 us
//     execute: 22.1 us

//   match all: %  vs  .*
//   docs matched: 10250
//   wildcard "%"
//     prepare: 76840.8 us
//     execute: 37287.7 us
//   regexp    ".*"
//     prepare: 76770.2 us
//     execute: 36855.2 us

// [       OK ]
// regexp_vs_wildcard_bench/RegexpVsWildcardBenchTestCase.europarl_big/mmap___1_5simd
// (75756 ms)
// [----------] 1 test from
// regexp_vs_wildcard_bench/RegexpVsWildcardBenchTestCase (75756 ms total)

// [----------] Global test environment tear-down
// [==========] 1 test from 1 test suite ran. (75756 ms total)
// [  PASSED  ] 1 test.
