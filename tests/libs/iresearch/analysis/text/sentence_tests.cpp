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
///
/// Conformance gate for the vendored StringZilla sentence and newline
/// engines (third_party/stringzilla) against UCD 17 SentenceBreakTest.txt.
////////////////////////////////////////////////////////////////////////////////

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "break_test_utils.hpp"
#include "gtest/gtest.h"
#include "iresearch/analysis/text/sz/stringzilla.hpp"
#include "tests_config.hpp"

namespace {

using Span = std::pair<size_t, size_t>;
using Finder = size_t (*)(sz_cptr_t, sz_size_t, sz_size_t*, sz_size_t*,
                          sz_size_t, sz_size_t*);

std::vector<Span> Matches(Finder finder, std::string_view text) {
  std::vector<Span> out;
  size_t starts[16];
  size_t lengths[16];
  size_t offset = 0;
  while (offset < text.size()) {
    size_t consumed = 0;
    const size_t count = finder(text.data() + offset, text.size() - offset,
                                starts, lengths, 16, &consumed);
    for (size_t k = 0; k < count; ++k) {
      out.emplace_back(offset + starts[k], offset + starts[k] + lengths[k]);
    }
    if (consumed == 0) {
      break;
    }
    offset += consumed;
  }
  return out;
}

void ExpectBackendsMatch(Finder serial, Finder simd, std::string_view corpus) {
  EXPECT_EQ(Matches(serial, corpus), Matches(simd, corpus));
}

std::string LoadCorpus() {
  std::ifstream in(IRS_TEST_RESOURCE_DIR "/unicode/SentenceBreakTest.txt");
  EXPECT_TRUE(in.is_open());
  std::string corpus;
  std::string line;
  while (std::getline(in, line)) {
    corpus += line;
    corpus += '\n';
  }
  return corpus;
}

}  // namespace

TEST(sentence_engine_test, sentence_break_test_conformance) {
  const auto cases = tests::LoadBreakTestCases(
    IRS_TEST_RESOURCE_DIR "/unicode/SentenceBreakTest.txt");
  ASSERT_GT(cases.size(), 400u);
  size_t failures = 0;
  for (const auto& c : cases) {
    std::vector<Span> expected;
    for (size_t k = 0; k + 1 < c.boundaries.size(); ++k) {
      expected.emplace_back(c.boundaries[k], c.boundaries[k + 1]);
    }
    const auto actual = Matches(sz_utf8_sentences_serial, c.bytes);
    if (actual != expected) {
      ++failures;
      EXPECT_EQ(expected, actual) << "line: " << c.line;
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(sentence_engine_test, sentences_simd_backend_matches_serial) {
  ExpectBackendsMatch(sz_utf8_sentences_serial, sz_utf8_sentences_haswell,
                      LoadCorpus());
}

TEST(sentence_engine_test, newlines_simd_backend_matches_serial) {
  const std::string corpus =
    LoadCorpus() + "mixed\r\nnel\xC2\x85ls\xE2\x80\xA8ps\xE2\x80\xA9tail";
  ExpectBackendsMatch(sz_utf8_newlines_serial, sz_utf8_newlines_haswell,
                      corpus);
}
