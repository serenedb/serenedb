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

#include <gtest/gtest.h>

#include <iresearch/analysis/text/case/case.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace {

using irs::analysis::casing::CaseConvertUtf8;
using irs::analysis::casing::CaseConvertUtf8Bound;

template<bool ToLower>
std::string Reference(std::string_view in) {
  std::string out;
  const auto* it = reinterpret_cast<const irs::byte_type*>(in.data());
  const auto* end = it + in.size();
  while (it != end) {
    const auto* start = it;
    uint32_t cp = irs::utf8_utils::ToChar32(it, end);
    if (cp == irs::utf8_utils::kInvalidChar32) {
      out.push_back(static_cast<char>(*start));
      continue;
    }
    cp = ToLower ? irs::utf8_utils::CharToLowerSimple(cp)
                 : irs::utf8_utils::CharToUpperSimple(cp);
    irs::byte_type buf[irs::utf8_utils::kMaxCharSize];
    const auto n = irs::utf8_utils::FromChar32(cp, buf);
    out.append(reinterpret_cast<const char*>(buf), n);
  }
  return out;
}

template<bool ToLower>
std::string Convert(std::string_view in) {
  std::string out(CaseConvertUtf8Bound(in.size()), '\xAA');
  const auto n =
    CaseConvertUtf8<ToLower>(in, reinterpret_cast<irs::byte_type*>(out.data()));
  EXPECT_LE(n, out.size()) << in;
  out.resize(n);
  return out;
}

void Check(std::string_view in) {
  ASSERT_EQ(Reference<true>(in), Convert<true>(in)) << in;
  ASSERT_EQ(Reference<false>(in), Convert<false>(in)) << in;
}

const std::vector<std::string_view> kPieces{
  "A",
  "z",
  "Mm",
  "Kn0",
  "\xC3\x84",
  "\xC3\xA9",
  "\xCE\xA9",
  "\xCF\x89",
  "\xD0\x94",
  "\xD1\x84",
  "\xD4\xB1",
  "\xE1\x83\x90",
  "\xE1\x8E\xA0",
  "\xEA\xAD\xB0",
  "\xF0\x90\x90\x80",
  "\xF0\x9E\xA4\x80",
  "\xF0\x9F\x98\x80",
  "\xFF",
  "\xE0\x41",
  "\xC3",
};

}  // namespace

TEST(case_convert_utf8_test, ascii_runs_across_block_boundaries) {
  const std::string ascii =
    "The Quick BROWN fox Jumps over the lazy dog 0123456789 ABCDEFGHIJKLMNOP";
  for (size_t n = 0; n <= ascii.size(); ++n) {
    Check(std::string_view{ascii}.substr(0, n));
  }
  for (size_t n = 0; n <= 70; ++n) {
    Check(ascii.substr(0, n) + "\xCE\xA9" + ascii.substr(0, n));
    Check("\xD0\x94" + ascii.substr(0, n) + "\xD0\xB4");
  }
}

TEST(case_convert_utf8_test, non_ascii_only) {
  Check("\xD0\x91\xD1\x8B\xD1\x81\xD1\x82\xD1\x80\xD0\xB0\xD1\x8F");
  Check("\xCE\x91\xCE\xBB\xCF\x86\xCE\xAC\xCE\xB2\xCE\xB7\xCF\x84\xCE\xBF");
  Check("\xE1\x8E\xA0\xE1\x8E\xA1\xEA\xAD\xB0\xF0\x90\x90\x80\xF0\x9E\xA4\x80");
}

TEST(case_convert_utf8_test, invalid_bytes_pass_through) {
  Check("\xFF");
  Check("abc\xFFxyz");
  Check(std::string(40, 'A') + "\xE0\x41" + std::string(40, 'b'));
  Check(std::string(31, 'A') + "\xC3");
  Check(std::string(32, 'A') + "\xC3");
}

TEST(case_convert_utf8_test, random_mixes_match_reference) {
  std::mt19937 rng{42};
  for (size_t iteration = 0; iteration < 5000; ++iteration) {
    std::string in;
    const size_t pieces = rng() % 48;
    for (size_t i = 0; i < pieces; ++i) {
      if (rng() % 3 == 0) {
        in.append(rng() % 40, static_cast<char>('A' + rng() % 26));
      } else {
        in.append(kPieces[rng() % kPieces.size()]);
      }
    }
    Check(in);
  }
}
