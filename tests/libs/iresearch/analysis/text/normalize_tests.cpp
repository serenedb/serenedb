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
/// Conformance gate for the vendored StringZilla UAX#15 engine
/// (third_party/stringzilla) against UCD 17 NormalizationTest.txt.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>
#include <stringzilla/utf8_norm/haswell.h>
#include <stringzilla/utf8_norm/icelake.h>
#include <stringzilla/utf8_norm/serial.h>
#include <stringzilla/utf8_norm/skylake.h>

#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text/normalize/normalize.hpp"
#include "iresearch/analysis/text/sz/stringzilla.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "tests_config.hpp"

namespace {

constexpr sz_normal_form_t kForms[] = {
  sz_normal_form_nfc_k, sz_normal_form_nfd_k, sz_normal_form_nfkc_k,
  sz_normal_form_nfkd_k};

std::string Normalize(std::string_view text, sz_normal_form_t form) {
  std::string out;
  out.resize(64 + text.size() * 18);
  const auto n =
    sz_utf8_norm_serial(text.data(), text.size(), form, out.data());
  out.resize(n);
  return out;
}

bool IsNormalized(std::string_view text, sz_normal_form_t form) {
  return sz_utf8_find_denormalized_serial(text.data(), text.size(), form) ==
         nullptr;
}

struct Case {
  std::string columns[5];
  std::string line;
  bool part1;
};

std::vector<Case> LoadCases(std::vector<bool>& part1_cps) {
  std::ifstream in(IRS_TEST_RESOURCE_DIR "/unicode/NormalizationTest.txt");
  EXPECT_TRUE(in.is_open());
  std::vector<Case> cases;
  std::string line;
  bool in_part1 = false;
  while (std::getline(in, line)) {
    if (line.starts_with("@Part")) {
      in_part1 = line.starts_with("@Part1");
      continue;
    }
    if (const auto pos = line.find('#'); pos != std::string::npos) {
      line.resize(pos);
    }
    if (line.empty()) {
      continue;
    }
    Case c;
    c.line = line;
    c.part1 = in_part1;
    std::istringstream fields(line);
    std::string field;
    size_t column = 0;
    while (column < 5 && std::getline(fields, field, ';')) {
      std::istringstream cps(field);
      std::string cp_hex;
      while (cps >> cp_hex) {
        const auto cp = static_cast<uint32_t>(std::stoul(cp_hex, nullptr, 16));
        if (column == 0 && in_part1) {
          part1_cps[cp] = true;
        }
        irs::byte_type buf[irs::utf8_utils::kMaxCharSize];
        const auto len = irs::utf8_utils::FromChar32(cp, buf);
        c.columns[column].append(reinterpret_cast<const char*>(buf), len);
      }
      ++column;
    }
    if (column == 5) {
      cases.push_back(std::move(c));
    }
  }
  return cases;
}

TEST(norm_stringzilla_test, normalization_test_conformance) {
  std::vector<bool> part1_cps(0x110000);
  const auto cases = LoadCases(part1_cps);
  ASSERT_GT(cases.size(), 18000u);
  size_t failures = 0;
  const auto check = [&](const std::string& expected, const std::string& input,
                         sz_normal_form_t form, const Case& c) {
    const auto actual = Normalize(input, form);
    if (actual != expected) {
      ++failures;
      EXPECT_EQ(expected, actual) << "form: " << form << " line: " << c.line;
    }
    const bool already = IsNormalized(input, form);
    if (already != (input == expected)) {
      ++failures;
      EXPECT_EQ(input == expected, already)
        << "find_denormalized disagrees, form: " << form << " line: " << c.line;
    }
  };
  for (const auto& c : cases) {
    const auto& [c1, c2, c3, c4, c5] = c.columns;
    for (const auto* input : {&c1, &c2, &c3}) {
      check(c2, *input, sz_normal_form_nfc_k, c);
      check(c3, *input, sz_normal_form_nfd_k, c);
    }
    for (const auto* input : {&c4, &c5}) {
      check(c4, *input, sz_normal_form_nfc_k, c);
      check(c5, *input, sz_normal_form_nfd_k, c);
    }
    for (const auto* input : {&c1, &c2, &c3, &c4, &c5}) {
      check(c4, *input, sz_normal_form_nfkc_k, c);
      check(c5, *input, sz_normal_form_nfkd_k, c);
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(norm_stringzilla_test, part1_unlisted_codepoints_are_normalization_inert) {
  std::vector<bool> part1_cps(0x110000);
  const auto cases = LoadCases(part1_cps);
  ASSERT_FALSE(cases.empty());
  size_t failures = 0;
  for (uint32_t cp = 0; cp < 0x110000; ++cp) {
    if (part1_cps[cp] || (cp >= 0xD800 && cp <= 0xDFFF)) {
      continue;
    }
    irs::byte_type buf[irs::utf8_utils::kMaxCharSize];
    const auto len = irs::utf8_utils::FromChar32(cp, buf);
    const std::string_view text{reinterpret_cast<const char*>(buf), len};
    for (const auto form : kForms) {
      if (!IsNormalized(text, form) || Normalize(text, form) != text) {
        ++failures;
        EXPECT_TRUE(false) << "cp: " << std::hex << cp << " form: " << form;
      }
    }
    if (failures > 20) {
      break;
    }
  }
  EXPECT_EQ(0u, failures);
}

template<sz_normal_form_t Form>
void CheckClassifyAndStripSafe() {
  std::vector<bool> part1_cps(0x110000);
  const auto cases = LoadCases(part1_cps);
  ASSERT_GT(cases.size(), 18000u);
  std::string buf_a;
  std::string buf_b;
  const auto decompose = [](std::string_view in, std::string& out) {
    out.resize(irs::analysis::normalize::Bound<Form>(in.size()));
    out.resize(irs::analysis::normalize::Decompose<Form>(in, out.data()));
  };
  const auto compose = [](std::string_view in, std::string& out) {
    out.resize(irs::analysis::normalize::Bound<Form>(in.size()));
    out.resize(irs::analysis::normalize::Compose<Form>(in, out.data()));
  };
  size_t failures = 0;
  size_t strip_safe = 0;
  for (const auto& c : cases) {
    for (const auto& s : c.columns) {
      const bool normalized = IsNormalized(s, Form);
      const bool denormalized =
        irs::analysis::normalize::Denormalized<Form>(s.data(), s.size());
      if (normalized != !denormalized) {
        ++failures;
        EXPECT_EQ(normalized, !denormalized) << "line: " << c.line;
      }
      if (!irs::analysis::normalize::StripSafe<Form>(s.data(), s.size())) {
        continue;
      }
      ++strip_safe;
      decompose(s, buf_a);
      irs::analysis::normalize::StripNonspacingMarks(buf_a, buf_b);
      compose(buf_b, buf_a);
      if (buf_a != s || !normalized) {
        ++failures;
        EXPECT_TRUE(false) << "StripSafe not an identity, line: " << c.line;
      }
    }
  }
  EXPECT_EQ(0u, failures);
  EXPECT_GT(strip_safe, 0u);
}

TEST(norm_stringzilla_test, classify_nfc_and_strip_safe_conformance) {
  CheckClassifyAndStripSafe<sz_normal_form_nfc_k>();
}

TEST(norm_stringzilla_test, classify_nfkc_and_strip_safe_conformance) {
  CheckClassifyAndStripSafe<sz_normal_form_nfkc_k>();
}

TEST(norm_stringzilla_test, simd_backends_match_serial) {
  const bool has_avx512 = irs::analysis::sz::HasAvx512();
  std::vector<bool> part1_cps(0x110000);
  const auto cases = LoadCases(part1_cps);
  ASSERT_FALSE(cases.empty());
  std::string serial_out;
  std::string simd_out;
  serial_out.resize(1 << 12);
  simd_out.resize(1 << 12);
  size_t failures = 0;
  for (const auto& c : cases) {
    for (const auto* input : {&c.columns[0], &c.columns[3]}) {
      for (const auto form : kForms) {
        const auto serial_len = sz_utf8_norm_serial(
          input->data(), input->size(), form, serial_out.data());
        const auto check_backend = [&](auto kernel, const char* name) {
          const auto simd_len =
            kernel(input->data(), input->size(), form, simd_out.data());
          if (simd_len != serial_len ||
              std::string_view{simd_out.data(), simd_len} !=
                std::string_view{serial_out.data(), serial_len}) {
            ++failures;
            EXPECT_TRUE(false) << name << " diverges, line: " << c.line;
          }
        };
        check_backend(sz_utf8_norm_haswell, "haswell");
        if (has_avx512) {
          check_backend(sz_utf8_norm_skylake, "skylake");
          check_backend(sz_utf8_norm_icelake, "icelake");
        }
      }
    }
    if (failures > 20) {
      break;
    }
  }
  EXPECT_EQ(0u, failures);
}

}  // namespace
