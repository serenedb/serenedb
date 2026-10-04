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

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <text_break_iterator.hpp>
#include <text_casing.hpp>
#include <text_locale.hpp>
#include <text_normalizer.hpp>
#include <text_utf8.hpp>
#include <unicode_properties.hpp>
#include <utility>
#include <vector>

#include "break_test_utils.hpp"
#include "tests_config.hpp"

namespace {

using duckdb::text::BreakIterator;
using duckdb::text::BreakKind;
using duckdb::text::BreakUnits;
using duckdb::text::CaseFolding;
using duckdb::text::CaseLocale;
using duckdb::text::CaseMap;
using duckdb::text::Locale;
using duckdb::text::NormalizationForm;
using duckdb::text::Normalizer;
using duckdb::text::PropertyRange;
using duckdb::text::UnicodeProperties;

using Boundaries = std::vector<std::pair<int64_t, int32_t>>;

Boundaries Break(BreakIterator& it, std::string_view text) {
  it.SetText(text.data(), text.size());
  Boundaries out;
  for (auto end = it.Next(); end != BreakIterator::DONE; end = it.Next()) {
    out.emplace_back(end, it.GetRuleStatus());
  }
  return out;
}

std::vector<int64_t> Positions(BreakIterator& it, std::string_view text) {
  std::vector<int64_t> out;
  for (const auto& [position, status] : Break(it, text)) {
    out.push_back(position);
  }
  return out;
}

std::string Lower(CaseLocale locale, std::string_view text) {
  std::string out;
  CaseMap::ToLower(locale, text, out);
  return out;
}

std::string Upper(CaseLocale locale, std::string_view text) {
  std::string out;
  CaseMap::ToUpper(locale, text, out);
  return out;
}

std::string Fold(CaseFolding folding, std::string_view text) {
  std::string out;
  CaseMap::Fold(folding, text, out);
  return out;
}

std::string Normalize(NormalizationForm form, std::string_view text) {
  std::string out;
  Normalizer::Normalize(form, text, out);
  return out;
}

bool Contains(const std::vector<PropertyRange>& ranges, uint32_t c) {
  for (const auto& range : ranges) {
    if (range.first <= c && c <= range.last) {
      return true;
    }
  }
  return false;
}

std::string Utf8(uint32_t c) {
  std::string out;
  duckdb::text::AppendUtf8(&c, 1, out);
  return out;
}

std::string_view Trim(std::string_view field) {
  const auto begin = field.find_first_not_of(' ');
  if (begin == std::string_view::npos) {
    return {};
  }
  return field.substr(begin, field.find_last_not_of(' ') + 1 - begin);
}

std::string HexToUtf8(std::string_view field) {
  std::string out;
  std::istringstream in{std::string{field}};
  std::string hex;
  while (in >> hex) {
    out += Utf8(static_cast<uint32_t>(std::stoul(hex, nullptr, 16)));
  }
  return out;
}

std::vector<std::vector<std::string>> LoadUcdRows(const char* path) {
  std::ifstream in(path);
  EXPECT_TRUE(in.is_open()) << path;
  std::vector<std::vector<std::string>> rows;
  std::string line;
  while (std::getline(in, line)) {
    if (const auto pos = line.find('#'); pos != std::string::npos) {
      line.resize(pos);
    }
    if (line.find(';') == std::string::npos) {
      continue;
    }
    std::vector<std::string> fields;
    std::istringstream ss(line);
    std::string field;
    while (std::getline(ss, field, ';')) {
      fields.push_back(field);
    }
    rows.push_back(std::move(fields));
  }
  return rows;
}

void CheckBreakTestConformance(BreakKind kind,
                               const std::vector<tests::BreakTestCase>& cases) {
  for (const auto units : {BreakUnits::UTF8, BreakUnits::UTF16}) {
    BreakIterator it{kind, Locale::FromName("en"), units};
    size_t failures = 0;
    for (const auto& c : cases) {
      const std::vector<int64_t> expected(c.boundaries.begin() + 1,
                                          c.boundaries.end());
      const auto actual = Positions(it, c.bytes);
      if (actual != expected) {
        ++failures;
        EXPECT_EQ(expected, actual) << "line: " << c.line;
        continue;
      }
      for (size_t k = 1; k < c.boundaries.size(); ++k) {
        const auto preceding = it.Preceding(c.boundaries[k]);
        if (preceding != c.boundaries[k - 1]) {
          ++failures;
          EXPECT_EQ(c.boundaries[k - 1], preceding)
            << "preceding " << c.boundaries[k] << " line: " << c.line;
        }
      }
    }
    EXPECT_EQ(0u, failures);
  }
}

struct NormalizationTestCase {
  std::string columns[5];
  std::string line;
};

std::vector<NormalizationTestCase> LoadNormalizationTestCases(
  std::vector<bool>& part1) {
  std::ifstream in(IRS_TEST_RESOURCE_DIR "/unicode/NormalizationTest.txt");
  EXPECT_TRUE(in.is_open());
  std::vector<NormalizationTestCase> cases;
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
    NormalizationTestCase c;
    c.line = line;
    std::istringstream fields(line);
    std::string field;
    size_t column = 0;
    while (column < 5 && std::getline(fields, field, ';')) {
      if (column == 0 && in_part1) {
        part1[std::stoul(field, nullptr, 16)] = true;
      }
      c.columns[column] = HexToUtf8(field);
      ++column;
    }
    if (column == 5) {
      cases.push_back(std::move(c));
    }
  }
  return cases;
}

}  // namespace

TEST(UnicodeLocaleTest, canonical_names) {
  const std::pair<std::string_view, std::string_view> kNames[] = {
    {"EN-us", "en_US"},
    {"tur", "tr"},
    {"de-DE", "de_DE"},
    {"sr_latn_rs", "sr_Latn_RS"},
    {"C.UTF-8", "c.UTF-8"},
    {"en@ss=standard;Collation=Phonebook",
     "en@collation=Phonebook;ss=standard"},
    {"es_419", "es_419"},
    {"de__phonebook", "de__PHONEBOOK"},
    {"en_US_POSIX", "en_US_POSIX"},
  };
  for (const auto& [name, canonical] : kNames) {
    SCOPED_TRACE(name);
    const auto locale = Locale::FromName(name);
    ASSERT_FALSE(locale.IsBogus());
    ASSERT_EQ(canonical, locale.GetName());
  }
  const auto locale = Locale::FromName("en_US.UTF-8@ss=standard");
  EXPECT_EQ("en", locale.GetLanguage());
  EXPECT_EQ("US", locale.GetRegion());
  EXPECT_EQ("en_US.UTF-8", locale.GetBaseName());
  EXPECT_EQ("standard", locale.GetKeyword("ss"));
  EXPECT_TRUE(Locale::FromName("en-u-ss-standard").IsBogus());
  EXPECT_TRUE(Locale{}.IsBogus());
  EXPECT_EQ("", Locale{}.GetName());
}

TEST(UnicodeLocaleTest, validation) {
  for (const auto* name :
       {"en", "en_US.UTF-8", "es_419", "de__POSIX", "sr_Latn_RS",
        "en@ss=standard", "tur", "ru_RU.UTF-16"}) {
    Locale locale;
    std::string error;
    EXPECT_TRUE(Locale::TryParse(name, locale, error)) << name << ": " << error;
    EXPECT_FALSE(locale.IsBogus()) << name;
  }
  for (const auto* name :
       {"", "C", "trk", "en_XX", "en-u-ss-standard", "en_US_toolongvariant",
        "en__", "123", "@ss=standard", "definitely_not_a_locale_@@@",
        "tr-TR "}) {
    Locale locale;
    std::string error;
    EXPECT_FALSE(Locale::TryParse(name, locale, error)) << name;
    EXPECT_FALSE(error.empty()) << name;
  }
}

TEST(UnicodeLocaleTest, case_locales) {
  const std::pair<std::string_view, CaseLocale> kLocales[] = {
    {"tr", CaseLocale::TURKISH},     {"tur_TR", CaseLocale::TURKISH},
    {"TR", CaseLocale::TURKISH},     {"az", CaseLocale::TURKISH},
    {"aze-AZ", CaseLocale::TURKISH}, {"lt", CaseLocale::LITHUANIAN},
    {"lit", CaseLocale::LITHUANIAN}, {"el", CaseLocale::GREEK},
    {"ell_GR", CaseLocale::GREEK},   {"nl", CaseLocale::DUTCH},
    {"nld", CaseLocale::DUTCH},      {"hy", CaseLocale::ARMENIAN},
    {"hye", CaseLocale::ARMENIAN},   {"hyw", CaseLocale::ROOT},
    {"tr.UTF-8", CaseLocale::ROOT},  {"el@x=y", CaseLocale::ROOT},
    {"en", CaseLocale::ROOT},        {"", CaseLocale::ROOT},
  };
  for (const auto& [name, expected] : kLocales) {
    SCOPED_TRACE(name);
    ASSERT_EQ(expected, Locale::CaseLocaleOf(name));
  }
  EXPECT_EQ(CaseLocale::TURKISH,
            Locale::FromName("tr_TR.UTF-8").GetCaseLocale());
  EXPECT_EQ(CaseLocale::GREEK, Locale::FromName("el@x=y").GetCaseLocale());
  EXPECT_EQ(CaseLocale::ROOT, Locale{}.GetCaseLocale());
}

TEST(UnicodeLocaleTest, fallback_and_collations) {
  std::string name = "sr_Latn_RS";
  ASSERT_TRUE(Locale::Truncate(name));
  EXPECT_EQ("sr_Latn", name);
  ASSERT_TRUE(Locale::Truncate(name));
  EXPECT_EQ("sr", name);
  ASSERT_TRUE(Locale::Truncate(name));
  EXPECT_EQ("", name);
  EXPECT_FALSE(Locale::Truncate(name));

  const std::pair<std::string_view, std::string_view> kCollations[] = {
    {"en", ""},         {"en_US.UTF-8", ""}, {"de_DE", ""},
    {"sv_SE", "sv"},    {"zh_TW", "zh_tw"},  {"zh_Hant_TW", "zh_tw"},
    {"zh_CN", "zh_cn"}, {"sr_BA", "sr_ba"},  {"fr_CA", "fr_ca"},
  };
  for (const auto& [locale, expected] : kCollations) {
    SCOPED_TRACE(locale);
    std::string collation;
    ASSERT_TRUE(Locale::FromName(locale).GetCollation(collation));
    ASSERT_EQ(expected, collation);
  }
  for (const auto* locale :
       {"de@collation=phonebook", "de__phonebook", "es__traditional", "sr_Latn",
        "en_US_POSIX", "en@colStrength=primary"}) {
    SCOPED_TRACE(locale);
    std::string collation;
    ASSERT_FALSE(Locale::FromName(locale).GetCollation(collation));
  }
}

TEST(UnicodeCaseTest, contextual_and_locale_mappings) {
  EXPECT_EQ("οδος οδος.", Lower(CaseLocale::ROOT, "ΟΔΟΣ ΟΔΟΣ."));
  EXPECT_EQ("σας", Lower(CaseLocale::ROOT, "ΣΑΣ"));
  EXPECT_EQ("istanbul ısparta", Lower(CaseLocale::TURKISH, "İSTANBUL ISPARTA"));
  EXPECT_EQ("i\xCC\x87", Lower(CaseLocale::ROOT, "İ"));
  EXPECT_EQ("i", Lower(CaseLocale::TURKISH, "I\xCC\x87"));
  EXPECT_EQ("i\xCC\x87\xCC\x80", Lower(CaseLocale::LITHUANIAN, "Ì"));
  EXPECT_EQ("i\xCC\x87\xCC\x81", Lower(CaseLocale::LITHUANIAN, "I\xCC\x81"));
  EXPECT_EQ("İSTANBUL", Upper(CaseLocale::TURKISH, "istanbul"));
  EXPECT_EQ("STRASSE", Upper(CaseLocale::ROOT, "straße"));
  EXPECT_EQ("I\xCC\x81", Upper(CaseLocale::LITHUANIAN, "i\xCC\x87\xCC\x81"));
  EXPECT_EQ("ΑΕΗΪΟΫΩ", Upper(CaseLocale::GREEK, "άέήίόύώ"));
  EXPECT_EQ("Ή", Upper(CaseLocale::GREEK, "ή"));
  EXPECT_EQ("Ϊ", Upper(CaseLocale::GREEK, "ΐ"));
  EXPECT_EQ("ΆΈΉΊΌΎΏ", Upper(CaseLocale::ROOT, "άέήίόύώ"));
  EXPECT_EQ("ԵՎ", Upper(CaseLocale::ARMENIAN, "և"));
  EXPECT_EQ("ԵՒ", Upper(CaseLocale::ROOT, "և"));
  EXPECT_EQ("strasse", Fold(CaseFolding::DEFAULT, "Straße"));
  EXPECT_EQ("i\xCC\x87", Fold(CaseFolding::DEFAULT, "İ"));
  EXPECT_EQ("ı", Fold(CaseFolding::TURKIC, "I"));
  EXPECT_EQ("i", Fold(CaseFolding::TURKIC, "İ"));
  EXPECT_EQ("Ꭰ", Fold(CaseFolding::DEFAULT, "ꭰ"));
  EXPECT_EQ("abc\xEF\xBF\xBD", Lower(CaseLocale::ROOT, "ABC\xFF"));
}

TEST(UnicodeCaseTest, case_folding_conformance) {
  absl::flat_hash_map<uint32_t, std::string> full;
  absl::flat_hash_map<uint32_t, std::string> turkic;
  for (const auto& fields :
       LoadUcdRows(IRS_TEST_RESOURCE_DIR "/unicode/CaseFolding.txt")) {
    ASSERT_GE(fields.size(), 3u);
    const auto c = static_cast<uint32_t>(std::stoul(fields[0], nullptr, 16));
    const auto status = Trim(fields[1]);
    if (status == "C" || status == "F") {
      full.emplace(c, HexToUtf8(fields[2]));
    } else if (status == "T") {
      turkic.emplace(c, HexToUtf8(fields[2]));
    }
  }
  ASSERT_EQ(1585u, full.size());
  ASSERT_EQ(2u, turkic.size());
  size_t failures = 0;
  for (uint32_t c = 0; c < 0x110000 && failures <= 20; ++c) {
    if (c >= 0xD800 && c <= 0xDFFF) {
      continue;
    }
    const auto text = Utf8(c);
    const auto it = full.find(c);
    const auto& expected = it == full.end() ? text : it->second;
    if (Fold(CaseFolding::DEFAULT, text) != expected) {
      ++failures;
      EXPECT_EQ(expected, Fold(CaseFolding::DEFAULT, text)) << std::hex << c;
    }
    const auto t = turkic.find(c);
    const auto& expected_turkic = t == turkic.end() ? expected : t->second;
    if (Fold(CaseFolding::TURKIC, text) != expected_turkic) {
      ++failures;
      EXPECT_EQ(expected_turkic, Fold(CaseFolding::TURKIC, text))
        << std::hex << c;
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(UnicodeCaseTest, special_casing_conformance) {
  size_t checked = 0;
  for (const auto& fields :
       LoadUcdRows(IRS_TEST_RESOURCE_DIR "/unicode/SpecialCasing.txt")) {
    ASSERT_GE(fields.size(), 4u);
    const auto condition = fields.size() > 4 ? Trim(fields[4]) : "";
    CaseLocale locale;
    if (condition.empty()) {
      locale = CaseLocale::ROOT;
    } else if (condition == "lt") {
      locale = CaseLocale::LITHUANIAN;
    } else if (condition == "tr" || condition == "az") {
      locale = CaseLocale::TURKISH;
    } else {
      continue;
    }
    const auto text = HexToUtf8(fields[0]);
    EXPECT_EQ(HexToUtf8(fields[1]), Lower(locale, text)) << fields[0];
    EXPECT_EQ(HexToUtf8(fields[3]), Upper(locale, text)) << fields[0];
    ++checked;
  }
  EXPECT_EQ(110u, checked);
}

TEST(UnicodeNormalizerTest, forms) {
  EXPECT_EQ("e\xCC\x81", Normalize(NormalizationForm::NFD, "é"));
  EXPECT_EQ("é", Normalize(NormalizationForm::NFC, "e\xCC\x81"));
  EXPECT_EQ("fi", Normalize(NormalizationForm::NFKC, "ﬁ"));
  EXPECT_EQ("1", Normalize(NormalizationForm::NFKD, "①"));
  EXPECT_EQ("full strasse fi",
            Normalize(NormalizationForm::NFKC_CF, "ＦＵＬＬ Straße ﬁ"));
  EXPECT_EQ("각", Normalize(NormalizationForm::NFC,
                            "\xE1\x84\x80\xE1\x85\xA1\xE1\x86\xA8"));
  EXPECT_EQ("\xE1\x84\x80\xE1\x85\xA1\xE1\x86\xA8",
            Normalize(NormalizationForm::NFD, "각"));
  EXPECT_EQ("a\xCC\x96\xCC\x81",
            Normalize(NormalizationForm::NFD, "a\xCC\x81\xCC\x96"));
  EXPECT_EQ("\xC3\xA1\xCC\x96",
            Normalize(NormalizationForm::NFC, "a\xCC\x81\xCC\x96"));
  EXPECT_EQ("abc", Normalize(NormalizationForm::NFKC_CF, "ABC"));
  EXPECT_TRUE(Normalizer::IsNonspacingMark(0x301));
  EXPECT_FALSE(Normalizer::IsNonspacingMark('a'));
  EXPECT_EQ(230, Normalizer::CombiningClass(0x301));
  EXPECT_EQ(220, Normalizer::CombiningClass(0x316));
}

TEST(UnicodeNormalizerTest, normalization_test_conformance) {
  std::vector<bool> part1(0x110000);
  const auto cases = LoadNormalizationTestCases(part1);
  ASSERT_EQ(20034u, cases.size());
  size_t failures = 0;
  std::vector<uint32_t> chars;
  std::vector<uint32_t> scratch;
  const auto check = [&](const std::string& expected, const std::string& input,
                         NormalizationForm form,
                         const NormalizationTestCase& c) {
    const auto actual = Normalize(form, input);
    if (actual != expected) {
      ++failures;
      EXPECT_EQ(expected, actual)
        << "form: " << static_cast<int>(form) << " line: " << c.line;
    }
    duckdb::text::DecodeUtf8(input, chars);
    const bool normalized =
      Normalizer::IsNormalized(form, chars.data(), chars.size(), scratch);
    if (normalized != (input == expected)) {
      ++failures;
      EXPECT_EQ(input == expected, normalized)
        << "form: " << static_cast<int>(form) << " line: " << c.line;
    }
  };
  for (const auto& c : cases) {
    const auto& [c1, c2, c3, c4, c5] = c.columns;
    for (const auto* input : {&c1, &c2, &c3}) {
      check(c2, *input, NormalizationForm::NFC, c);
      check(c3, *input, NormalizationForm::NFD, c);
    }
    for (const auto* input : {&c4, &c5}) {
      check(c4, *input, NormalizationForm::NFC, c);
      check(c5, *input, NormalizationForm::NFD, c);
    }
    for (const auto* input : {&c1, &c2, &c3, &c4, &c5}) {
      check(c4, *input, NormalizationForm::NFKC, c);
      check(c5, *input, NormalizationForm::NFKD, c);
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(UnicodeNormalizerTest, part1_unlisted_code_points_are_inert) {
  std::vector<bool> part1(0x110000);
  ASSERT_FALSE(LoadNormalizationTestCases(part1).empty());
  size_t failures = 0;
  for (uint32_t c = 0; c < 0x110000 && failures <= 20; ++c) {
    if (part1[c] || (c >= 0xD800 && c <= 0xDFFF)) {
      continue;
    }
    const auto text = Utf8(c);
    for (const auto form : {NormalizationForm::NFC, NormalizationForm::NFD,
                            NormalizationForm::NFKC, NormalizationForm::NFKD}) {
      if (Normalize(form, text) != text) {
        ++failures;
        ADD_FAILURE() << "cp: " << std::hex << c
                      << " form: " << static_cast<int>(form);
      }
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(UnicodeNormalizerTest, nfkc_casefold_is_canonically_closed) {
  const std::vector<std::vector<uint32_t>> tails{
    {}, {0x301}, {0x323, 0x301}, {0x308, 0x301}, {0x1161}, {0x11A8}, {0x3099}};
  std::vector<uint32_t> text;
  std::vector<uint32_t> decomposed;
  std::vector<uint32_t> expected;
  std::vector<uint32_t> actual;
  size_t failures = 0;
  for (uint32_t c = 0; c < 0x110000 && failures <= 20; ++c) {
    if (c >= 0xD800 && c <= 0xDFFF) {
      continue;
    }
    Normalizer::Normalize(NormalizationForm::NFD, &c, 1, decomposed);
    if (std::ranges::contains(decomposed, 0x345u)) {
      continue;
    }
    for (const auto& tail : tails) {
      text.assign(1, c);
      text.insert(text.end(), tail.begin(), tail.end());
      Normalizer::Normalize(NormalizationForm::NFD, text.data(), text.size(),
                            decomposed);
      Normalizer::Normalize(NormalizationForm::NFKC_CF, decomposed.data(),
                            decomposed.size(), expected);
      Normalizer::Normalize(NormalizationForm::NFKC_CF, text.data(),
                            text.size(), actual);
      if (actual != expected) {
        ++failures;
        ADD_FAILURE() << "cp: " << std::hex << c << " tail: " << tail.size();
      }
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(UnicodeNormalizerTest, nfkc_boundaries_split_normalization) {
  std::vector<bool> part1(0x110000);
  const auto cases = LoadNormalizationTestCases(part1);
  ASSERT_FALSE(cases.empty());
  size_t failures = 0;
  std::vector<uint32_t> chars;
  std::vector<uint32_t> whole;
  std::vector<uint32_t> head;
  std::vector<uint32_t> tail;
  for (const auto& c : cases) {
    for (const auto& column : c.columns) {
      duckdb::text::DecodeUtf8(column, chars);
      Normalizer::Normalize(NormalizationForm::NFKC, chars.data(), chars.size(),
                            whole);
      for (size_t i = 1; i < chars.size(); ++i) {
        if (!Normalizer::HasNfkcBoundaryBefore(chars[i])) {
          continue;
        }
        Normalizer::Normalize(NormalizationForm::NFKC, chars.data(), i, head);
        Normalizer::Normalize(NormalizationForm::NFKC, chars.data() + i,
                              chars.size() - i, tail);
        head.insert(head.end(), tail.begin(), tail.end());
        if (head != whole) {
          ++failures;
          ADD_FAILURE() << "split at " << i << " line: " << c.line;
        }
      }
    }
  }
  EXPECT_EQ(0u, failures);
}

TEST(UnicodeBreakIteratorTest, words) {
  for (const auto units : {BreakUnits::UTF8, BreakUnits::UTF16}) {
    BreakIterator it{BreakKind::WORD, Locale{}, units};
    EXPECT_FALSE(it.IsTailored());
    EXPECT_EQ((Boundaries{{5, 200}, {6, 0}, {7, 0}, {12, 200}, {13, 0}}),
              Break(it, "Hello, world!"));
    EXPECT_EQ((std::vector<int64_t>{12, 21, 36, 45, 57, 63}),
              Positions(it, "ภาษาไทยทดสอบการแบ่งคำ"));
    for (const auto& [position, status] : Break(it, "ภาษาไทยทดสอบการแบ่งคำ")) {
      EXPECT_NE(duckdb::text::WORD_NONE, status) << position;
    }
    EXPECT_TRUE(Break(it, "").empty());
  }
  EXPECT_TRUE((BreakIterator{BreakKind::WORD, Locale::FromName("en_US_POSIX"),
                             BreakUnits::UTF16}
                 .IsTailored()));
  EXPECT_FALSE((
    BreakIterator{BreakKind::WORD, Locale::FromName("en_US"), BreakUnits::UTF16}
      .IsTailored()));
  BreakIterator posix{BreakKind::WORD, Locale::FromName("en_US_POSIX"),
                      BreakUnits::UTF16};
  EXPECT_EQ((std::vector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 12}),
            Positions(posix, "x.y a:b 3.14"));
  BreakIterator root{BreakKind::WORD, Locale{}, BreakUnits::UTF16};
  EXPECT_EQ((std::vector<int64_t>{3, 4, 7, 8, 12}),
            Positions(root, "x.y a:b 3.14"));
}

TEST(UnicodeBreakIteratorTest, sentences) {
  BreakIterator it{BreakKind::SENTENCE, Locale{}, BreakUnits::UTF8};
  EXPECT_EQ((std::vector<int64_t>{13, 29}),
            Positions(it, "Hello world. Second sentence!"));
  EXPECT_EQ((std::vector<int64_t>{4, 19, 31}),
            Positions(it, "Mr. Smith arrived. He sat down."));
  BreakIterator suppressed{
    BreakKind::SENTENCE, Locale::FromName("en@ss=standard"), BreakUnits::UTF8};
  EXPECT_EQ((std::vector<int64_t>{19, 31}),
            Positions(suppressed, "Mr. Smith arrived. He sat down."));
  BreakIterator german{BreakKind::SENTENCE, Locale::FromName("de@ss=standard"),
                       BreakUnits::UTF8};
  EXPECT_EQ((std::vector<int64_t>{34}),
            Positions(german, "Das ist z.B. ein Test. Noch einer."));
  EXPECT_TRUE((BreakIterator{BreakKind::SENTENCE, Locale::FromName("el_GR"),
                             BreakUnits::UTF8}
                 .IsTailored()));
}

TEST(UnicodeBreakIteratorTest, word_break_test_conformance) {
  const auto cases = tests::LoadBreakTestCases(IRS_TEST_RESOURCE_DIR
                                               "/unicode/WordBreakTest.txt");
  ASSERT_EQ(1944u, cases.size());
  CheckBreakTestConformance(BreakKind::WORD, cases);
}

TEST(UnicodeBreakIteratorTest, sentence_break_test_conformance) {
  const auto cases = tests::LoadBreakTestCases(
    IRS_TEST_RESOURCE_DIR "/unicode/SentenceBreakTest.txt");
  ASSERT_EQ(512u, cases.size());
  CheckBreakTestConformance(BreakKind::SENTENCE, cases);
}

TEST(UnicodeBreakIteratorTest, preceding) {
  BreakIterator it{BreakKind::SENTENCE, Locale{}, BreakUnits::UTF8};
  constexpr std::string_view kText = "One. Two. Three.";
  it.SetText(kText.data(), kText.size());
  EXPECT_EQ(5, it.Preceding(6));
  EXPECT_EQ(10, it.Next());
  EXPECT_EQ(0, it.Preceding(5));
  EXPECT_EQ(5, it.Next());
  EXPECT_EQ(BreakIterator::DONE, it.Preceding(0));
  EXPECT_EQ(5, it.Next());
  EXPECT_EQ(16, it.Preceding(100));
  EXPECT_EQ(BreakIterator::DONE, it.Next());

  constexpr std::string_view kWide = "\xC3\x9Cn\xC3\xAB. Tw\xC3\xB6.";
  it.SetText(kWide.data(), kWide.size());
  EXPECT_EQ(BreakIterator::DONE, it.Preceding(1));
  EXPECT_EQ(7, it.Preceding(8));
  EXPECT_EQ(7, it.Preceding(10));
  EXPECT_EQ(12, it.Next());
}

TEST(UnicodePropertiesTest, lookups) {
  std::vector<PropertyRange> ranges;
  ASSERT_TRUE(UnicodeProperties::Lookup("L", ranges));
  EXPECT_TRUE(Contains(ranges, 'a'));
  EXPECT_FALSE(Contains(ranges, '1'));
  ASSERT_TRUE(UnicodeProperties::Lookup("gc=Lu", ranges));
  EXPECT_TRUE(Contains(ranges, 'A'));
  EXPECT_FALSE(Contains(ranges, 'a'));
  ASSERT_TRUE(UnicodeProperties::Lookup(" lower case ", ranges));
  EXPECT_TRUE(Contains(ranges, 'a'));
  ASSERT_TRUE(UnicodeProperties::Lookup("Alphabetic=No", ranges));
  EXPECT_TRUE(Contains(ranges, '1'));
  EXPECT_FALSE(Contains(ranges, 'a'));
  ASSERT_TRUE(UnicodeProperties::Lookup("Script=Greek", ranges));
  EXPECT_TRUE(Contains(ranges, 0x3B1));
  ASSERT_TRUE(UnicodeProperties::Lookup("scx=Grek", ranges));
  EXPECT_TRUE(Contains(ranges, 0x3B1));
  ASSERT_TRUE(UnicodeProperties::Lookup("Any", ranges));
  EXPECT_EQ(1, ranges.size());
  EXPECT_EQ(0x10FFFF, ranges[0].last);
  ASSERT_TRUE(UnicodeProperties::Lookup("ASCII", ranges));
  ASSERT_EQ(1, ranges.size());
  EXPECT_EQ(0x7F, ranges[0].last);
  ASSERT_TRUE(UnicodeProperties::Lookup("ccc=0x10", ranges));
  ASSERT_EQ(1, ranges.size());
  EXPECT_EQ(0x5B6, ranges[0].first);
  EXPECT_EQ(0x5B6, ranges[0].last);
  ASSERT_TRUE(UnicodeProperties::Lookup("Age=V3_2", ranges));
  EXPECT_TRUE(ranges.empty());
  ASSERT_TRUE(UnicodeProperties::Lookup("Age=1.1", ranges));
  EXPECT_TRUE(Contains(ranges, 'a'));
  EXPECT_FALSE(Contains(ranges, 0x20AC));
  ASSERT_TRUE(UnicodeProperties::Lookup("nv=0.5", ranges));
  EXPECT_TRUE(Contains(ranges, 0xBD));
  for (const auto* invalid :
       {"nonsense", "Name=LATIN SMALL LETTER A", "InSC=Consonant", "InPC", "L#",
        "Ll\xC3\xA9", "isL", "Script", "nv=1/2", "ccc=256", ""}) {
    EXPECT_FALSE(UnicodeProperties::Lookup(invalid, ranges)) << invalid;
  }
}
