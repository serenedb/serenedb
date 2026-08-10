////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

// Two things live here: `ComputeWildcardType`, the classifier that decides
// which filter a `%`/`_` pattern becomes, and the acceptance suite for the
// wildcard dialect itself.
//
// The dialect is not a regexp with a different spelling: a wildcard pattern is
// arbitrary bytes, every literal byte is taken as itself, and only `%`, `_`
// and `\` mean anything -- which is why a pattern carrying a NUL, a lone
// continuation byte or a truncated multi-byte prefix is expressible here and
// not as a regexp source string. `_` is one whole code point, not one byte.
// Those are the cases a re-implementation is most likely to get subtly wrong,
// so they are pinned byte for byte.

#include <string>
#include <string_view>
#include <utility>

#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/wildcard_utils.hpp"
#include "tests_shared.hpp"

class WildcardUtilsTest : public TestBase {
 protected:
  static irs::bytes_view B(std::string_view sv) {
    return irs::ViewCast<irs::byte_type>(sv);
  }

  static irs::RegexpAcceptor FromWildcard(std::string_view pattern) {
    return irs::RegexpAcceptor{irs::RegexpAcceptor::WildcardTag{}, B(pattern)};
  }

  static bool Accepts(const irs::RegexpAcceptor& a, std::string_view term) {
    return a.Matches(B(term));
  }
};

TEST_F(WildcardUtilsTest, wildcard_type) {
  ASSERT_EQ(irs::WildcardType::Term,
            irs::ComputeWildcardType(irs::ViewCast<irs::byte_type>(
              std::string_view("\xD0"))));  // invalid UTF-8 sequence
  ASSERT_EQ(irs::WildcardType::Term,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo"))));
  ASSERT_EQ(irs::WildcardType::Term,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("\xD0\xE2"))));
  ASSERT_EQ(irs::WildcardType::TermEscaped,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("\\foo"))));
  ASSERT_EQ(irs::WildcardType::TermEscaped,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("\\%foo"))));
  ASSERT_EQ(irs::WildcardType::Term,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("\foo"))));
  ASSERT_EQ(irs::WildcardType::Prefix,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo%"))));
  ASSERT_EQ(irs::WildcardType::TermEscaped,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("\\\\\\\\\\%"))));
  ASSERT_EQ(irs::WildcardType::Prefix,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo%%"))));
  ASSERT_EQ(irs::WildcardType::Prefix,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("\xD0\xE2\x25"))));
  ASSERT_EQ(irs::WildcardType::Term,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("\xD0\x25"))));
  ASSERT_EQ(irs::WildcardType::Prefix,
            irs::ComputeWildcardType(irs::ViewCast<irs::byte_type>(
              std::string_view("\xD0\xE2\x25\x25"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(irs::ViewCast<irs::byte_type>(
              std::string_view("\x25\xD0\xE2\x25\x25"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo%_"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo%\\"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("fo%o\\%"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo_%"))));
  ASSERT_EQ(irs::WildcardType::PrefixEscaped,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo\\_%"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo__"))));
  ASSERT_EQ(irs::WildcardType::PrefixEscaped,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo\\%%"))));
  ASSERT_EQ(irs::WildcardType::PrefixEscaped,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo\\%%%"))));
  ASSERT_EQ(irs::WildcardType::TermEscaped,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("foo\\%\\%"))));
  ASSERT_EQ(irs::WildcardType::Prefix,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("%"))));
  ASSERT_EQ(irs::WildcardType::Prefix,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("%%"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("%c%"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("%%c%"))));
  ASSERT_EQ(irs::WildcardType::Wildcard,
            irs::ComputeWildcardType(
              irs::ViewCast<irs::byte_type>(std::string_view("%c%%"))));
}

// Named regression tests for a real bug: a single 2-byte Cyrillic character
// used as an infix must not accept a *different* character sharing its leading
// byte. `р` and `с` and `ё` all begin 0xD0, so a model that consumed the lead
// byte and then took anything would accept all three.

TEST_F(WildcardUtilsTest, same_start) {
  {
    const auto a = FromWildcard("%р%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "р"));
    EXPECT_FALSE(Accepts(a, "с"));
    EXPECT_FALSE(Accepts(a, "ё"));
  }
  {
    const auto a = FromWildcard("%ара%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ара"));
    EXPECT_FALSE(Accepts(a, "аса"));
    EXPECT_FALSE(Accepts(a, "аёа"));
  }
}

// The same, for characters sharing the leading byte 0xD1.
TEST_F(WildcardUtilsTest, same_end) {
  {
    const auto a = FromWildcard("%ѿ%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ѿ"));
    EXPECT_FALSE(Accepts(a, "с"));
    EXPECT_FALSE(Accepts(a, "ё"));
  }
  {
    const auto a = FromWildcard("%аѿа%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "аѿа"));
    EXPECT_FALSE(Accepts(a, "аса"));
    EXPECT_FALSE(Accepts(a, "аёа"));
  }
}

// `%` over ASCII, including the overlapping-repeat shapes a backtracking
// matcher gets wrong.
TEST_F(WildcardUtilsTest, match_wildcard_any_str) {
  {
    const auto a = FromWildcard("%rc%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "corrction"));
  }
  {
    const auto a = FromWildcard("%bcebce%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "bcebcebce"));
  }
  {
    const auto a = FromWildcard("%bcebcd%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "bcebcebcd"));
  }
  {
    const auto a = FromWildcard("%bcebced%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "bcebcebced"));
    EXPECT_FALSE(Accepts(a, "bcebcebbced"));
  }
  {
    const auto a = FromWildcard("%bcebce");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "bcebcebce"));
    EXPECT_FALSE(Accepts(a, "bcebcebbce"));
  }
  {
    const auto a = FromWildcard("%rrc%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "corrction"));
  }
  {
    const auto a = FromWildcard("%arc%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "arrrc"));
  }
  {
    const auto a = FromWildcard("%aca%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "arrrc"));
  }
  {
    const auto a = FromWildcard("foo%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "foo"));
    EXPECT_TRUE(Accepts(a, "foobar"));
    EXPECT_FALSE(Accepts(a, "foa"));
    EXPECT_FALSE(Accepts(a, "foabar"));
    EXPECT_TRUE(Accepts(a, "foo\xE2\x9E\x96\xE2\x9E\x96"));
    EXPECT_TRUE(Accepts(a, "foo\xF0\x9F\x98\x81\xE2\x9E\x96\xE2\x9E\x96"));
    EXPECT_TRUE(
      Accepts(a, "foo\xD0\xBF\xF0\x9F\x98\x81\xE2\x9E\x96\xE2\x9E\x96"));
  }
  {
    const auto a = FromWildcard("%foo");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "foo"));
    EXPECT_TRUE(Accepts(a, "fofoo"));
    EXPECT_TRUE(Accepts(a, "foofoo"));
    EXPECT_TRUE(Accepts(a, "fooofoo"));
    EXPECT_TRUE(Accepts(a, "ffoo"));
    EXPECT_TRUE(Accepts(a, "fffoo"));
    EXPECT_TRUE(Accepts(a, "bfoo"));
    EXPECT_FALSE(Accepts(a, "foa"));
    EXPECT_FALSE(Accepts(a, "bfoa"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\xE2\x9E\x96\x66\x6F\x6F"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\xE2\x9E\x96\x66\x66\x6F\x6F"));
    EXPECT_TRUE(
      Accepts(a, "\xF0\x9F\x98\x81\xE2\x9E\x96\xE2\x9E\x96\x66\x6F\x6F"));
    EXPECT_TRUE(Accepts(
      a, "\xD0\xBF\xF0\x9F\x98\x81\xE2\x9E\x96\xE2\x9E\x96\x66\x6F\x6F"));
  }
  {
    const auto a = FromWildcard("%ffoo");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "ffoo"));
    EXPECT_TRUE(Accepts(a, "ffooffoo"));
    EXPECT_TRUE(Accepts(a, "fffoo"));
    EXPECT_TRUE(Accepts(a, "bffoo"));
    EXPECT_FALSE(Accepts(a, "ffob"));
    EXPECT_FALSE(Accepts(a, "bfoa"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\xE2\x9E\x96\x66\x66\x6F\x6F"));
    EXPECT_TRUE(
      Accepts(a, "\xF0\x9F\x98\x81\xE2\x9E\x96\xE2\x9E\x96\x66\x66\x6F\x6F"));
    EXPECT_TRUE(Accepts(
      a, "\xD0\xBF\xF0\x9F\x98\x81\xE2\x9E\x96\xE2\x9E\x96\x66\x66\x6F\x6F"));
  }
  {
    const auto a = FromWildcard("a%foo");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "affoo"));
    EXPECT_TRUE(Accepts(a, "aaafofoo"));
    EXPECT_TRUE(Accepts(a, "aaafafoo"));
    EXPECT_TRUE(Accepts(a, "aaafaffoo"));
    EXPECT_TRUE(Accepts(a, "aaafoofoo"));
    EXPECT_TRUE(Accepts(a, "aaafooffffoo"));
    EXPECT_TRUE(Accepts(a, "aaafooofoo"));
    EXPECT_FALSE(Accepts(a, "abcdfo"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaaaaaaaaaaafoo"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaaaaaaaabfoo"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaaaaaa\x66\x6F\x6F"));
  }
  {
    const auto a = FromWildcard("a%foo%boo");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "afooboo"));
    EXPECT_TRUE(Accepts(a, "afoofoobooboo"));
    EXPECT_TRUE(Accepts(a, "afoofooboofooboo"));
  }
  {
    const auto a = FromWildcard("a%a");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "aaa"));
    EXPECT_TRUE(Accepts(a, "abcdfsa"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaaaaaaaaaaa"));
    EXPECT_FALSE(Accepts(a, "aaaaaaaaaaaaaaab"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaaaaaa\xE2\x9E\x96\x61"));
    // A malformed sequence in the middle is not a code point `%` can consume.
    EXPECT_FALSE(Accepts(a, "aaaaaaaaaaaaa\xE2\x9E\x61"));
  }
  {
    const auto a = FromWildcard("v%%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "vcc"));
    EXPECT_TRUE(Accepts(a, "vccc"));
    EXPECT_TRUE(Accepts(a, "vczc"));
    EXPECT_TRUE(Accepts(a, "vczczvccccc"));
  }
  {
    const auto a = FromWildcard("v%%c");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "vcc"));
    EXPECT_TRUE(Accepts(a, "vccc"));
    EXPECT_TRUE(Accepts(a, "vczc"));
    EXPECT_TRUE(Accepts(a, "vczczvccccc"));
  }
  {
    const auto a = FromWildcard("v%c");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "vcc"));
    EXPECT_TRUE(Accepts(a, "vccc"));
    EXPECT_TRUE(Accepts(a, "vczc"));
    EXPECT_TRUE(Accepts(a, "vczczvccccc"));
  }
  {
    const auto a = FromWildcard("b%d%a");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "azbce1d"));
    EXPECT_FALSE(Accepts(a, "azbce1d1"));
    EXPECT_FALSE(Accepts(a, "azbce11d"));
    EXPECT_TRUE(Accepts(
      a, "\x62\x61\x7A\xD0\xBF\xD0\xBF\x62\x63\x64\xD0\xBF\x64\x64\x61"));
  }
  {
    const auto a = FromWildcard("a%b%d");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_FALSE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "azbce11d"));
  }
  {
    const auto a = FromWildcard("a%b%db");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "azbce1d"));
    EXPECT_TRUE(Accepts(a, "azbce1db"));
    EXPECT_FALSE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "azbce11db"));
  }
}

// `_` is exactly one code point, in every position -- which is what the
// alternation over the four UTF-8 byte tiers exists for.
TEST_F(WildcardUtilsTest, match_wildcard_any_char) {
  {
    const auto a = FromWildcard("_");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "\xD0\xBF"));          // 2-byte
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96"));      // 3-byte
    EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x81"));  // 4-byte
    EXPECT_FALSE(Accepts(a, "a\xF0\x9F\x98\x81"));
    EXPECT_FALSE(Accepts(a, "\xF0\x9F\x98\x81\xF0\x9F\x98\x81"));
  }
  {
    const auto a = FromWildcard("__");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x96"));
    EXPECT_FALSE(Accepts(a, "a\xE2\x9E\x96\xD0\xBF"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\xD0\xBF"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\xE2\x9E\x96"));
    EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x81\xF0\x9F\x98\x81"));
    EXPECT_FALSE(Accepts(a, "a\xF0\x9F\x98\x81\xF0\x9F\x98\x81"));
    EXPECT_TRUE(Accepts(a, "ba"));
    EXPECT_FALSE(Accepts(a, "azbce1d"));
    EXPECT_FALSE(Accepts(a, "azbce1d1"));
    EXPECT_FALSE(Accepts(a, "azbce11d"));
  }
  {
    const auto a = FromWildcard("a_");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a_"));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "ab"));
    EXPECT_FALSE(Accepts(a, "a\xF0\x9F\x98\x81\xF0\x9F\x98\x81"));
    EXPECT_TRUE(Accepts(a, "a\xF0\x9F\x98\x81"));
  }
  {
    const auto a = FromWildcard("_a");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "_a"));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "ba"));
    EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x81\x61"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\x61"));
    // An invalid UTF-8 sequence is not one code point.
    EXPECT_FALSE(Accepts(a, "\xE2\xFF\xFF\x61"));
  }
  {
    const auto a = FromWildcard("%_");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_TRUE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "azbce11d"));
  }
  {
    const auto a = FromWildcard("_%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_TRUE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "azbce11d"));
  }
  {
    const auto a = FromWildcard("%%_");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_TRUE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "azbce11d"));
  }
  {
    const auto a = FromWildcard("%_d");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "d"));
    EXPECT_TRUE(Accepts(a, "ad"));
    EXPECT_TRUE(Accepts(a, "aad"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_FALSE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "1azbce11d"));
  }
  {
    const auto a = FromWildcard("%_%_%d");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "ad"));
    EXPECT_TRUE(Accepts(a, "add"));
    EXPECT_FALSE(Accepts(a, "add1"));
    EXPECT_TRUE(Accepts(a, "abd"));
    EXPECT_TRUE(Accepts(a, "ddd"));
    EXPECT_TRUE(Accepts(a, "aad"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_FALSE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "1azbce11d"));
    // One multi-byte character plus 'd' is two code points, not three.
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x96\x64"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\x64\x64\x64"));
    EXPECT_TRUE(Accepts(a, "a\xE2\x9E\x96\x64"));
    EXPECT_TRUE(Accepts(a, "e\xF0\x9F\x98\x81\x64"));
  }
  {
    const auto a = FromWildcard("%_%_%d%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "ad"));
    EXPECT_TRUE(Accepts(a, "add"));
    EXPECT_TRUE(Accepts(a, "add1"));
    EXPECT_TRUE(Accepts(a, "abd"));
    EXPECT_TRUE(Accepts(a, "ddd"));
    EXPECT_TRUE(Accepts(a, "aad"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_TRUE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "1azbce11d"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x96\x64"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96\x64\x64"));
    EXPECT_TRUE(Accepts(a, "azbce\xE2\x9E\x96\x64"));
    EXPECT_TRUE(Accepts(a, "azbce\xF0\x9F\x98\x81\x64"));
    EXPECT_TRUE(Accepts(a, "azbce\xE2\x9E\x96\xF0\x9F\x98\x81\x64\xD0\xBF"));
    EXPECT_TRUE(Accepts(a, "azbce\xD0\xBF\xD0\xBF\x64\xD0\xBF"));
  }
  {
    const auto a = FromWildcard("%r_c%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "correc"));
    EXPECT_FALSE(Accepts(a, "corerc"));
    EXPECT_FALSE(Accepts(a, "correrction"));
    EXPECT_TRUE(Accepts(a, "corrrc"));
    EXPECT_TRUE(Accepts(a, "correction"));
  }
  {
    const auto a = FromWildcard("%_r_c%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "correction"));
  }
  {
    const auto a = FromWildcard("%a%_r_c%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "Error detection and correction"));
  }
  {
    const auto a = FromWildcard("%a%bce_bc");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abceabc"));
    EXPECT_TRUE(Accepts(a, "abcebbcecbc"));
    EXPECT_TRUE(Accepts(a, "abceabcbcebbc"));
    EXPECT_FALSE(Accepts(a, "abcebcebc"));
  }
  {
    const auto a = FromWildcard("%a%bc__bc");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "abcbbc"));
    EXPECT_FALSE(Accepts(a, "abcbcbcc"));
    EXPECT_FALSE(Accepts(a, "abcbcbcb"));
    EXPECT_TRUE(Accepts(a, "abcbbbc"));
    EXPECT_TRUE(Accepts(a, "abcbcbc"));
  }
  {
    const auto a = FromWildcard("%a%bc_bc");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abcbbc"));
    EXPECT_TRUE(Accepts(a, "abcabc"));
    EXPECT_TRUE(Accepts(a, "abccbc"));
    EXPECT_TRUE(Accepts(a, "abcbcbcbccbc"));
  }
  {
    const auto a = FromWildcard("%a%b_b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abab"));
    EXPECT_TRUE(Accepts(a, "abbb"));
    EXPECT_TRUE(Accepts(a, "abbbb"));
    EXPECT_TRUE(Accepts(a, "abbabbbbbbb"));
  }
  {
    const auto a = FromWildcard("%a%b__b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abcab"));
    EXPECT_TRUE(Accepts(a, "abbbb"));
    EXPECT_TRUE(Accepts(a, "abbbbb"));
    EXPECT_TRUE(Accepts(a, "abbbbbb"));
    EXPECT_TRUE(Accepts(a, "abbccbbbcbbbbbb"));
    EXPECT_TRUE(Accepts(a, "abbabbbbbbb"));
  }
  {
    const auto a = FromWildcard("%a%b___b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abcabbbcab"));
    EXPECT_FALSE(Accepts(a, "abbbb"));
    EXPECT_TRUE(Accepts(a, "abbbbb"));
    EXPECT_TRUE(Accepts(a, "abbbbbb"));
    EXPECT_TRUE(Accepts(a, "abbccbbbcbbbbbb"));
    EXPECT_TRUE(Accepts(a, "abbabbbbbbb"));
  }
  {
    const auto a = FromWildcard("%a%bce___bce");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abcabcebcebce"));
    EXPECT_TRUE(Accepts(a, "abbccbcebbbbce"));
    EXPECT_TRUE(Accepts(a, "abbccbcebcebce"));
    EXPECT_FALSE(Accepts(a, "abbccbcebcebbce"));
  }
  {
    const auto a = FromWildcard("%a%bce____bce");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, "abceabcdbcebcebce"));
  }
  {
    const auto a = FromWildcard("%a%bce_____b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abcebcebcebcebcebcb"));
  }
  {
    const auto a = FromWildcard("%a%__b_b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "a__bab"));
    EXPECT_TRUE(Accepts(a, "afasfdwerfwefbbb"));
    EXPECT_TRUE(Accepts(a, "abbbbbbbbbbbbbbbbbbbb"));
    EXPECT_TRUE(Accepts(a, "abbabbbbbbb"));
  }
  {
    const auto a = FromWildcard("%a%_bce____def___b%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "a__bcedefadefbabb"));
  }
  {
    const auto a = FromWildcard("a%bce_b");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "aabce1dbce1b"));
    EXPECT_FALSE(Accepts(a, "aabce1dbce11b"));
    EXPECT_FALSE(Accepts(a, "abce1bb"));
    EXPECT_TRUE(Accepts(a, "abceabce1b"));
    EXPECT_TRUE(Accepts(a, "abcebce1b"));
    EXPECT_TRUE(Accepts(a, "azbce1b"));
    EXPECT_FALSE(Accepts(a, "azbce1db"));
    EXPECT_FALSE(Accepts(a, "azbce11b"));
  }
  {
    const auto a = FromWildcard("a%bce_d");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "aabce1dbce1d"));
    EXPECT_FALSE(Accepts(a, "aabce1dbce11d"));
    EXPECT_TRUE(Accepts(a, "abceabce1d"));
    EXPECT_TRUE(Accepts(a, "abcebce1d"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_FALSE(Accepts(a, "azbce1d1"));
    EXPECT_FALSE(Accepts(a, "azbce11d"));
    // The one character between "bce" and 'd' may be of any byte length.
    EXPECT_TRUE(Accepts(a, "azbce\xD0\xBF\x64"));
    EXPECT_TRUE(Accepts(a, "azbce\xE2\x9E\x96\x64"));
    EXPECT_TRUE(Accepts(a, "azbce\xF0\x9F\x98\x81\x64"));
    EXPECT_FALSE(Accepts(a, "azbce\xE2\x9E\x96\xF0\x9F\x98\x81\x64"));
    EXPECT_FALSE(Accepts(a, "azbce\xD0\xBF\xD0\xBF\x64"));
    EXPECT_TRUE(Accepts(a, "az\xD0\xBF\xD0\xBF\x62\x63\x65\xD0\xBF\x64"));
    EXPECT_FALSE(Accepts(a, "az\xD0\xBF\xD0\xBF\x62\x63\x65\xD0\xBF\x64\x64"));
  }
  {
    const auto a = FromWildcard("a%_b");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "ababab"));
    EXPECT_TRUE(Accepts(a, "abababbbb"));
    EXPECT_TRUE(Accepts(a, "ababbbbb"));
    EXPECT_TRUE(Accepts(a, "abbbbbb"));
    EXPECT_TRUE(Accepts(a, "abb"));
    EXPECT_TRUE(Accepts(a, "aab"));
  }
  {
    const auto a = FromWildcard("a%_b%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "abababc"));
    EXPECT_TRUE(Accepts(a, "abababcababab"));
    EXPECT_TRUE(Accepts(a, "abababbbbc"));
    EXPECT_TRUE(Accepts(a, "ababbbbbc"));
    EXPECT_TRUE(Accepts(a, "abbbbbbc"));
    EXPECT_TRUE(Accepts(a, "abbc"));
    EXPECT_TRUE(Accepts(a, "aabc"));
  }
  {
    const auto a = FromWildcard("_%a_%_a_%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "baaaab"));
    EXPECT_TRUE(Accepts(a, "aaaaaaaaaaaaaaaaaa"));
  }
}

// The byte model `_` stands for, pinned at its boundaries:
// `[00-7F] | [C2-DF][80-BF] | [E0-EF][80-BF]{2} | [F0-F4][80-BF]{3}`.
// It is the loose model, so an overlong or a surrogate is one code point here
// even though it decodes to nothing; only the lead bytes that encode no
// sequence at all -- `C0`, `C1` and `F5`..`FF` -- are outside it.
TEST_F(WildcardUtilsTest, match_wildcard_any_char_byte_model) {
  const auto a = FromWildcard("_");
  ASSERT_TRUE(a.ok());

  // `[00-7F]`, both ends.
  EXPECT_TRUE(Accepts(a, std::string_view{"\x00", 1}));
  EXPECT_TRUE(Accepts(a, "\x7F"));

  // A continuation byte never leads a sequence.
  EXPECT_FALSE(Accepts(a, "\x80"));
  EXPECT_FALSE(Accepts(a, "\xBF"));

  // `C0` and `C1` lead only overlong two-byte forms and are excluded; `C2` is
  // the first lead byte that is not.
  EXPECT_FALSE(Accepts(a, "\xC0\x80"));
  EXPECT_FALSE(Accepts(a, "\xC1\xBF"));
  EXPECT_TRUE(Accepts(a, "\xC2\x80"));
  EXPECT_TRUE(Accepts(a, "\xDF\xBF"));

  // `[E0-EF]` and `[F0-F4]`, both ends, overlongs and surrogates included.
  EXPECT_TRUE(Accepts(a, "\xE0\x80\x80"));
  EXPECT_TRUE(Accepts(a, "\xED\xA0\x80"));
  EXPECT_TRUE(Accepts(a, "\xEF\xBF\xBF"));
  EXPECT_TRUE(Accepts(a, "\xF0\x80\x80\x80"));
  EXPECT_TRUE(Accepts(a, "\xF4\xBF\xBF\xBF"));

  // `F5`..`FF` lead nothing.
  EXPECT_FALSE(Accepts(a, "\xF5\x80\x80\x80"));
  EXPECT_FALSE(Accepts(a, "\xFF\x80\x80\x80"));
  EXPECT_FALSE(Accepts(a, "\xFF"));

  // A lead byte needs exactly as many continuations as it announces, and every
  // one of them has to be in `[80-BF]`.
  EXPECT_FALSE(Accepts(a, "\xC2"));
  EXPECT_FALSE(Accepts(a, "\xC2\xC2"));
  EXPECT_FALSE(Accepts(a, "\xC2\x80\x80"));
  EXPECT_FALSE(Accepts(a, "\xE2\x9E"));
  EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96"));
  EXPECT_FALSE(Accepts(a, "\xE2\x9E\x96\x80"));
  EXPECT_FALSE(Accepts(a, "\xF0\x9F\x98"));
  EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x81"));
}

// `\` escapes `_`, `%` and itself; a lone trailing `\` is dropped.
TEST_F(WildcardUtilsTest, match_wildcard_escapes) {
  {
    const auto a = FromWildcard("\\_a");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "_a"));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "ba"));
  }
  {
    // Three backslashes: an escaped backslash, then an escaped underscore.
    const auto a = FromWildcard("\\\\\\_a");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "\\_a"));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "\\_\xE2\x9E\x96"));
    EXPECT_FALSE(Accepts(a, "ba"));
  }
  {
    const auto a = FromWildcard("\\\\\\%a");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "\\%a"));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "ba"));
  }
  {
    // Escaping an ordinary character yields that character.
    const auto a = FromWildcard("\\a");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "\\a"));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "\\\\a"));
  }
  {
    const auto a = FromWildcard("foo\\%");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "foo"));
    EXPECT_TRUE(Accepts(a, "foo%"));
    EXPECT_FALSE(Accepts(a, "foobar"));
    EXPECT_FALSE(Accepts(a, "foa"));
    EXPECT_FALSE(Accepts(a, "foabar"));
    EXPECT_FALSE(Accepts(a, "foo\xE2\x9E\x96\xE2\x9E\x96"));
  }
  {
    const auto a = FromWildcard("%\\\\");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "\\"));
    EXPECT_TRUE(Accepts(a, "a\\"));
    EXPECT_TRUE(Accepts(a, "aa\\"));
    EXPECT_TRUE(Accepts(a, "azbce1\\"));
    EXPECT_FALSE(Accepts(a, "azbce1\\1"));
    EXPECT_TRUE(Accepts(a, "1azbce11\\"));
  }
  {
    const auto a = FromWildcard("%_\\\\");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "\\"));
    EXPECT_TRUE(Accepts(a, "a\\"));
    EXPECT_TRUE(Accepts(a, "aa\\"));
    EXPECT_TRUE(Accepts(a, "azbce1\\"));
    EXPECT_FALSE(Accepts(a, "azbce1\\1"));
    EXPECT_TRUE(Accepts(a, "1azbce11\\"));
  }
  // A pattern ending in a lone `\` drops it: the escape has nothing to escape.
  {
    const auto a = FromWildcard("a\\");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "a\\"));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "ba"));
  }
  {
    const auto a = FromWildcard("%\\");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "\\"));
    EXPECT_TRUE(Accepts(a, "a\\"));
    EXPECT_TRUE(Accepts(a, "aa\\"));
    EXPECT_TRUE(Accepts(a, "azbce1\\"));
    EXPECT_TRUE(Accepts(a, "azbce1\\1"));
    EXPECT_TRUE(Accepts(a, "1azbce11\\"));
  }
  {
    const auto a = FromWildcard("%_\\");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "\\"));
    EXPECT_TRUE(Accepts(a, "a\\"));
    EXPECT_TRUE(Accepts(a, "aa\\"));
    EXPECT_TRUE(Accepts(a, "azbce1\\"));
    EXPECT_TRUE(Accepts(a, "azbce1\\1"));
    EXPECT_TRUE(Accepts(a, "1azbce11\\"));
  }
}

// The empty pattern selects only the empty term; `%` selects everything.
TEST_F(WildcardUtilsTest, match_wildcard_empty_and_any) {
  {
    const auto a = FromWildcard(std::string_view{});
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "a"));
  }
  {
    const auto a = FromWildcard("");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "a"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x96"));
  }
  {
    const auto a = FromWildcard("%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "abc"));
    EXPECT_TRUE(Accepts(a, "\xD0\xBF"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96"));
    EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x81"));
  }
  {
    const auto a = FromWildcard("%%");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "a"));
    EXPECT_TRUE(Accepts(a, "aa"));
    EXPECT_TRUE(Accepts(a, "azbce1d"));
    EXPECT_TRUE(Accepts(a, "azbce1d1"));
    EXPECT_TRUE(Accepts(a, "azbce11d"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x96"));
    EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x81"));
    EXPECT_TRUE(Accepts(a, "a\xF0\x9F\x98\x81"));
    EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98\x81\xF0\x9F\x98\x81"));
  }
}

// A run of `%` is one `%`, and a run with `_`s in it is the `_`s: the language
// is what has to be equal, whatever the pattern spells it as.
TEST_F(WildcardUtilsTest, match_wildcard_star_runs_collapse) {
  constexpr std::string_view kTerms[]{
    "",
    "a",
    "b",
    "bs",
    "bas",
    "bxs",
    "bxxs",
    "bxxxs",
    "abs",
    "bsa",
    "bab",
    "abba",
    "s",
    "\xD0\xBF",
    "b\xD0\xBF\x73",
    "b\xD0\xBF\xD0\xBF\x73",
    "bxxxxxxxxs",
  };
  const std::pair<std::string_view, std::string_view> kEquivalent[]{
    {"%b%", "%b%%%"},
    {"b%%%%%s", "b%%%s"},
    {"b%%__%%%s%", "b%%%%%%%__%%%%%%%%s%"},
  };

  for (const auto& [lhs_pattern, rhs_pattern] : kEquivalent) {
    SCOPED_TRACE(testing::Message("Patterns: '")
                 << lhs_pattern << "' vs '" << rhs_pattern << "'");
    const auto lhs = FromWildcard(lhs_pattern);
    const auto rhs = FromWildcard(rhs_pattern);
    ASSERT_TRUE(lhs.ok());
    ASSERT_TRUE(rhs.ok());
    for (const auto term : kTerms) {
      EXPECT_EQ(Accepts(lhs, term), Accepts(rhs, term)) << "term: " << term;
    }
  }
}

// A NUL byte in the pattern is a byte like any other. This is the headline
// claim of the wildcard dialect: a regexp source string could not carry it.
TEST_F(WildcardUtilsTest, match_wildcard_nul_byte) {
  static constexpr auto kNull = std::string_view{"%\0%", 3};
  const auto a = FromWildcard(kNull);
  ASSERT_TRUE(a.ok());
  EXPECT_FALSE(Accepts(a, ""));
  EXPECT_TRUE(Accepts(a, kNull));
  EXPECT_TRUE(Accepts(a, std::string_view{"a\0", 2}));
  EXPECT_TRUE(Accepts(a, std::string_view{"\0a", 2}));
  EXPECT_TRUE(Accepts(a, std::string_view{"a\0a", 3}));
  EXPECT_FALSE(Accepts(a, "aa"));
}

// A pattern carrying a malformed UTF-8 sequence matches it byte for byte, and
// `%` / `_` around it still consume whole code points -- so a term whose
// malformed sequence is a *different* one is rejected.
TEST_F(WildcardUtilsTest, match_wildcard_invalid_utf8) {
  {
    // "_%<E2 9E 61>_%_<E2 9E 61>_%"
    const auto a =
      FromWildcard("\x5F\x25\xE2\x9E\x61\x5F\x25\x5F\xE2\x9E\x61\x5F\x25");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "\x98\xE2\x9E\x61\x97\x97\xE2\x9E\x61\x98"));
    EXPECT_FALSE(Accepts(a,
                         "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61"
                         "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61"));
  }
  {
    // The same shape over a *valid* 3-byte character, for contrast.
    const auto a =
      FromWildcard("\x5F\x25\xE2\x9E\x9E\x5F\x25\x5F\xE2\x9E\x9E\x5F\x25");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a,
                        "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E"
                        "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E"));
    EXPECT_FALSE(Accepts(a, "\x98\xE2\x9E\x9E\x97\x97\xE2\x9E\x9E\x98"));
  }
  {
    // "<E2 9E 61>%<E2 9E 61>"
    const auto a = FromWildcard("\xE2\x9E\x61\x25\xE2\x9E\x61");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x61\xE2\x9E\x61"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x61\x61\xE2\x9E\x61"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x61\x9E\x61\xE2\x9E\x61"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x61\x9E\x61\xE2\x9E\xE2\x9E\x61"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61"));
    EXPECT_FALSE(
      Accepts(a, "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61"));
    EXPECT_FALSE(Accepts(a,
                         "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61"
                         "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61"));
    EXPECT_FALSE(Accepts(a,
                         "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61"
                         "\xE2\x9E\x61\xE2\x9E\x61\xE2\x9E\x61\x61"));
  }
  {
    // "<E2 9E 9E>%<E2 9E 9E>" - valid characters, same shape.
    const auto a = FromWildcard("\xE2\x9E\x9E\x25\xE2\x9E\x9E");
    ASSERT_TRUE(a.ok());
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x9E\xE2\x9E\x9E"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x9E\x9E\x9E\xE2\x9E\xE2\x9E\x9E"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E"));
    EXPECT_TRUE(Accepts(a, "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E"));
    EXPECT_TRUE(Accepts(a,
                        "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E"
                        "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x9E\x9E\xE2\x9E\x9E"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x9E\x9E\x9E\xE2\x9E\x9E"));
    EXPECT_FALSE(Accepts(a,
                         "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E"
                         "\xE2\x9E\x9E\xE2\x9E\x9E\xE2\x9E\x9E\x9E"));
  }
}

// A truncated multi-byte prefix is a literal byte chain, not a character
// class: it selects exactly those bytes and nothing else.
TEST_F(WildcardUtilsTest, match_wildcard_truncated_utf8) {
  {
    const auto a = FromWildcard("\xD0");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "\xD0"));
    EXPECT_FALSE(Accepts(a, ""));
    EXPECT_FALSE(Accepts(a, "\xD0\xBF"));
    EXPECT_FALSE(Accepts(a, "\xD1"));
  }
  {
    const auto a = FromWildcard("\xE2\x9E");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "\xE2\x9E"));
    EXPECT_FALSE(Accepts(a, "\xE2"));
    EXPECT_FALSE(Accepts(a, "\xE2\x9E\x96"));
  }
  {
    const auto a = FromWildcard("\xF0\x9F\x98");
    ASSERT_TRUE(a.ok());
    EXPECT_TRUE(Accepts(a, "\xF0\x9F\x98"));
    EXPECT_FALSE(Accepts(a, "\xF0\x9F"));
    EXPECT_FALSE(Accepts(a, "\xF0\x9F\x98\x81"));
  }
}
