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

#include <duckdb/inet/inet_ipaddress.hpp>
#include <string_view>
#include <vector>

#include "connector/inet_terms.h"
#include "gtest/gtest.h"

namespace {

using namespace sdb::connector;

inet_terms::Inet Parse(std::string_view text) {
  const auto ip = ipaddress_from_string(text.data(), text.size());
  const auto value = inet_terms::FromAddress(
    static_cast<uint8_t>(ip.type), ip.address.upper, ip.address.lower, ip.mask);
  EXPECT_TRUE(value.has_value()) << text;
  return *value;
}

size_t TermCount(const inet_terms::Inet& value) {
  size_t count = 0;
  inet_terms::ForEachTerm(value, [&](const uint8_t*, size_t) { ++count; });
  return count;
}

TEST(inet_terms_test, exact_terms_sort_like_postgres) {
  const std::vector<std::string_view> ordered{
    "0.0.0.0/0", "10.0.0.0/8",  "10.0.0.5/8", "10.0.0.0/16",
    "10.0.0.1",  "10.0.1.0/24", "11.0.0.0/8", "255.255.255.255",
    "::/0",      "::/63",       "::/64",      "::1/127",
    "::1",       "8000::/1",    "8000::1/1",  "ffff::1"};
  for (size_t i = 1; i < ordered.size(); ++i) {
    EXPECT_LT(inet_terms::ExactTermOf(Parse(ordered[i - 1])),
              inet_terms::ExactTermOf(Parse(ordered[i])))
      << ordered[i - 1] << " < " << ordered[i];
  }
}

TEST(inet_terms_test, equal_values_share_the_exact_term) {
  EXPECT_EQ(inet_terms::ExactTermOf(Parse("10.0.0.1")),
            inet_terms::ExactTermOf(Parse("10.0.0.1/32")));
  EXPECT_NE(inet_terms::ExactTermOf(Parse("10.0.0.1/8")),
            inet_terms::ExactTermOf(Parse("10.0.0.2/8")));
  EXPECT_NE(inet_terms::ExactTermOf(Parse("10.0.0.0/8")),
            inet_terms::ExactTermOf(Parse("10.0.0.0/16")));
}

TEST(inet_terms_test, levels_are_prefixes_of_the_exact_term) {
  const auto value = Parse("10.1.2.3/20");
  const auto terms = inet_terms::GranularTermsOf(value);
  ASSERT_EQ(4, terms.size());
  for (size_t i = 1; i < terms.size(); ++i) {
    EXPECT_EQ(terms[0].substr(1, terms[i].size() - 1), terms[i].substr(1));
  }
  EXPECT_EQ("10.1.0.0/16", inet_terms::Describe(terms[2]));
  EXPECT_EQ("10.1.0.0/24", inet_terms::Describe(terms[1]));
}

TEST(inet_terms_test, networks_add_a_mask_term) {
  EXPECT_EQ(4, TermCount(Parse("10.1.2.3")));
  EXPECT_EQ(5, TermCount(Parse("10.1.2.3/24")));
  EXPECT_EQ(8, TermCount(Parse("2001:db8::1")));
  EXPECT_EQ(9, TermCount(Parse("2001:db8::/32")));
}

TEST(inet_terms_test, network_and_broadcast) {
  const auto v4 = Parse("10.1.2.3/20");
  EXPECT_EQ(inet_terms::Network(Parse("10.1.0.0"), 32),
            inet_terms::Network(v4, 20));
  EXPECT_EQ(inet_terms::Network(Parse("10.1.15.255"), 32),
            inet_terms::Broadcast(v4, 20));
  const auto v6 = Parse("2001:db8::8000:0:0:1/65");
  EXPECT_EQ(inet_terms::Network(Parse("2001:db8::8000:0:0:0"), 128),
            inet_terms::Network(v6, 65));
}

TEST(inet_terms_test, rejects_values_that_are_not_addresses) {
  EXPECT_FALSE(inet_terms::FromAddress(0, 0, 1, 32).has_value());
  EXPECT_FALSE(inet_terms::FromAddress(3, 0, 1, 32).has_value());
  EXPECT_FALSE(inet_terms::FromAddress(1, 1, 1, 32).has_value());
  EXPECT_FALSE(inet_terms::FromAddress(1, 0, 0x100000000, 32).has_value());
}

TEST(inet_terms_test, describe_renders_terms) {
  EXPECT_EQ("10.0.0.5/8",
            inet_terms::Describe(inet_terms::ExactTermOf(Parse("10.0.0.5/8"))));
  EXPECT_EQ("2001:db8::1", inet_terms::Describe(
                             inet_terms::ExactTermOf(Parse("2001:db8::1"))));
  EXPECT_EQ("/16", inet_terms::Describe(inet_terms::MaskTermOf(false, 16)));
  EXPECT_EQ("ipv6",
            inet_terms::Describe(inet_terms::CoarsestLevelPrefixOf(true)));
}

}  // namespace
