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
#include <utility>
#include <vector>

#include "connector/inet_terms.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

inet_terms::Inet Parse(std::string_view text) {
  const auto ip = ipaddress_from_string(text.data(), text.size());
  const auto value = inet_terms::FromAddress(
    static_cast<uint8_t>(ip.type), ip.address.upper, ip.address.lower, ip.mask);
  EXPECT_TRUE(value.has_value()) << text;
  return *value;
}

std::vector<irs::bstring> TermsOf(uint8_t ip_type, uint64_t upper,
                                  uint64_t lower, uint16_t mask) {
  duckdb::string_t slots[inet_terms::kMaxTermsPerValue];
  uint8_t bytes[inet_terms::kV6LongTermBytes];
  auto* end = bytes;
  const auto count =
    inet_terms::WriteTerms(ip_type, upper, lower, mask, slots, end);
  std::vector<irs::bstring> terms;
  for (uint32_t i = 0; i < count; ++i) {
    const irs::bstring term{
      reinterpret_cast<const irs::byte_type*>(slots[i].GetData()),
      slots[i].GetSize()};
    EXPECT_TRUE(slots[i] ==
                duckdb::string_t(reinterpret_cast<const char*>(term.data()),
                                 static_cast<uint32_t>(term.size())));
    terms.push_back(term);
  }
  return terms;
}

std::vector<irs::bstring> TermsOf(std::string_view text) {
  const auto ip = ipaddress_from_string(text.data(), text.size());
  return TermsOf(static_cast<uint8_t>(ip.type), ip.address.upper,
                 ip.address.lower, ip.mask);
}

size_t TermCount(std::string_view text) { return TermsOf(text).size(); }

void ExpectTermsMatchHelpers(uint8_t ip_type, uint64_t upper, uint64_t lower,
                             uint16_t mask) {
  const auto value = inet_terms::FromAddress(ip_type, upper, lower, mask);
  ASSERT_TRUE(value.has_value());
  auto expected = inet_terms::GranularTermsOf(*value);
  if (mask < value->MaxBits()) {
    expected.push_back(
      inet_terms::MaskTermOf(value->v6, static_cast<uint8_t>(mask)));
  }
  EXPECT_EQ(expected, TermsOf(ip_type, upper, lower, mask))
    << int{ip_type} << " " << upper << " " << lower << "/" << mask;
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

TEST(inet_terms_test, exact_term_omits_an_address_equal_to_its_network) {
  EXPECT_EQ(7, inet_terms::ExactTermOf(Parse("10.0.0.1")).size());
  EXPECT_EQ(7, inet_terms::ExactTermOf(Parse("10.0.0.0/8")).size());
  EXPECT_EQ(11, inet_terms::ExactTermOf(Parse("10.0.0.5/8")).size());
  EXPECT_EQ(19, inet_terms::ExactTermOf(Parse("2001:db8::1")).size());
  EXPECT_EQ(19, inet_terms::ExactTermOf(Parse("2001:db8::/32")).size());
  EXPECT_EQ(35, inet_terms::ExactTermOf(Parse("2001:db8::1/32")).size());
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
  EXPECT_EQ(4, TermCount("10.1.2.3"));
  EXPECT_EQ(5, TermCount("10.1.2.3/24"));
  EXPECT_EQ(8, TermCount("2001:db8::1"));
  EXPECT_EQ(9, TermCount("2001:db8::/32"));
}

TEST(inet_terms_test, written_terms_match_the_term_helpers) {
  for (const uint64_t address : {uint64_t{0}, uint64_t{0x0A010203},
                                 uint64_t{0x80FF00FF}, uint64_t{0xFFFFFFFF}}) {
    for (uint16_t mask = 0; mask <= 33; ++mask) {
      ExpectTermsMatchHelpers(inet_terms::kV4Type, 0, address, mask);
    }
  }
  const std::pair<uint64_t, uint64_t> v6_addresses[] = {
    {0, 0},
    {0x20010DB800000000, 1},
    {0x8000FFFF0000FFFF, 0xFF00FF00FF00FF00},
    {~uint64_t{0}, ~uint64_t{0}}};
  for (const auto& [upper, lower] : v6_addresses) {
    for (uint16_t mask = 0; mask <= 129; ++mask) {
      ExpectTermsMatchHelpers(inet_terms::kV6Type, upper, lower, mask);
    }
  }
  EXPECT_TRUE(TermsOf(0, 0, 1, 32).empty());
  EXPECT_TRUE(TermsOf(inet_terms::kV4Type, 1, 1, 32).empty());
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
}  // namespace sdb::connector
