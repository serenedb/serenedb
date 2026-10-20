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

#include <absl/numeric/int128.h>

#include <climits>
#include <duckdb/inet/inet_ipaddress.hpp>
#include <random>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/inet_terms.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

inet_terms::Inet Parse(std::string_view text) {
  const auto ip = ipaddress_from_string(text.data(), text.size());
  const auto value =
    inet_terms::FromAddress(static_cast<inet_terms::Family>(ip.type),
                            ip.address.upper, ip.address.lower, ip.mask);
  EXPECT_TRUE(value.has_value()) << text;
  return *value;
}

std::vector<irs::bstring> TermsOf(inet_terms::Family family, uint64_t upper,
                                  uint64_t lower, uint16_t mask) {
  duckdb::string_t slots[inet_terms::kMaxTerms];
  uint8_t bytes[inet_terms::kV6LongTermBytes];
  auto* end = bytes;
  const auto count =
    inet_terms::WriteTerms(family, upper, lower, mask, slots, end);
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
  return TermsOf(static_cast<inet_terms::Family>(ip.type), ip.address.upper,
                 ip.address.lower, ip.mask);
}

size_t TermCount(std::string_view text) { return TermsOf(text).size(); }

std::vector<irs::bstring> ExactAndLevelTermsOf(const inet_terms::Inet& value) {
  std::vector<irs::bstring> terms{inet_terms::ExactTermOf(value)};
  const auto network = inet_terms::Network(value, value.mask);
  for (uint32_t level = 1; level <= inet_terms::LevelCount(value.family);
       ++level) {
    terms.push_back(inet_terms::LevelTermOf(value.family, level, network));
  }
  return terms;
}

void ExpectTermsMatchHelpers(inet_terms::Family family, uint64_t upper,
                             uint64_t lower, uint16_t mask) {
  const auto value = inet_terms::FromAddress(family, upper, lower, mask);
  ASSERT_TRUE(value.has_value());
  auto expected = ExactAndLevelTermsOf(*value);
  if (mask < inet_terms::MaxBits(value->family)) {
    expected.push_back(
      inet_terms::MaskTermOf(value->family, static_cast<uint8_t>(mask)));
  }
  EXPECT_EQ(expected, TermsOf(family, upper, lower, mask))
    << int{std::to_underlying(family)} << " " << upper << " " << lower << "/"
    << mask;
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
  const auto terms = ExactAndLevelTermsOf(value);
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
    for (uint16_t mask = 0; mask <= 32; ++mask) {
      ExpectTermsMatchHelpers(inet_terms::Family::V4, 0, address, mask);
    }
  }
  const std::pair<uint64_t, uint64_t> v6_addresses[] = {
    {0, 0},
    {0x20010DB800000000, 1},
    {0x8000FFFF0000FFFF, 0xFF00FF00FF00FF00},
    {~uint64_t{0}, ~uint64_t{0}}};
  for (const auto& [upper, lower] : v6_addresses) {
    for (uint16_t mask = 0; mask <= 128; ++mask) {
      ExpectTermsMatchHelpers(inet_terms::Family::V6, upper, lower, mask);
    }
  }
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
  EXPECT_FALSE(
    inet_terms::FromAddress(static_cast<inet_terms::Family>(0), 0, 1, 32)
      .has_value());
  EXPECT_FALSE(
    inet_terms::FromAddress(static_cast<inet_terms::Family>(3), 0, 1, 32)
      .has_value());
  EXPECT_FALSE(
    inet_terms::FromAddress(inet_terms::Family::V4, 1, 1, 32).has_value());
  EXPECT_FALSE(
    inet_terms::FromAddress(inet_terms::Family::V4, 0, 0x100000000, 32)
      .has_value());
  EXPECT_FALSE(
    inet_terms::FromAddress(inet_terms::Family::V4, 0, 1, 33).has_value());
  EXPECT_FALSE(
    inet_terms::FromAddress(inet_terms::Family::V6, 0, 1, 129).has_value());
}

inet_terms::Bytes NetworkOf(std::string_view text) {
  const auto value = Parse(text);
  return inet_terms::Network(value, value.mask);
}

absl::uint128 BitsOf(inet_terms::Family family,
                     const inet_terms::Bytes& bytes) {
  absl::uint128 bits = 0;
  for (uint32_t i = 0; i < inet_terms::Width(family); ++i) {
    bits = (bits << CHAR_BIT) | bytes[i];
  }
  return bits;
}

inet_terms::Bytes BytesOf(inet_terms::Family family, absl::uint128 bits) {
  inet_terms::Bytes bytes{};
  for (uint32_t i = inet_terms::Width(family); i != 0; --i) {
    bytes[i - 1] = static_cast<uint8_t>(bits & 0xFF);
    bits >>= CHAR_BIT;
  }
  return bytes;
}

struct Piece {
  uint32_t level;
  std::string_view first;
  std::string_view last;
};

void ExpectSplit(std::string_view first, std::string_view last,
                 std::vector<Piece> expected) {
  const auto family = Parse(first).family;
  const auto pieces =
    inet_terms::SplitNetworks(family, NetworkOf(first), NetworkOf(last));
  ASSERT_EQ(expected.size(), pieces.size()) << first << " .. " << last;
  for (size_t i = 0; i < pieces.size(); ++i) {
    EXPECT_EQ(expected[i].level, pieces[i].level) << i;
    EXPECT_EQ(NetworkOf(expected[i].first), pieces[i].first) << i;
    EXPECT_EQ(NetworkOf(expected[i].last), pieces[i].last) << i;
  }
}

void ExpectSplitCovers(inet_terms::Family family, absl::uint128 first,
                       absl::uint128 last) {
  const uint32_t step =
    inet_terms::MaxBits(family) - inet_terms::LevelBits(family, 1);
  const auto pieces = inet_terms::SplitNetworks(family, BytesOf(family, first),
                                                BytesOf(family, last));
  ASSERT_FALSE(pieces.empty());
  EXPECT_LE(pieces.size(), 2 * inet_terms::LevelCount(family) + 1);
  absl::uint128 next = first;
  for (const auto& piece : pieces) {
    const absl::uint128 low = (absl::uint128{1} << (piece.level * step)) - 1;
    const auto piece_first = BitsOf(family, piece.first);
    const auto piece_last = BitsOf(family, piece.last) | low;
    ASSERT_EQ(next, piece_first) << "gap or overlap at level " << piece.level;
    ASSERT_EQ(0, piece_first & low) << "unaligned piece";
    ASSERT_LE(piece_first, piece_last);
    next = piece_last + 1;
    if (piece_last == last) {
      ASSERT_EQ(&piece, &pieces.back()) << "pieces past the last network";
    }
  }
  EXPECT_EQ(last, next - 1);
}

TEST(inet_terms_test, split_networks_takes_whole_buckets_at_aligned_bounds) {
  ExpectSplit("10.1.0.0", "10.2.255.255", {{2, "10.1.0.0", "10.2.0.0"}});
  ExpectSplit("10.1.0.0", "10.1.255.255", {{2, "10.1.0.0", "10.1.0.0"}});
  ExpectSplit("0.0.0.0", "255.255.255.255", {{3, "0.0.0.0", "255.0.0.0"}});
  ExpectSplit("2001:db8:1::", "2001:db8:1:ffff:ffff:ffff:ffff:ffff",
              {{5, "2001:db8:1::", "2001:db8:1::"}});
  ExpectSplit("::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
              {{7, "::", "ffff::"}});
}

TEST(inet_terms_test, split_networks_keeps_ragged_edges_exact) {
  ExpectSplit("10.1.0.5", "10.3.0.0",
              {{0, "10.1.0.5", "10.1.0.255"},
               {1, "10.1.1.0", "10.1.255.0"},
               {2, "10.2.0.0", "10.2.0.0"},
               {0, "10.3.0.0", "10.3.0.0"}});
  ExpectSplit("10.1.0.0", "10.2.255.254",
              {{2, "10.1.0.0", "10.1.0.0"},
               {1, "10.2.0.0", "10.2.254.0"},
               {0, "10.2.255.0", "10.2.255.254"}});
  ExpectSplit("10.1.2.3", "10.1.2.3", {{0, "10.1.2.3", "10.1.2.3"}});
}

TEST(inet_terms_test, split_networks_covers_exactly_the_range) {
  std::mt19937_64 random{20261019};
  const auto pick = [&](inet_terms::Family family) {
    const auto max_bits = inet_terms::MaxBits(family);
    absl::uint128 bits = absl::MakeUint128(random(), random());
    if (max_bits < 128) {
      bits &= (absl::uint128{1} << max_bits) - 1;
    }
    const uint32_t zeros = random() % (max_bits + 1);
    const absl::uint128 low =
      zeros == 128 ? ~absl::uint128{0} : (absl::uint128{1} << zeros) - 1;
    switch (random() % 4) {
      case 0:
        return bits & ~low;
      case 1:
        return bits | low;
      default:
        return bits;
    }
  };
  for (const auto family : {inet_terms::Family::V4, inet_terms::Family::V6}) {
    for (int i = 0; i < 20000; ++i) {
      auto first = pick(family);
      auto last = pick(family);
      if (first > last) {
        std::swap(first, last);
      }
      ExpectSplitCovers(family, first, last);
      if (HasFatalFailure()) {
        return;
      }
    }
  }
}

TEST(inet_terms_test, first_and_last_exact_terms_bound_a_network) {
  for (const std::string_view network :
       {"0.0.0.0", "10.1.0.0", "10.1.2.3", "255.255.255.255",
        "::", "2001:db8::", "2001:db8::1"}) {
    const auto value = Parse(network);
    const auto bytes = inet_terms::Network(value, value.mask);
    const auto first = inet_terms::FirstExactTermOf(value.family, bytes);
    const auto last = inet_terms::LastExactTermOf(value.family, bytes);
    for (uint32_t mask = 0; mask <= inet_terms::MaxBits(value.family); ++mask) {
      auto host = value;
      host.mask = static_cast<uint16_t>(mask);
      for (const bool with_host_bits : {false, true}) {
        if (with_host_bits) {
          host.address = inet_terms::Broadcast(value, mask);
        }
        const auto term = inet_terms::ExactTermOf(host);
        const bool same_network = inet_terms::Network(host, mask) == bytes;
        EXPECT_EQ(same_network, first <= term && term <= last)
          << network << " mask " << mask << " host bits " << with_host_bits;
      }
    }
  }
}

irs::bstring Hex(std::string_view hex) {
  irs::bstring bytes;
  for (size_t i = 0; i + 1 < hex.size(); i += 2) {
    bytes.push_back(static_cast<irs::byte_type>(
      std::stoi(std::string{hex.substr(i, 2)}, nullptr, 16)));
  }
  return bytes;
}

std::vector<irs::bstring> HexTerms(
  std::initializer_list<std::string_view> hex) {
  std::vector<irs::bstring> terms;
  for (const auto term : hex) {
    terms.push_back(Hex(term));
  }
  return terms;
}

TEST(inet_terms_test, written_terms_keep_their_on_disk_bytes) {
  EXPECT_EQ(HexTerms({"400a0102030020", "410a0102", "420a01", "430a"}),
            TermsOf("10.1.2.3"));
  EXPECT_EQ(
    HexTerms({"400a01020000180a010203", "410a0102", "420a01", "430a", "4818"}),
    TermsOf("10.1.2.3/24"));
  EXPECT_EQ(
    HexTerms({"6020010db80000000000000000000000010080",
              "6120010db800000000000000000000", "6220010db80000000000000000",
              "6320010db8000000000000", "6420010db800000000", "6520010db80000",
              "6620010db8", "672001"}),
    TermsOf("2001:db8::1"));
  EXPECT_EQ(
    HexTerms({"6020010db80000000000000000000000000020",
              "6120010db800000000000000000000", "6220010db80000000000000000",
              "6320010db8000000000000", "6420010db800000000", "6520010db80000",
              "6620010db8", "672001", "6820"}),
    TermsOf("2001:db8::/32"));
}

TEST(inet_terms_test, describe_renders_terms) {
  EXPECT_EQ("10.0.0.5/8",
            inet_terms::Describe(inet_terms::ExactTermOf(Parse("10.0.0.5/8"))));
  EXPECT_EQ("2001:db8::1", inet_terms::Describe(
                             inet_terms::ExactTermOf(Parse("2001:db8::1"))));
  EXPECT_EQ("/16", inet_terms::Describe(
                     inet_terms::MaskTermOf(inet_terms::Family::V4, 16)));
  EXPECT_EQ("ipv6", inet_terms::Describe(inet_terms::CoarsestLevelPrefixOf(
                      inet_terms::Family::V6)));
}

}  // namespace
}  // namespace sdb::connector
