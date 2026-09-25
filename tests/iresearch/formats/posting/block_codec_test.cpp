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

#include <algorithm>
#include <array>
#include <iresearch/formats/posting/block_codec.hpp>
#include <iresearch/formats/posting/block_kernels.hpp>
#include <random>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace {

namespace bc = irs::block_codec;

constexpr uint32_t kSlack = 16;
constexpr uint32_t kWidths = bc::kMaxWidth + 1;

template<uint32_t L>
using Values = std::array<uint32_t, bc::kBlockOf<L>>;

template<uint32_t L>
using Packed = std::array<irs::byte_type, 4 * L * 32 + kSlack>;

template<uint32_t B>
uint32_t Draw(std::mt19937& rng) {
  if constexpr (B == 0) {
    return 0;
  } else {
    return static_cast<uint32_t>(rng()) & bc::LowMask<B>();
  }
}

template<uint32_t B, uint32_t L>
void CheckVertical(std::mt19937& rng) {
  Values<L> values;
  for (auto& v : values) {
    v = Draw<B>(rng);
  }
  Packed<L> packed;
  packed.fill(0xAB);
  bc::PackVertical<B, L>(values.data(), packed.data());
  Values<L> out;
  bc::UnpackVertical<B, 0, L>(packed.data(), out.data());
  EXPECT_EQ(values, out) << "width " << B << " lanes " << L;
  bc::UnpackVertical<B, 1, L>(packed.data(), out.data());
  for (uint32_t i = 0; i != values.size(); ++i) {
    EXPECT_EQ(values[i] + 1, out[i])
      << "width " << B << " lanes " << L << " slot " << i;
  }
  if constexpr (L == bc::kWideLanes) {
    bc::UnpackWide<B, 0>(packed.data(), out.data());
    EXPECT_EQ(values, out) << "wide, width " << B;
    bc::UnpackWide<B, 1>(packed.data(), out.data());
    for (uint32_t i = 0; i != values.size(); ++i) {
      EXPECT_EQ(values[i] + 1, out[i]) << "wide, width " << B << " slot " << i;
    }
  }
}

template<uint32_t B>
void CheckHorizontal(std::mt19937& rng) {
  for (uint32_t len = 1; len != bc::kWideBlock; ++len) {
    std::vector<uint32_t> values(len);
    for (auto& v : values) {
      v = Draw<B>(rng);
    }
    Packed<bc::kWideLanes> packed;
    packed.fill(0xCD);
    bc::PackHorizontal<B>(values.data(), len, packed.data());
    std::vector<uint32_t> out(len + bc::kOutSlack);
    bc::UnpackHorizontal<B, 0>(packed.data(), len, out.data());
    out.resize(len);
    EXPECT_EQ(values, out) << "width " << B << " len " << len;
    const irs::doc_id_t prev = 5;
    std::vector<irs::doc_id_t> expected(len);
    auto doc = prev;
    for (uint32_t i = 0; i != len; ++i) {
      doc += values[i] + 1;
      expected[i] = doc;
    }
    std::vector<irs::doc_id_t> docs(len + bc::kOutSlack);
    bc::UnpackHorizontalDelta<B>(packed.data(), len, prev, docs.data());
    docs.resize(len);
    EXPECT_EQ(expected, docs) << "delta, width " << B << " len " << len;
  }
  if constexpr (B != 0) {
    std::array<uint32_t, bc::kWideLanes> group;
    for (auto& v : group) {
      v = Draw<B>(rng);
    }
    std::array<irs::byte_type, 32> expected{};
    bc::PackHorizontal<B>(group.data(), bc::kWideLanes, expected.data());
    bc::U32x8 lanes;
    std::memcpy(&lanes, group.data(), sizeof(lanes));
    std::array<irs::byte_type, 32> actual{};
    bc::PackGroupBits<(B > bc::kMaxPackGroupBits)>(B, lanes, actual.data());
    EXPECT_TRUE(
      std::equal(expected.begin(), expected.begin() + B, actual.begin()))
      << "group, width " << B;
  }
}

template<uint32_t B, uint32_t L>
void CheckVerticalDelta(std::mt19937& rng) {
  Values<L> gaps;
  for (auto& v : gaps) {
    v = Draw<B>(rng) >> 1;
  }
  Packed<L> packed;
  bc::PackVertical<B, L>(gaps.data(), packed.data());
  const irs::doc_id_t prev = 7;
  Values<L> expected;
  auto doc = prev;
  for (uint32_t i = 0; i != expected.size(); ++i) {
    doc += gaps[i] + 1;
    expected[i] = doc;
  }
  Values<L> out;
  bc::UnpackVerticalDelta<B, L>(packed.data(), prev, out.data());
  EXPECT_EQ(expected, out) << "fused, width " << B << " lanes " << L;
  bc::UnpackVertical<B, 0, L>(packed.data(), out.data());
  bc::ScanDocs(out.data(), bc::kBlockOf<L>, prev);
  EXPECT_EQ(expected, out) << "two pass, width " << B << " lanes " << L;
  if constexpr (L == bc::kWideLanes) {
    bc::UnpackVerticalDelta16<B>(packed.data(), prev, out.data());
    EXPECT_EQ(expected, out) << "fused 16 lanes, width " << B;
    bc::UnpackVertical<B, 0, L>(packed.data(), out.data());
    bc::ScanDocs16(out.data(), prev);
    EXPECT_EQ(expected, out) << "two pass 16 lanes, width " << B;
  }
}

template<uint32_t... B>
void CheckKernels(std::mt19937& rng, std::integer_sequence<uint32_t, B...>) {
  (CheckVertical<B, bc::kLanes>(rng), ...);
  (CheckVertical<B, bc::kWideLanes>(rng), ...);
  (CheckHorizontal<B>(rng), ...);
}

template<uint32_t... B>
void CheckDeltaKernels(std::mt19937& rng,
                       std::integer_sequence<uint32_t, B...>) {
  (CheckVerticalDelta<B, bc::kLanes>(rng), ...);
  (CheckVerticalDelta<B, bc::kWideLanes>(rng), ...);
}

std::vector<irs::doc_id_t> MakeDocs(std::mt19937& rng, uint32_t len,
                                    irs::doc_id_t prev, uint32_t max_gap,
                                    uint32_t outliers, uint32_t outlier_gap) {
  std::uniform_int_distribution<uint32_t> gap{1, max_gap};
  std::uniform_int_distribution<uint32_t> slot{0, len - 1};
  std::vector<uint32_t> gaps(len);
  for (auto& g : gaps) {
    g = gap(rng);
  }
  for (uint32_t i = 0; i != outliers; ++i) {
    gaps[slot(rng)] = outlier_gap;
  }
  std::vector<irs::doc_id_t> docs(len);
  for (uint32_t i = 0; i != len; ++i) {
    prev += gaps[i];
    docs[i] = prev;
  }
  return docs;
}

template<typename Codec>
using Buffer = std::array<irs::byte_type, Codec::kMaxBlockBytes + kSlack>;

template<typename Codec>
void CheckDocs(const std::vector<irs::doc_id_t>& docs, irs::doc_id_t prev,
               const bc::EncodeOptions& options) {
  const auto len = static_cast<uint32_t>(docs.size());
  const bool full = len == Codec::kBlock;
  Buffer<Codec> encoded;
  const auto size =
    full
      ? Codec::EncodeDeltaBlock(docs.data(), prev, encoded.data(), options)
      : Codec::EncodeDeltaTail(docs.data(), len, prev, encoded.data(), options);
  ASSERT_LE(size, Codec::kMaxBlockBytes);
  EXPECT_EQ(size, full ? Codec::DeltaBlockSize(encoded.data())
                       : Codec::DeltaTailSize(encoded.data(), len));
  std::vector<irs::doc_id_t> out(len + bc::kOutSlack);
  const auto* end =
    full ? Codec::DecodeDeltaBlock(encoded.data(), prev, out.data())
         : Codec::DecodeDeltaTail(encoded.data(), len, prev, out.data());
  EXPECT_EQ(encoded.data() + size, end);
  out.resize(len);
  EXPECT_EQ(docs, out) << "len " << len << " token " << uint32_t{encoded[0]};
  std::vector<irs::doc_id_t> portable(len + bc::kOutSlack);
  const auto* portable_end =
    full ? bc::kDeltaBlockDecoders<Codec::kLanes, false>[encoded[0]](
             encoded.data(), prev, portable.data())
         : bc::kDeltaTailDecoders<Codec::kLanes, false>[encoded[0]](
             encoded.data(), len, prev, portable.data());
  EXPECT_EQ(encoded.data() + size, portable_end);
  portable.resize(len);
  EXPECT_EQ(docs, portable)
    << "portable, len " << len << " token " << uint32_t{encoded[0]};
}

template<typename Codec>
void CheckValues(const std::vector<uint32_t>& values,
                 const bc::EncodeOptions& options) {
  const auto len = static_cast<uint32_t>(values.size());
  const bool full = len == Codec::kBlock;
  Buffer<Codec> encoded;
  const auto size =
    full ? Codec::EncodeValuesBlock(values.data(), encoded.data(), options)
         : Codec::EncodeValuesTail(values.data(), len, encoded.data(), options);
  ASSERT_LE(size, Codec::kMaxBlockBytes);
  EXPECT_EQ(size, full ? Codec::ValuesBlockSize(encoded.data())
                       : Codec::ValuesTailSize(encoded.data(), len));
  std::vector<uint32_t> out(len + bc::kOutSlack);
  const auto* end =
    full ? Codec::DecodeValuesBlock(encoded.data(), out.data())
         : Codec::DecodeValuesTail(encoded.data(), len, out.data());
  EXPECT_EQ(encoded.data() + size, end);
  out.resize(len);
  EXPECT_EQ(values, out) << "len " << len << " token " << uint32_t{encoded[0]};
}

template<typename Codec>
std::vector<uint32_t> Lengths() {
  return {1,
          2,
          3,
          7,
          8,
          9,
          16,
          17,
          31,
          32,
          33,
          63,
          64,
          100,
          127,
          128,
          Codec::kBlock - 1,
          Codec::kBlock};
}

std::vector<bc::EncodeOptions> AllOptions() {
  return {
    {},
    {.patch16 = false, .patch32 = false, .patch_mixed = false},
    {.patch16 = false,
     .patch32 = false,
     .patch_mixed = false,
     .patch_bitmap = false},
    {.patch16 = true, .patch32 = false, .patch_mixed = false},
    {.patch16 = false, .patch32 = true, .patch_mixed = false},
    {.patch16 = false, .patch32 = false, .patch_mixed = true},
    {.patch_bitmap = false},
    {.bitset_margin_percent = 0},
    {.bitset_margin_percent = 1000},
    {.bitset = false},
    {.minus_one = false},
  };
}

TEST(BlockKernelsTest, VerticalAndHorizontalRoundTrip) {
  std::mt19937 rng{42};
  CheckKernels(rng, std::make_integer_sequence<uint32_t, kWidths>{});
}

TEST(BlockKernelsTest, VerticalDeltaRoundTrip) {
  std::mt19937 rng{43};
  CheckDeltaKernels(rng, std::make_integer_sequence<uint32_t, kWidths>{});
}

TEST(BlockCodecTest, ExactBitWidths) {
  std::vector<uint32_t> values;
  for (uint32_t k = 1; k != 32; ++k) {
    const uint64_t power = uint64_t{1} << k;
    for (const uint64_t v : {power - 2, power - 1, power, power + 1}) {
      if (v <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
        values.push_back(static_cast<uint32_t>(v));
      }
    }
  }
  while (values.size() % 8 != 0) {
    values.push_back(std::numeric_limits<int32_t>::max());
  }
  for (size_t i = 0; i != values.size(); i += 8) {
    const auto widths = bc::BitWidths<true>(values.data() + i);
    for (uint32_t k = 0; k != 8; ++k) {
      EXPECT_EQ(std::bit_width(values[i + k]), static_cast<uint32_t>(widths[k]))
        << values[i + k];
    }
  }
}

template<typename Codec>
class BlockCodecTest : public ::testing::Test {};

using Codecs = ::testing::Types<bc::Codec128, bc::Codec256>;
TYPED_TEST_SUITE(BlockCodecTest, Codecs);

TYPED_TEST(BlockCodecTest, DocsRoundTrip) {
  std::mt19937 rng{44};
  for (const auto& options : AllOptions()) {
    for (const auto len : Lengths<TypeParam>()) {
      for (const irs::doc_id_t prev : {0U, 1U, 1000U, 50'000'000U}) {
        for (const uint32_t max_gap :
             {1U, 2U, 3U, 16U, 64U, 1000U, 70'000U, 1U << 20}) {
          for (const uint32_t outliers : {0U, 1U, 3U, 20U}) {
            for (const uint32_t outlier_gap :
                 {5'000U, 3'000'000U, 2'000'000'000U}) {
              if (uint64_t{prev} + uint64_t{len} * max_gap +
                    uint64_t{outliers} * outlier_gap >
                  std::numeric_limits<int32_t>::max()) {
                continue;
              }
              CheckDocs<TypeParam>(
                MakeDocs(rng, len, prev, max_gap, outliers, outlier_gap), prev,
                options);
            }
          }
        }
      }
    }
  }
}

TYPED_TEST(BlockCodecTest, DocsFirstBlockOfTerm) {
  std::mt19937 rng{45};
  for (const auto len : Lengths<TypeParam>()) {
    auto docs = MakeDocs(rng, len, 50'000'000, 64, 0, 0);
    CheckDocs<TypeParam>(docs, 0, {});
  }
}

TYPED_TEST(BlockCodecTest, ValuesRoundTrip) {
  std::mt19937 rng{46};
  for (const auto& options : AllOptions()) {
    for (const auto len : Lengths<TypeParam>()) {
      for (const uint32_t base : {0U, 1U}) {
        for (const uint32_t max :
             {1U, 2U, 4U, 10U, 300U, 70'000U, 0x7FFFFFFFU}) {
          for (const uint32_t outliers : {0U, 1U, 5U, 40U}) {
            std::uniform_int_distribution<uint32_t> value{base, max};
            std::uniform_int_distribution<uint32_t> slot{0, len - 1};
            std::vector<uint32_t> values(len);
            for (auto& v : values) {
              v = value(rng);
            }
            for (uint32_t i = 0; i != outliers; ++i) {
              values[slot(rng)] = 1'000'000 + value(rng) % 1000;
            }
            CheckValues<TypeParam>(values, options);
          }
        }
      }
    }
  }
}

TYPED_TEST(BlockCodecTest, ChosenEncodings) {
  constexpr uint32_t kN = TypeParam::kBlock;
  Buffer<TypeParam> encoded;
  std::vector<irs::doc_id_t> docs(kN);
  for (uint32_t i = 0; i != kN; ++i) {
    docs[i] = 10 + i;
  }
  EXPECT_EQ(1U, TypeParam::EncodeDeltaBlock(docs.data(), 9, encoded.data()));
  EXPECT_EQ(static_cast<irs::byte_type>(bc::DeltaEncoding::Run), encoded[0]);

  for (uint32_t i = 0; i != kN; ++i) {
    docs[i] = 12 + 3 * i;
  }
  EXPECT_EQ(2U, TypeParam::EncodeDeltaBlock(docs.data(), 9, encoded.data()));
  EXPECT_EQ(static_cast<irs::byte_type>(bc::DeltaEncoding::Same08), encoded[0]);

  for (uint32_t i = 0, doc = 0; i != kN; ++i) {
    doc += i % 16 == 15 ? 2 : 1;
    docs[i] = doc;
  }
  EXPECT_EQ(1 + kN / 8,
            TypeParam::EncodeDeltaBlock(docs.data(), 0, encoded.data(),
                                        {.bitset_margin_percent = 0}));
  EXPECT_EQ(static_cast<irs::byte_type>(bc::DeltaEncoding::Pack), encoded[0]);
  EXPECT_EQ(2 + 8 * ((kN + kN / 16 + 63) / 64),
            TypeParam::EncodeDeltaBlock(docs.data(), 0, encoded.data(),
                                        {.bitset_margin_percent = 1000}));
  EXPECT_EQ(static_cast<irs::byte_type>(bc::DeltaEncoding::Bitset), encoded[0]);

  for (uint32_t i = 0, doc = 0; i != kN; ++i) {
    doc += i % 4 == 3 ? 5 : 1;
    docs[i] = doc;
  }
  EXPECT_EQ(2 + kN / 4,
            TypeParam::EncodeDeltaBlock(docs.data(), 0, encoded.data()));
  EXPECT_EQ(static_cast<irs::byte_type>(bc::DeltaEncoding::Bitset), encoded[0]);

  docs.assign({1U, 2'000'000'002U});
  EXPECT_EQ(9U, TypeParam::EncodeDeltaTail(
                  docs.data(), 2, 0, encoded.data(),
                  {.patch16 = false, .patch32 = false, .patch_mixed = false}));
  EXPECT_EQ(static_cast<uint32_t>(bc::DeltaEncoding::Pack) + 30, encoded[0]);

  std::vector<uint32_t> freqs(kN, 1);
  EXPECT_EQ(1U, TypeParam::EncodeValuesBlock(freqs.data(), encoded.data()));
  EXPECT_EQ(static_cast<irs::byte_type>(bc::ValueEncoding::One), encoded[0]);

  for (uint32_t i = 0; i != kN; ++i) {
    freqs[i] = 1 + i % 4;
  }
  freqs[17] = 40;
  freqs[90] = 100;
  EXPECT_EQ(6 + kN / 4,
            TypeParam::EncodeValuesBlock(freqs.data(), encoded.data()));
  EXPECT_EQ(static_cast<irs::byte_type>(bc::ValueEncoding::Patch16MinusOne) + 2,
            encoded[0]);

  for (uint32_t i = 0; i != kN; ++i) {
    freqs[i] = 1 + i % 4;
  }
  EXPECT_EQ(1 + kN / 4,
            TypeParam::EncodeValuesBlock(freqs.data(), encoded.data()));
  EXPECT_EQ(static_cast<uint32_t>(bc::ValueEncoding::PackMinusOne) + 1,
            encoded[0]);

  for (uint32_t i = 0; i != kN; ++i) {
    freqs[i] = i % 3;
  }
  EXPECT_EQ(1 + kN / 4,
            TypeParam::EncodeValuesBlock(freqs.data(), encoded.data()));
  EXPECT_EQ(static_cast<uint32_t>(bc::ValueEncoding::Pack) + 1, encoded[0]);
}

}  // namespace
