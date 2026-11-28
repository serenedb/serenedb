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

#include <iresearch/formats/column/codecs/bytes/lz4.hpp>
#include <iresearch/formats/column/codecs/bytes/zstd.hpp>
#include <iresearch/formats/column/codecs/bytes/zxc.hpp>
#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "tests_shared.hpp"

namespace {

using irs::codecs::Lz4;
using irs::codecs::Zstd;
using irs::codecs::Zxc;

constexpr size_t kBeyondInt = size_t{std::numeric_limits<int>::max()} + 1;

std::string Text(size_t size, uint64_t seed) {
  std::string out;
  out.reserve(size);
  for (size_t i = 0; out.size() < size; ++i) {
    out += "row ";
    out += std::to_string((i * 7919 + seed) % 1000);
    out += " of a log line with some repeated words; ";
  }
  out.resize(size);
  return out;
}

template<typename Codec>
std::string Squeeze(typename Codec::Compressor& compressor,
                    std::string_view raw) {
  std::string out(Codec::Bound(raw.size()), '\0');
  out.resize(
    compressor.Compress(raw.data(), raw.size(), out.data(), out.size()));
  return out;
}

template<typename Codec>
void ExpectNoDictionaryFromTooLittleData(uint8_t level = Codec::kDefaultLevel) {
  typename Codec::Compressor compressor{TestAlloc(), level};
  const std::string frames = "abc";
  const size_t sizes[] = {frames.size()};
  EXPECT_FALSE(compressor.Train(frames, sizes));
  EXPECT_TRUE(compressor.Dictionary().empty());
  EXPECT_TRUE(compressor.Stored().empty());
}

TEST(ColByteCodecsTest, TooLittleDataTrainsNoDictionary) {
  ExpectNoDictionaryFromTooLittleData<Lz4>();
  ExpectNoDictionaryFromTooLittleData<Lz4>(LZ4HC_CLEVEL_DEFAULT);
  ExpectNoDictionaryFromTooLittleData<Zstd>();
  ExpectNoDictionaryFromTooLittleData<Zxc>();
}

TEST(ColByteCodecsTest, Lz4RefusesSizesBeyondAnInt) {
  const char src[16] = {};
  char dst[16];
  EXPECT_FALSE(Lz4::Decompressor::Decompress(src, kBeyondInt, dst, 16));
  EXPECT_FALSE(Lz4::Decompressor::Decompress(src, 16, dst, kBeyondInt));
  EXPECT_EQ(Lz4::Decompressor::DecompressPrefix(src, kBeyondInt, dst, 8, 16),
            0U);
  EXPECT_EQ(Lz4::Decompressor::DecompressPrefix(src, 16, dst, 8, kBeyondInt),
            0U);
  const auto dictionary = Text(1000, 1);
  Lz4::Compressor plain{TestAlloc(), 1};
  const auto frame = Squeeze<Lz4>(plain, dictionary);
  Lz4::Decompressor decompressor{TestAlloc()};
  ASSERT_TRUE(
    decompressor.LoadFrame(frame.data(), frame.size(), dictionary.size()));
  EXPECT_TRUE(decompressor.Inflate(src, kBeyondInt, dst, 16, 16).empty());
  EXPECT_TRUE(decompressor.Inflate(src, 16, dst, 16, kBeyondInt).empty());
}

TEST(ColByteCodecsTest, Lz4RefusesCorruptedFrames) {
  const auto raw = Text(4000, 2);
  Lz4::Compressor plain{TestAlloc(), 1};
  auto frame = Squeeze<Lz4>(plain, raw);
  std::string out(raw.size(), '\0');
  const auto cut = frame.substr(0, frame.size() / 4);
  EXPECT_EQ(Lz4::Decompressor::DecompressPrefix(
              cut.data(), cut.size(), out.data(), raw.size() / 2, raw.size()),
            0U);
  Lz4::Decompressor bare{TestAlloc()};
  EXPECT_TRUE(
    bare.Inflate(cut.data(), cut.size(), out.data(), raw.size(), raw.size())
      .empty());
  const auto dictionary = Text(1000, 3);
  Lz4::Compressor attached{TestAlloc(), 1};
  attached.Use(dictionary);
  const auto with_dictionary = Squeeze<Lz4>(attached, raw);
  const auto dictionary_frame = Squeeze<Lz4>(plain, dictionary);
  Lz4::Decompressor decompressor{TestAlloc()};
  ASSERT_TRUE(decompressor.LoadFrame(
    dictionary_frame.data(), dictionary_frame.size(), dictionary.size()));
  ASSERT_EQ(decompressor.Inflate(with_dictionary.data(), with_dictionary.size(),
                                 out.data(), raw.size(), raw.size()),
            raw);
  const auto broken = with_dictionary.substr(0, with_dictionary.size() / 4);
  EXPECT_TRUE(
    decompressor
      .Inflate(broken.data(), broken.size(), out.data(), raw.size(), raw.size())
      .empty());
  EXPECT_FALSE(decompressor.LoadFrame(cut.data(), cut.size(), raw.size()));
  EXPECT_TRUE(decompressor.Dictionary().empty());
}

TEST(ColByteCodecsTest, ZstdRefusesCorruptedFrames) {
  const auto raw = Text(4000, 4);
  Zstd::Compressor plain{TestAlloc(), 1};
  const auto frame = Squeeze<Zstd>(plain, raw);
  std::string out(raw.size() + 100, '\0');
  const auto cut = frame.substr(0, frame.size() / 2);
  Zstd::Decompressor bare{TestAlloc()};
  EXPECT_TRUE(
    bare.Inflate(cut.data(), cut.size(), out.data(), raw.size(), raw.size())
      .empty());
  const auto dictionary = Text(1000, 5);
  Zstd::Compressor attached{TestAlloc(), 1};
  attached.Use(dictionary);
  const auto with_dictionary = Squeeze<Zstd>(attached, raw);
  const auto dictionary_frame = Squeeze<Zstd>(plain, dictionary);
  Zstd::Decompressor decompressor{TestAlloc()};
  ASSERT_TRUE(decompressor.LoadFrame(
    dictionary_frame.data(), dictionary_frame.size(), dictionary.size()));
  ASSERT_EQ(decompressor.Inflate(with_dictionary.data(), with_dictionary.size(),
                                 out.data(), raw.size(), raw.size()),
            raw);
  EXPECT_TRUE(decompressor
                .Inflate(with_dictionary.data(), with_dictionary.size() / 2,
                         out.data(), raw.size(), raw.size())
                .empty());
  EXPECT_TRUE(decompressor
                .Inflate(with_dictionary.data(), with_dictionary.size(),
                         out.data(), raw.size() + 100, raw.size() + 100)
                .empty());
}

TEST(ColByteCodecsTest, ZstdAttachesAShorterDictionaryAtTheSameAddress) {
  const auto raw = Text(4000, 6);
  const auto longer = Text(2000, 7);
  const auto shorter = Text(1000, 8);
  Zstd::Compressor plain{TestAlloc(), 1};
  Zstd::Compressor with_longer{TestAlloc(), 1};
  with_longer.Use(longer);
  Zstd::Compressor with_shorter{TestAlloc(), 1};
  with_shorter.Use(shorter);
  std::string out(raw.size(), '\0');
  Zstd::Decompressor decompressor{TestAlloc()};
  const auto longer_frame = Squeeze<Zstd>(plain, longer);
  ASSERT_TRUE(decompressor.LoadFrame(longer_frame.data(), longer_frame.size(),
                                     longer.size()));
  const auto first = Squeeze<Zstd>(with_longer, raw);
  ASSERT_EQ(decompressor.Inflate(first.data(), first.size(), out.data(),
                                 raw.size(), raw.size()),
            raw);
  const auto* address = decompressor.Dictionary().data();
  const auto shorter_frame = Squeeze<Zstd>(plain, shorter);
  ASSERT_TRUE(decompressor.LoadFrame(shorter_frame.data(), shorter_frame.size(),
                                     shorter.size()));
  ASSERT_EQ(decompressor.Dictionary().data(), address);
  const auto second = Squeeze<Zstd>(with_shorter, raw);
  EXPECT_EQ(decompressor.Inflate(second.data(), second.size(), out.data(),
                                 raw.size(), raw.size()),
            raw);
}

TEST(ColByteCodecsTest, ZxcRefusesCorruptedFrames) {
  const auto raw = Text(4000, 9);
  Zxc::Compressor plain{TestAlloc(), Zxc::kDefaultLevel};
  const auto frame = Squeeze<Zxc>(plain, raw);
  std::string out(raw.size() + 100, '\0');
  Zxc::Decompressor bare{TestAlloc()};
  EXPECT_FALSE(
    bare.Decompress(frame.data(), frame.size() / 2, out.data(), raw.size()));
  EXPECT_FALSE(
    bare.Decompress(frame.data(), frame.size(), out.data(), raw.size() + 100));
  const auto dictionary = Text(1000, 10);
  Zxc::Compressor attached{TestAlloc(), Zxc::kDefaultLevel};
  attached.Use(dictionary);
  const auto with_dictionary = Squeeze<Zxc>(attached, raw);
  const auto dictionary_frame = Squeeze<Zxc>(plain, dictionary);
  Zxc::Decompressor decompressor{TestAlloc()};
  ASSERT_TRUE(decompressor.LoadFrame(
    dictionary_frame.data(), dictionary_frame.size(), dictionary.size()));
  ASSERT_EQ(decompressor.Inflate(with_dictionary.data(), with_dictionary.size(),
                                 out.data(), raw.size(), raw.size()),
            raw);
  EXPECT_TRUE(decompressor
                .Inflate(with_dictionary.data(), with_dictionary.size() / 2,
                         out.data(), raw.size(), raw.size())
                .empty());
}

TEST(ColByteCodecsTest, ZxcUsesTheTailOfAnOversizedDictionary) {
  const auto raw = Text(4000, 11);
  const auto dictionary = Text(3 * Zxc::kDictionaryCap, 12);
  Zxc::Compressor attached{TestAlloc(), Zxc::kDefaultLevel};
  attached.Use(dictionary);
  const auto with_dictionary = Squeeze<Zxc>(attached, raw);
  Zxc::Compressor plain{TestAlloc(), Zxc::kDefaultLevel};
  const auto dictionary_frame = Squeeze<Zxc>(plain, dictionary);
  Zxc::Decompressor decompressor{TestAlloc()};
  ASSERT_TRUE(decompressor.LoadFrame(
    dictionary_frame.data(), dictionary_frame.size(), dictionary.size()));
  std::string out(raw.size(), '\0');
  EXPECT_EQ(decompressor.Inflate(with_dictionary.data(), with_dictionary.size(),
                                 out.data(), raw.size(), raw.size()),
            raw);
}

}  // namespace
