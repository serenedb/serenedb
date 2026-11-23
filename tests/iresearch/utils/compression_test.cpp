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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <cstring>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <iresearch/utils/compression.hpp>
#include <magic_enum/magic_enum.hpp>
#include <random>
#include <thread>
#include <vector>

#include "tests_shared.hpp"

namespace {

namespace compression = irs::compression;

using Bytes = std::vector<irs::byte_type>;

constexpr uint32_t kFrame = 4096;
constexpr std::array kMethods{compression::kLz4, compression::kLz4Hc9,
                              compression::kZstd1};

Bytes Text(size_t size) {
  std::string text;
  for (size_t row = 0; text.size() < size; ++row) {
    absl::StrAppend(&text, "row ", row % 97, " url http://example.com/",
                    row % 13, " title ", row % 7, "\n");
  }
  return Bytes(text.begin(), text.begin() + static_cast<ptrdiff_t>(size));
}

Bytes Noise(size_t size) {
  std::mt19937_64 random{size};
  Bytes noise(size);
  for (auto& byte : noise) {
    byte = static_cast<irs::byte_type>(random());
  }
  return noise;
}

irs::bytes_view View(const Bytes& bytes) {
  return {bytes.data(), bytes.size()};
}

void Threads(size_t count, absl::FunctionRef<void(size_t)> body) {
  std::vector<std::thread> threads;
  threads.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    threads.emplace_back([&body, i] { body(i); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

irs::bstring Stored(const compression::Compressed& compressed) {
  irs::bstring stored;
  compression::ForEachStored(compressed.frames, compressed.slots.get(),
                             [&](irs::bytes_view frame) { stored += frame; });
  return stored;
}

std::string WritePacked(const compression::Compressed& packed) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  compression::WritePacked(serializer, packed);
  serializer.End();
  return {reinterpret_cast<const char*>(stream.GetData()),
          stream.GetPosition()};
}

compression::Packed ReadPacked(std::string& bytes) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(bytes.data()), bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  auto packed = compression::ReadPacked(deserializer, stream);
  deserializer.End();
  EXPECT_EQ(stream.GetPosition(), bytes.size());
  return packed;
}

compression::Frames ReadFrames(std::string bytes) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(bytes.data()), bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  auto frames = compression::ReadFrames(deserializer);
  deserializer.End();
  return frames;
}

Bytes RoundTrip(compression::Method method, const Bytes& raw) {
  const auto compressed = compression::Compress(method, View(raw), kFrame);
  auto bytes = WritePacked(compressed);
  const auto packed = ReadPacked(bytes);
  EXPECT_EQ(packed.frames.codec, method.codec);
  EXPECT_EQ(packed.frames.raw_bytes, raw.size());
  EXPECT_EQ(packed.frames.frame_bytes, kFrame);
  EXPECT_EQ(packed.frames.sizes, compressed.frames.sizes);
  EXPECT_TRUE(packed.frames.raw_sizes.empty());
  EXPECT_TRUE(packed.stored == Stored(compressed));
  const auto decoded = compression::Decompress(packed);
  return Bytes(decoded.get(), decoded.get() + raw.size());
}

TEST(Compression, RoundTripsEveryCodecAcrossFrameBoundaries) {
  for (const auto method : kMethods) {
    for (const size_t size : {size_t{0}, size_t{1}, size_t{1000},
                              size_t{kFrame}, size_t{kFrame} * 3 + 17}) {
      const auto raw = Text(size);
      EXPECT_EQ(RoundTrip(method, raw), raw)
        << magic_enum::enum_name(method.codec) << " " << size;
    }
  }
}

TEST(Compression, CompressesEveryFullFrame) {
  for (const auto method : kMethods) {
    const auto raw = Text(size_t{kFrame} * 3 + 17);
    const auto compressed = compression::Compress(method, View(raw), kFrame);
    ASSERT_EQ(compressed.frames.sizes.size(), 4u);
    for (size_t frame = 0; frame != 3; ++frame) {
      EXPECT_LT(compressed.frames.sizes[frame], kFrame / 2)
        << magic_enum::enum_name(method.codec) << " frame " << frame;
    }
  }
}

TEST(Compression, StoresFramesThatDoNotShrinkRaw) {
  for (const auto method : kMethods) {
    auto raw = Text(kFrame);
    const auto noise = Noise(kFrame + 100);
    raw.insert(raw.end(), noise.begin(), noise.end());
    const auto compressed = compression::Compress(method, View(raw), kFrame);
    ASSERT_EQ(compressed.frames.sizes.size(), 3u);
    EXPECT_LT(compressed.frames.sizes[0], kFrame);
    EXPECT_EQ(compressed.frames.sizes[1], kFrame);
    EXPECT_EQ(compressed.frames.sizes[2], 100u);
    EXPECT_EQ(RoundTrip(method, raw), raw);
  }
}

TEST(Compression, FramesCompressAndDecompressInParallel) {
  for (const auto method : kMethods) {
    auto raw = Text(size_t{kFrame} * 8 + 5);
    const auto noise = Noise(kFrame);
    std::copy(noise.begin(), noise.end(), raw.begin() + 3 * kFrame);
    const auto serial = compression::Compress(method, View(raw), kFrame);
    const auto parallel =
      compression::Compress(method, View(raw), kFrame, Threads);
    ASSERT_EQ(parallel.frames.sizes, serial.frames.sizes);
    EXPECT_TRUE(Stored(parallel) == Stored(serial));
    Bytes decoded(raw.size());
    compression::Decompress(parallel.frames, Stored(parallel), decoded,
                            Threads);
    EXPECT_EQ(decoded, raw);
  }
}

TEST(Compression, PaysOnlyWhenThePackedFormIsSmaller) {
  for (const auto method : kMethods) {
    for (const size_t size : {size_t{1}, size_t{16}, size_t{64}, size_t{256},
                              size_t{kFrame}, size_t{kFrame} * 3 + 5}) {
      for (const auto& raw : {Text(size), Noise(size)}) {
        const auto compressed =
          compression::Compress(method, View(raw), kFrame);
        EXPECT_EQ(compression::PackedBytes(compressed),
                  WritePacked(compressed).size());
        EXPECT_EQ(compression::Pays(compressed),
                  WritePacked(compressed).size() < raw.size())
          << magic_enum::enum_name(method.codec) << " " << size;
      }
    }
    EXPECT_FALSE(
      compression::Pays(compression::Compress(method, View(Text(16)), kFrame)));
    EXPECT_FALSE(compression::Pays(
      compression::Compress(method, View(Noise(kFrame)), kFrame)));
    EXPECT_TRUE(compression::Pays(
      compression::Compress(method, View(Text(kFrame)), kFrame)));
  }
}

TEST(Compression, CompressesIntoReusedSlots) {
  Bytes slots(size_t{kFrame} * 4);
  for (const auto method : kMethods) {
    for (const size_t size : {size_t{kFrame} * 4, size_t{kFrame} * 2 + 9}) {
      auto raw = Text(size);
      const auto noise = Noise(kFrame);
      std::copy(noise.begin(), noise.end(), raw.begin() + kFrame);
      const auto owned = compression::Compress(method, View(raw), kFrame);
      const auto frames =
        compression::Compress(method, View(raw), kFrame, slots, Threads);
      ASSERT_EQ(frames.sizes, owned.frames.sizes);
      EXPECT_EQ(frames.raw_bytes, raw.size());
      irs::bstring stored;
      compression::ForEachStored(frames, slots.data(),
                                 [&](irs::bytes_view f) { stored += f; });
      EXPECT_TRUE(stored == Stored(owned));
      Bytes decoded(raw.size());
      compression::Decompress(frames, stored, decoded);
      EXPECT_EQ(decoded, raw);
    }
  }
}

constexpr std::array kEven{size_t{kFrame}};
constexpr std::array kUneven{size_t{1000}, size_t{7}, size_t{kFrame}, size_t{1},
                             size_t{kFrame} - 3};

std::vector<irs::bytes_view> Frames(const Bytes& raw,
                                    std::span<const size_t> sizes) {
  std::vector<irs::bytes_view> frames;
  size_t at = 0;
  for (size_t frame = 0; at < raw.size(); ++frame) {
    const auto take = std::min(sizes[frame % sizes.size()], raw.size() - at);
    frames.push_back(View(raw).substr(at, take));
    at += take;
  }
  return frames;
}

bool AllButLastFull(std::span<const irs::bytes_view> frames) {
  return frames.empty() || absl::c_all_of(frames.first(frames.size() - 1),
                                          [](irs::bytes_view frame) {
                                            return frame.size() == kFrame;
                                          });
}

compression::Compressed CompressOne(compression::Method method,
                                    std::span<const irs::bytes_view> frames) {
  const std::vector<std::vector<irs::bytes_view>> blobs{
    {frames.begin(), frames.end()}};
  return std::move(
    compression::Compress(method, kFrame, blobs, Threads).front());
}

Bytes PackedRoundTrip(const compression::Compressed& compressed) {
  auto bytes = WritePacked(compressed);
  const auto packed = ReadPacked(bytes);
  EXPECT_EQ(packed.frames.sizes, compressed.frames.sizes);
  EXPECT_EQ(packed.frames.raw_sizes, compressed.frames.raw_sizes);
  const auto decoded = compression::Decompress(packed, Threads);
  return Bytes(decoded.get(), decoded.get() + packed.frames.raw_bytes);
}

TEST(Compression, FrameListsRoundTrip) {
  for (const auto method : kMethods) {
    for (const size_t size :
         {size_t{0}, size_t{1}, size_t{kFrame}, size_t{kFrame} * 3 + 17,
          size_t{kFrame} * 20 + 3}) {
      const auto raw = Text(size);
      for (const std::span<const size_t> sizes :
           {std::span<const size_t>{kEven}, std::span<const size_t>{kUneven}}) {
        const auto frames = Frames(raw, sizes);
        const auto compressed = CompressOne(method, frames);
        EXPECT_EQ(compressed.frames.raw_bytes, raw.size());
        EXPECT_EQ(compressed.frames.frame_bytes, kFrame);
        EXPECT_EQ(compressed.frames.sizes.size(), frames.size());
        EXPECT_EQ(compressed.frames.raw_sizes.empty(), AllButLastFull(frames));
        EXPECT_EQ(PackedRoundTrip(compressed), raw)
          << magic_enum::enum_name(method.codec) << " " << size << " "
          << sizes.size();
      }
    }
  }
}

TEST(Compression, KeepPlainTurnsTheSlotsBackIntoTheStream) {
  auto mixed = Text(size_t{kFrame} * 2 + 9);
  const auto noise = Noise(size_t{kFrame} + 100);
  mixed.insert(mixed.end(), noise.begin(), noise.end());
  for (const auto method : kMethods) {
    for (const auto& raw : {Noise(100), Noise(size_t{kFrame} * 3 + 17),
                            Text(size_t{kFrame} * 3 + 17), mixed}) {
      const auto frames = Frames(raw, kUneven);
      auto compressed = CompressOne(method, frames);
      ASSERT_FALSE(compression::IsPlain(compressed));
      compression::KeepPlain(compressed, frames);
      EXPECT_TRUE(compression::IsPlain(compressed));
      EXPECT_TRUE(compressed.frames.raw_sizes.empty());
      ASSERT_EQ(compressed.frames.raw_bytes, raw.size());
      EXPECT_EQ(
        Bytes(compressed.slots.get(), compressed.slots.get() + raw.size()), raw)
        << magic_enum::enum_name(method.codec) << " " << raw.size();
    }
  }
}

TEST(Compression, UnevenFramesKeepTheirSizes) {
  const auto raw = Text(size_t{kFrame} * 4 + 50);
  constexpr std::array kSizes{size_t{100}, size_t{kFrame}, size_t{kFrame},
                              size_t{kFrame}, size_t{kFrame} - 50};
  const auto compressed = CompressOne(compression::kZstd1, Frames(raw, kSizes));
  const absl::InlinedVector<uint32_t, 1> expected{100, kFrame, kFrame, kFrame,
                                                  kFrame - 50};
  EXPECT_EQ(compressed.frames.raw_sizes, expected);
  EXPECT_EQ(PackedRoundTrip(compressed), raw);
}

TEST(Compression, CompressesEveryBlobInOnePass) {
  for (const auto method : kMethods) {
    std::vector<Bytes> raws{Text(size_t{kFrame} * 3 + 17), Text(1),
                            Noise(size_t{kFrame} * 2),
                            Text(size_t{kFrame} * 5)};
    std::vector<std::vector<irs::bytes_view>> blobs;
    for (const auto& raw : raws) {
      blobs.push_back(Frames(raw, kUneven));
    }
    const auto together = compression::Compress(method, kFrame, blobs, Threads);
    ASSERT_EQ(together.size(), blobs.size());
    for (size_t i = 0; i != blobs.size(); ++i) {
      const auto alone = CompressOne(method, blobs[i]);
      EXPECT_EQ(together[i].frames.sizes, alone.frames.sizes) << i;
      EXPECT_EQ(together[i].frames.raw_sizes, alone.frames.raw_sizes) << i;
      EXPECT_TRUE(Stored(together[i]) == Stored(alone)) << i;
      EXPECT_EQ(PackedRoundTrip(together[i]), raws[i]) << i;
    }
  }
}

Bytes Lists(uint32_t slots, size_t lists, uint32_t max_id, uint64_t seed) {
  std::mt19937_64 random{seed};
  std::vector<uint32_t> ids(size_t{slots} * lists, compression::kStripPadding);
  for (size_t list = 0; list != lists; ++list) {
    const auto count = random() % (uint64_t{slots} + 1);
    for (size_t slot = 0; slot != count; ++slot) {
      ids[list * slots + slot] =
        static_cast<uint32_t>(random() % (uint64_t{max_id} + 1));
    }
  }
  const auto* bytes = reinterpret_cast<const irs::byte_type*>(ids.data());
  return Bytes(bytes, bytes + ids.size() * sizeof(uint32_t));
}

Bytes Decoded(const compression::Compressed& compressed) {
  Bytes decoded(compressed.frames.raw_bytes);
  compression::Decompress(compressed.frames, Stored(compressed), decoded,
                          Threads);
  return decoded;
}

TEST(Compression, StripRoundTripsListsOfEveryWidth) {
  for (const uint32_t slots : {1U, 3U, 64U, 300U}) {
    for (const uint32_t max_id : {0U, 1U, 1000U, 0xFFFFFFFEU}) {
      const auto raw = Lists(slots, 500, max_id, slots * 31 + max_id);
      const auto frame = slots * uint32_t{sizeof(uint32_t)} * 37;
      const auto compressed = compression::Compress(compression::Strip(slots),
                                                    View(raw), frame, Threads);
      EXPECT_EQ(compressed.frames.codec, compression::Codec::Strip);
      EXPECT_EQ(Decoded(compressed), raw) << slots << " " << max_id;
      if (max_id <= 1000) {
        EXPECT_LT(compression::StoredBytes(compressed.frames), raw.size() / 2)
          << slots << " " << max_id;
      }
    }
  }
}

TEST(Compression, StripStoresAFrameWithPaddingBeforeAnIdRaw) {
  constexpr uint32_t kSlots = 4;
  auto raw = Lists(kSlots, 64, 100, 7);
  const std::array<uint32_t, kSlots> gap{5, compression::kStripPadding, 7,
                                         compression::kStripPadding};
  std::memcpy(raw.data() + kSlots * sizeof(uint32_t) * 40, gap.data(),
              sizeof(gap));
  const auto frame = kSlots * uint32_t{sizeof(uint32_t)} * 32;
  const auto compressed =
    compression::Compress(compression::Strip(kSlots), View(raw), frame);
  ASSERT_EQ(compressed.frames.sizes.size(), 2U);
  EXPECT_LT(compressed.frames.sizes[0], frame);
  EXPECT_EQ(compressed.frames.sizes[1], frame);
  EXPECT_EQ(Decoded(compressed), raw);
}

TEST(Compression, CorruptStripFramesAreRejected) {
  constexpr uint32_t kSlots = 8;
  const auto raw = Lists(kSlots, 100, 1000, 11);
  const auto compressed =
    compression::Compress(compression::Strip(kSlots), View(raw), raw.size());
  ASSERT_EQ(compressed.frames.sizes.size(), 1U);
  ASSERT_LT(compressed.frames.sizes[0], raw.size());
  const auto stored = Stored(compressed);
  Bytes decoded(raw.size());

  auto truncated = compressed.frames;
  --truncated.sizes[0];
  ASSERT_SQL_ERROR(compression::Decompress(
                     truncated, stored.substr(0, truncated.sizes[0]), decoded),
                   ERRCODE_DATA_CORRUPTED);

  Bytes wrong_slots(stored.begin(), stored.end());
  wrong_slots[0] = 3;
  ASSERT_SQL_ERROR(
    compression::Decompress(compressed.frames, View(wrong_slots), decoded),
    ERRCODE_DATA_CORRUPTED);

  Bytes wrong_width(stored.begin(), stored.end());
  wrong_width[sizeof(uint32_t)] = 33;
  ASSERT_SQL_ERROR(
    compression::Decompress(compressed.frames, View(wrong_width), decoded),
    ERRCODE_DATA_CORRUPTED);

  Bytes zero_slots(stored.begin(), stored.end());
  std::fill_n(zero_slots.begin(), sizeof(uint32_t), irs::byte_type{0});
  ASSERT_SQL_ERROR(
    compression::Decompress(compressed.frames, View(zero_slots), decoded),
    ERRCODE_DATA_CORRUPTED);

  Bytes zero_width(stored.begin(), stored.end());
  zero_width[sizeof(uint32_t)] = 0;
  ASSERT_SQL_ERROR(
    compression::Decompress(compressed.frames, View(zero_width), decoded),
    ERRCODE_DATA_CORRUPTED);

  Bytes count_over(stored.begin(), stored.end());
  std::fill_n(count_over.begin() + sizeof(uint32_t) + sizeof(uint8_t),
              sizeof(uint32_t), irs::byte_type{0xFF});
  ASSERT_SQL_ERROR(
    compression::Decompress(compressed.frames, View(count_over), decoded),
    ERRCODE_DATA_CORRUPTED);

  Bytes trailing(stored.begin(), stored.end());
  trailing.push_back(0);
  auto longer = compressed.frames;
  ++longer.sizes[0];
  ASSERT_SQL_ERROR(compression::Decompress(longer, View(trailing), decoded),
                   ERRCODE_DATA_CORRUPTED);

  for (const uint32_t kept : {uint32_t{3}, uint32_t{6}}) {
    auto cut = compressed.frames;
    cut.sizes[0] = kept;
    ASSERT_SQL_ERROR(
      compression::Decompress(cut, stored.substr(0, kept), decoded),
      ERRCODE_DATA_CORRUPTED);
  }
}

TEST(Compression, StripStoresAFrameItCannotEncodeRaw) {
  const auto partial = Lists(4, 2, 1000, 13);
  const Bytes one_list(sizeof(uint32_t), irs::byte_type{1});
  for (const auto& [slots, raw] :
       {std::pair{uint32_t{4}, Bytes(partial.begin(), partial.end() - 2)},
        std::pair{uint32_t{1}, one_list}, std::pair{uint32_t{0}, partial}}) {
    const auto compressed =
      compression::Compress(compression::Strip(slots), View(raw), kFrame);
    ASSERT_EQ(compressed.frames.sizes.size(), 1U) << slots;
    EXPECT_EQ(compressed.frames.sizes[0], raw.size()) << slots;
    Bytes decoded(raw.size());
    compression::Decompress(compressed.frames, Stored(compressed), decoded);
    EXPECT_EQ(decoded, raw) << slots;
  }
}

std::string PackedObject(const compression::Frames& frames,
                         irs::bytes_view stored) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteObject(0, "frames", [&](duckdb::BinarySerializer& object) {
    compression::WriteFrames(object, frames);
  });
  serializer.OnPropertyBegin(1, "bytes");
  serializer.WriteValue(uint64_t{stored.size()});
  stream.WriteData(stored.data(), stored.size());
  serializer.OnPropertyEnd();
  serializer.End();
  return {reinterpret_cast<const char*>(stream.GetData()),
          stream.GetPosition()};
}

compression::Packed ReadPackedFromFile(std::string_view bytes) {
  duckdb::InputFileReader stream{std::span{
    reinterpret_cast<const duckdb::data_t*>(bytes.data()), bytes.size()}};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  return compression::ReadPacked(deserializer, stream);
}

TEST(Compression, ReadPackedRejectsBytesThatDoNotMatchItsFrames) {
  const auto raw = Text(size_t{kFrame} * 2);
  const auto compressed =
    compression::Compress(compression::kZstd1, View(raw), kFrame);
  const auto stored = Stored(compressed);

  auto longer = PackedObject(compressed.frames, stored + irs::byte_type{0});
  ASSERT_SQL_ERROR(ReadPacked(longer), ERRCODE_DATA_CORRUPTED);
  ASSERT_SQL_ERROR(ReadPackedFromFile(longer), ERRCODE_DATA_CORRUPTED);

  auto truncated = PackedObject(compressed.frames, stored);
  truncated.resize(truncated.size() - sizeof(duckdb::field_id_t) - 1);
  ASSERT_SQL_ERROR(ReadPacked(truncated), ERRCODE_DATA_CORRUPTED);
  ASSERT_SQL_ERROR(ReadPackedFromFile(truncated), ERRCODE_DATA_CORRUPTED);

  auto whole = PackedObject(compressed.frames, stored);
  EXPECT_TRUE(ReadPackedFromFile(whole).stored == stored);
}

TEST(Compression, TruncatedFrameIsCorrupted) {
  for (const auto method : kMethods) {
    const auto raw = Text(kFrame);
    const auto compressed = compression::Compress(method, View(raw), kFrame);
    auto frames = compressed.frames;
    --frames.sizes[0];
    Bytes decoded(raw.size());
    ASSERT_SQL_ERROR(
      compression::Decompress(
        frames, Stored(compressed).substr(0, frames.sizes[0]), decoded),
      ERRCODE_DATA_CORRUPTED);
  }
}

TEST(Compression, AFrameThatDecodesShortIsCorrupted) {
  for (const auto method : kMethods) {
    const auto raw = Text(kFrame);
    const auto compressed =
      compression::Compress(method, View(raw), 2 * kFrame);
    ASSERT_LT(compressed.frames.sizes[0], raw.size());
    auto longer = compressed.frames;
    longer.raw_bytes += 20;
    Bytes decoded(longer.raw_bytes);
    ASSERT_SQL_ERROR(
      compression::Decompress(longer, Stored(compressed), decoded),
      ERRCODE_DATA_CORRUPTED);
  }
}

TEST(Compression, ARangeOfFramesChecksItsRawSize) {
  const auto raw = Text(size_t{kFrame} * 3);
  const auto compressed =
    compression::Compress(compression::kZstd1, View(raw), kFrame);
  const auto stored = Stored(compressed);
  const auto starts = compression::FrameStarts(compressed.frames);
  const auto frame = irs::bytes_view{stored}.substr(
    starts[1].stored, starts[2].stored - starts[1].stored);
  Bytes shorter(kFrame - 1);
  ASSERT_SQL_ERROR(
    compression::Decompress(compressed.frames, 1, 2, frame, shorter),
    ERRCODE_DATA_CORRUPTED);
  Bytes decoded(kFrame);
  compression::Decompress(compressed.frames, 1, 2, frame, decoded);
  EXPECT_TRUE(absl::c_equal(decoded, View(raw).substr(kFrame, kFrame)));
}

TEST(Compression, SizesThatDoNotMatchTheFramesAreCorrupted) {
  const auto raw = Text(kFrame * 2);
  const auto compressed =
    compression::Compress(compression::kZstd1, View(raw), kFrame);
  Bytes shorter(raw.size() - 1);
  ASSERT_SQL_ERROR(
    compression::Decompress(compressed.frames, Stored(compressed), shorter),
    ERRCODE_DATA_CORRUPTED);
  Bytes decoded(raw.size());
  ASSERT_SQL_ERROR(compression::Decompress(
                     compressed.frames, Stored(compressed).substr(1), decoded),
                   ERRCODE_DATA_CORRUPTED);
}

TEST(Compression, ReadFramesRejectsInconsistentFrames) {
  const auto packed = [](const compression::Frames& frames) {
    duckdb::MemoryStream stream;
    duckdb::BinarySerializer serializer{stream};
    serializer.Begin();
    compression::WriteFrames(serializer, frames);
    serializer.End();
    return std::string{reinterpret_cast<const char*>(stream.GetData()),
                       stream.GetPosition()};
  };
  const compression::Frames valid{
    .codec = compression::Codec::Lz4,
    .raw_bytes = kFrame + 10,
    .frame_bytes = kFrame,
    .sizes = {100, 10},
  };
  EXPECT_EQ(ReadFrames(packed(valid)).sizes, valid.sizes);

  auto codec = valid;
  codec.codec = static_cast<compression::Codec>(7);
  ASSERT_SQL_ERROR(ReadFrames(packed(codec)), ERRCODE_DATA_CORRUPTED);

  auto frame_bytes = valid;
  frame_bytes.frame_bytes = 0;
  ASSERT_SQL_ERROR(ReadFrames(packed(frame_bytes)), ERRCODE_DATA_CORRUPTED);
  frame_bytes.frame_bytes = compression::kMaxFrameBytes + 1;
  ASSERT_SQL_ERROR(ReadFrames(packed(frame_bytes)), ERRCODE_DATA_CORRUPTED);

  auto count = valid;
  count.sizes.push_back(1);
  ASSERT_SQL_ERROR(ReadFrames(packed(count)), ERRCODE_DATA_CORRUPTED);

  auto empty = valid;
  empty.sizes[0] = 0;
  ASSERT_SQL_ERROR(ReadFrames(packed(empty)), ERRCODE_DATA_CORRUPTED);

  auto larger = valid;
  larger.sizes[1] = 11;
  ASSERT_SQL_ERROR(ReadFrames(packed(larger)), ERRCODE_DATA_CORRUPTED);

  auto uneven = valid;
  uneven.raw_sizes = {kFrame - 5, 15};
  EXPECT_EQ(ReadFrames(packed(uneven)).raw_sizes, uneven.raw_sizes);

  auto raw_count = uneven;
  raw_count.raw_sizes.push_back(1);
  ASSERT_SQL_ERROR(ReadFrames(packed(raw_count)), ERRCODE_DATA_CORRUPTED);

  auto raw_empty = uneven;
  raw_empty.raw_sizes = {kFrame, 0};
  ASSERT_SQL_ERROR(ReadFrames(packed(raw_empty)), ERRCODE_DATA_CORRUPTED);

  auto raw_over = uneven;
  raw_over.raw_sizes = {kFrame + 1, 9};
  ASSERT_SQL_ERROR(ReadFrames(packed(raw_over)), ERRCODE_DATA_CORRUPTED);

  auto raw_sum = uneven;
  raw_sum.raw_sizes = {kFrame - 5, 14};
  ASSERT_SQL_ERROR(ReadFrames(packed(raw_sum)), ERRCODE_DATA_CORRUPTED);

  auto raw_larger = uneven;
  raw_larger.sizes[1] = 16;
  ASSERT_SQL_ERROR(ReadFrames(packed(raw_larger)), ERRCODE_DATA_CORRUPTED);
}

}  // namespace
