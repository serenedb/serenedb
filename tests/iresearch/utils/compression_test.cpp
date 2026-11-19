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
constexpr std::array kMethods{compression::kLz4Hc9, compression::kZstd1};

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
      EXPECT_LT(compressed.frames.sizes[frame], kFrame / 4)
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

void WriteInPieces(compression::FrameWriter& writer, const Bytes& raw) {
  constexpr std::array kPieces{size_t{1000}, size_t{7}, size_t{kFrame} + 5,
                               size_t{1}, size_t{kFrame} * 3};
  size_t at = 0;
  for (size_t piece = 0; at < raw.size(); ++piece) {
    const auto take =
      std::min(kPieces[piece % kPieces.size()], raw.size() - at);
    if (piece % 2 == 0) {
      writer.WriteData(raw.data() + at, take);
    } else {
      auto* room = writer.Alloc(take);
      ASSERT_TRUE(room);
      std::memcpy(room, raw.data() + at, take);
    }
    at += take;
  }
}

std::vector<Bytes> WrittenFrames(const compression::FrameWriter& writer) {
  std::vector<Bytes> frames;
  writer.ForEachFrame([&](irs::bytes_view frame) {
    frames.emplace_back(frame.begin(), frame.end());
  });
  return frames;
}

Bytes PackedRoundTrip(const compression::Compressed& compressed) {
  auto bytes = WritePacked(compressed);
  const auto packed = ReadPacked(bytes);
  EXPECT_EQ(packed.frames.sizes, compressed.frames.sizes);
  EXPECT_EQ(packed.frames.raw_sizes, compressed.frames.raw_sizes);
  const auto decoded = compression::Decompress(packed, Threads);
  return Bytes(decoded.get(), decoded.get() + packed.frames.raw_bytes);
}

TEST(Compression, FrameWriterFramesRoundTrip) {
  for (const auto method : kMethods) {
    for (const size_t size :
         {size_t{0}, size_t{1}, size_t{kFrame}, size_t{kFrame} * 3 + 17,
          size_t{kFrame} * 20 + 3}) {
      const auto raw = Text(size);
      compression::FrameWriter writer{kFrame};
      WriteInPieces(writer, raw);
      ASSERT_EQ(writer.RawBytes(), raw.size());
      const auto frames = WrittenFrames(writer);
      Bytes joined;
      for (const auto& frame : frames) {
        EXPECT_FALSE(frame.empty());
        EXPECT_LE(frame.size(), kFrame);
        joined.insert(joined.end(), frame.begin(), frame.end());
      }
      EXPECT_EQ(joined, raw);
      const auto compressed = compression::Compress(method, writer, Threads);
      EXPECT_EQ(compressed.frames.raw_bytes, raw.size());
      EXPECT_EQ(compressed.frames.frame_bytes, kFrame);
      EXPECT_EQ(compressed.frames.sizes.size(), frames.size());
      EXPECT_EQ(PackedRoundTrip(compressed), raw)
        << magic_enum::enum_name(method.codec) << " " << size;
    }
  }
}

TEST(Compression, FrameWriterEndsFramesWithTheirPiece) {
  const auto raw = Text(size_t{kFrame} * 4 + 50);
  compression::FrameWriter writer{kFrame};
  EXPECT_FALSE(writer.Alloc(0));
  writer.WriteData(raw.data(), 100);
  const size_t big = size_t{kFrame} * 2 + 10;
  auto* room = writer.Alloc(big);
  ASSERT_TRUE(room);
  std::memcpy(room, raw.data() + 100, big);
  auto* small = writer.Alloc(5);
  ASSERT_TRUE(small);
  std::memcpy(small, raw.data() + 100 + big, 5);
  writer.WriteData(raw.data() + 105 + big, raw.size() - 105 - big);
  ASSERT_EQ(writer.RawBytes(), raw.size());
  const absl::InlinedVector<uint32_t, 1> expected{100, kFrame, kFrame, kFrame,
                                                  kFrame - 50};
  const auto frames = WrittenFrames(writer);
  ASSERT_EQ(frames.size(), expected.size());
  size_t at = 0;
  for (size_t frame = 0; frame != frames.size(); ++frame) {
    const auto begin = raw.begin() + static_cast<ptrdiff_t>(at);
    EXPECT_EQ(frames[frame], Bytes(begin, begin + expected[frame])) << frame;
    at += expected[frame];
  }
  const auto compressed =
    compression::Compress(compression::kZstd1, writer, Threads);
  EXPECT_EQ(compressed.frames.raw_sizes, expected);
  EXPECT_EQ(PackedRoundTrip(compressed), raw);
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
    .codec = compression::Codec::Lz4Hc,
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
