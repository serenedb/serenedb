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

irs::bytes_view Stored(const compression::Compressed& compressed) {
  return {compressed.bytes.get(), compression::StoredBytes(compressed.frames)};
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
  return ReadPacked(bytes).frames;
}

Bytes RoundTrip(compression::Method method, const Bytes& raw) {
  const auto compressed = compression::Compress(method, View(raw), kFrame);
  auto bytes = WritePacked(compressed);
  const auto packed = ReadPacked(bytes);
  EXPECT_EQ(packed.frames.codec, method.codec);
  EXPECT_EQ(packed.frames.raw_bytes, raw.size());
  EXPECT_EQ(packed.frames.frame_bytes, kFrame);
  EXPECT_EQ(packed.frames.sizes, compressed.frames.sizes);
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

TEST(Compression, ReadPackedRejectsInconsistentFrames) {
  const auto packed = [](const compression::Frames& frames) {
    const auto size = compression::StoredBytes(frames);
    compression::Compressed out{
      .frames = frames,
      .bytes = std::make_unique<irs::byte_type[]>(size),
    };
    return WritePacked(out);
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
}

}  // namespace
