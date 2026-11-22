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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iresearch/utils/string.hpp>
#include <vector>

#include "server/utils/frame_stream.h"

namespace {

using Bytes = std::vector<irs::byte_type>;

constexpr uint32_t kFrame = 4096;

Bytes Text(size_t size) {
  Bytes text(size);
  for (size_t i = 0; i != size; ++i) {
    text[i] = static_cast<irs::byte_type>('a' + i % 23);
  }
  return text;
}

void WriteInPieces(sdb::message::FrameStream& stream, const Bytes& raw) {
  constexpr std::array kPieces{size_t{1000}, size_t{7}, size_t{kFrame} + 5,
                               size_t{1}, size_t{kFrame} * 3};
  size_t at = 0;
  for (size_t piece = 0; at < raw.size(); ++piece) {
    const auto take =
      std::min(kPieces[piece % kPieces.size()], raw.size() - at);
    if (piece % 2 == 0) {
      stream.WriteData(raw.data() + at, take);
    } else {
      auto* room = stream.Alloc(take);
      ASSERT_TRUE(room);
      std::memcpy(room, raw.data() + at, take);
    }
    at += take;
  }
}

TEST(MessageFrameStream, FramesCoverTheWrittenBytes) {
  for (const size_t size : {size_t{0}, size_t{1}, size_t{kFrame},
                            size_t{kFrame} * 3 + 17, size_t{kFrame} * 20 + 3}) {
    const auto raw = Text(size);
    sdb::message::FrameStream stream{kFrame};
    WriteInPieces(stream, raw);
    Bytes joined;
    for (const auto frame : stream.Frames()) {
      EXPECT_FALSE(frame.empty());
      EXPECT_LE(frame.size(), kFrame);
      joined.insert(joined.end(), frame.begin(), frame.end());
    }
    EXPECT_EQ(joined, raw) << size;
  }
}

TEST(MessageFrameStream, FramesEndWithTheirChunk) {
  const auto raw = Text(size_t{kFrame} * 4 + 50);
  sdb::message::FrameStream stream{kFrame};
  stream.WriteData(raw.data(), 100);
  const size_t big = size_t{kFrame} * 2 + 10;
  auto* room = stream.Alloc(big);
  ASSERT_TRUE(room);
  std::memcpy(room, raw.data() + 100, big);
  auto* small = stream.Alloc(5);
  ASSERT_TRUE(small);
  std::memcpy(small, raw.data() + 100 + big, 5);
  stream.WriteData(raw.data() + 105 + big, raw.size() - 105 - big);
  constexpr std::array kExpected{size_t{100}, size_t{kFrame}, size_t{kFrame},
                                 size_t{kFrame}, size_t{kFrame} - 50};
  const auto frames = stream.Frames();
  ASSERT_EQ(frames.size(), kExpected.size());
  size_t at = 0;
  for (size_t frame = 0; frame != frames.size(); ++frame) {
    EXPECT_EQ(frames[frame], irs::bytes_view(raw.data() + at, kExpected[frame]))
      << frame;
    at += kExpected[frame];
  }
}

}  // namespace
