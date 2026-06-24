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

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <string_view>

#include "basics/message_buffer.h"
#include "network/pg/frame_reader.h"
#include "pg/protocol.h"

using namespace sdb::network::pg;

namespace {

constexpr uint32_t kMax = 1u << 20;

std::string TypedFrame(char type, std::string_view payload) {
  std::string frame;
  frame.push_back(type);
  char length[4];
  absl::big_endian::Store32(length, static_cast<uint32_t>(4 + payload.size()));
  frame.append(length, 4);
  frame.append(payload);
  return frame;
}

// Append `data` through Reserve/CommitWrite, filling each returned span fully.
// A small min_growth means a large frame spans several chunks, so Front() only
// ever exposes the head chunk -- exactly the case FrameReader must linearize
// and the codec test (contiguous strings) cannot reach.
void Feed(sdb::message::Buffer& buf, std::string_view data) {
  size_t off = 0;
  while (off < data.size()) {
    const auto span = buf.Reserve(1);
    const size_t n = std::min(span.size(), data.size() - off);
    std::memcpy(span.data(), data.data() + off, n);
    buf.CommitWrite(n);
    off += n;
  }
}

}  // namespace

// A frame that fits in the head chunk is borrowed in place: recv_consume is its
// length and Consume advances the recv buffer past it.
TEST(NetworkPgFrameReader, ContiguousBorrowed) {
  sdb::message::Buffer buf{64, kMax};
  FrameReader reader{buf};
  const std::string frame =
    TypedFrame(PQ_MSG_QUERY, std::string_view{"hi\0", 3});
  Feed(buf, frame);

  const auto f = reader.TryAssemble(FrameKind::Typed, kMax);
  EXPECT_EQ(f.status, FrameStatus::Ok);
  EXPECT_EQ(f.type, PQ_MSG_QUERY);
  EXPECT_EQ(f.payload, std::string_view("hi\0", 3));
  EXPECT_EQ(f.recv_consume, frame.size());
  // A single-chunk frame is borrowed straight from the recv buffer -- the
  // reader's scratch is never touched. The payload points into the head chunk,
  // just past the 1-byte type + 4-byte length header.
  EXPECT_EQ(static_cast<const void*>(f.payload.data()),
            static_cast<const void*>(buf.Front().data() + 5));
  reader.Consume(f);
  EXPECT_FALSE(buf.Readable());
}

// Partial arrival yields NeedMore until the whole frame is present.
TEST(NetworkPgFrameReader, IncrementalNeedMore) {
  sdb::message::Buffer buf{64, kMax};
  FrameReader reader{buf};
  const std::string frame = TypedFrame(PQ_MSG_QUERY, "hello");
  Feed(buf, std::string_view{frame}.substr(0, 4));
  EXPECT_EQ(reader.TryAssemble(FrameKind::Typed, kMax).status,
            FrameStatus::NeedMore);
  Feed(buf, std::string_view{frame}.substr(4));
  const auto f = reader.TryAssemble(FrameKind::Typed, kMax);
  EXPECT_EQ(f.status, FrameStatus::Ok);
  EXPECT_EQ(f.payload, "hello");
}

// A frame larger than a chunk spans several chunks: Front() never shows it
// whole, so it is linearized -- recv_consume is 0 (already consumed) and the
// payload is the reader's scratch view.
TEST(NetworkPgFrameReader, SpanningLinearized) {
  sdb::message::Buffer buf{16, kMax};  // tiny chunks force spanning
  FrameReader reader{buf};
  const std::string payload(1024, 'x');
  const std::string frame = TypedFrame(PQ_MSG_QUERY, payload);
  Feed(buf, std::string_view{frame}.substr(0, 8));
  EXPECT_EQ(reader.TryAssemble(FrameKind::Typed, kMax).status,
            FrameStatus::NeedMore);
  Feed(buf, std::string_view{frame}.substr(8));

  const auto f = reader.TryAssemble(FrameKind::Typed, kMax);
  EXPECT_EQ(f.status, FrameStatus::Ok);
  EXPECT_EQ(f.type, PQ_MSG_QUERY);
  EXPECT_EQ(f.payload.size(), payload.size());
  EXPECT_EQ(f.payload, payload);
  EXPECT_EQ(f.recv_consume, 0u);  // linearized -> already consumed
  reader.Consume(f);              // no-op
  EXPECT_FALSE(buf.Readable());
}

// Two pipelined frames in one buffer: assemble + Consume the first, then the
// second resumes from where the first left off.
TEST(NetworkPgFrameReader, Pipelined) {
  sdb::message::Buffer buf{64, kMax};
  FrameReader reader{buf};
  Feed(buf, TypedFrame(PQ_MSG_SYNC, "") + TypedFrame(PQ_MSG_QUERY, "x"));

  const auto first = reader.TryAssemble(FrameKind::Typed, kMax);
  EXPECT_EQ(first.status, FrameStatus::Ok);
  EXPECT_EQ(first.type, PQ_MSG_SYNC);
  EXPECT_TRUE(first.payload.empty());
  reader.Consume(first);

  const auto second = reader.TryAssemble(FrameKind::Typed, kMax);
  EXPECT_EQ(second.status, FrameStatus::Ok);
  EXPECT_EQ(second.type, PQ_MSG_QUERY);
  EXPECT_EQ(second.payload, "x");
  reader.Consume(second);
  EXPECT_FALSE(buf.Readable());
}

// An over-cap frame surfaces TooLarge even while spanning chunks.
TEST(NetworkPgFrameReader, TooLargeWhileSpanning) {
  sdb::message::Buffer buf{16, kMax};
  FrameReader reader{buf};
  const std::string frame = TypedFrame(PQ_MSG_QUERY, std::string(100, 'a'));
  Feed(buf, frame);
  const auto f = reader.TryAssemble(FrameKind::Typed, 8);
  EXPECT_EQ(f.status, FrameStatus::TooLarge);
}

// Startup frames carry no type byte; the length prefix covers the whole frame.
TEST(NetworkPgFrameReader, StartupFrame) {
  sdb::message::Buffer buf{64, kMax};
  FrameReader reader{buf};
  const std::string_view rest{"user\0postgres\0\0", 15};
  std::string frame;
  char length[4];
  absl::big_endian::Store32(length, static_cast<uint32_t>(4 + 4 + rest.size()));
  frame.append(length, 4);
  char code[4];
  absl::big_endian::Store32(code, static_cast<uint32_t>(PG_PROTOCOL_LATEST));
  frame.append(code, 4);
  frame.append(rest);
  Feed(buf, frame);

  const auto f =
    reader.TryAssemble(FrameKind::Startup, MAX_STARTUP_PACKET_LENGTH);
  EXPECT_EQ(f.status, FrameStatus::Ok);
  EXPECT_EQ(f.type, '\0');
  EXPECT_EQ(StartupCode(f.payload), static_cast<uint32_t>(PG_PROTOCOL_LATEST));
}
