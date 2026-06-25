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

#include <cstdint>
#include <string>
#include <string_view>

#include "network/pg/pg_frame_codec.h"
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

std::string StartupFrame(uint32_t code, std::string_view rest) {
  std::string frame;
  char length[4];
  absl::big_endian::Store32(length, static_cast<uint32_t>(4 + 4 + rest.size()));
  frame.append(length, 4);
  char code_bytes[4];
  absl::big_endian::Store32(code_bytes, code);
  frame.append(code_bytes, 4);
  frame.append(rest);
  return frame;
}

}  // namespace

TEST(NetworkPgCodec, TypedFrameFragmentation) {
  const std::string frame =
    TypedFrame(PQ_MSG_QUERY, std::string_view{"SELECT 1\0", 9});
  for (size_t n = 0; n < frame.size(); ++n) {
    const auto partial = ParseFrame(frame.substr(0, n), FrameKind::Typed, kMax);
    EXPECT_EQ(partial.status, FrameStatus::NeedMore) << "n=" << n;
  }
  const auto whole = ParseFrame(frame, FrameKind::Typed, kMax);
  ASSERT_EQ(whole.status, FrameStatus::Ok);
  EXPECT_EQ(whole.consumed, frame.size());
  EXPECT_EQ(whole.type, PQ_MSG_QUERY);
  EXPECT_EQ(whole.payload, std::string_view("SELECT 1\0", 9));
}

TEST(NetworkPgCodec, TypedFramePipelined) {
  const std::string one = TypedFrame(PQ_MSG_SYNC, "");
  const std::string two =
    one + TypedFrame(PQ_MSG_QUERY, std::string_view{"x\0", 2});
  const auto first = ParseFrame(two, FrameKind::Typed, kMax);
  ASSERT_EQ(first.status, FrameStatus::Ok);
  EXPECT_EQ(first.type, PQ_MSG_SYNC);
  EXPECT_EQ(first.consumed, one.size());
  EXPECT_TRUE(first.payload.empty());

  const auto second = ParseFrame(std::string_view{two}.substr(first.consumed),
                                 FrameKind::Typed, kMax);
  ASSERT_EQ(second.status, FrameStatus::Ok);
  EXPECT_EQ(second.type, PQ_MSG_QUERY);
}

TEST(NetworkPgCodec, StartupAndSslAndCancelCodes) {
  const std::string startup = StartupFrame(
    PG_PROTOCOL_LATEST, std::string_view{"user\0postgres\0\0", 15});
  const auto parsed =
    ParseFrame(startup, FrameKind::Startup, MAX_STARTUP_PACKET_LENGTH);
  ASSERT_EQ(parsed.status, FrameStatus::Ok);
  EXPECT_EQ(parsed.type, '\0');
  EXPECT_EQ(StartupCode(parsed.payload),
            static_cast<uint32_t>(PG_PROTOCOL_LATEST));

  const std::string ssl = StartupFrame(NEGOTIATE_SSL_CODE, "");
  const auto ssl_parsed =
    ParseFrame(ssl, FrameKind::Startup, MAX_STARTUP_PACKET_LENGTH);
  ASSERT_EQ(ssl_parsed.status, FrameStatus::Ok);
  EXPECT_EQ(ssl_parsed.consumed, 8u);
  EXPECT_EQ(StartupCode(ssl_parsed.payload),
            static_cast<uint32_t>(NEGOTIATE_SSL_CODE));

  const std::string cancel =
    StartupFrame(CANCEL_REQUEST_CODE,
                 std::string_view{"\x00\x00\x00\x01\x00\x00\x00\x02", 8});
  const auto cancel_parsed =
    ParseFrame(cancel, FrameKind::Startup, MAX_STARTUP_PACKET_LENGTH);
  ASSERT_EQ(cancel_parsed.status, FrameStatus::Ok);
  EXPECT_EQ(StartupCode(cancel_parsed.payload),
            static_cast<uint32_t>(CANCEL_REQUEST_CODE));
}

TEST(NetworkPgCodec, TooLargeAndMalformed) {
  const std::string frame =
    TypedFrame(PQ_MSG_QUERY, std::string_view{"abcdef", 6});
  const auto too_large = ParseFrame(frame, FrameKind::Typed, 5);
  EXPECT_EQ(too_large.status, FrameStatus::TooLarge);

  std::string bad;
  bad.push_back(PQ_MSG_QUERY);
  char length[4];
  absl::big_endian::Store32(length, 3);
  bad.append(length, 4);
  const auto malformed = ParseFrame(bad, FrameKind::Typed, kMax);
  EXPECT_EQ(malformed.status, FrameStatus::Malformed);
}
