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
#include <initializer_list>
#include <string>
#include <string_view>

#include "network/pg/bind_decoder.h"
#include "pg/serialize.h"

using namespace sdb::network::pg;

namespace {

void AppendU16(std::string& s, uint16_t v) {
  char b[2];
  absl::big_endian::Store16(b, v);
  s.append(b, 2);
}

// A Bind format-code array: int16 count followed by that many int16 codes.
std::string Formats(std::initializer_list<uint16_t> codes) {
  std::string s;
  AppendU16(s, static_cast<uint16_t>(codes.size()));
  for (uint16_t c : codes) {
    AppendU16(s, c);
  }
  return s;
}

}  // namespace

TEST(NetworkPgBindDecoder, EmptyArrayConsumesOnlyTheCount) {
  std::string payload = Formats({});
  std::string_view cursor = payload;
  EXPECT_TRUE(ParseBindFormats(cursor).empty());
  EXPECT_TRUE(cursor.empty());
}

TEST(NetworkPgBindDecoder, TextAndBinaryCodes) {
  std::string payload = Formats({0, 1, 0});
  std::string_view cursor = payload;
  const auto formats = ParseBindFormats(cursor);
  ASSERT_EQ(formats.size(), 3u);
  EXPECT_EQ(formats[0], sdb::pg::VarFormat::Text);
  EXPECT_EQ(formats[1], sdb::pg::VarFormat::Binary);
  EXPECT_EQ(formats[2], sdb::pg::VarFormat::Text);
  EXPECT_TRUE(cursor.empty());
}

TEST(NetworkPgBindDecoder, AdvancesCursorPastArrayOnly) {
  // A Bind message carries two arrays back to back (input formats, then output
  // formats); the first parse must leave the cursor at the start of the second.
  std::string payload = Formats({1});
  const std::string tail = Formats({0});
  payload += tail;
  std::string_view cursor = payload;
  const auto input = ParseBindFormats(cursor);
  ASSERT_EQ(input.size(), 1u);
  EXPECT_EQ(input[0], sdb::pg::VarFormat::Binary);
  EXPECT_EQ(cursor, tail);
  const auto output = ParseBindFormats(cursor);
  ASSERT_EQ(output.size(), 1u);
  EXPECT_EQ(output[0], sdb::pg::VarFormat::Text);
  EXPECT_TRUE(cursor.empty());
}

TEST(NetworkPgBindDecoder, RejectsTruncatedCount) {
  std::string payload(1, '\0');  // one byte; the count needs two
  std::string_view cursor = payload;
  EXPECT_ANY_THROW(ParseBindFormats(cursor));
}

TEST(NetworkPgBindDecoder, RejectsTruncatedCodes) {
  std::string payload;
  AppendU16(payload, 2);  // claims two codes
  AppendU16(payload, 0);  // but only one is present
  std::string_view cursor = payload;
  EXPECT_ANY_THROW(ParseBindFormats(cursor));
}

TEST(NetworkPgBindDecoder, RejectsOutOfRangeCode) {
  std::string payload = Formats({2});  // only 0 (text) and 1 (binary) are valid
  std::string_view cursor = payload;
  EXPECT_ANY_THROW(ParseBindFormats(cursor));
}
