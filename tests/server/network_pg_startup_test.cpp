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

#include <algorithm>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>

#include "network/pg/startup_request.h"
#include "pg/protocol.h"

using namespace sdb::network::pg;

namespace {

// A StartupMessage payload as the codec hands it to ParseStartup: the 4-byte
// version word followed by null-terminated key/value pairs and a final null.
std::string StartupPayload(
  uint32_t version,
  std::initializer_list<std::pair<std::string_view, std::string_view>> kv) {
  std::string p;
  char version_bytes[4];
  absl::big_endian::Store32(version_bytes, version);
  p.append(version_bytes, 4);
  for (const auto& [key, value] : kv) {
    p.append(key);
    p.push_back('\0');
    p.append(value);
    p.push_back('\0');
  }
  p.push_back('\0');
  return p;
}

}  // namespace

TEST(NetworkPgStartup, VersionAndParams) {
  const auto req = ParseStartup(StartupPayload(
    PG_PROTOCOL_LATEST, {{"user", "postgres"}, {"database", "mydb"}}));
  EXPECT_EQ(req.major, 3u);
  EXPECT_EQ(req.minor, 0u);
  EXPECT_EQ(req.params.size(), 2u);
  EXPECT_EQ(req.params.at("user"), "postgres");
  EXPECT_EQ(req.params.at("database"), "mydb");
  EXPECT_TRUE(req.unrecognized_pq.empty());
  EXPECT_FALSE(req.replication);
}

TEST(NetworkPgStartup, PqOptionsCollectedAndStripped) {
  const auto req = ParseStartup(StartupPayload(
    PG_PROTOCOL_LATEST,
    {{"user", "postgres"}, {"_pq_.foo", "1"}, {"_pq_.bar", "2"}}));
  EXPECT_FALSE(req.params.contains("_pq_.foo"));
  EXPECT_FALSE(req.params.contains("_pq_.bar"));
  EXPECT_TRUE(req.params.contains("user"));
  ASSERT_EQ(req.unrecognized_pq.size(), 2u);
  EXPECT_NE(std::find(req.unrecognized_pq.begin(), req.unrecognized_pq.end(),
                      "_pq_.foo"),
            req.unrecognized_pq.end());
}

TEST(NetworkPgStartup, DetectsReplicationMode) {
  // PG accepts a case-insensitive bool ({true,yes,on,1}) plus "database".
  for (const std::string_view mode :
       {"true", "TRUE", "On", "yes", "1", "database", "DATABASE"}) {
    const auto req =
      ParseStartup(StartupPayload(PG_PROTOCOL_LATEST, {{"replication", mode}}));
    EXPECT_TRUE(req.replication) << "mode=" << mode;
    // The parameter itself is retained (it is a normal GUC, not a _pq_ option).
    EXPECT_EQ(req.params.at("replication"), mode);
  }
  for (const std::string_view mode : {"false", "off", "no", "0"}) {
    const auto req =
      ParseStartup(StartupPayload(PG_PROTOCOL_LATEST, {{"replication", mode}}));
    EXPECT_FALSE(req.replication) << "mode=" << mode;
  }
}

TEST(NetworkPgStartup, DuplicateKeyLastWins) {
  const auto req = ParseStartup(StartupPayload(
    PG_PROTOCOL_LATEST, {{"user", "first"}, {"user", "second"}}));
  EXPECT_EQ(req.params.at("user"), "second");
}

TEST(NetworkPgStartup, NewerMinorPreserved) {
  const auto req = ParseStartup(StartupPayload(PG_PROTOCOL(3, 2), {}));
  EXPECT_EQ(req.major, 3u);
  EXPECT_EQ(req.minor, 2u);
}

TEST(NetworkPgStartup, ShortPayloadIsEmpty) {
  EXPECT_EQ(ParseStartup(std::string_view{"\x00\x03", 2}).major, 0u);
  EXPECT_TRUE(ParseStartup(std::string_view{}).params.empty());
}
