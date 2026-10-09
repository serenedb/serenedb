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

#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>

#include "pg/commands/create_subscription.h"
#include "replication/conninfo.h"

namespace sdb {
namespace {

TEST(SubscriptionLsn, ParsesPostgresText) {
  EXPECT_EQ(pg::ParseLsn("0/0"), std::optional<uint64_t>{0});
  EXPECT_EQ(pg::ParseLsn("0/1A2B3C"), std::optional<uint64_t>{0x1A2B3C});
  EXPECT_EQ(pg::ParseLsn("16/B374D848"),
            std::optional<uint64_t>{(uint64_t{0x16} << 32) | 0xB374D848});
  EXPECT_EQ(pg::ParseLsn("ffffffff/ffffffff"),
            std::optional<uint64_t>{UINT64_MAX});
}

TEST(SubscriptionLsn, RejectsMalformedText) {
  EXPECT_FALSE(pg::ParseLsn(""));
  EXPECT_FALSE(pg::ParseLsn("0"));
  EXPECT_FALSE(pg::ParseLsn("/1"));
  EXPECT_FALSE(pg::ParseLsn("1/"));
  EXPECT_FALSE(pg::ParseLsn("g/1"));
  EXPECT_FALSE(pg::ParseLsn("1/2/3"));
  EXPECT_FALSE(pg::ParseLsn("123456789/0"));
}

TEST(SubscriptionLsn, FormatsLikePostgres) {
  EXPECT_EQ(pg::FormatLsn(0), "0/0");
  EXPECT_EQ(pg::FormatLsn(0x1A2B3C), "0/1A2B3C");
  EXPECT_EQ(pg::FormatLsn((uint64_t{0x16} << 32) | 0xB374D848), "16/B374D848");
  for (const uint64_t lsn :
       {uint64_t{1}, uint64_t{0xFFFFFFFF}, uint64_t{0x100000000}, UINT64_MAX}) {
    EXPECT_EQ(pg::ParseLsn(pg::FormatLsn(lsn)), std::optional<uint64_t>{lsn});
  }
}

TEST(SubscriptionConnInfo, ParsesKeywordValuePairs) {
  const auto info = replication::ParseConnInfo(
    "host=publisher port=6543 user=repl password=secret dbname=source "
    "application_name=app sslmode=require connect_timeout=7");
  ASSERT_EQ(info.hosts.size(), 1);
  EXPECT_EQ(info.hosts[0].host, "publisher");
  EXPECT_EQ(info.hosts[0].port, "6543");
  EXPECT_EQ(info.user, "repl");
  EXPECT_EQ(info.password, "secret");
  EXPECT_EQ(info.dbname, "source");
  EXPECT_EQ(info.application_name, "app");
  EXPECT_EQ(info.sslmode, replication::SslMode::Require);
  EXPECT_EQ(info.connect_timeout, std::chrono::seconds{7});
}

TEST(SubscriptionConnInfo, DefaultsHostAndPort) {
  const auto info = replication::ParseConnInfo("dbname=source");
  ASSERT_EQ(info.hosts.size(), 1);
  EXPECT_EQ(info.hosts[0].host, "/tmp");
  EXPECT_EQ(info.hosts[0].port, "5432");
  EXPECT_TRUE(info.user.empty());
  EXPECT_EQ(info.sslmode, replication::SslMode::Prefer);
}

TEST(SubscriptionConnInfo, ParsesQuotedValues) {
  const auto info = replication::ParseConnInfo(
    R"(host=publisher password='it\'s a \\secret' dbname = 'my db')");
  EXPECT_EQ(info.password, R"(it's a \secret)");
  EXPECT_EQ(info.dbname, "my db");
}

TEST(SubscriptionConnInfo, ParsesUri) {
  const auto info = replication::ParseConnInfo(
    "postgresql://repl:p%40ss@pub1:6000,pub2/source?sslmode=verify-full&"
    "application_name=sub");
  ASSERT_EQ(info.hosts.size(), 2);
  EXPECT_EQ(info.hosts[0].host, "pub1");
  EXPECT_EQ(info.hosts[0].port, "6000");
  EXPECT_EQ(info.hosts[1].host, "pub2");
  EXPECT_EQ(info.hosts[1].port, "5432");
  EXPECT_EQ(info.user, "repl");
  EXPECT_EQ(info.password, "p@ss");
  EXPECT_EQ(info.dbname, "source");
  EXPECT_EQ(info.sslmode, replication::SslMode::VerifyFull);
  EXPECT_EQ(info.application_name, "sub");
}

TEST(SubscriptionConnInfo, MatchesPortsToHosts) {
  const auto info =
    replication::ParseConnInfo("host=a,b,c port=1,2,3 hostaddr=1.1.1.1,,");
  ASSERT_EQ(info.hosts.size(), 3);
  EXPECT_EQ(info.hosts[1].host, "b");
  EXPECT_EQ(info.hosts[2].port, "3");
  EXPECT_EQ(info.hosts[0].hostaddr, "1.1.1.1");
  EXPECT_TRUE(info.hosts[1].hostaddr.empty());
}

TEST(SubscriptionConnInfo, RejectsInvalidStrings) {
  EXPECT_THROW(replication::ParseConnInfo("host"), std::exception);
  EXPECT_THROW(replication::ParseConnInfo("bogus=1"), std::exception);
  EXPECT_THROW(replication::ParseConnInfo("sslmode=sometimes"), std::exception);
  EXPECT_THROW(replication::ParseConnInfo("host=a,b port=1,2,3"),
               std::exception);
  EXPECT_THROW(replication::ParseConnInfo("connect_timeout=soon"),
               std::exception);
}

}  // namespace
}  // namespace sdb
