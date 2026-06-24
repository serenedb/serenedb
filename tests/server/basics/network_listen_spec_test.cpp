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

#include <string>
#include <vector>

#include "network/listen_spec.h"

using namespace sdb;
using namespace sdb::network;

namespace {

std::vector<ListenSpec> Parse(std::vector<std::string> urls) {
  return ParseListenSpecs(urls);
}

TEST(ListenSpec, DefaultIsLoopbackPg) {
  const auto specs = Parse({});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, ListenProtocol::Pg);
  EXPECT_EQ(specs[0].transport, ListenTransport::Tcp);
  EXPECT_EQ(specs[0].endpoint.address().to_string(), "127.0.0.1");
  EXPECT_EQ(specs[0].endpoint.port(), 7890);
}

TEST(ListenSpec, PgTcpV4) {
  const auto specs = Parse({"postgres://127.0.0.1:5432"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, ListenProtocol::Pg);
  EXPECT_EQ(specs[0].endpoint.port(), 5432);
  EXPECT_FALSE(specs[0].endpoint.address().is_v6());
  EXPECT_EQ(specs[0].sslmode, SslMode::Prefer);
}

TEST(ListenSpec, PostgresqlAlias) {
  const auto specs = Parse({"postgresql://127.0.0.1:5432"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, ListenProtocol::Pg);
}

TEST(ListenSpec, IPv6Brackets) {
  const auto specs = Parse({"postgres://[::1]:5432"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_TRUE(specs[0].endpoint.address().is_v6());
  EXPECT_TRUE(specs[0].v6_only);
  EXPECT_EQ(specs[0].endpoint.port(), 5432);
}

TEST(ListenSpec, WildcardBindsBothFamilies) {
  const auto specs = Parse({"postgres://*:5432"});
  ASSERT_EQ(specs.size(), 2u);
  bool v4 = false;
  bool v6 = false;
  for (const auto& s : specs) {
    v4 |= s.endpoint.address().is_v4();
    v6 |= s.endpoint.address().is_v6();
    EXPECT_EQ(s.endpoint.port(), 5432);
  }
  EXPECT_TRUE(v4);
  EXPECT_TRUE(v6);
}

TEST(ListenSpec, MultipleCommaSeparated) {
  const auto specs =
    Parse({"postgres://127.0.0.1:5432,http://127.0.0.1:9200?api=es"});
  ASSERT_EQ(specs.size(), 2u);
  EXPECT_EQ(specs[0].protocol, ListenProtocol::Pg);
  EXPECT_EQ(specs[1].protocol, ListenProtocol::Http);
  ASSERT_EQ(specs[1].apis.size(), 1u);
  EXPECT_EQ(specs[1].apis[0], "es");
}

TEST(ListenSpec, HttpsImpliesTls) {
  const auto specs = Parse({"https://127.0.0.1:9443?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_TRUE(specs[0].https);
  EXPECT_TRUE(specs[0].TlsCapable());
}

TEST(ListenSpec, SslModeParsed) {
  EXPECT_EQ(Parse({"postgres://127.0.0.1:1?sslmode=disable"})[0].sslmode,
            SslMode::Disable);
  EXPECT_EQ(Parse({"postgres://127.0.0.1:1?sslmode=require"})[0].sslmode,
            SslMode::Require);
  const auto vf = Parse({"postgres://127.0.0.1:1?sslmode=verify-full"});
  EXPECT_EQ(vf[0].sslmode, SslMode::VerifyFull);
  EXPECT_TRUE(vf[0].RequireTls());
  EXPECT_TRUE(vf[0].RequireClientCert());
}

TEST(ListenSpec, UnixFromPath) {
  const auto specs = Parse({"postgres:///var/run/serenedb.sock"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].transport, ListenTransport::Unix);
  EXPECT_EQ(specs[0].unix_path, "/var/run/serenedb.sock");
  EXPECT_FALSE(specs[0].unix_abstract);
}

TEST(ListenSpec, UnixAbstract) {
  const auto specs = Parse({"postgres:///@serenedb"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].transport, ListenTransport::Unix);
  EXPECT_TRUE(specs[0].unix_abstract);
  EXPECT_EQ(specs[0].unix_path, "serenedb");
}

TEST(ListenSpec, UnixPgPortMakesPgsqlPath) {
  const auto specs = Parse({"postgres:///tmp/sdbsock?port=5499"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].transport, ListenTransport::Unix);
  EXPECT_EQ(specs[0].protocol, ListenProtocol::Pg);
  EXPECT_EQ(specs[0].unix_path, "/tmp/sdbsock");
  ASSERT_TRUE(specs[0].unix_port.has_value());
  EXPECT_EQ(*specs[0].unix_port, 5499);
}

TEST(ListenSpec, HttpUnixSpecialScheme) {
  // http is a WHATWG "special" scheme; the empty-authority unix form must still
  // parse to a unix listener (textual detection, not ada).
  const auto specs = Parse({"http:///tmp/sdbhttp.sock?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, ListenProtocol::Http);
  EXPECT_EQ(specs[0].transport, ListenTransport::Unix);
  EXPECT_EQ(specs[0].unix_path, "/tmp/sdbhttp.sock");
  ASSERT_EQ(specs[0].apis.size(), 1u);
  EXPECT_EQ(specs[0].apis[0], "es");
}

TEST(ListenSpec, UnixModeAndGroup) {
  const auto specs = Parse({"postgres:///run/s.sock?mode=0770&group=pg"});
  ASSERT_EQ(specs.size(), 1u);
  ASSERT_TRUE(specs[0].unix_mode.has_value());
  EXPECT_EQ(*specs[0].unix_mode, 0770u);
  EXPECT_EQ(specs[0].unix_group, "pg");
}

TEST(ListenSpec, OperationalParams) {
  const auto specs = Parse(
    {"postgres://127.0.0.1:2?backlog=512&reuseport=on&proxy_protocol=require"});
  ASSERT_EQ(specs.size(), 1u);
  ASSERT_TRUE(specs[0].backlog.has_value());
  EXPECT_EQ(*specs[0].backlog, 512);
  ASSERT_TRUE(specs[0].reuseport.has_value());
  EXPECT_TRUE(*specs[0].reuseport);
  EXPECT_EQ(specs[0].proxy, ProxyMode::Require);
}

}  // namespace
