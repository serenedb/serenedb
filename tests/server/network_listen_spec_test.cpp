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
#include <unistd.h>

#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "network/http/listen.h"
#include "network/listen_protocol.h"
#include "network/listen_spec.h"
#include "network/pg/listen.h"

using namespace sdb;
using namespace sdb::network;

namespace {

std::vector<ListenSpec> Parse(std::vector<std::string> urls) {
  return ParseListenSpecs(urls);
}

const ListenProtocol* Protocol(std::string_view scheme) {
  return Registry<ListenProtocol>::Find(scheme);
}

const http::Api* Api(std::string_view name) {
  return Registry<http::Api>::Find(name);
}

struct FlavorOptions final : ListenOptions {
  std::string flavor;
};

std::shared_ptr<const ListenOptions> ParseFlavor(
  const ListenSpec&, std::span<const ListenParam> params) {
  auto options = std::make_shared<FlavorOptions>();
  for (const auto& [key, value] : params) {
    EXPECT_EQ(key, "flavor");
    options->flavor = value;
  }
  return options;
}

std::unique_ptr<AcceptorBase> ListenFlavor(const ListenContext&,
                                           const ListenSpec&) {
  return nullptr;
}

SDB_LISTEN_PROTOCOL kFlavor{"listen-spec-test", &ParseFlavor, &ListenFlavor};

SDB_HTTP_API kListenSpecTestApi{"listen-spec-test"};

TEST(ListenSpec, DefaultIsLoopbackPg) {
  const auto specs = Parse({});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, Protocol("postgres"));
  EXPECT_EQ(specs[0].transport, ListenTransport::Tcp);
  EXPECT_EQ(specs[0].endpoint.address().to_string(), "127.0.0.1");
  EXPECT_EQ(specs[0].endpoint.port(), 7890);
}

TEST(ListenSpec, PgTcpV4) {
  const auto specs = Parse({"postgres://127.0.0.1:5432"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, Protocol("postgres"));
  EXPECT_EQ(specs[0].endpoint.port(), 5432);
  EXPECT_FALSE(specs[0].endpoint.address().is_v6());
  EXPECT_EQ(PgOptions(specs[0]).sslmode, SslMode::Prefer);
}

TEST(ListenSpec, PostgresqlAlias) {
  const auto specs = Parse({"postgresql://127.0.0.1:5432?sslmode=require"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, Protocol("postgresql"));
  EXPECT_EQ(PgOptions(specs[0]).sslmode, SslMode::Require);
}

TEST(ListenSpec, RegisteredProtocolParsesItsOwnParams) {
  const auto specs =
    Parse({"listen-spec-test://127.0.0.1:9999?flavor=plain&backlog=7"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, Protocol("listen-spec-test"));
  EXPECT_EQ(specs[0].transport, ListenTransport::Tcp);
  EXPECT_EQ(specs[0].endpoint.port(), 9999);
  ASSERT_TRUE(specs[0].backlog);
  EXPECT_EQ(*specs[0].backlog, 7);
  EXPECT_EQ(irs::utils::downCast<FlavorOptions>(*specs[0].options).flavor,
            "plain");
}

TEST(ListenSpec, UnknownSchemeListsTheRegisteredOnes) {
  EXPECT_DEATH(
    {
      dup2(STDERR_FILENO, STDOUT_FILENO);
      Parse({"mysql://127.0.0.1:3306"});
    },
    "unsupported scheme 'mysql' in endpoint 'mysql://127.0.0.1:3306' "
    "\\(([^)]*, )?http(, [^)]*)?, https(, [^)]*)?, postgres(, [^)]*)?, "
    "postgresql(, [^)]*)?\\)");
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
  EXPECT_EQ(specs[0].protocol, Protocol("postgres"));
  EXPECT_EQ(specs[1].protocol, Protocol("http"));
  const auto& apis = HttpOptions(specs[1]).apis;
  ASSERT_EQ(apis.size(), 1u);
  EXPECT_EQ(apis[0], Api("es"));
}

TEST(ListenSpec, HttpRepeatedApiListedOnce) {
  const auto specs =
    Parse({"http://127.0.0.1:9200?api=mcp&api=otel&api=mcp&api=otel"});
  ASSERT_EQ(specs.size(), 1u);
  const auto& apis = HttpOptions(specs[0]).apis;
  ASSERT_EQ(apis.size(), 2u);
  EXPECT_EQ(apis[0], Api("mcp"));
  EXPECT_EQ(apis[1], Api("otel"));
  const auto es = Parse({"http://127.0.0.1:9200?api=es&api=es"});
  ASSERT_EQ(HttpOptions(es[0]).apis.size(), 1u);
  EXPECT_EQ(HttpOptions(es[0]).apis[0], Api("es"));
}

TEST(ListenSpec, HttpRegisteredApi) {
  const auto specs =
    Parse({"http://127.0.0.1:9300?api=listen-spec-test&api=mcp&"
           "api=listen-spec-test"});
  ASSERT_EQ(specs.size(), 1u);
  const auto& apis = HttpOptions(specs[0]).apis;
  ASSERT_EQ(apis.size(), 2u);
  EXPECT_EQ(apis[0], Api("listen-spec-test"));
  EXPECT_EQ(apis[1], Api("mcp"));
}

TEST(ListenSpec, UnknownApiIsRejected) {
  EXPECT_DEATH(
    {
      dup2(STDERR_FILENO, STDOUT_FILENO);
      Parse({"http://127.0.0.1:9300?api=no-such-api"});
    },
    "unknown api 'no-such-api' in endpoint '[^']*' \\(known: ([^)]*, )?es(, "
    "[^)]*)?, mcp(, [^)]*)?, otel(, [^)]*)?, test(, [^)]*)?\\)");
}

TEST(ListenSpec, HttpDatabaseParam) {
  const auto specs = Parse({"http://127.0.0.1:4318?api=otel&db=telemetry"});
  ASSERT_EQ(specs.size(), 1u);
  const auto& options = HttpOptions(specs[0]);
  ASSERT_EQ(options.apis.size(), 1u);
  EXPECT_EQ(options.apis[0], Api("otel"));
  EXPECT_EQ(options.database, "telemetry");
  EXPECT_TRUE(
    HttpOptions(Parse({"http://127.0.0.1:4318?api=otel"})[0]).database.empty());
}

TEST(ListenSpec, HttpCreateSchemaParam) {
  EXPECT_TRUE(
    HttpOptions(Parse({"http://127.0.0.1:4318?api=otel"})[0]).create_schema);
  EXPECT_TRUE(
    HttpOptions(Parse({"http://127.0.0.1:4318?api=otel&create_schema=true"})[0])
      .create_schema);
  EXPECT_FALSE(
    HttpOptions(
      Parse({"http://127.0.0.1:4318?api=otel&create_schema=false"})[0])
      .create_schema);
}

TEST(ListenSpec, CreateSchemaOnPgIsRejected) {
  EXPECT_DEATH(
    {
      dup2(STDERR_FILENO, STDOUT_FILENO);
      Parse({"postgres://127.0.0.1:7890?create_schema=false"});
    },
    "unknown parameter 'create_schema' in endpoint");
}

TEST(ListenSpec, CreateSchemaNeedsBoolean) {
  EXPECT_DEATH(
    {
      dup2(STDERR_FILENO, STDOUT_FILENO);
      Parse({"http://127.0.0.1:4318?api=otel&create_schema=maybe"});
    },
    "invalid boolean for 'create_schema'");
}

struct Refused {
  std::string_view url;
  std::string_view message;
};

constexpr Refused kRefused[] = {
  {"postgres:///tmp/s.sock?sslmode=require",
   "'sslmode' is not valid on a unix endpoint"},
  {"postgres://127.0.0.1:1?sslmode=sometimes",
   "invalid sslmode in endpoint '[^']*': 'sometimes'"},
  {"postgres://127.0.0.1:1?port=5",
   "'port' query is only for a unix pg socket"},
  {"postgres://127.0.0.1:1?backlog=lots",
   "invalid number for 'backlog' in endpoint '[^']*': 'lots'"},
  {"postgres://127.0.0.1:1?api=es", "unknown parameter 'api' in endpoint"},
  {"http://127.0.0.1:1?api=es&sslmode=require",
   "'sslmode' is not valid on an http endpoint"},
  {"http://127.0.0.1:1?api=es&cert=/c.pem",
   "TLS material is not valid on a plaintext http endpoint"},
  {"https://127.0.0.1:1?api=es&proxy_protocol=require",
   "proxy_protocol is not supported on an https listener"},
  {"http://127.0.0.1:1?api=es&db=", "empty 'db' in endpoint"},
  {"http://127.0.0.1:1?api=es&schema=", "empty 'schema' in endpoint"},
  {"http://127.0.0.1:1?api=es&color=red", "unknown parameter 'color'"},
  {"http://127.0.0.1:1", "requires \\?api="},
  {"http://127.0.0.1:1?api=es&api=mcp",
   "api 'es' in endpoint '[^']*' needs a listener of its own"},
  {"https://127.0.0.1:1?api=otel&api=es&api=otel",
   "api 'es' in endpoint '[^']*' needs a listener of its own"},
};

TEST(ListenSpec, ProtocolParamsAreChecked) {
  for (const auto& refused : kRefused) {
    EXPECT_DEATH(
      {
        dup2(STDERR_FILENO, STDOUT_FILENO);
        Parse({std::string{refused.url}});
      },
      std::string{refused.message})
      << refused.url;
  }
}

TEST(ListenSpec, HttpsImpliesTls) {
  const auto specs = Parse({"https://127.0.0.1:9443?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].protocol, Protocol("https"));
  EXPECT_TRUE(HttpOptions(specs[0]).https);
  EXPECT_FALSE(HttpOptions(Parse({"http://127.0.0.1:9200?api=es"})[0]).https);
}

TEST(ListenSpec, SslModeParsed) {
  EXPECT_EQ(
    PgOptions(Parse({"postgres://127.0.0.1:1?sslmode=disable"})[0]).sslmode,
    SslMode::Disable);
  EXPECT_EQ(
    PgOptions(Parse({"postgres://127.0.0.1:1?sslmode=allow"})[0]).sslmode,
    SslMode::Allow);
  EXPECT_EQ(
    PgOptions(Parse({"postgres://127.0.0.1:1?sslmode=prefer"})[0]).sslmode,
    SslMode::Prefer);
  EXPECT_EQ(
    PgOptions(Parse({"postgres://127.0.0.1:1?sslmode=require"})[0]).sslmode,
    SslMode::Require);
  const auto ca =
    PgOptions(Parse({"postgres://127.0.0.1:1?sslmode=verify-ca"})[0]).sslmode;
  EXPECT_EQ(ca, SslMode::VerifyCa);
  EXPECT_TRUE(RequireTls(ca));
  EXPECT_TRUE(RequireClientCert(ca));
  const auto vf = Parse({"postgres://127.0.0.1:1?sslmode=verify-full"});
  const auto sslmode = PgOptions(vf[0]).sslmode;
  EXPECT_EQ(sslmode, SslMode::VerifyFull);
  EXPECT_TRUE(RequireTls(sslmode));
  EXPECT_TRUE(RequireClientCert(sslmode));
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
  EXPECT_EQ(specs[0].protocol, Protocol("postgres"));
  EXPECT_EQ(specs[0].unix_path, "/tmp/sdbsock");
  const auto& unix_port = PgOptions(specs[0]).unix_port;
  ASSERT_TRUE(unix_port);
  EXPECT_EQ(*unix_port, 5499);
}

TEST(ListenSpec, HttpKeepsItsDefaultPort) {
  const auto specs = Parse({"http://127.0.0.1:80?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].transport, ListenTransport::Tcp);
  EXPECT_EQ(specs[0].endpoint.port(), 80);
}

TEST(ListenSpec, HttpsKeepsItsDefaultPort) {
  const auto specs = Parse({"https://[::1]:443?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].endpoint.port(), 443);
}

TEST(ListenSpec, HttpWithoutPortUsesTheDefault) {
  const auto http = Parse({"http://0.0.0.0?api=es"});
  ASSERT_EQ(http.size(), 1u);
  EXPECT_EQ(http[0].endpoint.port(), 80);
  const auto https = Parse({"https://0.0.0.0?api=es"});
  ASSERT_EQ(https.size(), 1u);
  EXPECT_EQ(https[0].endpoint.port(), 443);
}

TEST(ListenSpec, HttpWithEmptyPortUsesTheDefault) {
  const auto specs = Parse({"http://0.0.0.0:?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].endpoint.port(), 80);
}

TEST(ListenSpec, PgWithoutPortIsRejected) {
  EXPECT_DEATH(
    {
      dup2(STDERR_FILENO, STDOUT_FILENO);
      Parse({"postgres://127.0.0.1"});
    },
    "missing or invalid port");
}

TEST(ListenSpec, MissingSlashesAreRejected) {
  EXPECT_DEATH(
    {
      dup2(STDERR_FILENO, STDOUT_FILENO);
      Parse({"postgres:5432"});
    },
    "expected scheme://");
}

TEST(ListenSpec, TcpPathIsIgnored) {
  const auto specs = Parse({"postgres://127.0.0.1:7890/postgres"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].transport, ListenTransport::Tcp);
  EXPECT_EQ(specs[0].endpoint.port(), 7890);
}

TEST(ListenSpec, HttpPathIsIgnored) {
  const auto specs = Parse({"http://127.0.0.1:9200/x?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_EQ(specs[0].transport, ListenTransport::Tcp);
  EXPECT_EQ(specs[0].endpoint.port(), 9200);
}

TEST(ListenSpec, HttpBackslashIsRejected) {
  EXPECT_DEATH(
    {
      dup2(STDERR_FILENO, STDOUT_FILENO);
      Parse({"http://0.0.0.0\\:8080?api=es"});
    },
    "is not allowed");
}

TEST(ListenSpec, HttpIPv6Brackets) {
  const auto specs = Parse({"http://[::1]:9200?api=es"});
  ASSERT_EQ(specs.size(), 1u);
  EXPECT_TRUE(specs[0].endpoint.address().is_v6());
  EXPECT_EQ(specs[0].endpoint.port(), 9200);
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
