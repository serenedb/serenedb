////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "basics/common.h"
#include "basics/logger/logger.h"
#include "endpoint/endpoint.h"
#include "endpoint/endpoint_ip.h"
#include "endpoint/endpoint_ip_v4.h"
#include "endpoint/endpoint_ip_v6.h"
#include "endpoint/endpoint_unix_domain.h"
#include "gtest/gtest.h"

using namespace sdb;
using namespace sdb::basics;
using namespace std;

#define FACTORY_NAME(name) name##Factory

#define FACTORY(name, specification) \
  sdb::Endpoint::FACTORY_NAME(name)(specification)

#define CHECK_ENDPOINT_FEATURE(type, specification, feature, expected) \
  e = FACTORY(type, specification);                                    \
  EXPECT_EQ((expected), (e->feature()));

#define CHECK_ENDPOINT_SERVER_FEATURE(type, specification, feature, expected) \
  e = sdb::Endpoint::serverFactory(specification, 1, true);                   \
  EXPECT_EQ((expected), (e->feature()));

TEST(EndpointTest, EndpointInvalid) {
  std::unique_ptr<Endpoint> e;

  EXPECT_TRUE(e == sdb::Endpoint::clientFactory(""));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("@"));

  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("http://"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("ssl://"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("unix://"));

  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("fish://127.0.0.1:8529"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("htp://127.0.0.1:8529"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("ftp://127.0.0.1:8529"));

  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("tcp//127.0.0.1:8529"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("tcp:127.0.0.1:8529"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("ssl:localhost"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("ssl//:localhost"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("unix///tmp/socket"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("unix:tmp/socket"));

  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("fish@tcp://127.0.0.1:8529"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("ssl@tcp://127.0.0.1:8529"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("https@tcp://127.0.0.1:8529"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("https@tcp://127.0.0.1:"));

  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("tcp://127.0.0.1:65536"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("tcp://127.0.0.1:65537"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("tcp://127.0.0.1:-1"));
  EXPECT_TRUE(e == sdb::Endpoint::clientFactory("tcp://127.0.0.1:6555555555"));
}

////////////////////////////////////////////////////////////////////////////////
/// test specification
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointSpecification) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", specification,
                         "http+tcp://127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", specification,
                         "http+tcp://127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", specification,
                         "http+ssl://127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "SSL://127.0.0.5", specification,
                         "http+ssl://127.0.0.5:8529");
  CHECK_ENDPOINT_FEATURE(client, "httP@ssl://localhost:4635", specification,
                         "http+ssl://127.0.0.1:4635");
  CHECK_ENDPOINT_FEATURE(client, "http://localhost", specification,
                         "http+tcp://127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "https://localhost", specification,
                         "http+ssl://127.0.0.1:8529");

  CHECK_ENDPOINT_SERVER_FEATURE(server, "unix:///path/to/socket", specification,
                                "http+unix:///path/to/socket");
  CHECK_ENDPOINT_SERVER_FEATURE(server, "htTp@UNIx:///a/b/c/d/e/f.s",
                                specification, "http+unix:///a/b/c/d/e/f.s");
}

////////////////////////////////////////////////////////////////////////////////
/// test types
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointTypes) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", type,
                         sdb::Endpoint::EndpointType::Client);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", type,
                         sdb::Endpoint::EndpointType::Client);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1", type,
                         sdb::Endpoint::EndpointType::Client);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", type,
                         sdb::Endpoint::EndpointType::Client);
  CHECK_ENDPOINT_FEATURE(client, "unix:///path/to/socket", type,
                         sdb::Endpoint::EndpointType::Client);

  CHECK_ENDPOINT_SERVER_FEATURE(server, "tcp://127.0.0.1", type,
                                sdb::Endpoint::EndpointType::Server);
  CHECK_ENDPOINT_SERVER_FEATURE(server, "tcp://localhost", type,
                                sdb::Endpoint::EndpointType::Server);
  CHECK_ENDPOINT_SERVER_FEATURE(server, "http://localhost", type,
                                sdb::Endpoint::EndpointType::Server);
  CHECK_ENDPOINT_SERVER_FEATURE(server, "ssl://127.0.0.1", type,
                                sdb::Endpoint::EndpointType::Server);
  CHECK_ENDPOINT_SERVER_FEATURE(server, "ssl://localhost", type,
                                sdb::Endpoint::EndpointType::Server);
  CHECK_ENDPOINT_SERVER_FEATURE(server, "https://localhost", type,
                                sdb::Endpoint::EndpointType::Server);
  CHECK_ENDPOINT_SERVER_FEATURE(server, "unix:///path/to/socket", type,
                                sdb::Endpoint::EndpointType::Server);
}

////////////////////////////////////////////////////////////////////////////////
/// test domains
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointDomains) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "tcp://192.168.173.13", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:8529", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:8529", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org:8529", domain,
                         AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:8529", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8529", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client,
                         "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529",
                         domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(
    client, "http@tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529", domain,
    AF_INET6);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "ssl://192.168.173.13", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:8529", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:8529", domain, AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org:8529", domain,
                         AF_INET);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:8529", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:8529", domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(client,
                         "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529",
                         domain, AF_INET6);
  CHECK_ENDPOINT_FEATURE(
    client, "http@ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529", domain,
    AF_INET6);

  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket", domain, AF_UNIX);
  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket/serene.sock", domain,
                         AF_UNIX);
  CHECK_ENDPOINT_FEATURE(client, "http@unix:///tmp/socket/serene.sock", domain,
                         AF_UNIX);
}

////////////////////////////////////////////////////////////////////////////////
/// test domain types
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointDomainTypes) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:8529", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:8529", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org:8529", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:8529", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8529", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client,
                         "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529",
                         domainType, sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "TCP://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "Tcp://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "tCP://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "http@tcp://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);

  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:8529", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:8529", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org:8529", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:8529", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:8529", domainType,
                         sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client,
                         "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529",
                         domainType, sdb::Endpoint::DomainType::IPv6);
  CHECK_ENDPOINT_FEATURE(client, "SSL://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "Ssl://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "sSL://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);
  CHECK_ENDPOINT_FEATURE(client, "http@ssl://127.0.0.1", domainType,
                         sdb::Endpoint::DomainType::IPv4);

  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket", domainType,
                         sdb::Endpoint::DomainType::UNIX);
  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket/serene.sock", domainType,
                         sdb::Endpoint::DomainType::UNIX);
  CHECK_ENDPOINT_FEATURE(client, "UNIX:///tmp/socket", domainType,
                         sdb::Endpoint::DomainType::UNIX);
  CHECK_ENDPOINT_FEATURE(client, "Unix:///tmp/socket", domainType,
                         sdb::Endpoint::DomainType::UNIX);
  CHECK_ENDPOINT_FEATURE(client, "uNIX:///tmp/socket", domainType,
                         sdb::Endpoint::DomainType::UNIX);
  CHECK_ENDPOINT_FEATURE(client, "http@unix:///tmp/socket", domainType,
                         sdb::Endpoint::DomainType::UNIX);
}

////////////////////////////////////////////////////////////////////////////////
/// test ports
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointPorts) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "http@tcp://www.serenedb.org:8529", port,
                         8529);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(
    client, "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:666", port, 666);
  CHECK_ENDPOINT_FEATURE(
    client, "http@tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:666", port,
    666);
  CHECK_ENDPOINT_FEATURE(client, "http://localhost:65535", port, 65535);

  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "https://www.serenedb.org:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "http@ssl://www.serenedb.org:8529", port,
                         8529);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]", port,
                         EndpointIp::kDefaultPortHttp);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:8529", port, 8529);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:8532", port, 8532);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:80", port, 80);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:443", port, 443);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:65535", port, 65535);
  CHECK_ENDPOINT_FEATURE(
    client, "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:666", port, 666);
  CHECK_ENDPOINT_FEATURE(
    client, "http@ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:666", port,
    666);

  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket", port, 0);
  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket/serene.sock", port, 0);
  CHECK_ENDPOINT_FEATURE(client, "http@unix:///tmp/socket/serene.sock", port,
                         0);
}

////////////////////////////////////////////////////////////////////////////////
/// test encryption
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointEncryption) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:8529", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:8529", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org:8529", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:8529", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client,
                         "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:666",
                         encryption, sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(
    client, "http@tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:666",
    encryption, sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "http://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::None);

  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client,
                         "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:666",
                         encryption, sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "SSL://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "Ssl://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "sSL://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "http@ssl://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);
  CHECK_ENDPOINT_FEATURE(client, "https://[::]:8529", encryption,
                         sdb::Endpoint::EncryptionType::SSL);

  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket/serene.sock", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "UNIX:///tmp/socket/serene.sock", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "Unix:///tmp/socket/serene.sock", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "uNIX:///tmp/socket/serene.sock", encryption,
                         sdb::Endpoint::EncryptionType::None);
  CHECK_ENDPOINT_FEATURE(client, "http@unix:///tmp/socket/serene.sock",
                         encryption, sdb::Endpoint::EncryptionType::None);
}

////////////////////////////////////////////////////////////////////////////////
/// test host
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointHost) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org", host,
                         "www.serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "tcp://serenedb.org", host, "serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "tcp://DE.triagens.SereneDB.org", host,
                         "de.triagens.serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "tcp://192.168.173.13:8529", host,
                         "192.168.173.13");
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:8529", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:8529", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org:8529", host,
                         "www.serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "tcp://serenedb.org:8529", host,
                         "serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]", host, "::");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:8529", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8529", host, "::");
  CHECK_ENDPOINT_FEATURE(client,
                         "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]",
                         host, "2001:0db8:0000:0000:0000:ff00:0042:8329");
  CHECK_ENDPOINT_FEATURE(client,
                         "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529",
                         host, "2001:0db8:0000:0000:0000:ff00:0042:8329");
  CHECK_ENDPOINT_FEATURE(client, "http@tcp://[::]:8529", host, "::");

  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org", host,
                         "www.serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "ssl://serenedb.org", host, "serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "ssl://DE.triagens.SereneDB.org", host,
                         "de.triagens.serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "ssl://192.168.173.13:8529", host,
                         "192.168.173.13");
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:8529", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org:8529", host,
                         "www.serenedb.org");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]", host, "::");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:8529", host, "127.0.0.1");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:8529", host, "::");
  CHECK_ENDPOINT_FEATURE(client,
                         "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]",
                         host, "2001:0db8:0000:0000:0000:ff00:0042:8329");
  CHECK_ENDPOINT_FEATURE(client,
                         "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529",
                         host, "2001:0db8:0000:0000:0000:ff00:0042:8329");
  CHECK_ENDPOINT_FEATURE(client, "http@ssl://[::]:8529", host, "::");

  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket", host, "localhost");
  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket/serene.sock", host,
                         "localhost");
  CHECK_ENDPOINT_FEATURE(client, "http@unix:///tmp/socket/serene.sock", host,
                         "localhost");
}

////////////////////////////////////////////////////////////////////////////////
/// test hoststring
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointHostString) {
  std::unique_ptr<Endpoint> e;

  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1", hostAndPort,
                         "127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost", hostAndPort,
                         "127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org", hostAndPort,
                         "www.serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://serenedb.org", hostAndPort,
                         "serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://DE.triagens.SereneDB.org", hostAndPort,
                         "de.triagens.serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://192.168.173.13:8529", hostAndPort,
                         "192.168.173.13:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://192.168.173.13:678", hostAndPort,
                         "192.168.173.13:678");
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:8529", hostAndPort,
                         "127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://127.0.0.1:44", hostAndPort,
                         "127.0.0.1:44");
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:8529", hostAndPort,
                         "127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://localhost:65535", hostAndPort,
                         "127.0.0.1:65535");
  CHECK_ENDPOINT_FEATURE(client, "tcp://www.serenedb.org:8529", hostAndPort,
                         "www.serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://serenedb.org:8529", hostAndPort,
                         "serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]", hostAndPort,
                         "[127.0.0.1]:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]", hostAndPort, "[::]:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:8529", hostAndPort,
                         "[127.0.0.1]:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:80", hostAndPort,
                         "[127.0.0.1]:80");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:555", hostAndPort,
                         "[127.0.0.1]:555");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[127.0.0.1]:65535", hostAndPort,
                         "[127.0.0.1]:65535");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8529", hostAndPort, "[::]:8529");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:80", hostAndPort, "[::]:80");
  CHECK_ENDPOINT_FEATURE(client, "tcp://[::]:8080", hostAndPort, "[::]:8080");
  CHECK_ENDPOINT_FEATURE(
    client, "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]", hostAndPort,
    "[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529");
  CHECK_ENDPOINT_FEATURE(
    client, "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529", hostAndPort,
    "[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529");
  CHECK_ENDPOINT_FEATURE(
    client, "tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:777", hostAndPort,
    "[2001:0db8:0000:0000:0000:ff00:0042:8329]:777");
  CHECK_ENDPOINT_FEATURE(
    client, "http@tcp://[2001:0db8:0000:0000:0000:ff00:0042:8329]:777",
    hostAndPort, "[2001:0db8:0000:0000:0000:ff00:0042:8329]:777");

  CHECK_ENDPOINT_FEATURE(client, "ssl://127.0.0.1", hostAndPort,
                         "127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost", hostAndPort,
                         "127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org", hostAndPort,
                         "www.serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://serenedb.org", hostAndPort,
                         "serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://DE.triagens.SereneDB.org", hostAndPort,
                         "de.triagens.serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://192.168.173.13:8529", hostAndPort,
                         "192.168.173.13:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://192.168.173.13:1234", hostAndPort,
                         "192.168.173.13:1234");
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:8529", hostAndPort,
                         "127.0.0.1:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://localhost:5", hostAndPort,
                         "127.0.0.1:5");
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org:8529", hostAndPort,
                         "www.serenedb.org:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://www.serenedb.org:12345", hostAndPort,
                         "www.serenedb.org:12345");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]", hostAndPort,
                         "[127.0.0.1]:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]", hostAndPort, "[::]:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:8529", hostAndPort,
                         "[127.0.0.1]:8529");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[127.0.0.1]:32768", hostAndPort,
                         "[127.0.0.1]:32768");
  CHECK_ENDPOINT_FEATURE(client, "ssl://[::]:8529", hostAndPort, "[::]:8529");
  CHECK_ENDPOINT_FEATURE(
    client, "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]", hostAndPort,
    "[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529");
  CHECK_ENDPOINT_FEATURE(
    client, "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529", hostAndPort,
    "[2001:0db8:0000:0000:0000:ff00:0042:8329]:8529");
  CHECK_ENDPOINT_FEATURE(
    client, "ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:994", hostAndPort,
    "[2001:0db8:0000:0000:0000:ff00:0042:8329]:994");
  CHECK_ENDPOINT_FEATURE(
    client, "http@ssl://[2001:0db8:0000:0000:0000:ff00:0042:8329]:994",
    hostAndPort, "[2001:0db8:0000:0000:0000:ff00:0042:8329]:994");

  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket", hostAndPort,
                         "localhost");
  CHECK_ENDPOINT_FEATURE(client, "unix:///tmp/socket/serene.sock", hostAndPort,
                         "localhost");
  CHECK_ENDPOINT_FEATURE(client, "http@unix:///tmp/socket/serene.sock",
                         hostAndPort, "localhost");
}

////////////////////////////////////////////////////////////////////////////////
/// test isconnected
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointIsConnectedServer1) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::serverFactory("tcp://127.0.0.1", 1, true);
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test isconnected
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointIsConnectedServer2) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::serverFactory("ssl://127.0.0.1", 1, true);
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test isconnected
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointIsConnectedServer3) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::serverFactory("unix:///tmp/socket", 1, true);
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test isconnected
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointIsConnectedClient1) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::clientFactory("tcp://127.0.0.1");
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test isconnected
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointIsConnectedClient2) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::clientFactory("ssl://127.0.0.1");
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test isconnected
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointIsConnectedClient3) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::clientFactory("unix:///tmp/socket");
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test server endpoint
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointServerTcpIpv4WithPort) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::serverFactory("tcp://127.0.0.1:667", 1, true);
  EXPECT_TRUE("http+tcp://127.0.0.1:667" == e->specification());
  EXPECT_TRUE(sdb::Endpoint::EndpointType::Server == e->type());
  EXPECT_TRUE(sdb::Endpoint::DomainType::IPv4 == e->domainType());
  EXPECT_TRUE(sdb::Endpoint::EncryptionType::None == e->encryption());
  EXPECT_TRUE(AF_INET == e->domain());
  EXPECT_TRUE("127.0.0.1" == e->host());
  EXPECT_TRUE(667 == e->port());
  EXPECT_TRUE("127.0.0.1:667" == e->hostAndPort());
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test server endpoint
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointServerUnix) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::serverFactory("unix:///path/to/serene.sock", 1, true);
  EXPECT_TRUE("http+unix:///path/to/serene.sock" == e->specification());
  EXPECT_TRUE(sdb::Endpoint::EndpointType::Server == e->type());
  EXPECT_TRUE(sdb::Endpoint::DomainType::UNIX == e->domainType());
  EXPECT_TRUE(sdb::Endpoint::EncryptionType::None == e->encryption());
  EXPECT_TRUE(AF_UNIX == e->domain());
  EXPECT_TRUE("localhost" == e->host());
  EXPECT_TRUE(0 == e->port());
  EXPECT_TRUE("localhost" == e->hostAndPort());
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test client endpoint
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointClientSslIpV6WithPortHttp) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::clientFactory(
    "http+ssl://[0001:0002:0003:0004:0005:0006:0007:0008]:43425");
  EXPECT_TRUE("http+ssl://[0001:0002:0003:0004:0005:0006:0007:0008]:43425" ==
              e->specification());
  EXPECT_TRUE(sdb::Endpoint::EndpointType::Client == e->type());
  EXPECT_TRUE(sdb::Endpoint::DomainType::IPv6 == e->domainType());
  EXPECT_TRUE(sdb::Endpoint::EncryptionType::SSL == e->encryption());
  EXPECT_TRUE(AF_INET6 == e->domain());
  EXPECT_TRUE("0001:0002:0003:0004:0005:0006:0007:0008" == e->host());
  EXPECT_TRUE(43425 == e->port());
  EXPECT_TRUE("[0001:0002:0003:0004:0005:0006:0007:0008]:43425" ==
              e->hostAndPort());
  EXPECT_TRUE(false == e->isConnected());
}

////////////////////////////////////////////////////////////////////////////////
/// test client endpoint
////////////////////////////////////////////////////////////////////////////////

TEST(EndpointTest, EndpointClientTcpIpv6WithoutPort) {
  std::unique_ptr<Endpoint> e;

  e = sdb::Endpoint::clientFactory("tcp://[::]");
  EXPECT_TRUE("http+tcp://[::]:8529" == e->specification());
  EXPECT_TRUE(sdb::Endpoint::EndpointType::Client == e->type());
  EXPECT_TRUE(sdb::Endpoint::DomainType::IPv6 == e->domainType());
  EXPECT_TRUE(sdb::Endpoint::EncryptionType::None == e->encryption());
  EXPECT_TRUE(AF_INET6 == e->domain());
  EXPECT_TRUE("::" == e->host());
  EXPECT_TRUE(8529 == e->port());
  EXPECT_TRUE("[::]:8529" == e->hostAndPort());
  EXPECT_TRUE(false == e->isConnected());
}
