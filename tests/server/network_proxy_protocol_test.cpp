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

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "network/proxy_protocol.h"

using namespace sdb::network;

namespace {

std::span<const uint8_t> Bytes(std::string_view s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

TEST(ProxyProtocol, V1Tcp4) {
  const std::string_view hdr =
    "PROXY TCP4 1.2.3.4 5.6.7.8 1111 2222\r\nSTARTUP";
  const auto r = ParseProxyHeader(Bytes(hdr));
  EXPECT_EQ(r.status, ProxyParse::Done);
  EXPECT_EQ(r.source_addr, "1.2.3.4");
  EXPECT_EQ(r.source_port, 1111);
  EXPECT_EQ(
    r.length,
    std::string_view("PROXY TCP4 1.2.3.4 5.6.7.8 1111 2222\r\n").size());
}

TEST(ProxyProtocol, V1Unknown) {
  const auto r = ParseProxyHeader(Bytes("PROXY UNKNOWN\r\nX"));
  EXPECT_EQ(r.status, ProxyParse::Done);
  EXPECT_TRUE(r.source_addr.empty());
  EXPECT_EQ(r.length, std::string_view("PROXY UNKNOWN\r\n").size());
}

TEST(ProxyProtocol, V1NeedMore) {
  const auto r = ParseProxyHeader(Bytes("PROXY TCP4 1.2.3.4"));
  EXPECT_EQ(r.status, ProxyParse::NeedMore);
}

TEST(ProxyProtocol, V1TooLong) {
  std::string s = "PROXY ";
  s.append(200, 'x');  // no CRLF, exceeds the 107-byte maximum
  const auto r = ParseProxyHeader(Bytes(s));
  EXPECT_EQ(r.status, ProxyParse::Invalid);
}

TEST(ProxyProtocol, NotProxyHttp) {
  const auto r = ParseProxyHeader(Bytes("GET / HTTP/1.1\r\n"));
  EXPECT_EQ(r.status, ProxyParse::NotProxy);
}

TEST(ProxyProtocol, NotProxyPgStartup) {
  // A pg StartupMessage begins with a 4-byte length, not 'P' or 0x0D.
  const uint8_t startup[] = {0x00, 0x00, 0x00, 0x08, 0x00, 0x03, 0x00, 0x00};
  const auto r = ParseProxyHeader(std::span<const uint8_t>{startup});
  EXPECT_EQ(r.status, ProxyParse::NotProxy);
}

TEST(ProxyProtocol, V2Tcp4) {
  std::vector<uint8_t> v = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D,
                            0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};
  v.push_back(0x21);                // version 2, command PROXY
  v.push_back(0x11);                // AF_INET, STREAM
  v.push_back(0x00);                // addr len hi
  v.push_back(12);                  // addr len lo (src4 dst4 sport2 dport2)
  v.insert(v.end(), {1, 2, 3, 4});  // src 1.2.3.4
  v.insert(v.end(), {5, 6, 7, 8});  // dst
  v.insert(v.end(), {0x04, 0xD2});  // sport 1234
  v.insert(v.end(), {0x16, 0x2E});  // dport
  v.insert(v.end(), {'X', 'Y'});    // trailing protocol bytes
  const auto r = ParseProxyHeader(std::span<const uint8_t>{v});
  EXPECT_EQ(r.status, ProxyParse::Done);
  EXPECT_EQ(r.source_addr, "1.2.3.4");
  EXPECT_EQ(r.source_port, 1234);
  EXPECT_EQ(r.length, 28u);  // 16 header + 12 addr
}

TEST(ProxyProtocol, V2NeedMore) {
  const uint8_t partial[] = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A};
  const auto r = ParseProxyHeader(std::span<const uint8_t>{partial});
  EXPECT_EQ(r.status, ProxyParse::NeedMore);
}

TEST(ProxyProtocol, V2Local) {
  std::vector<uint8_t> v = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D,
                            0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};
  v.push_back(0x20);  // version 2, command LOCAL (health check)
  v.push_back(0x00);  // AF_UNSPEC
  v.push_back(0x00);
  v.push_back(0x00);  // addr len 0
  const auto r = ParseProxyHeader(std::span<const uint8_t>{v});
  EXPECT_EQ(r.status, ProxyParse::Done);
  EXPECT_TRUE(r.source_addr.empty());
  EXPECT_EQ(r.length, 16u);
}

}  // namespace
