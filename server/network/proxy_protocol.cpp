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

#include "network/proxy_protocol.h"

#include <absl/strings/str_split.h>
#include <arpa/inet.h>
#include <fast_float/fast_float.h>

#include <array>
#include <charconv>
#include <cstring>
#include <string_view>
#include <vector>

namespace sdb::network {
namespace {

constexpr std::array<uint8_t, 12> kV2Sig = {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D,
                                            0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};
constexpr std::string_view kV1Prefix = "PROXY";
constexpr size_t kV1MaxLen = 107;  // "PROXY ... \r\n", spec maximum

uint16_t ParsePort(std::string_view s) {
  uint16_t v = 0;
  fast_float::from_chars(s.data(), s.data() + s.size(), v);
  return v;
}

ProxyResult ParseV1(std::span<const uint8_t> data) {
  const std::string_view text{reinterpret_cast<const char*>(data.data()),
                              data.size()};
  const auto crlf = text.find("\r\n");
  if (crlf == std::string_view::npos) {
    return ProxyResult{.status = data.size() > kV1MaxLen
                                   ? ProxyParse::Invalid
                                   : ProxyParse::NeedMore};
  }
  ProxyResult out;
  out.status = ProxyParse::Done;
  out.length = crlf + 2;
  const std::vector<std::string_view> f =
    absl::StrSplit(text.substr(0, crlf), ' ');
  // f[0]="PROXY"; f[1] in {TCP4,TCP6,UNKNOWN}; f[2]=src f[3]=dst f[4]=sport.
  if (f.size() >= 5 && (f[1] == "TCP4" || f[1] == "TCP6")) {
    out.source_addr = f[2];
    out.source_port = ParsePort(f[4]);
  }
  return out;
}

ProxyResult ParseV2(std::span<const uint8_t> data) {
  if (data.size() < 16) {
    return ProxyResult{.status = ProxyParse::NeedMore};
  }
  const uint8_t ver_cmd = data[12];
  const uint8_t fam = data[13];
  const size_t addr_len = (static_cast<size_t>(data[14]) << 8) | data[15];
  const size_t total = 16 + addr_len;
  if ((ver_cmd >> 4) != 0x2) {
    return ProxyResult{.status = ProxyParse::Invalid};
  }
  if (data.size() < total) {
    return ProxyResult{.status = ProxyParse::NeedMore};
  }
  ProxyResult out;
  out.status = ProxyParse::Done;
  out.length = total;
  const uint8_t cmd = ver_cmd & 0x0F;
  if (cmd != 0x1) {  // LOCAL (health check) -> no real peer address
    return out;
  }
  const uint8_t family = fam >> 4;
  char buf[INET6_ADDRSTRLEN] = {};
  if (family == 0x1 && addr_len >= 12) {  // AF_INET
    if (::inet_ntop(AF_INET, data.data() + 16, buf, sizeof(buf)) != nullptr) {
      out.source_addr = buf;
    }
    out.source_port = (static_cast<uint16_t>(data[24]) << 8) | data[25];
  } else if (family == 0x2 && addr_len >= 36) {  // AF_INET6
    if (::inet_ntop(AF_INET6, data.data() + 16, buf, sizeof(buf)) != nullptr) {
      out.source_addr = buf;
    }
    out.source_port = (static_cast<uint16_t>(data[48]) << 8) | data[49];
  }
  return out;
}

}  // namespace

ProxyResult ParseProxyHeader(std::span<const uint8_t> data) {
  if (data.empty()) {
    return ProxyResult{.status = ProxyParse::NeedMore};
  }
  if (data[0] == kV2Sig[0]) {
    const size_t n = std::min<size_t>(data.size(), kV2Sig.size());
    if (std::memcmp(data.data(), kV2Sig.data(), n) != 0) {
      return ProxyResult{.status = ProxyParse::NotProxy};
    }
    if (data.size() < kV2Sig.size()) {
      return ProxyResult{.status = ProxyParse::NeedMore};
    }
    return ParseV2(data);
  }
  if (data[0] == kV1Prefix[0]) {
    const size_t n = std::min<size_t>(data.size(), kV1Prefix.size());
    if (std::memcmp(data.data(), kV1Prefix.data(), n) != 0) {
      return ProxyResult{.status = ProxyParse::NotProxy};
    }
    if (data.size() < kV1Prefix.size()) {
      return ProxyResult{.status = ProxyParse::NeedMore};
    }
    return ParseV1(data);
  }
  return ProxyResult{.status = ProxyParse::NotProxy};
}

}  // namespace sdb::network
