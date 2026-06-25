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

#pragma once

#include <cstdint>
#include <span>
#include <string>

namespace sdb::network {

// HAProxy PROXY protocol (v1 text + v2 binary). When serenedb sits behind a TCP
// load balancer, the LB prefixes a PROXY header carrying the real client
// address; we parse it off the front of the stream so logging/auth see the true
// peer instead of the LB.
enum class ProxyParse : uint8_t {
  NeedMore,  // not enough bytes yet to decide / complete the header
  Done,      // a complete PROXY header was parsed (consume `length` bytes)
  NotProxy,  // the bytes are definitely not a PROXY header (no consume)
  Invalid,   // a malformed PROXY header (reject the connection)
};

struct ProxyResult {
  ProxyParse status = ProxyParse::NeedMore;
  size_t length = 0;        // header byte count to consume (status == Done)
  std::string source_addr;  // real client IP, textual; empty for UNKNOWN/LOCAL
  uint16_t source_port = 0;
};

// Parse a PROXY header from the front of `data` (a peek of the connection
// start). Pure; no IO. The caller peeks more on NeedMore, consumes `length` on
// Done, proceeds untouched on NotProxy, and closes on Invalid.
ProxyResult ParseProxyHeader(std::span<const uint8_t> data);

}  // namespace sdb::network
