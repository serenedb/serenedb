////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <openssl/opensslv.h>

#include <boost/asio/ssl/context.hpp>
#include <cstdint>
#include <string>

#include "basics/asio_ns.h"
#include "basics/containers/flat_hash_set.h"

namespace sdb {

// SSL protocol methods
enum SslProtocol {
  kSslUnknown = 0,
  // newer versions of OpenSSL do not support SSLv2 by default.
  // from https://www.openssl.org/news/cl110.txt:
  //   Changes between 1.0.2f and 1.0.2g [1 Mar 2016]
  //   * Disable SSLv2 default build, default negotiation and weak ciphers.
  //   SSLv2
  //     is by default disabled at build-time.  Builds that are not configured
  //     with "enable-ssl2" will not support SSLv2.
  kSslV2 = 1,  // unsupported in SereneDB!!
  kSslV23 = 2,
  kSslV3 = 3,
  kTlsV1 = 4,
  kTlsV12 = 5,
  kTlsV13 = 6,
  kTlsGeneric = 9,

  kSslLast
};

#define SSL_CONST const

/// returns a set with all available SSL protocols
containers::FlatHashSet<uint64_t> AvailableSslProtocols();

/// returns a string description the available SSL protocols
std::string AvailableSslProtocolsDescription();

asio_ns::ssl::context SslContext(SslProtocol, const std::string& keyfile);

std::string ProtocolName(SslProtocol protocol);

std::string LastSslError();

}  // namespace sdb
