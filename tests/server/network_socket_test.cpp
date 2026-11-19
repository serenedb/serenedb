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

#include "network/socket.h"

namespace sdb::network {
namespace {

TEST(NetworkSocketTest, AlpnProtocolIsEmptyBeforeTheHandshake) {
  asio_ns::io_context io;
  asio_ns::ssl::context ssl{asio_ns::ssl::context::tls_server};
  Socket<SocketKind::Ssl> socket{io, ssl};
  EXPECT_TRUE(socket.AlpnProtocol().empty());
  Socket<SocketKind::MaybeTls> upgradeable{io, ssl};
  EXPECT_TRUE(upgradeable.AlpnProtocol().empty());
}

}  // namespace
}  // namespace sdb::network
