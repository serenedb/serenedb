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
#include <memory>
#include <yaclib/algo/wait_group.hpp>

#include "network/acceptor.h"
#include "network/cancel_registry.h"
#include "network/http/router.h"
#include "network/http/session.h"
#include "network/io_context.h"
#include "server/utils/asio_ns.h"

namespace sdb::test {

using HttpAcceptor =
  network::Acceptor<network::HttpSession<network::SocketKind::Tcp>>;

inline asio_ns::ip::tcp::endpoint Loopback(std::uint16_t port) {
  return {asio_ns::ip::make_address("127.0.0.1"), port};
}

// Wires the deps the acceptor requires the same way Server does (cancel
// registry + session WaitGroup) and tears down in the server's order:
// stop accepting, terminate live sessions, wait for their futures, then
// stop the pool.
class HttpServerHarness {
 public:
  explicit HttpServerHarness(network::HttpRouter& router)
    : _context{.router = router} {
    _context.cancel = &_cancel;
    _context.sessions = &_sessions;
    _pool.Start();
    _acceptor = std::make_shared<HttpAcceptor>(_pool, Loopback(0), _context);
    server = Loopback(_acceptor->LocalEndpoint().port());
    _acceptor->Start();
  }

  ~HttpServerHarness() {
    _acceptor->Stop();
    _cancel.TerminateAll();
    _sessions.Done();
    _sessions.Wait();
    _pool.Stop();
  }

  asio_ns::ip::tcp::endpoint server;

 private:
  network::IoThreadPool _pool{1};
  network::CancelRegistry _cancel;
  yaclib::WaitGroup<> _sessions{1};
  network::HttpServerContext _context;
  std::shared_ptr<HttpAcceptor> _acceptor;
};

}  // namespace sdb::test
