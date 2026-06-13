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

#include <memory>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/asio_ns.h"
#include "network/asio_awaitable.h"
#include "network/io_context.h"

namespace sdb::network {

class AcceptorBase {
 public:
  virtual ~AcceptorBase() = default;
  virtual void Start() = 0;
  virtual void Stop() noexcept = 0;
};

template<typename Session>
class Acceptor final : public AcceptorBase,
                       public std::enable_shared_from_this<Acceptor<Session>> {
 public:
  Acceptor(IoThreadPool& pool, const asio_ns::ip::tcp::endpoint& bind,
           typename Session::Deps& deps)
    : _pool{pool}, _acceptor{pool.Next().Context()}, _deps{deps} {
    _acceptor.open(bind.protocol());
    _acceptor.set_option(asio_ns::socket_base::reuse_address{true});
    _acceptor.bind(bind);
    _acceptor.listen();
  }

  Acceptor(const Acceptor&) = delete;
  Acceptor& operator=(const Acceptor&) = delete;

  void Start() override {
    asio_ns::post(_acceptor.get_executor(),
                  [self = this->shared_from_this()] { self->Run().Detach(); });
  }

  void Stop() noexcept override {
    asio_ns::post(_acceptor.get_executor(), [self = this->shared_from_this()] {
      self->_running = false;
      asio_ns::error_code ec;
      self->_acceptor.close(ec);
    });
  }

  asio_ns::ip::tcp::endpoint LocalEndpoint() const {
    return _acceptor.local_endpoint();
  }

 private:
  yaclib::Future<> Run() {
    auto self = this->shared_from_this();
    while (_running) {
      auto connection = std::make_shared<Session>(_deps, _pool.Next());
      if (co_await AcceptInto(connection->Lowest()).NoThrow()) {
        // Accept failed: either a clean shutdown closed the acceptor, or a
        // transient error (e.g. fd exhaustion) -- drop this attempt and retry.
        if (!_running) {
          break;
        }
        continue;
      }
      // Sessions batch writes themselves (message::Buffer); Nagle on top only
      // adds delayed-ACK stalls to multi-write responses.
      asio_ns::error_code ignored;
      connection->Lowest().set_option(asio_ns::ip::tcp::no_delay{true}, ignored);
      asio_ns::post(connection->Lowest().get_executor(),
                    [connection] { connection->Start(); });
    }
    co_return {};
  }

  [[nodiscard]] auto AcceptInto(asio_ns::ip::tcp::socket& peer) {
    return Async<void>([this, &peer](auto&& handler) {
      _acceptor.async_accept(peer, std::forward<decltype(handler)>(handler));
    });
  }

  IoThreadPool& _pool;
  asio_ns::ip::tcp::acceptor _acceptor;
  typename Session::Deps& _deps;
  bool _running{true};
};

}  // namespace sdb::network
