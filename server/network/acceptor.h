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

#include <grp.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/task.hpp>

#include "basics/asio_ns.h"
#include "network/asio_awaitable.h"
#include "network/io_context.h"
#include "network/socket.h"

namespace sdb::network {

class AcceptorBase {
 public:
  virtual ~AcceptorBase() = default;
  virtual void Start() = 0;
  virtual void Stop() noexcept = 0;
};

struct AcceptorOptions {
  bool v6_only = false;
  std::optional<bool> reuseport;
  std::optional<int> backlog;
  // Per-accepted-connection TCP keepalive (ignored on unix):
  std::optional<bool> keepalive;
  std::optional<uint32_t> keepidle;
  std::optional<uint32_t> keepintvl;
  std::optional<uint32_t> keepcnt;
  // Unix-domain only:
  std::optional<uint32_t> unix_mode;
  std::string unix_group;
};

template<typename Session>
class Acceptor final : public AcceptorBase,
                       public std::enable_shared_from_this<Acceptor<Session>> {
  static constexpr bool kUnix = Session::kSocketKind == SocketKind::Unix;
  using Protocol = std::conditional_t<kUnix, asio_ns::local::stream_protocol,
                                      asio_ns::ip::tcp>;
  using EndpointType = typename Protocol::endpoint;
  using SocketType = typename Protocol::socket;

 public:
  Acceptor(IoThreadPool& pool, const EndpointType& bind,
           typename Session::Deps& deps, const AcceptorOptions& opts = {})
    : _pool{pool}, _acceptor{pool.Next().Context()}, _deps{deps}, _opts{opts} {
    asio_ns::error_code ec;
    if constexpr (kUnix) {
      const std::string path = bind.path();
      const bool abstract = !path.empty() && path.front() == '\0';
      if (!abstract) {
        ::unlink(path.c_str());
      }
      _acceptor.open();
      _acceptor.bind(bind);
      ListenWith(opts);
      if (!abstract) {
        if (opts.unix_mode) {
          ::chmod(path.c_str(), static_cast<::mode_t>(*opts.unix_mode));
        }
        if (!opts.unix_group.empty()) {
          if (const struct group* gr = ::getgrnam(opts.unix_group.c_str());
              gr != nullptr) {
            ::chown(path.c_str(), static_cast<::uid_t>(-1), gr->gr_gid);
          }
        }
      }
    } else {
      _acceptor.open(bind.protocol());
      _acceptor.set_option(asio_ns::socket_base::reuse_address{true});
      if (bind.address().is_v6() && opts.v6_only) {
        _acceptor.set_option(asio_ns::ip::v6_only{true}, ec);
      }
#ifdef SO_REUSEPORT
      if (opts.reuseport.value_or(false)) {
        using ReusePort =
          asio_ns::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>;
        _acceptor.set_option(ReusePort{true}, ec);
      }
#endif
      _acceptor.bind(bind);
      ListenWith(opts);
    }
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

  EndpointType LocalEndpoint() const { return _acceptor.local_endpoint(); }

 private:
  void ListenWith(const AcceptorOptions& opts) {
    if (opts.backlog) {
      _acceptor.listen(*opts.backlog);
    } else {
      _acceptor.listen();
    }
  }

  yaclib::Task<> Run() {
    auto self = this->shared_from_this();
    while (_running) {
      auto connection = duckdb::make_shared_ptr<Session>(_deps, _pool.Next());
      if (co_await AcceptInto(connection->Lowest()).NoThrow()) {
        // Accept failed: either a clean shutdown closed the acceptor, or a
        // transient error (e.g. fd exhaustion) -- drop this attempt and retry.
        if (!_running) {
          break;
        }
        continue;
      }
      if constexpr (!kUnix) {
        // Sessions batch writes themselves (message::Buffer); Nagle on top only
        // adds delayed-ACK stalls to multi-write responses. Not valid on unix.
        asio_ns::error_code ignored;
        connection->Lowest().set_option(asio_ns::ip::tcp::no_delay{true},
                                        ignored);
        if (_opts.keepalive) {
          connection->Lowest().set_option(
            asio_ns::socket_base::keep_alive{*_opts.keepalive}, ignored);
        }
        const int fd = connection->Lowest().native_handle();
        if (_opts.keepidle) {
          const int v = static_cast<int>(*_opts.keepidle);
          ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &v, sizeof(v));
        }
        if (_opts.keepintvl) {
          const int v = static_cast<int>(*_opts.keepintvl);
          ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &v, sizeof(v));
        }
        if (_opts.keepcnt) {
          const int v = static_cast<int>(*_opts.keepcnt);
          ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &v, sizeof(v));
        }
      }
      asio_ns::post(connection->Lowest().get_executor(),
                    [connection] { connection->Start(); });
    }
    co_return {};
  }

  [[nodiscard]] auto AcceptInto(SocketType& peer) {
    return Async<void>([this, &peer](auto&& handler) {
      _acceptor.async_accept(peer, std::forward<decltype(handler)>(handler));
    });
  }

  IoThreadPool& _pool;
  typename Protocol::acceptor _acceptor;
  typename Session::Deps& _deps;
  AcceptorOptions _opts;
  bool _running{true};
};

}  // namespace sdb::network
