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

#include <memory>

#include "basics/asio_ns.h"
#include "general_server/io_context.h"
#include "general_server/ssl_server_feature.h"

namespace sdb::rest {

enum class SocketType {
  Tcp = 1,
  Ssl = 2,
  Unix = 3,
};

/// Wrapper class that contains sockets / ssl-stream
/// and the corrsponding peer endpoint.
/// note: only create subclasses of this type via std::make_shared()!
/// this is important because we may need to pass a shared_pointer
/// to the AsioSocket into lambdas to guarantee that the AsioSocket
/// is still valid once the lambda executes.
template<SocketType T>
struct AsioSocket {};

template<>
struct AsioSocket<SocketType::Tcp>
  : public std::enable_shared_from_this<AsioSocket<SocketType::Tcp>> {
  explicit AsioSocket(IoContext& ctx)
    : context{ctx}, timer{ctx.io_context}, socket{ctx.io_context} {
    SDB_ASSERT(&socket == &socket.lowest_layer());
    context.incClients();
  }

  ~AsioSocket() {
    timer.cancel();
    if (socket.is_open()) {
      asio_ns::error_code ec;
      socket.close(ec);
    }
    context.decClients();
  }

  void setNonBlocking(bool v) { socket.non_blocking(v); }
  static constexpr bool supportsMixedIO() { return true; }
  size_t available(asio_ns::error_code& ec) const {
    return socket.available(ec);
  }

  template<typename F>
  void shutdown(F&& cb) {
    asio_ns::error_code ec;
    if (socket.is_open()) {
      socket.cancel(ec);
      if (!ec) {
        socket.shutdown(asio_ns::ip::tcp::socket::shutdown_both, ec);
      }
      if (!ec || ec == asio_ns::error::basic_errors::not_connected) {
        socket.close(ec);
      }
    }
    std::forward<F>(cb)(ec);
  }

  IoContext& context;
  asio_ns::steady_timer timer;
  asio_ns::ip::tcp::socket socket;
  asio_ns::ip::tcp::acceptor::endpoint_type peer;
  asio_ns::streambuf buffer;
};

template<>
struct AsioSocket<SocketType::Ssl>
  : public std::enable_shared_from_this<AsioSocket<SocketType::Ssl>> {
  AsioSocket(IoContext& ctx, SslServerFeature::SslContextList ssl_contexts)
    : context{ctx},
      timer{ctx.io_context},
      stored_ssl_contexts{std::move(ssl_contexts)},
      socket{ctx.io_context, (*stored_ssl_contexts)[0]} {
    context.incClients();
  }

  ~AsioSocket() {
    timer.cancel();
    if (socket.lowest_layer().is_open()) {
      asio_ns::error_code ec;
      socket.lowest_layer().close(ec);
    }
    context.decClients();
  }

  void setNonBlocking(bool v) { socket.lowest_layer().non_blocking(v); }
  static constexpr bool supportsMixedIO() { return false; }
  size_t available(asio_ns::error_code& ec) const { return 0; }

  template<typename F>
  void handshake(F&& cb) {
    // Perform SSL handshake and verify the remote host's certificate.
    socket.lowest_layer().set_option(asio_ns::ip::tcp::no_delay{true});
    socket.async_handshake(asio_ns::ssl::stream_base::server,
                           std::forward<F>(cb));
  }

  template<typename F>
  void shutdown(F&& cb) {
    if (!socket.lowest_layer().is_open()) {
      std::forward<F>(cb)(asio_ns::error_code{});
      return;
    }

    timer.expires_after(std::chrono::seconds{3});
    timer.async_wait([self = shared_from_this()](asio_ns::error_code ec) {
      if (!ec) {
        self->socket.lowest_layer().close(ec);
      }
    });

    socket.async_shutdown([self = shared_from_this(),
                           cb = std::forward<F>(cb)](asio_ns::error_code ec) {
      self->timer.cancel();
      if (!ec || ec == asio_ns::error::basic_errors::not_connected) {
        self->socket.lowest_layer().close(ec);
      }
      cb(ec);
    });
  }

  IoContext& context;
  asio_ns::steady_timer timer;
  SslServerFeature::SslContextList stored_ssl_contexts;
  asio_ns::ssl::stream<asio_ns::ip::tcp::socket> socket;
  asio_ns::ip::tcp::acceptor::endpoint_type peer;
  asio_ns::streambuf buffer;
};

#ifdef ASIO_HAS_LOCAL_SOCKETS

template<>
struct AsioSocket<SocketType::Unix>
  : public std::enable_shared_from_this<AsioSocket<SocketType::Unix>> {
  explicit AsioSocket(IoContext& ctx)
    : context{ctx}, timer{ctx.io_context}, socket{ctx.io_context} {
    SDB_ASSERT(&socket == &socket.lowest_layer());
    context.incClients();
  }

  ~AsioSocket() {
    timer.cancel();
    if (socket.is_open()) {
      asio_ns::error_code ec;
      socket.close(ec);
    }
    context.decClients();
  }

  void setNonBlocking(bool v) { socket.non_blocking(v); }
  static constexpr bool supportsMixedIO() { return true; }
  size_t available(asio_ns::error_code& ec) const {
    return socket.available(ec);
  }

  template<typename F>
  void shutdown(F&& cb) {
    asio_ns::error_code ec;
    if (socket.is_open()) {
      socket.cancel(ec);
      if (!ec) {
        socket.shutdown(asio_ns::ip::tcp::socket::shutdown_both, ec);
      }
      if (!ec || ec == asio_ns::error::basic_errors::not_connected) {
        socket.close(ec);
      }
    }
    std::forward<F>(cb)(ec);
  }

  IoContext& context;
  asio_ns::steady_timer timer;
  asio_ns::local::stream_protocol::socket socket;
  asio_ns::local::stream_protocol::acceptor::endpoint_type peer;
  asio_ns::streambuf buffer;
};

#endif

}  // namespace sdb::rest
