////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018-2020 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <fuerte/asio_ns.h>
#include <fuerte/loop.h>
#include <fuerte/types.h>

#include "basics/assert.h"
#include "basics/logger/logger.h"

namespace sdb::fuerte {
namespace {

template<typename SocketT, typename F, typename IsAbortedCb>
void ResolveConnect(const detail::ConnectionConfiguration& config,
                    asio_ns::ip::tcp::resolver& resolver, SocketT& socket,
                    F&& done, IsAbortedCb&& is_aborted) {
  auto cb = [&socket,
#ifdef SDB_GTEST
             fail = config.fail_connect_attempts > 0,
#endif
             done = std::forward<F>(done),
             is_aborted = std::forward<IsAbortedCb>(is_aborted)](
              auto ec, auto it) mutable {
#ifdef SDB_GTEST
    if (fail) {
      // use an error code != operation_aborted
      ec = boost::system::errc::make_error_code(
        boost::system::errc::not_enough_memory);
    }
#endif

    if (is_aborted()) {
      ec = asio_ns::error::operation_aborted;
    }

    if (ec) {  // error in address resolver
      SDB_DEBUG("xxxxx", Logger::FUERTE,
                "received error during address resolving: ", ec.message());
      done(ec);
      return;
    }

    try {
      // A successful resolve operation is guaranteed to pass a
      // non-empty range to the handler.
      asio_ns::async_connect(socket, it, [done](auto ec, auto it) mutable {
        if (ec) {
          SDB_DEBUG("xxxxx", Logger::FUERTE,
                    "executing async connect callback, error: ", ec.message());
        } else {
          SDB_DEBUG("xxxxx", Logger::FUERTE,
                    "executing async connect callback, no error");
        }
        std::forward<F>(done)(ec);
      });
    } catch (const std::bad_alloc&) {
      // definitely an OOM error
      done(boost::system::errc::make_error_code(
        boost::system::errc::not_enough_memory));
    } catch (...) {
      // probably not an OOM error, but we don't know what it actually is.
      // there is no code for a generic error that we could use here
      done(boost::system::errc::make_error_code(
        boost::system::errc::not_enough_memory));
    }
  };

  // windows does not like async_resolve
#ifdef _WIN32
  asio_ns::error_code ec;
  auto it = resolver.resolve(config._host, config._port, ec);
  cb(ec, it);
#else
  // Resolve the host asynchronously into a series of endpoints
  SDB_DEBUG("xxxxx", Logger::FUERTE, "scheduled callback to resolve host ",
            config.host, ":", config.port);
  resolver.async_resolve(config.host, config.port, std::move(cb));
#endif
}

}  // namespace

enum class ConnectTimerRole {
  Connect = 1,
  Reconnect = 2,
};

template<SocketType T>
struct Socket {};

template<>
struct Socket<SocketType::Tcp> {
  Socket(EventLoopService&, asio_ns::io_context& ctx)
    : resolver(ctx), socket(ctx), timer(ctx) {}

  ~Socket() {
    try {
      this->cancel();
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during tcp socket shutdown: ", ex.what());
    }
  }

  template<typename F>
  void connect(const detail::ConnectionConfiguration& config, F&& done) {
    ResolveConnect(
      config, resolver, socket,
      [this, done = std::forward<F>(done)](asio_ns::error_code ec) mutable {
        SDB_DEBUG("xxxxx", Logger::FUERTE,
                  "executing tcp connect callback, ec: ", ec.message(),
                  ", canceled: ", this->canceled);
        if (canceled) {
          // cancel() was already called on this socket
          SDB_ASSERT(!socket.is_open());
          ec = asio_ns::error::operation_aborted;
        }
        if (!ec) {
          // set TCP_NODELAY option on socket to disable Nagle's algorithm.
          socket.set_option(asio_ns::ip::tcp::no_delay(true), ec);
          if (ec) {
            SDB_ERROR(
              "xxxxx", Logger::FUERTE,
              "error setting no_delay option on socket: ", ec.message());
            SDB_ASSERT(false);
          }
        }
        done(ec);
      },
      [this]() { return canceled; });
  }

  bool isOpen() const { return socket.is_open(); }

  void rearm() { canceled = false; }

  void cancel() {
    canceled = true;
    try {
      timer.cancel();
      resolver.cancel();
      if (socket.is_open()) {  // non-graceful shutdown
        asio_ns::error_code ec;
        socket.close(ec);
      }
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during tcp socket cancelation: ", ex.what());
    }
  }

  template<typename F>
  void shutdown(F&& cb) {
    // ec is an out parameter here that is passed to the methods so they
    // can fill in whatever error happened. we ignore it here anyway. we
    // use the ec-variants of the methods here to prevent exceptions.
    asio_ns::error_code ec;
    try {
      timer.cancel();
      if (socket.is_open()) {
        socket.cancel(ec);
        socket.shutdown(asio_ns::ip::tcp::socket::shutdown_both, ec);
        socket.close(ec);
      }
    } catch (const std::exception& ex) {
      // an exception is unlikely to occur here, as we are using the error-code
      // variants of cancel/shutdown/close above
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during tcp socket shutdown: ", ex.what());
    }
    std::forward<F>(cb)(ec);
  }

  asio_ns::ip::tcp::resolver resolver;
  asio_ns::ip::tcp::socket socket;
  asio_ns::steady_timer timer;
  ConnectTimerRole connect_timer_role = ConnectTimerRole::Connect;
  bool canceled = false;
};

template<>
struct Socket<fuerte::SocketType::Ssl> {
  Socket(EventLoopService& loop, asio_ns::io_context& ctx)
    : resolver(ctx),
      socket(ctx, loop.sslContext()),
      timer(ctx),
      ctx(ctx),
      ssl_context(loop.sslContext()),
      cleanup_done(false) {}

  ~Socket() {
    try {
      this->cancel();
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during ssl socket shutdown: ", ex.what());
    }
  }

  template<typename F>
  void connect(const detail::ConnectionConfiguration& config, F&& done) {
    bool verify = config.verify_host;
    ResolveConnect(
      config, resolver, socket.next_layer(),
      [=, this](asio_ns::error_code ec) mutable {
        SDB_DEBUG("xxxxx", Logger::FUERTE,
                  "executing ssl connect callback, ec: ", ec.message(),
                  ", canceled: ", this->canceled);
        if (canceled) {
          // cancel() was already called on this socket
          SDB_ASSERT(!socket.lowest_layer().is_open());
          ec = asio_ns::error::operation_aborted;
        }
        if (ec) {
          done(ec);
          return;
        }

        // Perform SSL handshake and verify the remote host's certificate.
        try {
          // set TCP_NODELAY option on socket to disable Nagle's algorithm.
          socket.next_layer().set_option(asio_ns::ip::tcp::no_delay(true));

          // Set SNI Hostname (many hosts need this to handshake successfully)
          if (!SSL_set_tlsext_host_name(socket.native_handle(),
                                        config.host.c_str())) {
            boost::system::error_code ec{
              static_cast<int>(::ERR_get_error()),
              boost::asio::error::get_ssl_category()};
            done(ec);
            return;
          }

          if (verify) {
            socket.set_verify_mode(asio_ns::ssl::verify_peer);
            socket.set_verify_callback(
              asio_ns::ssl::host_name_verification{config.host});
          } else {
            socket.set_verify_mode(asio_ns::ssl::verify_none);
          }
        } catch (const std::bad_alloc&) {
          // definitely an OOM error
          done(boost::system::errc::make_error_code(
            boost::system::errc::not_enough_memory));
          return;
        } catch (const boost::system::system_error& exc) {
          // probably not an OOM error, but we don't know what it actually is.
          // there is no code for a generic error that we could use here
          done(exc.code());
          return;
        }
        socket.async_handshake(asio_ns::ssl::stream_base::client,
                               std::move(done));
      },
      [this]() { return canceled; });
  }

  bool isOpen() const { return socket.lowest_layer().is_open(); }

  void rearm() {
    // create a new socket and declare it ready
    socket = asio_ns::ssl::stream<asio_ns::ip::tcp::socket>(this->ctx,
                                                            this->ssl_context);
    canceled = false;
  }

  void cancel() {
    canceled = true;
    try {
      timer.cancel();
      resolver.cancel();
      if (socket.lowest_layer().is_open()) {  // non-graceful shutdown
        asio_ns::error_code ec;
        socket.lowest_layer().shutdown(asio_ns::ip::tcp::socket::shutdown_both,
                                       ec);
        socket.lowest_layer().close(ec);
      }
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during ssl socket cancelation: ", ex.what());
    }
  }

  template<typename F>
  void shutdown(F&& cb) {
    // The callback cb plays two roles here:
    //   1. As a callback to be called back.
    //   2. It captures by value a shared_ptr to the connection object of which
    //   this
    //      socket is a member. This means that the allocation of the connection
    //      and this of the socket is kept until all asynchronous operations are
    //      completed (or aborted).

    // ec is an out parameter here that is passed to the methods so they
    // can fill in whatever error happened. we ignore it here anyway. we
    // use the ec-variants of the methods here to prevent exceptions.
    asio_ns::error_code ec;

    if (!socket.lowest_layer().is_open()) {
      timer.cancel();
      std::forward<F>(cb)(ec);
      return;
    }

    socket.lowest_layer().cancel(ec);
    cleanup_done = false;
    // implicitly cancels any previous timers
    timer.expires_after(std::chrono::seconds(3));

    socket.async_shutdown([cb, this](asio_ns::error_code ec) {
      timer.cancel();
      if (!cleanup_done) {
        socket.lowest_layer().shutdown(asio_ns::ip::tcp::socket::shutdown_both,
                                       ec);
        socket.lowest_layer().close(ec);
        cleanup_done = true;
      }
      cb(ec);
    });
    timer.async_wait([cb(std::forward<F>(cb)), this](asio_ns::error_code ec) {
      // Copy in callback such that the connection object is kept alive long
      // enough, please do not delete, although it is not used here!
      if (!ec && !cleanup_done) {
        socket.lowest_layer().shutdown(asio_ns::ip::tcp::socket::shutdown_both,
                                       ec);
        socket.lowest_layer().close(ec);
        cleanup_done = true;
      }
    });
  }

  asio_ns::ip::tcp::resolver resolver;
  asio_ns::ssl::stream<asio_ns::ip::tcp::socket> socket;
  asio_ns::steady_timer timer;
  asio_ns::io_context& ctx;
  asio_ns::ssl::context& ssl_context;
  std::atomic<bool> cleanup_done;
  ConnectTimerRole connect_timer_role = ConnectTimerRole::Connect;
  bool canceled = false;
};

#ifdef ASIO_HAS_LOCAL_SOCKETS

template<>
struct Socket<fuerte::SocketType::Unix> {
  Socket(EventLoopService&, asio_ns::io_context& ctx)
    : socket(ctx), timer(ctx) {}

  ~Socket() {
    canceled = true;
    try {
      this->cancel();
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during unix socket shutdown: ", ex.what());
    }
  }

  template<typename F>
  void connect(const detail::ConnectionConfiguration& config, F&& done) {
    if (canceled) {
      // cancel() was already called on this socket
      done(asio_ns::error::operation_aborted);
      return;
    }

    asio_ns::local::stream_protocol::endpoint ep(config.host);
    socket.async_connect(ep, std::forward<F>(done));
  }

  bool isOpen() const { return socket.is_open(); }

  void rearm() { canceled = false; }

  void cancel() {
    canceled = true;
    try {
      timer.cancel();
      if (socket.is_open()) {  // non-graceful shutdown
        asio_ns::error_code ec;
        socket.close(ec);
      }
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during unix socket cancelation: ", ex.what());
    }
  }

  template<typename F>
  void shutdown(F&& cb) {
    // ec is an out parameter here that is passed to the methods so they
    // can fill in whatever error happened. we ignore it here anyway. we
    // use the ec-variants of the methods here to prevent exceptions.
    asio_ns::error_code ec;
    try {
      timer.cancel();
      if (socket.is_open()) {
        socket.cancel(ec);
        socket.shutdown(asio_ns::ip::tcp::socket::shutdown_both, ec);
        socket.close(ec);
      }
    } catch (const std::exception& ex) {
      // an exception is unlikely to occur here, as we are using the error-code
      // variants of cancel/shutdown/close above
      SDB_ERROR("xxxxx", Logger::FUERTE,
                "caught exception during unix socket shutdown: ", ex.what());
    }
    std::forward<F>(cb)(ec);
  }

  asio_ns::local::stream_protocol::socket socket;
  asio_ns::steady_timer timer;
  ConnectTimerRole connect_timer_role = ConnectTimerRole::Connect;
  bool canceled = false;
};

#endif

}  // namespace sdb::fuerte
