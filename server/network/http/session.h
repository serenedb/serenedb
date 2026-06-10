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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/asio_ns.h"
#include "basics/message_buffer.h"
#include "network/connection.h"
#include "network/http/h1_codec.h"
#include "network/http/router.h"
#include "network/io_executor.h"
#include "network/socket.h"

namespace sdb::network {

struct HttpServerContext {
  HttpRouter& router;
  asio_ns::ssl::context* ssl = nullptr;
};

template<SocketKind Kind>
class HttpSession final
  : public std::enable_shared_from_this<HttpSession<Kind>> {
 public:
  using Deps = HttpServerContext;

  HttpSession(HttpServerContext& ctx, IoExecutor& exec)
    requires(Kind != SocketKind::Ssl)
    : _socket{exec.Context()}, _deadline{exec.Context()}, _router{ctx.router} {}

  HttpSession(HttpServerContext& ctx, IoExecutor& exec)
    requires(Kind == SocketKind::Ssl)
    : _socket{exec.Context(), *ctx.ssl},
      _deadline{exec.Context()},
      _router{ctx.router} {}

  void Start() { Run().Detach(); }

  void Close() noexcept { _socket.Close(); }

  asio_ns::ip::tcp::socket& Lowest() noexcept { return _socket.Lowest(); }

 private:
  yaclib::Future<> Run();
  yaclib::Future<> Flush();
  yaclib::Future<> SendError(int status);

  // Bound the in-flight socket read with a per-connection deadline. The timer
  // and socket share this connection's single-threaded io_context, so the
  // expiry handler, socket cancel, and read completion are serialized on one
  // thread (no strand). On expiry, closing aborts the read -> the awaiting
  // coroutine resumes with an error -> the session tears down. The handler
  // touches only the socket; it never resumes the coroutine.
  void Arm(std::chrono::steady_clock::duration timeout) {
    _deadline.expires_after(timeout);
    _deadline.async_wait(
      [self = this->shared_from_this()](const asio_ns::error_code& ec) {
        if (ec) {
          return;  // cancelled or superseded by the next Arm/Disarm
        }
        self->_socket.Close();
      });
  }
  void Disarm() noexcept { _deadline.cancel(); }

  Socket<Kind> _socket;
  asio_ns::steady_timer _deadline;
  HttpRouter& _router;
  H1Codec _codec;
  message::Buffer _recv{kReadBlock, kBufferMaxGrowth};
  message::Buffer _send{kReadBlock, kBufferMaxGrowth};
  message::Buffer _dechunk{kReadBlock, kBufferMaxGrowth};
  ChunkSink _sink;
};

template<SocketKind Kind>
yaclib::Future<> HttpSession<Kind>::Flush() {
  if (!_send.Written().Empty()) {
    co_await _socket.Write(_send.Written());
    _send.Clear();
  }
  co_return {};
}

template<SocketKind Kind>
yaclib::Future<> HttpSession<Kind>::SendError(int status) {
  const HttpResponse error = HttpResponse::Error(status);
  _codec.EncodeHead(error, false, _send);
  _send.WriteUncommitted(error.body);
  co_await Flush();
  co_return {};
}

template<SocketKind Kind>
yaclib::Future<> HttpSession<Kind>::Run() {
  auto self = this->shared_from_this();
  if constexpr (Kind == SocketKind::Ssl) {
    try {
      Arm(kHttpHeaderReadTimeout);
      co_await _socket.Handshake();
      Disarm();
    } catch (const std::exception&) {
      _socket.Close();
      co_return {};
    }
  }
  try {
    for (;;) {
      H1Event event = H1Event::NeedMore;
      while (event != H1Event::Head && event != H1Event::Continue) {
        if (_recv.Readable()) {
          const auto fed = _codec.ParseHead(_recv.Front());
          _recv.Consume(fed.consumed);
          if (fed.event == H1Event::Error) {
            co_await SendError(_codec.ErrorStatus());
            _socket.Close();
            co_return {};
          }
          event = fed.event;
          if (event == H1Event::Head || event == H1Event::Continue) {
            break;
          }
          if (fed.consumed != 0) {
            continue;
          }
        }
        Arm(_recv.ReadableSize() == 0 ? kHttpKeepAliveIdleTimeout
                                      : kHttpHeaderReadTimeout);
        const size_t n = co_await _socket.ReadSome(_recv.Reserve(kReadBlock));
        Disarm();
        if (n == 0) {
          _socket.Close();
          co_return {};
        }
        _recv.CommitWrite(n);
      }

      if (event == H1Event::Continue) {
        _send.WriteUncommitted("HTTP/1.1 100 Continue\r\n\r\n");
        co_await Flush();
      }

      HttpRequest request = _codec.TakeHead();
      size_t pinned_body = 0;

      if (_codec.IsChunked()) {
        _dechunk.Clear();
        bool done = false;
        while (!done) {
          if (_recv.Readable()) {
            const auto body = _codec.DecodeBody(_recv.Front(), _dechunk);
            _recv.Consume(body.consumed);
            if (body.error) {
              co_await SendError(_codec.ErrorStatus());
              _socket.Close();
              co_return {};
            }
            if (body.done) {
              break;
            }
            if (body.consumed != 0) {
              continue;
            }
          }
          Arm(kHttpBodyReadTimeout);
          const size_t n = co_await _socket.ReadSome(_recv.Reserve(kReadBlock));
          Disarm();
          if (n == 0) {
            _socket.Close();
            co_return {};
          }
          _recv.CommitWrite(n);
        }
        request.body = _dechunk.Written();
      } else {
        const auto length = static_cast<size_t>(_codec.ContentLength());
        while (_recv.ReadableSize() < length) {
          Arm(kHttpBodyReadTimeout);
          const size_t n = co_await _socket.ReadSome(_recv.Reserve(kReadBlock));
          Disarm();
          if (n == 0) {
            _socket.Close();
            co_return {};
          }
          _recv.CommitWrite(n);
        }
        request.body = _recv.ReadableView(length);
        pinned_body = length;
      }

      const bool keep_alive = request.keep_alive;
      const bool is_head = request.method == HttpMethod::Head;
      HttpResponse response;
      try {
        response = co_await _router.Route(request);
      } catch (const std::exception&) {
        response = HttpResponse::Error(500);
      }
      _codec.EncodeHead(response, keep_alive, _send);
      if (response.IsStreaming()) {
        co_await Flush();  // send the head before any chunks
        if (!is_head) {
          auto& producer = *response.producer;
          for (;;) {
            _sink.Clear();
            bool more;
            try {
              more = co_await producer(_sink);
            } catch (const std::exception&) {
              // Head is already on the wire, so a clean HTTP error is no longer
              // possible: abandon the unterminated body and drop the
              // connection.
              _socket.Close();
              co_return {};
            }
            WriteChunk(_send, _sink.View());
            co_await Flush();
            if (!more) {
              WriteLastChunk(_send);
              co_await Flush();
              break;
            }
          }
        }
      } else {
        if (!is_head) {
          _send.WriteUncommitted(response.body);
        }
        co_await Flush();
      }

      if (pinned_body != 0) {
        _recv.Consume(pinned_body);
      }
      if (!keep_alive) {
        break;
      }
      _codec.Reset();
    }
  } catch (const std::exception&) {
  }
  _socket.Close();
  co_return {};
}

}  // namespace sdb::network
