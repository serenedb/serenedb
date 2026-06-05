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

#include <openssl/ssl.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

#include "basics/asio_ns.h"
#include "basics/message_sequence_view.h"
#include "network/asio_awaitable.h"

namespace sdb::network {

// MaybeTls is a single connection that starts plaintext and may upgrade to TLS
// in-band (pg-wire SSLRequest / STARTTLS style). It carries the same asio type
// as Ssl but flips a runtime flag at the handshake. Ssl is TLS-from-first-byte
// (HTTPS). Tcp/Unix are plaintext.
enum class SocketKind : uint8_t { Tcp, Ssl, Unix, MaybeTls };

template<SocketKind Kind>
struct StreamTraits;

template<>
struct StreamTraits<SocketKind::Tcp> {
  using Stream = asio_ns::ip::tcp::socket;
};

template<>
struct StreamTraits<SocketKind::Ssl> {
  using Stream = asio_ns::ssl::stream<asio_ns::ip::tcp::socket>;
};

template<>
struct StreamTraits<SocketKind::MaybeTls> {
  using Stream = asio_ns::ssl::stream<asio_ns::ip::tcp::socket>;
};

#ifdef ASIO_HAS_LOCAL_SOCKETS
template<>
struct StreamTraits<SocketKind::Unix> {
  using Stream = asio_ns::local::stream_protocol::socket;
};
#endif

template<SocketKind Kind>
class Socket final {
 public:
  using Stream = typename StreamTraits<Kind>::Stream;

  // True for the ssl::stream-backed kinds (Ssl always-TLS, MaybeTls
  // upgradeable).
  static constexpr bool kSslBacked =
    Kind == SocketKind::Ssl || Kind == SocketKind::MaybeTls;

  explicit Socket(asio_ns::io_context& io)
    requires(!kSslBacked)
    : _stream{io} {}

  Socket(asio_ns::io_context& io, asio_ns::ssl::context& ssl)
    requires(kSslBacked)
    : _stream{io, ssl} {}

  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;

  auto& Lowest() noexcept {
    if constexpr (kSslBacked) {
      // next_layer() is the concrete tcp::socket (basic_stream_socket) -- the
      // same object as lowest_layer() but the type async_accept/set_option
      // want.
      return _stream.next_layer();
    } else {
      return _stream;
    }
  }

  // For Ssl: always TLS. For MaybeTls: true only after a successful upgrade.
  [[nodiscard]] bool IsTls() const noexcept {
    if constexpr (Kind == SocketKind::Ssl) {
      return true;
    } else if constexpr (Kind == SocketKind::MaybeTls) {
      return _tls;
    } else {
      return false;
    }
  }

  [[nodiscard]] auto ReadSome(std::span<uint8_t> into) {
    return Async<std::size_t>([this, into](auto&& handler) {
      const auto buffer = asio_ns::buffer(into.data(), into.size());
      if constexpr (Kind == SocketKind::MaybeTls) {
        if (!_tls) {
          _stream.next_layer().async_read_some(
            buffer, std::forward<decltype(handler)>(handler));
          return;
        }
      }
      _stream.async_read_some(buffer, std::forward<decltype(handler)>(handler));
    });
  }

  [[nodiscard]] auto Write(message::SequenceView data) {
    return Async<std::size_t>([this, data](auto&& handler) {
      if constexpr (Kind == SocketKind::MaybeTls) {
        if (!_tls) {
          asio_ns::async_write(_stream.next_layer(), data,
                               std::forward<decltype(handler)>(handler));
          return;
        }
      }
      asio_ns::async_write(_stream, data,
                           std::forward<decltype(handler)>(handler));
    });
  }

  [[nodiscard]] auto Handshake()
    requires(kSslBacked)
  {
    Lowest().set_option(asio_ns::ip::tcp::no_delay{true});
    return Async<void>([this](auto&& handler) {
      _stream.async_handshake(asio_ns::ssl::stream_base::server,
                              std::forward<decltype(handler)>(handler));
    });
  }

  // Call after a successful in-band Handshake() so subsequent IO routes through
  // the TLS layer. Set only on success (a failed handshake must stay plaintext
  // and close).
  void MarkTls() noexcept
    requires(Kind == SocketKind::MaybeTls)
  {
    _tls = true;
  }

  // The ALPN protocol negotiated during the TLS handshake (empty if none) --
  // lets the HTTP session pick H1 vs a future H2 codec.
  [[nodiscard]] std::string_view AlpnProtocol() const
    requires(kSslBacked)
  {
    const unsigned char* data = nullptr;
    unsigned int len = 0;
    SSL_get0_alpn_selected(_stream.native_handle(), &data, &len);
    return {reinterpret_cast<const char*>(data), len};
  }

  void Close() noexcept {
    auto& lowest = Lowest();
    if (lowest.is_open()) {
      asio_ns::error_code ec;
      lowest.cancel(ec);
      lowest.close(ec);
    }
  }

 private:
  Stream _stream;
  bool _tls = false;
};

}  // namespace sdb::network
