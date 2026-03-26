////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016-2018 ArangoDB GmbH, Cologne, Germany
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
/// @author Jan Christoph Uhde
/// @author Ewout Prangsma
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <fuerte/connection.h>

#include <string_view>

#include "h1_connection.h"
#include "h2_connection.h"

namespace sdb::fuerte {

// Create an connection and start opening it.
std::shared_ptr<Connection> ConnectionBuilder::connect(EventLoopService& loop) {
  std::shared_ptr<Connection> result;

  if (_conf.protocol_type == ProtocolType::Http) {
    SDB_DEBUG("xxxxx", Logger::FUERTE, "fuerte - creating http 1.1 connection");
    if (_conf.socket_type == SocketType::Tcp) {
      result =
        std::make_shared<http::H1Connection<SocketType::Tcp>>(loop, _conf);
    } else if (_conf.socket_type == SocketType::Ssl) {
      result =
        std::make_shared<http::H1Connection<SocketType::Ssl>>(loop, _conf);
    }
#ifdef ASIO_HAS_LOCAL_SOCKETS
    else if (_conf.socket_type == SocketType::Unix) {
      result =
        std::make_shared<http::H1Connection<SocketType::Unix>>(loop, _conf);
    }
#endif
  } else if (_conf.protocol_type == ProtocolType::Http2) {
    SDB_DEBUG("xxxxx", Logger::FUERTE, "fuerte - creating http 2 connection");
    if (_conf.socket_type == SocketType::Tcp) {
      result =
        std::make_shared<http::H2Connection<SocketType::Tcp>>(loop, _conf);
    } else if (_conf.socket_type == SocketType::Ssl) {
      result =
        std::make_shared<http::H2Connection<SocketType::Ssl>>(loop, _conf);
    }
#ifdef ASIO_HAS_LOCAL_SOCKETS
    else if (_conf.socket_type == SocketType::Unix) {
      result =
        std::make_shared<http::H2Connection<SocketType::Unix>>(loop, _conf);
    }
#endif
  }
  if (!result) {
    throw std::logic_error("unsupported socket or protocol type");
  }

  return result;
}

void ParseSchema(std::string_view schema,
                 detail::ConnectionConfiguration& conf) {
  // non exthausive list of supported url schemas
  // "http+tcp://", "http+ssl://", "tcp://", "ssl://", "unix://", "http+unix://"
  // "http://", "https://"
  auto proto = schema;
  auto pos = schema.find('+');
  if (pos != std::string_view::npos && pos + 1 < proto.length()) {
    // got something like "http+tcp://"
    std::string_view socket = proto.substr(pos + 1);
    proto = proto.substr(0, pos);
    if (socket == "tcp" || socket == "srv") {
      conf.socket_type = SocketType::Tcp;
    } else if (socket == "ssl") {
      conf.socket_type = SocketType::Ssl;
    } else if (socket == "unix") {
      conf.socket_type = SocketType::Unix;
    } else if (conf.socket_type == SocketType::Undefined) {
      throw std::runtime_error{absl::StrCat("invalid socket type: ", proto)};
    }

    if (proto == "http") {
      conf.protocol_type = ProtocolType::Http;
    } else if (proto == "h2" || proto == "http2") {
      conf.protocol_type = ProtocolType::Http2;
    }

  } else {  // got only protocol
    if (proto == "http" || proto == "tcp") {
      conf.socket_type = SocketType::Tcp;
      conf.protocol_type = ProtocolType::Http;
    } else if (proto == "https" || proto == "ssl") {
      conf.socket_type = SocketType::Ssl;
      conf.protocol_type = ProtocolType::Http;
    } else if (proto == "unix") {
      conf.socket_type = SocketType::Unix;
      conf.protocol_type = ProtocolType::Http;
    } else if (proto == "h2" || proto == "http2") {
      conf.socket_type = SocketType::Tcp;
      conf.protocol_type = ProtocolType::Http2;
    } else if (proto == "h2s" || proto == "https2") {
      conf.socket_type = SocketType::Ssl;
      conf.protocol_type = ProtocolType::Http2;
    }
  }

  if (conf.socket_type == SocketType::Undefined ||
      conf.protocol_type == ProtocolType::Undefined) {
    throw std::runtime_error{absl::StrCat("invalid schema: ", proto)};
  }
}

namespace {

bool IsInvalid(char c) { return c == ' ' || c == '\r' || c == '\n'; }
char Lower(char c) { return (unsigned char)(c | 0x20); }
bool IsAlpha(char c) { return (Lower(c) >= 'a' && Lower(c) <= 'z'); }
bool IsNum(char c) { return ((c) >= '0' && (c) <= '9'); }
bool IsAlphanum(char c) { return IsNum(c) || IsAlpha(c); }
bool IsHostChar(char c) { return (IsAlphanum(c) || (c) == '.' || (c) == '-'); }

}  // namespace

ConnectionBuilder& ConnectionBuilder::endpoint(std::string_view spec) {
  if (spec.empty()) {
    throw std::runtime_error("invalid empty endpoint spec");
  }

  // we need to handle unix:// urls seperately
  auto pos = spec.find("://");
  if (pos != std::string_view::npos) {
    auto schema = absl::AsciiStrToLower(spec.substr(0, pos));
    ParseSchema(schema, _conf);
  }

  if (_conf.socket_type == SocketType::Unix) {
    if (std::string::npos != pos) {
      // unix:///a/b/c does not contain a port
      _conf.host = spec.substr(pos + 3);
    }
    return *this;
  }

  pos = (pos != std::string::npos) ? pos + 3 : 0;
  auto x = pos + 1;
  if (spec[pos] == '[') {  // ipv6 addresses contain colons
    pos++;                 // do not include '[' in actual address
    while (x < spec.size() && spec[x] != ']') {
      if (IsInvalid(spec[x])) {  // we could validate this better
        throw std::runtime_error{absl::StrCat("invalid ipv6 address: ", spec)};
      }
      x++;
    }
    if (spec[x] != ']') {
      throw std::runtime_error{absl::StrCat("invalid ipv6 address: ", spec)};
    }
    _conf.host = spec.substr(pos, ++x - pos - 1);  // do not include ']'
  } else {
    while (x < spec.size() && spec[x] != '/' && spec[x] != ':') {
      if (!IsHostChar(spec[x])) {
        throw std::runtime_error{absl::StrCat("invalid host in spec: ", spec)};
      }
      x++;
    }
    _conf.host = spec.substr(pos, x - pos);
  }
  if (_conf.host.empty()) {
    throw std::runtime_error{absl::StrCat("invalid host: ", spec)};
  }

  if (spec[x] == ':') {
    pos = ++x;
    while (x < spec.size() && spec[x] != '/' && spec[x] != '?') {
      if (!IsNum(spec[x])) {
        throw std::runtime_error{absl::StrCat("invalid port in spec: ", spec)};
      }
      x++;
    }
    _conf.port = spec.substr(pos, x - pos);
    if (_conf.port.empty()) {
      throw std::runtime_error{absl::StrCat("invalid port in spec: ", spec)};
    }
  }

  return *this;
}

/// @brief get the normalized endpoint
std::string ConnectionBuilder::normalizedEndpoint() const {
  std::string endpoint;
  if (ProtocolType::Http == _conf.protocol_type) {
    endpoint.append("http+");
  }

  if (SocketType::Tcp == _conf.socket_type) {
    endpoint.append("tcp://");
  } else if (SocketType::Ssl == _conf.socket_type) {
    endpoint.append("ssl://");
  } else if (SocketType::Unix == _conf.socket_type) {
    endpoint.append("unix://");
  }

  endpoint.append(_conf.host);
  endpoint.push_back(':');
  endpoint.append(_conf.port);

  return endpoint;
}

}  // namespace sdb::fuerte
