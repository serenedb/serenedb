////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace sdb::fuerte {

class Request;
class Response;

using MessageID = uint64_t;  // id that identifies a Request.
using StatusCode = uint32_t;

inline constexpr StatusCode kStatusUndefined = 0;
inline constexpr StatusCode kStatusOk = 200;
inline constexpr StatusCode kStatusCreated = 201;
inline constexpr StatusCode kStatusAccepted = 202;
inline constexpr StatusCode kStatusPartial = 203;
inline constexpr StatusCode kStatusNoContent = 204;
inline constexpr StatusCode kStatusTemporaryRedirect = 307;
inline constexpr StatusCode kStatusBadRequest = 400;
inline constexpr StatusCode kStatusUnauthorized = 401;
inline constexpr StatusCode kStatusForbidden = 403;
inline constexpr StatusCode kStatusNotFound = 404;
inline constexpr StatusCode kStatusMethodNotAllowed = 405;
inline constexpr StatusCode kStatusNotAcceptable = 406;
inline constexpr StatusCode kStatusConflict = 409;
inline constexpr StatusCode kStatusPreconditionFailed = 412;
inline constexpr StatusCode kStatusMisdirectedRequest = 421;
inline constexpr StatusCode kStatusInternalError = 500;
inline constexpr StatusCode kStatusServiceUnavailable = 503;
inline constexpr StatusCode kStatusVersionNotSupported = 505;

std::string StatusCodeToString(StatusCode);

bool StatusIsSuccess(StatusCode);

enum class Error : uint16_t {
  NoError = 0,

  CouldNotConnect = 1000,
  CloseRequested = 1001,
  ConnectionClosed = 1002,
  RequestTimeout = 1003,
  QueueCapacityExceeded = 1004,

  ReadError = 1102,
  WriteError = 1103,

  ConnectionCanceled = 1104,

  ProtocolError = 3000,
};
std::string ToString(Error error);

// RequestCallback is called for finished connection requests.
// If the given Error is zero, the request succeeded, otherwise an error
// occurred.
using RequestCallback = std::function<void(Error, std::unique_ptr<Request>,
                                           std::unique_ptr<Response>)>;
// ConnectionFailureCallback is called when a connection encounters a failure
// that is not related to a specific request.
// Examples are:
// - Host cannot be resolved
// - Cannot connect
// - Connection lost
using ConnectionFailureCallback =
  std::function<void(Error error_code, const std::string& error_message)>;

using StringMap = std::map<std::string, std::string>;

enum class RestVerb {
  Illegal = -1,
  Delete = 0,
  Get = 1,
  Post = 2,
  Put = 3,
  Head = 4,
  Patch = 5,
  Options = 6
};
std::string ToString(RestVerb type);
RestVerb FromString(std::string_view type);

enum class MessageType : int {
  Undefined = 0,
  Request = 1,
  Response = 2,
  Authentication = 1000
};
MessageType IntToMessageType(int integral);

std::string ToString(MessageType type);

enum class SocketType : uint8_t {
  Undefined = 0,
  Tcp = 1,
  Ssl = 2,
  Unix = 3,
};
std::string ToString(SocketType type);

enum class ProtocolType : uint8_t {
  Undefined = 0,
  Http = 1,
  Http2 = 2,
};
std::string ToString(ProtocolType type);

enum class ContentType : uint8_t {
  Unset = 0,
  Custom,
  VPack,
  Dump,
  Json,
  Html,
  Text,
  BatchPart,
  FormData,
};
ContentType ToContentType(std::string_view val);
std::string ToString(ContentType type);

enum class ContentEncoding : uint8_t {
  Identity = 0,
  Deflate = 1,
  Gzip = 2,
  Lz4 = 3,
  Custom = 4,
};
ContentEncoding ToContentEncoding(std::string_view val);
std::string ToString(ContentEncoding type);

enum class AuthenticationType { None, Basic, Jwt };
std::string ToString(AuthenticationType type);

namespace detail {

struct ConnectionConfiguration {
  ConnectionFailureCallback on_failure;
  SocketType socket_type = SocketType::Tcp;
  ProtocolType protocol_type = ProtocolType::Http;
  bool upgrade_h1_to_h2 = false;

  std::string host = "localhost";
  std::string port = "8529";
  bool verify_host = false;

  std::chrono::milliseconds connect_timeout{60'000};
  std::chrono::milliseconds idle_timeout{300'000};
  std::chrono::milliseconds connect_retry_pause{1'000};
  unsigned max_connect_retries = 3;
#ifdef SDB_GTEST
  unsigned fail_connect_attempts = 0;
#endif
  bool use_idle_timeout = true;

  AuthenticationType authentication_type = AuthenticationType::None;
  std::string user;
  std::string password;
  std::string jwt_token;
};

}  // namespace detail
}  // namespace sdb::fuerte
