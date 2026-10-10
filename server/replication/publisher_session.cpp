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

#include "replication/publisher_session.h"

#include <absl/base/internal/endian.h>
#include <absl/random/random.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iresearch/utils/pg/errcodes.hpp>
#include <memory>
#include <utility>
#include <yaclib/async/contract.hpp>

#include "network/asio_awaitable.h"
#include "network/credentials.h"
#include "network/io_context.h"
#include "network/pg/scram_client.h"
#include "network/pg/wire_frames.h"
#include "network/server.h"
#include "network/tls_context.h"
#include "pg/protocol.h"

namespace sdb::replication {
namespace {

using network::pg::FrameKind;
using network::pg::FrameStatus;

constexpr int32_t kAuthOk = 0;
constexpr int32_t kAuthCleartext = 3;
constexpr int32_t kAuthMd5 = 5;
constexpr int32_t kAuthSasl = 10;
constexpr int32_t kAuthSaslContinue = 11;
constexpr int32_t kAuthSaslFinal = 12;

network::TlsClientOptions ClientTlsOptions(const ConnInfo& conninfo) {
  network::TlsClientOptions options{
    .verify_peer =
      conninfo.sslmode == SslMode::VerifyCa ||
      conninfo.sslmode == SslMode::VerifyFull ||
      (conninfo.sslmode == SslMode::Require && !conninfo.sslrootcert.empty()),
    .root_cert = conninfo.sslrootcert,
    .cert_file = conninfo.sslcert,
    .key_file = conninfo.sslkey,
    .key_password = conninfo.sslpassword,
    .crl_file = conninfo.sslcrl,
    .crl_dir = conninfo.sslcrldir,
  };
  if (options.verify_peer && options.root_cert.empty()) {
    if (const char* home = std::getenv("HOME")) {
      std::filesystem::path path{home};
      path /= ".postgresql/root.crt";
      if (std::filesystem::exists(path)) {
        options.root_cert = path.string();
      }
    }
  }
  return options;
}

bool IsIpAddress(std::string_view host) {
  asio_ns::error_code ec;
  asio_ns::ip::make_address(host, ec);
  return !ec;
}

}  // namespace

PublisherTls::PublisherTls(const ConnInfo& conninfo)
  : tls{network::BuildClientTlsContext(ClientTlsOptions(conninfo))} {}

PublisherSession::PublisherSession(network::IoExecutor& exec, ConnInfo conninfo,
                                   size_t host_index,
                                   std::string application_name,
                                   bool require_password)
  : PublisherTls{conninfo},
    PgWireSession{exec, PublisherTls::tls, ClientTag{}},
    _conninfo{std::move(conninfo)},
    _host{_conninfo.hosts.at(host_index)},
    _application_name{_conninfo.application_name.empty()
                        ? std::move(application_name)
                        : _conninfo.application_name},
    _require_password{require_password},
    _password{_conninfo.password} {}

std::string PublisherSession::SocketPath() const {
  return absl::StrCat(_host.host, "/.s.PGSQL.", _host.port);
}

std::string PublisherSession::ServerName() const {
  if (_host.IsUnixSocket()) {
    return absl::StrCat("connection to server on socket \"", SocketPath(),
                        "\" failed");
  }
  return absl::StrCat("connection to server at \"",
                      _host.hostaddr.empty() ? _host.host : _host.hostaddr,
                      "\", port ", _host.port, " failed");
}

void PublisherSession::Fail(irs::pg::SqlErrorData error) {
  if (_error.errmsg.empty()) {
    _error = std::move(error);
  }
}

void PublisherSession::Fail(int errcode, std::string message) {
  irs::pg::SqlErrorData error;
  error.errcode = errcode;
  error.errmsg = std::move(message);
  Fail(std::move(error));
}

yaclib::Task<bool> PublisherSession::Connect() {
  if (_password.empty()) {
    _password = PasswordFromFile(_conninfo, _host);
  }
  if (!co_await OpenSocket() || !co_await NegotiateTls()) {
    co_return false;
  }
  const std::string& dbname =
    _conninfo.dbname.empty() ? _conninfo.user : _conninfo.dbname;
  const std::array<std::pair<std::string_view, std::string_view>, 4> params{{
    {"user", _conninfo.user},
    {"database", dbname},
    {"replication", "database"},
    {"application_name", _application_name},
  }};
  network::pg::WriteStartupMessage(this->_send, params);
  this->KickSend();
  co_return co_await Authenticate() && co_await CheckSessionAttrs();
}

yaclib::Task<bool> PublisherSession::CheckSessionAttrs() {
  const auto attrs = _conninfo.target_session_attrs;
  if (attrs == SessionAttrs::Any) {
    co_return true;
  }
  const bool by_recovery = attrs == SessionAttrs::Primary ||
                           attrs == SessionAttrs::Standby ||
                           attrs == SessionAttrs::PreferStandby;
  if (by_recovery ? _hot_standby.empty() : _read_only.empty()) {
    std::vector<PublisherRow> rows;
    if (!co_await Query(by_recovery ? "SELECT pg_catalog.pg_is_in_recovery()"
                                    : "SHOW transaction_read_only",
                        &rows)) {
      co_return false;
    }
    if (rows.empty() || rows.front().empty() || !rows.front().front()) {
      Fail(
        ERRCODE_CONNECTION_FAILURE,
        absl::StrCat(ServerName(), ": could not determine the server state"));
      co_return false;
    }
    const auto& state = *rows.front().front();
    (by_recovery ? _hot_standby : _read_only) =
      state == "t" || state == "on" ? "on" : "off";
  }
  const bool on = (by_recovery ? _hot_standby : _read_only) == "on" ||
                  (!by_recovery && _hot_standby == "on");
  std::string_view problem;
  switch (attrs) {
    case SessionAttrs::ReadWrite:
      problem = on ? "session is read-only" : "";
      break;
    case SessionAttrs::ReadOnly:
      problem = on ? "" : "session is not read-only";
      break;
    case SessionAttrs::Primary:
      problem = on ? "server is in hot standby mode" : "";
      break;
    case SessionAttrs::Standby:
    case SessionAttrs::PreferStandby:
      problem = on ? "" : "server is not in hot standby mode";
      break;
    case SessionAttrs::Any:
      break;
  }
  if (!problem.empty()) {
    Fail(ERRCODE_CONNECTION_FAILURE, absl::StrCat(ServerName(), ": ", problem));
    co_return false;
  }
  co_return true;
}

yaclib::Task<bool> PublisherSession::OpenSocket() {
  std::optional<asio_ns::steady_timer> timeout;
  if (_conninfo.connect_timeout.count() > 0) {
    timeout.emplace(this->_io, _conninfo.connect_timeout);
    timeout->async_wait([this](const asio_ns::error_code& ec) {
      if (!ec) {
        this->_socket.Lowest().cancel();
      }
    });
  }
  asio_ns::error_code connect_ec;
  if (_host.IsUnixSocket()) {
    auto path = SocketPath();
    if (path.front() == '@') {
      path.front() = '\0';
    }
    const asio_ns::generic::stream_protocol::endpoint endpoint{
      asio_ns::local::stream_protocol::endpoint{path}};
    connect_ec = co_await network::Async<void>([&](auto&& handler) {
                   this->_socket.Lowest().async_connect(
                     endpoint, std::forward<decltype(handler)>(handler));
                 }).NoThrow();
  } else {
    const std::string& address =
      _host.hostaddr.empty() ? _host.host : _host.hostaddr;
    asio_ns::ip::tcp::resolver resolver{this->_io};
    auto [resolve_ec, endpoints] =
      co_await network::Async<asio_ns::ip::tcp::resolver::results_type>(
        [&](auto&& handler) {
          resolver.async_resolve(address, _host.port,
                                 std::forward<decltype(handler)>(handler));
        })
        .NoThrow();
    if (resolve_ec) {
      if (timeout) {
        timeout->cancel();
      }
      Fail(ERRCODE_CONNECTION_FAILURE,
           absl::StrCat("could not translate host name \"", address,
                        "\" to address: ", resolve_ec.message()));
      co_return false;
    }
    std::vector<asio_ns::generic::stream_protocol::endpoint> targets;
    for (const auto& entry : endpoints) {
      targets.emplace_back(entry.endpoint());
    }
    auto [ec, endpoint] =
      co_await network::Async<asio_ns::generic::stream_protocol::endpoint>(
        [&](auto&& handler) {
          asio_ns::async_connect(this->_socket.Lowest(), targets,
                                 std::forward<decltype(handler)>(handler));
        })
        .NoThrow();
    connect_ec = ec;
    if (!ec) {
      this->_socket.Lowest().set_option(asio_ns::ip::tcp::no_delay{true});
    }
  }
  if (timeout) {
    timeout->cancel();
  }
  if (connect_ec) {
    Fail(ERRCODE_CONNECTION_FAILURE,
         absl::StrCat(ServerName(), ": ",
                      connect_ec == asio_ns::error::operation_aborted
                        ? std::string{"timeout expired"}
                        : connect_ec.message()));
    co_return false;
  }
  co_return true;
}

yaclib::Task<bool> PublisherSession::NegotiateTls() {
  if (_host.IsUnixSocket() || _conninfo.sslmode == SslMode::Disable ||
      _conninfo.sslmode == SslMode::Allow) {
    co_return true;
  }
  network::pg::WriteSslRequest(this->_send);
  this->KickSend();
  std::array<uint8_t, 1> answer{};
  auto [ec, n] = co_await this->_socket.ReadSome(answer).NoThrow();
  if (ec || n != 1) {
    Fail(ERRCODE_CONNECTION_FAILURE,
         absl::StrCat(ServerName(),
                      ": server closed the connection "
                      "unexpectedly"));
    co_return false;
  }
  if (answer[0] == 'N') {
    if (_conninfo.sslmode >= SslMode::Require) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           absl::StrCat(ServerName(),
                        ": server does not support SSL, but SSL was "
                        "required"));
      co_return false;
    }
    co_return true;
  }
  if (answer[0] != 'S') {
    Fail(ERRCODE_PROTOCOL_VIOLATION,
         absl::StrCat(ServerName(),
                      ": received invalid response to SSL negotiation: ",
                      std::string(1, static_cast<char>(answer[0]))));
    co_return false;
  }
  auto& stream = this->_socket.TlsStream();
  if (_conninfo.sslsni == "1" && !_host.host.empty() &&
      !IsIpAddress(_host.host)) {
    SSL_set_tlsext_host_name(stream.native_handle(), _host.host.c_str());
  }
  if (_conninfo.sslmode == SslMode::VerifyFull) {
    stream.set_verify_callback(asio_ns::ssl::host_name_verification(
      _host.host.empty() ? _host.hostaddr : _host.host));
  }
  const auto handshake_ec =
    co_await this->_socket.Handshake(asio_ns::ssl::stream_base::client)
      .NoThrow();
  if (handshake_ec) {
    Fail(ERRCODE_CONNECTION_FAILURE,
         absl::StrCat(ServerName(), ": SSL error: ", handshake_ec.message()));
    co_return false;
  }
  this->_socket.MarkTls();
  co_return true;
}

yaclib::Task<bool> PublisherSession::Authenticate() {
  network::pg::ScramClientSession scram{_password};
  bool password_requested = false;
  const auto need_password = [&] {
    password_requested = true;
    if (_password.empty()) {
      Fail(ERRCODE_INVALID_PASSWORD,
           absl::StrCat(ServerName(), ": fe_sendauth: no password supplied"));
      return false;
    }
    return true;
  };
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           absl::StrCat(ServerName(),
                        ": server closed the connection unexpectedly"));
      co_return false;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type == PQ_MSG_ERROR_RESPONSE) {
      Fail(network::pg::ParseErrorResponse(payload));
      this->_frames.Consume(frame);
      co_return false;
    }
    if (type == PQ_MSG_NEGOTIATE_PROTOCOL_VERSION ||
        type == PQ_MSG_NOTICE_RESPONSE) {
      this->_frames.Consume(frame);
      continue;
    }
    if (type != PQ_MSG_AUTHENTICATION_REQUEST || payload.size() < 4) {
      Fail(ERRCODE_PROTOCOL_VIOLATION,
           absl::StrCat(ServerName(),
                        ": expected authentication request "
                        "from server"));
      this->_frames.Consume(frame);
      co_return false;
    }
    const auto code =
      static_cast<int32_t>(absl::big_endian::Load32(payload.data()));
    const auto data = payload.substr(4);
    bool ok = true;
    switch (code) {
      case kAuthOk:
        break;
      case kAuthCleartext:
        ok = need_password();
        if (ok) {
          network::pg::WritePasswordMessage(this->_send, _password);
        }
        break;
      case kAuthMd5:
        ok = need_password() && data.size() >= 4;
        if (ok) {
          const auto* salt = reinterpret_cast<const uint8_t*>(data.data());
          network::pg::WritePasswordMessage(
            this->_send,
            network::BuildMd5Response(
              network::BuildMd5Verifier(_conninfo.user, _password), {salt, 4}));
        }
        break;
      case kAuthSasl:
        ok = need_password();
        if (ok && data.find(network::pg::ScramClientSession::kMechanism) ==
                    std::string_view::npos) {
          Fail(ERRCODE_FEATURE_NOT_SUPPORTED,
               absl::StrCat(ServerName(),
                            ": none of the server's SASL authentication "
                            "mechanisms are supported"));
          ok = false;
        }
        if (ok) {
          const auto first = scram.ClientFirst();
          ok = first.has_value();
          if (ok) {
            network::pg::WriteSaslInitialResponse(
              this->_send, network::pg::ScramClientSession::kMechanism, *first);
          }
        }
        break;
      case kAuthSaslContinue: {
        auto response = scram.ServerFirst(data);
        ok = response.has_value();
        if (ok) {
          network::pg::WriteSaslResponse(this->_send, *response);
        }
        break;
      }
      case kAuthSaslFinal:
        ok = scram.ServerFinal(data);
        break;
      default:
        Fail(ERRCODE_FEATURE_NOT_SUPPORTED,
             absl::StrCat(ServerName(), ": authentication method ", code,
                          " not supported"));
        ok = false;
        break;
    }
    this->_frames.Consume(frame);
    if (!ok) {
      Fail(ERRCODE_INVALID_PASSWORD,
           absl::StrCat(ServerName(), ": authentication failed"));
      co_return false;
    }
    this->KickSend();
    if (code == kAuthOk) {
      break;
    }
  }
  if (_require_password && !password_requested) {
    irs::pg::SqlErrorData error;
    error.errcode = ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED;
    error.errmsg = "password is required";
    error.errdetail =
      "Non-superuser cannot connect if the server does not request a "
      "password.";
    error.errhint =
      "Target server's authentication method must be changed, or set "
      "password_required=false in the subscription parameters.";
    Fail(std::move(error));
    co_return false;
  }
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           absl::StrCat(ServerName(),
                        ": server closed the connection unexpectedly"));
      co_return false;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type == PQ_MSG_ERROR_RESPONSE) {
      Fail(network::pg::ParseErrorResponse(payload));
      this->_frames.Consume(frame);
      co_return false;
    }
    if (type == PQ_MSG_PARAMETER_STATUS) {
      const auto separator = payload.find('\0');
      if (separator != std::string_view::npos) {
        const auto name = payload.substr(0, separator);
        auto value = payload.substr(separator + 1);
        value = value.substr(0, value.find('\0'));
        if (name == "server_version") {
          (void)absl::SimpleAtoi(value.substr(0, value.find_first_of(".( ")),
                                 &_server_version);
        } else if (name == "in_hot_standby") {
          _hot_standby = value;
        } else if (name == "default_transaction_read_only") {
          _read_only = value;
        }
      }
    }
    this->_frames.Consume(frame);
    if (type == PQ_MSG_READY_FOR_QUERY) {
      co_return true;
    }
  }
}

void PublisherSession::SendQuery(std::string_view sql) {
  network::pg::WriteQuery(this->_send, sql);
  this->KickSend();
}

yaclib::Task<bool> PublisherSession::Query(std::string_view sql,
                                           std::vector<PublisherRow>* rows) {
  SendQuery(sql);
  co_return co_await ReadUntilReady(rows);
}

yaclib::Task<bool> PublisherSession::ReadUntilReady(
  std::vector<PublisherRow>* rows) {
  bool ok = true;
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           "server closed the connection unexpectedly");
      co_return false;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type == PQ_MSG_DATA_ROW && rows && payload.size() >= 2) {
      const auto columns = absl::big_endian::Load16(payload.data());
      auto& row = rows->emplace_back();
      row.reserve(columns);
      size_t pos = 2;
      for (uint16_t i = 0; i < columns && pos + 4 <= payload.size(); ++i) {
        const auto size =
          static_cast<int32_t>(absl::big_endian::Load32(payload.data() + pos));
        pos += 4;
        if (size < 0) {
          row.emplace_back(std::nullopt);
          continue;
        }
        row.emplace_back(std::string{payload.substr(pos, size)});
        pos += static_cast<size_t>(size);
      }
    } else if (type == PQ_MSG_ERROR_RESPONSE) {
      Fail(network::pg::ParseErrorResponse(payload));
      ok = false;
    }
    this->_frames.Consume(frame);
    if (type == PQ_MSG_READY_FOR_QUERY) {
      co_return ok;
    }
  }
}

void PublisherCall::Start(Body body, yaclib::Promise<PublisherResult> promise) {
  Run(std::move(body), std::move(promise)).Detach();
}

yaclib::Task<> PublisherCall::Run(Body body,
                                  yaclib::Promise<PublisherResult> promise) {
  auto self = this->shared_from_this();
  auto writer = this->SendWriter();
  PublisherResult result;
  result.connected = co_await Connect();
  if (result.connected) {
    result.server_version = ServerVersion();
    result.ok = co_await body(*this);
  }
  result.error = Error();
  this->Stop();
  co_await std::move(writer);
  std::move(promise).Set(std::move(result));
  co_return {};
}

PublisherResult CallPublisher(const ConnInfo& conninfo,
                              std::string_view application_name,
                              bool require_password, PublisherCall::Body body) {
  auto* pool = Server::instance().IoPool();
  SDB_ASSERT(pool);
  PublisherResult result;
  const auto seed = absl::Uniform<uint64_t>(absl::BitGen{});
  const bool fallback =
    conninfo.target_session_attrs == SessionAttrs::PreferStandby;
  for (size_t attempt = 0; attempt < (fallback ? 2 : 1) * conninfo.hosts.size();
       ++attempt) {
    const auto host = attempt % conninfo.hosts.size();
    auto arranged = conninfo;
    ArrangeHosts(arranged, seed, attempt >= conninfo.hosts.size());
    auto& exec = pool->Next();
    auto call = duckdb::make_shared_ptr<PublisherCall>(
      exec, std::move(arranged), host, std::string{application_name},
      require_password);
    auto [future, promise] = yaclib::MakeContract<PublisherResult>();
    asio_ns::post(exec.Context(),
                  [call, body, promise = std::move(promise)]() mutable {
                    call->Start(std::move(body), std::move(promise));
                  });
    result = std::move(future).Get().Ok();
    if (result.connected) {
      break;
    }
  }
  return result;
}

}  // namespace sdb::replication
