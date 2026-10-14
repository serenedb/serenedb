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

#include <functional>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/async/promise.hpp>
#include <yaclib/coro/task.hpp>

#include "network/pg/pg_wire_session.h"
#include "replication/conninfo.h"
#include "server/utils/asio_ns.h"

namespace sdb::replication {

using PublisherRow = std::vector<std::optional<std::string>>;

struct PublisherTls {
  PublisherTls(const ConnInfo& conninfo, size_t host_index);

  asio_ns::ssl::context tls{asio_ns::ssl::context::tls_client};
  std::string tls_error;
};

class PublisherSession
  : private PublisherTls,
    public network::pg::PgWireSession<network::SocketKind::Client> {
 public:
  PublisherSession(network::IoExecutor& exec, ConnInfo conninfo,
                   size_t host_index, size_t encryption,
                   std::string application_name, bool require_password);

  int ServerVersion() const noexcept { return _server_version; }
  const irs::pg::SqlErrorData& Error() const noexcept { return _error; }
  size_t EncryptionAttempt() const noexcept { return _encryption; }
  bool RetryEncryption() const noexcept { return _retry_encryption; }

 protected:
  yaclib::Task<bool> Guarded(yaclib::Task<bool> task);
  yaclib::Task<bool> Connect();
  yaclib::Task<bool> Query(std::string_view sql,
                           std::vector<PublisherRow>* rows = nullptr);
  yaclib::Task<bool> ReadUntilReady(std::vector<PublisherRow>* rows);
  void SendQuery(std::string_view sql);
  void Fail(irs::pg::SqlErrorData error);
  void Fail(int errcode, std::string message);

  ConnInfo _conninfo;
  ConnHost _host;
  std::string _application_name;

 private:
  yaclib::Task<bool> OpenSocket();
  yaclib::Task<bool> NegotiateTls();
  yaclib::Task<bool> Authenticate();
  yaclib::Task<bool> CheckSessionAttrs();
  std::string SocketPath() const;
  std::string ServerName() const;
  void SetupFailed();

  size_t _encryption;
  bool _plain_fallback = false;
  bool _retry_encryption = false;
  bool _require_password;
  std::string _password;
  std::string _hot_standby;
  std::string _read_only;
  int _server_version = 0;
  irs::pg::SqlErrorData _error;
};

struct PublisherResult {
  bool connected = false;
  bool ok = false;
  bool retry_encryption = false;
  int server_version = 0;
  irs::pg::SqlErrorData error;
};

class PublisherCall final : public PublisherSession {
 public:
  using Body = std::function<yaclib::Task<bool>(PublisherCall&)>;

  using PublisherSession::PublisherSession;
  using PublisherSession::Query;

  void Start(Body body, yaclib::Promise<PublisherResult> promise);

 private:
  yaclib::Task<> Run(Body body, yaclib::Promise<PublisherResult> promise);
};

PublisherResult CallPublisher(const ConnInfo& conninfo,
                              std::string_view application_name,
                              bool require_password, PublisherCall::Body body);

}  // namespace sdb::replication
