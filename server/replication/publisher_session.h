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
  explicit PublisherTls(const ConnInfo& conninfo);

  asio_ns::ssl::context tls;
};

class PublisherSession
  : private PublisherTls,
    public network::pg::PgWireSession<network::SocketKind::MaybeTls> {
 public:
  PublisherSession(network::IoExecutor& exec, ConnInfo conninfo,
                   size_t host_index, std::string application_name,
                   bool require_password);

  int ServerVersion() const noexcept { return _server_version; }
  const irs::pg::SqlErrorData& Error() const noexcept { return _error; }

 protected:
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
  std::string ServerName() const;

  bool _require_password;
  int _server_version = 0;
  irs::pg::SqlErrorData _error;
};

struct PublisherResult {
  bool connected = false;
  bool ok = false;
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
