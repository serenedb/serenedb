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

#include "network/http/quack/service.h"

#include <absl/cleanup/cleanup.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/operator/cast_operators.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/read_stream.hpp>
#include <duckdb/logging/logger.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/coro.hpp>

#include "network/http/quack/session.h"
#include "network/http/quack/statement.h"
#include "quack_log.hpp"
#include "quack_message.hpp"
#include "quack_session_state.hpp"
#include "quack_settings.hpp"
#include "query/config.h"
#include "runtime/runtime.h"
#include "server/utils/asio_ns.h"

namespace sdb::network::http::quack {
namespace {

constexpr std::string_view kContentType = "application/vnd.duckdb";
constexpr size_t kWriteSlice = size_t{1} << 20;
constexpr auto kSweepInterval = std::chrono::seconds{1};
constexpr auto kPrepareWait = std::chrono::milliseconds{100};
constexpr size_t kTokenBytes = 16;

class BodyReader final : public duckdb::ReadStream {
 public:
  explicit BodyReader(const message::SequenceView& body)
    : _next{body.begin()}, _end{body.end()} {}

 private:
  void Underflow(duckdb::QueryContext, duckdb::data_ptr_t buffer,
                 duckdb::idx_t read_size) final {
    while (true) {
      const auto n = std::min<size_t>(read_size, ReadWindowRoom());
      std::memcpy(buffer, read_pos, n);
      read_pos += n;
      buffer += n;
      read_size -= n;
      if (read_size == 0) {
        return;
      }
      if (_next == _end) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                        ERR_MSG("the Quack request ends early"));
      }
      const auto chunk = *_next;
      ++_next;
      SetReadWindow(static_cast<const uint8_t*>(chunk.data()), chunk.size());
    }
  }

  message::SequenceView::ConstIterator _next;
  message::SequenceView::ConstIterator _end;
};

std::string Hex(std::span<const uint8_t> bytes) {
  static constexpr char kDigits[] = "0123456789ABCDEF";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (const auto byte : bytes) {
    out.push_back(kDigits[byte >> 4]);
    out.push_back(kDigits[byte & 0xF]);
  }
  return out;
}

std::string RandomToken() {
  uint8_t bytes[kTokenBytes];
  if (RAND_bytes(bytes, sizeof(bytes)) != 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("cannot generate a Quack session id"));
  }
  return Hex(bytes);
}

std::string ClientIdHash(std::string_view key, std::string_view client_id) {
  uint8_t digest[EVP_MAX_MD_SIZE];
  unsigned int length = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const uint8_t*>(client_id.data()), client_id.size(),
       digest, &length);
  return Hex({digest, length});
}

uint64_t DatabaseSetting(duckdb::DatabaseInstance& db, std::string_view name,
                         uint64_t default_value) {
  duckdb::Value value;
  if (!duckdb::DBConfig::GetConfig(db).TryGetCurrentSetting(
        duckdb::Identifier{name}, value) ||
      value.IsNull()) {
    return default_value;
  }
  return value.GetValue<uint64_t>();
}

bool LeaseElapsed(Connection::Clock::time_point renewed_at,
                  Connection::Clock::time_point now, uint64_t seconds) {
  return now > renewed_at &&
         static_cast<uint64_t>(
           std::chrono::duration_cast<std::chrono::seconds>(now - renewed_at)
             .count()) >= seconds;
}

// Errors the stream, so the requests waiting on it and the producers parked
// on its capacity wake, and interrupts the statement, which its session then
// finishes. Under the statement lock: a statement is stopped by the time the
// next one can be handed over.
void StopStatement(Connection& connection,
                   const duckdb::shared_ptr<duckdb::QuackResultStream>& stream,
                   const std::string& reason)
  ABSL_EXCLUSIVE_LOCKS_REQUIRED(connection.statement.mu) {
  if (auto session_state =
        duckdb::QuackSessionState::Get(connection.session->Context())) {
    session_state->Streams().Clear(reason);
  }
  if (!stream) {
    return;
  }
  const bool was_finished = stream->buffer.Finished();
  stream->buffer.SetError(
    duckdb::ErrorData{duckdb::ExceptionType::INTERRUPT, reason});
  if (!was_finished) {
    connection.session->Interrupt();
  }
}

template<typename Matches>
bool AbortStatementIf(Connection& connection, const std::string& reason,
                      Matches&& matches) {
  absl::MutexLock lock{&connection.statement.mu};
  if (!matches(connection.statement.stream.get(),
               connection.statement.query_uuid)) {
    return false;
  }
  auto stream = std::move(connection.statement.stream);
  if (stream) {
    connection.statement.abort_error =
      stream->buffer.HasError()
        ? stream->buffer.GetError()
        : duckdb::ErrorData{duckdb::ExceptionType::INTERRUPT, reason};
  }
  StopStatement(connection, stream, reason);
  return true;
}

void AbortStatement(Connection& connection, const std::string& reason) {
  AbortStatementIf(
    connection, reason,
    [](const duckdb::QuackResultStream*, duckdb::hugeint_t) { return true; });
}

// Makes `stream` the connection's statement and stops the one it replaces in
// one step: of two PREPAREs at once, one stops the other's statement.
void ReplaceStatement(Connection& connection,
                      duckdb::shared_ptr<duckdb::QuackResultStream> stream,
                      duckdb::hugeint_t query_uuid, const std::string& reason) {
  {
    absl::MutexLock lock{&connection.statement.mu};
    auto previous = std::move(connection.statement.stream);
    connection.statement.stream = std::move(stream);
    connection.statement.query_uuid = query_uuid;
    connection.statement.abort_error = duckdb::ErrorData{};
    StopStatement(connection, previous, reason);
  }
  SDB_PARK_ONCE_ON_FAILURE("quack_statement_replaced_late");
}

}  // namespace

Connection::Connection(std::string id, uint64_t lease_seconds,
                       duckdb::StorageVersion wire_version,
                       std::string client_id_hash,
                       duckdb::shared_ptr<Session> session)
  : id{std::move(id)},
    lease_seconds{lease_seconds},
    wire_version{wire_version},
    client_id_hash{std::move(client_id_hash)},
    session{std::move(session)},
    lease_renewed_at{Clock::now()} {}

Connection::~Connection() {
  AbortStatement(*this, "connection closed during fetch");
  session->Close();
}

bool Connection::LeaseExpiredLocked(Clock::time_point now) {
  if (!lease_expired && LeaseElapsed(lease_renewed_at, now, lease_seconds)) {
    lease_expired = true;
  }
  return lease_expired;
}

bool Connection::LeaseExpired() {
  absl::MutexLock lock{&lease_mu};
  return LeaseExpiredLocked(Clock::now());
}

bool Connection::TryRenewLease() {
  absl::MutexLock lock{&lease_mu};
  const auto now = Clock::now();
  if (LeaseExpiredLocked(now)) {
    return false;
  }
  lease_renewed_at = now;
  return true;
}

// One HTTP request: its body, where its handling stands while it waits on a
// statement, and its response.
struct Request {
  enum class Phase : uint8_t {
    Decode,
    Start,
    // A PREPARE waits for its statement's columns
    WaitBound,
    // The request waits on its stream
    Wait,
  };

  Request(BodyReader& body, std::string_view serenedb_header,
          duckdb::InterruptState waker)
    : body{body}, serenedb_header{serenedb_header}, waker{std::move(waker)} {}

  BodyReader& body;
  std::string_view serenedb_header;
  // Wakes the request's task at the next change of the stream it waits on
  const duckdb::InterruptState waker;
  // Set while the waker is registered with the stream
  bool armed = false;
  duckdb::StorageVersion wire_version = duckdb::StorageVersion::DUCKDB_LATEST;
  Phase phase = Phase::Decode;
  duckdb::MessageHeader header{duckdb::MessageType::INVALID, {}};
  duckdb::unique_ptr<duckdb::QuackMessage> message;
  std::shared_ptr<Connection> connection;
  int64_t start_time = 0;

  // PREPARE: the statement's stream and what the inline drain took from it
  duckdb::shared_ptr<duckdb::QuackResultStream> stream;
  bool answer_early = false;
  bool retain_result = false;
  uint64_t max_inline_rows = 0;
  uint64_t inline_rows = 0;
  uint64_t consumed = 0;
  duckdb::vector<duckdb::unique_ptr<duckdb::DataChunkWrapper>> results;

  // FETCH: the dense index of the batch the request names
  uint64_t dense_index = 0;

  duckdb::unique_ptr<duckdb::QuackMessage> response;
  duckdb::MemoryStream response_stream;
  const uint8_t* response_data = nullptr;
  size_t response_size = 0;

  // Registers the waker with `buffer` unless it waits there already. True when
  // it registered now: the caller then looks once more, so a change between
  // its look and the registration is not missed.
  template<typename Buffer>
  bool Arm(Buffer& buffer) {
    if (armed) {
      return false;
    }
    armed = true;
    buffer.AddWaker(waker);
    return true;
  }
};

Service::Service(CancelRegistry& cancel, yaclib::WaitGroup<>& tasks,
                 std::string server_id)
  : _cancel{cancel},
    _tasks{tasks},
    _db{irs::DuckDBEngine::Instance().instance()},
    _server_id{std::move(server_id)},
    _client_id_key{RandomToken()} {
  _sweep.Consume(SweepLoop());
  gInstance = this;
}

std::shared_ptr<Service> Service::Open(CancelRegistry& cancel,
                                       yaclib::WaitGroup<>& tasks,
                                       std::string_view url) {
  if (gInstance) {
    absl::StrAppend(&gInstance->_server_id, ",", url);
    return gInstance->shared_from_this();
  }
  return std::make_shared<Service>(cancel, tasks, std::string{url});
}

Service::~Service() {
  gInstance = nullptr;
  RequestStop();
  _sweep.Done();
  _sweep.Wait();
}

void Service::RequestStop() {
  _stopping.store(true, std::memory_order_release);
  irs::containers::FlatHashMap<std::string, std::shared_ptr<Connection>> closed;
  {
    absl::MutexLock lock{&_mu};
    closed.swap(_connections);
  }
  for (auto& [id, connection] : closed) {
    AbortStatement(*connection, "the server is shutting down");
  }
}

yaclib::Future<> Service::SweepLoop() {
  auto& runtime = Runtime::Get();
  while (!Stopping() && !runtime.IsStopping()) {
    co_await runtime.Delay(kSweepInterval);
    if (Stopping() || runtime.IsStopping()) {
      break;
    }
    SDB_IF_FAILURE("quack_sweep_paused") { continue; }
    Sweep();
  }
  co_return {};
}

void Service::Sweep() {
  std::vector<std::shared_ptr<Connection>> expired;
  {
    absl::MutexLock lock{&_mu};
    absl::erase_if(_connections, [&](const auto& entry) {
      if (!entry.second->LeaseExpired()) {
        return false;
      }
      expired.push_back(entry.second);
      return true;
    });
  }
  for (const auto& connection : expired) {
    AbortStatement(*connection, "connection heartbeat lease expired");
  }
  SweepCaches();
}

std::shared_ptr<Connection> Service::Find(std::string_view connection_id) {
  absl::MutexLock lock{&_mu};
  const auto it = _connections.find(connection_id);
  return it == _connections.end() ? nullptr : it->second;
}

bool Service::Disconnect(std::string_view connection_id) {
  std::shared_ptr<Connection> connection;
  {
    absl::MutexLock lock{&_mu};
    const auto it = _connections.find(connection_id);
    if (it == _connections.end()) {
      return false;
    }
    connection = std::move(it->second);
    _connections.erase(it);
  }
  AbortStatement(*connection, "connection closed during fetch");
  return true;
}

bool Service::RenewLease(const std::shared_ptr<Connection>& connection) {
  std::shared_ptr<Connection> expired;
  {
    absl::MutexLock lock{&_mu};
    const auto it = _connections.find(connection->id);
    if (it == _connections.end() || it->second != connection) {
      return false;
    }
    if (connection->TryRenewLease()) {
      return true;
    }
    expired = std::move(it->second);
    _connections.erase(it);
  }
  AbortStatement(*expired, "connection heartbeat lease expired");
  return false;
}

std::vector<ConnectionSnapshot> Service::Connections() {
  std::vector<std::shared_ptr<Connection>> connections;
  {
    absl::MutexLock lock{&_mu};
    connections.reserve(_connections.size());
    for (const auto& [id, connection] : _connections) {
      connections.push_back(connection);
    }
  }
  std::vector<ConnectionSnapshot> snapshots;
  snapshots.reserve(connections.size());
  for (const auto& connection : connections) {
    auto& snapshot = snapshots.emplace_back();
    snapshot.server_id = _server_id;
    snapshot.connection_id = connection->id;
    snapshot.client_id_hash = connection->client_id_hash;
    {
      absl::MutexLock lock{&connection->mu};
      snapshot.sql = connection->sql;
      snapshot.started_at = connection->query_started_at;
    }
    snapshot.state = connection->query_state.load();
    const auto cached = connection->cached_rows.load();
    if (cached != Connection::kNoCache) {
      snapshot.cached_rows = cached;
    }
  }
  return snapshots;
}

yaclib::Task<> Service::Handle(RequestContext& context,
                               const HttpRequest& http_request,
                               HttpResponseWriter& writer) {
  BodyReader body{http_request.body};
  const auto& task = context.Resumer();
  Request request{body, http_request.Header(duckdb::QUACK_SERENEDB_HEADER),
                  duckdb::InterruptState{duckdb::weak_ptr<duckdb::Task>{task}}};
  std::shared_ptr<asio_ns::steady_timer> prepare_timer;
  const auto waited_enough = std::make_shared<std::atomic<bool>>(false);
  absl::Cleanup stop_timer = [&] {
    if (prepare_timer) {
      prepare_timer->cancel();
    }
  };
  bool prepare_timer_enabled = true;
  SDB_IF_FAILURE("quack_prepare_answers_late") {
    prepare_timer_enabled = false;
  }
  for (;;) {
    auto response = Answer(request, context);
    if (response) {
      Finish(request, std::move(response));
      break;
    }
    if (prepare_timer_enabled && !prepare_timer &&
        request.header.type == duckdb::MessageType::PREPARE_REQUEST) {
      prepare_timer = std::make_shared<asio_ns::steady_timer>(
        context.Io().Context(), kPrepareWait);
      prepare_timer->async_wait([timer = prepare_timer, waited_enough,
                                 task](const asio_ns::error_code& ec) {
        if (ec) {
          return;
        }
        waited_enough->store(true, std::memory_order_release);
        task->RequestRun();
      });
    }
    co_await task->Park();
    request.armed = false;
    if (writer.Broken()) {
      co_return {};
    }
    if (waited_enough->load(std::memory_order_acquire)) {
      request.answer_early = true;
    }
  }
  const auto* data = reinterpret_cast<const char*>(request.response_data);
  const size_t size = request.response_size;
  writer.WriteHead(HttpStatus::Ok, kContentType, size);
  for (size_t offset = 0; offset < size; offset += kWriteSlice) {
    writer.Write({data + offset, std::min(kWriteSlice, size - offset)});
    co_await writer.Drain();
    if (writer.Broken()) {
      co_return {};
    }
  }
  writer.Finish();
  co_return {};
}

duckdb::unique_ptr<duckdb::QuackMessage> Service::Answer(
  Request& request, RequestContext& context) {
  try {
    if (request.phase == Request::Phase::Decode) {
      if (auto error = Decode(request)) {
        return error;
      }
      request.phase = Request::Phase::Start;
    }
    switch (request.message->Type()) {
      case duckdb::MessageType::CONNECTION_REQUEST:
        return Connect(request, context);
      case duckdb::MessageType::DISCONNECT_MESSAGE:
        if (!Disconnect(request.connection->id)) {
          return duckdb::make_uniq<duckdb::ErrorResponse>(
            "Connection does not exist / already disconnected");
        }
        return duckdb::make_uniq<duckdb::SuccessResponse>();
      case duckdb::MessageType::PREPARE_REQUEST:
        return Prepare(request);
      case duckdb::MessageType::FETCH_REQUEST:
        return Fetch(request);
      case duckdb::MessageType::SEND_DATA_REQUEST:
        return SendData(request);
      case duckdb::MessageType::CANCEL_REQUEST:
        return Cancel(request);
      case duckdb::MessageType::ACKNOWLEDGEMENT:
        return Acknowledge(request);
      case duckdb::MessageType::HEARTBEAT_REQUEST:
        return duckdb::make_uniq<duckdb::SuccessResponse>();
      default:
        return duckdb::make_uniq<duckdb::ErrorResponse>(
          "Unsupported message type for server");
    }
  } catch (const std::exception& e) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(duckdb::ErrorData{e});
  }
}

// Reads the request's message and finds its session, which the message
// renews. An error answer when the server cannot take the request.
duckdb::unique_ptr<duckdb::QuackMessage> Service::Decode(Request& request) {
  if (duckdb::ValidChecker::IsInvalidated(_db)) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      "The server database has been invalidated and the server must be "
      "restarted before it can be used again. Original error: \"%s\"",
      duckdb::ValidChecker::Get(_db).InvalidatedMessage());
  }
  request.start_time = duckdb::QuackNowMillis();
  duckdb::BinaryDeserializer deserializer{request.body};
  deserializer.SetChecks(
    duckdb::QuackPeerDataChecks(_db, duckdb::DeserializationChecks::VALUES));
  request.header = duckdb::QuackMessage::DeserializeHeader(deserializer);
  switch (request.header.type) {
    case duckdb::MessageType::CONNECTION_REQUEST:
      break;
    case duckdb::MessageType::PREPARE_REQUEST:
    case duckdb::MessageType::FETCH_REQUEST:
    case duckdb::MessageType::SEND_DATA_REQUEST:
    case duckdb::MessageType::DISCONNECT_MESSAGE:
    case duckdb::MessageType::CANCEL_REQUEST:
    case duckdb::MessageType::ACKNOWLEDGEMENT:
    case duckdb::MessageType::HEARTBEAT_REQUEST:
      request.connection = Find(request.header.connection_id);
      if (!request.connection) {
        return duckdb::make_uniq<duckdb::ErrorResponse>(
          duckdb::QUACK_INVALID_CONNECTION_ID);
      }
      break;
    default:
      return duckdb::make_uniq<duckdb::ErrorResponse>(
        "Unsupported message type for server");
  }
  SweepCaches();
  request.message =
    duckdb::QuackMessage::DeserializeMessage(deserializer, request.header);
  if (request.connection && !RenewLease(request.connection)) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      duckdb::QUACK_LEASE_EXPIRED);
  }
  return nullptr;
}

void Service::Finish(Request& request,
                     duckdb::unique_ptr<duckdb::QuackMessage> response) {
  if (response->Type() == duckdb::MessageType::ERROR_RESPONSE) {
    response->Cast<duckdb::ErrorResponse>().SetMustInvalidate(
      duckdb::ValidChecker::IsInvalidated(_db));
  }
  request.response = std::move(response);
  if (request.message) {
    auto& logger = duckdb::Logger::Get(_db);
    if (logger.ShouldLog(duckdb::QuackLogType::NAME,
                         duckdb::QuackLogType::LEVEL)) {
      std::string error;
      if (request.response->Type() == duckdb::MessageType::ERROR_RESPONSE) {
        error = request.response->Cast<duckdb::ErrorResponse>().ErrorMessage();
      }
      logger.WriteLog(
        duckdb::QuackLogType::NAME, duckdb::QuackLogType::LEVEL,
        duckdb::QuackLogType::ConstructLogMessage(
          request.header.type, request.header.connection_id,
          request.connection ? request.connection->client_id_hash
                             : std::string{},
          request.header.client_query_id, request.message->LoggableQuery(), "",
          duckdb::QuackNowMillis() - request.start_time,
          request.response->Type(), error));
    }
  }
  if (const auto raw = request.response->RawPayload()) {
    const auto body_start = request.response->RawPayloadStart();
    request.response_data = raw->GetData() + body_start;
    request.response_size = raw->GetPosition() - body_start;
    return;
  }
  request.response->ToMemoryStream(request.response_stream,
                                   request.connection
                                     ? request.connection->wire_version
                                     : request.wire_version);
  request.response_data = request.response_stream.GetData();
  request.response_size = request.response_stream.GetPosition();
}

bool Service::ReserveSession(uint64_t max_sessions) {
  absl::MutexLock lock{&_mu};
  if (max_sessions != 0 && _connections.size() + _opening >= max_sessions) {
    return false;
  }
  ++_opening;
  return true;
}

duckdb::unique_ptr<duckdb::QuackMessage> Service::Connect(
  Request& request, RequestContext& context) {
  auto& message = request.message->Cast<duckdb::ConnectionRequestMessage>();
  const auto version = std::min<uint64_t>(
    message.MaximumSupportedQuackVersion(), duckdb::QUACK_VERSION);
  if (message.MinimumSupportedQuackVersion() > version ||
      version < duckdb::QUACK_MIN_VERSION) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      "Unsupported Quack version - server only supports versions %llu to "
      "%llu of quack",
      duckdb::QUACK_MIN_VERSION, duckdb::QUACK_VERSION);
  }
  if (request.serenedb_header.empty()) {
    request.wire_version =
      duckdb::QuackDuckDBWireVersion(message.ClientVersion());
  } else {
    uint64_t client_version = 0;
    if (!duckdb::TryCast::Operation(
          duckdb::string_t{
            request.serenedb_header.data(),
            static_cast<uint32_t>(request.serenedb_header.size())},
          client_version, true)) {
      return duckdb::make_uniq<duckdb::ErrorResponse>(
        duckdb::ErrorData{duckdb::ExceptionType::INVALID_INPUT,
                          absl::StrCat(duckdb::QUACK_SERENEDB_HEADER,
                                       " names no storage version: \"",
                                       request.serenedb_header, "\"")});
    }
    request.wire_version = duckdb::QuackSereneDBWireVersion(client_version);
  }
  auto lease_seconds = message.HeartbeatTimeoutSeconds();
  if (lease_seconds == 0 ||
      lease_seconds > duckdb::MAX_HEARTBEAT_TIMEOUT_SECONDS) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      "heartbeat_timeout out of range - must be between 1 and %llu seconds",
      duckdb::MAX_HEARTBEAT_TIMEOUT_SECONDS);
  }
  lease_seconds = std::min<uint64_t>(
    lease_seconds, GetGlobalSetting(duckdb::DBConfig::GetConfig(_db),
                                    "sdb_quack_max_heartbeat_timeout")
                     .GetValue<uint64_t>());
  const auto max_sessions =
    GetGlobalSetting(duckdb::DBConfig::GetConfig(_db), "sdb_quack_max_sessions")
      .GetValue<uint64_t>();
  if (Stopping()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_ADMIN_SHUTDOWN),
                    ERR_MSG("the database system is shutting down"));
  }
  if (!ReserveSession(max_sessions)) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      "Too many Quack sessions: the server keeps at most %llu", max_sessions);
  }
  absl::Cleanup release_slot = [&] {
    absl::MutexLock lock{&_mu};
    --_opening;
  };
  auto auth = context.Authenticate(message.AuthString());
  if (auth.status != HttpStatus::None) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PASSWORD),
                    ERR_MSG("authentication failed"));
  }
  auto id = RandomToken();
  auto session = duckdb::make_shared_ptr<Session>(*this, id, auth.context.user,
                                                  context.Database());
  session->Start();
  auto connection = std::make_shared<Connection>(
    id, lease_seconds, request.wire_version,
    message.ClientId().empty()
      ? std::string{}
      : ClientIdHash(_client_id_key, message.ClientId()),
    std::move(session));
  {
    absl::MutexLock lock{&_mu};
    if (Stopping()) {
      connection->session->Close();
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_ADMIN_SHUTDOWN),
                      ERR_MSG("the database system is shutting down"));
    }
    _connections.emplace(id, connection);
    --_opening;
  }
  std::move(release_slot).Cancel();
  auto response = duckdb::make_uniq<duckdb::ConnectionResponseMessage>(
    id, version, lease_seconds);
  if (duckdb::IsSereneDBStorageVersion(request.wire_version)) {
    response->SetSereneDBStorageVersion(
      duckdb::StorageVersionToDisk(request.wire_version));
  }
  return response;
}

namespace {

// Fails the PREPARE's statement, unless a later PREPARE replaced it already.
duckdb::unique_ptr<duckdb::QuackMessage> FailStatement(
  Request& request, const duckdb::ErrorData& error) {
  auto& connection = *request.connection;
  const auto is_this_statement = [&](const duckdb::QuackResultStream* current,
                                     duckdb::hugeint_t) {
    return current == request.stream.get();
  };
  if (AbortStatementIf(connection, "query failed", is_this_statement)) {
    absl::MutexLock lock{&connection.mu};
    connection.sql.clear();
    connection.query_state = QueryState::Cancelled;
  }
  return duckdb::make_uniq<duckdb::ErrorResponse>(error);
}

// Puts a batch in the PREPARE response. A cached result keeps it too, with the
// header a replay sends.
void TakeInlineBatch(Request& request, duckdb::QuackFetchPayload& entry) {
  auto& stream = *request.stream;
  ++request.consumed;
  request.inline_rows += entry.rows;
  duckdb::MemoryStream payload{entry.payload->GetData(), entry.payload_size};
  payload.SetPosition(duckdb::QUACK_PAYLOAD_HEADER_BYTES);
  duckdb::BinaryDeserializer deserializer{payload};
  for (auto& chunk :
       duckdb::DecodeQuackChunkBlob(deserializer, entry.chunk_count)) {
    request.results.push_back(
      duckdb::make_uniq<duckdb::DataChunkWrapper>(*chunk));
  }
  if (request.retain_result) {
    duckdb::FetchResponseMessage header;
    header.SetChunkCount(entry.chunk_count);
    header.SetBatchIndex(request.consumed);
    const auto body_start =
      duckdb::QuackPrependHeader(*entry.payload, header, stream.wire_version);
    stream.Retain(request.consumed, std::move(entry.payload), body_start,
                  entry.rows);
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::QuackMessage> Service::Prepare(Request& request) {
  auto& message = request.message->Cast<duckdb::PrepareRequestMessage>();
  auto& connection = *request.connection;
  if (request.phase == Request::Phase::Start) {
    request.stream = duckdb::make_shared_ptr<duckdb::QuackResultStream>(
      duckdb::QuackServerStreamBufferBytes(_db), connection.wire_version);
    const bool client_query =
      message.QueryUUID() != duckdb::QUACK_INTERNAL_QUERY_UUID;
    request.retain_result =
      client_query && !message.DescribeOnly() &&
      DatabaseSetting(_db, "quack_enable_reconnects", 0) != 0;
    if (request.retain_result) {
      request.stream->RetainAll();
    }
    ReplaceStatement(connection, request.stream, message.QueryUUID(),
                     "superseded by a new query");
    {
      absl::MutexLock lock{&connection.mu};
      if (client_query) {
        connection.ClearResultCache();
      }
      if (!message.DescribeOnly()) {
        connection.sql = message.Query();
        connection.query_state = QueryState::Active;
        connection.query_started_at = duckdb::Timestamp::GetCurrentTimestamp();
      }
    }
    connection.session->Run(std::make_unique<StatementRunner>(
      connection.session->Context(), request.stream, message.Query(),
      message.DescribeOnly()));
    request.max_inline_rows =
      message.InlineRows().IsValid()
        ? message.InlineRows().GetIndex()
        : DatabaseSetting(_db, "quack_prepare_inline_rows",
                          duckdb::QUACK_PREPARE_INLINE_ROWS_DEFAULT);
    request.phase = Request::Phase::WaitBound;
  }

  auto& stream = *request.stream;
  if (request.phase == Request::Phase::WaitBound) {
    while (!stream.Answerable()) {
      if (stream.buffer.Finished()) {
        return FailStatement(
          request, stream.buffer.HasError()
                     ? stream.buffer.GetError()
                     : duckdb::ErrorData{duckdb::ExceptionType::INVALID_INPUT,
                                         "Query did not return any columns"});
      }
      if (!request.Arm(stream.buffer)) {
        return nullptr;
      }
    }
    request.phase = Request::Phase::Wait;
  }

  while (request.inline_rows < request.max_inline_rows) {
    duckdb::QuackFetchPayload entry;
    const auto status =
      stream.buffer.TryPopClaimed(request.consumed + 1, entry);
    if (status == duckdb::QuackClaimPopStatus::BATCH) {
      TakeInlineBatch(request, entry);
      continue;
    }
    if (status == duckdb::QuackClaimPopStatus::FINISHED) {
      break;
    }
    if (status == duckdb::QuackClaimPopStatus::ERRORED) {
      return FailStatement(request, stream.buffer.GetError());
    }
    if (request.answer_early) {
      break;
    }
    if (!request.Arm(stream.buffer)) {
      return nullptr;
    }
  }
  stream.prepare_batches.store(request.consumed, std::memory_order_release);
  if (request.retain_result) {
    CacheResult(request);
  }
  const bool needs_more_fetch =
    !stream.buffer.Exhausted() || stream.buffer.HasError();
  if (!needs_more_fetch) {
    connection.MarkQueryFinished();
  }
  auto columns = stream.GetColumns();
  auto response = duckdb::make_uniq<duckdb::PrepareResponseMessage>(
    columns.types, columns.names, std::move(request.results), needs_more_fetch,
    message.QueryUUID());
  response->SetDescribedOnly(columns.described_only);
  return response;
}

// Updates the cache with what its stream holds. A result past
// quack_cache_max_rows is too large to keep for a reconnect.
void Service::SyncCache(Connection& connection,
                        duckdb::QuackResultStream& stream) {
  absl::MutexLock lock{&connection.mu};
  auto& cache = connection.result_cache;
  if (!cache || cache->stream.get() != &stream) {
    return;
  }
  cache->last_served_at = duckdb::Timestamp::GetCurrentTimestamp();
  cache->retained_rows = stream.RetainedRows();
  const auto max_rows = DatabaseSetting(_db, "quack_cache_max_rows", 0);
  if (max_rows != 0 && cache->retained_rows > max_rows) {
    stream.DropRetention();
    connection.ClearResultCache();
    return;
  }
  connection.SyncCachedRows();
}

void Service::QueueForExpiry(Connection& connection) {
  if (connection.in_expiry_queue) {
    return;
  }
  connection.in_expiry_queue = true;
  absl::MutexLock lock{&_expiry_mu};
  _expiry.push({connection.result_cache->last_served_at, connection.id});
}

void Service::CacheResult(Request& request) {
  auto& message = request.message->Cast<duckdb::PrepareRequestMessage>();
  auto& connection = *request.connection;
  {
    absl::MutexLock lock{&connection.mu};
    connection.result_cache = std::make_unique<ResultCache>(
      message.Query(), message.QueryUUID(), request.stream, _live_caches);
    QueueForExpiry(connection);
  }
  SyncCache(connection, *request.stream);
}

void Service::SweepCaches() {
  if (_live_caches.load(std::memory_order_relaxed) == 0) {
    return;
  }
  const auto ttl_seconds = DatabaseSetting(_db, "quack_result_ttl", 0);
  if (ttl_seconds == 0) {
    return;
  }
  const int64_t ttl_micros =
    ttl_seconds > static_cast<uint64_t>(std::numeric_limits<int64_t>::max() /
                                        duckdb::Interval::MICROS_PER_SEC)
      ? std::numeric_limits<int64_t>::max()
      : static_cast<int64_t>(ttl_seconds) * duckdb::Interval::MICROS_PER_SEC;
  const auto now = duckdb::Timestamp::GetCurrentTimestamp();
  std::vector<ExpiryEntry> stale;
  {
    absl::MutexLock lock{&_expiry_mu};
    while (!_expiry.empty() &&
           now.value - _expiry.top().served_at.value > ttl_micros) {
      stale.push_back(_expiry.top());
      _expiry.pop();
    }
  }
  std::vector<ExpiryEntry> requeue;
  std::vector<std::unique_ptr<ResultCache>> dropped;
  std::vector<std::shared_ptr<Connection>> abandoned;
  for (auto& entry : stale) {
    auto connection = Find(entry.connection_id);
    if (!connection) {
      continue;
    }
    absl::MutexLock lock{&connection->mu};
    auto& cache = connection->result_cache;
    if (cache && now.value - cache->last_served_at.value > ttl_micros) {
      duckdb::shared_ptr<duckdb::QuackResultStream> active;
      {
        absl::MutexLock lock{&connection->statement.mu};
        active = connection->statement.stream;
      }
      if (cache->stream && cache->stream == active &&
          !cache->stream->buffer.Exhausted()) {
        connection->query_state = QueryState::Cancelled;
        abandoned.push_back(connection);
      }
      dropped.push_back(std::move(cache));
      connection->ClearResultCache();
    }
    if (connection->result_cache) {
      entry.served_at = connection->result_cache->last_served_at;
      requeue.push_back(std::move(entry));
    } else {
      connection->in_expiry_queue = false;
    }
  }
  for (const auto& connection : abandoned) {
    AbortStatement(*connection, "Query was interrupted");
  }
  if (!requeue.empty()) {
    absl::MutexLock lock{&_expiry_mu};
    for (auto& entry : requeue) {
      _expiry.push(std::move(entry));
    }
  }
}

namespace {

// The client has everything up to `dense_ack` and cannot ask for it again. A
// result cache keeps the payloads longer.
void ReleaseAcked(duckdb::QuackResultStream& stream, uint64_t dense_ack) {
  duckdb::lock_guard<duckdb::mutex> guard{stream.serve_lock};
  if (stream.retain_all) {
    return;
  }
  const auto last = stream.served.upper_bound(dense_ack);
  for (auto it = stream.served.begin(); it != last; ++it) {
    stream.retained_rows -= it->second.rows;
  }
  stream.served.erase(stream.served.begin(), last);
}

// The batch at `dense_index`, with the header the client sees before it: the
// one an earlier serve kept, or the next one popped, kept for a retry.
duckdb::QuackClaimPopStatus ServeBatch(
  duckdb::QuackResultStream& stream, uint64_t dense_index, uint64_t batch_index,
  duckdb::shared_ptr<duckdb::MemoryStream>& payload,
  duckdb::idx_t& body_start) {
  duckdb::lock_guard<duckdb::mutex> guard{stream.serve_lock};
  if (const auto it = stream.served.find(dense_index);
      it != stream.served.end()) {
    payload = it->second.payload;
    body_start = it->second.body_start;
    return duckdb::QuackClaimPopStatus::BATCH;
  }
  duckdb::QuackFetchPayload entry;
  const auto status = stream.buffer.TryPopClaimed(dense_index, entry);
  if (status != duckdb::QuackClaimPopStatus::BATCH) {
    return status;
  }
  duckdb::FetchResponseMessage header;
  header.SetChunkCount(entry.chunk_count);
  header.SetBatchIndex(batch_index);
  body_start =
    duckdb::QuackPrependHeader(*entry.payload, header, stream.wire_version);
  payload = duckdb::shared_ptr<duckdb::MemoryStream>(std::move(entry.payload));
  stream.served.emplace(dense_index, duckdb::QuackResultStream::RetainedPayload{
                                       payload, body_start, entry.rows});
  stream.retained_rows += entry.rows;
  return duckdb::QuackClaimPopStatus::BATCH;
}

}  // namespace

duckdb::unique_ptr<duckdb::QuackMessage> Service::Fetch(Request& request) {
  auto& message = request.message->Cast<duckdb::FetchRequestMessage>();
  auto& connection = *request.connection;
  if (request.phase == Request::Phase::Start) {
    duckdb::hugeint_t uuid;
    duckdb::ErrorData abort_error;
    {
      absl::MutexLock lock{&connection.statement.mu};
      request.stream = connection.statement.stream;
      uuid = connection.statement.query_uuid;
      abort_error = connection.statement.abort_error;
    }
    if (!request.stream && uuid == message.QueryUUID() &&
        abort_error.HasError()) {
      return duckdb::make_uniq<duckdb::ErrorResponse>(abort_error);
    }
    if (!request.stream || uuid != message.QueryUUID()) {
      return duckdb::make_uniq<duckdb::ErrorResponse>("Result has been closed");
    }
    if (message.BatchIndex() == 0) {
      return duckdb::make_uniq<duckdb::ErrorResponse>(
        "FETCH_REQUEST is missing its batch index");
    }
    const auto prepare_batches =
      request.stream->prepare_batches.load(std::memory_order_acquire);
    request.dense_index = message.BatchIndex() + prepare_batches;
    ReleaseAcked(*request.stream, message.AckIndex() + prepare_batches);
    request.phase = Request::Phase::Wait;
  }

  auto& stream = *request.stream;
  for (;;) {
    duckdb::shared_ptr<duckdb::MemoryStream> payload;
    duckdb::idx_t body_start = 0;
    const auto status = ServeBatch(stream, request.dense_index,
                                   message.BatchIndex(), payload, body_start);
    if (status == duckdb::QuackClaimPopStatus::BATCH) {
      SyncCache(connection, stream);
      return duckdb::make_uniq<duckdb::QuackRawPayloadResponse>(
        duckdb::MessageType::FETCH_RESPONSE, std::move(payload), body_start);
    }
    if (status == duckdb::QuackClaimPopStatus::ERRORED) {
      return duckdb::make_uniq<duckdb::ErrorResponse>(stream.buffer.GetError());
    }
    if (status == duckdb::QuackClaimPopStatus::FINISHED) {
      connection.MarkQueryFinished();
      auto response = duckdb::make_uniq<duckdb::FetchResponseMessage>();
      if (stream.announced_total.IsValid()) {
        response->SetTotalBatches(
          stream.announced_total.GetIndex() -
          stream.prepare_batches.load(std::memory_order_acquire));
      }
      return response;
    }
    if (!request.Arm(stream.buffer)) {
      return nullptr;
    }
  }
}

namespace {

// Code that trusts its values reads a client's batch, so it has exactly the
// stream's column types; the deserializer checked its values.
void ValidateClientBatch(
  const duckdb::QuackInsertStream& stream,
  const duckdb::vector<duckdb::unique_ptr<duckdb::DataChunk>>& chunks) {
  for (const auto& chunk : chunks) {
    if (chunk->GetTypes() != stream.types) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
        ERR_MSG("A batch does not have the column types of its data stream"));
    }
    if (chunk->size() > STANDARD_VECTOR_SIZE) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                      ERR_MSG("A batch chunk holds ", chunk->size(),
                              " rows, more than ", STANDARD_VECTOR_SIZE));
    }
  }
}

}  // namespace

duckdb::unique_ptr<duckdb::QuackMessage> Service::SendData(Request& request) {
  auto& message = request.message->Cast<duckdb::SendDataRequestMessage>();
  auto& connection = *request.connection;
  auto session_state =
    duckdb::QuackSessionState::Get(connection.session->Context());
  auto stream =
    session_state ? session_state->Streams().Find(message.StreamId()) : nullptr;
  if (!stream) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      "No active data stream '%s'", message.StreamId());
  }
  {
    absl::MutexLock lock{&connection.statement.mu};
    if (connection.statement.stream &&
        connection.statement.stream->buffer.HasError()) {
      return duckdb::make_uniq<duckdb::ErrorResponse>(
        connection.statement.stream->buffer.GetError());
    }
  }
  auto& chunks = message.Chunks();
  if (request.phase == Request::Phase::Start) {
    if (!chunks.empty() && !message.BatchIndex().IsValid()) {
      return duckdb::make_uniq<duckdb::ErrorResponse>(
        "send_data_request is missing its batch index");
    }
    try {
      ValidateClientBatch(*stream, chunks);
    } catch (const std::exception& e) {
      duckdb::ErrorData error{e};
      stream->buffer.SetError(error);
      return duckdb::make_uniq<duckdb::ErrorResponse>(std::move(error));
    }
    request.phase = Request::Phase::Wait;
  }
  if (!chunks.empty()) {
    size_t bytes = 0;
    for (const auto& chunk : chunks) {
      bytes += chunk->GetAllocationSize();
    }
    if (stream->buffer.TryPushBatch(message.BatchIndex().GetIndex(), chunks,
                                    bytes, &request.waker) ==
        duckdb::QuackPushStatus::NO_CAPACITY) {
      return nullptr;
    }
  }
  if (!message.TotalBatches().IsValid()) {
    if (stream->buffer.HasError()) {
      return duckdb::make_uniq<duckdb::ErrorResponse>(
        stream->buffer.GetError());
    }
    return duckdb::make_uniq<duckdb::SendDataResponseMessage>();
  }
  stream->buffer.Finish(message.TotalBatches());
  if (stream->buffer.HasError()) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(stream->buffer.GetError());
  }
  return duckdb::make_uniq<duckdb::SendDataResponseMessage>();
}

duckdb::unique_ptr<duckdb::QuackMessage> Service::Cancel(Request& request) {
  auto& message = request.message->Cast<duckdb::CancelRequestMessage>();
  auto& connection = *request.connection;
  const bool any = message.query_uuid == duckdb::QUACK_ANY_QUERY_UUID;
  duckdb::hugeint_t active;
  const auto is_target = [&](const duckdb::QuackResultStream*,
                             duckdb::hugeint_t uuid) {
    active = uuid;
    return any || uuid == message.query_uuid;
  };
  if (!AbortStatementIf(connection, "Interrupted: query was cancelled",
                        is_target)) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      "Attempted to cancel a different query with id '%s' instead of '%s'",
      message.query_uuid, active);
  }
  absl::MutexLock lock{&connection.mu};
  connection.query_state = QueryState::Cancelled;
  connection.ClearResultCache();
  return duckdb::make_uniq<duckdb::SuccessResponse>();
}

duckdb::unique_ptr<duckdb::QuackMessage> Service::Acknowledge(
  Request& request) {
  auto& message = request.message->Cast<duckdb::AcknowledgementMessage>();
  auto& connection = *request.connection;
  absl::MutexLock lock{&connection.mu};
  auto& cache = connection.result_cache;
  if (!cache) {
    return duckdb::make_uniq<duckdb::SuccessResponse>();
  }
  if (cache->query_uuid != message.QueryUUID()) {
    return duckdb::make_uniq<duckdb::ErrorResponse>(
      "Attempted to acknowledge a different query with id '%s' instead of "
      "'%s'",
      message.QueryUUID(), cache->query_uuid);
  }
  if (cache->stream) {
    cache->stream->DropRetention();
  }
  connection.ClearResultCache();
  return duckdb::make_uniq<duckdb::SuccessResponse>();
}

}  // namespace sdb::network::http::quack
