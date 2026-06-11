////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <atomic>
#include <duckdb.hpp>
#include <mutex>
#include <queue>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "basics/memory_types.h"
#include "basics/message_buffer.h"
#include "catalog/database.h"
#include "general_server/asio_socket.h"
#include "general_server/comm_task.h"
#include "general_server/generic_comm_task.h"
#include "pg/connection_context.h"
#include "pg/copy_messages_queue.h"
#include "pg/duckdb_sql_statement.h"
#include "pg/hba.h"
#include "pg/sql_error.h"
#include "utils/exec_context.h"

namespace sdb::pg {

class PostgresFeature;

// ClientHello -> Idle -> Processing -> Idle -> ErrorRecovery -> Idle....
enum class State : uint8_t {
  // Client authorization. No queries could be processed
  ClientHello,
  // Awaiting query
  Idle,
  // Processing query
  Processing,
  // Error recovery. Skipping to next Sync packet
  ErrorRecovery,
};

inline constexpr std::string_view kZero{"", 1};

inline constexpr std::string_view kAnonymObject{""};

inline constexpr std::array<char, 5> kCopyDone{
  'c', 0x00, 0x00, 0x00, 0x04,
};

using BufferView = std::string_view;

template<size_t N>
BufferView ToBuffer(const std::array<char, N>& data) {
  return BufferView{data.data(), data.size()};
}

class PgSQLCommTaskBase : public rest::CommTask {
 public:
  PgSQLCommTaskBase(rest::GeneralServer& server, ConnectionInfo info);
  ~PgSQLCommTaskBase() override;

  void ProcessFirstRoot() noexcept;
  void ProcessNextRoot() noexcept;
  void ProcessWakeup(yaclib::Result<> r) noexcept;

  virtual void SendAsync(message::SequenceView data) noexcept = 0;

  void ParseClientParameters(std::string_view data);
  void CancelPacket();
  bool IsCancelled() const noexcept {
    return _cancel_packet.load(std::memory_order_relaxed);
  }

  std::string_view DatabaseName() const noexcept;
  std::string_view UserName() const noexcept;

 protected:
  // Bound Query Portal (DuckDB-based)
  struct DuckDBPortal {
    void Reset(PgSQLCommTaskBase& task) {
      task.ReleaseResult(*this);
      stmt = nullptr;
      rows = 0;
    }
    DuckDBStatement* stmt{nullptr};
    uint64_t rows{0};
    duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
    duckdb::unique_ptr<duckdb::QueryResult> result;
    DuckDBBindInfo bind_info;
    SerializationContext serialization_context;
    std::vector<SerializationFunction> columns_serializers;
  };

  virtual void SetIOTimeoutImpl() = 0;

  std::string_view StartPacket() noexcept;
  void FinishPacket() noexcept;

  void HandleClientPacket(std::string_view packet);
  void HandleClientHello(std::string_view packet);

  void SendParameterStatus(std::string_view name, std::string_view value);

  // PG ReadyForQuery transaction-status indicator: 'I' idle (no txn block),
  // 'T' in a transaction block, 'E' in a failed transaction block.
  char TransactionStatusIndicator() const noexcept;
  // Sends a ReadyForQuery carrying the live transaction-status indicator.
  void SendReadyForQuery();

  void SendNotices();
  void SendError(const duckdb::ErrorData& error);
  void SendError(std::string_view message, int errcode);
  void SendNotice(char type, const pg::SqlErrorData& error);
  void SendNotice(char type, std::string_view message,
                  std::string_view sqlstate, std::string_view error_detail = {},
                  std::string_view error_hint = {},
                  std::string_view context = {}, std::string_view query = {},
                  int cursor_pos = -1);

  enum class ProcessState : uint8_t {
    More,
    Wait,
    DoneQuery,
    DonePacket,
  };

  void RunSimpleQuery(std::string_view query_string);
  void ParseQuery(std::string_view packet);
  void BindQuery(std::string_view packet);
  void DescribeQuery(std::string_view packet);
  void ExecuteQuery(std::string_view packet);
  void ExecuteClose(std::string_view packet);
  void DescribePortal(const DuckDBPortal& portal);
  void DescribeStatement(DuckDBStatement& statement);
  void DescribeAnalyzedQuery(duckdb::StatementReturnType return_type,
                             const std::vector<duckdb::LogicalType>& types,
                             const std::vector<std::string>& names,
                             const std::vector<VarFormat>& formats,
                             bool extended = true);
  duckdb::unique_ptr<duckdb::PendingQueryResult> PendingQueryEnsured(
    duckdb::PreparedStatement& prepared, duckdb::vector<duckdb::Value>& values,
    bool allow_stream_result);
  DuckDBPortal BindStatement(DuckDBStatement& stmt, DuckDBBindInfo bind_info);
  void BuildColumnSerializers(DuckDBPortal& portal);
  void DeallocateNamedStatement(std::string_view name);
  void ExecutePortal(DuckDBPortal& portal);
  void ExecuteNextSimpleStatement();
  void ReleaseResult(DuckDBPortal& portal);
  ProcessState ProcessQueryResult();
  void SendBatch(const duckdb::DataChunk& chunk);
  void SendCommandComplete(duckdb::StatementType stmt_type, uint64_t rows);
  std::optional<DuckDBBindInfo> ParseBindVars(std::string_view packet,
                                              std::string_view statement_name,
                                              const DuckDBStatement& stmt);
  std::optional<std::vector<VarFormat>> ParseBindFormats(
    std::string_view& packet);

  template<typename Func>
  void SafeCall(Func&& func) noexcept;

  using PacketsQueue = std::queue<std::string>;
  PostgresFeature& _feature;
  CopyMessagesQueue _copy_queue;
  PacketsQueue _queue;
  mutable absl::Mutex _queue_mutex;
  absl::Mutex _execution_mutex;
  containers::FlatHashMap<std::string, std::string> _client_parameters;
  DuckDBPortal* _current_portal{nullptr};
  std::string_view _current_query;
  // TODO: optimize portal layout
  containers::NodeHashMap<std::string, DuckDBStatement> _statements;
  containers::NodeHashMap<std::string, DuckDBPortal> _portals;
  DuckDBStatement _anonymous_statement;
  DuckDBPortal _anonymous_portal;
  uint64_t _key{0};
  bool _pop_packet{false};
  bool _success_packet{false};
  bool _ssl_handshake_passed{false};
  char _current_packet_type{0};
  std::atomic_bool _cancel_packet{false};
  std::atomic<State> _state{State::ClientHello};
  std::shared_ptr<ConnectionContext> _connection_ctx;
  duckdb::unique_ptr<duckdb::Connection> _duckdb_conn;
  message::Buffer _send;
};

template<rest::SocketType T>
class PgSQLCommTask final : public GenericCommTask<T, PgSQLCommTaskBase> {
  using Base = GenericCommTask<T, PgSQLCommTaskBase>;

 public:
  PgSQLCommTask(rest::GeneralServer& server, ConnectionInfo info,
                std::shared_ptr<rest::AsioSocket<T>> so);

 private:
  void Start() final;
  void SendAsync(message::SequenceView data) noexcept final;
  void Stop() final {
    this->CancelPacket();
    Base::Stop();
  }
  void Close(asio_ns::error_code err = {}) final {
    std::lock_guard lock{this->_queue_mutex};
    CloseImpl(err);
  }
  void CloseImpl(asio_ns::error_code close_error);
  bool ReadCallback(asio_ns::error_code ec) final;
  void SetIOTimeout() final {
    std::lock_guard lock{this->_queue_mutex};
    SetIOTimeoutImpl();
  }
  void SetIOTimeoutImpl() final;
  void TimeoutStop();

  std::string _packet;
  uint32_t _pending_len{0};
  std::atomic_bool _send_should_close = false;
  asio_ns::error_code _close_error;
};

}  // namespace sdb::pg
