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
#include "pg/hba.h"
#include "pg/serialize.h"
#include "pg/sql_analyzer_velox.h"
#include "pg/sql_collector.h"
#include "pg/sql_error.h"
#include "pg/sql_parser.h"
#include "pg/sql_statement.h"
#include "pg/sql_utils.h"
#include "query/query.h"
#include "utils/exec_context.h"
#include "utils/query_string.h"

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

using BufferView = std::string_view;

class PgSQLCommTaskBase : public rest::CommTask {
 public:
  PgSQLCommTaskBase(rest::GeneralServer& server, ConnectionInfo info);
  ~PgSQLCommTaskBase() override;

  void Process();
  void NextRootPortal();

  virtual void SendAsync(message::SequenceView data) = 0;

  void ParseClientParameters(std::string_view data);
  void CancelPacket();
  bool IsCancelled() const noexcept {
    return _cancel_packet.load(std::memory_order_relaxed);
  }

  std::string_view DatabaseName() const noexcept;
  std::string_view UserName() const noexcept;

 protected:
  // Binded Query
  struct SqlPortal {
    void Reset(PgSQLCommTaskBase& task) {
      task.ReleaseCursor(*this);
      stmt = nullptr;
      rows = 0;
    }
    SqlStatement* stmt{nullptr};
    uint64_t rows{0};
    std::unique_ptr<query::Cursor> cursor;
    BindInfo bind_info;
    SerializationContext serialization_context;
    std::vector<SerializationFunction> columns_serializers;
  };

  void Reschedule();
  virtual void SetIOTimeoutImpl() = 0;

  std::string_view StartPacket();
  // It's called when everything is done:
  // 1. after success way done or exception catched and sent related message
  void FinishPacket() noexcept;

  void HandleClientPacket(std::string_view packet);
  void HandleClientHello(std::string_view packet);
  void SendWarnings();
  void SendParameterStatus(std::string_view name, std::string_view value);
  void SendError(std::string_view message, std::string_view sqlstate);
  void SendError(std::string_view message, int errcode);
  void SendNotice(char type, std::string_view message,
                  std::string_view sqlstate, std::string_view error_detail = {},
                  std::string_view error_hint = {}, std::string_view query = {},
                  int cursor_pos = -1);
  void SendNotice(char type, const pg::SqlErrorData& what,
                  std::string_view query);

  void RunSimpleQuery(std::string_view query_string);
  void ParseQuery(std::string_view packet);
  void BindQuery(std::string_view packet);
  void DescribeQuery(std::string_view packet);
  void ExecuteQuery(std::string_view packet);
  void ExecuteClose(std::string_view packet);
  SqlStatement MakeStatement(std::string_view query_string);
  SqlPortal BindStatement(SqlStatement& stmt, BindInfo bind_info);
  void DescribePortal(const SqlPortal& portal);
  void DescribeStatement(SqlStatement& statement);
  void DescribeAnalyzedQuery(const SqlStatement& statement,
                             const std::vector<VarFormat>& formats,
                             bool extended = true);
  std::optional<BindInfo> ParseBindVars(
    std::string_view packet, const std::string_view statement_name,
    const std::vector<velox::TypePtr>& param_types);
  std::optional<std::vector<VarFormat>> ParseBindFormats(
    std::string_view& packet);
  void ExecutePortal(SqlPortal& portal, size_t limit);
  bool RegisterCursor(std::unique_ptr<query::Cursor> cursor, SqlPortal& portal);
  void ReleaseCursor(SqlPortal& portal);

  enum class ProcessState : uint8_t {
    More,
    Wait,
    DoneQuery,
    DonePacket,
  };

  void WakeupHandler() noexcept;
  ProcessState ProcessQueryResult();
  void SendBatch(const velox::RowVectorPtr& batch);
  void SendCommandComplete(const SqlTree& tree, uint64_t rows);

  using PacketsQueue = std::queue<std::string>;
  PostgresFeature& _feature;
  PacketsQueue _queue;
  mutable absl::Mutex _queue_mutex;
  absl::Mutex _execution_mutex;
  containers::FlatHashMap<std::string, std::string> _client_parameters;
  std::shared_ptr<catalog::Database> _database;
  SqlPortal* _current_portal{nullptr};
  std::string_view _current_query;
  containers::NodeHashMap<std::string, SqlStatement> _statements;
  // TODO: optimize portal layout
  containers::NodeHashMap<std::string, SqlPortal> _portals;
  SqlStatement _anonymous_statement;
  SqlPortal _anonymous_portal;
  uint64_t _key{0};
  bool _pop_packet{false};
  bool _success_packet{false};
  char _current_packet_type{0};
  std::atomic_bool _cancel_packet{false};
  std::atomic<State> _state{State::ClientHello};
  std::shared_ptr<ConnectionContext> _connection_ctx;
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
  void SendAsync(message::SequenceView data) final;
  void Stop() final {
    this->CancelPacket();
    Base::Stop();
  }
  void Close(asio_ns::error_code err = {}) final {
    std::lock_guard lock{this->_queue_mutex};
    Base::Close(err);
  }
  bool ReadCallback(asio_ns::error_code ec) final;
  void SetIOTimeout() final;
  void SetIOTimeoutImpl() final;
  void TimeoutStop();

  std::string _packet;
  uint32_t _pending_len{0};
};

}  // namespace sdb::pg
