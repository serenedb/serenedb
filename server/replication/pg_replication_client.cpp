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

#include "replication/pg_replication_client.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/subscription_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/parsed_data/alter_sequence_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/statement/alter_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "connector/duckdb_client_state.h"
#include "network/asio_awaitable.h"
#include "network/pg/wire_frames.h"
#include "pg/commands/create_subscription.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "pg/pg_types.h"
#include "pg/protocol.h"
#include "pg/sql_utils.h"
#include "replication/repl_source.h"
#include "replication/settings.h"

namespace sdb::replication {
namespace {

using network::pg::FrameKind;
using network::pg::FrameStatus;

constexpr auto kFeedbackTick = std::chrono::seconds{1};
constexpr int64_t kPgEpochMicros = 946684800LL * 1000000;
constexpr size_t kGroupTxns = 1000;
constexpr size_t kArenaBytes = 1 << 20;
constexpr size_t kOutboxMessages = 4096;
constexpr int64_t kGroupMicros = 100000;

int64_t SteadyMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
           std::chrono::steady_clock::now().time_since_epoch())
    .count();
}

int64_t NowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
           std::chrono::system_clock::now().time_since_epoch())
    .count();
}

std::string PublicationList(const std::vector<std::string>& publications) {
  return absl::StrJoin(publications, ", ",
                       [](std::string* out, const std::string& publication) {
                         out->append(pg::QuoteLiteral(publication));
                       });
}

std::vector<size_t> ReferencedFirstOrder(
  const std::vector<duckdb::optional_ptr<duckdb::TableCatalogEntry>>& locals) {
  const size_t n = locals.size();
  irs::containers::FlatHashMap<std::string, size_t> index;
  index.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    if (locals[i]) {
      index.try_emplace(locals[i]->name.GetIdentifierName(), i);
    }
  }
  std::vector<std::vector<std::string>> references(n);
  for (size_t i = 0; i < n; ++i) {
    if (!locals[i]) {
      continue;
    }
    for (const auto& constraint : locals[i]->GetConstraints()) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      const auto& info = constraint->Cast<duckdb::ForeignKeyConstraint>().info;
      if (info.type == duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
        references[i].push_back(info.table.GetIdentifierName());
      }
    }
  }
  std::vector<size_t> order;
  order.reserve(n);
  std::vector<uint8_t> state(n, 0);
  std::vector<std::pair<size_t, size_t>> stack;
  for (size_t start = 0; start < n; ++start) {
    if (state[start] != 0) {
      continue;
    }
    state[start] = 1;
    stack.emplace_back(start, 0);
    while (!stack.empty()) {
      const size_t node = stack.back().first;
      size_t cursor = stack.back().second;
      bool descended = false;
      if (locals[node]) {
        const auto& fks = references[node];
        while (cursor < fks.size()) {
          const auto it = index.find(fks[cursor]);
          ++cursor;
          if (it == index.end() || state[it->second] != 0) {
            continue;
          }
          stack.back().second = cursor;
          state[it->second] = 1;
          stack.emplace_back(it->second, 0);
          descended = true;
          break;
        }
      }
      if (!descended) {
        state[node] = 2;
        order.push_back(node);
        stack.pop_back();
      }
    }
  }
  return order;
}

void CheckTargetColumns(std::string_view schema, std::string_view table,
                        const std::vector<std::string>& missing,
                        const std::vector<std::string>& generated) {
  const auto report = [&](const std::vector<std::string>& columns,
                          std::string_view what) {
    if (columns.empty()) {
      return;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("logical replication target relation \"", schema, ".", table,
              "\" ", what, columns.size() == 1 ? " column: " : " columns: ",
              absl::StrJoin(columns, ", ",
                            [](std::string* out, const std::string& column) {
                              absl::StrAppend(out, "\"", column, "\"");
                            })));
  };
  report(missing, "is missing replicated");
  report(generated, "has incompatible generated");
}

}  // namespace

PgReplicationClient::PgReplicationClient(network::IoExecutor& exec,
                                         ReplicationTarget target,
                                         size_t host_index)
  : PublisherSession{exec, target.conninfo, host_index,
                     target.subscription_name, target.require_password},
    _target{std::move(target)},
    _flushed_lsn{_target.start_lsn} {
  _arena.reserve(kArenaBytes);
  _outbox.reserve(kOutboxMessages);
}

yaclib::Task<> PgReplicationClient::RunClient() {
  auto self = this->shared_from_this();
  auto writer = this->SendWriter();
  if (co_await Connect()) {
    _connected.store(true, std::memory_order_release);
    _stream.SetTask(this->_task.get());
    auto cpu = ReplicationMain();
    this->_handed_off = true;
    this->_task->Start();
    co_await _setup_done.AwaitOn(*this->_ioexec);
    bool streaming = _setup_ok;
    if (streaming && !co_await SyncTables()) {
      _sync_failed.store(true, std::memory_order_release);
      streaming = false;
    }
    streaming = streaming && co_await StartReplication();
    co_await RunJob(Job::Stream);
    if (streaming) {
      auto feedback = FeedbackLoop();
      co_await Feeder();
      this->Stop();
      _feedback_timer->cancel();
      co_await std::move(feedback);
    }
    _stream.Finish();
    this->Stop();
    co_await std::move(cpu);
  }
  if (const auto& error = LastError(); !error.errmsg.empty()) {
    SDB_WARN(
      REPLICATION, "subscription '", _target.subscription_name,
      "': ", error.errmsg,
      error.errdetail.empty() ? "" : absl::StrCat(" (", error.errdetail, ")"),
      error.context.empty() ? "" : absl::StrCat("; ", error.context));
  }
  this->Stop();
  co_await std::move(writer);
  co_return {};
}

yaclib::Task<bool> PgReplicationClient::RunJob(Job job) {
  _job_done.Reset();
  _job.store(job, std::memory_order_release);
  this->_task->RequestRun();
  if (job == Job::Stream) {
    co_return true;
  }
  co_await _job_done.AwaitOn(*this->_ioexec);
  co_return _job_ok;
}

yaclib::Task<bool> PgReplicationClient::SyncTables() {
  _sync_tables.clear();
  for (const auto& relation : _target.relations) {
    if (relation.state != 'r') {
      _sync_tables.push_back(
        {.schema = relation.schema, .table = relation.table});
    }
  }
  if (_sync_tables.empty()) {
    co_return true;
  }
  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' synchronizing ", _sync_tables.size(), " table(s)");
  const auto slot =
    absl::StrCat(_target.slot_name, "_sync_", _target.subscription_oid);
  std::vector<PublisherRow> rows;
  if (!co_await Query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ") ||
      !co_await Query(
        absl::StrCat("CREATE_REPLICATION_SLOT ", pg::QuoteIdentifier(slot),
                     " TEMPORARY LOGICAL pgoutput (SNAPSHOT "
                     "'use')"),
        &rows) ||
      rows.empty() || rows.front().size() < 2 || !rows.front()[1]) {
    co_return false;
  }
  const auto consistent_point = pg::ParseLsn(*rows.front()[1]);
  if (!consistent_point) {
    Fail(ERRCODE_PROTOCOL_VIOLATION, "invalid replication slot LSN");
    co_return false;
  }
  rows.clear();
  if (!co_await Query(
        absl::StrCat(
          "SELECT n.nspname, c.relname, a.attname, "
          "pg_get_expr(gpt.qual, gpt.relid), p.pubname, c.relkind, "
          "a.attgenerated <> '' "
          "FROM pg_publication p "
          "JOIN LATERAL pg_get_publication_tables(p.pubname) gpt ON true "
          "JOIN pg_class c ON c.oid = gpt.relid "
          "JOIN pg_namespace n ON n.oid = c.relnamespace "
          "JOIN unnest(gpt.attrs) WITH ORDINALITY AS pa(attnum, ord) ON true "
          "JOIN pg_attribute a ON a.attrelid = gpt.relid AND "
          "a.attnum = pa.attnum "
          "WHERE p.pubname IN (",
          PublicationList(_target.publications),
          ") ORDER BY n.nspname, c.relname, p.pubname, pa.ord"),
        &rows)) {
    co_return false;
  }
  irs::containers::FlatHashMap<std::string, size_t> index;
  for (size_t i = 0; i < _sync_tables.size(); ++i) {
    index.emplace(
      absl::StrCat(_sync_tables[i].schema, ".", _sync_tables[i].table), i);
  }
  std::vector<std::string> first_publication(_sync_tables.size());
  std::vector<bool> unfiltered(_sync_tables.size(), false);
  std::vector<std::vector<std::string>> filters(_sync_tables.size());
  for (const auto& row : rows) {
    if (row.size() < 5 || !row[0] || !row[1] || !row[2] || !row[4]) {
      continue;
    }
    const auto it = index.find(absl::StrCat(*row[0], ".", *row[1]));
    if (it == index.end()) {
      continue;
    }
    auto& table = _sync_tables[it->second];
    auto& first = first_publication[it->second];
    if (first.empty()) {
      first = *row[4];
      table.partitioned = row.size() > 5 && row[5] && *row[5] == "p";
    }
    if (*row[4] == first) {
      table.columns.push_back(*row[2]);
      table.generated |= row.size() > 6 && row[6] && *row[6] == "t";
    }
    if (!row[3]) {
      unfiltered[it->second] = true;
    } else if (!std::ranges::contains(filters[it->second], *row[3])) {
      filters[it->second].push_back(*row[3]);
    }
  }
  for (size_t i = 0; i < _sync_tables.size(); ++i) {
    if (!unfiltered[i] && !filters[i].empty()) {
      _sync_tables[i].row_filter = absl::StrJoin(
        filters[i], " OR ", [](std::string* out, const std::string& filter) {
          absl::StrAppend(out, "(", filter, ")");
        });
    }
  }
  if (!co_await RunJob(Job::Begin)) {
    co_return false;
  }
  const bool binary = _target.binary && ServerVersion() >= 16;
  bool ok = true;
  for (const auto i : _sync_order) {
    _sync_current = i;
    if (!co_await CopyTable(_sync_tables[i], binary)) {
      ok = false;
      break;
    }
  }
  if (!ok) {
    co_await RunJob(Job::Rollback);
    co_return false;
  }
  _sync_lsn = *consistent_point;
  if (!co_await RunJob(Job::Commit)) {
    co_return false;
  }
  co_return co_await Query("COMMIT") &&
    co_await Query(
      absl::StrCat("DROP_REPLICATION_SLOT ", pg::QuoteIdentifier(slot)));
}

yaclib::Task<bool> PgReplicationClient::CopyTable(const SyncTable& table,
                                                  bool binary) {
  std::string columns = absl::StrJoin(
    table.columns, ", ", [](std::string* out, const std::string& column) {
      out->append(pg::QuoteIdentifier(column));
    });
  const auto name = absl::StrCat(pg::QuoteIdentifier(table.schema), ".",
                                 pg::QuoteIdentifier(table.table));
  std::string query;
  if (table.row_filter || table.partitioned || table.generated) {
    query = absl::StrCat("COPY (SELECT ", columns, " FROM ",
                         table.partitioned ? "" : "ONLY ", name);
    if (table.row_filter) {
      absl::StrAppend(&query, " WHERE ", *table.row_filter);
    }
    query.append(") TO STDOUT");
  } else {
    query = absl::StrCat("COPY ", name, " (", columns, ") TO STDOUT");
  }
  if (binary) {
    query.append(" WITH (FORMAT binary)");
  }
  SendQuery(query);
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           "server closed the connection unexpectedly");
      co_return false;
    }
    const char type = frame.type;
    if (type == PQ_MSG_ERROR_RESPONSE) {
      Fail(network::pg::ParseErrorResponse(frame.payload));
      this->_frames.Consume(frame);
      co_await ReadUntilReady(nullptr);
      co_return false;
    }
    this->_frames.Consume(frame);
    if (type == PQ_MSG_COPY_OUT_RESPONSE) {
      break;
    }
  }
  sdb::pg::CopyInBridge bridge;
  this->_connection_ctx->SetSideChannel(&bridge);
  _copy_stmt =
    BuildCopyFromStdin(table.schema, table.table, table.columns, binary);
  _job_done.Reset();
  _job.store(Job::Copy, std::memory_order_release);
  this->_task->RequestRun();
  co_await this->RunCopyInFeeder(bridge, binary
                                           ? network::pg::CopyFormat::Binary
                                           : network::pg::CopyFormat::Text);
  co_await _job_done.AwaitOn(*this->_ioexec);
  this->_connection_ctx->SetSideChannel(&_batch);
  const bool copied = _job_ok;
  co_return co_await ReadUntilReady(nullptr) && copied;
}

yaclib::Task<bool> PgReplicationClient::StartReplication() {
  const bool streaming = _target.streaming && ServerVersion() >= 14;
  auto command = absl::StrCat(
    "START_REPLICATION SLOT ", pg::QuoteIdentifier(_target.slot_name),
    " LOGICAL ", pg::FormatLsn(_target.start_lsn), " (proto_version '",
    streaming ? "2" : "1", "', publication_names ",
    pg::QuoteLiteral(
      absl::StrJoin(_target.publications, ",",
                    [](std::string* out, const std::string& publication) {
                      out->append(pg::QuoteIdentifier(publication));
                    })));
  if (_target.binary && ServerVersion() >= 14) {
    command.append(", binary 'true'");
  }
  if (streaming) {
    command.append(", streaming 'on'");
  }
  if (ServerVersion() >= 16) {
    absl::StrAppend(&command, ", origin ", pg::QuoteLiteral(_target.origin));
  }
  command.append(")");
  SendQuery(command);
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           "server closed the connection unexpectedly");
      co_return false;
    }
    const char type = frame.type;
    if (type == PQ_MSG_ERROR_RESPONSE) {
      Fail(network::pg::ParseErrorResponse(frame.payload));
    }
    this->_frames.Consume(frame);
    if (type == PQ_MSG_COPY_BOTH_RESPONSE) {
      SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
               "' streaming from ", pg::FormatLsn(_target.start_lsn));
      co_return true;
    }
    if (type == PQ_MSG_READY_FOR_QUERY) {
      co_return false;
    }
  }
}

yaclib::Task<bool> PgReplicationClient::Publish(
  std::span<const PgOutputMessage> messages) {
  if (!_stream.Publish(messages)) {
    co_return false;
  }
  _publishing = true;
  _published = true;
  co_await _stream.Drained(*this->_ioexec);
  _stream.ResetDrained();
  _publishing = false;
  _last_activity = SteadyMicros();
  co_return true;
}

bool PgReplicationClient::Stage(std::string_view payload, bool in_stream) {
  if (_outbox.size() >= kOutboxMessages ||
      _arena.size() + payload.size() > _arena.capacity()) {
    return false;
  }
  const auto offset = _arena.size();
  _arena.append(payload);
  _outbox.push_back(
    DecodePgOutput({_arena.data() + offset, payload.size()}, in_stream));
  return true;
}

yaclib::Task<bool> PgReplicationClient::Flush() {
  if (_outbox.empty()) {
    co_return true;
  }
  const bool ok = co_await Publish(_outbox);
  _outbox.clear();
  _arena.clear();
  co_return ok;
}

yaclib::Task<bool> PgReplicationClient::Enqueue(std::string_view payload,
                                                bool in_stream) {
  if (Stage(payload, in_stream)) {
    co_return true;
  }
  if (!co_await Flush()) {
    co_return false;
  }
  if (Stage(payload, in_stream)) {
    co_return true;
  }
  const auto message = DecodePgOutput(payload, in_stream);
  co_return co_await Publish({&message, 1});
}

yaclib::Task<bool> PgReplicationClient::Enqueue(PgOutputMessage message) {
  if (_outbox.size() >= kOutboxMessages && !co_await Flush()) {
    co_return false;
  }
  _outbox.push_back(std::move(message));
  co_return true;
}

yaclib::Task<bool> PgReplicationClient::ReplayStream(
  uint32_t xid, const StreamCommitMessage& commit) {
  if (!co_await Flush() ||
      !co_await Enqueue(BeginMessage{.final_lsn = commit.commit_lsn,
                                     .commit_time = commit.commit_time,
                                     .xid = xid})) {
    co_return false;
  }
  if (auto spool = _spools.find(xid); spool != _spools.end()) {
    auto reader = spool->second.Read();
    std::string_view payload;
    while (reader.Next(payload)) {
      if (!co_await Enqueue(payload, true)) {
        co_return false;
      }
    }
    if (!co_await Flush()) {
      co_return false;
    }
    _spools.erase(spool);
  }
  _subxacts.erase(xid);
  co_return co_await Enqueue(CommitMessage{.commit_lsn = commit.commit_lsn,
                                           .end_lsn = commit.end_lsn,
                                           .commit_time = commit.commit_time});
}

yaclib::Task<> PgReplicationClient::Feeder() {
  auto& buffers = duckdb::BufferManager::GetBufferManager(
    irs::DuckDBEngine::Instance().instance());
  for (;;) {
    if (this->SendBroken() || _stream.Aborted()) {
      break;
    }
    if (this->_frames.TryAssemble(FrameKind::Typed, this->_max_message)
          .status == FrameStatus::NeedMore) {
      if (!co_await Flush()) {
        break;
      }
      if (_published) {
        _published = false;
        if (!co_await Publish({ReplStream::Idle(), 1})) {
          break;
        }
        _published = false;
      }
    }
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      break;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type == PQ_MSG_ERROR_RESPONSE) {
      Fail(network::pg::ParseErrorResponse(payload));
      this->_frames.Consume(frame);
      break;
    }
    if (type != PQ_MSG_COPY_DATA || payload.empty()) {
      this->_frames.Consume(frame);
      if (type == PQ_MSG_COPY_DONE) {
        break;
      }
      continue;
    }
    _last_receipt_time.store(NowMicros(), std::memory_order_relaxed);
    _last_activity = SteadyMicros();
    const auto record_lsn = [&](uint64_t lsn) {
      if (lsn > _received_lsn.load(std::memory_order_relaxed)) {
        _received_lsn.store(lsn, std::memory_order_relaxed);
      }
    };
    if (payload[0] == 'k' && payload.size() >= 18) {
      record_lsn(absl::big_endian::Load64(payload.data() + 1));
      _last_send_time.store(
        static_cast<int64_t>(absl::big_endian::Load64(payload.data() + 9)) +
          kPgEpochMicros,
        std::memory_order_relaxed);
      const bool reply = payload[17] != 0;
      this->_frames.Consume(frame);
      if (reply) {
        SendFeedback(false);
      }
      continue;
    }
    if (payload[0] != 'w' || payload.size() <= 25) {
      this->_frames.Consume(frame);
      continue;
    }
    record_lsn(std::max(absl::big_endian::Load64(payload.data() + 1),
                        absl::big_endian::Load64(payload.data() + 9)));
    _last_send_time.store(
      static_cast<int64_t>(absl::big_endian::Load64(payload.data() + 17)) +
        kPgEpochMicros,
      std::memory_order_relaxed);
    const auto message = payload.substr(25);
    bool ok = true;
    try {
      switch (message.front()) {
        case 'S': {
          const auto start =
            std::get<StreamStartMessage>(DecodePgOutput(message));
          _streamed_xid = start.xid;
          _spools.try_emplace(start.xid, buffers);
          break;
        }
        case 'E':
          _streamed_xid.reset();
          break;
        case 'c': {
          const auto commit =
            std::get<StreamCommitMessage>(DecodePgOutput(message));
          ok = co_await ReplayStream(commit.xid, commit);
          break;
        }
        case 'A': {
          const auto abort =
            std::get<StreamAbortMessage>(DecodePgOutput(message));
          if (abort.xid == abort.subxid) {
            _spools.erase(abort.xid);
            _subxacts.erase(abort.xid);
            break;
          }
          auto& subxacts = _subxacts[abort.xid];
          const auto it = std::ranges::find(
            subxacts, abort.subxid, &std::pair<uint32_t, size_t>::first);
          if (it != subxacts.end()) {
            if (auto spool = _spools.find(abort.xid); spool != _spools.end()) {
              spool->second.Truncate(it->second);
            }
            subxacts.erase(it, subxacts.end());
          }
          break;
        }
        default:
          if (_streamed_xid) {
            auto& spool = _spools.at(*_streamed_xid);
            const auto xid = StreamedMessageXid(message);
            if (xid != *_streamed_xid) {
              auto& subxacts = _subxacts[*_streamed_xid];
              if (!std::ranges::contains(subxacts, xid,
                                         &std::pair<uint32_t, size_t>::first)) {
                subxacts.emplace_back(xid, spool.Size());
              }
            }
            spool.Append(message);
          } else {
            ok = co_await Enqueue(message, false);
          }
          break;
      }
    } catch (const std::exception& ex) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' cannot process message: ", ex.what());
      ok = false;
    }
    this->_frames.Consume(frame);
    if (!ok) {
      break;
    }
  }
  _stream.Finish();
  co_return {};
}

void PgReplicationClient::SendFeedback(bool reply) {
  if (this->SendBroken()) {
    return;
  }
  const auto received = _received_lsn.load(std::memory_order_relaxed);
  auto flushed = _flushed_lsn.load(std::memory_order_relaxed);
  if (!_publishing && _outbox.empty() &&
      _apply_idle.load(std::memory_order_acquire) && !_streamed_xid &&
      _spools.empty() && received > flushed) {
    flushed = received;
  }
  std::array<char, 34> body{};
  body[0] = 'r';
  absl::big_endian::Store64(body.data() + 1, received);
  absl::big_endian::Store64(body.data() + 9, flushed);
  absl::big_endian::Store64(body.data() + 17, flushed);
  absl::big_endian::Store64(
    body.data() + 25, static_cast<uint64_t>(NowMicros() - kPgEpochMicros));
  body[33] = reply ? 1 : 0;
  network::pg::WriteCopyData(this->_send, {body.data(), body.size()});
  this->KickSend();
  _latest_end_time.store(NowMicros(), std::memory_order_relaxed);
}

yaclib::Future<> PgReplicationClient::FeedbackLoop() {
  auto& timer = _feedback_timer.emplace(this->_io);
  _last_activity = SteadyMicros();
  auto pinged_at = _last_activity;
  auto reported_at = _last_activity;
  while (!this->SendBroken()) {
    timer.expires_after(kFeedbackTick);
    co_await network::Async<void>([&](auto&& handler) {
      timer.async_wait(std::forward<decltype(handler)>(handler));
    }).NoThrow();
    if (this->SendBroken()) {
      break;
    }
    const auto now = SteadyMicros();
    const auto timeout = WalReceiverTimeoutMillis() * 1000;
    const auto silent = _publishing ? 0 : now - _last_activity;
    if (timeout > 0 && silent >= timeout) {
      Fail(ERRCODE_CONNECTION_FAILURE,
           "terminating logical replication worker due to timeout");
      this->Stop();
      break;
    }
    const bool ping =
      timeout > 0 && silent >= timeout / 2 && pinged_at != _last_activity;
    if (ping) {
      pinged_at = _last_activity;
    }
    const auto interval = WalReceiverStatusIntervalMillis() * 1000;
    if (ping || (interval > 0 && now - reported_at >= interval)) {
      reported_at = now;
      SendFeedback(ping);
    }
  }
  co_return {};
}

yaclib::Future<> PgReplicationClient::ReplicationMain() {
  co_await this->_task->Park();
  absl::Cleanup finish = [this] {
    if (_in_txn) {
      this->_txn_state->Rollback();
      _in_txn = false;
    }
    _job_ok = false;
    _job_done.Set();
    this->_task->Finish();
  };
  try {
    _setup_ok = SetupApplyConnection();
  } catch (const std::exception& ex) {
    ApplyFailed(network::pg::ToSqlError(ex));
  }
  _setup_done.Set();
  if (!_setup_ok) {
    _stream.Abort();
    this->Stop();
    co_return {};
  }
  for (;;) {
    auto job = _job.exchange(Job::None, std::memory_order_acq_rel);
    if (job == Job::None) {
      if (this->SendBroken()) {
        _stream.Abort();
        co_return {};
      }
      co_await this->_task->Park();
      continue;
    }
    if (job == Job::Stream) {
      break;
    }
    _job_ok = co_await RunDuckJob(job);
    _job_done.Set();
  }
  uint64_t reported = _flushed_lsn.load(std::memory_order_relaxed);
  for (;;) {
    while (!_stream.Ready()) {
      if (this->SendBroken()) {
        break;
      }
      if (!_in_txn) {
        _apply_idle.store(true, std::memory_order_release);
        const auto flushed = _flushed_lsn.load(std::memory_order_relaxed);
        if (flushed != reported) {
          reported = flushed;
          asio_ns::post(this->_io, [self = this->shared_from_this(), this] {
            SendFeedback(false);
          });
        }
      }
      co_await this->_task->Park();
    }
    if (this->SendBroken()) {
      _stream.Abort();
      break;
    }
    _apply_idle.store(false, std::memory_order_release);
    bool ok = false;
    try {
      const auto* message = _stream.Current();
      if (message == nullptr) {
        if (!_remote_txn) {
          FlushCommit();
        }
        break;
      }
      if (ReplStream::IsIdle(message)) {
        ok = _remote_txn || FlushCommit();
        _stream.Advance();
      } else {
        if (const auto fresh = _stream.TakeFresh(); Reorder(fresh)) {
          _stream.Replace(_reordered);
          message = _stream.Current();
        }
        ok = co_await ApplyMessage(*message);
      }
    } catch (const std::exception& ex) {
      ApplyFailed(network::pg::ToSqlError(ex));
    }
    if (!ok) {
      _stream.Abort();
      break;
    }
  }
  this->Stop();
  co_return {};
}

bool PgReplicationClient::SetupApplyConnection() {
  if (_target.database_oid == 0) {
    return false;
  }
  const std::string_view user =
    _target.owner_name.empty()
      ? std::string_view{irs::StaticStrings::kDefaultUser}
      : std::string_view{_target.owner_name};
  const duckdb::idx_t role =
    _target.owner_name.empty() ? pg::kRootUser : _target.owner_id;
  this->_conn = irs::DuckDBEngine::Instance().CreateConnection();
  this->_txn_state.emplace(this->_conn->context->transaction);
  this->_connection_ctx = std::make_shared<ConnectionContext>(
    *this->_conn->context, user, role, _target.database_name,
    _target.database_oid, nullptr, 0, nullptr);
  this->_client_state = &connector::SereneDBClientState::Register(
    *this->_conn->context, this->_connection_ctx);
  _batch.stream = &_stream;
  _batch.pass_through = [this](const PgOutputMessage& message) {
    return PassThrough(message);
  };
  this->_connection_ctx->SetSideChannel(&_batch);
  this->_conn->context->session_user.assign(user);
  connector::SetDefaultSearchPath(*this->_conn->context, _target.database_name);
  return true;
}

void PgReplicationClient::ApplyFailed(irs::pg::SqlErrorData error) {
  if (!_apply_error.errmsg.empty()) {
    return;
  }
  if (error.errcode == ERRCODE_T_R_SERIALIZATION_FAILURE) {
    _transient.store(true, std::memory_order_release);
  } else if (_target.disable_on_error) {
    _disable_requested.store(true, std::memory_order_release);
  }
  _apply_error = std::move(error);
}

yaclib::Task<bool> PgReplicationClient::RunDuckJob(Job job) {
  try {
    switch (job) {
      case Job::Begin:
        co_return BeginSync();
      case Job::Copy:
        co_return co_await RunLocalCopy();
      case Job::Commit:
        co_return CommitSync();
      case Job::Rollback:
        if (_in_txn) {
          this->_txn_state->Rollback();
          _in_txn = false;
        }
        co_return true;
      case Job::None:
      case Job::Stream:
        break;
    }
  } catch (const std::exception& ex) {
    ApplyFailed(network::pg::ToSqlError(ex));
  }
  co_return false;
}

bool PgReplicationClient::BeginSync() {
  this->_txn_state->Arm();
  _in_txn = true;
  std::vector<duckdb::optional_ptr<duckdb::TableCatalogEntry>> locals;
  locals.reserve(_sync_tables.size());
  for (auto& table : _sync_tables) {
    auto local = LookupTable(table.schema, table.table);
    if (!local) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ERR_MSG("logical replication target relation \"", table.schema, ".",
                table.table, "\" does not exist"));
    }
    std::vector<std::string> missing;
    std::vector<std::string> generated;
    for (const auto& name : table.columns) {
      const duckdb::ColumnDefinition* found = nullptr;
      for (const auto& column : local->GetColumns().Logical()) {
        if (column.Name().GetIdentifierName() == name) {
          found = &column;
          break;
        }
      }
      if (found == nullptr) {
        missing.push_back(name);
      } else if (found->Generated()) {
        generated.push_back(name);
      }
    }
    CheckTargetColumns(table.schema, table.table, missing, generated);
    table.owner = local->permissions.owner;
    locals.push_back(local);
  }
  _sync_order = ReferencedFirstOrder(locals);
  return true;
}

yaclib::Task<bool> PgReplicationClient::RunLocalCopy() {
  this->_client_state->copy_stdin_open_count = 0;
  this->_client_state->copy_stdin_done = false;
  auto* bridge = this->_connection_ctx->GetSideChannel<sdb::pg::CopyInBridge>();
  bool ok = false;
  try {
    const auto& table = _sync_tables[_sync_current];
    UseRole(table.owner);
    auto prepared = this->_conn->Prepare(std::move(_copy_stmt));
    if (prepared->HasError()) {
      prepared->GetErrorObject().Throw();
    }
    ok = (co_await RunPrepared(*prepared)).has_value();
  } catch (const std::exception& ex) {
    ApplyFailed(network::pg::ToSqlError(ex));
  }
  if (!ok && bridge != nullptr) {
    bridge->Abort();
  }
  co_return ok;
}

bool PgReplicationClient::CommitSync() {
  UpdateDefinition([&](duckdb::CreateSubscriptionInfo& definition) {
    for (auto& relation : definition.relations) {
      if (std::ranges::any_of(_sync_tables, [&](const SyncTable& table) {
            return table.schema == relation.schema &&
                   table.table == relation.table;
          })) {
        relation.state = 'r';
        relation.lsn = _sync_lsn;
      }
    }
  });
  _in_txn = false;
  if (auto error = this->_txn_state->Commit()) {
    ApplyFailed(std::move(*error));
    return false;
  }
  for (auto& relation : _target.relations) {
    if (relation.state != 'r') {
      relation.state = 'r';
      relation.lsn = _sync_lsn;
    }
  }
  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' synchronized ", _sync_tables.size(), " table(s) at ",
           pg::FormatLsn(_sync_lsn));
  return true;
}

void PgReplicationClient::UpdateDefinition(
  absl::FunctionRef<void(duckdb::CreateSubscriptionInfo&)> edit) {
  auto& context = *this->_conn->context;
  auto& catalog = duckdb::Catalog::GetCatalog(
                    context, duckdb::Identifier{_target.database_name})
                    .Cast<catalog::SereneDBCatalog>();
  const auto transaction = catalog.GetCatalogTransaction(context);
  auto entry = catalog.GetOidIndex().GetVisible(_target.subscription_oid,
                                                transaction.view);
  if (!entry || entry->type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("subscription \"", _target.subscription_name,
                            "\" does not exist"));
  }
  auto& current = entry->Cast<catalog::SubscriptionCatalogEntry>();
  auto definition =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateSubscriptionInfo>(
      current.GetInfo());
  edit(*definition);
  duckdb::ReplaceDefinitionInfo alter{std::move(definition)};
  alter.SetQualifiedName(duckdb::QualifiedName(current.name));
  catalog.Alter(transaction, alter);
}

duckdb::optional_ptr<duckdb::TableCatalogEntry>
PgReplicationClient::LookupTable(std::string_view schema,
                                 std::string_view table) {
  return duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    *this->_conn->context,
    duckdb::QualifiedName::FromCatalogSchema(
      duckdb::Identifier{_target.database_name}, {duckdb::Identifier{schema}},
      duckdb::Identifier{table}),
    duckdb::OnEntryNotFound::RETURN_NULL);
}

yaclib::Task<std::optional<int64_t>> PgReplicationClient::RunPrepared(
  duckdb::PreparedStatement& prepared) {
  network::pg::ClosingPending pending;
  duckdb::vector<duckdb::Value> values;
  auto result =
    co_await this->DriveToResult(prepared, values, pending, nullptr);
  if (!result) {
    co_return std::nullopt;
  }
  if (result->HasError()) {
    result->ThrowError();
  }
  auto chunk = result->Fetch();
  if (!chunk || chunk->size() == 0 || chunk->ColumnCount() == 0) {
    co_return 0;
  }
  co_return chunk->GetValue(0, 0).GetValue<int64_t>();
}

void PgReplicationClient::UseRole(duckdb::idx_t table_owner) {
  auto role = _target.owner_id;
  if (!_target.run_as_owner && table_owner != role &&
      table_owner != pg::kInvalidOid) {
    auto& context = *this->_conn->context;
    if (!auth::ClosureFor(&context, role)->is_superuser &&
        !auth::ClosureFor(&context, role)->CanSet(table_owner)) {
      const auto roles = auth::RolesOf(&context);
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("role \"", roles->NameOf(role), "\" cannot SET ROLE to \"",
                roles->NameOf(table_owner), "\""));
    }
    role = table_owner;
  }
  this->_connection_ctx->SetEffectiveRole(role);
}

const RelInfo* PgReplicationClient::Relation(uint32_t relation_id) const {
  auto it = _relations.find(relation_id);
  return it == _relations.end() ? nullptr : &it->second;
}

void PgReplicationClient::OnRelation(const RelationMessage& message) {
  RelInfo info;
  info.schema =
    message.namespace_name.empty() ? "pg_catalog" : message.namespace_name;
  info.table = message.relation_name;
  for (const auto& relation : _target.relations) {
    if (relation.schema == info.schema && relation.table == info.table) {
      info.ready = relation.state == 'r';
      info.sync_lsn = relation.lsn;
      break;
    }
  }
  this->_conn->context->RunFunctionInTransaction([&] {
    auto table = LookupTable(info.schema, info.table);
    if (!table) {
      return;
    }
    info.mapped = true;
    info.table_oid = table->oid;
    info.owner = table->permissions.owner;
    info.foreign_keys =
      std::ranges::any_of(table->GetConstraints(), [](const auto& constraint) {
        return constraint->type == duckdb::ConstraintType::FOREIGN_KEY;
      });
    info.columns.reserve(message.columns.size());
    for (const auto& remote : message.columns) {
      RelColumn column{.name = remote.name, .is_key = remote.is_key};
      bool found = false;
      for (const auto& local : table->GetColumns().Logical()) {
        if (local.Name().GetIdentifierName() != remote.name) {
          continue;
        }
        found = true;
        if (local.Generated()) {
          info.generated_columns.push_back(remote.name);
        } else {
          column.type = local.Type();
        }
        break;
      }
      if (!found) {
        info.missing_columns.push_back(remote.name);
      }
      info.columns.push_back(std::move(column));
    }
  });
  _relations[message.relation_id] = std::move(info);
}

void PgReplicationClient::CheckRelation(const RelInfo& relation) const {
  if (!relation.mapped) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("logical replication target relation \"", relation.schema, ".",
              relation.table, "\" does not exist"));
  }
  CheckTargetColumns(relation.schema, relation.table, relation.missing_columns,
                     relation.generated_columns);
}

bool PgReplicationClient::ApplyChanges(const RelInfo& relation) const {
  return relation.ready && _final_lsn >= relation.sync_lsn;
}

yaclib::Task<bool> PgReplicationClient::ApplyMessage(
  const PgOutputMessage& message) {
  if (const auto* begin = std::get_if<BeginMessage>(&message)) {
    BeginTxn(*begin);
    _stream.Advance();
    co_return true;
  }
  if (const auto* commit = std::get_if<CommitMessage>(&message)) {
    const bool ok = CommitTxn(*commit);
    _stream.Advance();
    co_return ok;
  }
  if (const auto* relation = std::get_if<RelationMessage>(&message)) {
    OnRelation(*relation);
    _stream.Advance();
    co_return true;
  }
  if (_skipping) {
    _stream.Advance();
    co_return true;
  }
  if (const auto* truncate = std::get_if<TruncateMessage>(&message)) {
    co_await ApplyTruncate(*truncate);
    _stream.Advance();
    co_return true;
  }
  const auto relation_id = RowRelId(message);
  if (!relation_id) {
    _stream.Advance();
    co_return true;
  }
  const auto* relation = Relation(*relation_id);
  if (relation == nullptr) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
      ERR_MSG("no relation map entry for remote relation ID ", *relation_id));
  }
  if (!ApplyChanges(*relation)) {
    _stream.Advance();
    co_return true;
  }
  CheckRelation(*relation);
  std::vector<PgColumn> cells;
  _batch.keys.clear();
  _batch.cols.clear();
  if (!RowShape(message, *relation, _batch.op, _batch.keys, _batch.cols, cells,
                _batch.full)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
      ERR_MSG("invalid logical replication message: malformed tuple for "
              "relation \"",
              relation->schema, ".", relation->table, "\""));
  }
  _batch.relid = *relation_id;
  _batch.rel = relation;
  UseRole(relation->owner);
  co_return co_await RunBatch();
}

void PgReplicationClient::BeginTxn(const BeginMessage& message) {
  _remote_txn = true;
  _final_lsn = message.final_lsn;
  _xid = message.xid;
  _skipping = _target.skip_lsn != 0 && message.final_lsn == _target.skip_lsn;
  if (_skipping) {
    SDB_INFO(REPLICATION,
             "logical replication starts skipping transaction "
             "at LSN ",
             pg::FormatLsn(message.final_lsn));
  }
  if (!_in_txn) {
    this->_txn_state->Arm();
    _in_txn = true;
  }
}

void PgReplicationClient::PushRemoteLsn(uint64_t end_lsn) {
  auto& context = *this->_conn->context;
  if (!context.transaction.HasActiveTransaction()) {
    return;
  }
  auto database = duckdb::DatabaseManager::Get(context).GetDatabase(
    context, duckdb::Identifier{_target.database_name});
  if (!database) {
    return;
  }
  auto& catalog = database->GetCatalog().Cast<duckdb::DuckCatalog>();
  auto& transaction = duckdb::DuckTransaction::Get(context, *database);
  auto entry = catalog.GetOidIndex().GetVisible(_target.subscription_oid,
                                                transaction.GetSnapshotView());
  if (!entry || entry->type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
    return;
  }
  transaction.PushSubscriptionLsn(
    entry->Cast<duckdb::SubscriptionCatalogEntry>(), end_lsn);
}

bool PgReplicationClient::CommitTxn(const CommitMessage& message) {
  _remote_txn = false;
  const bool skipped = _skipping;
  _skipping = false;
  if (skipped) {
    UpdateDefinition([](duckdb::CreateSubscriptionInfo& definition) {
      definition.skip_lsn = 0;
    });
    _target.skip_lsn = 0;
    SDB_INFO(REPLICATION,
             "logical replication completed skipping transaction at LSN ",
             pg::FormatLsn(_final_lsn));
  }
  GroupCommit(message);
  if (_hold_commits != 0 && --_hold_commits != 0) {
    return true;
  }
  if (_pending_txns >= kGroupTxns ||
      SteadyMicros() - _pending_since >= kGroupMicros) {
    return FlushCommit();
  }
  return true;
}

void PgReplicationClient::GroupCommit(const CommitMessage& message) {
  if (_in_txn) {
    PushRemoteLsn(message.end_lsn);
  }
  if (_pending_txns++ == 0) {
    _pending_since = SteadyMicros();
  }
  _pending_lsn = std::max(_pending_lsn, message.end_lsn);
}

bool PgReplicationClient::PassThrough(const PgOutputMessage& message) {
  if (_hold_commits != 0) {
    return false;
  }
  if (const auto* commit = std::get_if<CommitMessage>(&message)) {
    if (_pending_txns + 1 >= kGroupTxns ||
        (_pending_txns != 0 &&
         SteadyMicros() - _pending_since >= kGroupMicros)) {
      return false;
    }
    _remote_txn = false;
    GroupCommit(*commit);
    return true;
  }
  const auto& begin = std::get<BeginMessage>(message);
  if (_target.skip_lsn != 0 && begin.final_lsn == _target.skip_lsn) {
    return false;
  }
  _remote_txn = true;
  _final_lsn = begin.final_lsn;
  _xid = begin.xid;
  return true;
}

bool PgReplicationClient::Reorder(std::span<const PgOutputMessage> messages) {
  if (_skipping) {
    return false;
  }
  size_t prefix = messages.size();
  while (prefix != 0 &&
         !std::holds_alternative<CommitMessage>(messages[prefix - 1])) {
    --prefix;
  }
  if (prefix < 3) {
    return false;
  }
  size_t commits = 0;
  irs::containers::FlatHashMap<uint32_t, size_t> groups;
  uint64_t first_lsn = _remote_txn ? _final_lsn : UINT64_MAX;
  for (size_t i = 0; i < prefix; ++i) {
    const auto& message = messages[i];
    if (const auto* begin = std::get_if<BeginMessage>(&message)) {
      if (_target.skip_lsn != 0 && begin->final_lsn == _target.skip_lsn) {
        return false;
      }
      first_lsn = std::min(first_lsn, begin->final_lsn);
      continue;
    }
    if (std::holds_alternative<CommitMessage>(message)) {
      ++commits;
      continue;
    }
    const auto relation_id = RowRelId(message);
    if (!relation_id) {
      return false;
    }
    const auto* relation = Relation(*relation_id);
    if (relation == nullptr || !relation->mapped || relation->foreign_keys ||
        !relation->ready || first_lsn < relation->sync_lsn ||
        !relation->missing_columns.empty() ||
        !relation->generated_columns.empty()) {
      return false;
    }
    groups.try_emplace(*relation_id, groups.size());
  }
  if (groups.size() < 2) {
    return false;
  }
  std::vector<std::vector<size_t>> order(groups.size());
  for (size_t i = 0; i < prefix; ++i) {
    if (const auto relation_id = RowRelId(messages[i])) {
      order[groups.at(*relation_id)].push_back(i);
    }
  }
  const bool leading_begin = std::holds_alternative<BeginMessage>(messages[0]);
  _reordered.clear();
  _reordered.reserve(messages.size());
  if (leading_begin) {
    _reordered.push_back(messages[0]);
  }
  for (const auto& group : order) {
    for (const auto i : group) {
      _reordered.push_back(messages[i]);
    }
  }
  for (size_t i = leading_begin ? 1 : 0; i < prefix; ++i) {
    if (!RowRelId(messages[i])) {
      _reordered.push_back(messages[i]);
    }
  }
  _reordered.insert(_reordered.end(), messages.begin() + prefix,
                    messages.end());
  _hold_commits = commits;
  return true;
}

bool PgReplicationClient::FlushCommit() {
  if (_pending_txns == 0) {
    return true;
  }
  _pending_txns = 0;
  if (_in_txn) {
    _in_txn = false;
    if (auto error = this->_txn_state->Commit()) {
      ApplyFailed(std::move(*error));
      return false;
    }
  }
  if (_pending_lsn > _flushed_lsn.load(std::memory_order_relaxed)) {
    _flushed_lsn.store(_pending_lsn, std::memory_order_relaxed);
  }
  return true;
}

yaclib::Task<> PgReplicationClient::ApplyTruncate(
  const TruncateMessage& message) {
  auto& context = *this->_conn->context;
  std::vector<duckdb::reference<duckdb::TableCatalogEntry>> tables;
  std::vector<duckdb::idx_t> owners;
  const auto add = [&](duckdb::TableCatalogEntry& table, duckdb::idx_t owner) {
    if (std::ranges::none_of(tables, [&](const auto& member) {
          return member.get().oid == table.oid;
        })) {
      tables.emplace_back(table);
      owners.push_back(owner);
    }
  };
  std::vector<std::pair<std::string, std::string>> names;
  std::vector<duckdb::QualifiedName> sequences;
  context.RunFunctionInTransaction([&] {
    for (const auto relation_id : message.relation_ids) {
      const auto* relation = Relation(relation_id);
      if (relation == nullptr || !ApplyChanges(*relation)) {
        continue;
      }
      CheckRelation(*relation);
      if (auto table = LookupTable(relation->schema, relation->table)) {
        add(*table, relation->owner);
      }
    }
    for (size_t i = 0; message.cascade && i < tables.size(); ++i) {
      for (auto& referencing :
           duckdb::Binder::TruncateReferencingTables(context, tables[i])) {
        add(referencing, referencing.get().permissions.owner);
      }
    }
    for (auto& table : tables) {
      names.emplace_back(table.get().ParentSchemaName().GetIdentifierName(),
                         table.get().name.GetIdentifierName());
      if (message.restart_identity) {
        for (auto& sequence :
             duckdb::Binder::TruncateIdentitySequences(context, table)) {
          sequences.push_back(std::move(sequence));
        }
      }
    }
  });
  std::vector<std::pair<std::string_view, std::string_view>> group(
    names.begin(), names.end());
  std::vector<duckdb::optional_ptr<duckdb::TableCatalogEntry>> locals;
  locals.reserve(tables.size());
  for (auto& table : tables) {
    locals.emplace_back(&table.get());
  }
  for (auto& sequence : sequences) {
    auto statement = duckdb::make_uniq<duckdb::AlterStatement>();
    statement->info = duckdb::make_uniq<duckdb::RestartSequenceInfo>(
      duckdb::AlterEntryData(std::move(sequence),
                             duckdb::OnEntryNotFound::THROW_EXCEPTION),
      duckdb::optional<int64_t>());
    auto prepared = this->_conn->Prepare(std::move(statement));
    if (prepared->HasError()) {
      prepared->GetErrorObject().Throw();
    }
    co_await RunPrepared(*prepared);
  }
  const auto order = ReferencedFirstOrder(locals);
  for (const auto i : std::views::reverse(order)) {
    UseRole(owners[i]);
    auto prepared = this->_conn->Prepare(
      BuildTruncate(names[i].first, names[i].second, group));
    if (prepared->HasError()) {
      prepared->GetErrorObject().Throw();
    }
    co_await RunPrepared(*prepared);
  }
  co_return {};
}

std::string PgReplicationClient::ApplyContext() const {
  const auto* relation = _batch.rel;
  const char* type = _batch.op == 'I'   ? "INSERT"
                     : _batch.op == 'U' ? "UPDATE"
                                        : "DELETE";
  return absl::StrCat("processing remote data for replication origin \"pg_",
                      _target.subscription_oid, "\" during message type \"",
                      type, "\" for replication target relation \"",
                      relation->schema, ".", relation->table,
                      "\" in transaction ", _xid, ", finished at ",
                      pg::FormatLsn(_final_lsn));
}

yaclib::Task<bool> PgReplicationClient::RunBatch() {
  const auto& relation = *_batch.rel;
  StmtKey key{_batch.op, _batch.relid, _batch.keys, _batch.cols, _batch.full};
  auto it = _stmts.find(key);
  if (it == _stmts.end()) {
    const auto names = [&](const std::vector<size_t>& indexes) {
      std::vector<std::string> result;
      result.reserve(indexes.size());
      for (const auto i : indexes) {
        result.push_back(relation.columns[i].name);
      }
      return result;
    };
    auto prepared = this->_conn->Prepare(
      BuildReplStatement(_batch.op, relation.schema, relation.table,
                         names(_batch.keys), names(_batch.cols), _batch.full));
    if (prepared->HasError()) {
      prepared->GetErrorObject().Throw();
    }
    it = _stmts.emplace(std::move(key), std::move(prepared)).first;
  }
  _batch.rows = 0;
  _batch.touched.clear();
  _stream.ScanActive(true);
  absl::Cleanup scan = [this] { _stream.ScanActive(false); };
  std::optional<int64_t> affected;
  irs::pg::SqlErrorData error;
  try {
    affected = co_await RunPrepared(*it->second);
  } catch (const std::exception& ex) {
    error = network::pg::ToSqlError(ex);
  }
  std::move(scan).Invoke();
  if (!affected) {
    if (error.errcode == ERRCODE_UNIQUE_VIOLATION && _batch.op != 'D') {
      const bool insert = _batch.op == 'I';
      (insert ? _conflicts.insert_exists : _conflicts.update_exists)
        .fetch_add(1, std::memory_order_relaxed);
      error.errdetail = std::move(error.errmsg);
      error.errmsg = absl::StrCat(
        "conflict detected on relation \"", relation.schema, ".",
        relation.table,
        "\": conflict=", insert ? "insert_exists" : "update_exists");
    }
    if (error.errmsg.empty()) {
      error.errcode = ERRCODE_INTERNAL_ERROR;
      error.errmsg = "apply statement produced no result";
    }
    error.context = ApplyContext();
    ApplyFailed(std::move(error));
    co_return false;
  }
  if (_batch.op != 'I' && static_cast<uint64_t>(*affected) < _batch.rows) {
    const auto missing = _batch.rows - static_cast<uint64_t>(*affected);
    const bool update = _batch.op == 'U';
    (update ? _conflicts.update_missing : _conflicts.delete_missing)
      .fetch_add(missing, std::memory_order_relaxed);
    SDB_INFO(REPLICATION, "conflict detected on relation \"", relation.schema,
             ".", relation.table,
             "\": conflict=", update ? "update_missing" : "delete_missing",
             " (", missing, " row(s)); ", ApplyContext());
  }
  co_return true;
}

}  // namespace sdb::replication
