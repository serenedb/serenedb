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
#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/subscription_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
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
#include <yaclib/async/contract.hpp>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "connector/duckdb_client_state.h"
#include "network/asio_awaitable.h"
#include "network/pg/wire_frames.h"
#include "network/server.h"
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
constexpr size_t kOutboxBytes = 1 << 20;
constexpr size_t kOutboxMessages = 4096;
constexpr size_t kMaxStatements = 1024;
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
  std::vector<duckdb::Identifier> schemas(n);
  irs::containers::FlatHashMap<std::pair<std::string_view, std::string_view>,
                               size_t>
    index;
  index.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    if (locals[i]) {
      schemas[i] = locals[i]->ParentSchemaName();
      index.try_emplace(
        std::pair{std::string_view{schemas[i].GetIdentifierName()},
                  std::string_view{locals[i]->name.GetIdentifierName()}},
        i);
    }
  }
  std::vector<std::vector<size_t>> references(n);
  for (size_t i = 0; i < n; ++i) {
    if (!locals[i]) {
      continue;
    }
    for (const auto& constraint : locals[i]->GetConstraints()) {
      if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
        continue;
      }
      const auto& info = constraint->Cast<duckdb::ForeignKeyConstraint>().info;
      if (info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
        continue;
      }
      const auto& schema = info.schema.empty() ? schemas[i] : info.schema;
      const auto it =
        index.find(std::pair{std::string_view{schema.GetIdentifierName()},
                             std::string_view{info.table.GetIdentifierName()}});
      if (it != index.end()) {
        references[i].push_back(it->second);
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
      auto& [node, cursor] = stack.back();
      const auto& referenced = references[node];
      while (cursor < referenced.size() && state[referenced[cursor]] != 0) {
        ++cursor;
      }
      if (cursor == referenced.size()) {
        state[node] = 2;
        order.push_back(node);
        stack.pop_back();
        continue;
      }
      const auto next = referenced[cursor++];
      state[next] = 1;
      stack.emplace_back(next, 0);
    }
  }
  return order;
}

}  // namespace

PgReplicationClient::PgReplicationClient(network::IoExecutor& exec,
                                         ReplicationTarget target,
                                         size_t host_index, size_t encryption)
  : SyncSession{exec, std::move(target), host_index, encryption},
    _flushed_lsn{_target.start_lsn} {
  for (auto& outbox : _outboxes) {
    outbox.messages.reserve(kOutboxMessages + 1);
  }
  _relation_index.reserve(_target.relations.size());
  for (size_t i = 0; i < _target.relations.size(); ++i) {
    const auto& relation = _target.relations[i];
    _relation_index.try_emplace(std::pair{std::string_view{relation.schema},
                                          std::string_view{relation.table}},
                                i);
  }
}

yaclib::Task<> PgReplicationClient::RunClient() {
  auto self = this->shared_from_this();
  auto writer = this->SendWriter();
  if (co_await Guarded(Connect())) {
    _connected.store(true, std::memory_order_release);
    _stream.SetTask(this->_task.get());
    auto cpu = ReplicationMain();
    this->_handed_off = true;
    this->_task->Start();
    co_await _setup_done.AwaitOn(*this->_ioexec);
    bool streaming = _setup_ok;
    if (streaming && !co_await Guarded(SyncTables())) {
      _sync_failed.store(true, std::memory_order_release);
      streaming = false;
    }
    streaming = streaming && co_await Guarded(StartReplication());
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

yaclib::Task<bool> PgReplicationClient::SyncTables() {
  _sync_tables.clear();
  for (size_t i = 0; i < _target.relations.size(); ++i) {
    const auto& relation = _target.relations[i];
    if (relation.state != 'r') {
      _sync_tables.push_back({.schema = relation.schema,
                              .table = relation.table,
                              .relation = i,
                              .sync_id = relation.sync_id});
    }
  }
  if (_sync_tables.empty()) {
    co_return true;
  }
  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' synchronizing ", _sync_tables.size(), " table(s)");
  const auto workers =
    std::min<size_t>(std::max<uint64_t>(MaxSyncWorkersPerSubscription(), 1),
                     _sync_tables.size());
  co_return workers == 1 ? co_await SyncSequential()
                         : co_await SyncParallel(workers);
}

yaclib::Task<bool> PgReplicationClient::DescribeTables() {
  std::vector<PublisherRow> rows;
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
  irs::containers::FlatHashMap<std::pair<std::string_view, std::string_view>,
                               size_t>
    index;
  index.reserve(_sync_tables.size());
  for (size_t i = 0; i < _sync_tables.size(); ++i) {
    index.emplace(std::pair{std::string_view{_sync_tables[i].schema},
                            std::string_view{_sync_tables[i].table}},
                  i);
  }
  struct Listed {
    std::string_view first;
    std::string_view current;
    size_t matched = 0;
    bool unfiltered = false;
    std::vector<std::string_view> filters;
  };
  std::vector<Listed> listed(_sync_tables.size());
  const auto same_columns = [&](size_t i) {
    const auto& state = listed[i];
    if (state.current == state.first ||
        state.matched == _sync_tables[i].columns.size()) {
      return true;
    }
    Fail(ERRCODE_FEATURE_NOT_SUPPORTED,
         absl::StrCat("cannot use different column lists for table \"",
                      _sync_tables[i].schema, ".", _sync_tables[i].table,
                      "\" in different publications"));
    return false;
  };
  for (const auto& row : rows) {
    if (row.size() < 5 || !row[0] || !row[1] || !row[2] || !row[4]) {
      continue;
    }
    const auto it = index.find(
      std::pair{std::string_view{*row[0]}, std::string_view{*row[1]}});
    if (it == index.end()) {
      continue;
    }
    auto& table = _sync_tables[it->second];
    auto& state = listed[it->second];
    const std::string_view publication = *row[4];
    if (state.first.empty()) {
      state.first = publication;
      state.current = publication;
      table.partitioned = row.size() > 5 && row[5] && *row[5] == "p";
    } else if (publication != state.current) {
      if (!same_columns(it->second)) {
        co_return false;
      }
      state.current = publication;
      state.matched = 0;
    }
    if (publication == state.first) {
      table.columns.push_back(*row[2]);
      table.generated |= row.size() > 6 && row[6] && *row[6] == "t";
    } else if (state.matched < table.columns.size() &&
               table.columns[state.matched] == *row[2]) {
      ++state.matched;
    } else {
      state.matched = table.columns.size() + 1;
    }
    if (!row[3]) {
      state.unfiltered = true;
    } else if (!std::ranges::contains(state.filters, *row[3])) {
      state.filters.emplace_back(*row[3]);
    }
  }
  for (size_t i = 0; i < _sync_tables.size(); ++i) {
    if (!same_columns(i)) {
      co_return false;
    }
    const auto& state = listed[i];
    if (!state.unfiltered && !state.filters.empty()) {
      _sync_tables[i].row_filter = absl::StrJoin(
        state.filters, " OR ", [](std::string* out, std::string_view filter) {
          absl::StrAppend(out, "(", filter, ")");
        });
    }
  }
  co_return true;
}

std::string PgReplicationClient::SyncSlotName() const {
  return absl::StrFormat("pg_%u_sync_%016x", _target.subscription_oid,
                         absl::Uniform<uint64_t>(absl::BitGen{}));
}

yaclib::Task<bool> PgReplicationClient::SyncSequential() {
  const auto slot = SyncSlotName();
  std::vector<PublisherRow> rows;
  if (!co_await Query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ") ||
      !co_await Query(
        absl::StrCat("CREATE_REPLICATION_SLOT ", pg::QuoteIdentifier(slot),
                     " TEMPORARY LOGICAL pgoutput (SNAPSHOT 'use')"),
        &rows) ||
      rows.empty() || rows.front().size() < 2 || !rows.front()[1]) {
    co_return false;
  }
  const auto consistent_point = pg::ParseLsn(*rows.front()[1]);
  if (!consistent_point) {
    Fail(ERRCODE_PROTOCOL_VIOLATION, "invalid replication slot LSN");
    co_return false;
  }
  if (!co_await DescribeTables()) {
    co_return false;
  }
  const bool binary = _target.binary && ServerVersion() >= 16;
  for (auto& table : _sync_tables) {
    if (!co_await SyncOne(table, *consistent_point, binary)) {
      co_return false;
    }
    MarkSynced(table, *consistent_point);
  }
  co_return co_await Query("COMMIT") &&
    co_await Query(
      absl::StrCat("DROP_REPLICATION_SLOT ", pg::QuoteIdentifier(slot)));
}

yaclib::Task<bool> PgReplicationClient::SyncParallel(size_t workers) {
  if (!co_await DescribeTables()) {
    co_return false;
  }
  const auto slot = SyncSlotName();
  std::vector<PublisherRow> rows;
  if (!co_await Query(
        absl::StrCat("CREATE_REPLICATION_SLOT ", pg::QuoteIdentifier(slot),
                     " TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')"),
        &rows) ||
      rows.empty() || rows.front().size() < 3 || !rows.front()[1] ||
      !rows.front()[2]) {
    co_return false;
  }
  const auto consistent_point = pg::ParseLsn(*rows.front()[1]);
  if (!consistent_point) {
    Fail(ERRCODE_PROTOCOL_VIOLATION, "invalid replication slot LSN");
    co_return false;
  }
  SyncPlan plan{.tables = _sync_tables,
                .snapshot = *rows.front()[2],
                .lsn = *consistent_point,
                .binary = _target.binary && ServerVersion() >= 16,
                .done = std::vector<uint8_t>(_sync_tables.size(), 0)};
  auto relations = std::exchange(_target.relations, {});
  auto target = _target;
  _target.relations = std::move(relations);
  target.conninfo.hosts = {_host};
  auto* pool = Server::instance().IoPool();
  std::vector<duckdb::shared_ptr<TableSyncWorker>> sessions;
  std::vector<yaclib::Future<bool>> results;
  sessions.reserve(workers);
  results.reserve(workers);
  for (size_t i = 0; i < workers; ++i) {
    auto& exec = pool->Next();
    auto worker = duckdb::make_shared_ptr<TableSyncWorker>(
      exec, target, EncryptionAttempt(), plan);
    auto [future, promise] = yaclib::MakeContract<bool>();
    asio_ns::post(exec.Context(),
                  [worker, promise = std::move(promise)]() mutable {
                    worker->Start(std::move(promise));
                  });
    sessions.push_back(std::move(worker));
    results.push_back(std::move(future));
  }
  bool ok = true;
  for (size_t i = 0; i < workers; ++i) {
    if (!co_await std::move(results[i])) {
      ok = false;
      if (_apply_error.errmsg.empty()) {
        _apply_error = sessions[i]->LastError();
      }
    }
  }
  for (size_t i = 0; i < _sync_tables.size(); ++i) {
    if (plan.done[i] == 0) {
      ok = false;
      continue;
    }
    MarkSynced(_sync_tables[i], plan.lsn);
  }
  co_return co_await Query(
    absl::StrCat("DROP_REPLICATION_SLOT ", pg::QuoteIdentifier(slot))) &&
    ok;
}

void PgReplicationClient::MarkSynced(const SyncTable& table, uint64_t lsn) {
  auto& relation = _target.relations[table.relation];
  relation.state = 'r';
  relation.lsn = lsn;
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

void PgReplicationClient::Outbox::Clear() noexcept {
  messages.clear();
  recv = {};
  copies.Clear();
  bytes = 0;
}

yaclib::Task<> PgReplicationClient::AwaitDrained() {
  if (!_in_flight) {
    co_return {};
  }
  co_await _stream.Drained(*this->_ioexec);
  _stream.ResetDrained();
  _in_flight = false;
  _last_activity = SteadyMicros();
  _outboxes[_staging ^ 1].Clear();
  co_return {};
}

bool PgReplicationClient::Full() const noexcept {
  const auto& outbox = _outboxes[_staging];
  return outbox.messages.size() >= kOutboxMessages ||
         outbox.bytes >= kOutboxBytes;
}

yaclib::Task<bool> PgReplicationClient::Flush() {
  auto& outbox = _outboxes[_staging];
  if (outbox.messages.empty()) {
    if (!_in_flight) {
      outbox.recv = {};
    }
    co_return true;
  }
  co_await AwaitDrained();
  if (!_stream.Publish(outbox.messages)) {
    co_return false;
  }
  _in_flight = true;
  _published = true;
  _staging ^= 1;
  this->_recv.RetainConsumed(&_outboxes[_staging].recv);
  co_return true;
}

yaclib::Task<bool> PgReplicationClient::Enqueue(std::string_view payload,
                                                bool in_stream, bool borrowed) {
  if (Full() && !co_await Flush()) {
    co_return false;
  }
  auto& outbox = _outboxes[_staging];
  if (!borrowed) {
    message::Writer writer{outbox.copies};
    auto* data = writer.Alloc(payload.size());
    std::memcpy(data, payload.data(), payload.size());
    writer.Commit(false);
    payload = {reinterpret_cast<const char*>(data), payload.size()};
  }
  outbox.bytes += payload.size();
  outbox.messages.push_back(DecodePgOutput(payload, in_stream));
  co_return true;
}

yaclib::Task<bool> PgReplicationClient::Enqueue(PgOutputMessage message) {
  if (Full() && !co_await Flush()) {
    co_return false;
  }
  _outboxes[_staging].messages.push_back(std::move(message));
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
      if (!co_await Enqueue(payload, true, false)) {
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
  this->_recv.RetainConsumed(&_outboxes[_staging].recv);
  for (;;) {
    if (this->SendBroken() || _stream.Aborted()) {
      break;
    }
    auto frame =
      this->_frames.TryAssemble(FrameKind::Typed, this->_max_message);
    if (frame.status == FrameStatus::NeedMore) {
      if (_published || !_outboxes[_staging].messages.empty()) {
        _outboxes[_staging].messages.emplace_back(StreamStopMessage{});
      }
      if (!co_await Flush()) {
        break;
      }
      _published = false;
      SendFeedback(false, false);
      frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    }
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
      SendFeedback(false, reply);
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
            ok = co_await Enqueue(message, false, frame.recv_consume != 0);
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
  if (co_await Flush()) {
    co_await AwaitDrained();
    _outboxes[_staging].recv = {};
  }
  this->_recv.RetainConsumed(nullptr);
  _stream.Finish();
  co_return {};
}

void PgReplicationClient::SendFeedback(bool reply, bool force) {
  if (this->SendBroken()) {
    return;
  }
  const auto received = _received_lsn.load(std::memory_order_relaxed);
  auto flushed = _flushed_lsn.load(std::memory_order_relaxed);
  const bool pending = _in_flight && !_stream.IsDrained();
  if (!pending && _outboxes[_staging].messages.empty() &&
      _apply_idle.load(std::memory_order_acquire) && !_streamed_xid &&
      _spools.empty() && received > flushed) {
    flushed = received;
  }
  if (!force && !reply && flushed == _reported_flush) {
    return;
  }
  _reported_flush = flushed;
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
    const bool pending = _in_flight && !_stream.IsDrained();
    const auto silent = pending ? 0 : now - _last_activity;
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
      SendFeedback(ping, true);
    }
  }
  co_return {};
}

yaclib::Future<> PgReplicationClient::ReplicationMain() {
  co_await this->_task->Park();
  co_await ReplicationLoop();
  FinishJobs();
  co_return {};
}

yaclib::Task<> PgReplicationClient::ReplicationLoop() {
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
  if (!co_await RunJobs()) {
    _stream.Abort();
    co_return {};
  }
  _batch.stream = &_stream;
  _batch.pass_through = [this](const PgOutputMessage& message) {
    return PassThrough(message);
  };
  this->_connection_ctx->SetSideChannel(&_batch);
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
            SendFeedback(false, false);
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

void PgReplicationClient::UpdateDefinition(
  absl::FunctionRef<void(duckdb::CreateSubscriptionInfo&)> edit) {
  const auto& current = RequireSubscription();
  auto definition =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateSubscriptionInfo>(
      current.GetInfo());
  edit(*definition);
  duckdb::ReplaceDefinitionInfo alter{std::move(definition)};
  alter.SetQualifiedName(duckdb::QualifiedName(current.name));
  auto& catalog = Catalog();
  catalog.Alter(catalog.GetCatalogTransaction(*this->_conn->context), alter);
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
  if (const auto it = _relation_index.find(
        std::pair{std::string_view{info.schema}, std::string_view{info.table}});
      it != _relation_index.end()) {
    const auto& relation = _target.relations[it->second];
    info.ready = relation.state == 'r';
    info.sync_lsn = relation.lsn;
  }
  this->_conn->context->RunFunctionInTransaction([&] {
    auto table = LookupTable(info.schema, info.table);
    if (!table) {
      return;
    }
    info.mapped = true;
    info.table_oid = table->oid;
    info.owner = table->permissions.owner;
    table->ScanTriggers(
      table->ParentCatalog().GetCatalogTransaction(*this->_conn->context),
      [&](duckdb::CatalogEntry& entry) {
        info.triggers =
          info.triggers || entry.Cast<duckdb::TriggerCatalogEntry>().Fires(
                             duckdb::ReplicationRole::REPLICA);
      });
    const auto& locals = table->GetColumns();
    info.columns.reserve(message.columns.size());
    for (const auto& remote : message.columns) {
      auto& column = info.columns.emplace_back(
        RelColumn{.name = std::string{remote.name}, .is_key = remote.is_key});
      const duckdb::Identifier name{remote.name};
      if (!locals.ColumnExists(name)) {
        info.missing_columns.push_back(column.name);
        continue;
      }
      const auto& local = locals.GetColumn(name);
      if (local.Generated()) {
        info.generated_columns.push_back(column.name);
      } else {
        column.type = local.Type();
      }
    }
    const auto add_unique = [&](const auto& names) {
      std::vector<size_t> positions;
      positions.reserve(names.size());
      for (const auto& name : names) {
        const auto it = std::ranges::find(info.columns, name, &RelColumn::name);
        if (it == info.columns.end()) {
          return;
        }
        positions.push_back(it - info.columns.begin());
      }
      if (!positions.empty()) {
        info.unique_keys.push_back(std::move(positions));
      }
    };
    for (const auto& constraint : table->GetConstraints()) {
      if (constraint->type != duckdb::ConstraintType::UNIQUE) {
        continue;
      }
      const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
      std::vector<std::string_view> names;
      if (unique.HasIndex()) {
        names.push_back(
          locals.GetColumn(unique.GetIndex()).Name().GetIdentifierName());
      } else {
        names.reserve(unique.GetColumnNames().size());
        for (const auto& name : unique.GetColumnNames()) {
          names.push_back(name.GetIdentifierName());
        }
      }
      add_unique(names);
    }
    table->ParentSchema(*this->_conn->context)
      .Scan(*this->_conn->context, duckdb::CatalogType::INDEX_ENTRY,
            [&](duckdb::CatalogEntry& entry) {
              const auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
              if (!index.IsUnique() || index.GetTableName() != table->name) {
                return;
              }
              std::vector<std::string_view> names;
              for (const auto& expression : index.parsed_expressions) {
                if (expression->GetExpressionClass() !=
                    duckdb::ExpressionClass::COLUMN_REF) {
                  return;
                }
                names.push_back(expression->Cast<duckdb::ColumnRefExpression>()
                                  .GetColumnName()
                                  .GetIdentifierName());
              }
              add_unique(names);
            });
  });
  absl::erase_if(_stmts, [&](const auto& entry) {
    return entry.first.relid == message.relation_id;
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
  if (!ShapeRow(message, *relation, _batch.shape)) {
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
  if (auto* subscription = VisibleSubscription()) {
    duckdb::DuckTransaction::Get(context, *_database)
      .PushReplicationLsn(*subscription, end_lsn);
  }
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
  auto& groups = _reorder_groups;
  groups.clear();
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
    if (relation == nullptr || !relation->mapped || relation->triggers ||
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
  auto& order = _reorder_order;
  if (order.size() < groups.size()) {
    order.resize(groups.size());
  }
  for (size_t g = 0; g < groups.size(); ++g) {
    order[g].clear();
  }
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
  for (size_t g = 0; g < groups.size(); ++g) {
    for (const auto i : order[g]) {
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
  const auto op = _batch.shape.op;
  const char* type = op == 'I' ? "INSERT" : op == 'U' ? "UPDATE" : "DELETE";
  return absl::StrCat("processing remote data for replication origin \"pg_",
                      _target.subscription_oid, "\" during message type \"",
                      type, "\" for replication target relation \"",
                      relation->schema, ".", relation->table,
                      "\" in transaction ", _xid, ", finished at ",
                      pg::FormatLsn(_final_lsn));
}

yaclib::Task<bool> PgReplicationClient::MultipleUniqueConflicts(
  duckdb::ColumnDataCollection& rows) {
  if (_in_txn) {
    this->_txn_state->Rollback();
    _in_txn = false;
  }
  auto probe = BuildUniqueConflictProbe(_batch, rows);
  if (!probe) {
    co_return false;
  }
  std::optional<int64_t> conflicting;
  try {
    auto prepared = this->_conn->Prepare(std::move(probe));
    if (!prepared->HasError()) {
      conflicting = co_await RunPrepared(*prepared);
    }
  } catch (const std::exception&) {
  }
  co_return conflicting.value_or(0) > 0;
}

yaclib::Task<bool> PgReplicationClient::RunBatch() {
  const auto& relation = *_batch.rel;
  const auto& shape = _batch.shape;
  auto it = _stmts.find(StmtKeyView{_batch.relid, shape});
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
      BuildReplStatement(shape.op, relation.schema, relation.table,
                         names(shape.keys), names(shape.cols), shape.full));
    if (prepared->HasError()) {
      prepared->GetErrorObject().Throw();
    }
    if (_stmts.size() >= kMaxStatements) {
      _stmts.clear();
    }
    it = _stmts
           .emplace(StmtKey{shape.op, _batch.relid, shape.keys, shape.cols,
                            shape.full},
                    std::move(prepared))
           .first;
  }
  _batch.rows = 0;
  _batch.ResetTouched();
  std::optional<duckdb::ColumnDataCollection> retained;
  if (shape.op != 'D' && relation.unique_keys.size() > 1) {
    duckdb::vector<duckdb::LogicalType> types;
    types.reserve(shape.keys.size() + shape.cols.size());
    for (const auto i : shape.keys) {
      types.push_back(relation.columns[i].type);
    }
    for (const auto i : shape.cols) {
      types.push_back(relation.columns[i].type);
    }
    retained.emplace(
      duckdb::BufferManager::GetBufferManager(*this->_conn->context),
      std::move(types));
    _batch.retained = &*retained;
  }
  absl::Cleanup release = [this] { _batch.retained = nullptr; };
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
    if (error.errcode == ERRCODE_UNIQUE_VIOLATION && shape.op != 'D') {
      const bool insert = shape.op == 'I';
      std::string_view conflict = insert ? "insert_exists" : "update_exists";
      if (retained && co_await MultipleUniqueConflicts(*retained)) {
        conflict = "multiple_unique_conflicts";
        _conflicts.multiple_unique_conflicts.fetch_add(
          1, std::memory_order_relaxed);
      } else {
        (insert ? _conflicts.insert_exists : _conflicts.update_exists)
          .fetch_add(1, std::memory_order_relaxed);
      }
      error.errdetail = std::move(error.errmsg);
      error.errmsg =
        absl::StrCat("conflict detected on relation \"", relation.schema, ".",
                     relation.table, "\": conflict=", conflict);
    }
    if (error.errmsg.empty()) {
      error.errcode = ERRCODE_INTERNAL_ERROR;
      error.errmsg = "apply statement produced no result";
    }
    error.context = ApplyContext();
    ApplyFailed(std::move(error));
    co_return false;
  }
  if (shape.op != 'I' && static_cast<uint64_t>(*affected) < _batch.rows) {
    const auto missing = _batch.rows - static_cast<uint64_t>(*affected);
    const bool update = shape.op == 'U';
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
