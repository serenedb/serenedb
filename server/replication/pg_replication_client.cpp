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
#include <absl/strings/str_split.h>

#include <algorithm>
#include <cstdlib>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/pending_query_result.hpp>
#include <optional>
#include <string_view>
#include <utility>

#include "basics/asio_ns.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/message_buffer.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "network/asio_awaitable.h"
#include "network/cpu_resumer.h"
#include "network/pg/pg_frame_codec.h"
#include "network/pg/scram_client.h"
#include "pg/connection_context.h"
#include "pg/copy_in_bridge.h"
#include "replication/pgoutput.h"
#include "replication/repl_source.h"

namespace sdb::replication {

using network::pg::Frame;
using network::pg::FrameKind;
using network::pg::FrameStatus;

namespace {

// pg-wire typed-frame header: a 1-byte message type then a big-endian int32
// length (the length counts itself + the body, but not the type byte).
constexpr size_t kFrameTag = 1;
constexpr size_t kFrameHeader = kFrameTag + sizeof(int32_t);

// A big-endian int64 straight into the buffer.
void PutU64(message::Writer& w, uint64_t v) {
  absl::big_endian::Store64(w.Alloc(sizeof(uint64_t)), v);
}

// Compose a typed pg-wire frame (type byte + int32 length + body) directly into
// `send`: `body(w)` writes the payload through the Writer, so nothing is ever
// assembled in a temporary string (message::Writer streams into _send).
template<typename BodyFn>
void WriteFrame(message::Buffer& send, char type, BodyFn&& body) {
  message::Writer w{send};
  auto* prefix = w.Alloc(kFrameHeader);
  prefix[0] = static_cast<uint8_t>(type);
  std::forward<BodyFn>(body)(w);
  // length counts the int32 length field + body, not the type byte.
  absl::big_endian::Store32(prefix + kFrameTag,
                            static_cast<int32_t>(w.Written() - kFrameTag));
  w.Commit(false);
}

// Startup-phase message: length-prefixed, no type byte (length includes
// itself).
template<typename BodyFn>
void WriteStartup(message::Buffer& send, BodyFn&& body) {
  message::Writer w{send};
  auto* prefix = w.Alloc(sizeof(int32_t));
  std::forward<BodyFn>(body)(w);
  absl::big_endian::Store32(prefix, static_cast<int32_t>(w.Written()));
  w.Commit(false);
}

std::string QuoteIdent(std::string_view id) {
  std::string out;
  out.reserve(id.size() + 2);
  out.push_back('"');
  for (const char c : id) {
    if (c == '"') {
      out.push_back('"');
    }
    out.push_back(c);
  }
  out.push_back('"');
  return out;
}

std::string QuoteLiteral(std::string_view s) {
  std::string out;
  out.reserve(s.size() + 2);
  out.push_back('\'');
  for (const char c : s) {
    if (c == '\'') {
      out.push_back('\'');
    }
    out.push_back(c);
  }
  out.push_back('\'');
  return out;
}

// Order indices so every table is listed after all tables it references via a
// FOREIGN KEY that are also in the set (referenced-first). DuckDB enforces FK
// integrity per statement -- it rejects inserting a referencing row before its
// referenced row, and rejects truncating a referenced table while a referencing
// one still has rows -- so bulk apply must respect this order (copy in the
// returned order; truncate in reverse). References outside the set and FK
// cycles are skipped, leaving those tables in discovery order (best effort).
std::vector<size_t> ReferencedFirstOrder(
  const std::vector<std::shared_ptr<catalog::Table>>& locals) {
  const size_t n = locals.size();
  containers::FlatHashMap<ObjectId, size_t> index;
  index.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    if (locals[i]) {
      index.try_emplace(locals[i]->GetId(), i);
    }
  }
  std::vector<size_t> order;
  order.reserve(n);
  std::vector<uint8_t> state(n, 0);  // 0 = new, 1 = active, 2 = done
  std::vector<std::pair<size_t, size_t>> stack;  // (node, fk cursor)
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
        const auto& fks = locals[node]->ForeignKeys();
        while (cursor < fks.size()) {
          const auto it = index.find(fks[cursor].referenced_table);
          ++cursor;
          if (it == index.end() || state[it->second] != 0) {
            continue;  // outside the set, already done, or a cycle
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

}  // namespace

ReplicationTarget ParseConnInfo(std::string conninfo) {
  ReplicationTarget t;
  for (std::string_view kv :
       absl::StrSplit(conninfo, ' ', absl::SkipWhitespace())) {
    auto eq = kv.find('=');
    if (eq == std::string_view::npos) {
      continue;
    }
    std::string_view key = kv.substr(0, eq);
    std::string value{kv.substr(eq + 1)};
    if (key == "host" || key == "hostaddr") {
      t.host = std::move(value);
    } else if (key == "port") {
      t.port = std::move(value);
    } else if (key == "user") {
      t.user = std::move(value);
    } else if (key == "password") {
      t.password = std::move(value);
    } else if (key == "dbname") {
      t.dbname = std::move(value);
    }
  }
  if (t.host.empty()) {
    t.host = "localhost";
  }
  return t;
}

PgReplicationClient::PgReplicationClient(network::IoExecutor& exec,
                                         ReplicationTarget target)
  : PgWireSession<network::SocketKind::Tcp>{exec, ClientTag{}},
    _target(std::move(target)) {}

void PgReplicationClient::StartSlotDrop() { RunSlotDrop().Detach(); }

yaclib::Task<> PgReplicationClient::RunSlotDrop() {
  auto self = this->shared_from_this();
  auto writer = this->SendWriter();
  if (co_await ConnectAndAuthenticate()) {
    co_await SendSlotDrop();
  }
  this->Stop();
  co_await std::move(writer);
  co_return {};
}

yaclib::Task<bool> PgReplicationClient::SendSlotDrop() {
  // WAIT blocks on the publisher until the slot goes inactive, covering the
  // window between StopClient() on the streaming connection and this drop.
  WriteFrame(this->_send, 'Q', [&](message::Writer& w) {
    w.Write("DROP_REPLICATION_SLOT \"");
    w.Write(_target.slot_name);
    w.Write("\" WAIT\0");  // NUL-terminate the query
  });
  this->KickSend();
  bool ok = true;
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      co_return false;
    }
    const char type = frame.type;
    if (type == 'E') {
      ok = false;
    }
    this->_frames.Consume(frame);
    if (type == 'Z') {  // ReadyForQuery ends the command
      break;
    }
  }
  if (ok) {
    SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
             "' dropped publisher slot '", _target.slot_name, "'");
  } else {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' could not drop publisher slot '", _target.slot_name,
             "' (may need manual cleanup)");
  }
  co_return ok;
}

yaclib::Task<> PgReplicationClient::RunClient() {
  auto self = this->shared_from_this();
  auto writer = this->SendWriter();
  if (co_await ConnectAndAuthenticate()) {
    this->_task = duckdb::make_shared_ptr<network::CpuResumer>(
      duckdb::TaskScheduler::GetScheduler(DuckDBEngine::Instance().instance()),
      *this->_ioexec);
    _stream.SetTask(this->_task.get());
    // Bring the duck-side apply loop up first: it creates the local connection,
    // signals _setup_done, then waits for initial-copy jobs. The io side (here)
    // directs the initial sync and then feeds the streamed changes.
    auto cpu = ReplicationMain();
    this->_task->RequestRun();
    co_await _setup_done.AwaitOn(*this->_ioexec);
    bool streaming = false;
    if (_setup_ok) {
      streaming = co_await Handshake();
    }
    // Release the duck side from the copy-wait loop into the streaming control
    // loop (or straight to teardown when the handshake/setup failed).
    _copy_phase.store(CopyPhase::Stream, std::memory_order_release);
    this->_task->RequestRun();
    if (streaming) {
      try {
        co_await Feeder();
      } catch (const std::exception&) {
      }
    }
    // Wake the duck control loop so it sees EOF and unwinds -- for both the
    // streaming path (feeder ended/threw) and the setup/handshake-failed path
    // (no feeder ran). Finish() is io->duck (RequestRun); never touch the
    // duck-side Abort() from here (it Sets _consumed, which only the duck may).
    _stream.Finish();
    this->Stop();
    co_await std::move(cpu);
  }
  this->Stop();
  co_await std::move(writer);
  co_return {};
}

yaclib::Task<bool> PgReplicationClient::ConnectAndAuthenticate() {
  asio_ns::ip::tcp::resolver resolver{this->_io};
  auto [rec, endpoints] =
    co_await network::Async<asio_ns::ip::tcp::resolver::results_type>(
      [&](auto&& h) {
        resolver.async_resolve(_target.host, _target.port,
                               std::forward<decltype(h)>(h));
      })
      .NoThrow();
  if (rec) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' resolve ", _target.host, ":", _target.port,
             " failed: ", rec.message());
    co_return false;
  }
  auto [cec, endpoint] =
    co_await network::Async<asio_ns::ip::tcp::endpoint>([&](auto&& h) {
      asio_ns::async_connect(this->_socket.Lowest(), endpoints,
                             std::forward<decltype(h)>(h));
    }).NoThrow();
  if (cec) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' connect failed: ", cec.message());
    co_return false;
  }

  // StartupMessage: protocol 3.0 + user/database/replication params.
  WriteStartup(this->_send, [&](message::Writer& w) {
    absl::big_endian::Store32(w.Alloc(sizeof(int32_t)), 196608);
    auto param = [&](std::string_view k, std::string_view v) {
      w.Write(k);
      w.Write("\0");
      w.Write(v);
      w.Write("\0");
    };
    param("user", _target.user);
    if (!_target.dbname.empty()) {
      param("database", _target.dbname);
    }
    param("replication", "database");
    param("application_name", "serenedb_subscription");
    w.Write("\0");  // terminator
  });
  this->KickSend();

  co_return co_await Authenticate();
}

yaclib::Task<bool> PgReplicationClient::Authenticate() {
  network::pg::ScramClientSession scram{_target.password};
  bool sasl_started = false;

  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      co_return false;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type == 'E') {  // ErrorResponse
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' auth error from publisher");
      this->_frames.Consume(frame);
      co_return false;
    }
    if (type != 'R') {  // AuthenticationRequest
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' unexpected frame '", std::string_view{&type, 1},
               "' during auth");
      this->_frames.Consume(frame);
      co_return false;
    }
    if (payload.size() < sizeof(int32_t)) {
      this->_frames.Consume(frame);
      co_return false;
    }
    const int32_t code = absl::big_endian::Load32(payload.data());
    const std::string_view data = payload.substr(sizeof(int32_t));

    bool done = false;
    switch (code) {
      case 0: {  // AuthenticationOk
        done = true;
        break;
      }
      case 10: {  // AuthenticationSASL: server lists mechanisms
        const std::string client_first = scram.ClientFirst();
        WriteFrame(this->_send, 'p', [&](message::Writer& w) {  // SASLInitial
          w.Write(
            std::string_view{network::pg::ScramClientSession::kMechanism});
          w.Write("\0");
          absl::big_endian::Store32(w.Alloc(sizeof(int32_t)),
                                    static_cast<int32_t>(client_first.size()));
          w.Write(client_first);
        });
        this->KickSend();
        sasl_started = true;
        break;
      }
      case 11: {  // AuthenticationSASLContinue: server-first
        auto client_final = scram.ServerFirst(data);
        if (!sasl_started || !client_final) {
          this->_frames.Consume(frame);
          co_return false;
        }
        WriteFrame(this->_send, 'p', [&](message::Writer& w) {  // SASLResponse
          w.Write(*client_final);
        });
        this->KickSend();
        break;
      }
      case 12: {  // AuthenticationSASLFinal: server-final
        if (!scram.ServerFinal(data)) {
          SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
                   "' server signature mismatch");
          this->_frames.Consume(frame);
          co_return false;
        }
        break;
      }
      default: {
        SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
                 "' unsupported auth method ", code,
                 " (only SCRAM-SHA-256 is supported)");
        this->_frames.Consume(frame);
        co_return false;
      }
    }
    this->_frames.Consume(frame);
    if (done) {
      break;
    }
  }

  // Drain ParameterStatus/BackendKeyData/NoticeResponse until ReadyForQuery,
  // capturing the publisher's major version from server_version (gates binary
  // initial COPY, like PG's copy_table).
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok || frame.type == 'E') {
      this->_frames.Consume(frame);
      co_return false;
    }
    const char type = frame.type;
    if (type == 'S') {  // ParameterStatus: name\0value\0
      const std::string_view payload = frame.payload;
      const auto sep = payload.find('\0');
      if (sep != std::string_view::npos &&
          payload.substr(0, sep) == "server_version") {
        _server_version = std::atoi(payload.data() + sep + 1);
      }
    }
    this->_frames.Consume(frame);
    if (type == 'Z') {  // ReadyForQuery
      break;
    }
  }
  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' authenticated to publisher ", _target.host, ":", _target.port);
  co_return true;
}

yaclib::Task<bool> PgReplicationClient::Handshake() {
  // Initial table sync (only when the slot is new); then stream from the slot's
  // consistent point.
  co_await MaybeInitialCopy();

  // START_REPLICATION: send the query and expect CopyBothResponse ('W'). Text
  // by default (PG parity); binary is opt-in per subscription and the decode
  // path handles 'b'. PG appends binary 'true' after publication_names.
  WriteFrame(this->_send, 'Q', [&](message::Writer& w) {
    w.Write("START_REPLICATION SLOT \"");
    w.Write(_target.slot_name);
    w.Write("\" LOGICAL 0/0 (proto_version '1'");
    // Only send origin when non-default ('any' is the publisher default, and
    // the option predates by pg16 -- omitting it keeps older publishers happy).
    if (_target.origin != "any") {
      w.Write(", origin '");
      w.Write(_target.origin);
      w.Write("'");
    }
    w.Write(", publication_names '");
    for (size_t i = 0; i < _target.publications.size(); ++i) {
      if (i != 0) {
        w.Write(",");
      }
      w.Write("\"");
      w.Write(_target.publications[i]);
      w.Write("\"");
    }
    if (_target.binary) {
      w.Write("', binary 'true')\0");
    } else {
      w.Write("')\0");
    }
  });
  this->KickSend();
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      co_return false;
    }
    const char type = frame.type;
    this->_frames.Consume(frame);
    if (type == 'W') {
      SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
               "' streaming from publisher");
      co_return true;
    }
    if (type == 'E') {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' START_REPLICATION rejected");
      co_return false;
    }
  }
}

yaclib::Task<bool> PgReplicationClient::RunSimpleCommand(std::string_view sql) {
  WriteFrame(this->_send, 'Q', [&](message::Writer& w) {
    w.Write(sql);
    w.Write("\0");
  });
  this->KickSend();
  bool ok = true;
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      co_return false;
    }
    const char type = frame.type;
    this->_frames.Consume(frame);
    if (type == 'E') {
      ok = false;
    }
    if (type == 'Z') {
      break;
    }
  }
  co_return ok;
}

yaclib::Task<bool> PgReplicationClient::RunQueryRows(
  std::string_view sql,
  std::vector<std::vector<std::optional<std::string>>>& rows) {
  WriteFrame(this->_send, 'Q', [&](message::Writer& w) {
    w.Write(sql);
    w.Write("\0");
  });
  this->KickSend();
  bool ok = true;
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      co_return false;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type == 'D' && payload.size() >= sizeof(uint16_t)) {
      const uint16_t count = absl::big_endian::Load16(payload.data());
      std::vector<std::optional<std::string>> row;
      row.reserve(count);
      size_t pos = sizeof(uint16_t);
      bool truncated = false;
      for (uint16_t i = 0; i < count; ++i) {
        if (pos + sizeof(int32_t) > payload.size()) {
          truncated = true;
          break;
        }
        const int32_t len =
          static_cast<int32_t>(absl::big_endian::Load32(payload.data() + pos));
        pos += sizeof(int32_t);
        if (len < 0) {
          row.emplace_back(std::nullopt);
          continue;
        }
        if (pos + static_cast<size_t>(len) > payload.size()) {
          truncated = true;
          break;
        }
        row.emplace_back(std::string{payload.substr(pos, len)});
        pos += static_cast<size_t>(len);
      }
      if (!truncated) {
        rows.push_back(std::move(row));
      }
      this->_frames.Consume(frame);
      continue;
    }
    if (type == 'E') {
      ok = false;
    }
    this->_frames.Consume(frame);
    if (type == 'Z') {
      break;
    }
  }
  co_return ok;
}

yaclib::Task<> PgReplicationClient::MaybeInitialCopy() {
  // A pre-existing slot means initial sync already ran (durable per-table state
  // is future work); skip straight to streaming.
  {
    std::vector<std::vector<std::optional<std::string>>> rows;
    const bool ok = co_await RunQueryRows(
      "SELECT 1 FROM pg_replication_slots WHERE slot_name = " +
        QuoteLiteral(_target.slot_name),
      rows);
    if (ok && !rows.empty()) {
      co_return {};
    }
  }

  // WITH (create_slot = false): never create the slot -- the user pre-created
  // it (a missing slot then surfaces at START_REPLICATION). No exported
  // snapshot is available, so there is no initial copy either.
  if (!_target.create_slot) {
    SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
             "' create_slot = false: expecting an existing replication slot; "
             "skipping slot creation and initial copy");
    co_return {};
  }

  // WITH (copy_data = false): don't copy existing rows -- just create the slot
  // (no snapshot needed) and stream changes from now on.
  if (!_target.copy_data) {
    SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
             "' creating slot without initial copy (copy_data = false)");
    co_await RunSimpleCommand("CREATE_REPLICATION_SLOT " +
                              QuoteIdent(_target.slot_name) +
                              " LOGICAL pgoutput (SNAPSHOT 'nothing')");
    co_return {};
  }

  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' starting initial table sync");

  // A single REPEATABLE READ transaction pins one snapshot for the slot and
  // every table COPY, so the copied data and the streaming start point are
  // mutually consistent (no gap, no overlap).
  if (!co_await RunSimpleCommand(
        "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")) {
    co_return {};
  }
  if (!co_await RunSimpleCommand("CREATE_REPLICATION_SLOT " +
                                 QuoteIdent(_target.slot_name) +
                                 " LOGICAL pgoutput (SNAPSHOT 'use')")) {
    co_await RunSimpleCommand("COMMIT");
    co_return {};
  }

  std::string pub_list;
  for (size_t i = 0; i < _target.publications.size(); ++i) {
    if (i != 0) {
      pub_list += ", ";
    }
    pub_list += QuoteLiteral(_target.publications[i]);
  }
  // Modern (pg_get_publication_tables) metadata fetch, shaped server-side so
  // each row is one published column (in publication order) plus the table's
  // row filter as text -- honoring column lists and row filters, with no
  // client-side array/expression parsing.
  std::vector<std::vector<std::optional<std::string>>> rows;
  co_await RunQueryRows(
    "SELECT n.nspname, c.relname, a.attname,"
    " pg_get_expr(gpt.qual, gpt.relid), p.pubname"
    " FROM pg_publication p"
    " JOIN LATERAL pg_get_publication_tables(p.pubname) gpt ON true"
    " JOIN pg_class c ON c.oid = gpt.relid"
    " JOIN pg_namespace n ON n.oid = c.relnamespace"
    " JOIN unnest(gpt.attrs) WITH ORDINALITY AS pa(attnum, ord) ON true"
    " JOIN pg_attribute a ON a.attrelid = gpt.relid AND a.attnum = pa.attnum"
    " WHERE p.pubname IN (" +
      pub_list + ") ORDER BY n.nspname, c.relname, p.pubname, pa.ord",
    rows);

  struct TableAgg {
    std::string schema;
    std::string table;
    std::string first_pub;  // columns are taken from a single publication
    std::vector<std::string> columns;
    bool any_no_filter = false;        // a publication with no row filter wins
    std::vector<std::string> filters;  // distinct filters, OR'd together
  };
  std::vector<TableAgg> tables;
  containers::FlatHashMap<std::string, size_t> index;
  for (const auto& row : rows) {
    if (row.size() < 5 || !row[0] || !row[1] || !row[2] || !row[4]) {
      continue;
    }
    const std::string key = *row[0] + '\0' + *row[1];
    auto [it, inserted] = index.try_emplace(key, tables.size());
    if (inserted) {
      tables.push_back(TableAgg{*row[0], *row[1], *row[4], {}, false, {}});
    }
    auto& agg = tables[it->second];
    if (*row[4] == agg.first_pub) {
      agg.columns.push_back(*row[2]);
    }
    if (!row[3]) {
      agg.any_no_filter = true;
    } else if (std::find(agg.filters.begin(), agg.filters.end(), *row[3]) ==
               agg.filters.end()) {
      agg.filters.push_back(*row[3]);
    }
  }

  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  std::vector<std::shared_ptr<catalog::Table>> locals(tables.size());
  for (size_t i = 0; i < tables.size(); ++i) {
    const auto& agg = tables[i];
    if (agg.columns.empty()) {
      continue;
    }
    auto local = snapshot->GetTable(catalog::NoAccessCheck(),
                                    _target.database_id, agg.schema, agg.table);
    if (!local) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' initial sync: no local table ", agg.schema, ".", agg.table,
               "; skipped");
      continue;
    }
    locals[i] = std::move(local);
  }

  // Copy referenced (parent) tables before referencing (child) ones so each
  // table's rows exist before an FK pointing at them is checked.
  for (const size_t i : ReferencedFirstOrder(locals)) {
    if (!locals[i]) {
      continue;
    }
    const auto& agg = tables[i];
    std::optional<std::string> row_filter;
    if (!agg.any_no_filter && !agg.filters.empty()) {
      std::string combined;
      for (size_t j = 0; j < agg.filters.size(); ++j) {
        if (j != 0) {
          combined += " OR ";
        }
        combined += "(" + agg.filters[j] + ")";
      }
      row_filter = std::move(combined);
    }
    co_await CopyOneTable(agg.schema, agg.table, agg.columns, row_filter);
  }

  co_await RunSimpleCommand("COMMIT");
  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' initial table sync complete");
  co_return {};
}

yaclib::Task<> PgReplicationClient::CopyOneTable(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& columns,
  const std::optional<std::string>& row_filter) {
  std::string col_list;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i != 0) {
      col_list += ", ";
    }
    col_list += QuoteIdent(columns[i]);
  }
  const std::string qtable = QuoteIdent(schema) + "." + QuoteIdent(table);
  // Binary only when opted in AND the publisher is v16+ (pre-v16 initial sync
  // is always text even with binary = true) -- exactly PG's copy_table rule.
  const bool use_binary = _target.binary && _server_version >= 16;

  // Remote COPY ... TO STDOUT (a command to the publisher, so text is fine
  // here). With a row filter it becomes COPY (SELECT ...) like PG's copy_table.
  WriteFrame(this->_send, 'Q', [&](message::Writer& w) {
    if (row_filter) {
      w.Write("COPY (SELECT ");
      w.Write(col_list);
      w.Write(" FROM ONLY ");
      w.Write(qtable);
      w.Write(" WHERE ");
      w.Write(*row_filter);
      w.Write(") TO STDOUT");
    } else {
      w.Write("COPY ");
      w.Write(qtable);
      w.Write(" (");
      w.Write(col_list);
      w.Write(") TO STDOUT");
    }
    if (use_binary) {
      w.Write(" WITH (FORMAT binary)");
    }
    w.Write("\0");
  });
  this->KickSend();

  // Pipe the publisher's CopyData straight into a local COPY ... FROM STDIN,
  // built as an AST (never SQL text), as a normal wire session ingests a client
  // COPY -- the byte streams match.
  sdb::pg::CopyInBridge bridge;
  this->_connection_ctx->SetCopyInBridge(&bridge);
  _copy_stmt = BuildCopyFromStdin(schema, table, columns, use_binary);
  _copy_done.Reset();
  _copy_phase.store(CopyPhase::Job, std::memory_order_release);
  this->_task->RequestRun();

  bool eof_sent = false;
  for (;;) {
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      break;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type == 'd') {  // CopyData
      if (!bridge.Aborted() && !eof_sent && !payload.empty()) {
        bridge.Publish(payload.data(), payload.size());
        co_await bridge.Drained(*this->_ioexec);
        bridge.ResetDrained();
      }
      this->_frames.Consume(frame);
      continue;
    }
    if (type == 'c') {  // CopyDone
      if (!bridge.Aborted() && !eof_sent) {
        bridge.Finish();
        eof_sent = true;
      }
      this->_frames.Consume(frame);
      continue;
    }
    if (type == 'E') {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' initial copy of ", schema, ".", table, " rejected");
      this->_frames.Consume(frame);
      continue;  // keep draining to ReadyForQuery
    }
    this->_frames.Consume(frame);
    if (type == 'Z') {  // ReadyForQuery ends the command
      break;
    }
    // 'H' (CopyOutResponse), 'C' (CommandComplete), notices: nothing to do.
  }
  if (!eof_sent) {  // aborted / error before CopyDone -- unblock the worker
    bridge.Finish();
  }

  co_await _copy_done.AwaitOn(*this->_ioexec);
  _copy_done.Reset();
  this->_connection_ctx->SetCopyInBridge(nullptr);
  if (!_copy_ok) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' initial copy of ", schema, ".", table, " failed");
  }
  co_return {};
}

yaclib::Task<bool> PgReplicationClient::RunLocalCopy() {
  if (this->_client_state != nullptr) {
    this->_client_state->copy_stdin_open_count = 0;
    this->_client_state->copy_stdin_done = false;
  }
  auto* bridge = this->_connection_ctx->GetCopyInBridge();
  try {
    connector::GetSereneDBContext(*this->_conn->context)
      .AcquireCatalogSnapshot();
    auto prepared = this->_conn->Prepare(std::move(_copy_stmt));
    if (prepared->HasError()) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' initial copy prepare failed: ", prepared->GetError());
      if (bridge != nullptr) {
        bridge->Abort();
      }
      co_return false;
    }
    duckdb::vector<duckdb::Value> params;
    auto pending =
      prepared->PendingQuery(params, /*allow_stream_result=*/false);
    if (pending->HasError()) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' initial copy failed: ", pending->GetError());
      if (bridge != nullptr) {
        bridge->Abort();
      }
      co_return false;
    }
    co_await this->DriveQuery(*pending);
    auto result = pending->Execute();
    if (result->HasError()) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' initial copy failed: ", result->GetError());
      if (bridge != nullptr) {
        bridge->Abort();
      }
      co_return false;
    }
  } catch (const std::exception& ex) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' initial copy failed: ", ex.what());
    if (bridge != nullptr) {
      bridge->Abort();
    }
    co_return false;
  }
  co_return true;
}

yaclib::Task<> PgReplicationClient::Feeder() {
  // io-side: read the CopyBoth stream, decode each pgoutput message, and hand
  // it to the duck-side apply through _stream (lock-step -- the message's views
  // alias the live frame, valid until the apply consumes it). Keepalives and
  // standby feedback are handled here so _send stays single-threaded (io).
  for (;;) {
    if (this->SendBroken() || _stream.Aborted()) {
      break;
    }
    auto frame = co_await NextFrame(FrameKind::Typed, this->_max_message);
    if (frame.status != FrameStatus::Ok) {
      break;
    }
    const char type = frame.type;
    const std::string_view payload = frame.payload;
    if (type != 'd' || payload.empty()) {
      this->_frames.Consume(frame);
      if (type == 'c' || type == 'E') {
        break;
      }
      continue;
    }

    const char sub = payload[0];
    if (sub == 'k') {  // primary keepalive: int64 walEnd, sendTime, byte reply
      const bool reply = payload.size() > 17 && payload[17] != 0;
      if (payload.size() >= 9) {
        const uint64_t wal_end = absl::big_endian::Load64(payload.data() + 1);
        if (wal_end > _received_lsn.load(std::memory_order_relaxed)) {
          _received_lsn.store(wal_end, std::memory_order_relaxed);
        }
      }
      this->_frames.Consume(frame);
      if (reply) {
        SendFeedback();
      }
      continue;
    }
    if (sub != 'w' || payload.size() <= 25) {
      this->_frames.Consume(frame);
      continue;
    }

    // 'w': int64 walStart, walEnd, sendTime, then one pgoutput message.
    const uint64_t wal_end = absl::big_endian::Load64(payload.data() + 9);
    if (wal_end > _received_lsn.load(std::memory_order_relaxed)) {
      _received_lsn.store(wal_end, std::memory_order_relaxed);
    }
    PgOutputMessage decoded;
    try {
      decoded = DecodePgOutput(payload.substr(25));
    } catch (const std::exception& ex) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' decode error: ", ex.what());
      this->_frames.Consume(frame);
      continue;
    }
    // Publish and wait for the apply to consume it before advancing the frame
    // (the decoded message's tuple views point into this frame's buffer).
    if (!_stream.Publish(&decoded)) {
      break;
    }
    co_await _stream.Drained(*this->_ioexec);
    _stream.ResetDrained();
    this->_frames.Consume(frame);
  }
  _stream.Finish();
  co_return {};
}

yaclib::Future<> PgReplicationClient::ReplicationMain() {
  co_await this->_task->Park();
  absl::Cleanup finish = [this] { this->_task->Finish(); };

  _setup_ok = SetupApplyConnection();
  _setup_done.Set();
  if (!_setup_ok) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' cannot open local database ", _target.database_name);
    _stream.Abort();
    this->Stop();
    co_return {};
  }

  // Initial-sync phase: run each local COPY the io side posts (it feeds the
  // matching CopyInBridge), until the io side signals the switch to streaming.
  for (;;) {
    while (_copy_phase.load(std::memory_order_acquire) == CopyPhase::Waiting) {
      if (this->SendBroken()) {
        break;
      }
      co_await this->_task->Park();
    }
    if (this->SendBroken()) {
      _stream.Abort();
      this->Stop();
      co_return {};
    }
    if (_copy_phase.load(std::memory_order_acquire) == CopyPhase::Stream) {
      break;
    }
    _copy_ok = co_await RunLocalCopy();
    _copy_phase.store(CopyPhase::Waiting, std::memory_order_release);
    _copy_done.Set();
  }

  // Control loop: pull decoded messages from the feeder. Between batches the
  // task parks (idle-friendly); Begin/Commit/Relation are handled here, and a
  // run of same-(op,relation) rows becomes one batched DML whose scan drains
  // the run off _stream -- deserializing each row inside execution.
  for (;;) {
    while (!_stream.Ready()) {
      if (this->SendBroken()) {
        break;
      }
      co_await this->_task->Park();
    }
    if (this->SendBroken()) {
      _stream.Abort();
      break;
    }

    const PgOutputMessage* msg = nullptr;
    try {
      msg = _stream.Current();
    } catch (const std::exception& ex) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' stream error: ", ex.what());
      break;
    }
    if (msg == nullptr) {  // clean EOF
      break;
    }

    try {
      if (std::holds_alternative<BeginMessage>(*msg)) {
        co_await BeginTxn();
        _stream.Advance();
      } else if (auto* m = std::get_if<CommitMessage>(msg)) {
        if (m->end_lsn > _flushed_lsn.load(std::memory_order_relaxed)) {
          _flushed_lsn.store(m->end_lsn, std::memory_order_relaxed);
        }
        co_await CommitTxn();
        _stream.Advance();
      } else if (auto* m = std::get_if<RelationMessage>(msg)) {
        OnRelation(*m);
        _stream.Advance();
      } else if (auto* m = std::get_if<TruncateMessage>(msg)) {
        co_await ApplyTruncate(*m);
        _stream.Advance();
      } else {
        // A row change, or a message we don't act on.
        const auto relid = RowRelId(*msg);
        if (!relid) {  // Type/Origin/Truncate/... -- not applied
          _stream.Advance();
          continue;
        }
        const auto* rel = Relation(*relid);
        if (rel == nullptr || !rel->mapped) {  // unmapped table: skip
          _stream.Advance();
          continue;
        }
        char op = 0;
        std::vector<size_t> keys;
        std::vector<size_t> cols;
        std::vector<PgColumn> cells;
        bool full = false;
        if (!RowShape(*msg, *rel, op, keys, cols, cells, full)) {  // malformed
          _stream.Advance();
          continue;
        }
        _batch.op = op;
        _batch.relid = *relid;
        _batch.rel = rel;
        _batch.keys = std::move(keys);
        _batch.cols = std::move(cols);
        _batch.full = full;
        if (!StartBatch()) {
          _stream.Advance();  // prepare failed: skip this row
          continue;
        }
        // The scan consumes this row and the run after it; the boundary message
        // it stops on is handled on the next iteration (do not Advance here).
        if (!co_await RunBatchExec()) {
          if (_target.disable_on_error) {
            _disable_requested.store(true, std::memory_order_release);
          }
          _stream.Abort();
          break;
        }
      }
    } catch (const std::exception& ex) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' apply error: ", ex.what());
      if (_target.disable_on_error) {
        _disable_requested.store(true, std::memory_order_release);
      }
      _stream.Abort();
      break;
    }
  }
  SDB_INFO(REPLICATION, "subscription '", _target.subscription_name,
           "' stream ended");
  this->Stop();
  co_return {};
}

bool PgReplicationClient::SetupApplyConnection() {
  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto database = snapshot->GetDatabase(_target.database_id);
  if (!database) {
    return false;
  }
  // Apply with the subscription owner's privileges (PG-like RBAC). Fall back to
  // the internal user only when the owner is unresolved (e.g. bootstrap).
  const std::string_view user =
    _target.owner_name.empty() ? std::string_view{StaticStrings::kDefaultUser}
                               : std::string_view{_target.owner_name};
  const ObjectId role_id =
    _target.owner_name.empty() ? id::kRootUser : _target.owner_id;
  this->_conn = DuckDBEngine::Instance().CreateConnection();
  this->_txn_state.emplace(this->_conn->context->transaction);
  this->_connection_ctx = std::make_shared<ConnectionContext>(
    *this->_conn->context, user, role_id, _target.database_name,
    _target.database_id, std::move(database), /*send_buffer=*/nullptr,
    /*copy_queue=*/nullptr, /*backend_pid=*/0, /*cancel_registry=*/nullptr);
  this->_client_state = &connector::SereneDBClientState::Register(
    *this->_conn->context, this->_connection_ctx);
  _batch.stream = &_stream;
  // repl_src() reads the current batch off the connection at bind (Prepare) and
  // at execute; _batch is a stable member always holding the current batch, so
  // publish it once here.
  this->_connection_ctx->SetReplBatch(&_batch);
  this->_conn->context->session_user = std::string{user};
  std::vector<duckdb::CatalogSearchEntry> paths{duckdb::CatalogSearchEntry{
    duckdb::Identifier{_target.database_name}, duckdb::Identifier{"public"}}};
  this->_conn->context->client_data->catalog_search_path->SetDefaultPaths(
    std::vector{paths});
  this->_conn->context->client_data->catalog_search_path->Set(
    std::move(paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
  return true;
}

const RelInfo* PgReplicationClient::Relation(uint32_t relation_id) const {
  auto it = _relations.find(relation_id);
  return it == _relations.end() ? nullptr : &it->second;
}

void PgReplicationClient::OnRelation(const RelationMessage& msg) {
  RelInfo info;
  info.schema = msg.namespace_name.empty() ? "pg_catalog" : msg.namespace_name;
  info.table = msg.relation_name;

  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto table = snapshot->GetTable(catalog::NoAccessCheck(), _target.database_id,
                                  info.schema, info.table);
  if (!table) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' has no local table ", info.schema, ".", info.table,
             "; its changes are skipped");
    _relations[msg.relation_id] = std::move(info);
    return;
  }
  info.mapped = true;
  info.columns.reserve(msg.columns.size());
  for (const auto& rc : msg.columns) {
    RelColumn col;
    col.name = rc.name;
    col.is_key = rc.is_key;
    for (const auto& lc : table->Columns()) {
      if (lc.GetName() == rc.name) {
        col.type = lc.type;
        break;
      }
    }
    info.columns.push_back(std::move(col));
  }
  _relations[msg.relation_id] = std::move(info);
}

yaclib::Task<> PgReplicationClient::BeginTxn() {
  if (!_in_txn) {
    this->_txn_state->Arm();
    _in_txn = true;
  }
  co_return {};
}

yaclib::Task<> PgReplicationClient::CommitTxn() {
  if (_in_txn) {
    this->_txn_state->Commit();
    _in_txn = false;
  }
  // Advance the slot: feedback touches _send, so send it on the io thread.
  PostFeedback();
  co_return {};
}

yaclib::Task<> PgReplicationClient::ApplyTruncate(const TruncateMessage& msg) {
  // Runs inside the pgoutput transaction (Begin already armed _txn_state).
  // restart_identity isn't modelled; cascade is already expanded into the
  // relation set by the publisher, so we just truncate every mapped relation --
  // referencing (child) tables before referenced (parent) ones, since DuckDB
  // rejects truncating a table still referenced by an FK.
  connector::GetSereneDBContext(*this->_conn->context).AcquireCatalogSnapshot();
  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  std::vector<std::shared_ptr<catalog::Table>> locals;
  std::vector<const RelInfo*> rels;
  for (uint32_t relid : msg.relation_ids) {
    const auto* rel = Relation(relid);
    if (rel == nullptr || !rel->mapped) {
      continue;
    }
    locals.push_back(snapshot->GetTable(
      catalog::NoAccessCheck(), _target.database_id, rel->schema, rel->table));
    rels.push_back(rel);
  }
  auto order = ReferencedFirstOrder(locals);
  std::reverse(order.begin(), order.end());
  for (const size_t i : order) {
    const RelInfo* rel = rels[i];
    auto prepared =
      this->_conn->Prepare(BuildTruncate(rel->schema, rel->table));
    if (prepared->HasError()) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' truncate prepare failed: ", prepared->GetError());
      continue;
    }
    duckdb::vector<duckdb::Value> params;
    auto pending =
      prepared->PendingQuery(params, /*allow_stream_result=*/false);
    if (pending->HasError()) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' truncate failed: ", pending->GetError());
      continue;
    }
    co_await this->DriveQuery(*pending);
    auto result = pending->Execute();
    if (result->HasError()) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' truncate failed: ", result->GetError());
    }
  }
  co_return {};
}

bool PgReplicationClient::StartBatch() {
  const RelInfo& rel = *_batch.rel;
  // Binding the target table + repl_src resolves them through the catalog, and
  // the scan reads the snapshot to decode catalog-typed values, so a snapshot
  // must be in scope for the whole batch (prepare + decode + execute).
  connector::GetSereneDBContext(*this->_conn->context).AcquireCatalogSnapshot();
  _batch.snapshot = this->_connection_ctx->CatalogSnapshot();

  const auto names = [&](const std::vector<size_t>& idxs) {
    std::vector<std::string> v;
    v.reserve(idxs.size());
    for (size_t i : idxs) {
      v.push_back(rel.columns[i].name);
    }
    return v;
  };

  // Keyed only by (op, relation, column set); the rows flow in through
  // repl_src, so the same statement re-executes for every run of that shape.
  StmtKey key{_batch.op, _batch.relid, _batch.keys, _batch.cols, _batch.full};
  auto it = _stmts.find(key);
  if (it == _stmts.end()) {
    auto stmt =
      BuildReplStatement(_batch.op, rel.schema, rel.table, names(_batch.keys),
                         names(_batch.cols), _batch.full);
    it = _stmts.emplace(std::move(key), this->_conn->Prepare(std::move(stmt)))
           .first;
  }
  _batch_prepared = it->second.get();
  if (_batch_prepared->HasError()) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' prepare failed: ", _batch_prepared->GetError());
    _batch_prepared = nullptr;
    return false;
  }
  return true;
}

yaclib::Task<bool> PgReplicationClient::RunBatchExec() {
  bool ok = true;
  _stream.ScanActive(true);
  try {
    duckdb::vector<duckdb::Value> params;
    auto pending =
      _batch_prepared->PendingQuery(params, /*allow_stream_result=*/false);
    if (pending->HasError()) {
      SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
               "' apply failed: ", pending->GetError());
      ok = false;
    } else {
      co_await this->DriveQuery(*pending);
      auto result = pending->Execute();
      if (result->HasError()) {
        SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
                 "' apply failed: ", result->GetError());
        ok = false;
      }
    }
  } catch (const std::exception& ex) {
    SDB_WARN(REPLICATION, "subscription '", _target.subscription_name,
             "' apply failed: ", ex.what());
    ok = false;
  }
  _stream.ScanActive(false);
  co_return ok;
}

void PgReplicationClient::SendFeedback() {
  if (this->SendBroken()) {
    return;
  }
  const uint64_t received = _received_lsn.load(std::memory_order_relaxed);
  const uint64_t flushed = _flushed_lsn.load(std::memory_order_relaxed);
  // Standby status update: 'r' write, flush, apply, clock, replyRequested.
  WriteFrame(this->_send, 'd', [&](message::Writer& w) {
    w.Write("r");
    PutU64(w, received);
    PutU64(w, flushed);
    PutU64(w, flushed);
    PutU64(w, 0);   // clock (informational)
    w.Write("\0");  // no reply requested
  });
  this->KickSend();
}

void PgReplicationClient::PostFeedback() {
  // _send is only touched on the io thread (the feeder + the writer); the
  // control loop runs on the duck task, so it hands the feedback send to io.
  auto self = this->shared_from_this();
  asio_ns::post(this->_io, [self, this] { SendFeedback(); });
}

}  // namespace sdb::replication
