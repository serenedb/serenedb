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

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <limits>
#include <memory>
#include <string>

#include "quack_fetch_collector.hpp"

namespace sdb::network::http::quack {

class Session;

enum class QueryState : uint8_t {
  Idle,
  Active,
  Finished,
  Cancelled,
};

// A client's last query, kept so that a client that reconnects gets the same
// bytes again. It holds the stream's payloads past the client's acks.
struct ResultCache {
  ResultCache(std::string sql, duckdb::hugeint_t query_uuid,
              duckdb::shared_ptr<duckdb::QuackResultStream> stream,
              std::atomic<size_t>& live)
    : sql{std::move(sql)},
      query_uuid{query_uuid},
      stream{std::move(stream)},
      last_served_at{duckdb::Timestamp::GetCurrentTimestamp()},
      live{live} {
    live.fetch_add(1, std::memory_order_relaxed);
  }
  ResultCache(const ResultCache&) = delete;
  ResultCache& operator=(const ResultCache&) = delete;
  ~ResultCache() { live.fetch_sub(1, std::memory_order_relaxed); }

  std::string sql;
  duckdb::hugeint_t query_uuid;
  duckdb::shared_ptr<duckdb::QuackResultStream> stream;
  size_t retained_rows = 0;
  duckdb::timestamp_t last_served_at;
  std::atomic<size_t>& live;
};

// The statement a session runs, whose stream the client fetches.
struct Statement {
  absl::Mutex mu;
  duckdb::shared_ptr<duckdb::QuackResultStream> stream ABSL_GUARDED_BY(mu);
  duckdb::hugeint_t query_uuid ABSL_GUARDED_BY(mu) = 0;
  // A late FETCH of this query gets this error once the stream is gone.
  duckdb::ErrorData abort_error ABSL_GUARDED_BY(mu);
};

// A client's session on this server: the SQL session it runs on, its lease
// and the statement whose result it reads.
struct Connection {
  using Clock = std::chrono::steady_clock;

  Connection(std::string id, uint64_t lease_seconds,
             duckdb::StorageVersion wire_version, std::string client_id_hash,
             duckdb::shared_ptr<Session> session);
  ~Connection();

  // Renews the lease unless it ran out: an expired lease stays expired.
  bool TryRenewLease();
  bool LeaseExpired();

  void MarkQueryFinished() {
    auto expected = QueryState::Active;
    query_state.compare_exchange_strong(expected, QueryState::Finished);
  }

  void ClearResultCache() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu) {
    result_cache.reset();
    cached_rows.store(kNoCache, std::memory_order_relaxed);
  }

  void SyncCachedRows() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu) {
    cached_rows.store(result_cache ? result_cache->retained_rows : kNoCache,
                      std::memory_order_relaxed);
  }

  static constexpr size_t kNoCache = std::numeric_limits<size_t>::max();

  const std::string id;
  const uint64_t lease_seconds;
  const duckdb::StorageVersion wire_version;
  // HMAC of the client's CLIENT_ID under this server's key: stable across the
  // client's reconnects, empty without a client id.
  const std::string client_id_hash;
  const duckdb::shared_ptr<Session> session;

  absl::Mutex lease_mu;
  Clock::time_point lease_renewed_at ABSL_GUARDED_BY(lease_mu);
  bool lease_expired ABSL_GUARDED_BY(lease_mu) = false;

  absl::Mutex mu;
  std::unique_ptr<ResultCache> result_cache ABSL_GUARDED_BY(mu);
  bool in_expiry_queue ABSL_GUARDED_BY(mu) = false;
  std::string sql ABSL_GUARDED_BY(mu);
  duckdb::timestamp_t query_started_at ABSL_GUARDED_BY(mu){0};
  std::atomic<QueryState> query_state{QueryState::Idle};
  std::atomic<size_t> cached_rows{kNoCache};

  Statement statement;

 private:
  bool LeaseExpiredLocked(Clock::time_point now)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(lease_mu);
};

}  // namespace sdb::network::http::quack
