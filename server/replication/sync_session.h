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

#include <atomic>
#include <cstdint>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/parser/parsed_data/create_subscription_info.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/async/promise.hpp>
#include <yaclib/coro/task.hpp>

#include "replication/conninfo.h"
#include "replication/publisher_session.h"

namespace duckdb {

class AttachedDatabase;
class SQLStatement;
class TableCatalogEntry;

}  // namespace duckdb
namespace sdb::catalog {

class SereneDBCatalog;
class SubscriptionCatalogEntry;

}  // namespace sdb::catalog
namespace sdb::replication {

struct ReplicationTarget {
  duckdb::idx_t subscription_oid = 0;
  duckdb::idx_t database_oid = 0;
  std::string database_name;
  std::string subscription_name;
  ConnInfo conninfo;
  std::vector<std::string> publications;
  std::string slot_name;
  bool binary = false;
  bool streaming = false;
  bool disable_on_error = false;
  bool run_as_owner = false;
  bool require_password = false;
  std::string origin = "any";
  uint64_t start_lsn = 0;
  uint64_t skip_lsn = 0;
  duckdb::idx_t owner_id = 0;
  std::string owner_name;
  std::vector<duckdb::SubscriptionRelation> relations;

  bool operator==(const ReplicationTarget&) const = default;
};

struct SyncTable {
  std::string schema;
  std::string table;
  std::vector<std::string> columns;
  std::optional<std::string> row_filter;
  bool partitioned = false;
  bool generated = false;
  duckdb::idx_t owner = 0;
  size_t relation = 0;
  duckdb::idx_t sync_id = 0;
};

void CheckTargetColumns(std::string_view schema, std::string_view table,
                        const std::vector<std::string>& missing,
                        const std::vector<std::string>& generated);

class SyncSession : public PublisherSession {
 public:
  SyncSession(network::IoExecutor& exec, ReplicationTarget target,
              size_t host_index);

  bool DisableRequested() const noexcept {
    return _disable_requested.load(std::memory_order_acquire);
  }
  bool Transient() const noexcept {
    return _transient.load(std::memory_order_acquire);
  }
  const irs::pg::SqlErrorData& LastError() const noexcept {
    return _apply_error.errmsg.empty() ? Error() : _apply_error;
  }

 protected:
  enum class Job : uint8_t {
    None,
    Begin,
    Copy,
    Commit,
    Rollback,
    Stream,
  };

  yaclib::Task<bool> RunJob(Job job);
  yaclib::Task<bool> SyncOne(SyncTable& table, uint64_t lsn, bool binary);
  yaclib::Task<bool> RunJobs();
  void FinishJobs();
  bool SetupApplyConnection();
  void ApplyFailed(irs::pg::SqlErrorData error);
  duckdb::optional_ptr<duckdb::TableCatalogEntry> LookupTable(
    std::string_view schema, std::string_view table);
  catalog::SereneDBCatalog& Catalog() const;
  catalog::SubscriptionCatalogEntry* VisibleSubscription() const;
  catalog::SubscriptionCatalogEntry& RequireSubscription() const;
  yaclib::Task<std::optional<int64_t>> RunPrepared(
    duckdb::PreparedStatement& prepared);
  void UseRole(duckdb::idx_t table_owner);

  ReplicationTarget _target;
  duckdb::shared_ptr<duckdb::AttachedDatabase> _database;
  std::atomic<bool> _disable_requested{false};
  std::atomic<bool> _transient{false};
  irs::pg::SqlErrorData _apply_error;
  bool _in_txn = false;
  bool _setup_ok = false;
  yaclib::OneShotEvent _setup_done;

 private:
  yaclib::Task<bool> CopyTable(const SyncTable& table, bool binary);
  yaclib::Task<bool> RunDuckJob(Job job);
  bool BeginSync();
  yaclib::Task<bool> RunLocalCopy();
  bool CommitSync();

  std::atomic<Job> _job{Job::None};
  std::atomic<bool> _jobs_closed{false};
  bool _job_ok = false;
  yaclib::OneShotEvent _job_done;
  SyncTable* _sync_table = nullptr;
  uint64_t _sync_lsn = 0;
  duckdb::unique_ptr<duckdb::SQLStatement> _copy_stmt;
};

struct SyncPlan {
  std::vector<SyncTable>& tables;
  std::string snapshot;
  uint64_t lsn = 0;
  bool binary = false;
  std::atomic<size_t> next{0};
  std::vector<uint8_t> done;
};

class TableSyncWorker final : public SyncSession {
 public:
  TableSyncWorker(network::IoExecutor& exec, ReplicationTarget target,
                  SyncPlan& plan);

  void Start(yaclib::Promise<bool> promise);

 private:
  yaclib::Task<> Run(yaclib::Promise<bool> promise);
  yaclib::Future<> LocalMain();

  SyncPlan& _plan;
};

}  // namespace sdb::replication
