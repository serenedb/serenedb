////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <chrono>
#include <limits>
#include <memory>
#include <vector>

#include "basics/assert.h"
#include "basics/common.h"
#include "basics/errors.h"
#include "basics/result.h"
#include "catalog/database.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/identifiers/transaction_id.h"
#include "catalog/index.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/types.h"
#include "database/access_mode.h"
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "storage_engine/health_data.h"
#include "utils/exec_context.h"
#include "utils/operation_origin.h"

namespace vpack {
class Slice;
class Builder;
}  // namespace vpack
namespace sdb {

enum class RecoveryState : uint32_t {
  /// recovery is not yet started
  Before = 0,

  /// recovery is in progress
  InProgress,

  /// recovery is done
  Done,
};

namespace aql {

class OptimizerRulesFeature;

}  // namespace aql

class ReplicationClientsProgressTracker;
class DatabaseInitialSyncer;

class Result;
class TransactionTable;
class TransactionState;
class WalAccess;
class Index;

namespace rest {

class RestHandlerFactory;

}  // namespace rest
namespace transaction {

class Context;
class Manager;
class ManagerFeature;
class Methods;
struct Options;

}  // namespace transaction

class StorageSnapshot {
 public:
  StorageSnapshot() = default;
  StorageSnapshot(const StorageSnapshot&) = delete;
  StorageSnapshot& operator=(const StorageSnapshot&) = delete;
  virtual ~StorageSnapshot() = default;

  virtual Tick tick() const noexcept = 0;
};

struct IndexFactory {
  virtual ~IndexFactory() = default;

  virtual bool equal(vpack::Slice lhs, vpack::Slice rhs,
                     ObjectId database) const = 0;

  virtual std::shared_ptr<Index> instantiate(
    catalog::Table& collection, vpack::Slice definition, IndexId id,
    bool is_cluster_constructor) const = 0;

  virtual Result normalize(catalog::Table& collection,
                           vpack::Builder& normalized,
                           vpack::Slice definition) const = 0;
};

// Read from storage engine if unknown
static constexpr auto kRead = std::numeric_limits<uint64_t>::max();

struct IndexTombstone {
  ObjectId id;
  IndexType type = IndexType::kTypeUnknown;
  uint64_t number_documents = kRead;
  bool unique = false;
};

struct TableTombstone {
  ObjectId table;
  uint64_t number_documents = kRead;
  std::vector<IndexTombstone> indexes;
};

class StorageEngine : public SerenedFeature {
 public:
  using WriteProperties = absl::FunctionRef<vpack::Slice(bool internal)>;

  StorageEngine(Server& server, std::string_view engine,
                std::string_view feature);

  virtual HealthData healthCheck() { return {}; }

  virtual std::shared_ptr<TransactionState> createTransactionState(
    std::shared_ptr<const ExecContext> exec_context, TransactionId tid,
    const transaction::Options& options, transaction::OperationOrigin origin) {
    SDB_ASSERT(false);
    return {};
  }

  virtual Result createTableShard(const catalog::Table& table, bool is_new,
                                  std::shared_ptr<TableShard>& physical) {
    SDB_ASSERT(false);
    return {ERROR_NOT_IMPLEMENTED};
  }

  // return the name of the specific storage engine e.g. rocksdb
  // TODO(mbkkt) remove it and everything related to name
  virtual std::string_view typeName() const { return _type_name; }

  virtual Result VisitDatabases(
    absl::FunctionRef<Result(vpack::Slice database)> visitor) {
    return {};
  }

  // TODO: don't use rocksdb types?
  virtual Result VisitObjects(
    ObjectId database_id, RocksDBEntryType entry,
    absl::FunctionRef<Result(rocksdb::Slice, vpack::Slice)> visitor) {
    return {};
  }
  virtual Result VisitSchemaObjects(
    ObjectId database_id, ObjectId schema_id, RocksDBEntryType entry,
    absl::FunctionRef<Result(rocksdb::Slice, vpack::Slice)> visitor) {
    return {};
  }

  // return the absolute path for the VERSION file of a database
  // TODO(mbkkt) is it needed here?
  virtual std::string versionFilename(ObjectId id) const = 0;

  // return the path for a database
  // TODO(mbkkt) is it expected for dbservers?
  virtual std::string databasePath() const { return {}; }

  virtual Result flushWal(bool wait_for_sync = false,
                          bool flush_column_families = false) = 0;

  virtual void waitForEstimatorSync() = 0;

  // TODO(mbkkt) shouldn't be here
  virtual Result createDatabase(ObjectId id, vpack::Slice slice) { return {}; }

  virtual Result dropDatabase(ObjectId database_id) {
    SDB_ASSERT(false);
    return {ERROR_NOT_IMPLEMENTED};
  }

  bool inRecovery() { return recoveryState() < RecoveryState::Done; }

  virtual RecoveryState recoveryState() = 0;

  virtual Tick recoveryTick() = 0;

  virtual void createTable(const catalog::Table& table, TableShard& physical) {
    SDB_ASSERT(false);
  }
  virtual Result CreateIndex(const catalog::Index& index) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual Result MarkDeleted(const catalog::Table& table,
                             const TableShard& physical,
                             const TableTombstone& tombstone) {
    return {ERROR_NOT_IMPLEMENTED};
  };
  virtual Result MarkDeleted(const catalog::Index& index,
                             const IndexTombstone& tombstone) {
    return {ERROR_NOT_IMPLEMENTED};
  };

  virtual Result MarkDeleted(const catalog::Database& database) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual Result MarkDeleted(const catalog::Schema& schema) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual void prepareDropTable(ObjectId table) {}

  virtual Result DropTable(const TableTombstone& tombstone) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual Result DropIndex(IndexTombstone tombstone) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual void ChangeTable(const catalog::Table& collection,
                           const TableShard& physical) {
    SDB_ASSERT(false);
  }

  virtual Result RenameTable(const catalog::Table& collection,
                             const TableShard& physical,
                             std::string_view old_name) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result CreateFunction(ObjectId db, ObjectId schema_id, ObjectId id,
                                WriteProperties properties) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result DropFunction(ObjectId db, ObjectId schema_id, ObjectId id,
                              std::string_view name) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result CreateSchema(ObjectId db, ObjectId id,
                              WriteProperties properties) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual Result ChangeSchema(ObjectId db, ObjectId id,
                              WriteProperties properties) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual Result DropSchema(ObjectId db, ObjectId id) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result ChangeView(ObjectId db, ObjectId schema_id, ObjectId id,
                            WriteProperties properties) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result CreateView(ObjectId db, ObjectId schema_id, ObjectId id,
                            WriteProperties properties) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result DropView(ObjectId db, ObjectId schema_id, ObjectId id,
                          std::string_view name) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result ChangeRole(ObjectId id, WriteProperties properties) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result CreateRole(const catalog::Role& role) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result DropRole(const catalog::Role& role) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  // Compacts the entire database
  virtual yaclib::Future<Result> compactAll(bool change_level,
                                            bool compact_bottom_most_level) = 0;

  virtual void fillSystemIndexes(
    catalog::Table& col,
    std::vector<std::shared_ptr<Index>>& system_indexes) const {
    SDB_ASSERT(false);
  }

  virtual void prepareIndexes(
    catalog::Table& col, vpack::Slice indexes_slice,
    std::vector<std::shared_ptr<Index>>& indexes) const {
    SDB_ASSERT(false);
  }

  const IndexFactory& index_factory(std::string_view type) const noexcept;

  virtual Result enhanceIndexDefinition(catalog::Table& collection,
                                        vpack::Slice definition,
                                        vpack::Builder& normalized) const;

  std::shared_ptr<Index> prepareIndexFromSlice(
    vpack::Slice definition, bool generate_key, catalog::Table& collection,
    bool is_cluster_constructor) const;

  virtual void addOptimizerRules(aql::OptimizerRulesFeature& rules) {}
  virtual void addRestHandlers(rest::RestHandlerFactory& handlers) {}

  virtual void cleanupReplicationContexts() {}

  virtual ErrorCode getReplicationApplierConfiguration(
    ObjectId database, vpack::Builder& builder) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual ErrorCode getReplicationApplierConfiguration(
    vpack::Builder& builder) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual ErrorCode removeReplicationApplierConfiguration(ObjectId database) {
    SDB_ASSERT(false);
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual ErrorCode removeReplicationApplierConfiguration() {
    SDB_ASSERT(false);
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual ErrorCode saveReplicationApplierConfiguration(ObjectId database,
                                                        vpack::Slice slice,
                                                        bool do_sync) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual ErrorCode saveReplicationApplierConfiguration(vpack::Slice slice,
                                                        bool do_sync) {
    return {ERROR_NOT_IMPLEMENTED};
  }

  virtual Result createLoggerState(
    const ReplicationClientsProgressTracker* replication_clients,
    vpack::Builder& builder) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual Result createTickRanges(vpack::Builder& builder) {
    return {ERROR_NOT_IMPLEMENTED};
  }
  virtual Result firstTick(uint64_t& tick) { return {ERROR_NOT_IMPLEMENTED}; }
  virtual const WalAccess* walAccess() const {
    SDB_THROW(ERROR_NOT_IMPLEMENTED);
    return nullptr;
  }

  virtual void toPrometheus(std::string& result, std::string_view globals,
                            bool ensure_whitespace) const {}

  virtual Tick currentTick() const = 0;
  virtual Tick releasedTick() const = 0;
  virtual void releaseTick(Tick tick) = 0;
  virtual std::shared_ptr<StorageSnapshot> currentSnapshot() = 0;

 protected:
  std::string_view _type_name;
  containers::FlatHashMap<std::string, const IndexFactory*> _factories;
};

std::shared_ptr<TableShard> GetTableShard(ObjectId id);

}  // namespace sdb
