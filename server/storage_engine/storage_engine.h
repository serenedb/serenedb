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

  virtual bool VisitDatabases(
    absl::FunctionRef<bool(vpack::Slice database)> visitor) {
    return true;
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

  // database, table and index management

  // if not stated other wise functions may throw and the caller has to take
  // care of error handling the return values will be the usual  ERROR_*
  // codes.

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

  virtual Result createFunction(ObjectId db, ObjectId schema_id, ObjectId id,
                                WriteProperties properties) = 0;

  virtual Result dropFunction(ObjectId db, ObjectId schema_id, ObjectId id,
                              std::string_view name) = 0;

  // asks the storage engine to create a table as specified in the VPack
  // Slice object and persist the creation info. It is guaranteed by the server
  // that no other active table with the same name and id exists in the
  // same database when this function is called. If this operation fails
  // somewhere in the middle, the storage engine is required to fully clean up
  // the creation and throw only then, so that subsequent table creation
  // requests will not fail. the WAL entry for the table creation will be
  // written *after* the call to "createTable" returns
  virtual void createTable(const catalog::Table& table,
                           TableShard& physical) = 0;
  virtual Result MarkDeleted(const catalog::Table& table,
                             const TableShard& physical,
                             const TableTombstone& tombstone) {
    return {};
  };

  virtual Result MarkDeleted(const catalog::Database& database) { return {}; };
  virtual Result MarkDeleted(const catalog::Schema& schema) { return {}; };

  // method that is called prior to deletion of a table. allows the storage
  // engine to clean up arbitrary data for this table before the table
  // moves into status "deleted". this method may be called multiple times for
  // the same table
  virtual void prepareDropTable(ObjectId table) {}

  // asks the storage engine to drop the specified table and persist the
  // deletion info. Note that physical deletion of the table data must not
  // be carried out by this call, as there may
  // still be readers of the table's data. It is recommended that this
  // operation only sets a deletion flag for the collection but lets an async
  // task perform the actual deletion. the WAL entry for collection deletion
  // will be written *after* the call to "dropCollection" returns
  virtual Result dropCollection(const TableTombstone& tombstone) { return {}; }
  virtual Result dropIndex(IndexTombstone tombstone) { return {}; }

  // asks the storage engine to change properties of the collection as specified
  // in the VPack Slice object and persist them. If this operation fails
  // somewhere in the middle, the storage engine is required to fully revert the
  // property changes and throw only then, so that subsequent operations will
  // not fail. the WAL entry for the propery change will be written *after* the
  // call to "changeCollection" returns
  virtual void changeCollection(const catalog::Table& collection,
                                const TableShard& physical) {}

  // asks the storage engine to persist renaming of a collection
  virtual Result renameCollection(const catalog::Table& collection,
                                  const TableShard& physical,
                                  std::string_view old_name) {
    return {};
  }

  // If this operation fails somewhere in the middle, the storage engine is
  // required to fully revert the property changes and throw only then, so that
  // subsequent operations will not fail. The WAL entry for the property change
  // will be written *after* the call to "change*" returns

  virtual Result createSchema(ObjectId db, ObjectId id,
                              WriteProperties properties) = 0;
  virtual Result changeSchema(ObjectId db, ObjectId id,
                              WriteProperties properties) = 0;
  virtual Result dropSchema(ObjectId db, ObjectId id,
                            std::string_view name) = 0;

  virtual Result changeView(ObjectId db, ObjectId schema_id, ObjectId id,
                            WriteProperties properties) = 0;

  virtual Result createView(ObjectId db, ObjectId schema_id, ObjectId id,
                            WriteProperties properties) = 0;

  virtual Result dropView(ObjectId db, ObjectId schema_id, ObjectId id,
                          std::string_view name) = 0;

  virtual Result changeRole(ObjectId db, ObjectId id,
                            WriteProperties properties) = 0;

  virtual Result createRole(const catalog::Role& role) = 0;

  virtual Result dropRole(const catalog::Role& role) = 0;

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

  // management methods for synchronizing with external persistent stores
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
