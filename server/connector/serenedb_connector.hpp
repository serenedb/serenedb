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

#include <axiom/connectors/ConnectorMetadata.h>
#include <velox/common/file/File.h>
#include <velox/connectors/Connector.h>
#include <velox/dwio/common/Options.h>
#include <velox/dwio/common/ReaderFactory.h>
#include <velox/type/Type.h>
#include <velox/vector/DecodedVector.h>

#include <memory>

#include "basics/assert.h"
#include "basics/fwd.h"
#include "basics/misc.hpp"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "data_sink.hpp"
#include "data_source.hpp"
#include "file_table.hpp"
#include "query/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

class SereneDBColumnHandle final : public velox::connector::ColumnHandle {
 public:
  explicit SereneDBColumnHandle(const std::string& name, catalog::Column::Id id)
    : _name{name}, _id{id} {}

  const std::string& name() const final { return _name; }

  const catalog::Column::Id& Id() const noexcept { return _id; }

 private:
  std::string _name;
  catalog::Column::Id _id;
};

class SereneDBConnectorTableHandle final
  : public velox::connector::ConnectorTableHandle {
 public:
  explicit SereneDBConnectorTableHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::TableLayout& layout);

  bool supportsIndexLookup() const final { return false; }

  const std::string& name() const final { return _name; }

  ObjectId TableId() const noexcept { return _table_id; }

  const catalog::Column::Id& GetEffectiveColumnId() const noexcept {
    return _effective_column_id;
  }

  auto& GetTransaction() const noexcept { return _transaction; }

 private:
  std::string _name;
  ObjectId _table_id;
  catalog::Column::Id _effective_column_id;
  query::Transaction& _transaction;
};

class SereneDBColumn final : public axiom::connector::Column {
 public:
  explicit SereneDBColumn(std::string_view name, velox::TypePtr type,
                          catalog::Column::Id id)
    : Column{std::string{name}, type, false}, _id{id} {}

  catalog::Column::Id Id() const noexcept { return _id; }

 private:
  catalog::Column::Id _id;
};

class SereneDBTableLayout final : public axiom::connector::TableLayout {
 public:
  explicit SereneDBTableLayout(
    std::string_view name, const axiom::connector::Table& table,
    velox::connector::Connector& connector,
    std::vector<const axiom::connector::Column*> columns,
    std::vector<const axiom::connector::Column*> order_columns,
    std::vector<axiom::connector::SortOrder> sort_order)
    : TableLayout{std::string{name},    &table, &connector,
                  std::move(columns),   {},     std::move(order_columns),
                  std::move(sort_order)} {}

  std::pair<int64_t, int64_t> sample(
    const velox::connector::ConnectorTableHandlePtr&, float,
    const std::vector<velox::core::TypedExprPtr>&, velox::RowTypePtr,
    const std::vector<velox::common::Subfield>&, velox::HashStringAllocator*,
    std::vector<axiom::connector::ColumnStatistics>*) const final {
    return {1, 1};
  }

  velox::connector::ColumnHandlePtr createColumnHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    const std::string& column_name,
    std::vector<velox::common::Subfield> subfields) const final {
    if (!subfields.empty()) {
      SDB_THROW(ERROR_INTERNAL,
                "SereneDBTableLayout: subfields are not supported");
    }
    SDB_ASSERT(findColumn(column_name),
               "SereneDBTableLayout: can't find column handle for column ",
               column_name);
    return std::make_shared<SereneDBColumnHandle>(
      column_name,
      basics::downCast<const SereneDBColumn>(findColumn(column_name))->Id());
  }

  velox::connector::ConnectorTableHandlePtr createTableHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    std::vector<velox::connector::ColumnHandlePtr> column_handles,
    velox::core::ExpressionEvaluator& evaluator,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejected_filters) const final {
    rejected_filters = std::move(filters);

    if (const auto* read_file_table =
          dynamic_cast<const ReadFileTable*>(&this->table())) {
      return std::make_shared<FileTableHandle>(read_file_table->GetSource(),
                                               read_file_table->GetOptions());
    }

    SDB_ASSERT(!table().columnMap().empty(),
               "SereneDBConnectorTableHandle: need a column for count field");
    return std::make_shared<SereneDBConnectorTableHandle>(session, *this);
  }
};

class RocksDBTable final : public axiom::connector::Table {
  struct Init {
    const catalog::Table& collection;
    velox::RowTypePtr pk_type;
    std::vector<std::unique_ptr<const axiom::connector::Column>> column_handles;

    explicit Init(const catalog::Table& collection)
      : collection{collection}, pk_type{collection.PKType()} {
      column_handles.reserve(collection.RowType()->size());

      for (const auto& catalog_column : collection.Columns()) {
        auto serenedb_column = std::make_unique<SereneDBColumn>(
          catalog_column.name, catalog_column.type, catalog_column.id);
        column_handles.push_back(std::move(serenedb_column));
      }

      if (pk_type->children().empty()) {
        const auto generated_pk_name =
          catalog::Column::GeneratePKName(collection.RowType()->names());

        auto serenedb_column = std::make_unique<SereneDBColumn>(
          generated_pk_name, velox::BIGINT(), catalog::Column::kGeneratedPKId);
        column_handles.push_back(std::move(serenedb_column));

        pk_type = velox::ROW(std::move(generated_pk_name), velox::BIGINT());
      }
    }
  };

  explicit RocksDBTable(Init&& init, query::Transaction& transaction)
    : Table{std::string{init.collection.GetName()},
            std::move(init.column_handles)},
      _pk_type{std::move(init.pk_type)},
      _table_id{init.collection.GetId()},
      _transaction{transaction},
      _stats{transaction.GetTableStats(_table_id)} {
    std::vector<const axiom::connector::Column*> order_columns;
    std::vector<axiom::connector::SortOrder> sort_order;
    order_columns.reserve(_pk_type->size());
    // TODO(mbkkt) We want something like null order doesn't matter, because in
    // primary key nulls are not allowed. For now we just set nulls last,
    // because it's default.
    sort_order.resize(
      std::max(1U, _pk_type->size()),
      axiom::connector::SortOrder{.isAscending = true, .isNullsFirst = false});
    for (const auto& name : _pk_type->names()) {
      const auto* column = findColumn(name);
      SDB_ASSERT(column, "RocksDBTable: can't find PK column ", name);
      order_columns.push_back(column);
    }

    auto connector = velox::connector::getConnector("serenedb");
    auto layout = std::make_unique<SereneDBTableLayout>(
      name(), *this, *connector, allColumns(), std::move(order_columns),
      std::move(sort_order));
    _layouts.push_back(layout.get());
    _layout_handles.push_back(std::move(layout));
  }

 public:
  explicit RocksDBTable(const catalog::Table& collection,
                        query::Transaction& transaction)
    : RocksDBTable{Init{collection}, transaction} {}

  const std::vector<const axiom::connector::TableLayout*>& layouts()
    const final {
    return _layouts;
  }

  uint64_t numRows() const final { return _stats.num_rows; }

  std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
    axiom::connector::WriteKind kind) const final {
    SDB_ASSERT(_pk_type);
    if (kind == axiom::connector::WriteKind::kInsert &&
        basics::downCast<const SereneDBColumn>(allColumns().back())->Id() ==
          catalog::Column::kGeneratedPKId) {
      return {};
    }

    std::vector<velox::connector::ColumnHandlePtr> handles;
    handles.reserve(_pk_type->size());
    for (const auto& name : _pk_type->names()) {
      handles.push_back(std::make_shared<SereneDBColumnHandle>(
        name, basics::downCast<const SereneDBColumn>(findColumn(name))->Id()));
    }
    return handles;
  }

  const ObjectId& TableId() const noexcept { return _table_id; }

  const velox::RowTypePtr& PKType() const noexcept { return _pk_type; }

  bool IsUsedForUpdatePK() const noexcept { return _update_pk; }

  void SetUsedForUpdatePK(bool value = true) { _update_pk = value; }

  query::Transaction& GetTransaction() const noexcept { return _transaction; }

  void SetBulkInsert() { _bulk_insert = true; }

  bool IsBulkInsert() const noexcept { return _bulk_insert; }

 private:
  std::vector<std::unique_ptr<SereneDBTableLayout>> _layout_handles;
  std::vector<const axiom::connector::TableLayout*> _layouts;
  velox::RowTypePtr _pk_type;
  ObjectId _table_id;
  query::Transaction& _transaction;
  catalog::TableStats _stats;
  bool _update_pk = false;
  bool _bulk_insert = false;
};

class SereneDBConnectorSplit final : public velox::connector::ConnectorSplit {
 public:
  using ConnectorSplit::ConnectorSplit;
};

class SereneDBPartitionHandle final : public axiom::connector::PartitionHandle {
};

class SereneDBSplitSource final : public axiom::connector::SplitSource {
 public:
  std::vector<SplitAndGroup> getSplits(uint64_t /* targetBytes */) final {
    auto split_source = std::make_shared<SereneDBConnectorSplit>(
      StaticStrings::kSereneDBConnector);
    return {SplitAndGroup{std::move(split_source)}, SplitAndGroup{}};
  }
};

class SereneDBConnectorSplitManager final
  : public axiom::connector::ConnectorSplitManager {
 public:
  std::vector<axiom::connector::PartitionHandlePtr> listPartitions(
    const axiom::connector::ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr& table_handle) final {
    return {std::make_shared<SereneDBPartitionHandle>()};
  }

  std::shared_ptr<axiom::connector::SplitSource> getSplitSource(
    const axiom::connector::ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr& table_handle,
    const std::vector<axiom::connector::PartitionHandlePtr>& partitions,
    axiom::connector::SplitOptions options = {}) final {
    return std::make_shared<SereneDBSplitSource>();
  }
};

// Store info to create DataSink
class SereneDBConnectorInsertTableHandle final
  : public velox::connector::ConnectorInsertTableHandle {
 public:
  explicit SereneDBConnectorInsertTableHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::TablePtr& table, axiom::connector::WriteKind kind)
    : _session{session},
      _table{table},
      _kind{kind},
      _transaction{basics::downCast<RocksDBTable>(*table).GetTransaction()},
      _update_pk{basics::downCast<RocksDBTable>(*table).IsUsedForUpdatePK()} {
    GetTransaction().AddRocksDBWrite();
    if (_update_pk) {
      GetTransaction().AddRocksDBRead();
    }
  }

  bool supportsMultiThreading() const final { return false; }

  std::string toString() const final {
    return fmt::format("serenedb(table={}, kind={})", _table->name(), _kind);
  }

  const axiom::connector::TablePtr& Table() const noexcept { return _table; }

  auto Kind() const noexcept { return _kind; }

  query::Transaction& GetTransaction() const noexcept { return _transaction; }

  size_t NumberOfRowsAffected() const noexcept {
    const auto keys_affected =
      _transaction.EnsureRocksDBTransaction().GetNumKeys();
    if (_update_pk) {
      // Each affected rows has associated removed key and inserted one.
      // Update of PK is implemented as delete with old key + insert with new
      // key
      return keys_affected / 2;
    }
    return keys_affected;
  }

 private:
  axiom::connector::ConnectorSessionPtr _session;
  axiom::connector::TablePtr _table;
  axiom::connector::WriteKind _kind;
  query::Transaction& _transaction;
  std::vector<velox::connector::ColumnHandlePtr> _row_id_handles;
  bool _update_pk{};
};

// Store transaction/etc here
class SereneDBConnectorWriteHandle final
  : public axiom::connector::ConnectorWriteHandle {
 public:
  explicit SereneDBConnectorWriteHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::TablePtr& table, axiom::connector::WriteKind kind)
    : ConnectorWriteHandle{std::make_shared<SereneDBConnectorInsertTableHandle>(
                             session, table, kind),
                           velox::ROW("rows", velox::BIGINT())} {}
};

class SereneDBConnectorMetadata final
  : public axiom::connector::ConnectorMetadata {
 public:
  axiom::connector::TablePtr findTable(std::string_view name) final {
    VELOX_UNSUPPORTED();
  }

  axiom::connector::ConnectorSplitManager* splitManager() final {
    return &_split_manager;
  }

  axiom::connector::TablePtr createTable(
    const axiom::connector::ConnectorSessionPtr& session,
    const std::string& table_name, const velox::RowTypePtr& row_type,
    const folly::F14FastMap<std::string, velox::Variant>& options) final {
    VELOX_UNSUPPORTED();
  }

  axiom::connector::ConnectorWriteHandlePtr beginWrite(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::TablePtr& table, axiom::connector::WriteKind kind) {
    if (const auto* write_file_table =
          dynamic_cast<const WriteFileTable*>(table.get())) {
      SDB_ASSERT(kind == axiom::connector::WriteKind::kInsert);
      return std::make_shared<FileConnectorWriteHandle>(
        write_file_table->GetSink(), write_file_table->GetOptions());
    }

    return std::make_shared<SereneDBConnectorWriteHandle>(session, table, kind);
  }

  axiom::connector::RowsFuture finishWrite(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::ConnectorWriteHandlePtr& handle,
    const std::vector<velox::RowVectorPtr>& write_results) final {
    auto get_total_rows_from_write_results = [&write_results]() {
      // total_rows is computed by Velox TableWriter operator
      velox::DecodedVector decoded;
      SDB_ASSERT(write_results.size() == 1);
      SDB_ASSERT(write_results[0]->size() == 1);
      decoded.decode(*write_results[0]->childAt(0));
      return decoded.valueAt<int64_t>(0);
    };

    if (dynamic_cast<const FileInsertTableHandle*>(
          handle->veloxHandle().get()) != nullptr) {
      return yaclib::MakeFuture(get_total_rows_from_write_results());
    }

    const auto serene_insert_handle =
      std::dynamic_pointer_cast<const SereneDBConnectorInsertTableHandle>(
        handle->veloxHandle());
    SDB_ENSURE(serene_insert_handle, ERROR_INTERNAL,
               "Wrong type of insert table handle");
    auto& rocksdb_table =
      basics::downCast<const RocksDBTable>(*serene_insert_handle->Table());
    if (rocksdb_table.IsBulkInsert()) {
      return yaclib::MakeFuture(get_total_rows_from_write_results());
    }
    auto& transaction = serene_insert_handle->GetTransaction();
    auto* rocksdb_transaction = transaction.GetRocksDBTransaction();
    if (!rocksdb_transaction) [[unlikely]] {
      return yaclib::MakeFuture<int64_t>(0);
    }

    int64_t number_of_locked_primary_keys =
      serene_insert_handle->NumberOfRowsAffected();
    if (serene_insert_handle->Kind() == axiom::connector::WriteKind::kInsert ||
        serene_insert_handle->Kind() == axiom::connector::WriteKind::kDelete) {
      int64_t delta_num_rows =
        serene_insert_handle->Kind() == axiom::connector::WriteKind::kInsert
          ? number_of_locked_primary_keys
          : -number_of_locked_primary_keys;
      transaction.UpdateNumRows(rocksdb_table.TableId(), delta_num_rows);
    }

    if (!transaction.HasTransactionBegin()) {
      auto r = transaction.Commit();
      if (!r.ok()) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to commit transaction: ", r.errorMessage());
      }
    }
    return yaclib::MakeFuture(number_of_locked_primary_keys);
  }

  velox::ContinueFuture abortWrite(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::ConnectorWriteHandlePtr& handle) noexcept final
    try {
    if (dynamic_cast<const FileInsertTableHandle*>(
          handle->veloxHandle().get())) {
      return velox::ContinueFuture::make();
    }

    auto serene_insert_handle =
      std::dynamic_pointer_cast<const SereneDBConnectorInsertTableHandle>(
        handle->veloxHandle());
    SDB_ENSURE(serene_insert_handle, ERROR_INTERNAL,
               "Wrong type of insert table handle");
    auto& transaction = serene_insert_handle->GetTransaction();
    auto* rocksdb_transaction = transaction.GetRocksDBTransaction();
    if (!rocksdb_transaction) [[unlikely]] {
      return velox::ContinueFuture::make();
    }
    // TODO: should be rollback to last save point
    auto r = transaction.Rollback();
    if (!r.ok()) {
      SDB_THROW(ERROR_INTERNAL,
                "Failed to rollback transaction: ", r.errorMessage());
    }
    return velox::ContinueFuture::make();
  } catch (...) {
    return velox::ContinueFuture::make(std::current_exception());
  }

  bool dropTable(const axiom::connector::ConnectorSessionPtr& session,
                 std::string_view table_name, bool if_exists) final {
    VELOX_UNSUPPORTED();
  }

 private:
  SereneDBConnectorSplitManager _split_manager;
};

class SereneDBConnector final : public velox::connector::Connector {
 public:
  explicit SereneDBConnector(const std::string& id,
                             velox::config::ConfigPtr config,
                             rocksdb::TransactionDB& db,
                             rocksdb::ColumnFamilyHandle& cf,
                             std::string_view rocksdb_directory)
    : Connector{id, std::move(config)},
      _db{db},
      _cf{cf},
      _rocksdb_directory{rocksdb_directory} {}

  bool canAddDynamicFilter() const final { return false; }

  bool supportsSplitPreload() const final { return false; }

  bool supportsIndexLookup() const final { return false; }

  std::unique_ptr<velox::connector::DataSource> createDataSource(
    const velox::RowTypePtr& output_type,
    const velox::connector::ConnectorTableHandlePtr& table_handle,
    const velox::connector::ColumnHandleMap& column_handles,
    velox::connector::ConnectorQueryCtx* connector_query_ctx) final {
    if (const auto* file_handle =
          dynamic_cast<const FileTableHandle*>(table_handle.get())) {
      return std::make_unique<FileDataSource>(
        file_handle->GetSource(), file_handle->GetOptions(),
        *connector_query_ctx->memoryPool());
    }

    const auto& serene_table_handle =
      basics::downCast<const SereneDBConnectorTableHandle>(*table_handle);
    const auto& object_key = serene_table_handle.TableId();

    // need to remap names to oids
    std::vector<catalog::Column::Id> column_oids;
    if (output_type->size() > 0) {
      column_oids.reserve(output_type->size());
      for (const auto& name : output_type->names()) {
        auto handle = column_handles.find(name);
        SDB_ENSURE(handle != column_handles.end(), ERROR_INTERNAL,
                   "RocksDBDataSource: can't find column handle for ", name);
        column_oids.push_back(
          basics::downCast<const SereneDBColumnHandle>(handle->second)->Id());
      }
    } else {
      column_oids.push_back(serene_table_handle.GetEffectiveColumnId());
    }
    auto& transaction = serene_table_handle.GetTransaction();
    const auto& snapshot = transaction.EnsureRocksDBSnapshot();
    return std::make_unique<RocksDBDataSource>(
      *connector_query_ctx->memoryPool(), &snapshot, _db, _cf, output_type,
      column_oids, serene_table_handle.GetEffectiveColumnId(), object_key);
  }

  std::shared_ptr<velox::connector::IndexSource> createIndexSource(
    const velox::RowTypePtr& input_type, size_t num_join_keys,
    const std::vector<std::shared_ptr<velox::core::IndexLookupCondition>>&
      join_conditions,
    const velox::RowTypePtr& output_type,
    const velox::connector::ConnectorTableHandlePtr& table_handle,
    const velox::connector::ColumnHandleMap& column_handles,
    velox::connector::ConnectorQueryCtx* connector_query_ctx) final {
    VELOX_UNSUPPORTED();
  }

  std::unique_ptr<velox::connector::DataSink> createDataSink(
    velox::RowTypePtr input_type,
    velox::connector::ConnectorInsertTableHandlePtr
      connector_insert_table_handle,
    velox::connector::ConnectorQueryCtx* connector_query_ctx,
    velox::connector::CommitStrategy commit_strategy) final {
    if (const auto* file_handle = dynamic_cast<const FileInsertTableHandle*>(
          connector_insert_table_handle.get())) {
      return std::make_unique<FileDataSink>(
        file_handle->GetSink(), file_handle->GetOptions(),
        *connector_query_ctx->connectorMemoryPool());
    }

    auto& serene_insert_handle =
      basics::downCast<SereneDBConnectorInsertTableHandle>(
        *connector_insert_table_handle);
    auto& transaction = serene_insert_handle.GetTransaction();
    const auto& table =
      basics::downCast<const RocksDBTable>(*serene_insert_handle.Table());
    const auto& object_key = table.TableId();
    std::vector<catalog::Column::Id> column_oids;
    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kInsert ||
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate) {
      column_oids.reserve(input_type->size());
      for (auto& col : input_type->names()) {
        std::string_view real_name = catalog::Column::ExtractColumnName(col);
        auto handle = table.columnMap().find(real_name);
        SDB_ASSERT(handle != table.columnMap().end(),
                   "RocksDBDataSink: can't find column handle for ", real_name);
        column_oids.push_back(
          basics::downCast<const SereneDBColumn>(handle->second)->Id());
      }
      return irs::ResolveBool(
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate,
        [&]<bool IsUpdate>() -> std::unique_ptr<velox::connector::DataSink> {
          std::vector<velox::column_index_t> pk_indices;
          if constexpr (IsUpdate) {
            // TODO(Dronplane) discard writers not affected by update. We can
            // check meta and do not create writer if it is not interested in
            // the updated columns
            // TODO(Dronplane) if PK changes - all writers are affected!
            pk_indices.resize(table.PKType()->size());
            absl::c_iota(pk_indices, 0);
#ifdef SDB_DEV
            // SQL Analyzer should put PK columns at the start and with
            // correct order
            const auto& pk_handles =
              table.rowIdHandles(serene_insert_handle.Kind());
            SDB_ASSERT(pk_indices.size() == pk_handles.size());
            size_t pk_idx = 0;
            for (const auto& handle : pk_handles) {
              SDB_ASSERT(pk_indices[pk_idx++] ==
                         input_type->getChildIdx(handle->name()));
            }
#endif
            std::vector<catalog::Column::Id> all_column_oids;
            all_column_oids.reserve(table.type()->size());
            for (auto& col : table.type()->names()) {
              auto handle = table.columnMap().find(col);
              SDB_ASSERT(handle != table.columnMap().end(),
                         "RocksDBDataSink: can't find column handle for ", col);
              all_column_oids.push_back(
                basics::downCast<const SereneDBColumn>(handle->second)->Id());
            }
          } else {
            const auto& pk_handles =
              table.rowIdHandles(serene_insert_handle.Kind());
            pk_indices.reserve(pk_handles.size());
            for (const auto& handle : pk_handles) {
              pk_indices.push_back(input_type->getChildIdx(handle->name()));
            }
          }
          auto& rocksdb_transaction = transaction.EnsureRocksDBTransaction();

          if constexpr (IsUpdate) {
            std::vector<catalog::Column::Id> all_column_oids;
            if (table.IsUsedForUpdatePK()) {
              all_column_oids.reserve(table.type()->size());
              for (auto& col : table.type()->names()) {
                auto handle = table.columnMap().find(col);
                SDB_ASSERT(handle != table.columnMap().end(),
                           "RocksDBDataSink: can't find column handle for ",
                           col);
                all_column_oids.push_back(
                  basics::downCast<const SereneDBColumn>(handle->second)->Id());
              }
            }

            return std::make_unique<RocksDBUpdateDataSink>(
              rocksdb_transaction, _cf, *connector_query_ctx->memoryPool(),
              object_key, pk_indices, column_oids, all_column_oids,
              table.IsUsedForUpdatePK(), table.type(),
              std::vector<std::unique_ptr<SinkUpdateWriter>>{});
          } else {
            if (table.IsBulkInsert()) {
              return std::make_unique<SSTInsertDataSink>(
                _db, _cf, *connector_query_ctx->memoryPool(), object_key,
                pk_indices, column_oids, _rocksdb_directory);
            }

            return std::make_unique<RocksDBInsertDataSink>(
              rocksdb_transaction, _cf, *connector_query_ctx->memoryPool(),
              object_key, pk_indices, column_oids,
              std::vector<std::unique_ptr<SinkInsertWriter>>{});
          }
        });
    }

    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kDelete) {
      column_oids.reserve(table.type()->size());
      for (auto& col : table.type()->names()) {
        auto handle = table.columnMap().find(col);
        SDB_ASSERT(handle != table.columnMap().end(),
                   "RocksDBDataSink: can't find column handle for ", col);
        column_oids.push_back(
          basics::downCast<const SereneDBColumn>(handle->second)->Id());
      }
      auto& rocksdb_transaction = transaction.EnsureRocksDBTransaction();
      return std::make_unique<RocksDBDeleteDataSink>(
        rocksdb_transaction, _cf, table.type(), object_key, column_oids,
        std::vector<std::unique_ptr<SinkDeleteWriter>>{});
    }

    VELOX_UNSUPPORTED("Unsupported write kind");
  }

  folly::Executor* ioExecutor() const final { return nullptr; }

 private:
  rocksdb::TransactionDB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
  std::string _rocksdb_directory;
};

}  // namespace sdb::connector
