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
#include <velox/connectors/Connector.h>
#include <velox/type/Type.h>

#include "basics/assert.h"
#include "basics/fwd.h"
#include "basics/misc.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "data_sink.hpp"
#include "data_source.hpp"
#include "query/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

namespace sdb::connector {

inline std::shared_ptr<rocksdb::Transaction> ExtractTransaction(
  const axiom::connector::ConnectorSessionPtr& session) {
  SDB_ASSERT(session->config());
  auto& txn = basics::downCast<TxnState>(*session->config());

  if (txn.InsideTransaction()) {
    return txn.GetTransaction();
  }
  return nullptr;
}

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

  const catalog::Column::Id& GetCountField() const noexcept {
    return _table_count_field;
  }

  const auto& GetTransaction() const noexcept { return _txn; }

 private:
  std::string _name;
  ObjectId _table_id;
  catalog::Column::Id _table_count_field;
  std::shared_ptr<rocksdb::Transaction> _txn;
};

class SereneDBColumn final : public axiom::connector::Column {
 public:
  explicit SereneDBColumn(std::string_view name, velox::TypePtr type,
                          catalog::Column::Id id)
    : Column{std::string{name}, type}, _id{id} {}

  const catalog::Column::Id& Id() const noexcept { return _id; }

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
    SDB_ASSERT(!table().columnMap().empty(),
               "SereneDBConnectorTableHandle: need a column for count field");
    return std::make_shared<SereneDBConnectorTableHandle>(session, *this);
  }
};

class RocksDBTable final : public axiom::connector::Table {
 public:
  explicit RocksDBTable(const catalog::Table& collection)
    : Table{std::string{collection.GetName()}, collection.RowType()},
      _pk_type(collection.PKType()),
      _table_id(collection.GetId()) {
    _column_map.reserve(collection.RowType()->size());
    _column_handles.reserve(collection.RowType()->size());

    std::vector<const axiom::connector::Column*> columns;
    std::vector<const axiom::connector::Column*> order_columns;
    std::vector<axiom::connector::SortOrder> sort_order;
    columns.reserve(collection.RowType()->size());
    order_columns.reserve(_pk_type->size());
    // TODO(mbkkt) We want something like null order doesn't matter, because in
    // primary key nulls are not allowed. For now we just set nulls last,
    // because it's default.
    sort_order.resize(
      std::max(1U, _pk_type->size()),
      axiom::connector::SortOrder{.isAscending = true, .isNullsFirst = false});
    for (const auto& catalog_column : collection.Columns()) {
      auto serenedb_column = std::make_unique<SereneDBColumn>(
        catalog_column.name, catalog_column.type, catalog_column.id);
      columns.push_back(serenedb_column.get());
      _column_map.emplace(catalog_column.name, serenedb_column.get());
      _column_handles.push_back(std::move(serenedb_column));
    }
    for (const auto& name : _pk_type->names()) {
      const auto* column = findColumn(name);
      SDB_ASSERT(column, "RocksDBTable: can't find PK column ", name);
      order_columns.push_back(column);
    }

    if (_pk_type->children().empty()) {
      const auto generated_pk_name =
        catalog::Column::GeneratePKName(collection.RowType()->names());

      auto serenedb_column = std::make_unique<SereneDBColumn>(
        generated_pk_name, velox::BIGINT(), catalog::Column::kGeneratedPKId);
      columns.push_back(serenedb_column.get());

      _column_map.emplace(generated_pk_name, serenedb_column.get());
      order_columns.push_back(serenedb_column.get());
      _column_handles.push_back(std::move(serenedb_column));
      _pk_type = velox::ROW({std::move(generated_pk_name)}, {velox::BIGINT()});
    }

    auto connector = velox::connector::getConnector("serenedb");
    auto layout = std::make_unique<SereneDBTableLayout>(
      name(), *this, *connector, std::move(columns), std::move(order_columns),
      std::move(sort_order));
    _layouts.push_back(layout.get());
    _layout_handles.push_back(std::move(layout));
  }

  const folly::F14FastMap<std::string, const axiom::connector::Column*>&
  columnMap() const final {
    return _column_map;
  }

  const std::vector<const axiom::connector::TableLayout*>& layouts()
    const final {
    return _layouts;
  }

  uint64_t numRows() const final { return 1000; }

  std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
    axiom::connector::WriteKind kind) const final {
    SDB_ASSERT(_pk_type);
    if (kind == axiom::connector::WriteKind::kInsert &&
        _column_handles.back()->Id() == catalog::Column::kGeneratedPKId) {
      return {};
    }

    std::vector<velox::connector::ColumnHandlePtr> handles;
    handles.reserve(_pk_type->size());
    for (const auto& name : _pk_type->names()) {
      handles.push_back(std::make_shared<SereneDBColumnHandle>(
        name,
        basics::downCast<const SereneDBColumn>(_column_map.at(name))->Id()));
    }
    return handles;
  }

  const ObjectId& TableId() const noexcept { return _table_id; }

  const velox::RowTypePtr& PKType() const noexcept { return _pk_type; }

 private:
  std::vector<std::unique_ptr<SereneDBColumn>> _column_handles;
  std::vector<std::unique_ptr<SereneDBTableLayout>> _layout_handles;
  folly::F14FastMap<std::string, const axiom::connector::Column*> _column_map;
  std::vector<const axiom::connector::TableLayout*> _layouts;
  velox::RowTypePtr _pk_type;
  ObjectId _table_id;
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
    auto split_source = std::make_shared<SereneDBConnectorSplit>("serenedb");
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
    : _session{session}, _table{table}, _kind{kind} {
    _txn = ExtractTransaction(session);
  }

  bool supportsMultiThreading() const final { return false; }

  std::string toString() const final {
    return fmt::format("serenedb(table={}, kind={})", _table->name(), _kind);
  }

  const axiom::connector::TablePtr& Table() const noexcept { return _table; }

  const auto& GetTransaction() const noexcept { return _txn; }

  void SetTransaction(
    std::shared_ptr<rocksdb::Transaction> transaction) const noexcept {
    _txn = std::move(transaction);
  }

  auto Kind() const noexcept { return _kind; }

 private:
  axiom::connector::ConnectorSessionPtr _session;
  axiom::connector::TablePtr _table;
  axiom::connector::WriteKind _kind;
  mutable std::shared_ptr<rocksdb::Transaction> _txn;
  std::vector<velox::connector::ColumnHandlePtr> _row_id_handles;
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
    return std::make_shared<SereneDBConnectorWriteHandle>(session, table, kind);
  }

  axiom::connector::RowsFuture finishWrite(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::ConnectorWriteHandlePtr& handle,
    const std::vector<velox::RowVectorPtr>& write_results) final {
    // TODO(Dronplane) if we have here transaction from upstream, we should not
    // commit here. But currently we don't have such case so transaction must be
    // local and stored in the insert handle.
    auto serene_insert_handle =
      std::dynamic_pointer_cast<const SereneDBConnectorInsertTableHandle>(
        handle->veloxHandle());
    SDB_ENSURE(serene_insert_handle, ERROR_INTERNAL,
               "Wrong type of insert table handle");
    const auto& transaction = serene_insert_handle->GetTransaction();
    SDB_ASSERT(transaction);
    const int64_t number_of_locked_primary_keys = transaction->GetNumKeys();
    SDB_ASSERT(session->config());
    auto& config = basics::downCast<TxnState>(*session->config());
    if (!config.InsideTransaction()) {
      // Single statement transaction, we can commit here
      auto status = transaction->Commit();
      if (!status.ok()) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to commit transaction: ", status.ToString());
      }
    }

    return yaclib::MakeFuture(number_of_locked_primary_keys);
  }

  velox::ContinueFuture abortWrite(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::ConnectorWriteHandlePtr& handle) noexcept final
    try {
    auto serene_insert_handle =
      std::dynamic_pointer_cast<const SereneDBConnectorInsertTableHandle>(
        handle->veloxHandle());
    SDB_ENSURE(serene_insert_handle, ERROR_INTERNAL,
               "Wrong type of insert table handle");
    SDB_ASSERT(session->config());
    auto& txn = basics::downCast<TxnState>(*session->config());
    if (serene_insert_handle->GetTransaction() && !txn.InsideTransaction()) {
      auto status = serene_insert_handle->GetTransaction()->Rollback();
      if (!status.ok()) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to rollback transaction: ", status.ToString());
      }
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
                             rocksdb::ColumnFamilyHandle& cf)
    : Connector{id, std::move(config)}, _db{db}, _cf{cf} {}

  bool canAddDynamicFilter() const final { return false; }

  bool supportsSplitPreload() const final { return false; }

  bool supportsIndexLookup() const final { return false; }

  std::unique_ptr<velox::connector::DataSource> createDataSource(
    const velox::RowTypePtr& output_type,
    const velox::connector::ConnectorTableHandlePtr& table_handle,
    const velox::connector::ColumnHandleMap& column_handles,
    velox::connector::ConnectorQueryCtx* connector_query_ctx) final {
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
      column_oids.push_back(serene_table_handle.GetCountField());
    }
    const rocksdb::Snapshot* snapshot = nullptr;
    if (auto txn = serene_table_handle.GetTransaction()) {
      snapshot = txn->GetSnapshot();
    }
    return std::make_unique<RocksDBDataSource>(
      *connector_query_ctx->memoryPool(), snapshot, _db, _cf, output_type,
      column_oids, object_key);
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
    const auto& serene_insert_handle =
      basics::downCast<const SereneDBConnectorInsertTableHandle>(
        *connector_insert_table_handle);

    const auto& transaction = serene_insert_handle.GetTransaction();
    if (!transaction) {
      serene_insert_handle.SetTransaction(CreateTransaction(_db));
    }

    const auto& table =
      basics::downCast<const RocksDBTable>(*serene_insert_handle.Table());
    const auto& object_key = table.TableId();
    std::vector<catalog::Column::Id> column_oids;
    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kInsert ||
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate) {
      column_oids.reserve(input_type->size());
      for (auto& col : input_type->names()) {
        auto handle = table.columnMap().find(col);
        SDB_ASSERT(handle != table.columnMap().end(),
                   "RocksDBDataSink: can't find column handle for ", col);
        column_oids.push_back(
          basics::downCast<const SereneDBColumn>(handle->second)->Id());
      }
      return irs::ResolveBool(
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate,
        [&]<bool IsUpdate>() {
          std::vector<velox::column_index_t> pk_indices;
          if constexpr (IsUpdate) {
            pk_indices.resize(table.PKType()->size());
            std::iota(pk_indices.begin(), pk_indices.end(), 0);
#ifdef SDB_DEV
            // SQL Analyzer should put PK columns at the start and with correct
            // order
            const auto& pk_handles =
              table.rowIdHandles(serene_insert_handle.Kind());
            SDB_ASSERT(pk_indices.size() == pk_handles.size());
            size_t pk_idx = 0;
            for (const auto& handle : pk_handles) {
              SDB_ASSERT(pk_indices[pk_idx++] ==
                         input_type->getChildIdx(handle->name()));
            }
#endif
          } else {
            const auto& pk_handles =
              table.rowIdHandles(serene_insert_handle.Kind());
            pk_indices.reserve(pk_handles.size());
            for (const auto& handle : pk_handles) {
              pk_indices.push_back(input_type->getChildIdx(handle->name()));
            }
          }
          return std::make_unique<RocksDBDataSink>(
            *(serene_insert_handle.GetTransaction()), _cf,
            *connector_query_ctx->memoryPool(), object_key, pk_indices,
            column_oids, IsUpdate);
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
      return std::make_unique<RocksDBDeleteDataSink>(
        *(serene_insert_handle.GetTransaction()), _cf, table.type(), object_key,
        column_oids);
    }

    VELOX_UNSUPPORTED("Unsupported write kind");
  }

  folly::Executor* ioExecutor() const final { return nullptr; }

 private:
  rocksdb::TransactionDB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
};

}  // namespace sdb::connector
