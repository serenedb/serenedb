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

#include "basics/fwd.h"
#include "basics/misc.hpp"
#include "catalog/table.h"
#include "data_sink.hpp"
#include "data_source.hpp"
#include "rocksdb/utilities/transaction_db.h"

namespace sdb::connector {

class SereneDBColumnHandle final : public velox::connector::ColumnHandle {
 public:
  explicit SereneDBColumnHandle(const std::string& name) : _name{name} {}

  const std::string& name() const final { return _name; }

 private:
  std::string _name;
};

class SereneDBConnectorTableHandle final
  : public velox::connector::ConnectorTableHandle {
 public:
  explicit SereneDBConnectorTableHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::TableLayout& layout);

  bool supportsIndexLookup() const final { return false; }

  const std::string& name() const final { return _name; }

  const std::string& ObjectId() const noexcept { return _object_id; }

  const std::string& GetCountField() const noexcept {
    return _table_count_field;
  }

 private:
  std::string _name;
  std::string _object_id;
  std::string _table_count_field;
  // TODO(Dronplane) transaction/snapshot management here
};

class SereneDBColumn final : public axiom::connector::Column {
 public:
  explicit SereneDBColumn(std::string_view name, velox::TypePtr type)
    : Column{std::string{name}, type} {}
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
    VELOX_CHECK(subfields.empty());
    return std::make_shared<SereneDBColumnHandle>(column_name);
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
  explicit RocksDBTable(std::string_view name, const catalog::Table& collection)
    : Table{std::string{name}, collection.RowType()},
      _pk_type(collection.PKType()) {
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
      _pk_type->size(),
      axiom::connector::SortOrder{.isAscending = true, .isNullsFirst = false});
    for (const auto& [name, type] : std::views::zip(
           collection.RowType()->names(), collection.RowType()->children())) {
      auto column = std::make_unique<SereneDBColumn>(name, type);
      columns.push_back(column.get());
      _column_map.emplace(name, column.get());
      _column_handles.push_back(std::move(column));
    }
    for (const auto& name : _pk_type->names()) {
      const auto* column = findColumn(name);
      SDB_ASSERT(column, "RocksDBTable: can't find PK column ", name);
      order_columns.push_back(column);
    }
    auto connector = velox::connector::getConnector("serenedb");
    auto layout = std::make_unique<SereneDBTableLayout>(
      name, *this, *connector, std::move(columns), std::move(order_columns),
      std::move(sort_order));
    _layouts.push_back(layout.get());
    _layout_handles.push_back(std::move(layout));
    _object_id = absl::StrCat(collection.GetId());
  }

  const folly::F14FastMap<std::string, const axiom::connector::Column*>&
  columnMap() const final {
    return _column_map;
  }

  const std::vector<const axiom::connector::TableLayout*>& layouts()
    const final {
    return _layouts;
  }

  uint64_t numRows() const final { return 1; }

  std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
    axiom::connector::WriteKind kind) const final {
    SDB_ASSERT(_pk_type);
    SDB_ASSERT(!_pk_type->children().empty());
    std::vector<velox::connector::ColumnHandlePtr> handles;
    handles.reserve(_pk_type->size());
    for (const auto& name : _pk_type->names()) {
      handles.push_back(std::make_shared<SereneDBColumnHandle>(name));
    }
    return handles;
  }

  const std::string& ObjectId() const noexcept { return _object_id; }

  const velox::RowTypePtr& PKType() const noexcept { return _pk_type; }

 private:
  std::vector<std::unique_ptr<SereneDBColumn>> _column_handles;
  std::vector<std::unique_ptr<SereneDBTableLayout>> _layout_handles;
  folly::F14FastMap<std::string, const axiom::connector::Column*> _column_map;
  std::vector<const axiom::connector::TableLayout*> _layouts;
  velox::RowTypePtr _pk_type;
  std::string _object_id;
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
    : _session{session}, _table{table}, _kind{kind} {}

  bool supportsMultiThreading() const final { return false; }

  std::string toString() const final {
    return fmt::format("serenedb(table={}, kind={})", _table->name(), _kind);
  }

  const axiom::connector::TablePtr& Table() const noexcept { return _table; }

  rocksdb::Transaction* GetTransaction() const noexcept {
    // TODO(Dronplane) we will have here non-owned transaction from upper layer
    // so we will check it first when we have it.
    return _local_transaction.get();
  }

  void SetLocalTransaction(
    std::unique_ptr<rocksdb::Transaction>&& transaction) const noexcept {
    _local_transaction = std::move(transaction);
  }

  auto Kind() const noexcept { return _kind; }

 private:
  axiom::connector::ConnectorSessionPtr _session;
  axiom::connector::TablePtr _table;
  axiom::connector::WriteKind _kind;
  mutable std::unique_ptr<rocksdb::Transaction> _local_transaction;
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
    // TODO(Dronplane) transaction management. If this query is a part of larger
    // transaction, we need to store a transaction here from session or smth.
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
    VELOX_CHECK_NOT_NULL(serene_insert_handle,
                         "Wrong type of insert table handle");
    SDB_ASSERT(serene_insert_handle->GetTransaction());
    serene_insert_handle->GetTransaction()->Commit();
    return 0;
  }

  velox::ContinueFuture abortWrite(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::ConnectorWriteHandlePtr& handle) noexcept final
    try {
    // TODO(Dronplane) if we have here transaction from upstream, we should not
    // rollback here. But currently we don't have such case so transaction must
    // be local and stored in the insert handle.
    auto serene_insert_handle =
      std::dynamic_pointer_cast<const SereneDBConnectorInsertTableHandle>(
        handle->veloxHandle());
    VELOX_CHECK_NOT_NULL(serene_insert_handle,
                         "Wrong type of insert table handle");
    if (serene_insert_handle->GetTransaction()) {
      serene_insert_handle->GetTransaction()->Rollback();
    }
    return {};
  } catch (...) {
    return folly::exception_wrapper{folly::current_exception()};
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
    const auto& object_key = serene_table_handle.ObjectId();

    // need to remap names as outputType names might be aliases
    std::vector<std::string> names;
    if (output_type->size() > 0) {
      names.reserve(output_type->size());
      for (uint32_t i = 0; i < output_type->size(); ++i) {
        auto handle = column_handles.find(output_type->nameOf(i));
        VELOX_CHECK(handle != column_handles.end());
        names.push_back(handle->second->name());
      }
    } else {
      SDB_ASSERT(!serene_table_handle.GetCountField().empty(),
                 "RocksDBDataSource: count field table handle");
      names.push_back(serene_table_handle.GetCountField());
    }
    return std::make_unique<RocksDBDataSource>(
      *connector_query_ctx->memoryPool(),
      nullptr,  // TODO(Dronplane) snapshot management
      _db, _cf, output_type, std::move(names), object_key);
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

    auto* transaction = serene_insert_handle.GetTransaction();
    if (!transaction) {
      rocksdb::TransactionOptions txn_options;
      // we will lock rows explicitly
      txn_options.skip_concurrency_control = true;
      serene_insert_handle.SetLocalTransaction(
        std::unique_ptr<rocksdb::Transaction>(
          _db.BeginTransaction(rocksdb::WriteOptions{}, txn_options)));
    }

    const auto& table =
      basics::downCast<const RocksDBTable>(*serene_insert_handle.Table());
    const auto& object_key = table.ObjectId();

    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kInsert ||
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate) {
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
            *(serene_insert_handle.GetTransaction()), _cf, table.type(),
            *connector_query_ctx->memoryPool(), object_key, pk_indices,
            IsUpdate);
        });
    }
    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kDelete) {
      return std::make_unique<RocksDBDeleteDataSink>(
        *(serene_insert_handle.GetTransaction()), _cf, table.type(),
        object_key);
    }

    VELOX_UNSUPPORTED("Unsupported write kind");
  }

  folly::Executor* ioExecutor() const final { return nullptr; }

 private:
  rocksdb::TransactionDB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
};

}  // namespace sdb::connector
