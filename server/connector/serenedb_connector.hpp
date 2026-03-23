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

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <axiom/connectors/ConnectorMetadata.h>
#include <rocksdb/utilities/transaction.h>
#include <velox/common/file/File.h>
#include <velox/connectors/Connector.h>
#include <velox/connectors/hive/HiveConnectorUtil.h>
#include <velox/core/Expressions.h>
#include <velox/dwio/common/Options.h>
#include <velox/dwio/common/ReaderFactory.h>
#include <velox/expression/ExprConstants.h>
#include <velox/type/Type.h>
#include <velox/vector/DecodedVector.h>

#include <chrono>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/scorer.hpp>
#include <memory>
#include <ranges>
#include <thread>
#include <type_traits>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/fwd.h"
#include "basics/misc.hpp"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/rocksdb_filter.hpp"
#include "connector/search_count_data_source.hpp"
#include "connector/search_scan_data_source.hpp"
#include "connector/search_sink_writer.hpp"
#include "connector/sink_writer_base.hpp"
#include "data_sink.hpp"
#include "data_source.hpp"
#include "file_table.hpp"
#include "query/transaction.h"
#include "query/utils.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

inline void ExtractInputFields(const velox::core::TypedExprPtr& expr,
                               absl::flat_hash_set<std::string_view>& names) {
  if (expr->kind() == velox::core::ExprKind::kFieldAccess) {
    names.insert(
      basics::downCast<velox::core::FieldAccessTypedExpr>(expr.get())->name());
    return;
  }
  for (const auto& child : expr->inputs()) {
    ExtractInputFields(child, names);
  }
}

inline bool HasColumnOverlap(
  std::span<const catalog::Column::Id> index_columns,
  const containers::FlatHashSet<catalog::Column::Id>& updated_columns) {
  return absl::c_any_of(index_columns, [&](auto index_col) {
    return updated_columns.contains(index_col);
  });
}

template<axiom::connector::WriteKind Kind>
std::unique_ptr<SinkIndexWriter> MakeInvertedIndexWriter(
  irs::IndexWriter::Transaction& transaction,
  const catalog::InvertedIndex& index) {
  if constexpr (Kind == axiom::connector::WriteKind::kInsert) {
    return std::make_unique<SearchSinkInsertWriter>(
      transaction, MakeAnalyzerProvider(index), index.GetColumnIds());
  } else if constexpr (Kind == axiom::connector::WriteKind::kUpdate) {
    return std::make_unique<SearchSinkUpdateWriter>(
      transaction, MakeAnalyzerProvider(index), index.GetColumnIds());
  } else {
    static_assert(Kind == axiom::connector::WriteKind::kDelete,
                  "Unexpected WriteKind");
    return std::make_unique<SearchSinkDeleteWriter>(transaction);
  }
}

inline std::unique_ptr<SinkIndexWriter> CreateBackfillIndexWriter(
  ObjectId backfill_index_id, query::Transaction& transaction) {
  auto snapshot = transaction.GetCatalogSnapshot();
  auto shard = snapshot->GetIndexShard(backfill_index_id);
  SDB_ASSERT(shard);
  auto& inverted_shard = basics::downCast<search::InvertedIndexShard>(*shard);
  auto& index = basics::downCast<const catalog::InvertedIndex>(
    *snapshot->template GetObject<catalog::Index>(shard->GetIndexId()));
  return std::make_unique<SearchSinkBackfillWriter>(
    inverted_shard, MakeAnalyzerProvider(index), index.GetColumnIds());
}

template<axiom::connector::WriteKind Kind>
std::vector<std::unique_ptr<SinkIndexWriter>> CreateIndexWriters(
  ObjectId table_id, query::Transaction& transaction,
  std::span<const ColumnInfo> updated_columns = {}, bool pk_updated = false) {
  std::vector<std::unique_ptr<SinkIndexWriter>> writers;

  auto resolve_index_writer = [&](auto& transaction,
                                  const catalog::Index& index) {
    if constexpr (std::is_same_v<std::decay_t<decltype(transaction)>,
                                 irs::IndexWriter::Transaction>) {
      const auto& inverted_index =
        basics::downCast<catalog::InvertedIndex>(index);
      writers.push_back(
        MakeInvertedIndexWriter<Kind>(transaction, inverted_index));
    } else {
      SDB_UNREACHABLE();
    }
  };

  if constexpr (Kind == axiom::connector::WriteKind::kUpdate) {
    containers::FlatHashSet<catalog::Column::Id> update_column_ids;
    if (!pk_updated) {
      update_column_ids.reserve(updated_columns.size());
      for (const auto& c : updated_columns) {
        update_column_ids.emplace(c.id);
      }
    }
    auto index_filter =
      [&](std::span<const catalog::Column::Id> index_columns) {
        if (pk_updated) {
          return true;
        }
        return HasColumnOverlap(index_columns, update_column_ids);
      };

    transaction.EnsureIndexesTransactions(table_id, resolve_index_writer,
                                          index_filter);
  } else {
    transaction.EnsureIndexesTransactions(table_id, resolve_index_writer);
  }
#ifdef SDB_FAULT_INJECTION
  // failpoints are per process so we make unique name to allow multiple sqlogic
  // tests run in parallel without interference of failpoints
  // TODO(Dronplane): Find a better way. Maybe make failpoints database
  // bindable to allow parallel execution.
  auto table_ptr =
    transaction.GetCatalogSnapshot()->GetObject<catalog::Table>(table_id);
  SDB_ASSERT(table_ptr);
  auto one_index_fp =
    absl::StrCat(table_ptr->GetName(), "_connector_must_one_index");
  auto two_index_fp =
    absl::StrCat(table_ptr->GetName(), "_connector_must_two_index");
  SDB_IF_FAILURE(one_index_fp) {
    if (writers.size() != 1) {
      SDB_THROW(ERROR_DEBUG, one_index_fp, " condition failed");
    }
  }

  SDB_IF_FAILURE(two_index_fp) {
    if (writers.size() != 2) {
      SDB_THROW(ERROR_DEBUG, two_index_fp, " condition failed");
    }
  }
#endif
  return writers;
}

}  // namespace

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
  struct FilterColumn {
    catalog::Column::Id id;
    velox::TypePtr type;
  };

  explicit SereneDBConnectorTableHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    const axiom::connector::TableLayout& layout, std::vector<Point> points,
    velox::core::TypedExprPtr remaining_filter);

  bool supportsIndexLookup() const final { return false; }

  const std::string& name() const final { return _name; }

  std::string toString() const final {
    // TODO(mkornaukhov) implement filter printing
    if (_search_query) {
      return absl::StrCat(_name, ", scan=search");
    }
    if (!_points.empty()) {
      return absl::StrCat(_name, ", scan=point");
    }
    return absl::StrCat(_name, ", scan=full");
  }

  ObjectId TableId() const noexcept { return _table_id; }

  const catalog::Column::Id& GetEffectiveColumnId() const noexcept {
    return _effective_column_id;
  }

  void AddSearchQuery(ObjectId index_id, irs::Filter::Query::ptr&& query) {
    SDB_ASSERT(!_search_query);
    _search_query = std::move(query);
    _index_id = index_id;
  }

  auto& GetTransaction() const noexcept { return _transaction; }

  auto& GetPKType() const noexcept { return _pk_type; }

  auto& GetRemainingFilter() const noexcept { return _remaining_filter; }

  const std::vector<Point>& GetPoints() const noexcept { return _points; }

  const absl::flat_hash_map<std::string, FilterColumn>& GetTableColumnMap()
    const noexcept {
    return _table_column_map;
  }

  const irs::Filter::Query* GetSearchQuery() const noexcept {
    return _search_query.get();
  }

  ObjectId GetIndexId() const noexcept { return _index_id; }

  void AddScorer(std::shared_ptr<const irs::Scorer> scorer) noexcept {
    _scorer = std::move(scorer);
  }

  const irs::Scorer* GetScorer() const noexcept { return _scorer.get(); }

 private:
  std::string _name;
  ObjectId _table_id;
  catalog::Column::Id _effective_column_id;
  query::Transaction& _transaction;
  irs::Filter::Query::ptr _search_query;
  ObjectId _index_id = ObjectId::none();
  velox::RowTypePtr _pk_type;
  std::vector<Point> _points;
  velox::core::TypedExprPtr _remaining_filter;
  absl::flat_hash_map<std::string, FilterColumn> _table_column_map;
  std::shared_ptr<const irs::Scorer> _scorer;
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
    std::vector<velox::core::TypedExprPtr>& rejected_filters) const final;
};

class RocksDBTable : public axiom::connector::Table {
 protected:
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

  query::Transaction& GetTransaction() const noexcept { return _transaction; }

  decltype(auto) WriteConflictPolicy(this auto&& self) noexcept {
    return (self._write_conflict_policy);
  }

  decltype(auto) UsedForUpdatePK(this auto&& self) noexcept {
    return (self._update_pk);
  }

  decltype(auto) BulkInsert(this auto&& self) noexcept {
    return (self._bulk_insert);
  }

  decltype(auto) BackfillIndexId(this auto&& self) noexcept {
    return (self._backfill_index_id);
  }

 private:
  std::vector<std::unique_ptr<SereneDBTableLayout>> _layout_handles;
  std::vector<const axiom::connector::TableLayout*> _layouts;
  velox::RowTypePtr _pk_type;
  ObjectId _table_id;
  query::Transaction& _transaction;
  catalog::TableStats _stats;
  enum WriteConflictPolicy _write_conflict_policy =
    WriteConflictPolicy::EmitError;
  bool _update_pk = false;
  bool _bulk_insert = false;
  ObjectId _backfill_index_id;
};

class RocksDBInvertedIndexTable : public RocksDBTable {
  static Init MakeInit(const catalog::Table& collection) {
    Init init{collection};
    // Always add the score column so createColumnHandle can resolve it.
    // It is only included in the output type when a scorer is active.
    auto score_name =
      catalog::Column::GenerateScoreName(collection.RowType()->names());
    init.column_handles.push_back(std::make_unique<SereneDBColumn>(
      score_name, velox::REAL(), catalog::Column::kInvertedIndexScoreId));
    return init;
  }

 public:
  explicit RocksDBInvertedIndexTable(const catalog::Table& collection,
                                     query::Transaction& transaction,
                                     const catalog::InvertedIndex& index)
    : RocksDBTable{MakeInit(collection), transaction}, _index{index} {}

  const auto& GetIndex() const noexcept { return _index; }

  void SetScorer(std::shared_ptr<const irs::Scorer> scorer) noexcept {
    _scorer = std::move(scorer);
  }

  const std::shared_ptr<const irs::Scorer>& GetScorerPtr() const noexcept {
    return _scorer;
  }

 private:
  const catalog::InvertedIndex& _index;
  std::shared_ptr<const irs::Scorer> _scorer;
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
    if (const auto* file_handle =
          dynamic_cast<const FileTableHandle*>(table_handle.get())) {
      return std::make_shared<FileSplitSource>(
        file_handle->GetOptions(), StaticStrings::kSereneDBConnector, options);
    }
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
      _update_pk{basics::downCast<RocksDBTable>(*table).UsedForUpdatePK()} {
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

  uint64_t& NumberOfRowsAffected() const noexcept { return _rows_affected; }

 private:
  axiom::connector::ConnectorSessionPtr _session;
  axiom::connector::TablePtr _table;
  axiom::connector::WriteKind _kind;
  query::Transaction& _transaction;
  std::vector<velox::connector::ColumnHandlePtr> _row_id_handles;
  mutable uint64_t _rows_affected = 0;
  bool _update_pk = false;
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
        write_file_table->GetOptions());
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
    if (rocksdb_table.BulkInsert()) {
      return yaclib::MakeFuture(get_total_rows_from_write_results());
    }
    auto& transaction = serene_insert_handle->GetTransaction();

    const int64_t number_of_rows_affected =
      serene_insert_handle->NumberOfRowsAffected();
    const auto kind = serene_insert_handle->Kind();
    if (number_of_rows_affected != 0 &&
        kind != axiom::connector::WriteKind::kUpdate) {
      transaction.UpdateNumRows(rocksdb_table.TableId(),
                                kind == axiom::connector::WriteKind::kDelete
                                  ? -number_of_rows_affected
                                  : number_of_rows_affected);
    }

    if (!transaction.HasTransactionBegin()) {
      auto r = transaction.Commit();
      if (!r.ok()) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to commit transaction: ", r.errorMessage());
      }
    }
    return yaclib::MakeFuture(number_of_rows_affected);
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
    if (const auto* file_handle =
          dynamic_cast<const FileTableHandle*>(table_handle.get())) {
      return std::make_unique<FileDataSource>(
        file_handle->GetOptions(), file_handle->GetSubfieldFilters(),
        output_type, column_handles, *connector_query_ctx->memoryPool(),
        file_handle->GetRemainingFilter(),
        connector_query_ctx->expressionEvaluator());
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
    }

    velox::RowTypePtr read_type = output_type;
    const size_t output_column_count = output_type->size();
    velox::core::TypedExprPtr compiled_filter;

    if (const auto& remaining_filter =
          serene_table_handle.GetRemainingFilter()) {
      std::unordered_map<std::string, velox::core::TypedExprPtr> name_mapping;
      absl::flat_hash_set<std::string_view> output_original_names;
      for (size_t i = 0; i < output_type->size(); ++i) {
        const auto& mangled = output_type->nameOf(i);
        const auto sep = mangled.rfind(query::kColumnSeparator);
        SDB_ASSERT(sep != std::string::npos);
        std::string_view original{mangled.data(), sep};
        output_original_names.insert(original);
        name_mapping.emplace(
          std::string{original},
          std::make_shared<velox::core::FieldAccessTypedExpr>(
            output_type->childAt(i), mangled));
      }

      // Detect filter-only columns: referenced in filter but absent from
      // output. Extend column_oids and read_type so ApplyRemainingFilter can
      // read them.
      absl::flat_hash_set<std::string_view> filter_field_names;
      ExtractInputFields(remaining_filter, filter_field_names);

      const auto& table_col_map = serene_table_handle.GetTableColumnMap();
      std::vector<std::string> extra_names;
      std::vector<velox::TypePtr> extra_types;
      bool need_rewrite = false;
      for (const auto& fname : filter_field_names) {
        if (output_original_names.contains(fname)) {
          need_rewrite = true;
          continue;
        }
        auto it = table_col_map.find(fname);
        SDB_ASSERT(it != table_col_map.end());
        column_oids.push_back(it->second.id);
        extra_names.emplace_back(fname);
        extra_types.push_back(it->second.type);
      }

      if (!extra_names.empty()) {
        auto names = output_type->names();
        auto types = output_type->children();
        names.insert(names.end(), std::move_iterator(extra_names.begin()),
                     std::move_iterator(extra_names.end()));
        types.insert(types.end(), std::move_iterator(extra_types.begin()),
                     std::move_iterator(extra_types.end()));
        read_type = velox::ROW(std::move(names), std::move(types));
      }

      compiled_filter = need_rewrite
                          ? remaining_filter->rewriteInputNames(name_mapping)
                          : remaining_filter;
    }

    // For COUNT(*) with no output and no filter columns, use effective_col_id
    // as the row iterator driver.
    if (column_oids.empty()) {
      SDB_ASSERT(output_type->size() == 0);
      column_oids.push_back(serene_table_handle.GetEffectiveColumnId());
    }
    auto& transaction = serene_table_handle.GetTransaction();

    // We cannot have precalculated points for search query
    SDB_ASSERT(serene_table_handle.GetPoints().empty() ||
               !serene_table_handle.GetSearchQuery());

    const bool needs_read_your_own_writes =
      transaction.HasRocksDBWrite() &&
      transaction.Get<VariableType::Bool>("sdb_read_your_own_writes");

    const auto* snapshot = &transaction.EnsureRocksDBSnapshot();
    if (needs_read_your_own_writes) {
      auto& rocksdb_transaction = transaction.GetRocksDBTransaction();
      SDB_ASSERT(snapshot == rocksdb_transaction.GetSnapshot());
      if (serene_table_handle.GetSearchQuery()) {
        SDB_THROW(
          ERROR_NOT_IMPLEMENTED,
          "sdb_read_your_own_writes is not supported for inverted index");
      }

#ifdef SDB_FAULT_INJECTION
      // TODO(mkornaukhov): Find a better way. Maybe make failpoints database
      // bindable to allow parallel execution.
      auto table_ptr =
        transaction.GetCatalogSnapshot()->GetObject<catalog::Table>(object_key);
      SDB_ASSERT(table_ptr);
      auto fail_on_ryow = absl::StrCat(table_ptr->GetName(), "_fail_on_ryow");
      SDB_IF_FAILURE(fail_on_ryow) {
        SDB_THROW(ERROR_DEBUG, fail_on_ryow, " condition failed");
      }
#endif

      const auto& points = serene_table_handle.GetPoints();
      if (!points.empty()) {
        return std::make_unique<RocksDBRYOWPointLookupDataSource>(
          *connector_query_ctx->memoryPool(), rocksdb_transaction, _cf,
          read_type, column_oids, object_key,
          PointsToRowVector(points, serene_table_handle.GetPKType(),
                            connector_query_ctx->memoryPool()),
          output_column_count, compiled_filter,
          connector_query_ctx->expressionEvaluator());
      }

      return std::make_unique<RocksDBRYOWFullScanDataSource>(
        *connector_query_ctx->memoryPool(), rocksdb_transaction, _cf, read_type,
        column_oids, serene_table_handle.GetEffectiveColumnId(), object_key,
        output_column_count, compiled_filter,
        connector_query_ctx->expressionEvaluator());
    }

    if (serene_table_handle.GetSearchQuery()) {
      const auto& search_snapshot =
        transaction.EnsureSearchSnapshot(serene_table_handle.GetIndexId());
      if (output_type->size() == 0) {
        return std::make_unique<SearchCountDataSource>(
          *connector_query_ctx->memoryPool(), output_type,
          search_snapshot.reader, *serene_table_handle.GetSearchQuery());
      }
      return std::make_unique<SearchScanDataSource>(
        *connector_query_ctx->memoryPool(),
        search_snapshot.snapshot->GetSnapshot(), _db, _cf, output_type,
        std::move(column_oids), serene_table_handle.GetEffectiveColumnId(),
        object_key, search_snapshot.reader,
        *serene_table_handle.GetSearchQuery(), serene_table_handle.GetScorer());
    }

    const auto& points = serene_table_handle.GetPoints();
    if (!points.empty()) {
      return std::make_unique<RocksDBSnapshotPointLookupDataSource>(
        *connector_query_ctx->memoryPool(), _db, _cf, read_type, column_oids,
        object_key, snapshot,
        PointsToRowVector(points, serene_table_handle.GetPKType(),
                          connector_query_ctx->memoryPool()),
        output_column_count, compiled_filter,
        connector_query_ctx->expressionEvaluator());
    }

    return std::make_unique<RocksDBSnapshotFullScanDataSource>(
      *connector_query_ctx->memoryPool(), _db, _cf, read_type, column_oids,
      serene_table_handle.GetEffectiveColumnId(), object_key,
      output_column_count, snapshot, compiled_filter,
      connector_query_ctx->expressionEvaluator());
  }

  std::shared_ptr<velox::connector::IndexSource> createIndexSource(
    const velox::RowTypePtr& input_type,
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
        file_handle->GetOptions(), *connector_query_ctx->memoryPool(),
        *connector_query_ctx->connectorMemoryPool());
    }

    auto& serene_insert_handle =
      basics::downCast<SereneDBConnectorInsertTableHandle>(
        *connector_insert_table_handle);
    auto& transaction = serene_insert_handle.GetTransaction();
    const auto& table =
      basics::downCast<const RocksDBTable>(*serene_insert_handle.Table());
    const auto& object_key = table.TableId();

    auto table_shard =
      transaction.GetCatalogSnapshot()->GetTableShard(object_key);
    SDB_ASSERT(table_shard);
    auto& table_lock = table_shard->GetTableLock();

    std::vector<ColumnInfo> columns;
    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kInsert ||
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate) {
      columns.reserve(input_type->size());
      for (auto& col : input_type->names()) {
        std::string_view real_name = catalog::Column::ExtractColumnName(col);
        auto handle = table.columnMap().find(real_name);
        SDB_ASSERT(handle != table.columnMap().end(),
                   "RocksDBDataSink: can't find column handle for ", real_name);
        const auto* column =
          basics::downCast<const SereneDBColumn>(handle->second);
        columns.emplace_back(column->Id(), column->name());
      }

      return irs::ResolveBool(
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate,
        [&]<bool IsUpdate>() -> std::unique_ptr<velox::connector::DataSink> {
          std::vector<velox::column_index_t> pk_indices;
          if constexpr (IsUpdate) {
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
            auto update_sinks =
              CreateIndexWriters<axiom::connector::WriteKind::kUpdate>(
                object_key, transaction,
                std::span{columns.begin() + table.PKType()->size(),
                          columns.end()},
                table.UsedForUpdatePK());
            if (table.UsedForUpdatePK() || !update_sinks.empty()) {
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
              table.name(), rocksdb_transaction, _cf,
              *connector_query_ctx->memoryPool(), object_key, pk_indices,
              columns, all_column_oids, table.UsedForUpdatePK(), table.type(),
              serene_insert_handle.NumberOfRowsAffected(),
              std::move(update_sinks), table_lock);
          } else if (table.BackfillIndexId().isSet()) {
            auto backfill_writer =
              CreateBackfillIndexWriter(table.BackfillIndexId(), transaction);
            return std::make_unique<RocksDBIndexBackfillDataSink>(
              *connector_query_ctx->memoryPool(), object_key, pk_indices,
              columns, std::move(backfill_writer), table_lock);
          } else {
            auto insert_sinks =
              CreateIndexWriters<axiom::connector::WriteKind::kInsert>(
                object_key, transaction);
            if (table.BulkInsert()) {
              const bool is_generated_pk = pk_indices.empty();
              if (is_generated_pk) {
                return std::make_unique<SSTInsertDataSink<true>>(
                  _db, _cf, *connector_query_ctx->memoryPool(), object_key,
                  pk_indices, columns, std::move(insert_sinks), table_lock);
              } else {
                return std::make_unique<SSTInsertDataSink<false>>(
                  _db, _cf, *connector_query_ctx->memoryPool(), object_key,
                  pk_indices, columns, std::move(insert_sinks), table_lock);
              }
            }

            return std::make_unique<RocksDBInsertDataSink>(
              table.name(), rocksdb_transaction, _cf,
              *connector_query_ctx->memoryPool(), object_key, pk_indices,
              columns, table.WriteConflictPolicy(),
              serene_insert_handle.NumberOfRowsAffected(),
              std::move(insert_sinks), table_lock);
          }
        });
    }

    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kDelete) {
      columns.reserve(table.type()->size());
      for (auto& col : table.type()->names()) {
        auto handle = table.columnMap().find(col);
        SDB_ASSERT(handle != table.columnMap().end(),
                   "RocksDBDataSink: can't find column handle for ", col);
        const auto* column =
          basics::downCast<const SereneDBColumn>(handle->second);
        columns.emplace_back(column->Id(), column->name());
      }
      auto& rocksdb_transaction = transaction.EnsureRocksDBTransaction();
      auto delete_sinks =
        CreateIndexWriters<axiom::connector::WriteKind::kDelete>(object_key,
                                                                 transaction);
      return std::make_unique<RocksDBDeleteDataSink>(
        rocksdb_transaction, _cf, table.type(), object_key, columns,
        serene_insert_handle.NumberOfRowsAffected(), std::move(delete_sinks),
        table_lock);
    }

    VELOX_UNSUPPORTED("Unsupported write kind");
  }

  folly::Executor* ioExecutor() const final { return nullptr; }

 private:
  rocksdb::TransactionDB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
};

}  // namespace sdb::connector
