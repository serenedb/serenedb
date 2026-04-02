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
#include <velox/dwio/common/BufferedInput.h>
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
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/parquet_materializer.hpp"
#include "connector/rocksdb_filter.hpp"
#include "connector/rocksdb_materializer.hpp"
#include "connector/search_filter_printer.hpp"
#include "connector/search_scan_data_source.hpp"
#include "connector/search_sink_writer.hpp"
#include "connector/secondary_data_source.hpp"
#include "connector/secondary_sink_writer.hpp"
#include "connector/sink_writer_base.hpp"
#include "connector/text_materializer.hpp"
#include "data_sink.hpp"
#include "data_source.hpp"
#include "file_table.hpp"
#include "pg/command_executor.h"
#include "query/transaction.h"
#include "query/utils.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

inline void ExtractInputFields(
  const velox::core::TypedExprPtr& expr,
  containers::FlatHashSet<std::string_view>& names) {
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

template<axiom::connector::WriteKind Kind, bool Unique>
std::unique_ptr<SinkIndexWriter> MakeSecondaryIndexWriter(
  rocksdb::Transaction& transaction, ObjectId shard_id,
  std::span<const catalog::Column::Id> columns,
  std::vector<velox::column_index_t> sk_children,
  std::vector<velox::column_index_t> old_sk_children) {
  if constexpr (Kind == axiom::connector::WriteKind::kInsert) {
    return std::make_unique<SecondarySinkInsertWriter<Unique>>(
      transaction, shard_id, columns, std::move(sk_children));
  } else if constexpr (Kind == axiom::connector::WriteKind::kUpdate) {
    return std::make_unique<SecondarySinkUpdateWriter<Unique>>(
      transaction, shard_id, columns, std::move(sk_children),
      std::move(old_sk_children));
  } else {
    static_assert(Kind == axiom::connector::WriteKind::kDelete,
                  "Unexpected WriteKind");
    return std::make_unique<SecondarySinkDeleteWriter<Unique>>(
      transaction, shard_id, columns, std::move(sk_children));
  }
}

template<axiom::connector::WriteKind Kind>
std::unique_ptr<SinkIndexWriter> MakeInvertedIndexWriter(
  irs::IndexWriter::Transaction& index_transaction,
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const catalog::InvertedIndex& index) {
  if constexpr (Kind == axiom::connector::WriteKind::kInsert) {
    return std::make_unique<SearchSinkInsertWriter>(
      index_transaction, MakeAnalyzerProvider(snapshot, index),
      index.GetColumnIds());
  } else if constexpr (Kind == axiom::connector::WriteKind::kUpdate) {
    return std::make_unique<SearchSinkUpdateWriter>(
      index_transaction, MakeAnalyzerProvider(snapshot, index),
      index.GetColumnIds());
  } else {
    static_assert(Kind == axiom::connector::WriteKind::kDelete,
                  "Unexpected WriteKind");
    return std::make_unique<SearchSinkDeleteWriter>(index_transaction);
  }
}

inline std::unique_ptr<SinkIndexWriter> CreateBackfillIndexWriter(
  ObjectId backfill_index_id, query::Transaction& transaction) {
  auto snapshot = transaction.EnsureCatalogSnapshot();
  auto shard = snapshot->GetIndexShard(backfill_index_id);
  SDB_ASSERT(shard);
  auto index =
    snapshot->template GetObject<catalog::Index>(shard->GetIndexId());
  SDB_ASSERT(index);

  SDB_ASSERT(shard->GetType() == IndexType::Inverted);
  auto& inverted_shard = basics::downCast<search::InvertedIndexShard>(*shard);
  auto& inverted_index = basics::downCast<const catalog::InvertedIndex>(*index);
  return std::make_unique<SearchSinkBackfillWriter>(
    inverted_shard, MakeAnalyzerProvider(snapshot, inverted_index),
    inverted_index.GetColumnIds());
}

template<axiom::connector::WriteKind Kind>
std::vector<std::unique_ptr<SinkIndexWriter>> CreateIndexWriters(
  ObjectId table_id, query::Transaction& transaction,
  std::span<const ColumnInfo> updated_columns, bool pk_updated,
  const velox::RowType* input_type) {
  std::vector<std::unique_ptr<SinkIndexWriter>> writers;

  auto resolve_index_writer = [&](auto& index_transaction,
                                  const catalog::Index& index) {
    if constexpr (std::is_same_v<std::decay_t<decltype(index_transaction)>,
                                 irs::IndexWriter::Transaction>) {
      const auto& inverted_index =
        basics::downCast<catalog::InvertedIndex>(index);
      auto snapshot = transaction.EnsureCatalogSnapshot();
      writers.push_back(MakeInvertedIndexWriter<Kind>(
        index_transaction, snapshot, inverted_index));
    } else if constexpr (std::is_same_v<
                           std::decay_t<decltype(index_transaction)>,
                           rocksdb::Transaction>) {
      auto snapshot = transaction.EnsureCatalogSnapshot();
      auto shard = snapshot->GetIndexShard(index.GetId());
      SDB_ASSERT(shard);
      auto table =
        snapshot->template GetObject<catalog::Table>(index.GetRelationId());
      SDB_ASSERT(table);
      SDB_ASSERT(input_type);
      const auto& id2col = table->IdToColumn();
      std::vector<velox::column_index_t> sk_children;
      std::vector<velox::column_index_t> old_sk_children;
      sk_children.reserve(index.GetColumnIds().size());
      for (auto col_id : index.GetColumnIds()) {
        auto it = id2col.find(col_id);
        SDB_ASSERT(it != id2col.end());
        const auto& name = it->second->name;
        if constexpr (Kind == axiom::connector::WriteKind::kUpdate) {
          auto old_name = catalog::Column::GenerateOldValueName(name);
          old_sk_children.push_back(input_type->getChildIdx(old_name));
          auto maybe = input_type->getChildIdxIfExists(name);
          sk_children.push_back(maybe.value_or(old_sk_children.back()));
        } else {
          sk_children.push_back(input_type->getChildIdx(name));
        }
      }
      const auto& sec_index =
        basics::downCast<const catalog::SecondaryIndex>(index);
      irs::ResolveBool(sec_index.IsUnique(), [&]<bool Unique>() {
        writers.push_back(MakeSecondaryIndexWriter<Kind, Unique>(
          index_transaction, shard->GetId(), index.GetColumnIds(),
          std::move(sk_children), std::move(old_sk_children)));
      });
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
    transaction.EnsureCatalogSnapshot()->GetObject<catalog::Table>(table_id);
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
    const axiom::connector::TableLayout& layout,
    std::vector<SpecificPoint> points,
    velox::core::TypedExprPtr remaining_filter);

  bool supportsIndexLookup() const final { return false; }

  const std::string& name() const final { return _name; }

  std::string toString() const final;

  ObjectId TableId() const noexcept { return _table_id; }

  const catalog::Column::Id& GetEffectiveColumnId() const noexcept {
    return _effective_column_id;
  }

  auto& GetTransaction() const noexcept { return _transaction; }

  auto& GetPKType() const noexcept { return _pk_type; }

  auto& GetRemainingFilter() const noexcept { return _remaining_filter; }

  const std::vector<SpecificPoint>& GetPoints() const noexcept {
    return _points;
  }

  const containers::FlatHashMap<std::string, FilterColumn>& GetTableColumnMap()
    const noexcept {
    return _table_column_map;
  }

 private:
  std::string _name;
  ObjectId _table_id;
  catalog::Column::Id _effective_column_id;
  query::Transaction& _transaction;
  velox::RowTypePtr _pk_type;
  std::vector<SpecificPoint> _points;
  velox::core::TypedExprPtr _remaining_filter;
  containers::FlatHashMap<std::string, FilterColumn> _table_column_map;
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

inline catalog::Column::Id ComputeEffectiveColumnId(
  const folly::F14FastMap<std::string, const axiom::connector::Column*>&
    column_map) {
  SDB_ASSERT(!column_map.empty());
  auto id =
    basics::downCast<const SereneDBColumn>(column_map.begin()->second)->Id();
  if (id == catalog::Column::kGeneratedPKId) {
    SDB_ASSERT(column_map.size() >= 2);
    id = basics::downCast<const SereneDBColumn>(
           std::next(column_map.begin())->second)
           ->Id();
  }
  return id;
}

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
      _stats{transaction.GetTableStats(_table_id)},
      _catalog_table{init.collection} {
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

  const auto& PKType() const noexcept { return _pk_type; }

  const auto& CatalogTable() const noexcept { return _catalog_table; }

  auto& GetTransaction() const noexcept { return _transaction; }

  decltype(auto) WriteConflictPolicy(this auto&& self) noexcept {
    return (self._write_conflict_policy);
  }

  decltype(auto) UsedForUpdatePK(this auto&& self) noexcept {
    return (self._update_pk);
  }

  decltype(auto) BulkInsert(this auto&& self) noexcept {
    return (self._bulk_insert);
  }

  decltype(auto) CreateIndexState(this auto&& self) noexcept {
    return (self._create_index_state);
  }

 private:
  std::vector<std::unique_ptr<SereneDBTableLayout>> _layout_handles;
  std::vector<const axiom::connector::TableLayout*> _layouts;
  velox::RowTypePtr _pk_type;
  ObjectId _table_id;
  query::Transaction& _transaction;
  catalog::TableStats _stats;
  const catalog::Table& _catalog_table;
  enum WriteConflictPolicy _write_conflict_policy =
    WriteConflictPolicy::EmitError;
  bool _update_pk = false;
  bool _bulk_insert = false;
  pg::CreateIndexState* _create_index_state = nullptr;
};

class IndexTable : public axiom::connector::Table {
 public:
  static std::vector<std::unique_ptr<const axiom::connector::Column>>
  CopyColumns(const axiom::connector::Table& table,
              const catalog::Index& index) {
    std::vector<std::unique_ptr<const axiom::connector::Column>> columns;
    columns.reserve(table.allColumns().size() + 1);
    for (const auto* col : table.allColumns()) {
      auto* sdb_col = basics::downCast<const SereneDBColumn>(col);
      columns.push_back(std::make_unique<SereneDBColumn>(
        col->name(), col->type(), sdb_col->Id()));
    }
    if (index.GetIndexType() == IndexType::Inverted) {
      // Always add the score column so createColumnHandle can resolve it.
      // It is only included in the output type when a scorer is active.
      auto score_name =
        catalog::Column::GenerateScoreName(table.type()->names());
      columns.push_back(std::make_unique<SereneDBColumn>(
        score_name, velox::REAL(), catalog::Column::kInvertedIndexScoreId));
    }
    return columns;
  }

  IndexTable(query::Transaction& transaction, axiom::connector::TablePtr table,
             const catalog::Index& index)
    : Table(table->name(), CopyColumns(*table, index)),
      _transaction{transaction},
      _table{std::move(table)},
      _index{index} {
    auto* source = _table->layouts().front();
    auto layout = std::make_unique<SereneDBTableLayout>(
      name(), *this, *source->connector(), allColumns(),
      std::vector<const axiom::connector::Column*>{},
      std::vector<axiom::connector::SortOrder>{});
    _layouts.push_back(layout.get());
    _layout_handles.push_back(std::move(layout));
  }

  const auto& GetIndex() const noexcept { return _index; }
  const auto& GetTable() const noexcept { return _table; }
  auto& GetTransaction() const noexcept { return _transaction; }

  const std::vector<const axiom::connector::TableLayout*>& layouts()
    const final {
    return _layouts;
  }

  uint64_t numRows() const final { return _table->numRows(); }

  std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
    axiom::connector::WriteKind kind) const final {
    return _table->rowIdHandles(kind);
  }

  void SetScorer(std::shared_ptr<const irs::Scorer> scorer) noexcept {
    SDB_ASSERT(_index.GetIndexType() == IndexType::Inverted);
    _scorer = std::move(scorer);
  }

  const std::shared_ptr<const irs::Scorer>& GetScorerPtr() const noexcept {
    SDB_ASSERT(_index.GetIndexType() == IndexType::Inverted);
    return _scorer;
  }

 private:
  query::Transaction& _transaction;
  axiom::connector::TablePtr _table;
  const catalog::Index& _index;
  std::vector<std::unique_ptr<SereneDBTableLayout>> _layout_handles;
  std::vector<const axiom::connector::TableLayout*> _layouts;
  std::shared_ptr<const irs::Scorer> _scorer;
};

class InvertedIndexTableHandle final
  : public velox::connector::ConnectorTableHandle {
 public:
  using FilterColumn = SereneDBConnectorTableHandle::FilterColumn;

  InvertedIndexTableHandle(const IndexTable& table, ObjectId index_id,
                           irs::Filter::Query::ptr search_query,
                           std::shared_ptr<const irs::Scorer> scorer,
                           std::string search_filter_str)
    : velox::connector::ConnectorTableHandle{StaticStrings::kSereneDBConnector},
      _name{table.name()},
      _table_id{table.GetIndex().GetRelationId()},
      _transaction{table.GetTransaction()},
      _index_id{index_id},
      _search_query{std::move(search_query)},
      _scorer{std::move(scorer)},
      _underlying_table{table.GetTable()},
      _search_filter_str{std::move(search_filter_str)} {
    const auto& column_map = table.columnMap();
    _effective_column_id = ComputeEffectiveColumnId(column_map);
    for (const auto& [orig_name, col_ptr] : column_map) {
      const auto* scol = basics::downCast<const SereneDBColumn>(col_ptr);
      _table_column_map.emplace(orig_name,
                                FilterColumn{scol->Id(), scol->type()});
    }
  }

  bool supportsIndexLookup() const final { return false; }

  const std::string& name() const final { return _name; }

  std::string toString() const final {
    return absl::StrCat(_name, ", type=search_lookup",
                        _search_filter_str.empty()
                          ? ""
                          : absl::StrCat(", filter=", _search_filter_str));
  }

  ObjectId TableId() const noexcept { return _table_id; }

  catalog::Column::Id GetEffectiveColumnId() const noexcept {
    return _effective_column_id;
  }

  auto& GetTransaction() const noexcept { return _transaction; }

  ObjectId GetIndexId() const noexcept { return _index_id; }

  const auto& GetSearchQuery() const noexcept { return *_search_query; }

  const irs::Scorer* GetScorer() const noexcept { return _scorer.get(); }

  const auto& GetTableColumnMap() const noexcept { return _table_column_map; }

  const axiom::connector::Table& GetUnderlyingTable() const noexcept {
    return *_underlying_table;
  }

 private:
  std::string _name;
  ObjectId _table_id;
  catalog::Column::Id _effective_column_id;
  query::Transaction& _transaction;
  ObjectId _index_id;
  irs::Filter::Query::ptr _search_query;
  std::shared_ptr<const irs::Scorer> _scorer;
  axiom::connector::TablePtr _underlying_table;
  containers::FlatHashMap<std::string, FilterColumn> _table_column_map;
  std::string _search_filter_str;
};

class SecondaryIndexTableHandle final
  : public velox::connector::ConnectorTableHandle {
 public:
  using FilterColumn = SereneDBConnectorTableHandle::FilterColumn;

  SecondaryIndexTableHandle(std::string name, ObjectId table_id,
                            query::Transaction& transaction, ObjectId shard_id,
                            std::vector<SpecificPoint> points,
                            velox::RowTypePtr sk_type,
                            const axiom::connector::Table& underlying_table,
                            bool unique)
    : velox::connector::ConnectorTableHandle{StaticStrings::kSereneDBConnector},
      _name{std::move(name)},
      _table_id{table_id},
      _transaction{transaction},
      _effective_column_id{
        ComputeEffectiveColumnId(underlying_table.columnMap())},
      _shard_id{shard_id},
      _points{std::move(points)},
      _sk_type{std::move(sk_type)},
      _underlying_table{&underlying_table},
      _unique{unique} {}

  bool supportsIndexLookup() const final { return false; }

  const std::string& name() const final { return _name; }

  std::string toString() const final {
    const auto& names = _sk_type->names();
    const auto& types = _sk_type->children();
    std::string points_str = absl::StrJoin(
      _points, ", ", [&](std::string* out, const SpecificPoint& point) {
        absl::StrAppend(out, "(");
        for (size_t i = 0; i < point.size(); ++i) {
          if (i > 0) {
            absl::StrAppend(out, ", ");
          }
          absl::StrAppend(out, names[i], "=", point[i].toString(types[i]));
        }
        absl::StrAppend(out, ")");
      });
    return absl::StrCat(_name, ", scan=secondary_index, points=[", points_str,
                        "]");
  }

  ObjectId TableId() const noexcept { return _table_id; }

  catalog::Column::Id GetEffectiveColumnId() const noexcept {
    return _effective_column_id;
  }

  auto& GetTransaction() const noexcept { return _transaction; }

  ObjectId GetShardId() const noexcept { return _shard_id; }

  const auto& GetPoints() const noexcept { return _points; }

  const auto& GetSKType() const noexcept { return _sk_type; }

  const auto& GetUnderlyingTable() const noexcept { return *_underlying_table; }

  bool IsUnique() const noexcept { return _unique; }

 private:
  std::string _name;
  ObjectId _table_id;
  query::Transaction& _transaction;
  catalog::Column::Id _effective_column_id;
  ObjectId _shard_id;
  std::vector<SpecificPoint> _points;
  velox::RowTypePtr _sk_type;
  const axiom::connector::Table* _underlying_table;
  bool _unique;
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
    auto& transaction = serene_insert_handle->GetTransaction();
    const auto kind = serene_insert_handle->Kind();

    const int64_t number_of_rows_affected =
      rocksdb_table.BulkInsert() ? get_total_rows_from_write_results()
                                 : serene_insert_handle->NumberOfRowsAffected();

    if (kind != axiom::connector::WriteKind::kUpdate) {
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

    std::vector<catalog::Column::Id> column_oids;
    if (output_type->size() > 0) {
      column_oids.reserve(output_type->size());
      for (const auto& name : output_type->names()) {
        auto handle = column_handles.find(name);
        SDB_ENSURE(handle != column_handles.end(), ERROR_INTERNAL,
                   "DataSource: can't find column handle for ", name);
        column_oids.push_back(
          basics::downCast<const SereneDBColumnHandle>(handle->second)->Id());
      }
    }

    if (const auto* sec_handle =
          dynamic_cast<const SecondaryIndexTableHandle*>(table_handle.get())) {
      return CreateSecondaryIndexDataSource(
        output_type, *sec_handle, std::move(column_oids), column_handles,
        connector_query_ctx);
    }

    if (const auto* inv_handle =
          dynamic_cast<const InvertedIndexTableHandle*>(table_handle.get())) {
      return CreateSearchInvertedIndexDataSource(
        output_type, *inv_handle, std::move(column_oids), column_handles,
        connector_query_ctx);
    }

    const auto& serene_table_handle =
      basics::downCast<const SereneDBConnectorTableHandle>(*table_handle);
    const auto& object_key = serene_table_handle.TableId();

    velox::RowTypePtr read_type = output_type;
    const size_t output_column_count = output_type->size();
    velox::core::TypedExprPtr compiled_filter;

    if (const auto& remaining_filter =
          serene_table_handle.GetRemainingFilter()) {
      std::unordered_map<std::string, velox::core::TypedExprPtr> name_mapping;
      containers::FlatHashSet<std::string_view> output_original_names;
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
      containers::FlatHashSet<std::string_view> filter_field_names;
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

    const bool needs_read_your_own_writes =
      transaction.HasRocksDBWrite() &&
      transaction.Get<VariableType::Bool>("sdb_read_your_own_writes");

    const auto* snapshot = &transaction.EnsureRocksDBSnapshot();
    if (needs_read_your_own_writes) {
      auto& rocksdb_transaction = transaction.GetRocksDBTransaction();
      SDB_ASSERT(snapshot == rocksdb_transaction.GetSnapshot());

#ifdef SDB_FAULT_INJECTION
      // TODO(mkornaukhov): Find a better way. Maybe make failpoints database
      // bindable to allow parallel execution.
      auto table_ptr =
        transaction.EnsureCatalogSnapshot()->GetObject<catalog::Table>(
          object_key);
      SDB_ASSERT(table_ptr);
      auto fail_on_ryow = absl::StrCat(table_ptr->GetName(), "_fail_on_ryow");
      SDB_IF_FAILURE(fail_on_ryow) {
        SDB_THROW(ERROR_DEBUG, fail_on_ryow, " condition failed");
      }
#endif

      const auto& points = serene_table_handle.GetPoints();
      if (!points.empty()) {
        return std::make_unique<RocksDBRYOWPointLookupDataSource>(
          *connector_query_ctx->memoryPool(), _cf, read_type, column_oids,
          object_key, points, output_column_count, compiled_filter,
          rocksdb_transaction.GetSnapshot(),
          connector_query_ctx->expressionEvaluator(), rocksdb_transaction,
          PrimaryKeyBuilder{object_key, serene_table_handle.GetPKType()},
          PrimaryKeyColumnBuilder{});
      }

      return std::make_unique<RocksDBRYOWFullScanDataSource>(
        *connector_query_ctx->memoryPool(), rocksdb_transaction, _cf, read_type,
        column_oids, serene_table_handle.GetEffectiveColumnId(), object_key,
        output_column_count, rocksdb_transaction.GetSnapshot(), compiled_filter,
        connector_query_ctx->expressionEvaluator());
    }

    const auto& points = serene_table_handle.GetPoints();
    if (!points.empty()) {
      return std::make_unique<RocksDBSnapshotPointLookupDataSource>(
        *connector_query_ctx->memoryPool(), _cf, read_type, column_oids,
        object_key, points, output_column_count, compiled_filter, snapshot,
        connector_query_ctx->expressionEvaluator(), _db,
        PrimaryKeyBuilder{object_key, serene_table_handle.GetPKType()},
        PrimaryKeyColumnBuilder{});
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
      transaction.EnsureCatalogSnapshot()->GetTableShard(object_key);
    SDB_ASSERT(table_shard);
    auto& table_lock = table_shard->GetTableLock();

    std::vector<ColumnInfo> columns;
    if (serene_insert_handle.Kind() == axiom::connector::WriteKind::kInsert ||
        serene_insert_handle.Kind() == axiom::connector::WriteKind::kUpdate) {
      columns.reserve(input_type->size());
      for (auto& col : input_type->names()) {
        std::string_view real_name = catalog::Column::ExtractColumnName(col);
        auto handle = table.columnMap().find(real_name);
        if (handle == table.columnMap().end()) {
          SDB_ASSERT(catalog::Column::IsOldValueName(col),
                     "Unknown column in input: ", col);
          columns.emplace_back(std::numeric_limits<catalog::Column::Id>::max(),
                               col);
          continue;
        }
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
                table.UsedForUpdatePK(), &input_type->asRow());
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
          } else if (auto* cis = table.CreateIndexState()) {
            auto snapshot = transaction.EnsureCatalogSnapshot();
            auto shard = snapshot->GetIndexShard(cis->index_id);
            SDB_ASSERT(shard);

            auto index =
              snapshot->GetObject<catalog::Index>(shard->GetIndexId());

            containers::FlatHashMap<catalog::Column::Id, const ColumnInfo*>
              id2colinfo;
            id2colinfo.reserve(columns.size());
            for (auto& col : columns) {
              id2colinfo.emplace(col.id, &col);
            }

            if (shard->GetType() == IndexType::Secondary) {
              SDB_ASSERT(index);

              // Build SK column indices into the input RowVector
              std::vector<velox::column_index_t> sk_children;
              sk_children.reserve(index->GetColumnIds().size());
              for (auto col_id : index->GetColumnIds()) {
                auto it = id2colinfo.find(col_id);
                SDB_ASSERT(it != id2colinfo.end());
                sk_children.push_back(it->second - columns.data());
              }

              // For generated-PK tables, include generated PK in key_childs
              auto backfill_pk_indices = pk_indices;
              if (backfill_pk_indices.empty()) {
                auto it = id2colinfo.find(catalog::Column::kGeneratedPKId);
                if (it != id2colinfo.end()) {
                  backfill_pk_indices.push_back(it->second - columns.data());
                }
              }

              const auto& sec_index =
                basics::downCast<const catalog::SecondaryIndex>(*index);
              return irs::ResolveBool(
                sec_index.IsUnique(),
                [&]<bool UniqueIdx>()
                  -> std::unique_ptr<velox::connector::DataSink> {
                  return std::make_unique<
                    SSTInsertDataSink<false, true, UniqueIdx>>(
                    _db, _cf, *connector_query_ctx->memoryPool(),
                    shard->GetId(), backfill_pk_indices, columns,
                    std::vector<std::unique_ptr<SinkIndexWriter>>{}, table_lock,
                    std::move(sk_children));
                });
            }

            SDB_ASSERT(shard->GetType() == IndexType::Inverted);
            auto backfill_writer =
              CreateBackfillIndexWriter(cis->index_id, transaction);
            return std::make_unique<RocksDBIndexBackfillDataSink>(
              *connector_query_ctx->memoryPool(), object_key, pk_indices,
              columns, std::move(backfill_writer), table_lock, cis->progress);
          } else {
            auto insert_sinks =
              CreateIndexWriters<axiom::connector::WriteKind::kInsert>(
                object_key, transaction, {}, false, &input_type->asRow());
            if (table.BulkInsert()) {
              const bool is_generated_pk = pk_indices.empty();
              if (is_generated_pk) {
                return std::make_unique<SSTInsertDataSink<true, false, false>>(
                  _db, _cf, *connector_query_ctx->memoryPool(), object_key,
                  pk_indices, columns, std::move(insert_sinks), table_lock,
                  std::vector<velox::column_index_t>{});
              } else {
                return std::make_unique<SSTInsertDataSink<false, false, false>>(
                  _db, _cf, *connector_query_ctx->memoryPool(), object_key,
                  pk_indices, columns, std::move(insert_sinks), table_lock,
                  std::vector<velox::column_index_t>{});
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
        CreateIndexWriters<axiom::connector::WriteKind::kDelete>(
          object_key, transaction, {}, false, &input_type->asRow());
      // PK columns are first in the input. For generated PK,
      // FillColumnsInfo adds the generated PK as column 0.
      size_t pk_count = table.PKType()->size();
      if (pk_count == 0) {
        pk_count = 1;  // generated PK at index 0
      }
      std::vector<velox::column_index_t> del_pk_indices(pk_count);
      absl::c_iota(del_pk_indices, 0);
      return std::make_unique<RocksDBDeleteDataSink>(
        rocksdb_transaction, _cf, table.type(), object_key, del_pk_indices,
        columns, serene_insert_handle.NumberOfRowsAffected(),
        std::move(delete_sinks), table_lock);
    }

    VELOX_UNSUPPORTED("Unsupported write kind");
  }

  folly::Executor* ioExecutor() const final { return nullptr; }

  template<typename Handle, typename Func>
  auto CreateWithMaterializer(
    const Handle& handle, const velox::RowTypePtr& output_type,
    const velox::RowTypePtr& reader_type,
    std::vector<catalog::Column::Id> column_oids,
    const velox::connector::ColumnHandleMap& column_handles,
    velox::memory::MemoryPool& pool, const rocksdb::Snapshot* snapshot,
    Func&& func) {
    const auto& underlying_table = handle.GetUnderlyingTable();
    if (const auto* file_table =
          dynamic_cast<const ReadFileTable*>(&underlying_table)) {
      auto [source, reader, row_reader] = FileDataSource::CreateReader(
        *file_table->GetOptions(), pool, reader_type, column_handles, {},
        nullptr, nullptr);
      auto format = file_table->GetOptions()->Reader()->fileFormat();
      if (format == velox::dwio::common::FileFormat::TEXT) {
        TextMaterializer mat{pool,
                             std::move(source),
                             std::move(reader),
                             std::move(row_reader),
                             output_type,
                             std::move(column_oids)};
        return func(std::move(mat), _db);
      }
      SDB_ASSERT(format == velox::dwio::common::FileFormat::PARQUET,
                 "Only parquet and text formats are supported");
      ParquetMaterializer mat{pool,
                              std::move(source),
                              std::move(reader),
                              std::move(row_reader),
                              output_type,
                              std::move(column_oids)};
      return func(std::move(mat), _db);
    }

    auto& transaction = handle.GetTransaction();
    const bool needs_ryow =
      transaction.HasRocksDBWrite() &&
      transaction.template Get<VariableType::Bool>("sdb_read_your_own_writes");
    if (needs_ryow) {
      auto& trx = transaction.GetRocksDBTransaction();
      RocksDBMaterializer mat{pool,
                              snapshot,
                              nullptr,
                              &trx,
                              _cf,
                              output_type,
                              std::move(column_oids),
                              handle.GetEffectiveColumnId(),
                              handle.TableId()};
      return func(std::move(mat), trx);
    }
    RocksDBMaterializer mat{pool,
                            snapshot,
                            &_db,
                            nullptr,
                            _cf,
                            output_type,
                            std::move(column_oids),
                            handle.GetEffectiveColumnId(),
                            handle.TableId()};
    return func(std::move(mat), _db);
  }

  std::unique_ptr<velox::connector::DataSource> CreateSecondaryIndexDataSource(
    const velox::RowTypePtr& output_type,
    const SecondaryIndexTableHandle& handle,
    std::vector<catalog::Column::Id> column_oids,
    const velox::connector::ColumnHandleMap& column_handles,
    velox::connector::ConnectorQueryCtx* connector_query_ctx) {
    if (column_oids.empty()) {
      SDB_ASSERT(output_type->size() == 0);
      column_oids.push_back(handle.GetEffectiveColumnId());
    }
    auto& transaction = handle.GetTransaction();
    const auto* snapshot = &transaction.EnsureRocksDBSnapshot();
    auto& pool = *connector_query_ctx->memoryPool();

    const auto& points = handle.GetPoints();
    bool has_null_filter = absl::c_any_of(points, [](const auto& sp) {
      return absl::c_any_of(sp, [](const auto& v) { return v.isNull(); });
    });

    return CreateWithMaterializer(
      handle, output_type, output_type, column_oids, column_handles, pool,
      snapshot,
      [&](auto mat,
          auto& source) -> std::unique_ptr<velox::connector::DataSource> {
        using Mat = decltype(mat);
        using Src = std::remove_reference_t<decltype(source)>;
        constexpr bool kRYOW = std::is_same_v<Src, rocksdb::Transaction>;
        if (handle.IsUnique() && !has_null_filter) {
          SecondaryKeyBuilder key_builder{handle.GetShardId(),
                                          handle.GetSKType()};
          return std::make_unique<
            RocksDBPointLookupDataSource<SecondaryLookupPolicy<kRYOW, Mat>>>(
            pool, _cf, output_type, column_oids, handle.TableId(), points,
            output_type->size(), nullptr, snapshot, nullptr, source,
            std::move(key_builder),
            SecondaryKeyColumnBuilder<Mat>{std::move(mat), pool});
        }
        return irs::ResolveBool(
          handle.IsUnique(),
          [&]<bool UniqueIdx>()
            -> std::unique_ptr<velox::connector::DataSource> {
            // Iterator scan path still uses variant values.
            return std::make_unique<
              SecondaryIndexDataSource<Mat, UniqueIdx, Src>>(
              pool, std::move(mat), source, _cf, snapshot, handle.GetShardId(),
              points, handle.GetSKType());
          });
      });
  }

  std::unique_ptr<velox::connector::DataSource>
  CreateSearchInvertedIndexDataSource(
    const velox::RowTypePtr& output_type,
    const InvertedIndexTableHandle& handle,
    std::vector<catalog::Column::Id> column_oids,
    const velox::connector::ColumnHandleMap& column_handles,
    velox::connector::ConnectorQueryCtx* connector_query_ctx) {
    if (column_oids.empty()) {
      SDB_ASSERT(output_type->size() == 0);
      column_oids.push_back(handle.GetEffectiveColumnId());
    }

    auto& transaction = handle.GetTransaction();

    const bool needs_read_your_own_writes =
      transaction.HasRocksDBWrite() &&
      transaction.Get<VariableType::Bool>("sdb_read_your_own_writes");
    if (needs_read_your_own_writes) {
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "sdb_read_your_own_writes is not supported for inverted index");
    }

    auto& pool = *connector_query_ctx->memoryPool();
    const auto& search_snapshot =
      transaction.EnsureSearchSnapshot(handle.GetIndexId());

    // TODO: teach ScanSpec to skip the score column instead of stripping it
    // from the type.
    std::vector<std::string> reader_names;
    std::vector<velox::TypePtr> reader_types;
    for (size_t i = 0; i < output_type->size(); ++i) {
      if (column_oids[i] != catalog::Column::kInvertedIndexScoreId) {
        reader_names.push_back(output_type->nameOf(i));
        reader_types.push_back(output_type->childAt(i));
      }
    }
    auto reader_type =
      velox::ROW(std::move(reader_names), std::move(reader_types));

    return CreateWithMaterializer(
      handle, output_type, reader_type, column_oids, column_handles, pool,
      search_snapshot.snapshot->GetSnapshot(),
      [&](auto mat,
          auto& source) -> std::unique_ptr<velox::connector::DataSource> {
        using Mat = decltype(mat);
        return std::make_unique<SearchDataSource<Mat>>(
          pool, std::move(mat), search_snapshot.reader, handle.GetSearchQuery(),
          handle.GetScorer());
      });
  }

 private:
  rocksdb::TransactionDB& _db;
  rocksdb::ColumnFamilyHandle& _cf;
};

}  // namespace sdb::connector
