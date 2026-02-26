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
#include "connector/serenedb_connector.hpp"

namespace sdb::connector::test {

class MockTableHandle;

class MockConnector final : public velox::connector::Connector {
 public:
  explicit MockConnector(const std::string& id, velox::config::ConfigPtr config)
    : Connector{id, std::move(config)} {}
  bool canAddDynamicFilter() const final { return false; }

  bool supportsSplitPreload() const final { return false; }

  bool supportsIndexLookup() const final { return false; }

  std::unique_ptr<velox::connector::DataSource> createDataSource(
    const velox::RowTypePtr& output_type,
    const velox::connector::ConnectorTableHandlePtr& table_handle,
    const velox::connector::ColumnHandleMap& column_handles,
    velox::connector::ConnectorQueryCtx* connector_query_ctx) final {
    VELOX_UNSUPPORTED();
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
    VELOX_UNSUPPORTED();
  }

  void RegisterTableHandle(const std::string& table,
                           std::shared_ptr<MockTableHandle> handle) {
    _table_handles[table] = std::move(handle);
  }

  folly::Executor* ioExecutor() const final { return nullptr; }

  containers::FlatHashMap<std::string, std::shared_ptr<MockTableHandle>>
    _table_handles;
};

class MockColumnHandle final : public velox::connector::ColumnHandle {
 public:
  explicit MockColumnHandle(const std::string& name, catalog::Column::Id id)
    : _name{name}, _id{id} {}

  const std::string& name() const final { return _name; }

  const catalog::Column::Id& Id() const noexcept { return _id; }

 private:
  std::string _name;
  catalog::Column::Id _id;
};

class MockTableHandle final : public velox::connector::ConnectorTableHandle {
 public:
  MockTableHandle(std::string connector_id, std::string_view name,
                  std::vector<velox::core::TypedExprPtr>&& filters)
    : velox::connector::ConnectorTableHandle(connector_id),
      _name(name),
      _filters(std::move(filters)) {}

  const std::string& name() const final { return _name; };

  auto& AcceptedFilters() { return _filters; }

 private:
  std::string _name;
  std::vector<velox::core::TypedExprPtr> _filters;
};

class MockLayout final : public axiom::connector::TableLayout {
 public:
  MockLayout(std::string name, const axiom::connector::Table* table,
             velox::connector::Connector* connector,
             std::vector<const axiom::connector::Column*> columns)
    : axiom::connector::TableLayout(name, table, connector, columns, {}, {},
                                    {}) {
    for (const auto& c : columns) {
      auto& serene_column = basics::downCast<connector::SereneDBColumn>(*c);
      _column_map[c->name()] = serene_column.Id();
    }
  }

  std::pair<int64_t, int64_t> sample(
    const velox::connector::ConnectorTableHandlePtr& handle, float pct,
    const std::vector<velox::core::TypedExprPtr>& extraFilters,
    velox::RowTypePtr outputType = nullptr,
    const std::vector<velox::common::Subfield>& fields = {},
    velox::HashStringAllocator* allocator = nullptr,
    std::vector<axiom::connector::ColumnStatistics>* statistics =
      nullptr) const final {
    return {0, 0};
  }

  velox::connector::ColumnHandlePtr createColumnHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    const std::string& columnName,
    std::vector<velox::common::Subfield> subfields = {}) const final {
    return std::make_shared<MockColumnHandle>(columnName,
                                              _column_map.at(columnName));
  }

  velox::connector::ConnectorTableHandlePtr createTableHandle(
    const axiom::connector::ConnectorSessionPtr& session,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& evaluator,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejectedFilters) const final {
    // accept everything. We will use it for testing later
    auto handle =
      std::make_shared<MockTableHandle>("test", "empty", std::move(filters));
    basics::downCast<MockConnector>(*connector())
      .RegisterTableHandle(table().name(), handle);
    return handle;
  }

 private:
  containers::FlatHashMap<std::string, catalog::Column::Id> _column_map;
};

class TableMock final : public axiom::connector::Table {
 public:
  TableMock(
    MockConnector& connector, std::string name,
    std::vector<std::unique_ptr<const axiom::connector::Column>>&& columns,
    folly::F14FastMap<std::string, velox::Variant> options)
    : axiom::connector::Table(name, std::move(columns), options),
      _layout(name, this, &connector, allColumns()) {
    _layouts.push_back(&_layout);
  }

  uint64_t numRows() const final { return 0; }
  const std::vector<const axiom::connector::TableLayout*>& layouts()
    const final {
    return _layouts;
  }

 private:
  MockLayout _layout;
  std::vector<const axiom::connector::TableLayout*> _layouts;
};

}  // namespace sdb::connector::test
