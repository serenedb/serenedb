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

#include "serenedb_connector.hpp"

#include "basics/static_strings.h"
#include "search_filter_builder.hpp"

namespace sdb::connector {

SereneDBConnectorTableHandle::SereneDBConnectorTableHandle(
  const axiom::connector::ConnectorSessionPtr& session,
  const axiom::connector::TableLayout& layout)
  : velox::connector::ConnectorTableHandle{StaticStrings::kSereneDBConnector},
    _name{layout.name()},
    _table_id{basics::downCast<RocksDBTable>(layout.table()).TableId()},
    _transaction{
      basics::downCast<RocksDBTable>(layout.table()).GetTransaction()} {
  const auto& column_map = layout.table().columnMap();
  SDB_ASSERT(!column_map.empty(),
             "Tables without columns must be processed in analyzer step");

  // TODO(Dronplane): measure the performance! Maybe it's worth selecting the
  // smallest possible field as the effective column, not just the first
  _effective_column_id =
    basics::downCast<const SereneDBColumn>(column_map.begin()->second)->Id();
  if (_effective_column_id == catalog::Column::kGeneratedPKId) {
    // Iterating over generated primary key gives 0 rows,
    // use another one
    SDB_ASSERT(column_map.size() >= 2);
    _effective_column_id = basics::downCast<const SereneDBColumn>(
                             std::next(column_map.begin())->second)
                             ->Id();
  }
  _transaction.AddRocksDBRead();
}

velox::connector::ConnectorTableHandlePtr
SereneDBTableLayout::createTableHandle(
  const axiom::connector::ConnectorSessionPtr& session,
  std::vector<velox::connector::ColumnHandlePtr> column_handles,
  velox::core::ExpressionEvaluator& evaluator,
  std::vector<velox::core::TypedExprPtr> filters,
  std::vector<velox::core::TypedExprPtr>& rejected_filters) const {
  if (const auto* inverted_index_table =
        dynamic_cast<const RocksDBInvertedIndexTable*>(&this->table())) {
    const auto& index = inverted_index_table->GetIndex();
    auto column_getter = [&](std::string_view name) -> const SereneDBColumn* {
      const auto* column = inverted_index_table->findColumn(name);
      if (column) {
        const auto* serene_column = basics::downCast<SereneDBColumn>(column);
        auto index_columns = index.GetColumnIds();

        if (absl::c_find(index_columns, serene_column->Id()) !=
            index.GetColumnIds().end()) {
          return serene_column;
        }
      }
      return nullptr;
    };
    irs::And conjunct_root;
    for (auto& filter_expr : filters) {
      size_t size_before = conjunct_root.size();
      auto result =
        search::ExprToFilter(conjunct_root, filter_expr, column_getter);
      if (result.fail()) {
        SDB_ASSERT(conjunct_root.size() <= size_before + 1);
        if (conjunct_root.size() > size_before) {
          conjunct_root.PopBack();
        }
        rejected_filters.push_back(std::move(filter_expr));
      }
    }
    if (!conjunct_root.empty()) {
      auto handle =
        std::make_shared<SereneDBConnectorTableHandle>(session, *this);
      const auto& snapshot =
        inverted_index_table->GetTransaction().EnsureSearchSnapshot(
          inverted_index_table->GetIndex().GetId());
      // TODO(Dronplane) link irs memory manager to velox pool
      handle->AddSearchQuery(inverted_index_table->GetIndex().GetId(),
                             conjunct_root.prepare({.index = snapshot.reader}));
      return handle;
    }
  }

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

}  // namespace sdb::connector
