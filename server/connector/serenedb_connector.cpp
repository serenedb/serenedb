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

namespace sdb::connector {

SereneDBConnectorTableHandle::SereneDBConnectorTableHandle(
  const axiom::connector::ConnectorSessionPtr& session,
  const axiom::connector::TableLayout& layout)
  : velox::connector::ConnectorTableHandle{StaticStrings::kSereneDBConnector},
    _name{layout.name()},
    _table_id{basics::downCast<RocksDBTable>(layout.table()).TableId()} {
  const auto& column_map = layout.table().columnMap();
  SDB_ASSERT(!column_map.empty(),
             "Tables without columns must be processed in analyzer step");

  // TODO(Dronplane): measure the performance! Maybe it worth select smallest
  // possible field as count field not just first
  _table_count_field =
    basics::downCast<const SereneDBColumn>(column_map.begin()->second)->Id();
  if (_table_count_field == catalog::Column::kGeneratedPKId) {
    // Iterating over generated primary key gives 0 rows,
    // use another one
    SDB_ASSERT(column_map.size() >= 2);
    _table_count_field = basics::downCast<const SereneDBColumn>(
                           std::next(column_map.begin())->second)
                           ->Id();
  }
  _txn = ExtractTransaction(session);
}

uint64_t RocksDBTable::numRows() const {
  uint64_t count = 0;
  uint64_t size = 0;
  SDB_ASSERT(!_column_handles.empty() && _column_handles.front());
  const auto* column_handle = _column_handles.front().get();
  if (column_handle->Id() == catalog::Column::kGeneratedPKId) {
    SDB_ASSERT(_column_handles.size() >= 2);
    column_handle = _column_handles[1].get();
  }
  auto [start, end] =
    key_utils::CreateTableColumnRange(TableId(), column_handle->Id());
  auto* db = GetServerEngine().db();
  auto connector = std::dynamic_pointer_cast<SereneDBConnector>(
    velox::connector::getConnector(StaticStrings::kSereneDBConnector));
  SDB_ASSERT(connector);
  auto* cf = &connector->GetColumnFamily();
  db->GetApproximateMemTableStats(cf, rocksdb::Range{start, end}, &count,
                                  &size);
  return count;
}

}  // namespace sdb::connector
