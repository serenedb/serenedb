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

namespace sdb::connector {

SereneDBConnectorTableHandle::SereneDBConnectorTableHandle(
  const axiom::connector::ConnectorSessionPtr& session,
  const axiom::connector::TableLayout& layout)
  : velox::connector::ConnectorTableHandle{"serenedb"},
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

}  // namespace sdb::connector
