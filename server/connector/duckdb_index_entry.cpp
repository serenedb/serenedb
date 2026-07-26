////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "connector/duckdb_index_entry.h"

#include "basics/down_cast.h"
#include "catalog/inverted_index.h"

namespace sdb::connector {

SereneDBIndexEntry::SereneDBIndexEntry(duckdb::Catalog& catalog,
                                       duckdb::SchemaCatalogEntry& schema,
                                       duckdb::CreateIndexInfo& info,
                                       catalog::IndexInfoRef index,
                                       std::string table_name)
  : duckdb::IndexCatalogEntry{catalog, schema, info},
    _sdb_index{std::move(index)},
    _table_name{std::move(table_name)},
    _relation_id{_sdb_index->GetRelationId()} {
  // An ART on the store table or an iresearch directory: the storage is not a
  // duckdb index block list, and the definition lives in the serenedb log.
  // An index carries no owner and no ACL: postgres gives it none, so every
  // privilege decision reads the relation it is built on.
  catalog::AdoptEntryIdentity(*this, _sdb_index->GetId());
}

}  // namespace sdb::connector
