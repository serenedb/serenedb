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

#pragma once

#include <duckdb/main/database.hpp>

namespace sdb::connector {

// Elasticsearch-compatible operations as table functions; the HTTP handlers
// are thin glue around CALL es_*(...). An ES index is a table in the "es"
// schema: "_id" VARCHAR primary key, one typed column per mapped property,
// "_source" VARCHAR with the original document; "text" properties get an
// inverted index, which is also what distinguishes them from "keyword" when
// the mapping is read back.
//
//   es_create_index(name, mappings_json) -> (acknowledged BOOLEAN)
//   es_drop_index(name)                  -> (acknowledged BOOLEAN)
//   es_mapping(name)                     -> (mappings VARCHAR)
//   es_cat_indices()                     -> (index VARCHAR, docs_count BIGINT)
void RegisterEsFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
