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

#include <string>

#include "catalog/object.h"

namespace sdb::catalog {

// A SQL view stored in the catalog.
// The view body is stored as SQL text (SELECT query).
// DuckDB parses it on demand to create view entries.
class PgSqlView final : public SchemaObject {
 public:
  PgSqlView(ObjectId database_id, ObjectId id, std::string_view name,
            std::string query);

  static std::shared_ptr<PgSqlView> ReadInternal(vpack::Slice slice,
                                                 ReadContext ctx);

  void WriteInternal(vpack::Builder& b) const final;
  std::shared_ptr<Object> Clone() const final;

  std::string_view GetQuery() const noexcept { return _query; }

 private:
  std::string _query;  // SELECT query text
};

}  // namespace sdb::catalog
