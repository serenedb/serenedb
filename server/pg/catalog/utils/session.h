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

#include <array>
#include <cstdint>
#include <deque>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/catalog.h"

namespace sdb::pg {

struct SessionSchema {
  std::string name;
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> entry;
  bool system;
};

struct SessionTemporary {
  duckdb::CatalogTransaction transaction;
  duckdb::SchemaCatalogEntry& schema;
};

struct RegOutRecent {
  uint64_t key = 0;
  std::string_view text;
};

inline constexpr size_t kRegOutLimit = 4096;

struct Session {
  duckdb::ClientContext* context;
  duckdb::DuckCatalog* database;
  std::optional<duckdb::CatalogTransaction> transaction;
  std::vector<SessionSchema> search_path;
  std::optional<SessionTemporary> temporary;
  mutable irs::containers::FlatHashMap<std::pair<uint8_t, uint64_t>,
                                       std::string_view>
    reg_out;
  mutable std::deque<std::string> reg_out_texts;
  mutable std::array<RegOutRecent, 64> reg_out_recent;
};

Session MakeSession(duckdb::ClientContext* context);

struct SessionState final : duckdb::FunctionLocalState {
  explicit SessionState(duckdb::ClientContext* context)
    : session{MakeSession(context)} {}

  Session session;
};

}  // namespace sdb::pg
