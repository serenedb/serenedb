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

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/main/attached_database.hpp>
#include <optional>
#include <span>
#include <string>

#include "catalog/catalog.h"

namespace sdb::search {

struct MissingStorage {
  duckdb::reference<const duckdb::CatalogEntry> entry;
  std::string path;
};

// Opens the durable search storages of [catalogs] at once; the first missing
// storage by oid, if any.
std::optional<MissingStorage> OpenSearchStorages(
  std::span<const duckdb::reference<catalog::SereneDBCatalog>> catalogs);

void RecoverSearchStorages(
  std::span<const duckdb::shared_ptr<duckdb::AttachedDatabase>> attached);
void StartSearchStorageTasks();

}  // namespace sdb::search
