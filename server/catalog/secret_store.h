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

#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_storage.hpp>
#include <string>
#include <string_view>

#include "catalog/cluster.h"

namespace sdb::catalog {

duckdb::unique_ptr<duckdb::SecretStorage> CreateSecretStorage(
  duckdb::DatabaseInstance& db, const std::string& name, bool persistent);

duckdb::unique_ptr<duckdb::CatalogEntry> MakeSecretEntry(
  ClusterCatalog& cluster, duckdb::unique_ptr<const duckdb::BaseSecret> secret,
  std::string_view storage, bool persistent);

}  // namespace sdb::catalog
