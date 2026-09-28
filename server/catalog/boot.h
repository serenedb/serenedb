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

#include <duckdb/common/identifier.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class AttachedDatabase;
class ClientContext;
struct AttachInfo;
struct DBConfig;

}  // namespace duckdb
namespace sdb::catalog {

struct DataDirectory final : duckdb::StorageExtensionInfo {
  explicit DataDirectory(std::string directory);

  std::string ClusterFile() const;
  std::string DatabaseDir() const;
  std::string DatabaseFile(duckdb::idx_t oid) const;

  std::string directory;
};

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AttachVisibility visibility);

void RemoveDatabaseFiles(duckdb::AttachedDatabase& cluster, duckdb::idx_t oid);

void RegisterClusterStorage(duckdb::DBConfig& config,
                            duckdb::shared_ptr<DataDirectory> layout);

void InitCatalog(std::string_view directory);

}  // namespace sdb::catalog
