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

#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <string>
#include <string_view>

namespace duckdb {

struct DBConfig;

}  // namespace duckdb
namespace sdb::catalog {

struct DataDirectory final : duckdb::StorageExtensionInfo {
  explicit DataDirectory(std::string directory);

  std::string ClusterFile() const;
  std::string DatabaseFile(std::string_view name) const;

  std::string directory;
};

void RegisterClusterStorage(duckdb::DBConfig& config,
                            duckdb::shared_ptr<DataDirectory> layout);

void InitCatalog(std::string_view directory);

}  // namespace sdb::catalog
