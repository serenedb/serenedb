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
#include <duckdb/main/config.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/transaction/transaction_manager.hpp>
#include <string>
#include <string_view>

namespace duckdb {

class AttachedDatabase;
class Catalog;
class ClientContext;
struct AttachInfo;
struct DBConfig;

}  // namespace duckdb
namespace sdb::catalog {

struct DataDirectory {
  explicit DataDirectory(std::string directory);

  bool InMemory() const noexcept { return directory.empty(); }
  std::string LogDir() const;
  std::string StoreDir() const;

  std::string directory;
  std::string engine;
};

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AccessMode access_mode,
            duckdb::AttachVisibility visibility);

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog);

duckdb::InternalCatalog CreateInternalCatalog(duckdb::AttachedDatabase& db,
                                              const DataDirectory& layout);

void InitCatalog(bool default_database, bool initialized);

void CloseLog();

}  // namespace sdb::catalog
