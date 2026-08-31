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

#include <cstdint>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/entry.h"

namespace duckdb {

class Serializer;
class Deserializer;
class ClientContext;
class Connection;

}  // namespace duckdb
namespace sdb::catalog {

// One foreign server, in the form a catalog entry is built from. duckdb has a
// FOREIGN_SERVER_ENTRY but no CreateInfo under it, so this is one of ours.
// Owner and ACL live on the entry: duckdb's CreateInfo has nowhere to put them.
class CreateForeignServerInfo final : public duckdb::CreateInfo {
 public:
  CreateForeignServerInfo()
    : duckdb::CreateInfo{duckdb::CatalogType::FOREIGN_SERVER_ENTRY} {}
  CreateForeignServerInfo(ObjectId id, ObjectId database_id,
                          std::string_view name, std::string fdw_name,
                          std::vector<std::string> option_keys,
                          std::vector<std::string> option_values);

  void Serialize(duckdb::Serializer& sink) const final;
  std::string ToString() const final;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  static duckdb::unique_ptr<duckdb::CreateInfo> Deserialize(
    duckdb::Deserializer& src);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  void SetId(ObjectId id) noexcept { oid = id.id(); }

  ObjectId GetDatabaseId() const noexcept { return ObjectId{parent_oid}; }
  void SetDatabaseId(ObjectId id) noexcept { parent_oid = id.id(); }
  ObjectId GetParentId() const noexcept { return GetDatabaseId(); }

  std::string_view GetName() const noexcept {
    return GetQualifiedName().Name().GetIdentifierName();
  }

  std::string_view GetFdwName() const noexcept { return _fdw_name; }

  std::span<const std::string> OptionKeys() const noexcept {
    return _option_keys;
  }
  std::span<const std::string> OptionValues() const noexcept {
    return _option_values;
  }
  // "key=value" strings in insertion order (the pg_foreign_server text[]
  // shape), unredacted -- pg_foreign_server is superuser-only.
  std::vector<std::string> GetStringOptions() const;

 private:
  std::string _fdw_name;
  std::vector<std::string> _option_keys;
  std::vector<std::string> _option_values;
};

// Identity of the foreign-server attachment currently holding `server_name`, or
// 0 when no foreign-server attachment holds it. Capture the id before changing
// the catalog row: a detach names the attachment it means to remove, so an
// older DROP can never tear down a concurrent same-named CREATE's attachment.
uint64_t ForeignServerAttachmentId(std::string_view server_name);

// A foreign server whose attachment outlived its catalog row, with the identity
// observed when the row was removed.
struct ForeignServerAttachment {
  std::string name;
  uint64_t attachment_id = 0;
};

// True when the FDW name maps to a connector storage type.
bool IsSupportedFdw(std::string_view fdw_name);

// The supported FDW names, comma-separated -- for error hints.
std::string SupportedFdwList();

// Outcome of RunForeignServerAttach; Failed carries the connector's message.
struct ForeignServerAttachResult {
  enum class Status : uint8_t {
    Unsupported,
    Attached,
    Failed,
  };
  Status status = Status::Unsupported;
  std::string error;
  // Identity of the attachment created on success; 0 otherwise.
  uint64_t attachment_id = 0;
};

// Registers the transient secret, runs the ATTACH on `conn`, drops the secret,
// and reports the outcome. Credentials come from the server's OPTIONS; the
// attach alias is the server name.
ForeignServerAttachResult RunForeignServerAttach(
  duckdb::Connection& conn, const CreateForeignServerInfo& server);

// Best-effort DETACH of a server's live attachment (drop plans remove catalog
// state only, never the attachment); errors are swallowed -- boot replay may
// have skipped a down remote. Detaches only while `attachment_id` still holds
// the alias, so it can destroy neither a newer attachment nor a same-named
// serenedb database; a zero id is a no-op.
void DetachForeignServerAttachment(std::string_view server_name,
                                   uint64_t attachment_id);

}  // namespace sdb::catalog
