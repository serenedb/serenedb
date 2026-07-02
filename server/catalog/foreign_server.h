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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/object.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

class ForeignServer : public Object {
 public:
  static std::shared_ptr<ForeignServer> Deserialize(duckdb::Deserializer& src,
                                                    ReadContext ctx);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  ForeignServer(ObjectId schema_id, ObjectId id, std::string_view name,
                std::string fdw_name, std::vector<std::string> option_keys,
                std::vector<std::string> option_values);

  std::string_view GetFdwName() const noexcept { return _fdw_name; }

  const std::vector<std::string>& GetOptionKeys() const noexcept {
    return _option_keys;
  }
  const std::vector<std::string>& GetOptionValues() const noexcept {
    return _option_values;
  }

 private:
  std::string _fdw_name;
  std::vector<std::string> _option_keys;
  std::vector<std::string> _option_values;
};

class UserMapping;

// Builds `ATTACH '<connstr>' AS "<name>" (TYPE <storage>)` for a foreign
// server, mapping the FDW name to the connector storage type (clickhouse_fdw ->
// clickhouse, postgres_fdw -> postgres) and the option keys to that dialect
// (database <-> dbname). Returns "" if the FDW is not a supported connector.
// If a `public_mapping` is supplied (a PUBLIC USER MAPPING for this server),
// its options override the server's (e.g. user/password). Shared by create-time
// attach and boot replay so both stay in sync. `alias` overrides the ATTACH
// name (defaults to the server name); used to probe-attach to a throwaway name
// when validating new credentials before detaching the live attachment.
std::string BuildForeignServerAttachSql(
  const ForeignServer& server, const UserMapping* public_mapping = nullptr,
  std::string_view alias = {});

// Quote an SQL identifier with double quotes, doubling any embedded quote.
std::string QuoteSqlIdentifier(std::string_view name);

}  // namespace sdb::catalog
