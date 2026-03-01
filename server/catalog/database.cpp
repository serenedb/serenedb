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

#include "catalog/database.h"

#include "basics/assert.h"
#include "basics/static_strings.h"
#include "catalog/table_options.h"
#include "general_server/server_options_feature.h"
#include "vpack/serializer.h"

namespace sdb::catalog {

DatabaseOptions MakeDatabaseOptions(std::string_view name, ObjectId id) {
  const auto& server_options = GetServerOptions();
  return {
    .name = std::string{name},
    .replicationFactor = server_options.cluster_default_replication_factor,
    .writeConcern = server_options.cluster_write_concern,
  };
}

DatabaseOptions MakeSystemDatabaseOptions() {
  return MakeDatabaseOptions(StaticStrings::kSystemDatabase, id::kSystemDB);
}

void Database::WriteInternal(vpack::Builder& b) const {
  const DatabaseOptions options{
    .name = _name,
    .replicationFactor = _replication_factor,
    .writeConcern = _write_concern,
  };
  vpack::WriteTuple(b, options);
}

void Database::WriteProperties(vpack::Builder& b) const {
  SDB_ASSERT(b.isOpenObject());
  struct DatabaseProperties {
    // NOLINTBEGIN
    std::string_view name;
    std::string id;
    uint32_t replicationFactor = 1;
    uint32_t writeConcern = 1;
    // NOLINTEND
  };
  vpack::WriteObject(b,
                     vpack::Embedded{DatabaseProperties{
                       .name = GetName(),
                       .id = absl::StrCat(GetId()),
                       .replicationFactor = GetReplicationFactor(),
                       .writeConcern = GetWriteConcern(),
                     }},
                     ObjectProperties{});
}

}  // namespace sdb::catalog
