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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>

#include "catalog/object.h"

namespace sdb::catalog {

// NOLINTBEGIN
struct DatabaseOptions {
  std::string name;  // TODO(gnusi): change to std::string_view
  ObjectId owner_id;
  uint32_t replicationFactor = 1;  // 0 for redundant
  uint32_t writeConcern = 1;
};
// NOLINTEND

DatabaseOptions MakeDatabaseOptions(std::string_view name = {},
                                    ObjectId id = {});
DatabaseOptions MakeSystemDatabaseOptions();

class Database final : public Object {
 public:
  explicit Database(ObjectId id, DatabaseOptions options)
    : Object{options.owner_id, id, std::move(options.name),
             ObjectType::Database},
      _replication_factor{options.replicationFactor},
      _write_concern{options.writeConcern} {}

  void WriteInternal(vpack::Builder& build) const final;
  void WriteProperties(vpack::Builder& build) const;

  auto GetReplicationFactor() const noexcept { return _replication_factor; }
  auto GetWriteConcern() const noexcept { return _write_concern; }

 private:
  uint32_t _replication_factor;
  uint32_t _write_concern;
};

}  // namespace sdb::catalog
