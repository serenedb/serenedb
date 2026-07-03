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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>

#include "basics/system-compiler.h"
#include "catalog/object.h"
#include "query/config.h"

namespace sdb::catalog {

class VirtualTable;

struct MaterializedData {
  std::vector<duckdb::Vector> columns;
  duckdb::idx_t row_count = 0;
};

class VirtualTableSnapshot : public Object {
 public:
  std::shared_ptr<Object> Clone() const final { return nullptr; }
  void Serialize(duckdb::Serializer&) const override {}
  virtual duckdb::LogicalType RowType() const noexcept = 0;

  // Returns a reference to lazily materialized data.
  // The data is owned by the snapshot and lives as long as the snapshot does.
  virtual const MaterializedData& GetData(std::vector<std::string> names) = 0;

  const VirtualTable& GetTable() const noexcept {
    SDB_ASSERT(_table);
    return *_table;
  }

 protected:
  using Object::Object;

  const VirtualTable* _table = nullptr;
};

// non owning description of catalog table
// kind of C++ namespaces but with virtual functions
class VirtualTable {
 public:
  virtual std::shared_ptr<VirtualTableSnapshot> CreateSnapshot(
    ObjectId database, const Config& config) const = 0;

  virtual ~VirtualTable() = default;

  ObjectId Id() const noexcept { return _id; }
  std::string_view GetName() const noexcept { return _name; }

  virtual duckdb::LogicalType RowType() const noexcept = 0;

  AclView GetAcl() const noexcept { return _acl; }

 protected:
  ObjectId _id;
  std::string_view _name;
  AclView _acl{&kSystemPublicSelect, 1};
};

}  // namespace sdb::catalog
