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

#include <velox/type/Type.h>
#include <velox/vector/BaseVector.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/FlatVector.h>

#include "basics/fwd.h"
#include "basics/system-compiler.h"
#include "catalog/object.h"

namespace sdb::catalog {

class VirtualTable;

class VritualTableSnapshot : public SchemaObject {
 public:
  virtual velox::RowTypePtr RowType() const noexcept = 0;

  virtual velox::RowVectorPtr GetData(std::vector<std::string> names,
                                      velox::memory::MemoryPool& pool) = 0;

  const VirtualTable& GetTable() const noexcept {
    SDB_ASSERT(_table);
    return *_table;
  }

 protected:
  using SchemaObject::SchemaObject;

  const VirtualTable* _table = nullptr;
};

// non owning description of catalog table
// kind of C++ namespaces but with virtual functions
class VirtualTable {
 public:
  virtual std::shared_ptr<VritualTableSnapshot> CreateSnapshot(
    ObjectId database) const = 0;

  virtual ~VirtualTable() = default;

  ObjectId Id() const noexcept { return _id; }
  std::string_view Name() const noexcept { return _name; }

  virtual velox::RowTypePtr RowType() const noexcept = 0;

 protected:
  ObjectId _id;
  std::string_view _name;
};

}  // namespace sdb::catalog
