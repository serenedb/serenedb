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

#include <string>

#include "catalog/object.h"

namespace sdb::catalog {

enum class IndexType : uint8_t {
  Invalid = 0,
  Secondary,
  Inverted,
};

struct IndexOptions {
  ObjectId id;
  ObjectId table;
  std::string name;
  IndexType type = IndexType::Invalid;
  vpack::Slice options;
};

class Index : public SchemaObject {
 public:
  Index(IndexOptions options, ObjectId database_id);

  auto GetIndexType() const noexcept { return _type; }
  auto GetTableId() const noexcept { return _table_id; }

 private:
  ObjectId _table_id;
  IndexType _type;
};

}  // namespace sdb::catalog
