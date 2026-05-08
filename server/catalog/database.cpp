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

namespace sdb::catalog {

Database::Database(ObjectId id, std::string_view name)
  : Object{{}, id, std::string{name}, ObjectType::Database} {}

std::shared_ptr<Database> Database::ReadInternal(vpack::Slice slice,
                                                 ReadContext ctx) {
  auto name_slice = slice.get("name");
  if (!name_slice.isString()) {
    return nullptr;
  }
  return std::make_shared<Database>(ctx.id, name_slice.stringView());
}

void Database::WriteInternal(vpack::Builder& b) const {
  b.openObject();
  WriteObject(b, [](vpack::Builder&) {});
  b.close();
}

std::shared_ptr<Object> Database::Clone() const {
  return std::make_shared<Database>(GetId(), GetName());
}

}  // namespace sdb::catalog
