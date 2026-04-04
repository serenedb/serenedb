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
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <memory>
#include <string>
#include <vector>

#include "basics/fwd.h"
#include "catalog/object.h"

namespace sdb::pg {

class PgCompositeType;
}

namespace sdb::catalog {

class CompositeType : public SchemaObject {
 public:
  CompositeType(std::string_view name, velox::RowTypePtr row_type);

  const std::shared_ptr<const pg::PgCompositeType>& GetPgType() const noexcept {
    return _pg_type;
  }

  void WriteInternal(vpack::Builder& b) const final;

  static Result Instantiate(std::shared_ptr<CompositeType>& result, ObjectId id,
                            vpack::Slice slice);

  CompositeType(ObjectId id, std::string_view name, velox::RowTypePtr row_type);

 private:
  std::shared_ptr<const pg::PgCompositeType> _pg_type;
};

}  // namespace sdb::catalog
