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
#include <string>

#include "catalog/object.h"

namespace sdb::pg {
class FunctionImpl;
}

namespace sdb::catalog {

// A SQL function stored in the catalog.
// The function body is stored as SQL text in FunctionImpl.
// DuckDB parses it on demand to create macro entries.
class Function final : public SchemaObject {
 public:
  Function(std::string_view name, std::unique_ptr<pg::FunctionImpl> impl,
           ObjectId database_id);

  ~Function() final;

  void WriteProperties(vpack::Builder& build) const final;
  void WriteInternal(vpack::Builder& build) const final;

  pg::FunctionImpl& GetImpl() const noexcept { return *_impl; }

  // Create from VPack (RocksDB load)
  static Result Instantiate(std::shared_ptr<Function>& function,
                            ObjectId database_id, vpack::Slice definition);

 private:
  std::unique_ptr<pg::FunctionImpl> _impl;
};

}  // namespace sdb::catalog
