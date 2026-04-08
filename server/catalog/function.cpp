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

#include "catalog/function.h"

#include <vpack/vpack_helper.h>

#include "basics/static_strings.h"
#include "catalog/identifiers/identifier.h"

namespace sdb::catalog {

namespace {

struct FunctionMeta {
  Identifier id;
  std::string_view name;
};

}  // namespace

Function::Function(std::string_view name,
                   std::unique_ptr<pg::FunctionImpl> impl,
                   ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 {},
                 {},
                 std::string{name},
                 ObjectType::Function},
    _impl{std::move(impl)} {
  SDB_ASSERT(!this->GetName().empty());
  SDB_ASSERT(_impl);
}

Function::~Function() = default;

Result Function::Instantiate(std::shared_ptr<Function>& function,
                             ObjectId database_id, vpack::Slice definition) {
  auto name = basics::VPackHelper::getString(
    definition, StaticStrings::kDataSourceName, {});
  if (name.empty()) {
    return {ERROR_BAD_PARAMETER, "Function name must be non-empty"};
  }

  auto impl_slice = definition.get("implementation");
  std::unique_ptr<pg::FunctionImpl> impl;
  auto r = pg::FunctionImpl::FromVPack(impl_slice, impl);
  if (!r.ok()) {
    return r;
  }

  function = std::make_shared<Function>(name, std::move(impl), database_id);
  return {};
}

void Function::WriteProperties(vpack::Builder& builder) const {
  SDB_ASSERT(builder.isOpenObject());
  vpack::WriteObject(builder, vpack::Embedded{FunctionMeta{
                                .id = Identifier{GetId().id()},
                                .name = GetName(),
                              }});
  builder.add("implementation");
  _impl->ToVPack(builder);
}

void Function::WriteInternal(vpack::Builder& builder) const {
  SDB_ASSERT(builder.isOpenObject());
  vpack::WriteObject(builder, vpack::Embedded{FunctionMeta{
                                .id = Identifier{GetId().id()},
                                .name = GetName(),
                              }});
  builder.add("implementation");
  _impl->ToVPack(builder);
}

}  // namespace sdb::catalog
