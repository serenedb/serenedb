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

#include <velox/type/Type.h>

#include <string_view>

#include "basics/fwd.h"
#include "basics/static_strings.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/object.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/sql_function_impl.h"
#include "query/types.h"
#include "utils/velox_vpack.h"
#include "vpack/serializer.h"
#include "vpack/vpack_helper.h"

namespace sdb::catalog {

bool FunctionParameter::IsCollection() const { return aql::IsCollection(type); }
void FunctionParameter::MarkAsCollection() { type = aql::COLLECTION(); }

bool FunctionSignature::Matches(
  const std::vector<velox::TypePtr>& arg_types) const {
  return std::ranges::equal(
    arg_types, parameters,
    [](const velox::TypePtr& arg, const FunctionParameter& param) {
      SDB_ASSERT(param.type);
      SDB_ASSERT(arg);
      return *arg == *param.type ||
             (arg == pg::PG_UNKNOWN() && param.type == velox::VARCHAR());
    });
}

bool FunctionSignature::ReturnsTable() const {
  return return_type && return_type->kind() == velox::TypeKind::ROW;
}

bool FunctionSignature::ReturnsVoid() const { return pg::IsVoid(return_type); }

bool FunctionSignature::IsProcedure() const {
  return pg::IsProcedure(return_type);
}
void FunctionSignature::MarkAsProcedure() { return_type = pg::PROCEDURE(); }

namespace {

// NOLINTBEGIN
struct FunctionMeta {
  Identifier id;
  std::string_view name;
};
// NOLINTEND

}  // namespace

Result FunctionProperties::Read(FunctionProperties& properties,
                                vpack::Slice slice, bool is_user_request) {
  if (!slice.isObject()) {
    return {ERROR_BAD_PARAMETER, "Function definition must be an object"};
  }

  properties.name = basics::VPackHelper::getString(
    slice, sdb::StaticStrings::kDataSourceName, {});

  if (properties.name.empty()) {
    return {ERROR_BAD_PARAMETER, "Function name must be a non-empty string"};
  }

  properties.id = Identifier{basics::VPackHelper::extractIdValue(slice)};
  properties.implementation = slice.get("implementation");

  if (auto r = vpack::ReadNothrow(is_user_request, slice.get("signature"),
                                  properties.signature);
      !r.ok()) {
    return r;
  }

  if (auto r = vpack::ReadNothrow(is_user_request, slice.get("options"),
                                  properties.options);
      !r.ok()) {
    return r;
  }

  return {};
}

Result catalog::Function::Instantiate(
  std::shared_ptr<catalog::Function>& function, ObjectId database,
  vpack::Slice definition, bool is_user_request) {
  FunctionProperties properties;
  auto r = FunctionProperties::Read(properties, definition, is_user_request);
  if (!r.ok()) {
    return r;
  }

  if (is_user_request) {
    properties.id = {};
  }

  auto from_vpack = [&]<typename T> {
    std::unique_ptr<T> impl;

    Result r;
    if constexpr (std::is_same_v<T, pg::FunctionImpl>) {
      r = T::FromVPack(ObjectId{database}, properties.implementation, impl,
                       properties.signature.IsProcedure());
    } else {
#ifdef SDB_CLUSTER
      r = T::FromVPack(ObjectId{database}, properties.implementation, impl);
#endif
    }

    if (!r.ok()) {
      return r;
    }
    function = std::make_shared<catalog::Function>(std::move(properties),
                                                   std::move(impl), database);
    return Result{};
  };

  switch (properties.options.language) {
    case FunctionLanguage::SQL:
      return from_vpack.operator()<pg::FunctionImpl>();
    case FunctionLanguage::AnalyzerJson:
      return from_vpack.operator()<search::AnalyzerImpl>();
    default:
      return {ERROR_BAD_PARAMETER,
              "Unsupported function language: ", properties.options.language};
  }
}

catalog::Function::Function(std::string_view name, FunctionSignature signature,
                            FunctionOptions options, aql::FunctionImpl impl)
  : SchemaObject{{},
                 {},
                 {},
                 {},  // TOOD(mbkkt) think about id
                 std::string{name},
                 ObjectType::Function},
    _signature{std::move(signature)},
    _options{std::move(options)},
    _aql_impl{std::move(impl)} {
  SDB_ASSERT(!this->GetName().empty());
  SDB_ASSERT(_options.language == FunctionLanguage::AqlNative);
  SDB_ASSERT(_aql_impl);
}

catalog::Function::Function(std::string_view name, FunctionSignature signature,
                            FunctionOptions options)
  : SchemaObject{{},
                 {},
                 {},
                 {},  // TOOD(mbkkt) think about id
                 std::string{name},
                 ObjectType::Function},
    _signature{std::move(signature)},
    _options{std::move(options)} {
  SDB_ASSERT(!this->GetName().empty());
  SDB_ASSERT(_options.language == FunctionLanguage::VeloxNative ||
             _options.language == FunctionLanguage::Decorator);
}

catalog::Function::Function(FunctionProperties&& properties,
                            std::unique_ptr<search::AnalyzerImpl> analyzer,
                            ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 {},
                 properties.id,
                 std::move(properties.name),
                 ObjectType::Function},
    _signature{std::move(properties.signature)},
    _options{std::move(properties.options)},
    _analyzer_impl{std::move(analyzer)} {
  SDB_ASSERT(!this->GetName().empty());
  SDB_ASSERT(_options.language == FunctionLanguage::AnalyzerJson);
  SDB_ASSERT(_analyzer_impl);
}

catalog::Function::Function(FunctionProperties&& properties,
                            std::unique_ptr<pg::FunctionImpl> function,
                            ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 {},
                 properties.id,
                 std::move(properties.name),
                 ObjectType::Function},
    _signature{std::move(properties.signature)},
    _options{std::move(properties.options)},
    _sql_impl{std::move(function)} {
  SDB_ASSERT(!this->GetName().empty());
  SDB_ASSERT(_options.language == FunctionLanguage::SQL);
  SDB_ASSERT(_sql_impl);
}

catalog::Function::~Function() = default;

void catalog::Function::WriteProperties(vpack::Builder& builder) const {
  SDB_ASSERT(builder.isOpenObject());
  vpack::WriteObject(builder, vpack::Embedded{FunctionMeta{
                                .id = Identifier{GetId().id()},
                                .name = GetName(),
                              }});
  builder.add("signature");
  vpack::WriteObject(builder, _signature);
  builder.add("options");
  vpack::WriteObject(builder, _options);
  builder.add("implementation");
  switch (_options.language) {
    case FunctionLanguage::SQL:
      _sql_impl->ToVPack(builder);
      break;
#ifdef SDB_CLUSTER
    case FunctionLanguage::AnalyzerJson:
      _analyzer_impl->ToVPack(builder);
      break;
#endif
    default:
      SDB_ENSURE(false, ERROR_BAD_PARAMETER,
                 "Unsupported function language: ", _options.language);
  }
}

void catalog::Function::WriteInternal(vpack::Builder& builder) const {
  SDB_ASSERT(builder.isOpenObject());
  vpack::WriteObject(builder, vpack::Embedded{FunctionMeta{
                                .id = Identifier{GetId().id()},
                                .name = GetName(),
                              }});
  builder.add("signature");
  vpack::WriteTuple(builder, _signature);
  builder.add("options");
  vpack::WriteTuple(builder, _options);
  builder.add("implementation");
  switch (_options.language) {
    case FunctionLanguage::SQL:
      _sql_impl->ToVPack(builder);
      break;
#ifdef SDB_CLUSTER
    case FunctionLanguage::AnalyzerJson:
      _analyzer_impl->ToVPack(builder);
      break;
#endif
    default:
      SDB_ENSURE(false, ERROR_BAD_PARAMETER,
                 "Unsupported function language: ", _options.language);
  }
}

}  // namespace sdb::catalog
