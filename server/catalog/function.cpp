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

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/function/scalar_macro_function.hpp>

#include "basics/static_strings.h"
#include "catalog/user_type.h"

namespace sdb::catalog {

PgSqlFunction::PgSqlFunction(ObjectId schema_id, ObjectId id,
                             std::string_view name,
                             duckdb::unique_ptr<duckdb::CreateMacroInfo> info)
  : Object{schema_id, id, std::string{name}, ObjectType::PgSqlFunction},
    _info{std::move(info)} {}

std::shared_ptr<PgSqlFunction> PgSqlFunction::ReadInternal(vpack::Slice slice,
                                                           ReadContext ctx) {
  auto name =
    basics::VPackHelper::getString(slice, StaticStrings::kDataSourceName, {});

  auto info_slice = slice.get("info");
  SDB_ASSERT(info_slice.isString());
  auto str = info_slice.stringViewUnchecked();
  duckdb::MemoryStream stream(
    const_cast<duckdb::data_t*>(
      reinterpret_cast<const duckdb::data_t*>(str.data())),
    str.size());
  duckdb::BinaryDeserializer deserializer(stream);
  auto create_info = duckdb::CreateInfo::Deserialize(deserializer);
  auto macro_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      std::move(create_info));
  return std::make_shared<PgSqlFunction>(ctx.schema_id, ctx.id, name,
                                         std::move(macro_info));
}

void PgSqlFunction::WriteInternal(vpack::Builder& builder) const {
  builder.openObject();
  builder.add(StaticStrings::kDataSourceName, GetName());

  // Serialize CreateMacroInfo via DuckDB BinarySerializer
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(*_info, stream);
  auto data = stream.GetData();
  auto size = stream.GetPosition();
  builder.add("info",
              std::string_view{reinterpret_cast<const char*>(data), size});

  builder.close();
}

std::shared_ptr<Object> PgSqlFunction::Clone() const {
  auto cloned_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      _info->Copy());
  return std::make_shared<PgSqlFunction>(GetParentId(), GetId(), GetName(),
                                         std::move(cloned_info));
}

Refs PgSqlFunction::ExtractRefs(RefKinds kinds) const {
  Refs out;
  // Param/return types are bound at CREATE time; body CASTs stay unbound and
  // are handled by WalkExpr below.
  auto add_type = [&](const duckdb::LogicalType& t) {
    if (RefKinds::None == (kinds & RefKinds::Types)) {
      return;
    }
    if (auto ext = t.GetExtensionInfo()) {
      if (auto it = ext->properties.find(kPgSqlTypeOidProp);
          it != ext->properties.end()) {
        out.bound_types.push_back(ObjectId{it->second.GetValue<uint64_t>()});
      }
    }
  };
  for (const auto& macro : _info->macros) {
    if (!macro) {
      continue;
    }
    for (const auto& t : macro->types) {
      add_type(t);
    }
    for (const auto& t : macro->return_types) {
      add_type(t);
    }
    if (macro->type != duckdb::MacroType::SCALAR_MACRO) {
      continue;
    }
    const auto& sm = macro->Cast<duckdb::ScalarMacroFunction>();
    if (sm.expression) {
      auto body = ::sdb::ExtractRefs(*sm.expression, kinds);
      out.sequences.insert(out.sequences.end(), body.sequences.begin(),
                           body.sequences.end());
      out.relations.insert(out.relations.end(), body.relations.begin(),
                           body.relations.end());
      out.functions.insert(out.functions.end(), body.functions.begin(),
                           body.functions.end());
      out.unbound_types.insert(out.unbound_types.end(),
                               body.unbound_types.begin(),
                               body.unbound_types.end());
      out.bound_types.insert(out.bound_types.end(), body.bound_types.begin(),
                             body.bound_types.end());
    }
  }
  return out;
}

}  // namespace sdb::catalog
