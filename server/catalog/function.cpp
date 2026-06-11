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

#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/function/table_macro_function.hpp>
#include <duckdb/parser/query_node.hpp>

#include "basics/serializer.h"
#include "catalog/create_info_serde.h"

namespace sdb::catalog {

PgSqlFunction::PgSqlFunction(ObjectId schema_id, ObjectId id,
                             std::string_view name,
                             duckdb::unique_ptr<duckdb::CreateMacroInfo> info)
  : Object{schema_id, id, std::string{name}, ObjectType::PgSqlFunction},
    _info{std::move(info)} {}

std::shared_ptr<PgSqlFunction> PgSqlFunction::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  CreateInfoReadData<duckdb::CreateMacroInfo> data;
  basics::ReadTuple(src, data);
  return std::make_shared<PgSqlFunction>(ctx.schema_id, ctx.id, data.name,
                                         std::move(data.info.info));
}

void PgSqlFunction::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, CreateInfoWriteData<duckdb::CreateMacroInfo>{
                             GetName(), {_info.get()}});
}

std::shared_ptr<Object> PgSqlFunction::Clone() const {
  auto cloned_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      _info->Copy());
  return std::make_shared<PgSqlFunction>(GetParentId(), GetId(), GetName(),
                                         std::move(cloned_info));
}

Refs PgSqlFunction::GetRefs(RefKinds kinds) const {
  Refs out;
  auto append = [&](Refs body) {
    out.sequences.insert(out.sequences.end(), body.sequences.begin(),
                         body.sequences.end());
    out.relations.insert(out.relations.end(), body.relations.begin(),
                         body.relations.end());
    out.functions.insert(out.functions.end(), body.functions.begin(),
                         body.functions.end());
    out.unbound_types.insert(out.unbound_types.end(),
                             body.unbound_types.begin(),
                             body.unbound_types.end());
    out.types.insert(out.types.end(), body.types.begin(), body.types.end());
  };
  const bool wants_types = RefKinds::None != (kinds & RefKinds::Types);
  for (const auto& macro : _info->macros) {
    if (!macro) {
      continue;
    }
    if (wants_types) {
      for (const auto& t : macro->types) {
        CollectTypeRefs(t, out);
      }
      for (const auto& t : macro->return_types) {
        CollectTypeRefs(t, out);
      }
    }
    if (macro->type == duckdb::MacroType::SCALAR_MACRO) {
      const auto& sm = macro->Cast<duckdb::ScalarMacroFunction>();
      if (sm.expression) {
        append(ExtractRefs(*sm.expression, kinds));
      }
    } else if (macro->type == duckdb::MacroType::TABLE_MACRO) {
      const auto& tm = macro->Cast<duckdb::TableMacroFunction>();
      if (tm.query_node) {
        append(ExtractRefs(*tm.query_node, kinds));
      }
    }
  }
  return out;
}

}  // namespace sdb::catalog
