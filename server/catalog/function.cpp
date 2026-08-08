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

namespace sdb::catalog {

Refs MacroRefs(const duckdb::CreateMacroInfo& info, RefKinds kinds) {
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
  for (const auto& macro : info.macros) {
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
