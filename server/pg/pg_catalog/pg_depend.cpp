////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "pg/pg_catalog/pg_depend.h"

#include <vector>

#include "catalog/catalog.h"
#include "catalog/object_dependency.h"
#include "pg/pg_catalog/pg_attrdef.h"
#include "pg/pg_catalog/pg_class.h"
#include "pg/pg_catalog/pg_constraint.h"
#include "pg/pg_catalog/pg_namespace.h"
#include "pg/pg_catalog/pg_proc.h"
#include "pg/pg_catalog/pg_rewrite.h"
#include "pg/pg_catalog/pg_type.h"

namespace sdb::pg {
namespace {

Oid ClassOid(catalog::DependClass c) {
  switch (c) {
    case catalog::DependClass::Relation:
      return Oid{PgClass::kId};
    case catalog::DependClass::Type:
      return Oid{PgType::kId};
    case catalog::DependClass::Proc:
      return Oid{PgProc::kId};
    case catalog::DependClass::Namespace:
      return Oid{PgNamespace::kId};
    case catalog::DependClass::Constraint:
      return Oid{PgConstraint::kId};
    case catalog::DependClass::Rewrite:
      return Oid{PgRewrite::kId};
    case catalog::DependClass::Attrdef:
      return Oid{PgAttrdef::kId};
  }
  return Oid{0};
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgDepend>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();
  const auto edges = catalog->CollectPgDependEdges(GetDatabaseId());

  std::vector<PgDepend> values;
  values.reserve(edges.size());
  for (const auto& e : edges) {
    Oid objid;
    switch (e.dependent_class) {
      case catalog::DependClass::Rewrite:
        objid = ViewRuleOid(e.dependent.id());
        break;
      case catalog::DependClass::Attrdef:
        objid = AttrdefOid(e.dependent.id());
        break;
      default:
        objid = Oid{e.dependent.id()};
        break;
    }
    values.push_back(PgDepend{
      ClassOid(e.dependent_class),
      objid,
      e.dependent_sub,
      ClassOid(e.referenced_class),
      Oid{e.referenced.id()},
      e.referenced_sub,
      static_cast<PgDepend::Deptype>(static_cast<char>(e.deptype)),
    });
  }

  auto result = CreateColumns<PgDepend>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
