#include "role_utils.h"

#include "catalog/catalog.h"

namespace sdb::auth {

std::vector<std::shared_ptr<catalog::Role>> GetRoles() {
  return catalog::CatalogFeature::instance()
    .Global()
    .GetCatalogSnapshot()
    ->GetRoles();
}

std::shared_ptr<catalog::Role> GetRole(std::string_view name) {
  return catalog::CatalogFeature::instance()
    .Global()
    .GetCatalogSnapshot()
    ->GetRole(name);
}

}  // namespace sdb::auth
