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

#include "pg/types/aclitem.h"

#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <iresearch/utils/assert.hpp>
#include <memory>
#include <optional>
#include <string>

#include "auth/role_closure.h"
#include "connector/common.h"
#include "pg/catalog/utils/acl.h"
#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

struct AclitemInputState final : InputState {
  explicit AclitemInputState(duckdb::ClientContext* context)
    : roles{auth::RolesOf(context)} {}

  std::shared_ptr<const auth::RoleGraph> roles;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitAclitemInput(
  duckdb::CastLocalStateParameters& parameters) {
  return duckdb::make_uniq<AclitemInputState>(parameters.context.get());
}

bool TextToAclitem(duckdb::Vector& source, duckdb::Vector& result,
                   duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto& roles = parameters.local_state->Cast<AclitemInputState>().roles;
  SDB_ASSERT(roles);
  bool converted = true;
  std::string text;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count,
    [&](duckdb::string_t input) -> std::optional<duckdb::string_t> {
      duckdb::AclItem item{};
      if (auto error =
            ParseAcl(connector::AsView(input), *roles, false, item)) {
        InputError(std::move(*error), parameters);
        converted = false;
        return std::nullopt;
      }
      text.clear();
      AppendAcl(text, item, *roles);
      return duckdb::StringVector::AddString(result, text);
    });
  return converted;
}

}  // namespace

void RegisterAclitem(duckdb::CastFunctionSet& casts) {
  RegisterTextInput(casts, ACLITEM(), TextToAclitem, InitAclitemInput);
}

}  // namespace sdb::pg
