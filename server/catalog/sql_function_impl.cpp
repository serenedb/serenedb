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

#include "catalog/sql_function_impl.h"

#include <vpack/vpack_helper.h>

namespace sdb::pg {

Result FunctionImpl::FromVPack(vpack::Slice slice,
                               std::unique_ptr<FunctionImpl>& out) {
  auto sql = basics::VPackHelper::getString(slice, "sql", {});
  if (sql.empty()) {
    return {ERROR_BAD_PARAMETER, "function must contain sql field"};
  }
  out = std::make_unique<FunctionImpl>(std::string{sql});
  return {};
}

void FunctionImpl::ToVPack(vpack::Builder& builder) const {
  builder.openObject();
  builder.add("sql", _sql);
  builder.close();
}

}  // namespace sdb::pg
