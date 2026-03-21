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

#include "catalog/native_functions.h"

#include <absl/strings/ascii.h>

#include "basics/containers/flat_hash_set.h"

namespace sdb::native {

struct HashEq {
  using is_transparent = void;

  size_t operator()(const std::unique_ptr<catalog::Function>& function) const {
    return absl::HashOf(function->GetName());
  }

  size_t operator()(std::string_view name) const { return absl::HashOf(name); }

  bool operator()(const std::unique_ptr<catalog::Function>& lhs,
                  const std::unique_ptr<catalog::Function>& rhs) const {
    return lhs->GetName() == rhs->GetName();
  }
  bool operator()(const std::unique_ptr<catalog::Function>& lhs,
                  std::string_view rhs) const {
    return lhs->GetName() == rhs;
  }
};

static containers::FlatHashSet<std::unique_ptr<catalog::Function>, HashEq,
                               HashEq>
  gFunctions;

const catalog::Function* GetFunction(std::string_view name) {
  auto it = gFunctions.find(name);
  if (it == gFunctions.end()) {
    return nullptr;
  }
  SDB_ASSERT(*it);
  return it->get();
}

void VisitFunctions(absl::FunctionRef<void(const catalog::Function&)> visitor) {
  for (const auto& function : gFunctions) {
    SDB_ASSERT(function);
    visitor(*function);
  }
}

void ClearFunctions() { gFunctions.clear(); }

}  // namespace sdb::native
