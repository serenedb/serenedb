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

#pragma once

#include <absl/functional/function_ref.h>

#include <memory>
#include <string_view>

#include "catalog/opclass.h"

namespace sdb::pg {

// Populate the in-memory system opclass map. Mirrors InitSystemViews: the
// entries live for the process lifetime, are not persisted, and shadow no
// user objects. Safe to call once at startup before any connection is
// accepted.
void InitSystemOpClasses();

// Lookup a system opclass by (schema, name). Returns nullptr if absent.
std::shared_ptr<catalog::OpClass> GetSystemOpClass(std::string_view schema,
                                                   std::string_view name);

// Visit every system opclass in `schema`. The visitor must not retain a
// reference past the call.
void VisitSystemOpClasses(
  std::string_view schema,
  absl::FunctionRef<void(const catalog::OpClass&)> visitor);

}  // namespace sdb::pg
