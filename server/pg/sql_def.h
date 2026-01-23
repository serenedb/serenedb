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

#pragma once

#include "basics/containers/flat_hash_set.h"
#include "sql_exception.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

struct DefElem;

namespace sdb::pg {

class ExpressionContext;

[[noreturn]] void ErrorConflictingDefElem(DefElem& n);
[[noreturn]] void ErrorUnknownDefElem(DefElem& n);

std::string_view GetString(DefElem& n);
double GetNumeric(DefElem& n);
bool GetBool(DefElem& n);

template<typename T>
using Setter = void (*)(DefElem& node, T& obj);

template<typename T>
struct GetSetters;

template<typename T>
void ProcessOptions(List* options, T& value) {
  if (!options) {
    return;
  }

  using Setters = GetSetters<T>;

  containers::FlatHashSet<const char*> seen;
  seen.reserve(list_length(options));

  ListCell* lc;
  for_each_from(lc, options, 0) {
    auto option = castNode(DefElem, lfirst(lc));
    const auto name = option->defname;

    const auto setter = Setters::kMapping.find(name);

    if (setter == Setters::kMapping.end()) {
      ErrorUnknownDefElem(*option);
    }

    auto [_, isNew] = seen.emplace(setter->first.data());

    if (!isNew) {
      ErrorConflictingDefElem(*option);
    }

    auto func = setter->second;
    (*func)(*option, value);
  }
}

}  // namespace sdb::pg
