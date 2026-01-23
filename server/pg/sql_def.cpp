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

#include "sql_def.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include "basics/system-compiler.h"
#include "nodes/nodes.h"
#include "nodes/value.h"
#include "sql_exception_macro.h"

namespace sdb::pg {

std::string_view GetString(DefElem& n) {
  auto* arg = n.arg;

  if (!arg) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG(n.defname, " requires a parameter value"));
  }

  switch (nodeTag(arg)) {
    case T_Boolean:
      return boolVal(arg) ? "true" : "false";
    case T_String:
      return {strVal(arg)};
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("unrecognized node type: ", nodeTag(arg)));
      break;
  }

  SDB_UNREACHABLE();
}

double GetNumeric(DefElem& n) {
  auto error_invalid_arg = [&] [[noreturn]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG(n.defname, " requires a numeric value"));
  };
  auto* arg = n.arg;
  if (!arg) {
    error_invalid_arg();
  }
  switch (nodeTag(arg)) {
    case T_Integer:
      return intVal(arg);
    case T_Float:
      // TODO(mbkkt) maybe avoid this macro?
      return floatVal(arg);
    default:
      error_invalid_arg();
  }
}

bool GetBool(DefElem& n) {
  auto* arg = n.arg;

  if (!arg) {
    // Postgres behavior
    return true;
  }

  switch (nodeTag(arg)) {
    case T_Integer: {
      switch (intVal(arg)) {
        case 0:
          return false;
        case 1:
          return true;
        default:
          break;
      }
    } break;
    case T_String: {
      const std::string_view str = strVal(arg);

      if (absl::EqualsIgnoreCase(str, "true") ||
          absl::EqualsIgnoreCase(str, "on")) {
        return true;
      }
      if (absl::EqualsIgnoreCase(str, "false") ||
          absl::EqualsIgnoreCase(str, "off")) {
        return false;
      }
    } break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG(n.defname, " requires a Boolean value"));
      break;
  }

  SDB_UNREACHABLE();
}

void ErrorConflictingDefElem(DefElem& n) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_SYNTAX_ERROR),  //
    ERR_MSG("conflicting or redundant options"),
    CURSOR_POS(n.location));  // TODO: use errorPosition from SqlUtils
}

void ErrorUnknownDefElem(DefElem& n) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_SYNTAX_ERROR),
    ERR_MSG("option \"", n.defname, "\" not recognized"),
    CURSOR_POS(n.location));  // TODO: use errorPosition from SqlUtils
}

}  // namespace sdb::pg
