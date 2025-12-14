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

#include "sql_parser.h"

#include "basics/assert.h"
#include "basics/static_strings.h"
#include "catalog/identifiers/object_id.h"
#include "pg/connection_context.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_statement.h"
#include "pg/sql_utils.h"
#include "utils/query_string.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/memnodes.h"
#include "parser/parser.h"
#include "pg_query.h"
#include "src/pg_query_internal.h"
#include "utils/palloc.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {

struct SqlParseResult {
  List* tree{};
  ErrorData* error{};
};

SqlParseResult SqlParse(const char* input, int parser_options) {
  SqlParseResult result{};
  MemoryContext parse_context = CurrentMemoryContext;

  PG_TRY();
  {
    RawParseMode raw_parse_mode = RAW_PARSE_DEFAULT;
    switch (parser_options & PG_QUERY_PARSE_MODE_BITMASK) {
      case PG_QUERY_PARSE_TYPE_NAME:
        raw_parse_mode = RAW_PARSE_TYPE_NAME;
        break;
      case PG_QUERY_PARSE_PLPGSQL_EXPR:
        raw_parse_mode = RAW_PARSE_PLPGSQL_EXPR;
        break;
      case PG_QUERY_PARSE_PLPGSQL_ASSIGN1:
        raw_parse_mode = RAW_PARSE_PLPGSQL_ASSIGN1;
        break;
      case PG_QUERY_PARSE_PLPGSQL_ASSIGN2:
        raw_parse_mode = RAW_PARSE_PLPGSQL_ASSIGN2;
        break;
      case PG_QUERY_PARSE_PLPGSQL_ASSIGN3:
        raw_parse_mode = RAW_PARSE_PLPGSQL_ASSIGN3;
        break;
    }

    if ((parser_options & PG_QUERY_DISABLE_BACKSLASH_QUOTE) ==
        PG_QUERY_DISABLE_BACKSLASH_QUOTE) {
      backslash_quote = BACKSLASH_QUOTE_OFF;
    } else {
      backslash_quote = BACKSLASH_QUOTE_SAFE_ENCODING;
    }
    standard_conforming_strings =
      !((parser_options & PG_QUERY_DISABLE_STANDARD_CONFORMING_STRINGS) ==
        PG_QUERY_DISABLE_STANDARD_CONFORMING_STRINGS);
    escape_string_warning =
      !((parser_options & PG_QUERY_DISABLE_ESCAPE_STRING_WARNING) ==
        PG_QUERY_DISABLE_ESCAPE_STRING_WARNING);

    result.tree = raw_parser(input, raw_parse_mode);

    backslash_quote = BACKSLASH_QUOTE_SAFE_ENCODING;
    standard_conforming_strings = true;
    escape_string_warning = true;
  }
  PG_CATCH();
  {
    MemoryContextSwitchTo(parse_context);
    result.error = CopyErrorData();

    FlushErrorState();
  }
  PG_END_TRY();

  return result;
}

}  // namespace

List* Parse(MemoryContextData& ctx, const QueryString& query_string) {
  if (query_string.empty()) {
    return {};  // Empty root case to be handled by the protocol
  }

  auto scope = EnterMemoryContext(ctx);

  // TODO: warnings?
  auto [result, err] = SqlParse(query_string.data(), PG_QUERY_PARSE_DEFAULT);

  // TODO(gnusi): track memory in memory context

  if (err) {
    // TODO copy all necessary fields from ErrorData
    THROW_SQL_ERROR(ERR_CODE(err->sqlerrcode), CURSOR_POS(err->cursorpos),
                    ERR_MSG(err->message ? err->message : ""),
                    ERR_DETAIL(err->detail ? err->detail : ""),
                    ERR_HINT(err->hint ? err->hint : ""));
  }

  return result;
}

List* ParseExpressions(MemoryContextData& ctx,
                       const QueryString& query_string) {
  if (query_string.empty()) {
    return {};
  }

  auto scope = EnterMemoryContext(ctx);

  auto [result, err] =
    SqlParse(query_string.data(), PG_QUERY_PARSE_PLPGSQL_EXPR);

  if (err) {
    THROW_SQL_ERROR(ERR_CODE(err->sqlerrcode), CURSOR_POS(err->cursorpos),
                    ERR_MSG(err->message ? err->message : ""),
                    ERR_DETAIL(err->detail ? err->detail : ""),
                    ERR_HINT(err->hint ? err->hint : ""));
  }

  return result;
}

Node* ParseSingleExpression(MemoryContextData& ctx,
                            const QueryString& query_string) {
  auto list = ParseExpressions(ctx, query_string);
  SDB_ASSERT(list_length(list) == 1);
  auto* raw_stmt = list_nth_node(RawStmt, list, 0);
  SDB_ASSERT(IsA(raw_stmt->stmt, SelectStmt));
  auto* select = castNode(SelectStmt, raw_stmt->stmt);
  SDB_ASSERT(list_length(select->targetList) == 1);
  auto* expr = list_nth_node(ResTarget, select->targetList, 0)->val;
  return expr;
}

pg::SqlStatement ParseSystemView(std::string_view query) {
  static auto gConnCtx = std::make_shared<ConnectionContext>(
    ExecContext::superuser().user(), StaticStrings::kSystemDatabase,
    id::kSystemDB);

  pg::SqlStatement res;
  res.query_string = std::make_shared<QueryString>(query);
  // Note use resetMemoryContext for re-binding!
  res.memory_context = pg::CreateMemoryContext();
  res.tree = {
    .list = pg::Parse(*res.memory_context, *res.query_string),
    .root_idx = 0,
  };
  res.NextRoot(gConnCtx);

  return res;
}

}  // namespace sdb::pg
