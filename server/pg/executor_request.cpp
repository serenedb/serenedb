#include "executor_request.h"

#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

CreateFunctionRequest::CreateFunctionRequest(const Node& node)
  : ExecutorRequest{Type::CreateFunction, node} {
  SDB_ASSERT(node.type == NodeTag::T_CreateFunctionStmt);
}

const CreateFunctionStmt& CreateFunctionRequest::GetStmt() {
  return *castNode(CreateFunctionStmt, &node);
}

}  // namespace sdb::pg
