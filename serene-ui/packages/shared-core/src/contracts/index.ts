import { connectionContracts } from "./connection.contracts";
import { githubContracts } from "./github.contracts";
import { queryExecutionContracts } from "./query-execution.contracts";
import { savedQueryContracts } from "./saved-queries.contracts";

export const apiContracts = {
    connection: connectionContracts,
    github: githubContracts,
    queryExecution: queryExecutionContracts,
    savedQuery: savedQueryContracts,
};
