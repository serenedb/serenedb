import { parse } from "libpg-query";

type SchemaMetadataInvalidationTargets = {
    connection: boolean;
    database: boolean;
};

interface ParseResult {
    stmts?: Array<{
        stmt?: Record<string, unknown>;
    }>;
}

const CONNECTION_LEVEL_STATEMENTS = new Set([
    "CreatedbStmt",
    "DropdbStmt",
    "AlterDatabaseStmt",
    "AlterDatabaseSetStmt",
]);

const DATABASE_LEVEL_STATEMENTS = new Set([
    "AlterCollationStmt",
    "AlterDefaultPrivilegesStmt",
    "AlterDomainStmt",
    "AlterEnumStmt",
    "AlterExtensionContentsStmt",
    "AlterExtensionStmt",
    "AlterObjectDependsStmt",
    "AlterObjectSchemaStmt",
    "AlterOwnerStmt",
    "AlterPublicationStmt",
    "AlterSeqStmt",
    "AlterSubscriptionStmt",
    "AlterTableStmt",
    "AlterTSConfigurationStmt",
    "AlterTSDictionaryStmt",
    "CommentStmt",
    "CompositeTypeStmt",
    "CreateAmStmt",
    "CreateCastStmt",
    "CreateConversionStmt",
    "CreateDomainStmt",
    "CreateEnumStmt",
    "CreateExtensionStmt",
    "CreateForeignServerStmt",
    "CreateForeignTableStmt",
    "CreateFunctionStmt",
    "CreateOpClassStmt",
    "CreateOpFamilyStmt",
    "CreatePLangStmt",
    "CreatePublicationStmt",
    "CreateRangeStmt",
    "CreateSchemaStmt",
    "CreateSeqStmt",
    "CreateStmt",
    "CreateSubscriptionStmt",
    "CreateTableAsStmt",
    "CreateTransformStmt",
    "CreateTrigStmt",
    "CreateUserMappingStmt",
    "CreatedbStmt",
    "DefineStmt",
    "DropOwnedStmt",
    "DropStmt",
    "GrantRoleStmt",
    "GrantStmt",
    "IndexStmt",
    "RefreshMatViewStmt",
    "ReindexStmt",
    "RenameStmt",
    "RuleStmt",
    "SecLabelStmt",
    "TruncateStmt",
    "ViewStmt",
]);

const getStatementType = (statement?: Record<string, unknown>) =>
    statement ? Object.keys(statement)[0] : undefined;

export const getSchemaMetadataInvalidationTargets = async (
    query: string | undefined,
): Promise<SchemaMetadataInvalidationTargets | null> => {
    if (!query?.trim()) {
        return null;
    }

    try {
        const parsed = (await parse(query)) as ParseResult;
        const statementTypes = (parsed.stmts || [])
            .map((statement) => getStatementType(statement.stmt))
            .filter((statementType): statementType is string =>
                Boolean(statementType),
            );

        if (!statementTypes.length) {
            return null;
        }

        const targets: SchemaMetadataInvalidationTargets = {
            connection: false,
            database: false,
        };

        statementTypes.forEach((statementType) => {
            if (CONNECTION_LEVEL_STATEMENTS.has(statementType)) {
                targets.connection = true;
                return;
            }

            if (DATABASE_LEVEL_STATEMENTS.has(statementType)) {
                targets.database = true;
            }
        });

        return targets.connection || targets.database ? targets : null;
    } catch {
        return null;
    }
};
