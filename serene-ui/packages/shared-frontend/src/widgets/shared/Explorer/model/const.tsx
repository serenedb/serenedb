import {
    ConnectionIcon,
    DatabaseIcon,
    TreeColumnIcon,
    TreeColumnsIcon,
    TreeIndexesIcon,
    TreeIndexIcon,
    TreePublisherIcon,
    TreeSchemaIcon,
    TreeSchemasIcon,
    TreeSubscriberIcon,
    TreeTableIcon,
    TreeTablesIcon,
    TreeViewIcon,
    TreeViewsIcon,
} from "../../../../shared";

import type { ExplorerNodeData } from "./types";

export const nodeTemplates = {
    connection: {
        icon: <ConnectionIcon />,
        getChildrenQuery: () => ({
            query: "SELECT datname as name FROM pg_database WHERE datistemplate = false ORDER BY datname",
            database: "",
        }),
        nodeType: "query",
        formatToNodes: (
            databases: { name: string }[],
            node: ExplorerNodeData,
        ) => {
            return databases.map((db, index) => ({
                id: node.id + "/" + "d-" + index,
                name: db.name,
                type: "database",
                parentId: node.id,
                context: {
                    connectionId: node.context?.connectionId,
                    database: db.name,
                },
            }));
        },
        getNodes: () => [],
    },
    database: {
        icon: <DatabaseIcon className="size-3.5 mr-0.5" />,
        getChildrenQuery: {
            query: "",
            database: "",
        },
        nodeType: "static",
        formatToNodes: () => [],
        getNodes: (node: ExplorerNodeData) => {
            return [
                {
                    name: "Catalogs",
                    type: "catalogs",
                },
                {
                    name: "Publications",
                    type: "publications",
                },
                {
                    name: "Schemas",
                    type: "schemas",
                },
                {
                    name: "Subscriptions",
                    type: "subscriptions",
                },
            ].map((db, index) => ({
                id: node.id + "/" + "d-" + index,
                name: db.name,
                type: db.type,
                parentId: node.id,
                context: {
                    ...node.context,
                },
            }));
        },
    },
    schemas: {
        icon: <TreeSchemaIcon />,
        getChildrenQuery: (node: ExplorerNodeData) => ({
            query: `SELECT
    n.oid AS id,
    s.schema_name as name
FROM information_schema.schemata s
JOIN pg_namespace n
    ON n.nspname = s.schema_name
WHERE s.schema_name NOT IN ('information_schema')
  AND s.schema_name NOT LIKE 'pg_%'
  AND has_schema_privilege(current_user, s.schema_name, 'USAGE');`,
            database: node.context?.database || "",
        }),
        nodeType: "query",
        formatToNodes: (
            schemas: { id: number; name: string }[],
            node: ExplorerNodeData,
        ) => {
            return schemas.map((schema, index) => ({
                id: node.id + "/" + "s-" + index,
                name: schema.name,
                type: "schema",
                parentId: node.id,
                context: {
                    connectionId: node.context?.connectionId,
                    database: node.context?.database,
                    schemaId: schema.id,
                },
            }));
        },
        getNodes: () => [],
    },
    schema: {
        icon: <TreeSchemaIcon />,
        getChildrenQuery: () => {},
        nodeType: "static",
        formatToNodes: () => [],
        getNodes: (node: ExplorerNodeData) => {
            return [
                {
                    name: "Tables",
                    type: "tables",
                },
                {
                    name: "Views",
                    type: "views",
                },
            ].map((db, index) => ({
                id: node.id + "/" + "s-" + index,
                name: db.name,
                type: db.type,
                parentId: node.id,
                context: {
                    ...node.context,
                },
            }));
        },
    },
    tables: {
        icon: <TreeTablesIcon />,
        getChildrenQuery: (node: ExplorerNodeData) => ({
            query: `
SELECT
    c.oid       AS id,
    c.relname   AS name
FROM pg_class c
JOIN pg_namespace n
    ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND has_schema_privilege(current_user, n.nspname, 'USAGE')
  AND has_table_privilege(current_user, c.oid, 'SELECT')
  AND n.oid = ${node.context?.schemaId || node.context?.catalogId}
`,
            database: node.context?.database || "",
        }),
        nodeType: "query",
        formatToNodes: (
            tables: { id: number; name: string }[],
            node: ExplorerNodeData,
        ) => {
            return tables.map((table, index) => ({
                id: node.id + "/" + "t-" + index,
                name: table.name,
                type: "table",
                parentId: node.id,
                context: {
                    connectionId: node.context?.connectionId,
                    database: node.context?.database,
                    schemaId: node.context?.schemaId,
                    catalogId: node.context?.catalogId,
                    tableId: table.id,
                },
            }));
        },
        getNodes: () => [],
    },
    table: {
        icon: <TreeTableIcon />,
        getChildrenQuery: () => {},
        nodeType: "static",
        formatToNodes: () => [],
        getNodes: (node: ExplorerNodeData) => {
            return [
                {
                    name: "Columns",
                    type: "columns",
                },
                {
                    name: "Indexes",
                    type: "indexes",
                },
            ].map((db, index) => ({
                id: node.id + "/" + "t-" + index,
                name: db.name,
                type: db.type,
                parentId: node.id,
                context: {
                    ...node.context,
                },
            }));
        },
    },
    columns: {
        icon: <TreeColumnsIcon />,
        getChildrenQuery: (node: ExplorerNodeData) => ({
            query: `SELECT
    c.column_name AS name,
    COALESCE(
        NULLIF(c.udt_name || '(' || c.character_maximum_length::varchar || ')',
               c.udt_name || '(NULL)'),
        NULLIF(c.udt_name || '(' || c.numeric_precision::varchar || ',' || c.numeric_scale::varchar || ')',
               c.udt_name || '(,)'),
        NULLIF(c.udt_name || '(' || c.numeric_precision::varchar || ')',
               c.udt_name || '(NULL)'),
        NULLIF(c.udt_name || '(' || c.datetime_precision::varchar || ')',
               c.udt_name || '(NULL)'),
        c.udt_name
    ) AS data_type,
    CASE WHEN c.data_type = 'ARRAY' THEN 'YES' ELSE 'NO' END AS is_array,
    pg_catalog.col_description(cls.oid, c.ordinal_position) AS column_comment
FROM pg_class cls
JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
JOIN information_schema.columns c
      ON c.table_schema = nsp.nspname
     AND c.table_name = cls.relname
WHERE cls.oid = ${node.context?.tableId || node.context?.viewId}
ORDER BY c.ordinal_position;
`,
            database: node.context?.database || "",
        }),
        nodeType: "query",
        formatToNodes: (
            tables: { name: string; data_type: string; is_array: string }[],
            node: ExplorerNodeData,
        ) => {
            return tables.map((table, index) => ({
                id: node.id + "/" + "t-" + index,
                name: table.name,
                type: "column",
                parentId: node.id,
                context: {
                    ...node.context,
                    column_data_type: table.data_type,
                    column_is_array: table.is_array,
                },
            }));
        },
        getNodes: () => [],
    },
    column: {
        icon: <TreeColumnIcon />,
        getChildrenQuery: () => {},
        nodeType: "entity",
        formatToNodes: () => [],
        getNodes: () => [],
    },
    catalogs: {
        icon: <TreeSchemasIcon />,
        getChildrenQuery: (node: ExplorerNodeData) => ({
            query: `SELECT
    n.oid AS id,
    s.schema_name as name
FROM information_schema.schemata s
JOIN pg_namespace n
    ON n.nspname = s.schema_name
WHERE (s.schema_name IN ('information_schema')
  OR s.schema_name LIKE 'pg_%')
  AND has_schema_privilege(current_user, s.schema_name, 'USAGE');`,
            database: node.context?.database || "",
        }),
        nodeType: "query",
        formatToNodes: (
            catalogs: { id: number; name: string }[],
            node: ExplorerNodeData,
        ) => {
            return catalogs.map((catalog, index) => ({
                id: node.id + "/" + "d-" + index,
                name: catalog.name,
                type: "catalog",
                parentId: node.id,
                context: {
                    connectionId: node.context?.connectionId,
                    database: node.context?.database,
                    catalogId: catalog.id,
                },
            }));
        },
        getNodes: () => [],
    },
    catalog: {
        icon: <TreeSchemaIcon />,
        getChildrenQuery: () => {},
        nodeType: "static",
        formatToNodes: () => [],
        getNodes: (node: ExplorerNodeData) => {
            return [
                {
                    name: "Tables",
                    type: "tables",
                },
                {
                    name: "Views",
                    type: "views",
                },
            ].map((db, index) => ({
                id: node.id + "/" + "d-" + index,
                name: db.name,
                type: db.type,
                parentId: node.id,
                context: {
                    ...node.context,
                },
            }));
        },
    },
    publications: {
        icon: <TreePublisherIcon className="size-3.5 mr-0.5" />,
        getChildrenQuery: {
            query: "",
            database: "",
        },
        nodeType: "static",
        formatToNodes: () => [],
        getNodes: () => [],
    },
    subscriptions: {
        icon: <TreeSubscriberIcon />,
        getChildrenQuery: {
            query: "",
            database: "",
        },
        nodeType: "static",
        formatToNodes: () => [],
        getNodes: () => [],
    },
    views: {
        icon: <TreeViewsIcon />,
        getChildrenQuery: (node: ExplorerNodeData) => {
            return {
                query: `
SELECT
    n.oid          AS schema_oid,
    c.oid          AS id,
    v.table_name   AS name
FROM information_schema.views v
JOIN pg_namespace n
    ON n.nspname = v.table_schema
JOIN pg_class c
    ON c.relname = v.table_name
   AND c.relnamespace = n.oid
WHERE
    n.oid = ${node.context?.schemaId || node.context?.catalogId}
    AND has_schema_privilege(current_user, v.table_schema, 'USAGE')
    AND has_table_privilege(current_user, c.oid, 'SELECT')
ORDER BY v.table_schema, v.table_name;
`,
                database: node.context?.database || "",
            };
        },
        nodeType: "query",
        formatToNodes: (
            views: { id: number; name: string }[],
            node: ExplorerNodeData,
        ) => {
            return views.map((view, index) => ({
                id: node.id + "/" + "v-" + index,
                name: view.name,
                type: "view",
                parentId: node.id,
                context: {
                    connectionId: node.context?.connectionId,
                    database: node.context?.database,
                    viewId: view.id,
                },
            }));
        },
        getNodes: () => [],
    },
    view: {
        icon: <TreeViewIcon />,
        getChildrenQuery: () => {},
        nodeType: "static",
        formatToNodes: () => [],
        getNodes: (node: ExplorerNodeData) => {
            return [
                {
                    name: "Columns",
                    type: "columns",
                },
            ].map((db, index) => ({
                id: node.id + "/" + "v-" + index,
                name: db.name,
                type: db.type,
                parentId: node.id,
                context: {
                    ...node.context,
                },
            }));
        },
    },
    indexes: {
        icon: <TreeIndexesIcon />,
        getChildrenQuery: (node: ExplorerNodeData) => {
            return {
                query: `
SELECT
    (SELECT relname FROM pg_class c WHERE c.oid = i.indexrelid) AS name,
    gs.i AS index_order,
    i.indexrelid AS id,
    i.indisunique,
    i.indisprimary,
    COALESCE(a.attname, pg_get_indexdef(i.indexrelid, gs.i, false)) AS index_column,
    i.indoption[gs.i - 1] = 0 AS ascending
FROM pg_index i
JOIN generate_series(1, array_length(i.indkey, 1)) AS gs(i)
    ON true
LEFT JOIN pg_attribute a
    ON i.indrelid = a.attrelid AND a.attnum = i.indkey[gs.i]
WHERE i.indrelid = ${node.context?.tableId}
ORDER BY i.indexrelid, gs.i;
`,
                database: node.context?.database || "",
            };
        },
        nodeType: "query",
        formatToNodes: (
            views: { id: number; name: string }[],
            node: ExplorerNodeData,
        ) => {
            return views.map((view, index) => ({
                id: node.id + "/" + "v-" + index,
                name: view.name,
                type: "index",
                parentId: node.id,
                context: {
                    ...node.context,
                },
            }));
        },
        getNodes: () => [],
    },
    index: {
        icon: <TreeIndexIcon />,
        getChildrenQuery: () => {},
        nodeType: "entity",
        formatToNodes: () => [],
        getNodes: () => [],
    },
};
