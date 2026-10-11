package com.serenedb.drivers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class JdbcMetadataTest {

    private static final List<String> TABLES = List.of(
            "author", "book", "category", "edition", "edition_note", "measure");

    private static final int[] RESULT_SET_TYPES = {
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.TYPE_SCROLL_SENSITIVE};

    private static String database;
    private static String reader;
    private static Connection conn;
    private static DatabaseMetaData md;

    @BeforeAll
    static void setUp() throws Exception {
        database = Target.createDatabase("meta");
        reader = database + "_reader";
        try (Connection admin = Target.admin()) {
            Target.execute(admin, "DROP ROLE IF EXISTS " + reader, "CREATE ROLE " + reader);
        }
        conn = Target.connect(database);
        Target.execute(conn,
                "CREATE SCHEMA app",
                "CREATE TYPE app.mood AS ENUM ('sad', 'ok', 'happy')",
                "CREATE TYPE app.pair AS (a integer, b text)",
                "CREATE TABLE app.author (id integer PRIMARY KEY, name varchar(100) NOT NULL UNIQUE,"
                        + " email text, born date, rating numeric(4,2) DEFAULT 0,"
                        + " active boolean DEFAULT true, mood app.mood)",
                "COMMENT ON TABLE app.author IS 'people who write'",
                "COMMENT ON COLUMN app.author.name IS 'display name'",
                "CREATE TABLE app.book (id bigserial PRIMARY KEY,"
                        + " author_id integer NOT NULL REFERENCES app.author (id),"
                        + " title varchar(200) NOT NULL, pages integer CHECK (pages >= 0),"
                        + " price numeric(8,2), published timestamp with time zone, cover bytea,"
                        + " tags text[], meta json, isbn char(13), uid uuid,"
                        + " CONSTRAINT book_author_title_uniq UNIQUE (author_id, title))",
                "CREATE TABLE app.edition (book_id bigint NOT NULL REFERENCES app.book (id),"
                        + " no smallint NOT NULL, year integer, PRIMARY KEY (book_id, no))",
                "CREATE TABLE app.edition_note (book_id bigint, no smallint, note text,"
                        + " FOREIGN KEY (book_id, no) REFERENCES app.edition (book_id, no))",
                "CREATE TABLE app.category (id integer PRIMARY KEY,"
                        + " parent_id integer REFERENCES app.category (id), name text)",
                "CREATE TABLE app.measure (id integer PRIMARY KEY, a integer,"
                        + " doubled integer GENERATED ALWAYS AS (a * 2) STORED, p app.pair,"
                        + " r real, d double precision, t time, tz time with time zone,"
                        + " ts timestamp, iv interval, c char, c3 char(3), b3 bit(3),"
                        + " vb bit varying(8), ip inet, small smallint[], codes varchar(10)[],"
                        + " n numeric, o oid, nm name)",
                "CREATE INDEX book_title_idx ON app.book (title)",
                "CREATE INDEX book_multi_idx ON app.book (author_id, published DESC)",
                "CREATE INDEX book_lower_idx ON app.book (lower(title))",
                "CREATE INDEX book_partial_idx ON app.book (price) WHERE price > 10",
                "CREATE UNIQUE INDEX author_email_idx ON app.author (email)",
                "COMMENT ON INDEX app.book_title_idx IS 'by title'",
                "CREATE VIEW app.cheap_books AS SELECT id, title, price FROM app.book WHERE price < 10",
                "COMMENT ON VIEW app.cheap_books IS 'under ten'",
                "CREATE SEQUENCE app.counter_seq START 5 INCREMENT 2",
                "COMMENT ON SEQUENCE app.counter_seq IS 'counts'",
                "CREATE FUNCTION app.add_one(i integer) RETURNS integer LANGUAGE sql AS 'SELECT i + 1'",
                "COMMENT ON FUNCTION app.add_one IS 'increments'",
                "CREATE FUNCTION app.book_count(a integer) RETURNS bigint LANGUAGE sql"
                        + " AS 'SELECT count(*) FROM app.book WHERE author_id = a'",
                "CREATE FUNCTION app.pairs() RETURNS TABLE (k integer, v text) LANGUAGE sql"
                        + " AS 'SELECT 1, ''a'''",
                "CREATE PROCEDURE app.touch(x integer) LANGUAGE sql AS 'SELECT 1'",
                "GRANT SELECT ON app.author TO " + reader,
                "GRANT SELECT (title), UPDATE (price) ON app.book TO " + reader);
        md = conn.getMetaData();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
        Target.dropDatabase(database);
        try (Connection admin = Target.admin()) {
            Target.execute(admin, "DROP ROLE IF EXISTS " + reader);
        }
    }

    private static String normalize(String column, String value) {
        String v = value.replace(database, "<db>");
        if (column.equalsIgnoreCase("SPECIFIC_NAME")) {
            v = v.replaceAll("_\\d+$", "_<oid>");
        }
        return v;
    }

    private static Json.Table read(ResultSet rs) throws SQLException {
        return Json.read(rs, JdbcMetadataTest::normalize);
    }

    private interface Lookup {
        Json.Table get() throws SQLException;
    }

    private static void golden(String name, Lookup lookup) throws Exception {
        String text;
        try {
            text = lookup.get().render();
        } catch (SQLException e) {
            if (!Target.isSereneDB()) {
                throw e;
            }
            text = Json.object(Map.of("error", e.getMessage()));
        }
        Target.checkGolden(Target.fixtures("jdbc").resolve(name + ".json"), text);
    }

    private interface PerTable {
        ResultSet apply(String table) throws SQLException;
    }

    private static Json.Table perTable(PerTable call) throws SQLException {
        Json.Table all = null;
        for (String t : TABLES) {
            Json.Table one = read(call.apply(t));
            all = all == null ? one : all.plus(one);
        }
        return all;
    }

    @Test
    void tables() throws Exception {
        golden("getTables", () -> read(md.getTables(null, "app", "%", null)));
    }

    @Test
    void tablesOfType() throws Exception {
        golden("getTables_views_sequences", () ->
                read(md.getTables(null, "app", "%", new String[] {"VIEW", "SEQUENCE", "TYPE"})));
    }

    @Test
    void informationSchemaTables() throws Exception {
        golden("getTables_information_schema", () -> read(md.getTables(null, "information_schema", "%", null)));
    }

    @Test
    void systemTables() throws Exception {
        golden("getTables_pg_catalog", () -> read(md.getTables(null, "pg_catalog", "pg_%",
                new String[] {"SYSTEM TABLE", "SYSTEM VIEW"})));
    }

    @Test
    void columns() throws Exception {
        golden("getColumns", () -> read(md.getColumns(null, "app", "%", "%")));
    }

    @Test
    void primaryKeys() throws Exception {
        golden("getPrimaryKeys", () -> perTable(t -> md.getPrimaryKeys(null, "app", t)));
    }

    @Test
    void importedKeys() throws Exception {
        golden("getImportedKeys", () -> perTable(t -> md.getImportedKeys(null, "app", t)));
    }

    @Test
    void exportedKeys() throws Exception {
        golden("getExportedKeys", () -> perTable(t -> md.getExportedKeys(null, "app", t)));
    }

    @Test
    void crossReference() throws Exception {
        golden("getCrossReference", () -> read(md.getCrossReference(null, "app", "book", null, "app", "edition"))
                .plus(read(md.getCrossReference(null, "app", "edition", null, "app", "edition_note")))
                .plus(read(md.getCrossReference(null, "app", "category", null, "app", "category"))));
    }

    @Test
    void indexInfo() throws Exception {
        golden("getIndexInfo", () -> perTable(t -> md.getIndexInfo(null, "app", t, false, false)));
    }

    @Test
    void uniqueIndexInfo() throws Exception {
        golden("getIndexInfo_unique", () -> perTable(t -> md.getIndexInfo(null, "app", t, true, true)));
    }

    @Test
    void procedures() throws Exception {
        golden("getProcedures", () -> read(md.getProcedures(null, "app", "%")));
    }

    @Test
    void procedureColumns() throws Exception {
        golden("getProcedureColumns", () -> read(md.getProcedureColumns(null, "app", "%", "%")));
    }

    @Test
    void functions() throws Exception {
        golden("getFunctions", () -> read(md.getFunctions(null, "app", "%")));
    }

    @Test
    void functionColumns() throws Exception {
        golden("getFunctionColumns", () -> read(md.getFunctionColumns(null, "app", "%", "%")));
    }

    @Test
    void builtinFunctions() throws Exception {
        golden("getFunctions_builtin", () -> read(md.getFunctions(null, "pg_catalog", "lower"))
                .plus(read(md.getFunctions(null, "pg_catalog", "now")))
                .plus(read(md.getFunctions(null, "pg_catalog", "length"))));
    }

    @Test
    void typeInfo() throws Exception {
        golden("getTypeInfo", () -> read(md.getTypeInfo()));
    }

    @Test
    void udts() throws Exception {
        golden("getUDTs", () -> read(md.getUDTs(null, "app", "%", null)));
    }

    @Test
    void schemas() throws Exception {
        golden("getSchemas", () -> read(md.getSchemas()).plus(read(md.getSchemas(null, "app%"))));
    }

    @Test
    void catalogs() throws Exception {
        golden("getCatalogs", () -> {
            Json.Table all = read(md.getCatalogs());
            return new Json.Table(all.columns(),
                    all.rows().stream().filter(row -> row.get(0).equals("<db>")).toList());
        });
    }

    @Test
    void tableTypes() throws Exception {
        golden("getTableTypes", () -> read(md.getTableTypes()));
    }

    @Test
    void tablePrivileges() throws Exception {
        golden("getTablePrivileges", () -> read(md.getTablePrivileges(null, "app", "%")).sorted());
    }

    @Test
    void columnPrivileges() throws Exception {
        golden("getColumnPrivileges", () -> read(md.getColumnPrivileges(null, "app", "book", "%")).sorted());
    }

    @Test
    void bestRowIdentifier() throws Exception {
        golden("getBestRowIdentifier", () -> perTable(
                t -> md.getBestRowIdentifier(null, "app", t, DatabaseMetaData.bestRowSession, true)));
    }

    @Test
    void versionColumns() throws Exception {
        golden("getVersionColumns", () -> perTable(t -> md.getVersionColumns(null, "app", t)));
    }

    @Test
    void clientInfoProperties() throws Exception {
        golden("getClientInfoProperties", () -> read(md.getClientInfoProperties()));
    }

    @Test
    void scalarProperties() throws Exception {
        Map<String, Object> props = new TreeMap<>();
        for (Method m : DatabaseMetaData.class.getMethods()) {
            Class<?> r = m.getReturnType();
            boolean scalar = r == boolean.class || r == int.class || r == long.class
                    || r == String.class || r.isEnum() || r == java.sql.RowIdLifetime.class;
            if (m.getParameterCount() != 0 || !scalar) {
                continue;
            }
            Object v;
            try {
                v = m.invoke(md);
            } catch (InvocationTargetException e) {
                if (e.getCause() instanceof SQLFeatureNotSupportedException) {
                    v = "<not implemented>";
                } else if (e.getCause() instanceof SQLException failure && Target.isSereneDB()) {
                    v = "<error: " + failure.getMessage() + ">";
                } else {
                    throw e;
                }
            }
            props.put(m.getName(), v instanceof String s ? normalizeUrl(s)
                    : v == null || v instanceof Number || v instanceof Boolean ? v : v.toString());
        }
        for (int type : RESULT_SET_TYPES) {
            props.put("supportsResultSetType(" + type + ")", md.supportsResultSetType(type));
            props.put("ownUpdatesAreVisible(" + type + ")", md.ownUpdatesAreVisible(type));
            props.put("ownDeletesAreVisible(" + type + ")", md.ownDeletesAreVisible(type));
            props.put("ownInsertsAreVisible(" + type + ")", md.ownInsertsAreVisible(type));
            props.put("othersUpdatesAreVisible(" + type + ")", md.othersUpdatesAreVisible(type));
            props.put("othersDeletesAreVisible(" + type + ")", md.othersDeletesAreVisible(type));
            props.put("othersInsertsAreVisible(" + type + ")", md.othersInsertsAreVisible(type));
            props.put("updatesAreDetected(" + type + ")", md.updatesAreDetected(type));
            props.put("deletesAreDetected(" + type + ")", md.deletesAreDetected(type));
            props.put("insertsAreDetected(" + type + ")", md.insertsAreDetected(type));
            for (int concurrency : new int[] {ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE}) {
                props.put("supportsResultSetConcurrency(" + type + "," + concurrency + ")",
                        md.supportsResultSetConcurrency(type, concurrency));
            }
        }
        for (int level : new int[] {Connection.TRANSACTION_NONE, Connection.TRANSACTION_READ_UNCOMMITTED,
                Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_REPEATABLE_READ,
                Connection.TRANSACTION_SERIALIZABLE}) {
            props.put("supportsTransactionIsolationLevel(" + level + ")",
                    md.supportsTransactionIsolationLevel(level));
        }
        for (int holdability : new int[] {ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT}) {
            props.put("supportsResultSetHoldability(" + holdability + ")",
                    md.supportsResultSetHoldability(holdability));
        }
        Target.checkGolden(Target.fixtures("jdbc").resolve("properties.json"), Json.object(props));
    }

    private static String normalizeUrl(String value) {
        return value.replace(Target.host() + ":" + Target.port(), "<server>").replace(database, "<db>");
    }

    private static final List<String> QUERIES = List.of(
            "SELECT * FROM app.book",
            "SELECT * FROM app.measure",
            "SELECT * FROM app.cheap_books",
            "SELECT a.id, a.name AS writer, b.title, count(*) AS n FROM app.author a"
                    + " JOIN app.book b ON b.author_id = a.id GROUP BY a.id, a.name, b.title",
            "SELECT 1::int2 AS i2, 2::int4 AS i4, 3::int8 AS i8, 1.5::numeric(10,3) AS n,"
                    + " 'x'::varchar(10) AS v, 'y'::char(3) AS c, 'z'::text AS t, true AS b,"
                    + " now() AS ts, current_date AS d, '1 day'::interval AS iv, ARRAY[1, 2] AS arr,"
                    + " '{\"a\": 1}'::json AS j, 1.5::float4 AS f4, 2.5::float8 AS f8,"
                    + " 'abc'::bytea AS by, NULL AS nothing");

    @Test
    void resultSetMetaData() throws Exception {
        List<String> columns = List.of("query", "index", "name", "label", "type", "typeName",
                "className", "precision", "scale", "displaySize", "nullable", "autoIncrement",
                "signed", "caseSensitive", "searchable", "currency", "readOnly", "writable",
                "definitelyWritable", "table", "schema", "catalog");
        List<List<Object>> rows = new ArrayList<>();
        for (int q = 0; q < QUERIES.size(); q++) {
            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(QUERIES.get(q))) {
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    rows.add(List.of(q, i, rsmd.getColumnName(i), rsmd.getColumnLabel(i),
                            rsmd.getColumnType(i), rsmd.getColumnTypeName(i),
                            rsmd.getColumnClassName(i), rsmd.getPrecision(i), rsmd.getScale(i),
                            rsmd.getColumnDisplaySize(i), rsmd.isNullable(i), rsmd.isAutoIncrement(i),
                            rsmd.isSigned(i), rsmd.isCaseSensitive(i), rsmd.isSearchable(i),
                            rsmd.isCurrency(i), rsmd.isReadOnly(i), rsmd.isWritable(i),
                            rsmd.isDefinitelyWritable(i), rsmd.getTableName(i), rsmd.getSchemaName(i),
                            normalize("catalog", rsmd.getCatalogName(i))));
                }
            }
        }
        golden("ResultSetMetaData", () -> new Json.Table(columns, rows));
    }

    private static final List<String> STATEMENTS = List.of(
            "INSERT INTO app.book (author_id, title, pages, price, published, cover, tags, uid)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            "SELECT * FROM app.book WHERE price > ? AND title = ? AND published < ?",
            "UPDATE app.author SET rating = ?, active = ?, born = ? WHERE id = ?",
            "SELECT ?::int2, ?::numeric(10,2), ?::varchar(5), ?::bool, ?::date, ?::uuid, ?::bytea");

    @Test
    void parameterMetaData() throws Exception {
        List<String> columns = List.of("statement", "index", "type", "typeName", "className",
                "precision", "scale", "nullable", "signed", "mode");
        List<List<Object>> rows = new ArrayList<>();
        for (int s = 0; s < STATEMENTS.size(); s++) {
            try (PreparedStatement ps = conn.prepareStatement(STATEMENTS.get(s))) {
                ParameterMetaData pmd = ps.getParameterMetaData();
                for (int i = 1; i <= pmd.getParameterCount(); i++) {
                    rows.add(List.of(s, i, pmd.getParameterType(i), pmd.getParameterTypeName(i),
                            pmd.getParameterClassName(i), pmd.getPrecision(i), pmd.getScale(i),
                            pmd.isNullable(i), pmd.isSigned(i), pmd.getParameterMode(i)));
                }
            }
        }
        golden("ParameterMetaData", () -> new Json.Table(columns, rows));
    }
}
