package com.serenedb.drivers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcMetadataGapTest {

    private static String database;
    private static Connection conn;
    private static DatabaseMetaData md;

    @BeforeAll
    static void setUp() throws Exception {
        database = Target.createDatabase("meta_gap");
        conn = Target.connect(database);
        Target.execute(conn,
                "CREATE TABLE owner (id integer PRIMARY KEY, name text NOT NULL)",
                "CREATE TABLE pet (id integer PRIMARY KEY, owner_id integer REFERENCES owner (id))");
        md = conn.getMetaData();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
        Target.dropDatabase(database);
    }

    private static List<String> column(ResultSet rs, String name) throws SQLException {
        List<String> out = new ArrayList<>();
        try (rs) {
            while (rs.next()) {
                out.add(rs.getString(name));
            }
        }
        return out;
    }

    @Test
    void foreignKeysAreListed() throws Throwable {
        assertEquals(List.of("pet_owner_id_fkey"), column(md.getImportedKeys(null, "public", "pet"), "FK_NAME"));
    }

    @Test
    void resultColumnsKnowTheirTable() throws Throwable {
        Target.xfailOnSereneDB("RowDescription carries no table oid and column number", () -> {
            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT id, name FROM owner")) {
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals("owner", rsmd.getTableName(2));
                assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(2));
            }
        });
    }

    @Test
    void tableFunctionColumnsAreListed() throws Throwable {
        Target.execute(conn, "CREATE FUNCTION pairs() RETURNS TABLE (k integer, v text) LANGUAGE sql AS 'SELECT 1, ''a'''");
        assertEquals(List.of("k", "v"), column(md.getFunctionColumns(null, "public", "pairs", "%"), "COLUMN_NAME"));
    }

    @Test
    void descendingIndexColumnsAreReported() throws Throwable {
        Target.xfailOnSereneDB("indexes use access method 'secondary' and indoption drops DESC", () -> {
            Target.execute(conn, "CREATE INDEX pet_owner_desc_idx ON pet (owner_id DESC)");
            Map<String, String> order = new LinkedHashMap<>();
            try (ResultSet rs = md.getIndexInfo(null, "public", "pet", false, false)) {
                while (rs.next()) {
                    order.put(rs.getString("INDEX_NAME"), rs.getString("ASC_OR_DESC"));
                }
            }
            assertEquals(Map.of("pet_pkey", "A", "pet_owner_desc_idx", "D"), order);
        });
    }

    @Test
    void sqlKeywordsAreUnique() throws Throwable {
        List<String> words = Arrays.asList(md.getSQLKeywords().split(","));
        assertEquals(words.size(), new HashSet<>(words).size());
    }

    @Test
    void cascadeRulesAreReported() throws Throwable {
        Target.xfailOnSereneDB("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT", () -> {
            Target.execute(conn, "CREATE TABLE collar (id integer PRIMARY KEY,"
                    + " pet_id integer REFERENCES pet (id) ON DELETE CASCADE ON UPDATE SET NULL)");
            try (ResultSet rs = md.getImportedKeys(null, "public", "collar")) {
                assertTrue(rs.next());
                assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("DELETE_RULE"));
                assertEquals(DatabaseMetaData.importedKeySetNull, rs.getInt("UPDATE_RULE"));
            }
        });
    }

    @Test
    void materializedViewsAreListed() throws Throwable {
        Target.xfailOnSereneDB("no CREATE MATERIALIZED VIEW", () -> {
            Target.execute(conn, "CREATE MATERIALIZED VIEW owner_names AS SELECT name FROM owner");
            assertEquals(List.of("owner_names"),
                    column(md.getTables(null, "public", "%", new String[] {"MATERIALIZED VIEW"}), "TABLE_NAME"));
        });
    }

    @Test
    void partitionedTablesAreListed() throws Throwable {
        Target.xfailOnSereneDB("no PARTITION BY", () -> {
            Target.execute(conn, "CREATE TABLE sales (id integer, at date) PARTITION BY RANGE (at)");
            assertEquals(List.of("sales"),
                    column(md.getTables(null, "public", "%", new String[] {"PARTITIONED TABLE"}), "TABLE_NAME"));
        });
    }

    @Test
    void domainsAreDistinctTypes() throws Throwable {
        Target.xfailOnSereneDB("no CREATE DOMAIN", () -> {
            Target.execute(conn, "CREATE DOMAIN posint AS integer CHECK (VALUE > 0)");
            try (ResultSet rs = md.getUDTs(null, "public", "posint", new int[] {Types.DISTINCT})) {
                assertTrue(rs.next());
                assertEquals(Types.INTEGER, rs.getInt("BASE_TYPE"));
            }
        });
    }

    @Test
    void outParametersAreReported() throws Throwable {
        Target.xfailOnSereneDB("no OUT parameters in CREATE FUNCTION", () -> {
            Target.execute(conn, "CREATE FUNCTION split(a integer, OUT lo integer, OUT hi integer)"
                    + " LANGUAGE sql AS 'SELECT a / 2, a - a / 2'");
            Map<String, Integer> modes = new LinkedHashMap<>();
            try (ResultSet rs = md.getFunctionColumns(null, "public", "split", "%")) {
                while (rs.next()) {
                    modes.put(rs.getString("COLUMN_NAME"), rs.getInt("COLUMN_TYPE"));
                }
            }
            assertEquals(Map.of("a", DatabaseMetaData.functionColumnIn,
                    "lo", DatabaseMetaData.functionColumnOut, "hi", DatabaseMetaData.functionColumnOut), modes);
        });
    }

    @Test
    void procedureCommentsAreRemarks() throws Throwable {
        Target.xfailOnSereneDB("no COMMENT ON PROCEDURE", () -> {
            Target.execute(conn, "CREATE PROCEDURE noop() LANGUAGE sql AS 'SELECT 1'",
                    "COMMENT ON PROCEDURE noop IS 'does nothing'");
            assertEquals(List.of("does nothing"), column(md.getProcedures(null, "public", "noop"), "REMARKS"));
        });
    }

    @Test
    void inheritedColumnsAreListed() throws Throwable {
        Target.xfailOnSereneDB("no INHERITS", () -> {
            Target.execute(conn, "CREATE TABLE puppy (age integer) INHERITS (pet)");
            assertEquals(List.of("owner_id"), column(md.getColumns(null, "public", "puppy", "owner_id"), "COLUMN_NAME"));
        });
    }
}
