package com.serenedb.drivers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.postgresql.copy.CopyManager;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcFeaturesTest {

    private static String database;

    @BeforeAll
    static void setUp() throws Exception {
        database = Target.createDatabase("features");
        try (Connection conn = Target.connect(database)) {
            Target.execute(conn,
                    "CREATE SCHEMA app",
                    "CREATE TABLE app.note (id serial PRIMARY KEY, body text)",
                    "CREATE TABLE item (id integer PRIMARY KEY, name text)",
                    "INSERT INTO item SELECT i, 'item ' || i FROM generate_series(1, 100) i",
                    "CREATE FUNCTION add_one(i integer) RETURNS integer LANGUAGE sql AS 'SELECT i + 1'");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        Target.dropDatabase(database);
    }

    private static Connection connect(String parameters) throws SQLException {
        return DriverManager.getConnection(Target.url(database) + parameters, Target.credentials());
    }

    private static String one(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void generatedKeys() throws Exception {
        try (Connection conn = connect(""); Statement st = conn.createStatement()) {
            st.executeUpdate("INSERT INTO app.note (body) VALUES ('first')", Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = st.getGeneratedKeys()) {
                assertTrue(keys.next());
                assertTrue(keys.getInt("id") > 0);
                assertEquals("first", keys.getString("body"));
            }
        }
    }

    @Test
    void rewrittenBatchInserts() throws Exception {
        try (Connection conn = connect("?reWriteBatchedInserts=true");
             PreparedStatement ps = conn.prepareStatement("INSERT INTO app.note (body) VALUES (?)")) {
            for (int i = 0; i < 25; i++) {
                ps.setString(1, "batch " + i);
                ps.addBatch();
            }
            assertEquals(25, ps.executeBatch().length);
            assertEquals("25", one(conn, "SELECT count(*) FROM app.note WHERE body LIKE 'batch %'"));
        }
    }

    @Test
    void cursorFetchStreamsRows() throws Exception {
        try (Connection conn = connect("")) {
            conn.setAutoCommit(false);
            try (Statement st = conn.createStatement()) {
                st.setFetchSize(7);
                int rows = 0;
                try (ResultSet rs = st.executeQuery("SELECT id FROM item ORDER BY id")) {
                    while (rs.next()) {
                        assertEquals(++rows, rs.getInt(1));
                    }
                }
                assertEquals(100, rows);
            }
            conn.commit();
        }
    }

    @Test
    void maxRowsLimitsResults() throws Exception {
        try (Connection conn = connect(""); Statement st = conn.createStatement()) {
            st.setMaxRows(5);
            int rows = 0;
            try (ResultSet rs = st.executeQuery("SELECT id FROM item")) {
                while (rs.next()) {
                    rows++;
                }
            }
            assertEquals(5, rows);
        }
    }

    @Test
    void transactionIsolationAndReadOnly() throws Exception {
        try (Connection conn = connect("")) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
            conn.setReadOnly(true);
            conn.setAutoCommit(false);
            assertThrows(SQLException.class, () -> conn.createStatement()
                    .executeUpdate("INSERT INTO item VALUES (1000, 'read only')"));
            conn.rollback();
        }
    }

    @Test
    void queryTimeoutAndCancel() throws Exception {
        try (Connection conn = connect(""); Statement st = conn.createStatement()) {
            st.setQueryTimeout(1);
            assertEquals("57014", assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT pg_sleep(5)")).getSQLState());
        }
        try (Connection conn = connect(""); Statement st = conn.createStatement()) {
            Thread canceller = new Thread(() -> {
                try {
                    Thread.sleep(500);
                    st.cancel();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            canceller.start();
            assertEquals("57014", assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT pg_sleep(5)")).getSQLState());
            canceller.join();
        }
    }

    @Test
    void copyInAndOut() throws Exception {
        try (Connection conn = connect("")) {
            CopyManager copy = conn.unwrap(PGConnection.class).getCopyAPI();
            assertEquals(2, copy.copyIn("COPY item FROM STDIN (FORMAT csv)",
                    new StringReader("500,five hundred\n501,\"five, one\"\n")));
            StringWriter out = new StringWriter();
            copy.copyOut("COPY (SELECT * FROM item WHERE id >= 500 ORDER BY id) TO STDOUT (FORMAT csv)", out);
            assertEquals("500,five hundred\n501,\"five, one\"\n", out.toString());
        }
    }

    @Test
    void scrollableAndMultipleResults() throws Exception {
        try (Connection conn = connect("")) {
            try (Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                 ResultSet rs = st.executeQuery("SELECT id FROM item WHERE id <= 100 ORDER BY id")) {
                assertTrue(rs.last());
                assertEquals(100, rs.getInt(1));
                assertTrue(rs.absolute(3));
                assertEquals(3, rs.getInt(1));
                assertTrue(rs.previous());
                assertEquals(2, rs.getInt(1));
            }
            try (Statement st = conn.createStatement()) {
                assertTrue(st.execute("SELECT 1; SELECT 2"));
                try (ResultSet rs = st.getResultSet()) {
                    rs.next();
                    assertEquals(1, rs.getInt(1));
                }
                assertTrue(st.getMoreResults());
                try (ResultSet rs = st.getResultSet()) {
                    rs.next();
                    assertEquals(2, rs.getInt(1));
                }
                assertFalse(st.getMoreResults());
            }
        }
    }

    @Test
    void validityClientInfoAndEscapes() throws Exception {
        try (Connection conn = connect("")) {
            assertTrue(conn.isValid(2));
            conn.setClientInfo("ApplicationName", "meat hook");
            assertEquals("meat hook", one(conn, "SHOW application_name"));
            assertEquals("PUDGE 2024-01-02", one(conn, "SELECT {fn ucase('pudge')} || ' ' || {d '2024-01-02'}"));
        }
    }

    @Test
    void applicationNameStartupParameter() throws Exception {
        Properties props = Target.credentials();
        props.setProperty("ApplicationName", "hook");
        try (Connection conn = DriverManager.getConnection(Target.url(database), props)) {
            assertEquals("hook", one(conn, "SHOW application_name"));
        }
    }

    @Test
    void applicationNameInPgStatActivity() throws Throwable {
        Target.xfailOnSereneDB("pg_stat_activity has no application_name", () -> {
            try (Connection conn = connect("?ApplicationName=hook")) {
                assertEquals("hook", one(conn,
                        "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()"));
            }
        });
    }

    @Test
    void currentSchemaParameter() throws Throwable {
        Target.xfailOnSereneDB("startup parameters other than user, database and application_name are ignored",
                () -> {
                    try (Connection conn = connect("?currentSchema=app")) {
                        assertEquals("app", one(conn, "SELECT current_schema()"));
                    }
                });
    }

    @Test
    void optionsParameter() throws Throwable {
        Target.xfailOnSereneDB("startup parameters other than user, database and application_name are ignored",
                () -> {
                    try (Connection conn = connect("?options=-c%20statement_timeout%3D1234")) {
                        assertEquals("1234ms", one(conn, "SHOW statement_timeout"));
                    }
                });
    }

    @Test
    void savepoints() throws Throwable {
        Target.xfailOnSereneDB("no SAVEPOINT (#722)", () -> {
            try (Connection conn = connect("")) {
                conn.setAutoCommit(false);
                Savepoint sp = conn.setSavepoint("before");
                conn.createStatement().executeUpdate("INSERT INTO item VALUES (2000, 'undone')");
                conn.rollback(sp);
                conn.commit();
                assertEquals("0", one(conn, "SELECT count(*) FROM item WHERE id = 2000"));
            }
        });
    }

    @Test
    void noticesBecomeWarnings() throws Throwable {
        Target.xfailOnSereneDB("no NOTICE when DROP ... IF EXISTS skips a missing object", () -> {
            try (Connection conn = connect(""); Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS nothing_here");
                assertNotNull(st.getWarnings());
            }
        });
    }

    @Test
    void listenNotify() throws Throwable {
        Target.xfailOnSereneDB("no LISTEN/NOTIFY (#723)", () -> {
            try (Connection conn = connect(""); Statement st = conn.createStatement()) {
                st.execute("LISTEN hook");
                st.execute("NOTIFY hook, 'pudge'");
                PGNotification[] notifications = conn.unwrap(PGConnection.class).getNotifications(2000);
                assertEquals("pudge", notifications[0].getParameter());
            }
        });
    }

    @Test
    void largeObjects() throws Throwable {
        Target.xfailOnSereneDB("no large-object functions (lo_creat over fastpath)", () -> {
            try (Connection conn = connect("")) {
                conn.setAutoCommit(false);
                LargeObjectManager objects = conn.unwrap(PGConnection.class).getLargeObjectAPI();
                long oid = objects.createLO();
                try (LargeObject lo = objects.open(oid, LargeObjectManager.WRITE)) {
                    lo.write("blob".getBytes());
                }
                try (LargeObject lo = objects.open(oid, LargeObjectManager.READ)) {
                    assertArrayEquals("blob".getBytes(), lo.read(4));
                }
                conn.commit();
            }
        });
    }

    @Test
    void updatableResultSet() throws Throwable {
        Target.xfailOnSereneDB("RowDescription carries no table oid and column number", () -> {
            try (Connection conn = connect("");
                 Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                 ResultSet rs = st.executeQuery("SELECT id, name FROM item WHERE id = 3")) {
                assertTrue(rs.next());
                rs.updateString("name", "renamed");
                rs.updateRow();
                assertEquals("renamed", one(conn, "SELECT name FROM item WHERE id = 3"));
            }
        });
    }

    @Test
    void callableStatementReturnValue() throws Throwable {
        Target.xfailOnSereneDB("void-typed parameters are not dropped from function calls,"
                + " pgjdbc binds the return slot of {? = call f(?)} as $1::void", () -> {
            try (Connection conn = connect(""); CallableStatement cs = conn.prepareCall("{? = call add_one(?)}")) {
                cs.registerOutParameter(1, Types.INTEGER);
                cs.setInt(2, 41);
                cs.execute();
                assertEquals(42, cs.getInt(1));
            }
        });
    }
}
