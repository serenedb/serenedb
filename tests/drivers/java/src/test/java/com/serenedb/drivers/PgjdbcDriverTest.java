package com.serenedb.drivers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * pgjdbc driver matrix against SereneDB.
 *
 * pgjdbc has two protocol modes (extended is default; simple is opt-in via
 * preferQueryMode=simple). We exercise both. The "binary" axis is selected
 * via binaryTransfer/prepareThreshold settings; we run two connections,
 * one biased to text-format params, one biased to binary-format params.
 */
class PgjdbcDriverTest {

    private static final String DRIVER_KEY = "java_pgjdbc";

    private static Connection extendedTextConn;
    private static Connection extendedBinaryConn;
    private static Connection simpleConn;
    private static String schema;

    @BeforeAll
    static void setUp() throws Exception {
        Class.forName("org.postgresql.Driver");
        String host = System.getProperty("test.host", "localhost");
        String port = System.getProperty("test.port", "5432");
        String database = System.getProperty("test.database", "serenedb");
        String user = System.getProperty("test.user", "serenedb");

        String base = "jdbc:postgresql://" + host + ":" + port + "/" + database;

        Properties textProps = new Properties();
        textProps.setProperty("user", user);
        textProps.setProperty("binaryTransfer", "false");
        textProps.setProperty("prepareThreshold", "1");
        extendedTextConn = DriverManager.getConnection(base, textProps);
        extendedTextConn.setAutoCommit(true);

        Properties binProps = new Properties();
        binProps.setProperty("user", user);
        binProps.setProperty("binaryTransfer", "true");
        binProps.setProperty("prepareThreshold", "1");
        extendedBinaryConn = DriverManager.getConnection(base, binProps);
        extendedBinaryConn.setAutoCommit(true);

        Properties simpleProps = new Properties();
        simpleProps.setProperty("user", user);
        simpleProps.setProperty("preferQueryMode", "simple");
        simpleConn = DriverManager.getConnection(base, simpleProps);
        simpleConn.setAutoCommit(true);

        schema = SpecLoader.schemaName(DRIVER_KEY);
        try (Statement st = extendedTextConn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS \"" + schema + "\"");
            st.execute("SET search_path TO \"" + schema + "\", public, pg_catalog");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (extendedTextConn != null) {
            try (Statement st = extendedTextConn.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
            extendedTextConn.close();
        }
        if (extendedBinaryConn != null) extendedBinaryConn.close();
        if (simpleConn != null) simpleConn.close();
    }

    // ---- smoke -----------------------------------------------------------

    @Test
    @DisplayName("smoke: SELECT 1")
    void smokeSelectOne() throws Exception {
        try (Statement st = extendedTextConn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    @DisplayName("smoke: server_version is reported")
    void smokeServerVersion() throws Exception {
        org.postgresql.PGConnection pg = extendedTextConn.unwrap(org.postgresql.PGConnection.class);
        // pgjdbc exposes the parameter status map.
        var ps = pg.getParameterStatuses();
        assertNotNull(ps.get("server_version"));
        assertNotEquals("", ps.get("server_version"));
    }

    // ---- round-trip matrix ----------------------------------------------

    static Stream<SpecLoader.Case> cases() throws Exception {
        return SpecLoader.casesFor(DRIVER_KEY).stream();
    }

    static Stream<org.junit.jupiter.params.provider.Arguments> casesAsArgs() throws Exception {
        return SpecLoader.casesFor(DRIVER_KEY).stream().map(c ->
            org.junit.jupiter.params.provider.Arguments.of(
                c.type().oid(), c.type().name(), c.protocol(), c.sampleIdx(), c));
    }

    @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
    @MethodSource("casesAsArgs")
    void roundtrip(int oid, String name, String protocol, int idx,
                   SpecLoader.Case c) throws Exception {
        String expected = serverText(c.type().pgTypname(), c.sample());
        String actual = switch (c.protocol()) {
            case "simple" -> runInline(simpleConn, c);
            case "extended-noparam" -> runInline(extendedTextConn, c);
            case "extended-text" -> runBound(extendedTextConn, c);
            case "extended-binary" -> runBound(extendedBinaryConn, c);
            default -> throw new IllegalArgumentException(c.protocol());
        };
        assertEquals(expected, actual,
                "type=" + c.type().name() + " proto=" + c.protocol() +
                        " sample=" + c.sample());
    }

    // ---- helpers ---------------------------------------------------------

    /** Server's canonical text form for sample::pgTypname. */
    private static String serverText(String pgTypname, Object sample) throws Exception {
        try (PreparedStatement ps = extendedTextConn.prepareStatement(
                "SELECT (?::text)::" + pgTypname + "::text")) {
            if (sample == null) {
                ps.setNull(1, Types.VARCHAR);
            } else {
                ps.setString(1, sample.toString());
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return rs.getString(1);
            }
        }
    }

    /** Inline literal cast: used by simple and extended-noparam. */
    private static String runInline(Connection c, SpecLoader.Case cs) throws Exception {
        Object sample = cs.sample();
        String sql;
        if (sample == null) {
            sql = "SELECT NULL::" + cs.type().pgTypname() + "::text AS v";
        } else {
            String lit = sample.toString().replace("'", "''");
            sql = "SELECT '" + lit + "'::" + cs.type().pgTypname() + "::text AS v";
        }
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) return null;
            return rs.getString(1);
        }
    }

    /** Parameterised: $1 -> text/binary depending on the connection. */
    private static String runBound(Connection c, SpecLoader.Case cs) throws Exception {
        String sql = "SELECT (" + cs.type().castSql().replace("$1", "?")
                + ")::text AS v";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            if (cs.sample() == null) {
                ps.setNull(1, Types.VARCHAR);
            } else {
                ps.setString(1, cs.sample().toString());
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return rs.getString(1);
            }
        }
    }
}
