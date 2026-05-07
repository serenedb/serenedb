package com.serenedb.drivers;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * r2dbc-postgresql round-trip matrix. Reactive-async non-blocking driver.
 * Same conceptual flow as PgjdbcDriverTest but the API returns Publishers,
 * so we block-wait for results in tests.
 */
class R2dbcDriverTest {

    private static final String DRIVER_KEY = "java_r2dbc";
    private static ConnectionFactory factory;
    private static String schema;

    @BeforeAll
    static void setUp() throws Exception {
        var host = System.getProperty("test.host", "localhost");
        var port = Integer.parseInt(System.getProperty("test.port", "5432"));
        var database = System.getProperty("test.database", "postgres");
        var user = System.getProperty("test.user", "postgres");

        factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "postgresql")
                .option(ConnectionFactoryOptions.HOST, host)
                .option(ConnectionFactoryOptions.PORT, port)
                .option(ConnectionFactoryOptions.USER, user)
                .option(ConnectionFactoryOptions.DATABASE, database)
                .build());

        schema = SpecLoader.schemaName(DRIVER_KEY);
        Mono.from(factory.create())
            .flatMap(c -> Mono.from(c.createStatement(
                    "CREATE SCHEMA IF NOT EXISTS \"" + schema + "\"").execute())
                .flatMap(r -> Mono.from(r.getRowsUpdated()))
                .then(Mono.from(c.createStatement(
                    "SET search_path TO \"" + schema + "\", public, pg_catalog")
                    .execute()).flatMap(r -> Mono.from(r.getRowsUpdated())))
                .then(Mono.from(c.close())))
            .block(Duration.ofSeconds(10));
    }

    @AfterAll
    static void tearDown() {
        if (factory == null) return;
        try {
            Mono.from(factory.create())
                .flatMap(c -> Mono.from(c.createStatement(
                    "DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE").execute())
                    .flatMap(r -> Mono.from(r.getRowsUpdated()))
                    .then(Mono.from(c.close())))
                .block(Duration.ofSeconds(5));
        } catch (Exception ignore) {}
    }

    @Test
    void smokeSelectOne() {
        Integer v = Mono.from(factory.create())
            .flatMap(c -> Mono.from(c.createStatement("SELECT 1 AS v").execute())
                .flatMap(r -> Mono.from(r.map((row, meta) -> row.get("v", Integer.class))))
                .doFinally(s -> Mono.from(c.close()).subscribe()))
            .block(Duration.ofSeconds(10));
        assertEquals(1, v);
    }

    static Stream<Arguments> casesAsArgs() throws Exception {
        return SpecLoader.casesFor(DRIVER_KEY).stream().map(c ->
            Arguments.of(c.type().oid(), c.type().name(), c.protocol(),
                         c.sampleIdx(), c));
    }

    @ParameterizedTest(name = "{0}-{1}-{2}-{3}")
    @MethodSource("casesAsArgs")
    void roundtrip(int oid, String name, String protocol, int idx,
                   SpecLoader.Case c) throws Exception {
        Connection conn = Mono.from(factory.create())
            .block(Duration.ofSeconds(10));
        try {
            // Re-set search_path on this fresh connection
            Mono.from(conn.createStatement(
                "SET search_path TO \"" + schema + "\", public, pg_catalog")
                .execute()).flatMap(r -> Mono.from(r.getRowsUpdated()))
                .block(Duration.ofSeconds(5));

            String expected = serverText(conn, c.type().pgTypname(), c.sample());
            String actual = switch (c.protocol()) {
                case "simple", "extended-noparam" -> runInline(conn, c);
                case "extended-text", "extended-binary" -> runBound(conn, c);
                default -> throw new IllegalArgumentException(c.protocol());
            };
            assertEquals(expected, actual,
                "type=" + c.type().name() + " proto=" + c.protocol() +
                " sample=" + c.sample());
        } finally {
            Mono.from(conn.close()).block(Duration.ofSeconds(5));
        }
    }

    private static String serverText(Connection conn, String pgTypname, Object sample) {
        if (sample == null) return null;
        var stmt = conn.createStatement(
            "SELECT (($1::text)::" + pgTypname + ")::text AS v");
        stmt.bind("$1", sample.toString());
        java.util.Optional<String> opt = Mono.from(stmt.execute())
            .flatMap(r -> Mono.from(r.map((row, m) ->
                java.util.Optional.<String>ofNullable(row.get("v", String.class)))))
            .block(Duration.ofSeconds(5));
        return opt == null ? null : opt.orElse(null);
    }

    private static String runInline(Connection conn, SpecLoader.Case c) {
        Object sample = c.sample();
        String sql;
        if (sample == null) {
            sql = "SELECT NULL::" + c.type().pgTypname() + "::text AS v";
        } else {
            String lit = sample.toString().replace("'", "''");
            sql = "SELECT '" + lit + "'::" + c.type().pgTypname() + "::text AS v";
        }
        java.util.Optional<String> opt = Mono.from(conn.createStatement(sql).execute())
            .flatMap(r -> Mono.from(r.map((row, m) ->
                java.util.Optional.<String>ofNullable(row.get("v", String.class)))))
            .block(Duration.ofSeconds(5));
        return opt == null ? null : opt.orElse(null);
    }

    private static String runBound(Connection conn, SpecLoader.Case c) {
        // Same trick as the JDBC test: $1::text::<type> so we can always
        // bind a string regardless of target type.
        String cast = c.type().castSql().replaceFirst("\\$1", "\\$1::text");
        var stmt = conn.createStatement("SELECT (" + cast + ")::text AS v");
        if (c.sample() == null) {
            stmt.bindNull("$1", String.class);
        } else {
            stmt.bind("$1", c.sample().toString());
        }
        java.util.Optional<String> opt = Mono.from(stmt.execute())
            .flatMap(r -> Mono.from(r.map((row, m) ->
                java.util.Optional.<String>ofNullable(row.get("v", String.class)))))
            .block(Duration.ofSeconds(5));
        return opt == null ? null : opt.orElse(null);
    }
}
