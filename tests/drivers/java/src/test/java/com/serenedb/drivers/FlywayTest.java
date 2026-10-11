package com.serenedb.drivers;

import com.serenedb.drivers.flyway.V3__Promote;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationState;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.migration.JavaMigration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlywayTest {

    private static final String[] ADVISORY_LOCKS = {
            "CREATE SCHEMA locks",
            "CREATE FUNCTION locks.pg_try_advisory_xact_lock(key bigint) RETURNS boolean LANGUAGE sql AS 'SELECT true'",
            "CREATE FUNCTION locks.pg_try_advisory_lock(key bigint) RETURNS boolean LANGUAGE sql AS 'SELECT true'",
            "CREATE FUNCTION locks.pg_advisory_unlock(key bigint) RETURNS boolean LANGUAGE sql AS 'SELECT true'"};

    private String database;

    @BeforeEach
    void setUp() throws Exception {
        database = Target.createDatabase("flyway");
    }

    @AfterEach
    void tearDown() throws Exception {
        Target.dropDatabase(database);
    }

    private void standInAdvisoryLocks() throws SQLException {
        if (Target.isSereneDB()) {
            try (Connection conn = Target.connect(database)) {
                Target.execute(conn, ADVISORY_LOCKS);
            }
        }
    }

    private FluentConfiguration configure(String location, JavaMigration... java) throws SQLException {
        standInAdvisoryLocks();
        DataSource ds = Target.dataSource(database);
        return Flyway.configure()
                .dataSource(Target.isSereneDB() ? Target.lowercaseBooleans(ds) : ds)
                .initSql("SET search_path TO public, locks")
                .locations("classpath:flyway/" + location)
                .javaMigrations(java)
                .cleanDisabled(false);
    }

    private Flyway flyway() throws SQLException {
        return configure("sql", new V3__Promote()).load();
    }

    private List<String> query(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection conn = Target.connect(database); Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    private static List<String> describe(MigrationInfo[] infos) {
        return Arrays.stream(infos).map(i -> (i.getVersion() == null ? "R" : i.getVersion().getVersion())
                + " " + i.getDescription() + " " + i.getType() + " " + i.getState()).toList();
    }

    @Test
    void migrateAppliesEveryMigration() throws Exception {
        Flyway flyway = flyway();
        assertEquals(List.of("1 create schema SQL PENDING", "2 seed and extend SQL PENDING",
                "3 Promote JDBC PENDING", "4 function SQL PENDING", "R hero view SQL PENDING"),
                describe(flyway.info().all()));
        assertEquals(5, flyway.migrate().migrationsExecuted);
        assertEquals(List.of("1 create schema SQL SUCCESS", "2 seed and extend SQL SUCCESS",
                "3 Promote JDBC SUCCESS", "4 function SQL SUCCESS", "R hero view SQL SUCCESS"),
                describe(flyway.info().applied()));
        assertEquals("4", flyway.info().current().getVersion().getVersion());
        assertEquals(List.of("Crystal Maiden 10", "Pudge 26", "Lion 8"),
                query("SELECT name || ' ' || level FROM hero ORDER BY id"));
        assertEquals(List.of("Butcher"), query("SELECT title FROM hero WHERE title IS NOT NULL"));
        assertEquals(0, new BigDecimal("5600").compareTo(
                new BigDecimal(query("SELECT hero_power(level, gold) FROM hero WHERE name = 'Pudge'").get(0))));
        assertEquals(List.of("Dire 2", "Radiant 1"), query("SELECT guild || ' ' || heroes FROM hero_summary ORDER BY guild"));
        assertEquals(0, flyway.migrate().migrationsExecuted);
    }

    @Test
    void historyTableRecordsChecksums() throws Exception {
        Flyway flyway = flyway();
        flyway.migrate();
        assertEquals(List.of("1|V1__create_schema.sql|SQL|t", "2|V2__seed_and_extend.sql|SQL|t",
                        "3|com.serenedb.drivers.flyway.V3__Promote|JDBC|t", "4|V4__function.sql|SQL|t",
                        "null|R__hero_view.sql|SQL|t"),
                query("SELECT coalesce(version, 'null') || '|' || script || '|' || type || '|'"
                        + " || CASE WHEN success THEN 't' ELSE 'f' END FROM flyway_schema_history ORDER BY installed_rank"));
        assertEquals(List.of("postgres"), query("SELECT DISTINCT installed_by FROM flyway_schema_history"));
    }

    @Test
    void targetVersionStopsEarly() throws Exception {
        assertEquals(3, configure("sql", new V3__Promote()).target("2").load().migrate().migrationsExecuted);
        assertEquals(List.of("hero"), query("SELECT table_name FROM information_schema.tables"
                + " WHERE table_schema = 'public' AND table_name = 'hero'"));
        assertEquals(List.of(), query("SELECT proname FROM pg_proc WHERE proname = 'hero_power'"));
    }

    @Test
    void validateAndRepairChecksumDrift() throws Exception {
        Flyway flyway = flyway();
        flyway.migrate();
        assertTrue(flyway.validateWithResult().validationSuccessful);
        try (Connection conn = Target.connect(database)) {
            Target.execute(conn, "UPDATE flyway_schema_history SET checksum = 42 WHERE version = '1'");
        }
        assertFalse(flyway.validateWithResult().validationSuccessful);
        assertThrows(FlywayException.class, flyway::validate);
        assertFalse(flyway.repair().repairActions.isEmpty());
        assertTrue(flyway.validateWithResult().validationSuccessful);
    }

    @Test
    void repeatableMigrationReappliesWhenChanged() throws Exception {
        Flyway flyway = flyway();
        flyway.migrate();
        try (Connection conn = Target.connect(database)) {
            Target.execute(conn, "UPDATE flyway_schema_history SET checksum = 7 WHERE version IS NULL");
        }
        assertEquals(1, flyway.migrate().migrationsExecuted);
        assertEquals(List.of("2"), query("SELECT count(*) FROM flyway_schema_history WHERE version IS NULL"));
    }

    @Test
    void cleanDropsEveryObject() throws Throwable {
        Flyway flyway = flyway();
        flyway.migrate();
        if (Target.isSereneDB()) {
            try (Connection conn = Target.connect(database)) {
                Target.execute(conn, "ALTER TABLE hero DROP CONSTRAINT hero_guild_id_fkey");
            }
        }
        flyway.clean();
        assertEquals(List.of(), query("SELECT c.relname FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE n.nspname = 'public'"));
        assertEquals(List.of(), query("SELECT p.proname FROM pg_proc p JOIN pg_namespace n"
                + " ON n.oid = p.pronamespace WHERE n.nspname = 'public'"));
        assertEquals(5, flyway.migrate().migrationsExecuted);
    }

    @Test
    void cleanDropsReferencedTables() throws Throwable {
        try (Connection conn = Target.connect(database)) {
            Target.execute(conn, "CREATE TABLE lair (id integer PRIMARY KEY)");
            Target.execute(conn, "CREATE TABLE minion (id integer PRIMARY KEY, lair_id integer REFERENCES lair (id))");
            Target.xfailOnSereneDB("DROP TABLE ... CASCADE cannot drop a table that a foreign key references",
                    () -> Target.execute(conn, "DROP TABLE lair CASCADE"));
            Target.execute(conn, "DROP TABLE minion");
            Target.execute(conn, "DROP TABLE IF EXISTS lair");
        }
    }

    @Test
    void baselineExistingSchema() throws Exception {
        try (Connection conn = Target.connect(database)) {
            for (String script : List.of("V1__create_schema.sql", "V2__seed_and_extend.sql")) {
                try (var in = getClass().getResourceAsStream("/flyway/sql/" + script)) {
                    Target.execute(conn, new String(in.readAllBytes()));
                }
            }
        }
        Flyway flyway = configure("sql", new V3__Promote()).baselineVersion("2").baselineDescription("legacy")
                .baselineOnMigrate(true).load();
        assertEquals(3, flyway.migrate().migrationsExecuted);
        assertEquals(List.of("2 legacy BASELINE BASELINE", "3 Promote JDBC SUCCESS", "4 function SQL SUCCESS",
                "R hero view SQL SUCCESS"), describe(flyway.info().applied()));
        assertEquals(List.of("Pudge 26"), query("SELECT name || ' ' || level FROM hero WHERE name = 'Pudge'"));
    }

    @Test
    void failedMigrationRollsBack() throws Exception {
        Flyway flyway = configure("failing").load();
        assertThrows(FlywayException.class, flyway::migrate);
        assertEquals(List.of("survivor"), query("SELECT table_name FROM information_schema.tables"
                + " WHERE table_schema = 'public' AND table_name IN ('survivor', 'casualty')"));
        assertEquals(List.of("1|t"), query("SELECT version || '|' || CASE WHEN success THEN 't' ELSE 'f' END"
                + " FROM flyway_schema_history ORDER BY installed_rank"));
        assertEquals(MigrationState.PENDING, flyway.info().pending()[0].getState());
    }

    @Test
    void stockMigrate() throws Throwable {
        Target.xfailOnSereneDB("no advisory lock functions (pg_try_advisory_xact_lock),"
                + " and bind parameters reject TRUE/FALSE", () -> {
            Flyway flyway = Flyway.configure().dataSource(Target.dataSource(database))
                    .locations("classpath:flyway/sql").javaMigrations(new V3__Promote()).load();
            assertEquals(5, flyway.migrate().migrationsExecuted);
        });
    }
}
