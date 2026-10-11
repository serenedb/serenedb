package com.serenedb.drivers;

import liquibase.Scope;
import liquibase.command.CommandResults;
import liquibase.command.CommandScope;
import liquibase.command.core.DiffCommandStep;
import liquibase.command.core.DropAllCommandStep;
import liquibase.command.core.GenerateChangelogCommandStep;
import liquibase.command.core.HistoryCommandStep;
import liquibase.command.core.RollbackCommandStep;
import liquibase.command.core.StatusCommandStep;
import liquibase.command.core.TagCommandStep;
import liquibase.command.core.UpdateCommandStep;
import liquibase.command.core.ValidateCommandStep;
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep;
import liquibase.command.core.helpers.DiffOutputControlCommandStep;
import liquibase.command.core.helpers.ReferenceDbUrlConnectionCommandStep;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.DiffResult;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.structure.core.Catalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiquibaseTest {

    private static final String CHANGELOG = "liquibase/changelog.xml";
    private static final String CHANGELOG_FILE = "changelogFile";

    private String database;

    @BeforeEach
    void setUp() throws Exception {
        database = Target.createDatabase("liquibase");
    }

    @AfterEach
    void tearDown() throws Exception {
        Target.dropDatabase(database);
        Target.dropDatabase(Target.databaseName("liquibase_ref"));
    }

    private static Database open(String db) throws SQLException, liquibase.exception.DatabaseException {
        return DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(Target.connect(db)));
    }

    private static String run(String db, String[] command, Map<String, Object> args) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (Database target = open(db)) {
            Scope.child(Map.of(Scope.Attr.resourceAccessor.name(), new ClassLoaderResourceAccessor()), () -> {
                CommandScope scope = new CommandScope(command)
                        .addArgumentValue(DbUrlConnectionArgumentsCommandStep.DATABASE_ARG, target)
                        .setOutput(out);
                args.forEach(scope::addArgumentValue);
                scope.execute();
            });
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private static void update(String db, String changelog) throws Exception {
        run(db, UpdateCommandStep.COMMAND_NAME, Map.of(CHANGELOG_FILE, changelog));
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

    private List<String> columns(String table) throws SQLException {
        return query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public'"
                + " AND table_name = '" + table + "' ORDER BY ordinal_position");
    }

    @Test
    void updateAppliesEveryChangeSet() throws Exception {
        update(database, CHANGELOG);
        assertEquals(List.of("1|EXECUTED|null", "2|EXECUTED|null", "3|EXECUTED|seeded", "4|EXECUTED|null",
                "5|EXECUTED|null"), query("SELECT id || '|' || exectype || '|' || coalesce(tag, 'null')"
                + " FROM databasechangelog ORDER BY orderexecuted"));
        assertEquals(List.of("Pudge|Dire|Butcher|100"), query("SELECT h.name || '|' || v.guild || '|' || h.title"
                + " || '|' || h.id FROM hero h JOIN hero_view v ON v.name = h.name"));
        assertEquals(List.of("id", "hero_id", "price"), columns("item"));
        assertEquals(List.of("f"), query("SELECT CASE WHEN locked THEN 't' ELSE 'f' END FROM databasechangeloglock"));
        update(database, CHANGELOG);
        assertEquals(List.of("5"), query("SELECT count(*) FROM databasechangelog"));
    }

    @Test
    void statusValidateAndHistory() throws Exception {
        assertTrue(run(database, StatusCommandStep.COMMAND_NAME,
                Map.of(CHANGELOG_FILE, CHANGELOG)).contains("5 changesets have not been applied"));
        update(database, CHANGELOG);
        assertTrue(run(database, StatusCommandStep.COMMAND_NAME,
                Map.of(CHANGELOG_FILE, CHANGELOG)).contains("is up to date"));
        run(database, ValidateCommandStep.COMMAND_NAME, Map.of(CHANGELOG_FILE, CHANGELOG));
        String history = run(database, HistoryCommandStep.COMMAND_NAME, Map.of());
        assertTrue(history.contains("liquibase/changelog.xml") && history.contains("seeded"), history);
    }

    @Test
    void rollbackToTag() throws Exception {
        update(database, CHANGELOG);
        run(database, TagCommandStep.COMMAND_NAME, Map.of(TagCommandStep.TAG_ARG.getName(), "latest"));
        run(database, RollbackCommandStep.COMMAND_NAME, Map.of(CHANGELOG_FILE, CHANGELOG,
                RollbackCommandStep.TAG_ARG.getName(), "seeded"));
        assertEquals(List.of(), columns("item"));
        assertEquals(List.of("id", "guild_id", "name", "level", "gold", "created"), columns("hero"));
        assertEquals(List.of(), query("SELECT table_name FROM information_schema.tables"
                + " WHERE table_schema = 'public' AND table_name IN ('item', 'hero_view')"));
        assertEquals(List.of("2"), query("SELECT count(*) FROM databasechangelog"));
        update(database, CHANGELOG);
        assertEquals(List.of("Butcher"), query("SELECT title FROM hero"));
    }

    @Test
    void diffAgainstIdenticalDatabaseIsEmpty() throws Throwable {
        String reference = Target.createDatabase("liquibase_ref");
        update(database, CHANGELOG);
        update(reference, CHANGELOG);
        try (Database ref = open(reference)) {
            CommandResults results = Scope.child(Map.of(Scope.Attr.resourceAccessor.name(),
                    new ClassLoaderResourceAccessor()), () -> {
                try (Database target = open(database)) {
                    return new CommandScope(DiffCommandStep.COMMAND_NAME)
                            .addArgumentValue(DbUrlConnectionArgumentsCommandStep.DATABASE_ARG, target)
                            .addArgumentValue(ReferenceDbUrlConnectionCommandStep.REFERENCE_DATABASE_ARG, ref)
                            .setOutput(new ByteArrayOutputStream())
                            .execute();
                }
            });
            DiffResult diff = (DiffResult) results.getResult(DiffCommandStep.DIFF_RESULT.getName());
            assertTrue(diff.getMissingObjects().isEmpty(), diff.getMissingObjects().toString());
            assertTrue(diff.getUnexpectedObjects().isEmpty(), diff.getUnexpectedObjects().toString());
            assertEquals(List.of(), diff.getChangedObjects().keySet().stream()
                    .filter(o -> !(o instanceof Catalog)).map(Object::toString).toList());
        }
    }

    @Test
    void generateChangelogMatchesPostgreSQL() throws Exception {
        update(database, CHANGELOG);
        Path out = Files.createTempDirectory("liquibase").resolve("generated.xml");
        String text;
        try {
            run(database, GenerateChangelogCommandStep.COMMAND_NAME, Map.of(
                    GenerateChangelogCommandStep.CHANGELOG_FILE_ARG.getName(), out.toString(),
                    GenerateChangelogCommandStep.AUTHOR_ARG.getName(), "drivers",
                    DiffOutputControlCommandStep.INCLUDE_SCHEMA_ARG.getName(), false));
            text = Files.readString(out).replaceAll("id=\"\\d+-(\\d+)\"", "id=\"generated-$1\"");
        } catch (Exception e) {
            if (!Target.isSereneDB()) {
                throw e;
            }
            Throwable root = e;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            text = Json.object(Map.of("error", root.getMessage()));
        }
        Target.checkGolden(Target.fixtures("liquibase").resolve("generated-changelog.xml"), text);
    }

    @Test
    void dropAllRemovesEverything() throws Throwable {
        update(database, CHANGELOG);
        Target.xfailOnSereneDB("DROP TABLE ... CASCADE fails on a table a foreign key references", () -> {
            run(database, DropAllCommandStep.COMMAND_NAME, Map.of());
            assertEquals(List.of(), query("SELECT c.relname FROM pg_class c JOIN pg_namespace n"
                    + " ON n.oid = c.relnamespace WHERE n.nspname = 'public'"));
        });
    }

    @Test
    void autoIncrementColumns() throws Throwable {
        Target.xfailOnSereneDB("no identity columns (#1426)", () -> {
            update(database, "liquibase/gaps/identity.xml");
            try (Connection conn = Target.connect(database)) {
                Target.execute(conn, "INSERT INTO counter (hits) VALUES (1), (2)");
            }
            assertEquals(List.of("1", "2"), query("SELECT id FROM counter ORDER BY id"));
        });
    }

    @Test
    void renameColumnOfReferencedTable() throws Throwable {
        Target.xfailOnSereneDB("cannot rename, retype or drop columns of a table a foreign key references", () -> {
            update(database, "liquibase/gaps/rename_referenced.xml");
            assertEquals(List.of("id", "founded_on"), columns("clan"));
        });
    }

    @Test
    void addForeignKeyConstraint() throws Throwable {
        Target.xfailOnSereneDB("no ALTER TABLE ADD FOREIGN KEY (#1427)", () -> {
            update(database, "liquibase/gaps/foreign_key.xml");
            assertEquals(List.of("pet_owner_fk"), query("SELECT conname FROM pg_constraint WHERE contype = 'f'"));
        });
    }
}
