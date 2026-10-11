package com.serenedb.drivers;

import com.serenedb.drivers.hibernate.Address;
import com.serenedb.drivers.hibernate.Author;
import com.serenedb.drivers.hibernate.Bike;
import com.serenedb.drivers.hibernate.Book;
import com.serenedb.drivers.hibernate.Car;
import com.serenedb.drivers.hibernate.CardPayment;
import com.serenedb.drivers.hibernate.CashPayment;
import com.serenedb.drivers.hibernate.Counter;
import com.serenedb.drivers.hibernate.Genre;
import com.serenedb.drivers.hibernate.Guild;
import com.serenedb.drivers.hibernate.Hero;
import com.serenedb.drivers.hibernate.Media;
import com.serenedb.drivers.hibernate.Notice;
import com.serenedb.drivers.hibernate.Payment;
import com.serenedb.drivers.hibernate.Podcast;
import com.serenedb.drivers.hibernate.Shift;
import com.serenedb.drivers.hibernate.Tag;
import com.serenedb.drivers.hibernate.Vehicle;
import com.serenedb.drivers.hibernate.Video;
import jakarta.persistence.LockModeType;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.criteria.Root;
import org.hibernate.SessionFactory;
import org.hibernate.jpa.HibernatePersistenceConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HibernateTest {

    private static final Class<?>[] ENTITIES = {
            Author.class, Book.class, Tag.class, Media.class, Podcast.class, Video.class, Vehicle.class,
            Car.class, Bike.class, Payment.class, CardPayment.class, CashPayment.class, Notice.class, Guild.class, Hero.class};

    private static final Pattern CREATE_TABLE = Pattern.compile("create table (\\S+) \\((.*)\\)");
    private static final Pattern ADD_FOREIGN_KEY = Pattern.compile(
            "alter table if exists (\\S+) add constraint (\\S+) foreign key \\(([^)]*)\\) references (\\S+)");

    private record Summary(String title, Integer pages) {}

    private static String database;
    private static SessionFactory sf;

    private static HibernatePersistenceConfiguration configuration(String db, Class<?>... entities) {
        String password = System.getenv("SDB_DRV_PASSWORD");
        return new HibernatePersistenceConfiguration("drivers")
                .managedClasses(entities)
                .jdbcUrl(Target.url(db))
                .jdbcCredentials(Target.user(), password == null ? "" : password)
                .property("hibernate.hbm2ddl.halt_on_error", "true")
                .property("hibernate.hbm2ddl.delimiter", "")
                .property("hibernate.jdbc.batch_size", "20")
                .property("hibernate.order_inserts", "true")
                .property("hibernate.order_updates", "true");
    }

    private static HibernatePersistenceConfiguration directTimeBinding(String db, Class<?>... entities) {
        return configuration(db, entities).property("hibernate.type.java_time_use_direct_jdbc", "true");
    }

    private static String createScript(Class<?>... entities) {
        StringWriter create = new StringWriter();
        try (var scriptOnly = configuration("postgres", entities)
                .property("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect")
                .property("jakarta.persistence.database-product-name", "PostgreSQL")
                .property("jakarta.persistence.database-major-version", "18")
                .property("hibernate.boot.allow_jdbc_metadata_access", "false")
                .property("jakarta.persistence.schema-generation.scripts.action", "create")
                .property("jakarta.persistence.schema-generation.scripts.create-target", create)
                .createEntityManagerFactory()) {
            return create.toString();
        }
    }

    private static List<String> inlineForeignKeys(String script) {
        Map<String, String> tables = new LinkedHashMap<>();
        Map<String, List<String>> keys = new HashMap<>();
        Map<String, Set<String>> references = new HashMap<>();
        List<String> before = new ArrayList<>();
        List<String> after = new ArrayList<>();
        for (String sql : script.split("\n")) {
            Matcher table = CREATE_TABLE.matcher(sql);
            Matcher key = ADD_FOREIGN_KEY.matcher(sql);
            if (table.matches()) {
                tables.put(table.group(1), table.group(2));
            } else if (key.matches()) {
                keys.computeIfAbsent(key.group(1), k -> new ArrayList<>()).add(
                        "constraint " + key.group(2) + " foreign key (" + key.group(3) + ") references " + key.group(4));
                if (!key.group(1).equals(key.group(4))) {
                    references.computeIfAbsent(key.group(1), k -> new LinkedHashSet<>()).add(key.group(4));
                }
            } else if (!sql.isBlank()) {
                (tables.isEmpty() ? before : after).add(sql);
            }
        }
        List<String> out = new ArrayList<>(before);
        Set<String> created = new LinkedHashSet<>();
        while (created.size() < tables.size()) {
            for (var t : tables.entrySet()) {
                if (!created.contains(t.getKey())
                        && created.containsAll(references.getOrDefault(t.getKey(), Set.of()))) {
                    StringBuilder body = new StringBuilder(t.getValue());
                    for (String key : keys.getOrDefault(t.getKey(), List.of())) {
                        body.append(", ").append(key);
                    }
                    out.add("create table " + t.getKey() + " (" + body + ")");
                    created.add(t.getKey());
                }
            }
        }
        out.addAll(after);
        return out;
    }

    @BeforeAll
    static void setUp() throws Exception {
        database = Target.createDatabase("hibernate");
        try (Connection conn = Target.connect(database)) {
            Target.execute(conn, inlineForeignKeys(createScript(ENTITIES)).toArray(String[]::new));
        }
        sf = directTimeBinding(database, ENTITIES).createEntityManagerFactory();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (sf != null) {
            sf.close();
        }
        Target.dropDatabase(database);
        Target.dropDatabase(Target.databaseName("hibernate_stock"));
    }

    @Test
    void stockSchemaExportAndDrop() throws Throwable {
        Target.xfailOnSereneDB("Hibernate adds foreign keys with ALTER TABLE (#1427)", () -> {
            String stock = Target.createDatabase("hibernate_stock");
            try (var exported = configuration(stock, ENTITIES)
                    .property("jakarta.persistence.schema-generation.database.action", "create")
                    .createEntityManagerFactory()) {
                exported.getSchemaManager().validate();
                exported.inTransaction(s -> s.persist(new Author("Stocky")));
                exported.getSchemaManager().drop(true);
            }
        });
    }

    @Test
    void identityGeneratedIds() throws Throwable {
        Target.xfailOnSereneDB("no identity columns (#1426)", () -> {
            String stock = Target.createDatabase("hibernate_stock");
            try (var identity = configuration(stock, Counter.class)
                    .property("jakarta.persistence.schema-generation.database.action", "drop-and-create")
                    .createEntityManagerFactory()) {
                Counter first = new Counter();
                Counter second = new Counter();
                identity.inTransaction(s -> {
                    s.persist(first);
                    s.persist(second);
                });
                assertEquals(first.id + 1, second.id);
            }
        });
    }

    @Test
    void schemaValidates() throws Exception {
        sf.getSchemaManager().validate();
    }

    @Test
    void schemaUpdateIsNoOp() {
        StringWriter update = new StringWriter();
        try (var migrator = configuration(database, ENTITIES)
                .property("hibernate.schema_update.unique_constraint_strategy", "SKIP")
                .property("hibernate.hbm2ddl.scripts.action", "update")
                .property("jakarta.persistence.schema-generation.scripts.create-target", update)
                .createEntityManagerFactory()) {
            assertEquals("", update.toString());
        }
    }

    @Test
    void defaultBindingOfBooleansAndDates() throws Throwable {
        Target.xfailOnSereneDB("bind parameters reject TRUE/FALSE and dates with a zone offset", () -> {
            try (var stock = configuration(database, Notice.class).createEntityManagerFactory()) {
                Notice notice = new Notice();
                notice.pinned = true;
                notice.day = LocalDate.of(2024, 2, 29);
                stock.inTransaction(s -> s.persist(notice));
                Notice read = stock.fromTransaction(s -> s.find(Notice.class, notice.id));
                assertTrue(read.pinned);
                assertEquals(LocalDate.of(2024, 2, 29), read.day);
            }
        });
    }

    @Test
    void timeColumns() throws Throwable {
        Target.xfailOnSereneDB("time(p) is rejected (#1434)", () -> {
            try (Connection conn = Target.connect(database)) {
                Target.execute(conn, inlineForeignKeys(createScript(Shift.class)).toArray(String[]::new));
            }
            try (var shifts = directTimeBinding(database, Shift.class).createEntityManagerFactory()) {
                Shift shift = new Shift();
                shift.start = LocalTime.of(7, 8, 9);
                shifts.inTransaction(s -> s.persist(shift));
                assertEquals(LocalTime.of(7, 8, 9),
                        shifts.fromTransaction(s -> s.find(Shift.class, shift.id)).start);
            }
        });
    }

    @Test
    void crudRoundTripsEveryColumnType() {
        OffsetDateTime published = OffsetDateTime.of(2024, 5, 6, 7, 8, 9, 123456000, ZoneOffset.UTC);
        UUID uid = UUID.randomUUID();
        Long bookId = sf.fromTransaction(s -> {
            Author author = new Author("Pudge");
            author.born = LocalDate.of(2004, 2, 1);
            author.address = new Address("Hook Lane 1", "Dire");
            author.aliases.addAll(Set.of("Butcher", "Meat"));
            Book book = author.write("Fresh Meat");
            book.price = new BigDecimal("12.50");
            book.pages = 321;
            book.published = published;
            book.genre = Genre.HORROR;
            book.uid = uid;
            book.cover = new byte[] {0, 1, 2, (byte) 255};
            book.readingTime = Duration.ofHours(3).plusSeconds(7);
            book.tags.add(new Tag("gore"));
            s.persist(book.tags.iterator().next());
            s.persist(author);
            return book.id;
        });
        sf.inTransaction(s -> {
            Book book = s.find(Book.class, bookId);
            assertEquals("Fresh Meat", book.title);
            assertEquals(new BigDecimal("12.50"), book.price);
            assertEquals(published.toInstant(), book.published.toInstant());
            assertEquals(Genre.HORROR, book.genre);
            assertEquals(uid, book.uid);
            assertArrayEquals(new byte[] {0, 1, 2, (byte) 255}, book.cover);
            assertEquals(Duration.ofHours(3).plusSeconds(7), book.readingTime);
            assertEquals("Pudge", book.author.name);
            assertEquals("Dire", book.author.address.city);
            assertEquals(Set.of("Butcher", "Meat"), book.author.aliases);
            assertEquals(Set.of("gore"), book.tags.stream().map(t -> t.label).collect(java.util.stream.Collectors.toSet()));
            book.pages = 322;
        });
        assertEquals(322, sf.<Integer>fromTransaction(s -> s.find(Book.class, bookId).pages));
    }

    @Test
    void optimisticLockingBumpsVersion() {
        Long id = sf.fromTransaction(s -> {
            Author author = new Author("Axe");
            s.persist(author);
            return author.id;
        });
        Author stale = sf.fromTransaction(s -> s.find(Author.class, id));
        sf.inTransaction(s -> s.find(Author.class, id).born = LocalDate.of(1990, 1, 1));
        assertEquals(1, sf.<Integer>fromTransaction(s -> s.find(Author.class, id).version));
        stale.born = LocalDate.of(2000, 1, 1);
        assertThrows(OptimisticLockException.class, () -> sf.inTransaction(s -> s.merge(stale)));
    }

    @Test
    void hqlQueries() {
        sf.inTransaction(s -> {
            Author lina = new Author("Lina");
            for (int i = 0; i < 6; i++) {
                Book book = lina.write("Laguna " + i);
                book.pages = i * 100;
                book.price = BigDecimal.valueOf(i * 3 + 1);
                book.genre = i % 2 == 0 ? Genre.FANTASY : Genre.POETRY;
            }
            s.persist(lina);
        });
        sf.inTransaction(s -> {
            assertEquals(6L, s.createSelectionQuery(
                    "select count(b) from Book b join b.author a where a.name = :name", Long.class)
                    .setParameter("name", "Lina").getSingleResult());
            List<Object[]> perGenre = s.createSelectionQuery(
                    "select b.genre, count(b), sum(b.pages), max(b.price) from Book b where b.author.name = 'Lina'"
                            + " group by b.genre having count(b) > 1 order by b.genre", Object[].class)
                    .getResultList();
            assertEquals(2, perGenre.size());
            assertEquals(Genre.FANTASY, perGenre.get(0)[0]);
            assertEquals(600L, perGenre.get(0)[2]);
            List<String> page = s.createSelectionQuery(
                    "select b.title from Book b where b.author.name = 'Lina' order by b.pages desc", String.class)
                    .setFirstResult(1).setMaxResults(2).getResultList();
            assertEquals(List.of("Laguna 4", "Laguna 3"), page);
            assertEquals("LAGUNA 5|laguna 5|8|Lag|L5", s.createSelectionQuery(
                    "select upper(b.title) || '|' || lower(b.title) || '|' || length(b.title) || '|'"
                            + " || substring(b.title, 1, 3) || '|' || concat(left(b.title, 1), right(b.title, 1))"
                            + " from Book b where b.pages = 500", String.class).getSingleResult());
            assertEquals(List.of("Laguna 0", "Laguna 1"), s.createSelectionQuery(
                    "select b.title from Book b where b.pages in :pages and b.author.name = 'Lina'"
                            + " order by b.title", String.class)
                    .setParameter("pages", List.of(0, 100)).getResultList());
            assertEquals(2L, s.createSelectionQuery(
                    "select count(b) from Book b where b.price > (select avg(x.price) from Book x"
                            + " where x.author = b.author) + 4 and b.author.name = 'Lina'", Long.class)
                    .getSingleResult());
            assertEquals("big", s.createSelectionQuery(
                    "select case when b.pages >= 300 then 'big' else 'small' end from Book b"
                            + " where b.title = 'Laguna 4'", String.class).getSingleResult());
            Author fetched = s.createSelectionQuery(
                    "select distinct a from Author a left join fetch a.books where a.name = 'Lina'", Author.class)
                    .getSingleResult();
            assertEquals(6, fetched.books.size());
            assertEquals("Laguna 0", fetched.books.get(0).title);
            assertEquals(new Summary("Laguna 2", 200), s.createSelectionQuery(
                    "select b.title, b.pages from Book b where b.pages = 200", Summary.class)
                    .getSingleResult());
            assertEquals(LocalDate.now().getYear(), s.createSelectionQuery(
                    "select year(current_date)", Integer.class).getSingleResult());
        });
        int updated = sf.fromTransaction(s -> s.createMutationQuery(
                "update Book b set b.price = b.price * 2 where b.genre = :genre and b.title like 'Laguna%'")
                .setParameter("genre", Genre.POETRY).executeUpdate());
        assertEquals(3, updated);
    }

    @Test
    void bulkUpdateThroughAssociation() throws Throwable {
        sf.inTransaction(s -> {
            Author enigma = new Author("Enigma");
            enigma.write("Black Hole").pages = 1;
            s.persist(enigma);
        });
        Target.xfailOnSereneDB("no ctid system column, used for UPDATE ... FROM with a join", () ->
                assertEquals(1, sf.<Integer>fromTransaction(s -> s.createMutationQuery(
                        "update Book b set b.pages = 2 where b.author.name = 'Enigma'").executeUpdate())));
    }

    @Test
    void fullRowUpdateOfReferencedRow() throws Throwable {
        Long id = sf.fromTransaction(s -> {
            Guild guild = new Guild();
            guild.name = "Radiant";
            Hero hero = new Hero();
            hero.guild = guild;
            s.persist(guild);
            s.persist(hero);
            return guild.id;
        });
        Target.xfailOnSereneDB("UPDATE setting a UNIQUE column of a row a foreign key references fails as"
                + " 'still referenced', even to the same value", () -> {
            sf.inTransaction(s -> s.find(Guild.class, id).motto = "Defend the Ancient");
            assertEquals("Defend the Ancient", sf.fromTransaction(s -> s.find(Guild.class, id)).motto);
        });
    }

    @Test
    void criteriaQueries() {
        sf.inTransaction(s -> {
            Author tiny = new Author("Tiny");
            tiny.write("Avalanche").pages = 10;
            tiny.write("Toss").pages = 20;
            s.persist(tiny);
        });
        sf.inTransaction(s -> {
            CriteriaBuilder cb = s.getCriteriaBuilder();
            CriteriaQuery<Book> q = cb.createQuery(Book.class);
            Root<Book> book = q.from(Book.class);
            Join<Book, Author> author = book.join("author");
            q.select(book).where(cb.equal(author.get("name"), "Tiny"), cb.gt(book.get("pages"), 15))
                    .orderBy(cb.desc(book.get("title")));
            assertEquals(List.of("Toss"), s.createQuery(q).getResultList().stream().map(b -> b.title).toList());
            CriteriaQuery<Long> count = cb.createQuery(Long.class);
            Root<Book> counted = count.from(Book.class);
            count.select(cb.count(counted)).where(cb.like(counted.get("title"), "A%"),
                    cb.equal(counted.get("author").get("name"), "Tiny"));
            assertEquals(1L, s.createQuery(count).getSingleResult());
        });
    }

    @Test
    void batchedInsertsAndBulkDelete() {
        sf.inTransaction(s -> {
            for (int i = 0; i < 100; i++) {
                s.persist(new Tag("batch-" + i));
                if (i % 20 == 19) {
                    s.flush();
                    s.clear();
                }
            }
        });
        assertEquals(100L, sf.<Long>fromTransaction(s -> s.createSelectionQuery(
                "select count(t) from Tag t where t.label like 'batch-%'", Long.class).getSingleResult()));
        assertEquals(100, sf.<Integer>fromTransaction(s -> s.createMutationQuery(
                "delete from Tag t where t.label like 'batch-%'").executeUpdate()));
    }

    @Test
    void inheritanceStrategies() {
        sf.inTransaction(s -> {
            Podcast podcast = new Podcast();
            podcast.title = "Roshan Pit";
            podcast.host = "Tidehunter";
            Video video = new Video();
            video.title = "Ravage";
            video.minutes = 4;
            Car car = new Car();
            car.wheels = 4;
            car.doors = 5;
            Bike bike = new Bike();
            bike.wheels = 2;
            bike.gears = 21;
            CardPayment card = new CardPayment();
            card.amount = new BigDecimal("9.99");
            card.card = "visa";
            CashPayment cash = new CashPayment();
            cash.amount = new BigDecimal("5.00");
            cash.currency = "gold";
            for (Object o : List.of(podcast, video, car, bike, card, cash)) {
                s.persist(o);
            }
        });
        sf.inTransaction(s -> {
            assertEquals(List.of("Ravage", "Roshan Pit"), s.createSelectionQuery(
                    "select m.title from Media m order by m.title", String.class).getResultList());
            assertEquals("Tidehunter", s.createSelectionQuery(
                    "from Podcast", Podcast.class).getSingleResult().host);
            assertEquals(List.of(2, 4), s.createSelectionQuery(
                    "select v.wheels from Vehicle v order by v.wheels", Integer.class).getResultList());
            assertTrue(s.createSelectionQuery("from Vehicle v where type(v) = Bike", Vehicle.class)
                    .getSingleResult() instanceof Bike);
            assertEquals(new BigDecimal("14.99"), s.createSelectionQuery(
                    "select sum(p.amount) from Payment p", BigDecimal.class).getSingleResult());
            assertEquals(List.of(CardPayment.class, CashPayment.class), s.createSelectionQuery(
                    "from Payment p order by p.amount desc", Payment.class).getResultList()
                    .stream().map(Object::getClass).toList());
        });
    }

    @Test
    void manyToManyAndElementCollections() {
        Long id = sf.fromTransaction(s -> {
            Tag a = new Tag("m2m-a");
            Tag b = new Tag("m2m-b");
            s.persist(a);
            s.persist(b);
            Author author = new Author("Ogre Magi");
            author.aliases.add("Aggron");
            Book book = author.write("Multicast");
            book.tags.addAll(Set.of(a, b));
            s.persist(author);
            return book.id;
        });
        sf.inTransaction(s -> {
            Book book = s.find(Book.class, id);
            assertEquals(2, book.tags.size());
            book.tags.removeIf(t -> t.label.equals("m2m-a"));
            book.author.aliases.clear();
        });
        sf.inTransaction(s -> {
            Book book = s.find(Book.class, id);
            assertEquals(1, book.tags.size());
            assertTrue(book.author.aliases.isEmpty());
        });
    }

    @Test
    void rollbackDiscardsChanges() {
        assertThrows(IllegalStateException.class, () -> sf.inTransaction(s -> {
            s.persist(new Author("Rolled Back"));
            s.flush();
            throw new IllegalStateException();
        }));
        assertNull(sf.fromTransaction(s -> s.createSelectionQuery(
                "from Author a where a.name = 'Rolled Back'", Author.class).getSingleResultOrNull()));
    }

    @Test
    void nativeQueries() {
        sf.inTransaction(s -> {
            assertEquals(5, ((Number) s.createNativeQuery("select 2 + 3", Object.class).getSingleResult()).intValue());
            s.createNativeMutationQuery("insert into Tag (id, label) values (nextval('tag_seq'), :label)")
                    .setParameter("label", "native").executeUpdate();
            assertNotNull(s.createSelectionQuery("from Tag t where t.label = 'native'", Tag.class).getSingleResult());
        });
    }

    @Test
    void pessimisticLocking() throws Throwable {
        Long id = sf.fromTransaction(s -> {
            Author author = new Author("Bristleback");
            s.persist(author);
            return author.id;
        });
        Target.xfailOnSereneDB("no SELECT ... FOR UPDATE (#1431)", () -> sf.inTransaction(s -> {
            Author locked = s.find(Author.class, id, LockModeType.PESSIMISTIC_WRITE);
            locked.born = LocalDate.of(1999, 9, 9);
        }));
    }

    @Test
    void orphanRemovalDeletesChildThenParent() throws Throwable {
        Long id = sf.fromTransaction(s -> {
            Author author = new Author("Undying");
            author.write("Tombstone");
            author.write("Decay");
            s.persist(author);
            return author.id;
        });
        Target.xfailOnSereneDB("deleting a parent row after its children in one transaction (#1433)", () -> {
            sf.inTransaction(s -> s.remove(s.find(Author.class, id)));
            assertNull(sf.fromTransaction(s -> s.find(Author.class, id)));
        });
    }

    @Test
    void schemaTruncateResynchronizeAndDrop() throws Exception {
        String scratch = Target.createDatabase("hibernate_truncate");
        try {
            try (Connection conn = Target.connect(scratch)) {
                Target.execute(conn, inlineForeignKeys(createScript(ENTITIES)).toArray(String[]::new));
            }
            try (var truncated = directTimeBinding(scratch, ENTITIES).createEntityManagerFactory()) {
                truncated.inTransaction(s -> s.persist(new Tag("gone")));
                truncated.getSchemaManager().truncate();
                assertEquals(0L, truncated.<Long>fromTransaction(s -> s.createSelectionQuery(
                        "select count(t) from Tag t", Long.class).getSingleResult()));
                truncated.getSchemaManager().resynchronizeGenerators();
                truncated.inTransaction(s -> s.persist(new Tag("back")));
            }
        } finally {
            Target.dropDatabase(scratch);
        }
    }

    @Test
    void schemaDropRemovesEveryTable() throws Exception {
        String scratch = Target.createDatabase("hibernate_drop");
        try {
            try (Connection conn = Target.connect(scratch)) {
                Target.execute(conn, inlineForeignKeys(createScript(ENTITIES)).toArray(String[]::new));
            }
            try (var dropped = directTimeBinding(scratch, ENTITIES).createEntityManagerFactory()) {
                dropped.getSchemaManager().drop(false);
            }
            try (Connection conn = Target.connect(scratch); Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_class c JOIN pg_namespace n"
                         + " ON n.oid = c.relnamespace WHERE n.nspname = 'public'")) {
                rs.next();
                assertEquals(0, rs.getInt(1));
            }
        } finally {
            Target.dropDatabase(scratch);
        }
    }
}
