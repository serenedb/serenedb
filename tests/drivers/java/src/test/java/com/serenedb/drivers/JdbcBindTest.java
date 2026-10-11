package com.serenedb.drivers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcBindTest {

    private interface Setter {
        void bind(PreparedStatement ps) throws SQLException;
    }

    record Case(String name, String sql, Setter setter, String expected, String gap) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static final UUID UID = UUID.fromString("6e6e4fc0-fa3b-4222-b45d-4b9c5a90f924");

    private static final String BIND_GAP = "bind parameters reject TRUE/FALSE and dates or times with a zone offset";

    static List<Case> cases() {
        return List.of(
                new Case("setBoolean(true)", "SELECT ?::bool", ps -> ps.setBoolean(1, true), "t", BIND_GAP),
                new Case("setBoolean(false)", "SELECT ?::bool", ps -> ps.setBoolean(1, false), "f", BIND_GAP),
                new Case("setObject(Boolean)", "SELECT ?::bool", ps -> ps.setObject(1, Boolean.TRUE), "t", BIND_GAP),
                new Case("setDate", "SELECT ?::date", ps -> ps.setDate(1, java.sql.Date.valueOf("2004-02-01")),
                        "2004-02-01", BIND_GAP),
                new Case("setTime", "SELECT ?::time", ps -> ps.setTime(1, Time.valueOf("07:08:09")),
                        "07:08:09", BIND_GAP),
                new Case("setObject(LocalDate)", "SELECT ?::date", ps -> ps.setObject(1, LocalDate.of(2004, 2, 1)),
                        "2004-02-01", null),
                new Case("setObject(LocalTime)", "SELECT ?::time", ps -> ps.setObject(1, LocalTime.of(7, 8, 9)),
                        "07:08:09", null),
                new Case("setObject(LocalDateTime)", "SELECT ?::timestamp",
                        ps -> ps.setObject(1, LocalDateTime.of(2024, 5, 6, 7, 8, 9)), "2024-05-06 07:08:09", null),
                new Case("setObject(OffsetDateTime)", "SELECT ?::timestamptz",
                        ps -> ps.setObject(1, OffsetDateTime.of(2024, 5, 6, 7, 8, 9, 0, ZoneOffset.ofHours(3))),
                        "2024-05-06 04:08:09+00", null),
                new Case("setTimestamp", "SELECT ?::timestamp",
                        ps -> ps.setTimestamp(1, Timestamp.valueOf("2024-05-06 07:08:09.5")), "2024-05-06 07:08:09.5", null),
                new Case("setObject(UUID)", "SELECT ?::uuid", ps -> ps.setObject(1, UID), UID.toString(), null),
                new Case("setBigDecimal", "SELECT ?::numeric(10,2)", ps -> ps.setBigDecimal(1, new BigDecimal("12.5")),
                        "12.50", null),
                new Case("setBigDecimal(bare numeric)", "SELECT ?::numeric",
                        ps -> ps.setBigDecimal(1, new BigDecimal("12.50")), "12.50",
                        "bare numeric is numeric(18,3) (#1380)"),
                new Case("setShort", "SELECT ?::int2", ps -> ps.setShort(1, (short) -7), "-7", null),
                new Case("setInt", "SELECT ? + 1", ps -> ps.setInt(1, 41), "42", null),
                new Case("setLong", "SELECT ?::int8", ps -> ps.setLong(1, 1L << 40), "1099511627776", null),
                new Case("setFloat", "SELECT ?::float4", ps -> ps.setFloat(1, 1.5f), "1.5", null),
                new Case("setDouble", "SELECT ?::float8", ps -> ps.setDouble(1, 2.25), "2.25", null),
                new Case("setString", "SELECT ? || '!'", ps -> ps.setString(1, "hook"), "hook!", null),
                new Case("setBytes", "SELECT ?::bytea", ps -> ps.setBytes(1, new byte[] {0, 1, (byte) 255}),
                        "\\x0001ff", null),
                new Case("setNull", "SELECT ?::int4 IS NULL", ps -> ps.setNull(1, Types.INTEGER), "t", null),
                new Case("setArray(int4)", "SELECT ?::int4[]",
                        ps -> ps.setArray(1, ps.getConnection().createArrayOf("int4", new Integer[] {1, 2, 3})),
                        "{1,2,3}", null),
                new Case("setArray(text)", "SELECT ?::text[]",
                        ps -> ps.setArray(1, ps.getConnection().createArrayOf("text", new String[] {"a", "b c"})),
                        "{a,\"b c\"}", null),
                new Case("setObject(String, OTHER)", "SELECT ?::json",
                        ps -> ps.setObject(1, "{\"a\": 1}", Types.OTHER), "{\"a\": 1}", null));
    }

    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        conn = Target.admin();
        Target.execute(conn, "SET TimeZone = 'UTC'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        conn.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void bind(Case c) throws Throwable {
        if (c.gap() == null) {
            assertEquals(c.expected(), roundTrip(c));
        } else {
            Target.xfailOnSereneDB(c.gap(), () -> assertEquals(c.expected(), roundTrip(c)));
        }
    }

    private static String roundTrip(Case c) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(c.sql())) {
            c.setter().bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}
