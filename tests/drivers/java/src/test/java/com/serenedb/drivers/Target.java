package com.serenedb.drivers;

import org.junit.jupiter.api.function.Executable;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class Target {

    private static Boolean serenedb;

    public static String host() {
        return System.getProperty("test.host", "localhost");
    }

    public static String port() {
        return System.getProperty("test.port", "5432");
    }

    public static String user() {
        return System.getProperty("test.user", "postgres");
    }

    public static String url(String database) {
        return "jdbc:postgresql://" + host() + ":" + port() + "/" + database;
    }

    public static Properties credentials() {
        Properties props = new Properties();
        props.setProperty("user", user());
        String password = System.getenv("SDB_DRV_PASSWORD");
        if (password != null) {
            props.setProperty("password", password);
        }
        return props;
    }

    public static Connection connect(String database) throws SQLException {
        return DriverManager.getConnection(url(database), credentials());
    }

    public static DataSource dataSource(String database) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(url(database));
        ds.setUser(user());
        ds.setPassword(System.getenv("SDB_DRV_PASSWORD"));
        return ds;
    }

    public static Connection admin() throws SQLException {
        return connect(System.getProperty("test.database", "postgres"));
    }

    public static synchronized boolean isSereneDB() throws SQLException {
        if (serenedb == null) {
            try (Connection conn = admin();
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT version()")) {
                rs.next();
                serenedb = rs.getString(1).contains("SereneDB");
            }
        }
        return serenedb;
    }

    public static String databaseName(String suite) {
        return "java_" + suite + "_" + System.getProperty("test.run_id", "0");
    }

    public static String createDatabase(String suite) throws SQLException {
        String name = databaseName(suite);
        try (Connection conn = admin(); Statement st = conn.createStatement()) {
            st.execute("DROP DATABASE IF EXISTS " + name);
            st.execute("CREATE DATABASE " + name);
        }
        return name;
    }

    public static void dropDatabase(String name) throws SQLException {
        try (Connection conn = admin(); Statement st = conn.createStatement()) {
            st.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                    + " WHERE datname = '" + name + "' AND pid <> pg_backend_pid()");
            st.execute("DROP DATABASE IF EXISTS " + name);
        }
    }

    public static void execute(Connection conn, String... statements) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String sql : statements) {
                st.execute(sql);
            }
        }
    }

    public static DataSource lowercaseBooleans(DataSource base) {
        return proxy(DataSource.class, base);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, T target) {
        return (T) Proxy.newProxyInstance(Target.class.getClassLoader(), new Class<?>[] {type}, (p, m, args) -> {
            if (target instanceof PreparedStatement ps && args != null && args.length == 2
                    && (m.getName().equals("setBoolean") || m.getName().equals("setObject"))
                    && args[1] instanceof Boolean b) {
                ps.setObject((Integer) args[0], b.toString(), Types.OTHER);
                return null;
            }
            Object result;
            try {
                result = m.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (result instanceof CallableStatement cs) {
                return proxy(CallableStatement.class, cs);
            }
            if (result instanceof PreparedStatement ps) {
                return proxy(PreparedStatement.class, ps);
            }
            if (result instanceof Connection c) {
                return proxy(Connection.class, c);
            }
            return result;
        });
    }

    public static void xfailOnSereneDB(String reason, Executable body) throws Throwable {
        if (!isSereneDB()) {
            body.execute();
            return;
        }
        Throwable failure = assertThrows(Throwable.class, body,
                "passes on SereneDB now, drop the xfail: " + reason);
        System.out.println("XFAIL " + reason + ": " + failure);
    }

    public static Path fixtures(String suite) {
        return Path.of(System.getProperty("test.fixtures")).resolve(suite);
    }

    public static void checkGolden(Path path, String text) throws IOException, SQLException {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        Path twin = path.resolveSibling(
                name.substring(0, dot) + ".serenedb" + name.substring(dot));
        if ("1".equals(System.getenv("SDB_DRV_UPDATE_GOLDENS"))) {
            if (!isSereneDB()) {
                Files.createDirectories(path.getParent());
                Files.writeString(path, text);
            } else if (text.equals(Files.readString(path))) {
                Files.deleteIfExists(twin);
            } else {
                Files.writeString(twin, text);
            }
            return;
        }
        Path expected = isSereneDB() && Files.exists(twin) ? twin : path;
        assertEquals(Files.readString(expected), text, expected.toString());
    }

    private Target() {}
}
