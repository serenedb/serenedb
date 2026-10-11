package com.serenedb.drivers;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

public final class Json {

    public record Table(List<String> columns, List<List<Object>> rows) {

        public Table plus(Table other) {
            List<List<Object>> all = new ArrayList<>(rows);
            all.addAll(other.rows);
            return new Table(columns, all);
        }

        public Table sorted() {
            List<List<Object>> all = new ArrayList<>(rows);
            all.sort(Comparator.comparing(Json::value));
            return new Table(columns, all);
        }

        public String render() {
            StringBuilder out = new StringBuilder("{\n \"columns\": ").append(value(columns))
                    .append(",\n \"rows\": [");
            for (int i = 0; i < rows.size(); i++) {
                out.append(i == 0 ? "\n  " : ",\n  ").append(value(rows.get(i)));
            }
            return out.append(rows.isEmpty() ? "]\n}\n" : "\n ]\n}\n").toString();
        }
    }

    public static Table read(ResultSet rs, BinaryOperator<String> normalize) throws SQLException {
        try (rs) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                columns.add(md.getColumnLabel(i));
            }
            List<List<Object>> rows = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columns.size(); i++) {
                    Object v = plain(rs.getObject(i));
                    row.add(v instanceof String s ? normalize.apply(columns.get(i - 1), s) : v);
                }
                rows.add(row);
            }
            return new Table(columns, rows);
        }
    }

    public static String object(Map<String, ?> map) {
        StringBuilder out = new StringBuilder("{");
        String sep = "\n ";
        for (var e : new TreeMap<>(map).entrySet()) {
            out.append(sep).append(quote(e.getKey())).append(": ").append(value(e.getValue()));
            sep = ",\n ";
        }
        return out.append("\n}\n").toString();
    }

    private static Object plain(Object v) throws SQLException {
        if (v instanceof Array a) {
            return Arrays.asList((Object[]) a.getArray());
        }
        if (v instanceof byte[] b) {
            return Arrays.toString(b);
        }
        return v;
    }

    private static String value(Object v) {
        if (v == null || v instanceof Boolean || v instanceof Number) {
            return String.valueOf(v);
        }
        if (v instanceof List<?> list) {
            StringBuilder out = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                out.append(i == 0 ? "" : ", ").append(value(list.get(i)));
            }
            return out.append(']').toString();
        }
        return quote(v.toString());
    }

    private static String quote(String s) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }

    private Json() {}
}
