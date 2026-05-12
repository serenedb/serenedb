package com.serenedb.drivers;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

/** Mirrors tests/drivers/harness/spec_loader.py for Java. */
public final class SpecLoader {

    public static final List<String> PROTOCOLS = List.of(
            "simple", "extended-noparam", "extended-text", "extended-binary");

    public record TypeSpec(
            int oid,
            String name,
            String pgTypname,
            String castSql,
            boolean text,
            boolean binary,
            List<Object> samples,
            Map<String, List<String>> skip) {

        public boolean isSkipped(String driverKey, String protocol) {
            List<String> modes = skip.getOrDefault(driverKey, List.of());
            return modes.contains("all") || modes.contains(protocol);
        }

        public boolean supports(String protocol) {
            return "extended-binary".equals(protocol) ? binary : text;
        }
    }

    public record Case(TypeSpec type, String protocol, int sampleIdx, Object sample) {}

    public static List<TypeSpec> load() throws IOException {
        Path spec = Path.of(System.getProperty("test.spec"), "types.yaml");
        try (var in = Files.newBufferedReader(spec)) {
            List<Map<String, Object>> raw = new Yaml().load(in);
            List<TypeSpec> out = new ArrayList<>(raw.size());
            for (var e : raw) {
                String name = (String) e.getOrDefault("name", e.get("pg_typname"));
                String pgTypname = (String) e.getOrDefault("pg_typname", name);
                String castSql = (String) e.getOrDefault("cast_sql", "$1::" + pgTypname);
                List<Object> samples = (List<Object>) e.getOrDefault("samples", List.of());
                if (samples == null) samples = List.of();
                Map<String, List<String>> skip = new HashMap<>();
                Object skipRaw = e.get("skip");
                if (skipRaw instanceof Map<?, ?> skipMap) {
                    for (var entry : skipMap.entrySet()) {
                        skip.put(String.valueOf(entry.getKey()),
                                (List<String>) entry.getValue());
                    }
                }
                out.add(new TypeSpec(
                        ((Number) e.get("oid")).intValue(),
                        name,
                        pgTypname,
                        castSql,
                        Boolean.TRUE.equals(e.get("text")),
                        Boolean.TRUE.equals(e.get("binary")),
                        samples,
                        skip));
            }
            return out;
        }
    }

    public static List<Case> casesFor(String driverKey) throws IOException {
        Pattern typeRegex = Pattern.compile(
                System.getProperty("test.types", ".*"));
        List<String> protos = Arrays.asList(
                System.getProperty("test.protocols",
                        String.join(",", PROTOCOLS)).split(","));
        List<Case> out = new ArrayList<>();
        for (var ts : load()) {
            if (!typeRegex.matcher(ts.name()).find()) continue;
            if (!ts.text() && !ts.binary()) continue;
            for (var p : protos) {
                if (!PROTOCOLS.contains(p)) continue;
                if (!ts.supports(p)) continue;
                if (ts.isSkipped(driverKey, p)) continue;
                for (int i = 0; i < ts.samples().size(); i++) {
                    out.add(new Case(ts, p, i, ts.samples().get(i)));
                }
            }
        }
        return out;
    }

    public static String schemaName(String driverKey) {
        return "drv_" + driverKey + "_" + System.getProperty("test.run_id", "0");
    }

    private SpecLoader() {}
}
