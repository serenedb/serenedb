import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.InetAddressRange;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public final class Bench {
  static final long SEED = 20261019L;
  static final long REPS_BUDGET_NS = 400_000_000L;
  static final int MIN_REPS = 15;
  static final int MAX_REPS = 300;
  static final int SQL_WARMUP = 3;
  static final int LUCENE_WARMUP = 50;
  static final int LUCENE = 0;
  static final int SERENEDB = 1;
  static final int POSTGRES = 2;
  static final long[] WALL = new long[3];
  static final int MERGE = rows("BENCH_MERGE", 0);
  static long sink;
  static int mismatches;

  record Dataset(String name, String file, String column, String sdbType, String pgType, int rows) {
    String table() {
      return "bench_" + name;
    }
  }

  record PgIndex(String kind, String definition) {}

  record PgVariant(String label, List<PgIndex> indexes) {}

  record QueryPair(String name, String predicate, Query lucene) {}

  static final class Result {
    long truth;
    long luceneCount;
    long luceneVisitCount;
    long sdbCount;
    long[] pgCount;
    String sdbPlan;
    String[] pgPlan;
    double lucene;
    double luceneVisit;
    double sdb;
    double[] pg;
    int reps;
  }

  interface Runner {
    long run() throws Exception;
  }

  static final class Visit extends SimpleCollector {
    final int[] window = new int[2048];
    long hits;

    @Override
    public void collect(int doc) {
      window[(int) (hits++ & 2047)] = doc;
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
      stream.forEach(doc -> window[(int) (hits++ & 2047)] = doc);
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  static final CollectorManager<Visit, Long> VISIT = new CollectorManager<>() {
    @Override
    public Visit newCollector() {
      return new Visit();
    }

    @Override
    public Long reduce(Collection<Visit> collectors) {
      return collectors.stream().mapToLong(c -> c.hits).sum();
    }
  };

  static long visit(IndexSearcher searcher, Query q) throws IOException {
    return searcher.search(q, VISIT);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 2 && args[0].equals("prepare")) {
      prepare(Paths.get(args[1]).toAbsolutePath());
    } else if (args.length == 3 && args[0].equals("bench")) {
      bench(Paths.get(args[1]).toAbsolutePath(), args[2]);
    } else {
      throw new IllegalArgumentException("usage: prepare <data_dir> | bench <data_dir> <inet|int|string>");
    }
  }

  static String env(String name) {
    final String v = System.getenv(name);
    if (v == null || v.isEmpty()) {
      throw new IllegalStateException(name + " is not set");
    }
    return v;
  }

  static int rows(String name, int dflt) {
    final String v = System.getenv(name);
    return v == null || v.isEmpty() ? dflt : Integer.parseInt(v);
  }

  static final Dataset HOSTS =
      new Dataset("hosts", "hosts.csv", "ip", "INET", "inet", rows("BENCH_HOSTS", 2_000_000));
  static final Dataset NETWORKS =
      new Dataset("networks", "networks.csv", "net", "CIDR", "cidr", rows("BENCH_NETWORKS", 1_000_000));
  static final Dataset INTS =
      new Dataset("ints", "ints.csv", "v", "INTEGER", "integer", rows("BENCH_INTS", 2_000_000));
  static final Dataset STAMPS =
      new Dataset("stamps", "stamps.csv", "ts", "BIGINT", "bigint", rows("BENCH_STAMPS", 2_000_000));
  static final Dataset LOWCARD =
      new Dataset("lowcard", "lowcard.csv", "v", "INTEGER", "integer", rows("BENCH_LOWCARD", 2_000_000));
  static final Dataset STRINGS = new Dataset(
      "strings", "strings.csv", "s", "VARCHAR", "varchar COLLATE \"C\"", rows("BENCH_STRINGS", 2_000_000));

  static void prepare(Path data) throws IOException {
    Files.createDirectories(data);
    write(data, HOSTS, Bench::host);
    write(data, NETWORKS, Bench::network);
    write(data, INTS, r -> Integer.toString(r.nextInt(1_000_000)));
    write(data, STAMPS, stamps());
    write(data, LOWCARD, lowcard());
    write(data, STRINGS, Bench::word);
  }

  static Function<SplittableRandom, String> stamps() {
    final long[] clock = {1_767_225_600_000L};
    return r -> Long.toString((clock[0] += r.nextInt(20)) + r.nextInt(2_000));
  }

  static Function<SplittableRandom, String> lowcard() {
    final int n = 1000;
    final double[] cdf = new double[n];
    double sum = 0;
    for (int i = 0; i < n; ++i) {
      sum += 1.0 / (i + 1);
      cdf[i] = sum;
    }
    final int[] values = new int[n];
    final SplittableRandom shuffle = new SplittableRandom(SEED);
    for (int i = 0; i < n; ++i) {
      final int j = shuffle.nextInt(i + 1);
      values[i] = values[j];
      values[j] = i;
    }
    final double total = sum;
    return r -> {
      final int i = Arrays.binarySearch(cdf, r.nextDouble() * total);
      return Integer.toString(values[Math.min(n - 1, i < 0 ? -i - 1 : i)]);
    };
  }

  static void write(Path data, Dataset ds, Function<SplittableRandom, String> gen) throws IOException {
    final SplittableRandom r = new SplittableRandom(SEED + ds.name().hashCode());
    final Path out = data.resolve(ds.file());
    try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.US_ASCII)) {
      for (int i = 0; i < ds.rows(); ++i) {
        w.write(gen.apply(r));
        w.write('\n');
      }
    }
    System.out.printf(Locale.ROOT, "wrote %s: %,d rows%n", out, ds.rows());
  }

  static byte[] v6Base() {
    final byte[] a = new byte[16];
    a[0] = 0x20;
    a[1] = 0x01;
    a[2] = 0x0d;
    a[3] = (byte) 0xb8;
    return a;
  }

  static String host(SplittableRandom r) {
    if (r.nextInt(10) == 0) {
      final byte[] a = v6Base();
      a[5] = (byte) r.nextInt(256);
      for (int i = 8; i < 16; ++i) {
        a[i] = (byte) r.nextInt(256);
      }
      return v6Text(a);
    }
    return v4Text(r.nextBoolean() ? (10 << 24) | r.nextInt(1 << 24) : r.nextInt());
  }

  static final int[] V4_MASKS = {8, 12, 16, 18, 20, 22, 24, 28, 32};
  static final int[] V4_WEIGHTS = {1, 2, 10, 5, 15, 15, 40, 7, 5};
  static final int[] V6_MASKS = {32, 48, 56, 64};
  static final int[] V6_WEIGHTS = {1, 4, 2, 3};

  static int pick(SplittableRandom r, int[] values, int[] weights) {
    int x = r.nextInt(Arrays.stream(weights).sum());
    for (int i = 0; i < values.length; ++i) {
      x -= weights[i];
      if (x < 0) {
        return values[i];
      }
    }
    return values[values.length - 1];
  }

  static String network(SplittableRandom r) {
    if (r.nextInt(10) == 0) {
      final int mask = pick(r, V6_MASKS, V6_WEIGHTS);
      final byte[] a = v6Base();
      for (int i = 5; i < 16; ++i) {
        a[i] = (byte) r.nextInt(256);
      }
      return v6Text(network(a, mask)) + "/" + mask;
    }
    final int mask = pick(r, V4_MASKS, V4_WEIGHTS);
    final int v = r.nextInt(10) < 6 ? (10 << 24) | r.nextInt(1 << 24) : r.nextInt();
    return v4Text(v & (int) (0xFFFFFFFFL << (32 - mask))) + "/" + mask;
  }

  static String word(SplittableRandom r) {
    final char[] c = new char[8];
    for (int i = 0; i < c.length; ++i) {
      c[i] = (char) ('a' + r.nextInt(26));
    }
    return new String(c);
  }

  static String v4Text(int v) {
    return ((v >>> 24) & 255) + "." + ((v >>> 16) & 255) + "." + ((v >>> 8) & 255) + "." + (v & 255);
  }

  static String v6Text(byte[] a) {
    try {
      return InetAddress.getByAddress(a).getHostAddress();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static InetAddress addr(String text) {
    try {
      return InetAddress.getByName(text);
    } catch (IOException e) {
      throw new IllegalArgumentException(text, e);
    }
  }

  static byte[] network(byte[] a, int mask) {
    final byte[] out = a.clone();
    for (int i = 0; i < out.length; ++i) {
      final int first = i * 8;
      if (first >= mask) {
        out[i] = 0;
      } else if (first + 8 > mask) {
        out[i] &= (byte) (0xFF << (8 - (mask - first)));
      }
    }
    return out;
  }

  static byte[] broadcast(byte[] a, int mask) {
    final byte[] out = a.clone();
    for (int i = 0; i < out.length; ++i) {
      final int first = i * 8;
      if (first >= mask) {
        out[i] = (byte) 0xFF;
      } else if (first + 8 > mask) {
        out[i] |= (byte) (0xFF >>> (mask - first));
      }
    }
    return out;
  }

  static InetAddress[] cidrRange(String cidr) {
    final int slash = cidr.indexOf('/');
    final byte[] b = addr(slash < 0 ? cidr : cidr.substring(0, slash)).getAddress();
    final int mask = slash < 0 ? b.length * 8 : Integer.parseInt(cidr.substring(slash + 1));
    try {
      return new InetAddress[] {
        InetAddress.getByAddress(network(b, mask)), InetAddress.getByAddress(broadcast(b, mask))
      };
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static Query both(Query a, Query b) {
    return new BooleanQuery.Builder().add(a, Occur.MUST).add(b, Occur.MUST).build();
  }

  static Query not(Query must, Query mustNot) {
    return new BooleanQuery.Builder().add(must, Occur.MUST).add(mustNot, Occur.MUST_NOT).build();
  }

  static List<String> sample(List<String> lines, int k) {
    final List<String> in = new ArrayList<>();
    for (int i = 1; i <= k; ++i) {
      in.add(lines.get((i * 7919) % lines.size()));
    }
    return in;
  }

  static String quoted(List<String> values) {
    return String.join(", ", values.stream().map(s -> "'" + s + "'").toList());
  }

  static List<QueryPair> hostQueries(List<String> lines) {
    final String exact = lines.get(123_457 % lines.size());
    final List<String> in = sample(lines, 10);
    final List<QueryPair> q = new ArrayList<>();
    q.add(new QueryPair("exact", "ip = '" + exact + "'", InetAddressPoint.newExactQuery("ip", addr(exact))));
    q.add(new QueryPair("IN (10)", "ip IN (" + quoted(in) + ")",
        InetAddressPoint.newSetQuery("ip", in.stream().map(Bench::addr).toArray(InetAddress[]::new))));
    q.add(new QueryPair("< 10.1.0.0", "ip < '10.1.0.0'",
        InetAddressPoint.newRangeQuery("ip", addr("0.0.0.0"), addr("10.0.255.255"))));
    q.add(new QueryPair("BETWEEN 10.1.0.0 AND 10.2.255.255", "ip BETWEEN '10.1.0.0' AND '10.2.255.255'",
        InetAddressPoint.newRangeQuery("ip", addr("10.1.0.0"), addr("10.2.255.255"))));
    for (String net : List.of("10.0.0.0/8", "10.1.0.0/16", "10.1.0.0/20", "10.1.2.0/24", "2001:db8:5::/48")) {
      final int slash = net.indexOf('/');
      q.add(new QueryPair("<<= " + net, "ip <<= '" + net + "'", InetAddressPoint.newPrefixQuery(
          "ip", addr(net.substring(0, slash)), Integer.parseInt(net.substring(slash + 1)))));
    }
    return q;
  }

  static List<QueryPair> networkQueries(List<String> lines) {
    final List<QueryPair> q = new ArrayList<>();
    for (String a : List.of("10.1.2.3", "77.88.99.11", "2001:db8:0:5::1")) {
      final InetAddress x = addr(a);
      q.add(new QueryPair(">>= " + a, "net >>= '" + a + "'", InetAddressRange.newContainsQuery("net", x, x)));
    }
    final String some = lines.get(98_765 % lines.size());
    final InetAddress[] s = cidrRange(some);
    q.add(new QueryPair("exact " + some, "net = '" + some + "'",
        both(InetAddressRange.newWithinQuery("net", s[0], s[1]),
            InetAddressRange.newContainsQuery("net", s[0], s[1]))));
    final InetAddress[] r16 = cidrRange("10.1.0.0/16");
    final InetAddress[] r12 = cidrRange("10.16.0.0/12");
    final InetAddress[] r24 = cidrRange("10.1.2.0/24");
    q.add(new QueryPair("<<= 10.1.0.0/16", "net <<= '10.1.0.0/16'",
        InetAddressRange.newWithinQuery("net", r16[0], r16[1])));
    q.add(new QueryPair("<<= 10.16.0.0/12", "net <<= '10.16.0.0/12'",
        InetAddressRange.newWithinQuery("net", r12[0], r12[1])));
    q.add(new QueryPair("<< 10.1.0.0/16", "net << '10.1.0.0/16'",
        not(InetAddressRange.newWithinQuery("net", r16[0], r16[1]),
            InetAddressRange.newContainsQuery("net", r16[0], r16[1]))));
    q.add(new QueryPair(">> 10.1.2.0/24", "net >> '10.1.2.0/24'",
        not(InetAddressRange.newContainsQuery("net", r24[0], r24[1]),
            InetAddressRange.newWithinQuery("net", r24[0], r24[1]))));
    q.add(new QueryPair("&& 10.1.0.0/16", "net && '10.1.0.0/16'",
        InetAddressRange.newIntersectsQuery("net", r16[0], r16[1])));
    return q;
  }

  static final String[] RANGE_PERCENTS = {"0.001", "0.01", "0.1", "1", "10", "30", "50", "90"};

  static boolean isLong(Dataset ds) {
    return ds.sdbType().equals("BIGINT");
  }

  static Query pointRange(Dataset ds, long lo, long hi) {
    return isLong(ds) ? LongPoint.newRangeQuery(ds.column(), lo, hi)
                      : IntPoint.newRangeQuery(ds.column(), (int) lo, (int) Math.min(hi, Integer.MAX_VALUE));
  }

  static Query pointSet(Dataset ds, long[] values) {
    return isLong(ds) ? LongPoint.newSetQuery(ds.column(), values)
                      : IntPoint.newSetQuery(ds.column(), Arrays.stream(values).mapToInt(v -> (int) v).toArray());
  }

  static List<QueryPair> numericQueries(Dataset ds, List<String> lines) {
    final String c = ds.column();
    final long[] sorted = lines.stream().mapToLong(Long::parseLong).sorted().toArray();
    final int last = sorted.length - 1;
    final long exact = Long.parseLong(lines.get(123_457 % lines.size()));
    final List<QueryPair> q = new ArrayList<>();
    q.add(new QueryPair("exact", c + " = " + exact, pointRange(ds, exact, exact)));
    for (int k : new int[] {10, 1000}) {
      final long[] in = sample(lines, k).stream().mapToLong(Long::parseLong).toArray();
      final String inSql = String.join(", ", Arrays.stream(in).mapToObj(Long::toString).toList());
      q.add(new QueryPair("IN (" + k + ")", c + " IN (" + inSql + ")", pointSet(ds, in)));
    }
    for (String percent : RANGE_PERCENTS) {
      final double f = Double.parseDouble(percent) / 100;
      final long lo = sorted[(int) Math.floor(last * (0.5 - f / 2))];
      final long hi = sorted[(int) Math.ceil(last * (0.5 + f / 2))];
      q.add(new QueryPair("range " + percent + "%", c + " BETWEEN " + lo + " AND " + hi, pointRange(ds, lo, hi)));
    }
    q.add(new QueryPair("all", c + " >= " + sorted[0], pointRange(ds, sorted[0], Long.MAX_VALUE)));
    return q;
  }

  static List<QueryPair> stringQueries(List<String> lines) {
    final String exact = lines.get(123_457 % lines.size());
    final List<String> in = sample(lines, 10);
    final String p2 = exact.substring(0, 2);
    final String p3 = exact.substring(0, 3);
    final String p2next = p2.substring(0, 1) + (char) (p2.charAt(1) + 1);
    final List<QueryPair> q = new ArrayList<>();
    q.add(new QueryPair("exact", "s = '" + exact + "'", new TermQuery(new Term("s", exact))));
    q.add(new QueryPair("IN (10)", "s IN (" + quoted(in) + ")",
        new TermInSetQuery("s", in.stream().map(BytesRef::new).toList())));
    q.add(new QueryPair("prefix 3 chars", "s LIKE '" + p3 + "%'", new PrefixQuery(new Term("s", p3))));
    q.add(new QueryPair("prefix 2 chars", "s LIKE '" + p2 + "%'", new PrefixQuery(new Term("s", p2))));
    q.add(new QueryPair("range", "s >= '" + p2 + "' AND s < '" + p2next + "'",
        TermRangeQuery.newStringRange("s", p2, p2next, true, false)));
    return q;
  }

  static List<QueryPair> queries(Dataset ds, List<String> lines) {
    return switch (ds.name()) {
      case "hosts" -> hostQueries(lines);
      case "networks" -> networkQueries(lines);
      case "ints", "stamps", "lowcard" -> numericQueries(ds, lines);
      default -> stringQueries(lines);
    };
  }

  static List<PgVariant> pgVariants(Dataset ds) {
    final PgIndex btree = new PgIndex("btree", "(" + ds.column() + ")");
    return switch (ds.name()) {
      case "hosts", "networks" -> List.of(
          new PgVariant("B-tree + GiST",
              List.of(btree, new PgIndex("gist", "USING gist (" + ds.column() + " inet_ops)"))),
          new PgVariant("B-tree + SP-GiST",
              List.of(btree, new PgIndex("spgist", "USING spgist (" + ds.column() + " inet_ops)"))));
      default -> List.of(new PgVariant("B-tree", List.of(btree)));
    };
  }

  static void addField(Document doc, Dataset ds, String line) {
    switch (ds.name()) {
      case "hosts" -> doc.add(new InetAddressPoint("ip", addr(line)));
      case "networks" -> {
        final InetAddress[] r = cidrRange(line);
        doc.add(new InetAddressRange("net", r[0], r[1]));
      }
      case "ints", "lowcard" -> doc.add(new IntPoint(ds.column(), Integer.parseInt(line)));
      case "stamps" -> doc.add(new LongPoint(ds.column(), Long.parseLong(line)));
      default -> doc.add(new StringField("s", line, Field.Store.NO));
    }
  }

  static void bench(Path data, String type) throws Exception {
    final List<Dataset> sets = switch (type) {
      case "inet" -> List.of(HOSTS, NETWORKS);
      case "int" -> List.of(INTS, STAMPS, LOWCARD);
      case "string" -> List.of(STRINGS);
      default -> throw new IllegalArgumentException("type must be inet, int or string: " + type);
    };
    final StringBuilder out = new StringBuilder();
    final StringBuilder plans = new StringBuilder();
    try (Connection sdb = connect(Integer.parseInt(env("SDB_PORT")));
        Connection pg = connect(Integer.parseInt(env("PG_PORT")))) {
      final double sdbFloor;
      final double pgFloor;
      try (Prepared s = new Prepared(sdb, "SELECT 1"); Prepared p = new Prepared(pg, "SELECT 1")) {
        warm(s);
        warm(p);
        sdbFloor = median(s::count, MAX_REPS);
        pgFloor = median(p::count, MAX_REPS);
      }
      out.append(String.format(Locale.ROOT,
          "`SELECT 1` round trip (client + wire + parse floor, median of %d): SereneDB %s µs, PostgreSQL %s µs.%n%n",
          MAX_REPS, fmt(sdbFloor), fmt(pgFloor)));
      for (Dataset ds : sets) {
        runDataset(data, ds, sdb, pg, out, plans);
      }
    }
    out.append(String.format(Locale.ROOT,
        "Wall time per engine for `%s` (ingest + every query run incl. warm-up and EXPLAIN): "
            + "Lucene %.1f s, SereneDB %.1f s, PostgreSQL %.1f s.%n%n",
        type, WALL[LUCENE] / 1e9, WALL[SERENEDB] / 1e9, WALL[POSTGRES] / 1e9));
    out.append(mismatches == 0
            ? "Every engine returns the reference count (PostgreSQL with index scans off) for every query.\n\n"
            : "**" + mismatches + " queries where an engine differs from the reference count "
                + "(PostgreSQL with index scans off).**\n\n");
    Files.writeString(Paths.get(env("REPORT")), out, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    Files.writeString(Paths.get(env("PLANS")), plans, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    System.out.print(out);
  }

  static Connection connect(int port) throws Exception {
    final long deadline = System.nanoTime() + 120_000_000_000L;
    while (true) {
      try {
        return DriverManager.getConnection(
            "jdbc:postgresql://127.0.0.1:" + port + "/postgres?user=postgres&prepareThreshold=1");
      } catch (SQLException e) {
        if (System.nanoTime() > deadline) {
          throw e;
        }
        Thread.sleep(500);
      }
    }
  }

  static void runDataset(Path data, Dataset ds, Connection sdb, Connection pg, StringBuilder out,
      StringBuilder plans) throws Exception {
    final Path csv = data.resolve(ds.file());
    if (!Files.exists(csv)) {
      throw new IllegalStateException(csv + " is missing: run `run.sh prepare` first");
    }
    final String table = ds.table();
    final String index = table + "_idx";
    final int threads = rows("BENCH_LUCENE_THREADS", 8);

    final Path luceneDir = data.resolve("lucene-" + ds.name());
    deleteRecursively(luceneDir);
    final long l0 = System.nanoTime();
    final List<String> lines = Files.readAllLines(csv, StandardCharsets.US_ASCII);
    final FSDirectory dir = FSDirectory.open(luceneDir);
    luceneIndex(dir, lines, ds, threads);
    final DirectoryReader reader = DirectoryReader.open(dir);
    final double luceneIngest = (System.nanoTime() - l0) / 1e9;
    final Map<String, Long> luceneFiles = new TreeMap<>();
    try (Stream<Path> files = Files.list(luceneDir)) {
      for (Path p : files.toList()) {
        addFile(luceneFiles, p);
      }
    }
    WALL[LUCENE] += System.nanoTime() - l0;

    try (dir; reader; Statement sst = sdb.createStatement(); Statement pst = pg.createStatement()) {
      final IndexSearcher searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);

      final long s0 = System.nanoTime();
      sst.execute("DROP TABLE IF EXISTS " + table);
      sst.execute("CREATE TABLE " + table + " (" + ds.column() + " " + ds.sdbType() + ")");
      sst.execute("CREATE INDEX " + index + " ON " + table + " USING inverted(" + ds.column() + ")");
      final long s1 = System.nanoTime();
      sst.execute("COPY " + table + " FROM '" + csv + "' (FORMAT CSV, HEADER false)");
      sst.execute("VACUUM (REFRESH_TABLE) " + table);
      if (MERGE > 0) {
        sst.execute("SET sdb_compact_target_segments = " + MERGE);
        sst.execute("VACUUM (COMPACT_TABLE) " + table);
      }
      final double sdbIngest = (System.nanoTime() - s1) / 1e9;
      final Map<String, Long> sdbFiles = new TreeMap<>();
      int sdbSegments = 0;
      for (Path d : sdbIndexDirs(sst, Paths.get(env("SDB_DATA")), index)) {
        sdbSegments += liveSegmentFiles(d, sdbFiles);
      }
      WALL[SERENEDB] += System.nanoTime() - s0;

      final long p0 = System.nanoTime();
      pst.execute("DROP TABLE IF EXISTS " + table);
      pst.execute("CREATE TABLE " + table + " (" + ds.column() + " " + ds.pgType() + ")");
      final double pgCopy =
          timed(pst, "COPY " + table + " FROM '" + env("PG_DATA_MOUNT") + "/" + ds.file() + "' (FORMAT csv)");
      final double pgVacuum = timed(pst, "VACUUM (ANALYZE) " + table);
      WALL[POSTGRES] += System.nanoTime() - p0;

      final List<QueryPair> queries = queries(ds, lines);
      final List<PgVariant> variants = pgVariants(ds);
      final Result[] results = new Result[queries.size()];
      final Map<String, Double> buildSec = new HashMap<>();
      final Map<String, Long> sizeBytes = new HashMap<>();
      final Set<String> built = new LinkedHashSet<>();
      for (int v = 0; v < variants.size(); ++v) {
        final PgVariant variant = variants.get(v);
        final long v0 = System.nanoTime();
        for (String kind : List.copyOf(built)) {
          if (variant.indexes().stream().noneMatch(i -> i.kind().equals(kind))) {
            pst.execute("DROP INDEX " + table + "_" + kind);
            built.remove(kind);
          }
        }
        for (PgIndex idx : variant.indexes()) {
          if (built.add(idx.kind())) {
            final String name = table + "_" + idx.kind();
            buildSec.put(idx.kind(), timed(pst, "CREATE INDEX " + name + " ON " + table + " " + idx.definition()));
            sizeBytes.put(idx.kind(), pgRelationSize(pst, name));
          }
        }
        WALL[POSTGRES] += System.nanoTime() - v0;
        for (int i = 0; i < queries.size(); ++i) {
          final QueryPair q = queries.get(i);
          final String sdbSql = "SELECT count(*) FROM " + index + " WHERE " + q.predicate();
          final String pgSql = "SELECT count(*) FROM " + table + " WHERE " + q.predicate();
          if (v == 0) {
            results[i] = measureAll(q, variants.size(), searcher, sdb, sst, sdbSql, pg, pst, pgSql, plans);
          } else {
            measurePg(results[i], v, pg, pst, pgSql, plans);
          }
        }
      }
      sst.execute("DROP TABLE " + table);
      pst.execute("DROP TABLE " + table);

      report(out, ds, lines.size(), threads, reader.leaves().size(), luceneIngest, luceneFiles, sdbIngest, sdbFiles,
          sdbSegments, pgCopy, pgVacuum, variants, buildSec, sizeBytes, queries, results);
    }
    deleteRecursively(luceneDir);
  }

  static void luceneIndex(FSDirectory dir, List<String> lines, Dataset ds, int threads) throws Exception {
    final IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setRAMBufferSizeMB(512);
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        final List<Future<?>> parts = new ArrayList<>();
        final int chunk = (lines.size() + threads - 1) / threads;
        for (int t = 0; t < threads; ++t) {
          final int from = t * chunk;
          final int to = Math.min(lines.size(), from + chunk);
          parts.add(pool.submit(() -> {
            for (int i = from; i < to; ++i) {
              final Document doc = new Document();
              addField(doc, ds, lines.get(i));
              writer.addDocument(doc);
            }
            return null;
          }));
        }
        for (Future<?> f : parts) {
          f.get();
        }
      } finally {
        pool.shutdown();
      }
      if (MERGE > 0) {
        writer.forceMerge(MERGE);
      }
      writer.commit();
    }
  }

  static Result measureAll(QueryPair q, int variants, IndexSearcher searcher, Connection sdb, Statement sst,
      String sdbSql, Connection pg, Statement pst, String pgSql, StringBuilder plans) throws Exception {
    final Result r = new Result();
    r.pgCount = new long[variants];
    r.pgPlan = new String[variants];
    r.pg = new double[variants];
    try (Prepared s = new Prepared(sdb, sdbSql); Prepared p = new Prepared(pg, pgSql)) {
      long a = System.nanoTime();
      r.luceneCount = searcher.count(q.lucene());
      r.luceneVisitCount = visit(searcher, q.lucene());
      for (int i = 0; i < LUCENE_WARMUP; ++i) {
        sink += searcher.count(q.lucene());
        sink += visit(searcher, q.lucene());
      }
      WALL[LUCENE] += System.nanoTime() - a;

      a = System.nanoTime();
      r.sdbPlan = sdbPlan(explain(sst, sdbSql, "SereneDB", plans));
      r.sdbCount = s.count();
      final long sdbProbe = warm(s);
      WALL[SERENEDB] += System.nanoTime() - a;

      a = System.nanoTime();
      r.truth = seqScanCount(pst, pgSql);
      r.pgPlan[0] = pgPlan(explain(pst, pgSql, "PostgreSQL", plans));
      r.pgCount[0] = p.count();
      final long pgProbe = warm(p);
      WALL[POSTGRES] += System.nanoTime() - a;

      r.reps = (int) Math.max(MIN_REPS, Math.min(MAX_REPS, REPS_BUDGET_NS / Math.max(1, Math.max(sdbProbe, pgProbe))));

      a = System.nanoTime();
      r.lucene = median(() -> searcher.count(q.lucene()), r.reps);
      r.luceneVisit = median(() -> visit(searcher, q.lucene()), r.reps);
      WALL[LUCENE] += System.nanoTime() - a;
      a = System.nanoTime();
      r.sdb = median(s::count, r.reps);
      WALL[SERENEDB] += System.nanoTime() - a;
      a = System.nanoTime();
      r.pg[0] = median(p::count, r.reps);
      WALL[POSTGRES] += System.nanoTime() - a;
    }
    return r;
  }

  static void measurePg(Result r, int v, Connection pg, Statement pst, String pgSql, StringBuilder plans)
      throws Exception {
    final long a = System.nanoTime();
    try (Prepared p = new Prepared(pg, pgSql)) {
      r.pgPlan[v] = pgPlan(explain(pst, pgSql, "PostgreSQL", plans));
      r.pgCount[v] = p.count();
      warm(p);
      r.pg[v] = median(p::count, r.reps);
    }
    WALL[POSTGRES] += System.nanoTime() - a;
  }

  static void report(StringBuilder out, Dataset ds, int rows, int threads, int segments, double luceneIngest,
      Map<String, Long> luceneFiles, double sdbIngest, Map<String, Long> sdbFiles, int sdbSegments, double pgCopy,
      double pgVacuum, List<PgVariant> variants, Map<String, Double> buildSec, Map<String, Long> sizeBytes,
      List<QueryPair> queries, Result[] results) {
    final long luceneBytes = luceneFiles.values().stream().mapToLong(Long::longValue).sum();
    final long sdbBytes = sdbFiles.values().stream().mapToLong(Long::longValue).sum();
    out.append(String.format(Locale.ROOT, "### %s: %,d rows, SereneDB `%s`, PostgreSQL `%s`%n%n", ds.name(), rows,
        ds.sdbType(), ds.pgType()));
    out.append("| | Lucene ").append(Version.LATEST).append(" | SereneDB |");
    for (PgVariant v : variants) {
      out.append(" PostgreSQL ").append(v.label()).append(" |");
    }
    out.append("\n|---|---:|---:|").append("---:|".repeat(variants.size())).append('\n');
    out.append(String.format(Locale.ROOT, "| ingest until searchable, s | %.2f | %.2f |", luceneIngest, sdbIngest));
    for (PgVariant v : variants) {
      double sec = pgCopy;
      for (PgIndex i : v.indexes()) {
        sec += buildSec.get(i.kind());
      }
      out.append(String.format(Locale.ROOT, " %.2f |", sec));
    }
    out.append(String.format(Locale.ROOT, "%n| index size on disk, MB | %s | %s |", mb(luceneBytes), mb(sdbBytes)));
    for (PgVariant v : variants) {
      long bytes = 0;
      for (PgIndex i : v.indexes()) {
        bytes += sizeBytes.get(i.kind());
      }
      out.append(' ').append(mb(bytes)).append(" |");
    }
    out.append("\n\n");
    final StringBuilder pgParts = new StringBuilder(String.format(Locale.ROOT, "COPY %.2f s", pgCopy));
    for (Map.Entry<String, Double> e : buildSec.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
      pgParts.append(String.format(Locale.ROOT, ", %s build %.2f s / %s MB", e.getKey(), e.getValue(),
          mb(sizeBytes.get(e.getKey()))));
    }
    out.append(String.format(Locale.ROOT,
        "Lucene: %d indexing threads, %d segments (%s). SereneDB: %d segments (%s). PostgreSQL: %s; "
            + "`VACUUM (ANALYZE)` after COPY, not in ingest: %.2f s.%n%n",
        threads, segments, byExtension(luceneFiles), sdbSegments, byExtension(sdbFiles), pgParts, pgVacuum));

    out.append("Median query latency, µs. Lucene `count` may answer from index statistics; "
        + "Lucene `visit` writes every matching document id into a 2048-entry window:\n\n");
    out.append("| query | rows (reference) | Lucene count, µs | Lucene visit, µs | SereneDB, µs |");
    for (PgVariant v : variants) {
      out.append(" PG ").append(v.label()).append(", µs |");
    }
    out.append(" runs |\n|---|---:|---:|---:|---:|").append("---:|".repeat(variants.size())).append("---:|\n");
    for (int i = 0; i < queries.size(); ++i) {
      final Result r = results[i];
      out.append("| ").append(queries.get(i).name()).append(" | ").append(rowsCell(r, variants)).append(" | ")
          .append(fmt(r.lucene)).append(" | ").append(fmt(r.luceneVisit)).append(" | ").append(fmt(r.sdb))
          .append(" |");
      for (double pg : r.pg) {
        out.append(' ').append(fmt(pg)).append(" |");
      }
      out.append(' ').append(r.reps).append(" |\n");
    }
    out.append('\n');

    out.append("| query | predicate | SereneDB plan |");
    for (PgVariant v : variants) {
      out.append(" PG plan, ").append(v.label()).append(" |");
    }
    out.append("\n|---|---|---|").append("---|".repeat(variants.size())).append('\n');
    for (int i = 0; i < queries.size(); ++i) {
      final QueryPair q = queries.get(i);
      final String pred = q.predicate().length() > 60 ? q.predicate().substring(0, 57) + "..." : q.predicate();
      out.append("| ").append(q.name()).append(" | `").append(pred).append("` | ").append(results[i].sdbPlan)
          .append(" |");
      for (String plan : results[i].pgPlan) {
        out.append(' ').append(plan).append(" |");
      }
      out.append('\n');
    }
    out.append('\n');
  }

  static String rowsCell(Result r, List<PgVariant> variants) {
    final StringBuilder wrong = new StringBuilder();
    if (r.luceneCount != r.truth) {
      wrong.append(String.format(Locale.ROOT, ", Lucene %,d", r.luceneCount));
    }
    if (r.luceneVisitCount != r.truth) {
      wrong.append(String.format(Locale.ROOT, ", Lucene visit %,d", r.luceneVisitCount));
    }
    if (r.sdbCount != r.truth) {
      wrong.append(String.format(Locale.ROOT, ", SereneDB %,d", r.sdbCount));
    }
    for (int v = 0; v < variants.size(); ++v) {
      if (r.pgCount[v] != r.truth) {
        wrong.append(String.format(Locale.ROOT, ", PG %s %,d", variants.get(v).label(), r.pgCount[v]));
      }
    }
    if (wrong.isEmpty()) {
      return String.format(Locale.ROOT, "%,d", r.truth);
    }
    ++mismatches;
    return String.format(Locale.ROOT, "%,d **MISMATCH**%s", r.truth, wrong);
  }

  static String mb(long bytes) {
    return String.format(Locale.ROOT, "%.1f", bytes / 1e6);
  }

  static String fmt(double micros) {
    return String.format(Locale.ROOT, "%,.0f", micros);
  }

  static final class Prepared implements AutoCloseable {
    private final PreparedStatement ps;

    Prepared(Connection conn, String sql) throws SQLException {
      ps = conn.prepareStatement(sql);
    }

    long count() throws SQLException {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }

    @Override
    public void close() throws SQLException {
      ps.close();
    }
  }

  static long warm(Prepared p) throws SQLException {
    long last = 0;
    for (int i = 0; i < SQL_WARMUP; ++i) {
      final long a = System.nanoTime();
      sink += p.count();
      last = System.nanoTime() - a;
    }
    return last;
  }

  static double median(Runner r, int reps) throws Exception {
    final long[] t = new long[reps];
    for (int i = 0; i < reps; ++i) {
      final long a = System.nanoTime();
      sink += r.run();
      t[i] = System.nanoTime() - a;
    }
    Arrays.sort(t);
    return t[reps / 2] / 1000.0;
  }

  static final List<String> INDEX_SCANS = List.of("enable_indexscan", "enable_indexonlyscan", "enable_bitmapscan");

  static long seqScanCount(Statement st, String sql) throws SQLException {
    for (String s : INDEX_SCANS) {
      st.execute("SET " + s + " = off");
    }
    try (ResultSet rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    } finally {
      for (String s : INDEX_SCANS) {
        st.execute("RESET " + s);
      }
    }
  }

  static double timed(Statement st, String sql) throws SQLException {
    final long a = System.nanoTime();
    st.execute(sql);
    return (System.nanoTime() - a) / 1e9;
  }

  static String explain(Statement st, String sql, String engine, StringBuilder plans) throws SQLException {
    final StringBuilder text = new StringBuilder();
    try (ResultSet rs = st.executeQuery("EXPLAIN " + sql)) {
      final int cols = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        for (int c = 1; c <= cols; ++c) {
          text.append(rs.getString(c)).append('\n');
        }
      }
    }
    plans.append("-- ").append(engine).append(": ").append(sql).append('\n').append(text).append('\n');
    return text.toString();
  }

  static String sdbPlan(String plan) {
    return plan.contains("IRESEARCH_SCAN") && plan.contains("Index Filter") ? "inverted index" : "**no index**";
  }

  static final Pattern PG_SCAN =
      Pattern.compile("(Index Only Scan|Index Scan|Bitmap Index Scan)(?: Backward)? (?:using|on) (\\S+)");

  static String pgPlan(String plan) {
    final Set<String> used = new LinkedHashSet<>();
    for (String line : plan.split("\n")) {
      final Matcher m = PG_SCAN.matcher(line);
      if (m.find()) {
        final String name = m.group(2);
        used.add(name.substring(name.lastIndexOf('_') + 1) + switch (m.group(1)) {
          case "Index Only Scan" -> " index-only";
          case "Index Scan" -> " index scan";
          default -> " bitmap";
        });
      }
      if (line.contains("Seq Scan")) {
        used.add("**seq scan**");
      }
    }
    String s = used.isEmpty() ? "**no index**" : String.join(" + ", used);
    if (plan.contains("Gather")) {
      s += ", parallel";
    }
    return s;
  }

  static long pgRelationSize(Statement st, String name) throws SQLException {
    try (ResultSet rs = st.executeQuery("SELECT pg_relation_size('" + name + "')")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  static List<Path> sdbIndexDirs(Statement st, Path sdbData, String index) throws SQLException, IOException {
    final long oid;
    try (ResultSet rs = st.executeQuery("SELECT oid FROM pg_class WHERE relname = '" + index + "'")) {
      rs.next();
      oid = rs.getLong(1);
    }
    try (Stream<Path> dbs = Files.list(sdbData.resolve("engine_v1"))) {
      return dbs.map(db -> db.resolve(Long.toString(oid))).filter(Files::isDirectory).toList();
    }
  }

  static final Pattern SEGMENT_META = Pattern.compile("(_\\d+)\\.\\d+\\.sm");

  static int liveSegmentFiles(Path dir, Map<String, Long> bytes) throws IOException {
    final List<Path> files;
    try (Stream<Path> list = Files.list(dir)) {
      files = list.toList();
    }
    final Path meta = files.stream().filter(p -> p.getFileName().toString().startsWith("segments_"))
        .max(Comparator.comparingLong(p -> Long.parseLong(p.getFileName().toString().substring(9))))
        .orElseThrow();
    final Set<String> segments = new LinkedHashSet<>();
    final Matcher m =
        SEGMENT_META.matcher(new String(Files.readAllBytes(meta), StandardCharsets.ISO_8859_1));
    while (m.find()) {
      segments.add(m.group(1) + ".");
    }
    addFile(bytes, meta);
    for (Path p : files) {
      final String name = p.getFileName().toString();
      if (segments.stream().anyMatch(name::startsWith)) {
        addFile(bytes, p);
      }
    }
    return segments.size();
  }

  static void addFile(Map<String, Long> bytes, Path p) throws IOException {
    final String name = p.getFileName().toString();
    final int dot = name.lastIndexOf('.');
    final String ext = name.startsWith("segments_") ? "segments" : dot < 0 ? name : name.substring(dot + 1);
    bytes.merge(ext, Files.size(p), Long::sum);
  }

  static String byExtension(Map<String, Long> bytes) {
    return String.join(", ", bytes.entrySet().stream()
        .filter(e -> e.getValue() >= 100_000)
        .map(e -> e.getKey() + " " + mb(e.getValue()) + " MB").toList());
  }

  static void deleteRecursively(Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (Stream<Path> files = Files.walk(dir)) {
      for (Path p : files.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(p);
      }
    }
  }
}
