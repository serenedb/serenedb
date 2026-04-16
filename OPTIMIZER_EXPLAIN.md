## Schema

```sql
CREATE TEXT SEARCH DICTIONARY test_english(
    template = 'text', locale = 'en_US.UTF-8', case = 'none', stemming = false,
    accent = false, frequency = true, position = true
);

-- Single-col PK + three secondary indexes (single / composite / disjoint).
CREATE TABLE t1 (pk INT PRIMARY KEY, x INT, y INT, z INT);
INSERT INTO t1 VALUES (1,10,100,1000),(2,20,200,2000),(3,30,300,3000);
CREATE INDEX sk_x   ON t1(x);
CREATE INDEX sk_xy  ON t1(x, y);
CREATE INDEX sk_yz  ON t1(y, z);

-- Composite PK.
CREATE TABLE t2 (a INT, b INT, v INT, PRIMARY KEY (a, b));
INSERT INTO t2 VALUES (1, 1, 11),(1, 2, 12),(2, 1, 21);

-- Inverted index: text columns + HNSW vector.
CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, title TEXT, emb FLOAT[2]);
INSERT INTO docs VALUES
  (1, 'pudge mid',            'lane',      ARRAY[1.0, 1.0]::FLOAT[2]),
  (2, 'vedernikoff carries',  'safe lane', ARRAY[2.0, 2.0]::FLOAT[2]),
  (3, 'babsky afk',           'off',       ARRAY[3.0, 3.0]::FLOAT[2]);
CREATE INDEX iv ON docs USING inverted(body test_english, title test_english, emb hnsw);
```

---
## rocksdb: primary key (single-col)

#### `WHERE pk = 2`
```sql
SELECT * FROM t1 WHERE pk = 2
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk = 2)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│       Filter: (pk=2)      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk IN (1,2,3)`
```sql
SELECT * FROM t1 WHERE pk IN (1,2,3)
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│ ((pk = 1) OR (pk = 2) OR  │
│         (pk = 3))         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│          Filter:          │
│   (pk=1), (pk=2), (pk=3)  │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk < 3`
```sql
SELECT * FROM t1 WHERE pk < 3
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk < 3)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│          Filter:          │
│       {pk=(-inf, 3)}      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk <= 2`
```sql
SELECT * FROM t1 WHERE pk <= 2
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│         (pk <= 2)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│          Filter:          │
│       {pk=(-inf, 2]}      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk > 1`
```sql
SELECT * FROM t1 WHERE pk > 1
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk > 1)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│          Filter:          │
│       {pk=(1, +inf)}      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk BETWEEN 1 AND 2`
```sql
SELECT * FROM t1 WHERE pk BETWEEN 1 AND 2
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│    (pk BETWEEN 1 AND 2)   │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│    Filter: {pk=[1, 2]}    │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk != 2` -- splits into two ranges
```sql
SELECT * FROM t1 WHERE pk != 2
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│         (pk != 2)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│          Filter:          │
│  {pk=(-inf, 2)}, {pk=(2,  │
│           +inf)}          │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk = 2 OR pk = 3` -- two points
```sql
SELECT * FROM t1 WHERE pk = 2 OR pk = 3
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│   ((pk = 2) OR (pk = 3))  │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│          Filter:          │
│       (pk=2), (pk=3)      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk IS NULL` -- PK cannot be null -> contradictory claim, empty scan, 0 rows
```sql
SELECT * FROM t1 WHERE pk IS NULL
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│        (pk IS NULL)       │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│     Filter: {pk=empty}    │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
```

#### `WHERE pk IS NOT NULL` -- unconstrained -> full table, all 3 rows
```sql
SELECT * FROM t1 WHERE pk IS NOT NULL ORDER BY pk
```
```
┌───────────────────────────┐
│          ORDER_BY         │
│    ────────────────────   │
│ postgres.public.t1.pk ASC │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│      (pk IS NOT NULL)     │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
1|10|100|1000
2|20|200|2000
3|30|300|3000
```

#### `WHERE pk IS NULL OR pk = 2` -- IS-NULL branch contradictory; OR collapses to single pk=2 point
```sql
SELECT * FROM t1 WHERE pk IS NULL OR pk = 2
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│ ((pk IS NULL) OR (pk = 2))│
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│       Filter: (pk=2)      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
2|20|200|2000
```

## rocksdb: primary key (composite)

#### `WHERE a = 1 AND b = 2` -- full equality -> point
```sql
SELECT * FROM t2 WHERE a = 1 AND b = 2
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│   ((a = 1) AND (b = 2))   │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t2         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│     Filter: (a=1, b=2)    │
│                           │
│        Projections:       │
│             a             │
│             b             │
│             v             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE a = 1` -- prefix only -> range
```sql
SELECT * FROM t2 WHERE a = 1
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│          (a = 1)          │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t2         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│       Filter: {a=1}       │
│                           │
│        Projections:       │
│             a             │
│             b             │
│             v             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE a = 1 AND b < 2` -- prefix + trailing range
```sql
SELECT * FROM t2 WHERE a = 1 AND b < 2
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│   ((a = 1) AND (b < 2))   │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t2         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│          Filter:          │
│     {a=1, b=(-inf, 2)}    │
│                           │
│        Projections:       │
│             a             │
│             b             │
│             v             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE a = 1 AND b IN (1, 2)` -- prefix + IN -> 2 points
```sql
SELECT * FROM t2 WHERE a = 1 AND b IN (1, 2)
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│ ((a = 1) AND ((b = 1) OR  │
│         (b = 2)))         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t2         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│          Filter:          │
│   (a=1, b=1), (a=1, b=2)  │
│                           │
│        Projections:       │
│             a             │
│             b             │
│             v             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE a IN (1, 2) AND b = 1` -- cross product -> 2 points
```sql
SELECT * FROM t2 WHERE a IN (1, 2) AND b = 1
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│ ((b = 1) AND ((a = 1) OR  │
│         (a = 2)))         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t2         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│          Filter:          │
│   (a=1, b=1), (a=2, b=1)  │
│                           │
│        Projections:       │
│             a             │
│             b             │
│             v             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE b = 1` -- only suffix -> full table
```sql
SELECT * FROM t2 WHERE b = 1
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             a             │
│             b             │
│             v             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (b = 1)          │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│         Table: t2         │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             b             │
│             a             │
│             v             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## rocksdb: secondary key

#### `WHERE x = 10` -- single-col SK equality -> sk_x point
```sql
SELECT * FROM t1 WHERE x = 10
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (x = 10)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│       Filter: (x=10)      │
│                           │
│        Projections:       │
│             x             │
│             pk            │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE x IN (10, 20)` -- IN on SK
```sql
SELECT * FROM t1 WHERE x IN (10, 20)
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│   ((x = 10) OR (x = 20))  │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│          Filter:          │
│       (x=10), (x=20)      │
│                           │
│        Projections:       │
│             x             │
│             pk            │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE x = 10 AND y = 100` -- sk_xy beats sk_x (more cover)
```sql
SELECT * FROM t1 WHERE x = 10 AND y = 100
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│  ((x = 10) AND (y = 100)) │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│   Filter: (x=10, y=100)   │
│                           │
│        Projections:       │
│             x             │
│             y             │
│             pk            │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE x < 20` -- SK range
```sql
SELECT * FROM t1 WHERE x < 20
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (x < 20)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_range│
│           s_scan          │
│                           │
│          Filter:          │
│       {x=(-inf, 20)}      │
│                           │
│        Projections:       │
│             x             │
│             pk            │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE x BETWEEN 10 AND 20` -- SK range closed
```sql
SELECT * FROM t1 WHERE x BETWEEN 10 AND 20
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│   (x BETWEEN 10 AND 20)   │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_range│
│           s_scan          │
│                           │
│    Filter: {x=[10, 20]}   │
│                           │
│        Projections:       │
│             x             │
│             pk            │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE y = 100` -- sk_yz, prefix equality -> range
```sql
SELECT * FROM t1 WHERE y = 100
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│         (y = 100)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_range│
│           s_scan          │
│                           │
│      Filter: {y=100}      │
│                           │
│        Projections:       │
│             y             │
│             pk            │
│             x             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE y = 100 AND z = 1000` -- sk_yz full -> point
```sql
SELECT * FROM t1 WHERE y = 100 AND z = 1000
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│ ((y = 100) AND (z = 1000))│
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│          Filter:          │
│      (y=100, z=1000)      │
│                           │
│        Projections:       │
│             y             │
│             z             │
│             pk            │
│             x             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE z = 1000` -- z leads no index -> full table
```sql
SELECT * FROM t1 WHERE z = 1000
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│         (z = 1000)        │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             z             │
│             pk            │
│             x             │
│             y             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE x IS NULL` -- SK cannot be null -> empty scan, 0 rows
```sql
SELECT * FROM t1 WHERE x IS NULL
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│        (x IS NULL)        │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_range│
│           s_scan          │
│                           │
│     Filter: {x=empty}     │
│                           │
│        Projections:       │
│             x             │
│             pk            │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## rocksdb: cost-based tiebreak

#### `WHERE pk = 2 AND x = 10` -- both possible, PK wins tiebreak
```sql
SELECT * FROM t1 WHERE pk = 2 AND x = 10
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│  ((pk = 2) AND (x = 10))  │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│       Filter: (pk=2)      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE pk < 5 AND x = 10` -- PK range vs SK point -- points beat ranges
```sql
SELECT * FROM t1 WHERE pk < 5 AND x = 10
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│  ((pk < 5) AND (x = 10))  │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│       Filter: (x=10)      │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE x = 10 AND y = 100` -- sk_xy (2-col cover) beats sk_x (1-col)
```sql
SELECT * FROM t1 WHERE x = 10 AND y = 100
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│  ((x = 10) AND (y = 100)) │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│   Filter: (x=10, y=100)   │
│                           │
│        Projections:       │
│             x             │
│             y             │
│             pk            │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE x = 10 OR y = 100` -- OR spans two SKs; no single index covers -> full table
```sql
SELECT * FROM t1 WHERE x = 10 OR y = 100
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│  ((x = 10) OR (y = 100))  │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             x             │
│             y             │
│             pk            │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## rocksdb: FROM index_name

#### `FROM sk_x` -- bare full SK scan
```sql
SELECT * FROM sk_x
```
```
┌───────────────────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_fulls│
│            can            │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM sk_x WHERE x = 10` -- FROM-index + covering predicate
```sql
SELECT * FROM sk_x WHERE x = 10
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (x = 10)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│       Filter: (x=10)      │
│                           │
│        Projections:       │
│             x             │
│             pk            │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM sk_x WHERE pk = 1` -- predicate not on the index -> rule skips
```sql
SELECT * FROM sk_x WHERE pk = 1
```
```
┌───────────────────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk = 1)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_fulls│
│            can            │
│                           │
│        Projections:       │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM sk_xy WHERE x = 10 AND y = 100` -- FROM composite, full cover
```sql
SELECT * FROM sk_xy WHERE x = 10 AND y = 100
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│  ((x = 10) AND (y = 100)) │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│   Filter: (x=10, y=100)   │
│                           │
│        Projections:       │
│             x             │
│             y             │
│             pk            │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM sk_xy WHERE x = 10` -- FROM composite, prefix only -> range
```sql
SELECT * FROM sk_xy WHERE x = 10
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             pk            │
│             x             │
│             y             │
│             z             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (x = 10)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_range│
│           s_scan          │
│                           │
│       Filter: {x=10}      │
│                           │
│        Projections:       │
│             x             │
│             pk            │
│             y             │
│             z             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM iv` -- bare full iresearch scan
```sql
SELECT id, body FROM iv
```
```
┌───────────────────────────┐
│     IRESEARCH_FULLSCAN    │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             id            │
│            body           │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## rocksdb: DML (UPDATE / DELETE)

#### UPDATE by PK point
```sql
UPDATE t1 SET y = 999 WHERE pk = 2
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│            999            │
│             pk            │
│             x             │
│             y             │
│             z             │
│           rowid           │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk = 2)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│       Filter: (pk=2)      │
│      Projections: pk      │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
UPDATE 1
```

#### DELETE by PK point
```sql
DELETE FROM t1 WHERE pk = 2
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk = 2)         │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│       Filter: (pk=2)      │
│      Projections: pk      │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
DELETE 1
```

#### UPDATE by composite PK point
```sql
UPDATE t2 SET v = 111 WHERE a = 1 AND b = 2
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│             #1            │
│             #2            │
│             #3            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│            111            │
│             a             │
│             b             │
│           rowid           │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #2            │
│             #3            │
│             #4            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│   ((a = 1) AND (b = 2))   │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t2         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│     Filter: (a=1, b=2)    │
│                           │
│        Projections:       │
│             a             │
│             b             │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
UPDATE 1
```

#### DELETE by SK point
```sql
DELETE FROM t1 WHERE x = 30
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (x = 30)         │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│       Filter: (x=30)      │
│       Projections: x      │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
DELETE 1
```

#### UPDATE by SK range
```sql
UPDATE t1 SET z = 7777 WHERE x < 20
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│            7777           │
│             pk            │
│             x             │
│             y             │
│             z             │
│           rowid           │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (x < 20)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_range│
│           s_scan          │
│                           │
│          Filter:          │
│       {x=(-inf, 20)}      │
│                           │
│       Projections: x      │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
UPDATE 1
```

#### DELETE by PK range
```sql
DELETE FROM t1 WHERE pk < 5
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk < 5)         │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_RA...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_ranges_│
│            scan           │
│                           │
│          Filter:          │
│       {pk=(-inf, 5)}      │
│                           │
│      Projections: pk      │
│                           │
│          ~0 rows          │
└───────────────────────────┘

--- result ---
DELETE 1
```

## iresearch: boolean filter (lookup)

#### `WHERE sdb_phrase(body, 'pudge')`
```sql
SELECT id, body FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│            body           │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│      iresearch_lookup     │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE sdb_term_eq(title, 'lane')`
```sql
SELECT id, title FROM docs WHERE sdb_term_eq(title, 'lane')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│           title           │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│      iresearch_lookup     │
│                           │
│          Filter:          │
│   AND[Term(title(string)  │
│          =lane)]          │
│                           │
│        Projections:       │
│           title           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE sdb_phrase AND id > 0` -- partial claim
```sql
SELECT id FROM docs WHERE sdb_phrase(body, 'pudge') AND id > 0
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #1            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (id > 0)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│      iresearch_lookup     │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE sdb_phrase OR sdb_phrase` -- OR merged into iresearch tree
```sql
SELECT id FROM docs WHERE sdb_phrase(body,'pudge') OR sdb_phrase(body,'babsky')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│      iresearch_lookup     │
│                           │
│          Filter:          │
│ AND[OR[PHRASE[body(string)│
│   = <Term:pudge(0, 0); >] │
│  || PHRASE[body(string) = │
│  <Term:babsky(0, 0); >]]] │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE NOT sdb_phrase` -- negation inside iresearch
```sql
SELECT id FROM docs WHERE NOT sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│      iresearch_lookup     │
│                           │
│          Filter:          │
│ AND[NOT[PHRASE[body(string│
│) = <Term:pudge(0, 0); >]]]│
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE sdb_term_in(title, 'lane', 'off')` -- term-level IN-list
```sql
SELECT id FROM docs WHERE sdb_term_in(title, 'lane', 'off')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│      iresearch_lookup     │
│                           │
│          Filter:          │
│ AND[TERMS[title(string), {│
│ ['lane', 1],['off', 1],}, │
│             1]]           │
│                           │
│        Projections:       │
│           title           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## iresearch: scoring -- bm25 / tfidf

#### `bm25(tableoid)` -- default (k1=1.2, b=0.75)
```sql
SELECT id, bm25(tableoid) FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│       bm25(tableoid)      │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│    iresearch_score_scan   │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.2, b=0.75)   │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `bm25(tableoid, 1.5, 0.8)` -- tuned params plumbed to bind-data
```sql
SELECT id, bm25(tableoid, 1.5, 0.8) FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│  bm25(tableoid, 1.5, 0.8) │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│    iresearch_score_scan   │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.5, b=0.8)    │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `tfidf(tableoid)` -- default (with_norms=false)
```sql
SELECT id, tfidf(tableoid) FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│      tfidf(tableoid)      │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│    iresearch_score_scan   │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│  tfidf(with_norms=false)  │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `tfidf(tableoid, true)` -- with length norms
```sql
SELECT id, tfidf(tableoid, true) FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│   tfidf(tableoid, true)   │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│    iresearch_score_scan   │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│   tfidf(with_norms=true)  │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `bm25 + LIMIT 5` -- topk pullup, LIMIT dropped
```sql
SELECT id, bm25(tableoid) FROM docs WHERE sdb_phrase(body, 'pudge') LIMIT 5
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│       bm25(tableoid)      │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│ iresearch_score_topk_scan │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.2, b=0.75)   │
│                           │
│          TopK: 5          │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `bm25 + ORDER BY bm25 DESC LIMIT 5` -- TopN descending -> topk pullup
```sql
SELECT id, bm25(tableoid, 1.5, 0.8) AS s FROM docs WHERE sdb_phrase(body, 'pudge') ORDER BY s DESC LIMIT 5
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│             s             │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│ iresearch_score_topk_scan │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.5, b=0.8)    │
│                           │
│          TopK: 5          │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `tfidf + LIMIT 2` -- topk pullup for TF-IDF too
```sql
SELECT id, tfidf(tableoid) FROM docs WHERE sdb_phrase(body, 'pudge') LIMIT 2
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│      tfidf(tableoid)      │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│ iresearch_score_topk_scan │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│  tfidf(with_norms=false)  │
│                           │
│          TopK: 2          │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `bm25 + ORDER BY another col LIMIT` -- no pullup (LIMIT kept)
```sql
SELECT id, bm25(tableoid) FROM docs WHERE sdb_phrase(body, 'pudge') ORDER BY id DESC LIMIT 5
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│       bm25(tableoid)      │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│ iresearch_score_topk_scan │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.2, b=0.75)   │
│                           │
│          TopK: 5          │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## iresearch: offsets

#### `sdb_offsets(body)` -- single column
```sql
SELECT id, sdb_offsets(body) FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│     sdb_offsets(body)     │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│iresearch_lookup_with_offse│
│             ts            │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│       Offsets: body       │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `sdb_offsets(body), sdb_offsets(title)` -- multi-column (dedup per column)
```sql
SELECT id, sdb_offsets(body), sdb_offsets(title) FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│     sdb_offsets(body)     │
│     sdb_offsets(title)    │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│iresearch_lookup_with_offse│
│             ts            │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│    Offsets: body, title   │
│                           │
│        Projections:       │
│            body           │
│             id            │
│           title           │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `sdb_offsets` with no search predicate -- still attaches (rule always claims offsets when col is indexed)
```sql
SELECT id, sdb_offsets(body) FROM docs
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│     sdb_offsets(body)     │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             id            │
│            body           │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## iresearch: score + offsets combined

#### `bm25 + offsets`
```sql
SELECT id, bm25(tableoid), sdb_offsets(body) FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│       bm25(tableoid)      │
│     sdb_offsets(body)     │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│iresearch_score_scan_with_o│
│           ffsets          │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.2, b=0.75)   │
│                           │
│       Offsets: body       │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `bm25 + offsets + LIMIT 3` -- topk + offsets (LIMIT dropped)
```sql
SELECT id, bm25(tableoid, 1.5, 0.8), sdb_offsets(body) FROM docs WHERE sdb_phrase(body, 'pudge') LIMIT 3
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│  bm25(tableoid, 1.5, 0.8) │
│     sdb_offsets(body)     │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│iresearch_score_topk_scan_w│
│        ith_offsets        │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.5, b=0.8)    │
│                           │
│          TopK: 3          │
│       Offsets: body       │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `tfidf + multi-offsets + LIMIT`
```sql
SELECT id, tfidf(tableoid, true), sdb_offsets(body), sdb_offsets(title) FROM docs WHERE sdb_phrase(body, 'pudge') LIMIT 2
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│   tfidf(tableoid, true)   │
│     sdb_offsets(body)     │
│     sdb_offsets(title)    │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│iresearch_score_topk_scan_w│
│        ith_offsets        │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│   tfidf(with_norms=true)  │
│                           │
│          TopK: 2          │
│    Offsets: body, title   │
│                           │
│        Projections:       │
│            body           │
│             id            │
│           title           │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## iresearch: ANN (vector distance)

#### `ORDER BY sdb_l2_distance LIMIT k` on table
```sql
SELECT id FROM docs ORDER BY sdb_l2_distance(emb, ARRAY[1.0, 1.0]::FLOAT[2]) LIMIT 1
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│ sdb_l2_distance(emb, [1.0,│
│            1.0])          │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│  IRESEARCH_ANN_TOPK_SCAN  │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│  iresearch_ann_topk_scan  │
│                           │
│          TopK: 1          │
│          Dims: 2          │
│                           │
│        Projections:       │
│             id            │
│            emb            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE sdb_l2_distance < r` on table
```sql
SELECT id FROM docs WHERE sdb_l2_distance(emb, ARRAY[1.0, 1.0]::FLOAT[2]) < 1.5
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│  IRESEARCH_ANN_RANGE_SCAN │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│  iresearch_ann_range_scan │
│                           │
│      Radius: 1.500000     │
│          Dims: 2          │
│                           │
│        Projections:       │
│            emb            │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `ORDER BY sdb_l2_distance LIMIT k` FROM iv
```sql
SELECT id FROM iv ORDER BY sdb_l2_distance(emb, ARRAY[1.0, 1.0]::FLOAT[2]) LIMIT 2
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│ sdb_l2_distance(emb, [1.0,│
│            1.0])          │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│  IRESEARCH_ANN_TOPK_SCAN  │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│  iresearch_ann_topk_scan  │
│                           │
│          TopK: 2          │
│          Dims: 2          │
│                           │
│        Projections:       │
│             id            │
│            emb            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `WHERE sdb_l2_distance < r` FROM iv
```sql
SELECT id FROM iv WHERE sdb_l2_distance(emb, ARRAY[1.0, 1.0]::FLOAT[2]) < 2.0
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│  IRESEARCH_ANN_RANGE_SCAN │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│  iresearch_ann_range_scan │
│                           │
│      Radius: 2.000000     │
│          Dims: 2          │
│                           │
│        Projections:       │
│            emb            │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `ORDER BY sdb_l2_distance DESC LIMIT k` -- DESC not supported -> full iresearch scan
```sql
SELECT id FROM docs ORDER BY sdb_l2_distance(emb, ARRAY[1.0, 1.0]::FLOAT[2]) DESC LIMIT 1
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           TOP_N           │
│    ────────────────────   │
│           Top: 1          │
│     Order By: #1 DESC     │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│ sdb_l2_distance(emb, [1.0,│
│            1.0])          │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             id            │
│            emb            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## iresearch: FROM iv (index scan entry)

#### `SELECT * FROM iv` -- bare iresearch_fullscan
```sql
SELECT id, body FROM iv
```
```
┌───────────────────────────┐
│     IRESEARCH_FULLSCAN    │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│        Projections:       │
│             id            │
│            body           │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM iv WHERE sdb_phrase` -- lookup via index
```sql
SELECT id FROM iv WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│      iresearch_lookup     │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM iv + bm25 + LIMIT` -- scoring + topk on index
```sql
SELECT id, bm25(tableoid) FROM iv WHERE sdb_phrase(body, 'pudge') LIMIT 2
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│       bm25(tableoid)      │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│ iresearch_score_topk_scan │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│           Score:          │
│    bm25(k1=1.2, b=0.75)   │
│                           │
│          TopK: 2          │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `FROM iv + sdb_offsets` -- offsets on index
```sql
SELECT id, sdb_offsets(body) FROM iv WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             id            │
│     sdb_offsets(body)     │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      IRESEARCH_LOOKUP     │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│iresearch_lookup_with_offse│
│             ts            │
│                           │
│          Filter:          │
│ AND[PHRASE[body(string) = │
│   <Term:pudge(0, 0); >]]  │
│                           │
│       Offsets: body       │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

## mutation guard (DELETE / UPDATE)

#### `DELETE FROM docs WHERE sdb_phrase` -- iresearch skipped, rocksdb full table
```sql
DELETE FROM docs WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│ sdb_phrase(body, 'pudge') │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│     Projections: body     │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `UPDATE docs ... WHERE sdb_phrase` -- same guard
```sql
UPDATE docs SET title = 'x' WHERE sdb_phrase(body, 'pudge')
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│            'x'            │
│             id            │
│            body           │
│           title           │
│            emb            │
│           rowid           │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│ sdb_phrase(body, 'pudge') │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│   ROCKSDB_TABLE_FULLSCAN  │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│   rocksdb_table_fullscan  │
│                           │
│     Projections: body     │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `DELETE ... WHERE pk = 2` -- rocksdb rule fires (guard only affects iresearch)
```sql
DELETE FROM t1 WHERE pk = 2
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (pk = 2)         │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│       Filter: (pk=2)      │
│      Projections: pk      │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `UPDATE ... WHERE x = 10` -- rocksdb SK fires under UPDATE
```sql
UPDATE t1 SET y = 42 WHERE x = 10
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #0            │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             42            │
│             pk            │
│             x             │
│             y             │
│             z             │
│           rowid           │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│             #1            │
│             #2            │
│             #3            │
│             #4            │
│             #5            │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│          (x = 10)         │
│                           │
│           ~1 row          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_SECONDARY_KEY_...  │
│    ────────────────────   │
│         Table: t1         │
│                           │
│         Strategy:         │
│rocksdb_secondary_key_point│
│          s_lookup         │
│                           │
│       Filter: (x=10)      │
│       Projections: x      │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```

#### `DELETE FROM docs WHERE sdb_phrase AND id = 1` -- full scan, nothing claimed
```sql
DELETE FROM docs WHERE sdb_phrase(body, 'pudge') AND id = 1
```
```
┌───────────────────────────┐
│         EXTENSION         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│  ((id = 1) AND sdb_phrase │
│      (body, 'pudge'))     │
│                           │
│          ~0 rows          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│ROCKSDB_PRIMARY_KEY_PO...  │
│    ────────────────────   │
│        Table: docs        │
│                           │
│         Strategy:         │
│rocksdb_primary_key_points_│
│           lookup          │
│                           │
│       Filter: (id=1)      │
│                           │
│        Projections:       │
│            body           │
│             id            │
│                           │
│          ~0 rows          │
└───────────────────────────┘

```
