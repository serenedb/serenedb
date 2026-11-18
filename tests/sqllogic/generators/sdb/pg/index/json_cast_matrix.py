from sqllogic_gen import Test

t = Test("Every cast of an indexed JSON path, through an inverted index and a "
         "search table, against the same predicate on the table itself: "
         "mismatches is always 0.", seed=5)

SMALL = ["0", "-0.0", "0.1", "0.10000000149011612", "-0.4", "0.4", "0.5",
         "0.6", "1", "1.5", "2.5", "3.14", "41.5", "41.6", "42", "42.4",
         "42.5", "43.5", "100", "126.4"]
WIDE = ["-1000000000000000", "-42.5", "-1.5", "0", "42.5", "2147483648",
        "9007199254740991", "9007199254740992", "9007199254740993",
        "1000000000000000000", "1500000000000000000"]

SMALL_CASTS = {
    "TINYINT": ["0", "1", "2", "42", "43", "126"],
    "SMALLINT": ["0", "42", "100"],
    "INTEGER": ["0", "1", "42", "43"],
    "BIGINT": ["0", "42"],
    "UTINYINT": ["0", "1", "42"],
    "USMALLINT": ["0", "42"],
    "UINTEGER": ["0", "42"],
    "REAL": ["0.1", "41.6", "42.5", "3.14"],
    "DOUBLE": ["0", "0.1", "0.10000000149011612", "3.14", "42", "42.4"],
}
WIDE_CASTS = {
    "BIGINT": ["0", "2147483648", "9007199254740992", "9007199254740993",
               "1000000000000000000"],
    "REAL": ["-42.5", "1000000000000000000", "9007199254740992"],
    "DOUBLE": ["-1000000000000000", "-1.5", "9007199254740992",
               "9007199254740993", "1000000000000000000"],
}


def rows(leaves):
    out = [f"({i}, '{{\"v\": {leaf}}}')" for i, leaf in enumerate(leaves)]
    out.append(f"({len(leaves)}, '{{\"v\": null}}')")
    out.append(f"({len(leaves) + 1}, '{{\"w\": 1}}')")
    return out


def predicates(cast, constants):
    path = f"(doc->'v')::{cast}"
    lo, hi = constants[0], constants[-1]
    preds = [f"{path} IN ({', '.join(constants)})",
             f"NOT ({path} IN ({', '.join(constants[:2])}))",
             f"{path} BETWEEN {lo} AND {hi}",
             f"{path} NOT BETWEEN {lo} AND {hi}",
             f"{path} IS NULL",
             f"{path} IS NOT NULL"]
    for c in constants:
        for op in ("=", "<>", "<", "<=", ">", ">="):
            preds.append(f"{path} {op} {c}")
        preds.append(f"NOT ({path} = {c})")
    return preds


t.dictionary("jcm_kw", "keyword() WITH (frequency)")
for name, leaves, casts in (("jcm", SMALL, SMALL_CASTS),
                            ("jcw", WIDE, WIDE_CASTS)):
    t.section(f"{name}: " + ", ".join(casts))
    t.table(name, "id INTEGER PRIMARY KEY, doc JSON")
    t.table(f"{name}_s", "id INTEGER PRIMARY KEY, doc JSON")
    t.index(f"{name}_idx", name, "id, (doc->'v') jcm_kw")
    t.index(f"{name}_s_idx", f"{name}_s", "id, (doc->'v') jcm_kw")
    t.insert(name, rows(leaves))
    t.statement(f"INSERT INTO {name}_s SELECT * FROM {name}")
    for cast, constants in casts.items():
        for pred in predicates(cast, constants):
            truth = f"SELECT id FROM {name} WHERE {pred}"
            t.same_rows(f"SELECT id FROM {name}_idx WHERE {pred}", truth)
            t.same_rows(f"SELECT id FROM {name}_s WHERE {pred}", truth)
t.write()
