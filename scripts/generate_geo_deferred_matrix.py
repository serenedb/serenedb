import json
import random
import sys

rng = random.Random(7)
out = []


def emit(*lines):
    out.extend(lines)


def stmt(sql):
    emit("statement ok", sql, "")


def lit(text):
    return text.replace("'", "''")


def ring(cx, cy, w, h):
    return [[cx, cy], [cx + w, cy], [cx + w, cy + h], [cx, cy + h], [cx, cy]]


def rnd(lo, hi):
    return round(rng.uniform(lo, hi), 6)


def random_shape(i):
    far = i % 17 == 0
    cx = rnd(-10.0, 10.0) if far else rnd(37.580, 37.640)
    cy = rnd(-10.0, 10.0) if far else rnd(55.700, 55.745)
    kind = i % 10
    if kind in (0, 1, 2):
        return {"type": "Point", "coordinates": [cx, cy]}
    if kind == 3:
        w, h = rnd(0.0005, 0.008), rnd(0.0005, 0.008)
        return {"type": "Polygon", "coordinates": [ring(cx, cy, w, h)]}
    if kind == 4:
        w, h = rnd(0.004, 0.015), rnd(0.004, 0.015)
        hole = ring(cx + w / 4, cy + h / 4, w / 2, h / 2)
        hole.reverse()
        return {"type": "Polygon",
                "coordinates": [ring(cx, cy, w, h), hole]}
    if kind == 5:
        return {"type": "LineString",
                "coordinates": [[cx, cy], [cx + rnd(0.0005, 0.006), cy + rnd(-0.004, 0.004)]]}
    if kind == 6:
        return {"type": "MultiPoint",
                "coordinates": [[cx, cy], [cx + rnd(0.0005, 0.008), cy + rnd(0.0005, 0.008)]]}
    if kind == 7:
        return {"type": "MultiLineString",
                "coordinates": [[[cx, cy], [cx + 0.002, cy + 0.001]],
                                [[cx + 0.003, cy], [cx + 0.005, cy + 0.002]]]}
    if kind == 9:
        w, h = rnd(0.004, 0.015), rnd(0.004, 0.015)
        return {"type": "Polygon",
                "coordinates": [ring(cx, cy, w, h),
                                ring(cx + w / 4, cy + h / 4, w / 2, h / 2)]}
    return {"type": "MultiPolygon",
            "coordinates": [[ring(cx, cy, 0.004, 0.004)],
                            [ring(cx + 0.01, cy + 0.01, 0.003, 0.006)]]}


def wkt(shape):
    def pts(coords):
        return ", ".join(f"{x} {y}" for x, y in coords)
    t, c = shape["type"], shape["coordinates"]
    if t == "Point":
        return f"POINT({c[0]} {c[1]})"
    if t == "LineString":
        return f"LINESTRING({pts(c)})"
    if t == "Polygon":
        return "POLYGON(" + ", ".join(f"({pts(r)})" for r in c) + ")"
    if t == "MultiPoint":
        return "MULTIPOINT(" + ", ".join(f"({x} {y})" for x, y in c) + ")"
    if t == "MultiLineString":
        return "MULTILINESTRING(" + ", ".join(f"({pts(l)})" for l in c) + ")"
    return "MULTIPOLYGON(" + ", ".join(
        "(" + ", ".join(f"({pts(r)})" for r in p) + ")" for p in c) + ")"


ROWS = 2000
shapes = [random_shape(i) for i in range(ROWS)]
points = [s for s in shapes if s["type"] == "Point"]

polys = [s for s in shapes if s["type"] == "Polygon" and s["coordinates"][0][0][0] > 30]
big = [s for s in polys if len(s["coordinates"]) > 1]
outlier = next(s for s in shapes if s["type"] == "Point" and s["coordinates"][0] < 30)
def inner(shape, dx=0.0003, size=0.0004):
    x, y = shape["coordinates"][0][0]
    return ring(x + dx, y + dx, size, size)
QUERIES = [
    {"type": "Polygon", "coordinates": [ring(37.600, 55.715, 0.006, 0.006)]},
    {"type": "Polygon", "coordinates": [ring(37.590, 55.705, 0.020, 0.015)]},
    {"type": "Polygon", "coordinates": [ring(37.570, 55.690, 0.090, 0.070)]},
    {"type": "Polygon", "coordinates": [ring(37.585, 55.702, 0.050, 0.040),
                                        list(reversed(ring(37.600, 55.712, 0.020, 0.015)))]},
    {"type": "Polygon", "coordinates": [ring(-5.0, -5.0, 10.0, 10.0)]},
    {"type": "Point", "coordinates": [polys[0]["coordinates"][0][0][0] + 0.0001,
                                      polys[0]["coordinates"][0][0][1] + 0.0001]},
    {"type": "Point", "coordinates": [polys[1]["coordinates"][0][0][0] + 0.0002,
                                      polys[1]["coordinates"][0][0][1] + 0.0002]},
    {"type": "LineString", "coordinates": [[37.580, 55.700], [37.640, 55.745]]},
    {"type": "LineString", "coordinates": [[polys[2]["coordinates"][0][0][0] + 0.0001,
                                            polys[2]["coordinates"][0][0][1] + 0.0001],
                                           [polys[2]["coordinates"][0][0][0] + 0.0003,
                                            polys[2]["coordinates"][0][0][1] + 0.0002]]},
    {"type": "MultiPolygon", "coordinates": [[ring(37.585, 55.705, 0.010, 0.010)],
                                             [ring(37.620, 55.730, 0.012, 0.008)]]},
    {"type": "MultiPoint", "coordinates": [[37.61, 55.72], [37.62, 55.73]]},
    {"type": "Polygon", "coordinates": [inner(polys[0])]},
    {"type": "Polygon", "coordinates": [inner(big[0])]},
    {"type": "Polygon", "coordinates": [inner(big[1], 0.0002, 0.0002)]},
    {"type": "MultiPolygon", "coordinates": [[inner(big[0])], [inner(big[0], 0.0008, 0.0002)]]},
]
CENTROIDS = [[37.610, 55.722], [37.585, 55.703], [37.640, 55.745], outlier["coordinates"]]
RADII = [150, 800, 2500, 6000]


def geo_predicates(field, as_geometry=False):
    preds = []
    for q in QUERIES:
        shape = f"'{lit(json.dumps(q, separators=(',', ':')))}'"
        if as_geometry:
            shape = f"'{wkt(q)}'::GEOMETRY('OGC:CRS84')"
        preds += [f"ST_Intersects({field}, {shape})",
                  f"ST_Intersects({shape}, {field})",
                  f"ST_Contains({shape}, {field})",
                  f"ST_Contains({field}, {shape})"]
    for c in CENTROIDS:
        centroid = f"'{json.dumps({'type': 'Point', 'coordinates': c})}'"
        if as_geometry:
            centroid = f"'POINT({c[0]} {c[1]})'::GEOMETRY('OGC:CRS84')"
        for r in RADII:
            preds.append(f"ST_Distance_Centroid({field}, {centroid}) < {r}")
            preds.append(f"ST_Distance_Centroid({field}, {centroid}) <= {r}")
            for lo in (0, r // 3):
                for incl_min in ("true", "false"):
                    for incl_max in ("true", "false"):
                        preds.append(
                            f"ST_Distance_Between({field}, {centroid}, {lo}, {r}, "
                            f"{incl_min}, {incl_max})")
    return preds


CONTEXTS = ["", " AND id % 3 = 0",
            " AND ST_Intersects(geo, '{\"type\":\"Polygon\",\"coordinates\":"
            "[[[37.595,55.705],[37.630,55.705],[37.630,55.735],[37.595,55.735],[37.595,55.705]]]}')"]


def check(idx, pred, extra):
    deferred = f"SELECT id FROM {idx} WHERE {pred}{extra}"
    inline = f"SELECT id FROM {idx} WHERE (({pred}) OR id < 0){extra}"
    emit("query",
         f"SELECT (SELECT count(*) FROM ({inline})) AS matches, "
         f"(SELECT count(*) FROM (({deferred} EXCEPT {inline}) UNION ALL "
         f"({inline} EXCEPT {deferred}))) AS mismatches",
         "----", "")


emit("# Generated: every deferred geo shape. A required geo predicate is checked",
     "# by a table filter; the same predicate under OR is checked inline. Both",
     "# must return the same rows: mismatches is always 0.",
     "")

DICTS = {
    "gdm_shape": "encode_geojson()",
    "gdm_centroid": "encode_geojson(type := 'centroid')",
    "gdm_point": "encode_geojson(type := 'point')",
    "gdm_s2point": "encode_geojson(coding := 's2point')",
    "gdm_f64": "encode_geojson(coding := 's2latlngf64')",
    "gdm_u32": "encode_geojson(coding := 's2latlngu32')",
    "gdm_centroid_s2": "encode_geojson(type := 'centroid', coding := 's2point')",
    "gdm_geopoint": "encode_geopoint('lat', 'lng')",
}
for name, template in DICTS.items():
    stmt(f"CREATE TEXT SEARCH DICTIONARY {name} AS {template}")

stmt("CREATE TABLE gdm (id INTEGER PRIMARY KEY, geo JSON)")
for start in range(0, ROWS, 100):
    values = ",\n    ".join(
        f"({i}, '{lit(json.dumps(shapes[i], separators=(',', ':')))}')"
        for i in range(start, min(ROWS, start + 100)))
    stmt(f"INSERT INTO gdm VALUES\n    {values}")
JSON_INDEXES = ["gdm_shape", "gdm_centroid", "gdm_point", "gdm_s2point",
                "gdm_f64", "gdm_u32", "gdm_centroid_s2"]
for name in JSON_INDEXES:
    stmt(f"CREATE INDEX {name}_idx ON gdm USING inverted (id, geo {name})")
stmt("VACUUM (REFRESH_TABLE) gdm")

stmt("CREATE TABLE gdm_g (id INTEGER PRIMARY KEY, geo GEOMETRY('OGC:CRS84'))")
for start in range(0, ROWS, 100):
    values = ",\n    ".join(f"({i}, '{wkt(shapes[i])}'::GEOMETRY('OGC:CRS84'))"
                            for i in range(start, min(ROWS, start + 100)))
    stmt(f"INSERT INTO gdm_g VALUES\n    {values}")
stmt("CREATE INDEX gdm_g_source_idx ON gdm_g USING inverted (id, geo gdm_shape)")
stmt("CREATE INDEX gdm_g_s2point_idx ON gdm_g USING inverted (id, geo gdm_s2point)")
stmt("VACUUM (REFRESH_TABLE) gdm_g")

stmt("CREATE TABLE gdm_p (id INTEGER PRIMARY KEY, geo JSON)")
values = ",\n    ".join(
    f"({i}, '{{\"lat\":{s['coordinates'][1]},\"lng\":{s['coordinates'][0]}}}')"
    for i, s in enumerate(points))
stmt(f"INSERT INTO gdm_p VALUES\n    {values}")
stmt("CREATE INDEX gdm_p_idx ON gdm_p USING inverted (id, geo gdm_geopoint)")
stmt("VACUUM (REFRESH_TABLE) gdm_p")

stmt("CREATE TABLE gdm_s (id INTEGER, geo JSON) WITH (storage = 'search')")
stmt("CREATE INDEX gdm_s_idx ON gdm_s USING inverted (id, geo gdm_shape)")
stmt("INSERT INTO gdm_s SELECT id, geo FROM gdm")
stmt("VACUUM (REFRESH_TABLE) gdm_s")

for name in JSON_INDEXES:
    emit(f"# --- JSON column, {DICTS[name]}.", "")
    for pred in geo_predicates("geo"):
        for extra in CONTEXTS:
            check(f"{name}_idx", pred, extra)

for idx in ("gdm_g_source_idx", "gdm_g_s2point_idx"):
    emit(f"# --- GEOMETRY column ({idx}), GeoJSON and GEOMETRY query shapes.", "")
    for pred in geo_predicates("geo") + geo_predicates("geo", as_geometry=True):
        for extra in CONTEXTS[:2]:
            check(idx, pred, extra)

emit("# --- encode_geopoint over latitude and longitude fields.", "")
for pred in geo_predicates("geo"):
    for extra in CONTEXTS[:2]:
        check("gdm_p_idx", pred, extra)

emit("# --- Search table.", "")
for pred in geo_predicates("geo"):
    check("gdm_s_idx", pred, "")

emit("# --- Where each shape is checked.", "")
square = lit(json.dumps(QUERIES[0], separators=(",", ":")))
EXPLAINS = [
    f"SELECT id FROM gdm_shape_idx WHERE ST_Intersects(geo, '{square}')",
    f"SELECT id FROM gdm_shape_idx WHERE ST_Contains('{square}', geo)",
    f"SELECT id FROM gdm_shape_idx WHERE ST_Contains(geo, '{square}')",
    "SELECT id FROM gdm_shape_idx WHERE ST_Distance_Between(geo, "
    "'{\"type\":\"Point\",\"coordinates\":[37.61,55.722]}', 100, 2500, true, false)",
    "SELECT id FROM gdm_shape_idx WHERE ST_Distance_Centroid(geo, "
    "'{\"type\":\"Point\",\"coordinates\":[37.61,55.722]}') < 2500",
    f"SELECT id FROM gdm_u32_idx WHERE ST_Intersects(geo, '{square}') AND id % 3 = 0",
    f"SELECT id FROM gdm_shape_idx WHERE ST_Intersects(geo, '{square}') OR id < 0",
    f"SELECT id FROM gdm_g_s2point_idx WHERE ST_Intersects(geo, '{wkt(QUERIES[0])}'::GEOMETRY('OGC:CRS84'))",
    "SELECT id FROM gdm_p_idx WHERE ST_Distance_Centroid(geo, "
    "'{\"type\":\"Point\",\"coordinates\":[37.61,55.722]}') <= 2500",
    f"SELECT id FROM gdm_s_idx WHERE ST_Intersects(geo, '{square}')",
]
for sql in EXPLAINS:
    emit("query", f"EXPLAIN {sql}", "----", "")

sys.stdout.write("\n".join(out) + "\n")
