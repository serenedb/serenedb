from sqllogic_gen import RECORDED, Test

NUMERIC_IDS = [0, 2, 3, 4, 5, 6, 7, 8, 9]
NUMERIC_FORMATS = [
    "'9G999G999G999G999G999'",
    "'9G999G999G999G999G999D999G999G999G999G999'",
    "'9999999999999999.999999999999999PR'",
    "'9999999999999999.999999999999999S'",
    "'MI9999999999999999.999999999999999'",
    "'FMS9999999999999999.999999999999999'",
    "'FM9999999999999999.999999999999999THPR'",
    "'SG9999999999999999.999999999999999th'",
    "'0999999999999999.999999999999999'",
    "'S0999999999999999.999999999999999'",
    "'FM0999999999999999.999999999999999'",
    "'FM9999999999999999.099999999999999'",
    "'FM9999999999990999.990999999999999'",
    "'FM0999999999999999.999909999999999'",
    "'FM9999999990999999.099999999999999'",
    "'L9999999999999999.099999999999999'",
    "'FM9999999999999999.99999999999999'",
    "'FM9999999999999999.999999999999999'",
    "'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9'",
    "'FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9'",
    r"""E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999'""",
    "'999999SG9999999999'",
    "'9.999EEEE'",
    "'FMRN'",
]

FLOAT_SPECIALS = ("(VALUES (1, '0'::float8), (2, '-4.2'), (3, '4.2e9'), (4, '1.2e-5'), (5, 'inf'), "
                  "(6, '-inf'), (7, 'nan')) v(id, val)")
FLOAT_SPECIAL_EXPRESSIONS = [
    f"to_char({value}, '{form}')" for form in ("9.999EEEE", "MI9999999999.99", "MI99.99")
    for value in ("val", "val::float4")]

EXPONENTS = "(VALUES (-10), (-9), (-5), (-1), (0), (1), (5), (9), (20)) v(e)"
EXPONENT_EXPRESSIONS = ["to_char(('1.2345e' || e)::numeric(38,10), '9.999EEEE')",
                        "to_char(('-1.2345e' || e)::numeric(38,10), '9.99EEEE')"]

PAIRS = ("(VALUES (123::int8, 456::int8), (123, 4567890123456789), (4567890123456789, 123), "
         "(4567890123456789, 4567890123456789), (4567890123456789, -4567890123456789)) v(q1, q2)")
PAIR_EXPRESSIONS = [
    "to_char(q1, '9G999G999G999G999G999')",
    "to_char(q2, '9,999,999,999,999,999')",
    "to_char(q1, '9G999G999G999G999G999D999G999')",
    "to_char(q2, '9,999,999,999,999,999.999,999')",
    "to_char(q1 * -1, '9999999999999999PR')",
    "to_char(q2 * -1, '9999999999999999.999PR')",
    "to_char(q1 * -1, '9999999999999999S')",
    "to_char(q2 * -1, 'S9999999999999999')",
]

BIGINTS = "(VALUES (456::int8), (4567890123456789), (123), (-4567890123456789)) v(q2)"
BIGINT_FORMATS = [
    "'MI9999999999999999'",
    "'9999999999999999PL'",
    "'FMS9999999999999999'",
    "'FM9999999999999999THPR'",
    "'SG9999999999999999th'",
    "'0999999999999999'",
    "'S0999999999999999'",
    "'FM0999999999999999'",
    "'FM9999999999999999.000'",
    "'L9999999999999999.000'",
    "'FM9999999999999999.999'",
    "'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9'",
    r"""E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999'""",
    "'999999SG9999999999'",
    "'FMRN'",
]

GRIDS = [
    ("(VALUES (1), (2), (3), (4), (11), (12), (13), (21), (101), (-1)) v(n)", "n", 10,
     ["to_char(n, '9999th')", "to_char(n, '9999TH')", "to_char(n, 'FM9999th')"]),
    ("(VALUES (0), (-1), (1), (4), (9), (14), (40), (90), (400), (900), (1994), (3999), (4000)) v(n)",
     "n", 13, ["to_char(n, 'RN')", "to_char(n, 'FMRN')", "to_char(n, 'rn')"]),
    ("(VALUES (0::int8), (3999), (2147483648), (-2147483649)) v(n)", "n", 4, ["to_char(n, 'RN')"]),
    ("(VALUES (1, 0.4::float8), (2, 0.5), (3, 3999.4), (4, 3999.5), (5, 'NaN'), (6, 'Infinity'), "
     "(7, '-Infinity'), (8, 2147483647.0), (9, -2147483648.0), (10, -2147483649.0)) v(id, n)", "id",
     10, ["to_char(n, 'RN')", "to_char(n::float4, 'RN')"]),
    ("(VALUES (1, 0.4::numeric(38,10)), (2, 0.5), (3, 3999.4), (4, 3999.5), (5, 12345678901), "
     "(6, -12345678901), (7, 2147483647.5)) v(id, n)", "id", 7, ["to_char(n, 'RN')"]),
    ("(VALUES (0), (1), (-1), (12345), (-1234567), (2147483647)) v(n)", "n", 6,
     ["to_char(n, '9.99EEEE')", "to_char(n, '9EEEE')", "to_char(n::int8, '9.999EEEE')",
      "to_char(n::float8, '9.99EEEE')", "to_char(n::numeric(38,10), '9.99EEEE')"]),
    ("(VALUES (1, 0::float8), (2, 1.5e-10), (3, -4.2e30), (4, 'NaN'), (5, 'Infinity'), "
     "(6, '-Infinity')) v(id, n)", "id", 6,
     ["to_char(n, '9.999EEEE')", "to_char(n, '99.9999999999999999999999999999999999EEEE')",
      "to_char(n::float4, '9.999EEEE')"]),
    ("(VALUES (12), (-12), (0)) v(n)", "n", 3,
     ["to_char(n, '9999V99')", "to_char(n, '9V9999999999')", "to_char(n, '9V99999999999999999999')"]),
    ("(VALUES (1, 12.345::float8), (2, -12.345), (3, 0)) v(id, n)", "id", 3,
     ["to_char(n, '9999V99')", "to_char(n, '9V999999999')", "to_char(n::float4, '9999V99')"]),
    ("(VALUES (1, 12.345::numeric(10,3)), (2, -12.345), (3, 0)) v(id, n)", "id", 3,
     ["to_char(n, '9999V99')", "to_char(n, '9V999999999')"]),
    ("(VALUES (1, 1.5::float8), (2, 123456789012345678.0), (3, '1e20'), (4, 0.1)) v(id, n)", "id", 4,
     ["to_char(n, 'FM99999999999999999999.999999999999999')",
      "to_char(n, '999999999999999999999.99')"]),
    ("(VALUES (1, 1.5::float4), (2, 12345678.0::float4), (3, '1e20'::float4), (4, 0.1::float4), "
     "(5, 123.456::float4)) v(id, n)", "id", 5,
     ["to_char(n, 'FM99999999999999999999.999999999999999')", "to_char(n, '999999999.99')"]),
    ("(VALUES (1, -2147483649::numeric(38,10)), (2, 2147483648), (3, -2147483648.4)) v(id, n)", "id",
     3, ["to_char(n, 'RN')"]),
    ("(VALUES (1, 1.25::numeric(4,2)), (2, -9.95)) v(id, n)", "id", 2,
     ["to_char(n, '9.9')", "to_char(n, 'FM00.00')"]),
    ("(VALUES (1, 1234567.25::numeric(9,2)), (2, -0.05)) v(id, n)", "id", 2,
     ["to_char(n, '9999999.99')", "to_char(n, 'FM0000000.0')"]),
    ("(VALUES (1, 1234567890123456.25::numeric(18,2)), (2, -0.05)) v(id, n)", "id", 2,
     ["to_char(n, '999999999999999.99')"]),
]

SCALARS = [
    "to_char('100'::numeric, 'FM999.9')",
    "to_char('100'::numeric, 'FM999.')",
    "to_char('100'::numeric, 'FM999')",
    "to_char('12345678901'::float8, 'FM9999999999D9999900000000000000000')",
    "to_char('100'::numeric, 'rn')",
    "to_char('1234'::numeric, 'rn')",
    "to_char('1235'::float4, 'rn')",
    "to_char('1236'::float8, 'rn')",
    "to_char('1237'::float8, 'fmrn')",
    "to_char('100e9'::numeric(38,10), 'RN')",
    "to_char('100e9'::float4, 'RN')",
    "to_char('100e9'::float8, 'RN')",
    "to_char(1234.56::numeric, '99999V99')",
    "to_char(1234.56::float4, '99999V99')",
    "to_char(1234.56::float8, '99999V99')",
    "to_char('100'::numeric, 'foo999')",
    r"to_char('100'::numeric, 'f\oo999')",
    r"to_char('100'::numeric, 'f\\oo999')",
    r"""to_char('100'::numeric, 'f\"oo999')""",
    r"""to_char('100'::numeric, 'f\\"oo999')""",
    """to_char('100'::numeric, 'f"ool"999')""",
    r"""to_char('100'::numeric, 'f"\ool"999')""",
    r"""to_char('100'::numeric, 'f"\\ool"999')""",
    r"""to_char('100'::numeric, 'f"ool\"999')""",
    r"""to_char('100'::numeric, 'f"ool\\"999')""",
    "to_char(1234, '9.99EEEE')",
    "to_char(1234::int8, '9.99eeee')",
    "to_char(-1234::int8, '9.99eeee')",
    "to_char(1234, '99999V99')",
    "to_char(1234::int8, '99999V99')",
    "to_char(12, 'B99')",
    "to_char(12, '9B9')",
    "to_char(1.5, '.9B')",
    "to_char(12, '99C')",
    "to_char(12, '99SP')",
    "to_char(12, 'L99')",
    "to_char(-12, 'SG99')",
    "to_char(12, 'SG99')",
    "to_char(12, '99SG')",
    "to_char(0, 'FM9.99')",
    "to_char(0.5, 'FM9.99')",
    "to_char(0.5, 'FM0.99')",
    "to_char(0, 'FM0.00')",
    "to_char(0::float8, 'FM9.99')",
    "to_char(0.5::float8, 'FM9.99')",
    "to_char(-0.5, 'FM9.99')",
    "to_char(0, '9.99')",
    "to_char(0.5, '9.99')",
    "to_char('NaN'::float8, 'FM999.99')",
    "to_char('NaN'::float8, 'FM9.99')",
    "to_char('Infinity'::float8, 'FM99999999.99')",
    "to_char(5, 'RN9')",
    "to_char(5, '9RN')",
    "to_char(5, 'FMRN99')",
    "to_char(5, 'FM9,999')",
    "to_char(5, 'FM9G999')",
    "to_char(5, 'RNth')",
    "to_char(1234, '99th')",
    "to_char(5, 'FM9MI')",
    "to_char(-5, 'FM9PL')",
    "to_char(5, '9MI')",
    "to_char(-5, '9PL')",
    "to_char(1.5::float8, '9.' || repeat('9', 400) || 'EEEE') = ' 1.5' || repeat('0', 399) || 'e+00'",
    "to_char(7, '9.' || repeat('9', 400) || 'EEEE') = ' 7.' || repeat('0', 400) || 'e+00'",
    "to_char(7::float4, '9.' || repeat('9', 400) || 'EEEE') = ' 7.' || repeat('0', 400) || 'e+00'",
    "md5(to_char(5e-324::float8, '9.' || repeat('9', 400) || 'EEEE'))",
    "to_char(1, '')",
    "to_char(1::int8, '')",
    "to_char(1.5::float8, '')",
    "to_char(1.5, '')",
    "to_char(1.5::float4, '')",
]

INVALID = [
    "to_char(1, '9.99EEEE9')",
    "to_char(1, '9.99EEEEEEEE')",
    "to_char(1, '9PR9')",
    "to_char(1, '9PR0')",
    "to_char(1, '9.9.9')",
    "to_char(1, '9D9.9')",
    "to_char(1, '9V9.9')",
    "to_char(1, '9.9V9')",
    "to_char(1, 'S9S')",
    "to_char(1, 'PL9S')",
    "to_char(1, 'MI9S')",
    "to_char(1, '9PRS')",
    "to_char(1, 'S9MI')",
    "to_char(1, 'S9PL')",
    "to_char(1, 'S9SG')",
    "to_char(1, 'S9PR')",
    "to_char(1, 'PL9PR')",
    "to_char(1, 'MI9PR')",
    "to_char(1, 'RNRN')",
    "to_char(1, 'B9.99EEEE')",
    "to_char(1, 'FM9.99EEEE')",
    "to_char(1, 'S9.99EEEE')",
    "to_char(1, '9.99PREEEE')",
    "to_char(1, 'MI9.99EEEE')",
    "to_char(1, 'PL9.99EEEE')",
    "to_char(1, 'RNEEEE')",
    "to_char(1, '9V9EEEE')",
    "to_char(1, '.RN')",
    "to_char(5, 'RN0')",
    "to_char(1, 'SRN')",
    "to_char('NaN'::float8, '999th')",
    "to_char('Infinity'::float8, '9999999999th')",
    "to_char(12::int8, '9V999999999999999999')",
    "to_char(12::int8, '9V99999999999999999999')",
    "to_char(9223372036854775807::int8, '9V9')",
]

t = Test("to_char of numbers, from PostgreSQL's numeric, int8 and float tests: each value list "
         "under each format of its group, the one-off cases and the formats PostgreSQL rejects, "
         "every answer recorded from PostgreSQL.")
t.statement("SET lc_monetary = 'C'", onlyif="postgres")
t.statement("CREATE TABLE to_char_num (id int4, val numeric(38,10))")
t.statement("INSERT INTO to_char_num VALUES (0, 0), (2, -34338492.215397047), (3, 4.31), "
            "(4, 7799461.4119), (5, 16397.038491), (6, 93901.57763026), (7, -83028485), (8, 74881), "
            "(9, -24926804.045047420)")
t.section("Every numeric of the table under each format.")
for form in NUMERIC_FORMATS:
    t.query(f"SELECT id, to_char(val, {form}) AS a FROM to_char_num ORDER BY id", ["id", "a"],
            [[i, RECORDED] for i in NUMERIC_IDS])
t.section("Special floats, exponents and bigint pairs.")
for expression in FLOAT_SPECIAL_EXPRESSIONS:
    t.query(f"SELECT id, {expression} AS a FROM {FLOAT_SPECIALS} ORDER BY id", ["id", "a"],
            [[i, RECORDED] for i in range(1, 8)])
for expression in EXPONENT_EXPRESSIONS:
    t.query(f"SELECT e, {expression} AS a FROM {EXPONENTS} ORDER BY e", ["e", "a"], 9)
for expression in PAIR_EXPRESSIONS:
    t.query(f"SELECT q1, q2, {expression} AS a FROM {PAIRS} ORDER BY q1, q2", ["q1", "q2", "a"], 5)
for form in BIGINT_FORMATS:
    t.query(f"SELECT q2, to_char(q2, {form}) AS a FROM {BIGINTS} ORDER BY q2", ["q2", "a"], 4)
t.section("Value lists under the formats of their group.")
for values, key, rows, expressions in GRIDS:
    for expression in expressions:
        t.query(f"SELECT {key}, {expression} AS a FROM {values} ORDER BY {key}", [key, "a"], rows)
t.section("One-off values and formats.")
for expression in SCALARS:
    t.query(f"SELECT {expression} AS a", ["a"])
t.query("SELECT n, f, to_char(n, f) AS a FROM (VALUES (1, '999'), (1, '999'), (2, '0000'), "
        "(3, 'FM9th')) v(n, f) ORDER BY n, f", ["n", "f", "a"], 4)
t.section("Formats PostgreSQL rejects.")
for expression in INVALID:
    t.query(f"SELECT {expression} AS a", ["a"])
t.write()
