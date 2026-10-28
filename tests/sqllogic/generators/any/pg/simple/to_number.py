from sqllogic_gen import Test

READS = [
    "to_number('-34,338,492', '99G999G999')",
    "to_number('-34,338,492.654,878', '99G999G999D999G999')",
    "to_number('<564646.654564>', '999999.999999PR')",
    "to_number('0.00001-', '9.999999S')",
    "to_number('5.01-', 'FM9.999999S')",
    "to_number('5.01-', 'FM9.999999MI')",
    "to_number('5 4 4 4 4 8 . 7 8', '9 9 9 9 9 9 . 9 9')",
    "to_number('.01', 'FM9.99')",
    "to_number('.0', '99999999.99999999')",
    "to_number('0', '99.99')",
    "to_number('.-01', 'S99.99')",
    "to_number('.01-', '99.99S')",
    "to_number(' . 0 1-', ' 9 9 . 9 9 S')",
    "to_number('34,50', '999,99')",
    "to_number('123,000', '999G')",
    "to_number('123456', '999G999')",
    "to_number('$1234.56', 'L9,999.99')",
    "to_number('$1234.56', 'L99,999.99')",
    "to_number('$1,234.56', 'L99,999.99')",
    "to_number('1234.56', 'L99,999.99')",
    "to_number('1,234.56', 'L99,999.99')",
    "to_number('42nd', '99th')",
    "to_number('123456', '99999V99')",
    "to_number('CvIiI', 'rn')",
    "to_number('MMXX  ', 'RN')",
    "to_number('  XIV', '  RN')",
    "to_number('  XIV  ', '  RN')",
    "to_number('M CC', 'RN')",
    "to_number('XIV', 'RN')",
    "to_number('xiv', 'rn')",
    "to_number('MMXX', 'FMRN')",
    "to_number('+5', 'S9')",
    "to_number('5+', '9S')",
    "to_number('5-', '9S')",
    "to_number('+5', 'SG9')",
    "to_number('-5', 'SG9')",
    "to_number('5', 'SG9')",
    "to_number('-1.5', 'S9.9')",
    "to_number('1.5-', '9.9S')",
    "to_number('1-', '9S')",
    "to_number('1+', '9S')",
    "to_number('1x', '9S')",
    "to_number('+5', 'PL9')",
    "to_number('-5', 'MI9')",
    "to_number('5-', '9MI')",
    "to_number('5+', '9PL')",
    "to_number('<5>', '9PR')",
    "to_number('5', '9PR')",
    "to_number('5', 'B9')",
    "to_number('-5', 'B9')",
    "to_number('5', '9C')",
    "to_number('5', '9SP')",
    "to_number('5 4', '9 9')",
    "to_number('5x4', '9x9')",
    "to_number('5é4', '9é9')",
    r"""to_number('5"4', '9\"9')""",
    """to_number('12abc', '99"abc"')""",
    "to_number('1,234', '9,999')",
    "to_number('1234', '9,999')",
    "to_number('1 234', '9G999')",
    "to_number('234', 'FM9,999')",
    "to_number('1,234', 'FM9,999')",
    "to_number('$1', 'L9')",
    "to_number('1', 'L9')",
    "to_number('€1', 'L9')",
    "to_number('$$1', 'LL9')",
    "to_number('12th', '99th')",
    "to_number('12', '99th')",
    "to_number('1st', '9TH')",
    "to_number('12é', '99th')",
    "to_number('1x2', '9th9')",
    "to_number('1.5', '9.9')",
    "to_number('.5', '.9')",
    "to_number('1.', '9.')",
    "to_number('1.55', '9.9')",
    "to_number('1.5', '9D9')",
    "to_number('12', '0000')",
    "to_number('0012', '0000')",
    "to_number(' 12', '9999')",
    "to_number('1', '9V9')",
    "to_number('12', '9V9')",
    "to_number('123', '9V99')",
    "to_number('1', '')",
    "to_number('1', NULL)",
    "to_number(NULL, '9')",
    "to_number('1', NULL::text)",
    "to_number(NULL::text, '9.9')",
    "to_number('1x', '99PL')",
    "to_number('1', NULLIF('9', '9'))",
    "to_number(NULLIF('1', '1'), '9.9')",
    "to_number('5 ', '99')",
    "to_number('+5', '99')",
    "to_number('1.55', '99.9')",
    "to_number('1x', '99S')",
    "to_number('12', '99S')",
    "to_number('1+', '99PL')",
    "to_number('1-', '99MI')",
    "to_number('1x', '99MI')",
    "to_number('5x', '9MI')",
    "to_number('5+', '9MI')",
    "to_number('5-', '9PL')",
]

REJECTED = [
    "to_number('15', '9.9')",
    "to_number('12345', '99.999')",
    "to_number('MM5', 'RN9')",
    "to_number('abc', '999')",
    "to_number('-', 'S9')",
    "to_number('', 'L9')",
    "to_number('1.2', '9.9EEEE')",
    "to_number('CM', 'MIRN')",
    "to_number('CM', 'RNRN')",
    "to_number('', 'RN')",
] + [f"to_number('{roman}', 'RN')" for roman in (
    "12", "XD", "XM", "viv", "DCCCD", "XIXL", "MCCM", "MMMM", "VV", "VX", "IL", "VIX", "LXC", "DCM",
    "MMMDCM", "CLXC", "qiv", " ")]

t = Test("to_number, from PostgreSQL's numeric tests: each text read with its format, every Roman "
         "numeral up to 3999 read back, formats taken from a column or a parameter, and the inputs "
         "PostgreSQL rejects, every answer recorded from PostgreSQL.")
t.section("Each text read with its format.")
for expression in READS:
    t.query(f"SELECT {expression} AS a", ["a"])
t.query("WITH rows AS (SELECT i, to_char(i, 'RN') AS roman FROM generate_series(1, 3999) AS i) "
        "SELECT bool_and(to_number(roman, 'RN') = i) AS valid FROM rows", ["valid"], [["t"]])
t.section("Formats and texts taken from a column or a parameter.")
t.query("SELECT to_number(t, f)::float8 AS a FROM (VALUES ('12.5', '99.9'), ('12.5', '99.9'), "
        "('7', '9'), ('XIV', 'RN')) v(t, f) ORDER BY a", ["a"], 4)
t.query("SELECT to_number(t, '9.99999')::float8 AS a FROM (VALUES ('1.23456'), ('2.5')) v(t) "
        "ORDER BY a", ["a"], 2)
t.query("SELECT to_number(t, 'RN') AS a FROM (VALUES ('XIV'), ('MMXX')) v(t) ORDER BY a", ["a"], 2)
t.statement("PREPARE to_number_format(text) AS SELECT to_number('1.5', $1) AS a")
t.query("EXECUTE to_number_format('9.9')", ["a"])
t.query("EXECUTE to_number_format(NULL)", ["a"])
t.statement("PREPARE to_number_value(text) AS SELECT to_number($1, '9.99') AS a")
t.query("EXECUTE to_number_value('1.25')", ["a"])
t.query("EXECUTE to_number_value(NULL)", ["a"])
t.section("Inputs PostgreSQL rejects.")
for expression in REJECTED:
    t.query(f"SELECT {expression} AS a", ["a"])
t.write()
