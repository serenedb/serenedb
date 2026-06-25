CREATE DATABASE IF NOT EXISTS chtest;

CREATE TABLE chtest.widgets
(
    id    UInt32,
    name  String,
    price Decimal(10, 2),
    stock Int32,
    tags  Array(String),
    note  Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO chtest.widgets (id, name, price, stock, tags, note) VALUES
    (1, 'gadget',      9.99,  10, ['red', 'small'],   'in stock'),
    (2, 'gizmo',      19.99,  0,  ['blue'],           NULL),
    (3, 'thingamajig', 4.50,  5,  [],                 'low'),
    (4, 'gadget',     14.99,  3,  ['green', 'large'], NULL),
    (5, 'widget',     99.95,  42, ['red'],            'flagship');

CREATE TABLE chtest.types_all
(
    c_uint8       UInt8,
    c_int64       Int64,
    c_float64     Float64,
    c_bool        Bool,
    c_string      String,
    c_fixedstr    FixedString(4),
    c_date        Date,
    c_datetime    DateTime,
    c_datetime64  DateTime64(3),
    c_decimal     Decimal(18, 4),
    c_enum8       Enum8('a' = 1, 'b' = 2),
    c_uuid        UUID,
    c_ipv4        IPv4,
    c_array_int32 Array(Int32),
    c_map         Map(String, Int32),
    c_lowcard     LowCardinality(String),
    c_nullable    Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY c_uint8;

INSERT INTO chtest.types_all
(
    c_uint8, c_int64, c_float64, c_bool, c_string, c_fixedstr,
    c_date, c_datetime, c_datetime64, c_decimal, c_enum8, c_uuid,
    c_ipv4, c_array_int32, c_map, c_lowcard, c_nullable
) VALUES
    (
        1, 9223372036854775807, 3.5, true, 'hello', 'abcd',
        '2021-01-01', '2021-01-01 12:00:00', '2021-01-01 12:00:00.123',
        12345.6789, 'a', '00000000-0000-0000-0000-000000000001',
        '192.168.0.1', [1, 2, 3], {'x': 1, 'y': 2}, 'low', 42
    ),
    (
        2, -9223372036854775807, -1.25, false, 'world', 'wxyz',
        '2022-12-31', '2022-12-31 23:59:59', '2022-12-31 23:59:59.999',
        -0.0001, 'b', '00000000-0000-0000-0000-000000000002',
        '10.0.0.255', [], {}, 'high', NULL
    );

CREATE TABLE chtest.vedernikoff
(
    id  Int64,
    val String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO chtest.vedernikoff (id, val) VALUES
    (1, 'clickclack babsky'),
    (2, 'deadinside kostya'),
    (3, 'vedernikoff pudge'),
    (4, 'pudge goes mid'),
    (5, 'vedernikoff kostya'),
    (6, 'anchin reads manga');

-- A user whose password contains a SPACE, for the FDW connection-string
-- escaping + credential-redaction tests (the default user, opened with no
-- password by CLICKHOUSE_SKIP_USER_SETUP, rejects a non-empty password).
CREATE USER IF NOT EXISTS scanner IDENTIFIED WITH plaintext_password BY 'pass word';
GRANT SELECT ON chtest.* TO scanner;
