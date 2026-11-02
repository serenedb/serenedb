from sqllogic_gen import Test
from temporal_gen import diff, labelled

t = Test("strftime formats (ISO weeks, 12-hour clocks, names, padding, time zone "
         "offsets) and month and day names of timestamps, dates and zoned "
         "timestamps, with infinities and over 1950-2060: every grouping returns "
         "the same keys, counts and sums with compressed materialization off and "
         "on.")

COLUMNS = ("ts", "d", "tstz")
FORMATS = (
    "%Y", "%Y-%m", "%Y-%m-%d", "%Y-%m-%d %H", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%j", "%Y %B %d %A",
    "%d.%m.%Y", "%Y/%b", "%m", "%Y-%m-%d %I %p", "%Y-%m-%d %I", "%Y-%U", "%Y-%W", "%G-%V", "%G-W%V", "%G-%V-%u",
    "%G-%V %A", "%G-%V %w", "%Y-%V", "%G-%m", "%G", "%V", "%B %V", "%G-%V %H", "%y-%m", "%y-%m-%d", "%y", "%y-%j",
    "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %z", "%c", "%x", "%Y-%m-%d %p", "%-d/%-m/%Y", "%B %Y", "%A", "%a %d %b %Y",
    "%Y-%m-%dT%H:%M", "%H", "%Y-%m-%d %-I%p",
)
INFINITE = ("%Y-%m", "%Y-%m-%d", "%G-%V", "%y-%m", "%Y-%m-%d %H")
WIDE = ("%Y-%m", "%y-%m", "%y", "%G-%V", "%Y", "%Y-%m-%d")
NAMES = (("mn", "monthname"), ("dn", "dayname"), ("ld", "last_day"))

formats = [(c, f) for c in COLUMNS for f in FORMATS]
shapes = [labelled(f"f{n} {c} {f}", f"strftime({c}, '{f}')", "tb") for n, (c, f) in enumerate(formats, 1)]
shapes += [labelled(f"{short} {c}", f"{name}({c})", "tb") for c in COLUMNS for short, name in NAMES]
infinite = []
for c in COLUMNS:
    infinite += [labelled(f"inf {c} {f}", f"strftime({c}, '{f}')", "tb_inf") for f in INFINITE]
    infinite += [labelled(f"inf {short} {c}", f"{name}({c})", "tb_inf") for short, name in NAMES]
wide = [labelled(f"wide {c} {f}", f"strftime({c}, '{f}')", "tb_wide") for c in COLUMNS for f in WIDE]

t.statement("SET TimeZone = 'Europe/Berlin';")
t.statement("CREATE TABLE tb AS SELECT TIMESTAMP '2024-12-23 00:00:00' + INTERVAL (i * 37) MINUTE AS ts, "
            "(TIMESTAMP '2024-12-23 00:00:00' + INTERVAL (i * 37) MINUTE)::TIMESTAMPTZ AS tstz, "
            "(DATE '2024-12-23' + (i % 400)::INTEGER)::DATE AS d, i AS v FROM range(0, 1500) t(i);")
t.statement("INSERT INTO tb VALUES (NULL, NULL, NULL, -1);")
t.statement("CREATE TABLE tb_inf AS SELECT * FROM tb;")
t.statement("INSERT INTO tb_inf VALUES (TIMESTAMP 'infinity', TIMESTAMPTZ 'infinity', DATE 'infinity', -2), "
            "(TIMESTAMP '-infinity', TIMESTAMPTZ '-infinity', DATE '-infinity', -3);")
t.statement("CREATE TABLE tb_wide AS SELECT TIMESTAMP '1950-06-01 00:00:00' + INTERVAL (i * 29) DAY AS ts, "
            "(TIMESTAMP '1950-06-01 00:00:00' + INTERVAL (i * 29) DAY)::TIMESTAMPTZ AS tstz, "
            "(DATE '1950-06-01' + (i * 29)::INTEGER)::DATE AS d, i AS v FROM range(0, 1400) t(i);")
for name, selects in (("tb", shapes), ("inf", infinite), ("wide", wide)):
    diff(t, name, selects, "compressed_materialization", ["shape", "k", "c", "s"])

t.write()
