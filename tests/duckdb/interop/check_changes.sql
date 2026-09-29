SELECT o.s2.twice(21) AS a, (SELECT count(*) FROM o.main.tm(5)) AS c;
SELECT name, score, gen FROM o.main.t WHERE id = 4242;
SELECT id, name, score FROM o.main.t WHERE id = 100000;
SELECT k FROM o.s2.w WHERE i = 1234;
SELECT i FROM o.s2.w WHERE k = 'w77';
SELECT count(*) AS n, count(DISTINCT yy) AS yys, max(z) AS max_z FROM o.s.other2;
