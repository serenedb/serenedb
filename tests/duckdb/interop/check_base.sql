SELECT o.main.add1(41) AS a, (SELECT count(*) FROM o.main.tm(5)) AS c;
SELECT name, score, gen FROM o.main.t WHERE id = 4242;
SELECT id FROM o.main.t WHERE name = 'n77';
SELECT count(*) AS n FROM o.main.t WHERE score BETWEEN 10 AND 20;
