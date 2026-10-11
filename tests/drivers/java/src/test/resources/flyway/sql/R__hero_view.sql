CREATE OR REPLACE VIEW hero_summary AS
SELECT g.name AS guild, count(h.id) AS heroes, sum(h.gold) AS gold
FROM guild g
LEFT JOIN hero h ON h.guild_id = g.id
GROUP BY g.name;
