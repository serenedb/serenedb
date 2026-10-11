CREATE FUNCTION hero_power(lvl integer, gold numeric) RETURNS numeric
LANGUAGE sql
AS $$
    SELECT lvl * 100 + gold
$$;
