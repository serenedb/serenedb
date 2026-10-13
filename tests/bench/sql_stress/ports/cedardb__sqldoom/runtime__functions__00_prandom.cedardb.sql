CREATE OR REPLACE FUNCTION doom_prandom(p_actor bigint, p_tic bigint,
                                       p_use bigint) RETURNS int
LANGUAGE sql IMMUTABLE AS $prnd$
-- Doom's P_Random, as far as a set-based engine can have one.
--
-- m_random.c draws rndtable[++prndindex & 0xff]: one global table, walked in
-- whatever order the thinker list runs. There is no such order here -- every
-- monster on the map is one row of one statement, evaluated as a set -- so a
-- walking index cannot be reproduced, and anything recorded against it could
-- never be replayed. The table below is Doom's; the index into it is derived
-- instead, from (actor, tic, call site). That is reproducible by
-- construction: the same world, ticked again on the same input, draws the
-- same numbers however the rows happen to be evaluated.
--
-- p_use separates call sites, so two draws for one actor in one tic -- a
-- shot's spread and its damage roll, say -- are independent rather than equal.
--
-- The three multipliers are large odd 32-bit constants and the sum goes
-- through an xor-shift-multiply before a byte is taken out of the middle.
-- Small multipliers are not enough on their own: a tic counter times 40503
-- leaves the first tics below the bits being extracted, and tic 0 and tic 1
-- drew the same number. The final multiplier is deliberately 16-bit so the
-- product stays inside a signed 64-bit integer.
  SELECT get_byte(
    '\x00086ddcdef1956b4bf8fe8c10424a15d32f50f29a1bcd80a1594d245f6e5530d48cd3f9164fc8321cbc348cca7844913e46b8be5bc598e0956819b2fcb6cab68dc50451b5f2912a27e39cc6e1c1db5d7aaff900af8f46ef2ef6a335a36da88702eb195c14918a4d45a64eb0add4a6715ea12932ef316fa4463c0225ab4b889c0b382a928ae549924d3d62c4876a3fc5c35660cb7165aaf7b57150fa6c07ffed81e24f6b70a667f118dfef78c63a3c528003b8428fe091e051cea32d3f5aa8723b219f5f1c8b7b627dc40f46c2fd360e6de24711a15dba57f48a14347bfb1a24112e34e7e84c1fdd5425d8a5d46ac5f2622b27affe91be5476debb8878a3ecf9'::bytea,
    ((xor(((ABS(p_actor) * 2654435761 + ABS(p_tic) * 2246822519
        + ABS(p_use) * 3266489917) % 4294967296)
      , (((ABS(p_actor) * 2654435761 + ABS(p_tic) * 2246822519
        + ABS(p_use) * 3266489917) % 4294967296) / 32768)
     )) * 40503 % 4294967296 / 256 % 256)::int)
$prnd$;
