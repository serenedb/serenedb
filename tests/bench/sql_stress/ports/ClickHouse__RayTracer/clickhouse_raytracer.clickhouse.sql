CREATE OR REPLACE MACRO tuplePlus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] + ch_ly[1], ch_lx[2] + ch_ly[2], ch_lx[3] + ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMinus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] - ch_ly[1], ch_lx[2] - ch_ly[2], ch_lx[3] - ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMultiplyByNumber(a, s) AS list_transform([a], ch_lx -> list_transform([s], ch_lk -> (ch_lx[1] * ch_lk, ch_lx[2] * ch_lk, ch_lx[3] * ch_lk))[1])[1];
CREATE OR REPLACE MACRO dotProduct(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> ch_lx[1] * ch_ly[1] + ch_lx[2] * ch_ly[2] + ch_lx[3] * ch_ly[3])[1])[1];
CREATE OR REPLACE MACRO L2Norm(a) AS list_transform([a], ch_lx -> sqrt(ch_lx[1] * ch_lx[1] + ch_lx[2] * ch_lx[2] + ch_lx[3] * ch_lx[3]))[1];
CREATE OR REPLACE MACRO L2Normalize(a) AS list_transform([a], ch_lx -> list_transform([sqrt(ch_lx[1] * ch_lx[1] + ch_lx[2] * ch_lx[2] + ch_lx[3] * ch_lx[3])], ch_ln -> (ch_lx[1] / ch_ln, ch_lx[2] / ch_ln, ch_lx[3] / ch_ln))[1])[1];
CREATE OR REPLACE MACRO clamp(x, lo, hi) AS greatest(lo, least(hi, x));
CREATE OR REPLACE MACRO indexOf(a, x) AS coalesce(list_position(a, x), 0);
CREATE OR REPLACE MACRO ch_numbers_n(n) AS TABLE SELECT range AS number FROM range(CAST(n AS BIGINT));
CREATE OR REPLACE MACRO toInt64(x) AS CAST(trunc(x) AS BIGINT);
CREATE OR REPLACE MACRO toInt32(x) AS CAST(trunc(x) AS INTEGER);
CREATE OR REPLACE MACRO toUInt8(x) AS CAST(trunc(x) AS UTINYINT);
CREATE OR REPLACE MACRO ch_u64(x) AS list_transform([CAST(x AS HUGEINT)], ch_lv -> CAST(CASE WHEN ch_lv < 0 THEN ch_lv + CAST('18446744073709551616' AS HUGEINT) ELSE ch_lv END AS UBIGINT))[1];
CREATE OR REPLACE MACRO ch_mul64(a, c) AS CAST((CAST(a AS UHUGEINT) * CAST(c AS UHUGEINT)) % CAST('18446744073709551616' AS UHUGEINT) AS UBIGINT);
CREATE OR REPLACE MACRO ch_shr_xor(x, s) AS list_transform([x], ch_lv -> xor(ch_lv, ch_lv >> s))[1];
CREATE OR REPLACE MACRO ch_intHash64(x) AS ch_shr_xor(ch_mul64(ch_shr_xor(ch_mul64(ch_shr_xor(xor(ch_u64(x), CAST(5544725790478674055 AS UBIGINT)), 33), 18397679294719823053), 33), 14181476777654086739), 33);
CREATE OR REPLACE MACRO ch_hash128to64(lo, hi) AS list_transform([hi], ch_lh -> list_transform([ch_shr_xor(ch_mul64(xor(lo, ch_lh), 11376068507788127593), 47)], ch_la -> ch_mul64(ch_shr_xor(ch_mul64(xor(ch_lh, ch_la), 11376068507788127593), 47), 11376068507788127593))[1])[1];
CREATE OR REPLACE MACRO cityHash64(a, b) AS ch_hash128to64(ch_intHash64(a), ch_intHash64(b)), (a, b, c) AS ch_hash128to64(ch_hash128to64(ch_intHash64(a), ch_intHash64(b)), ch_intHash64(c));
CREATE OR REPLACE MACRO ch_intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO ch_toFloat64(x) AS CAST(x AS DOUBLE);
CREATE OR REPLACE MACRO banner() AS ['.###..##...#.......#....#...#....................', '#...#..#...........#....#...#....................', '#......#...#...###.#.##.#...#..##..#..#..###..##.', '#......#...#..#....#.#..#####.#..#.#..#.#....#..#', '#......#...#..#....##...#...#.#..#.#..#..##..####', '#...#..#...#..#....#.#..#...#.#..#.#..#....#.#...', '.###...##..#...###.#..#.#...#..##...###.###...###'];
CREATE OR REPLACE MACRO bannerW() AS 49;
CREATE OR REPLACE MACRO bannerH() AS 7;
CREATE OR REPLACE MACRO spacing() AS 1.0::DOUBLE;
CREATE OR REPLACE MACRO radius() AS 0.92::DOUBLE;
CREATE OR REPLACE MACRO z0() AS 3.0::DOUBLE;
CREATE OR REPLACE MACRO target() AS (24.0::DOUBLE, 0.0::DOUBLE, 6.0::DOUBLE);
CREATE OR REPLACE MACRO eye() AS (24.0::DOUBLE, -42.0::DOUBLE, 12.0::DOUBLE);
CREATE OR REPLACE MACRO fovDeg() AS 34.0::DOUBLE;
CREATE OR REPLACE MACRO W() AS 96::UBIGINT;
CREATE OR REPLACE MACRO H() AS 40::UBIGINT;
CREATE OR REPLACE MACRO imgW() AS W()::DOUBLE;
CREATE OR REPLACE MACRO imgH() AS H()::DOUBLE;
CREATE OR REPLACE MACRO maxDepth() AS 4;
CREATE OR REPLACE MACRO lightPos() AS (9.0::DOUBLE, -30.0::DOUBLE, 52.0::DOUBLE);
CREATE OR REPLACE MACRO lightR() AS 5.0::DOUBLE;
CREATE OR REPLACE MACRO skyColor() AS (0.42::DOUBLE, 0.60::DOUBLE, 0.95::DOUBLE);
CREATE OR REPLACE MACRO hazeColor() AS (0.66::DOUBLE, 0.78::DOUBLE, 0.96::DOUBLE);
CREATE OR REPLACE MACRO fogStart() AS 35.0::DOUBLE;
CREATE OR REPLACE MACRO fogRange() AS 120.0::DOUBLE;
CREATE OR REPLACE MACRO floorA() AS (0.90::DOUBLE, 0.27::DOUBLE, 0.23::DOUBLE);
CREATE OR REPLACE MACRO floorB() AS (0.93::DOUBLE, 0.93::DOUBLE, 0.95::DOUBLE);
CREATE OR REPLACE MACRO tile() AS 5.0::DOUBLE;
CREATE OR REPLACE MACRO floorAmb() AS 0.30::DOUBLE;
CREATE OR REPLACE MACRO floorDiff() AS 0.85::DOUBLE;
CREATE OR REPLACE MACRO specColor() AS (1.0::DOUBLE, 1.0::DOUBLE, 1.0::DOUBLE);
CREATE OR REPLACE MACRO shininess() AS 60.0::DOUBLE;
CREATE OR REPLACE MACRO reflect_k() AS 0.72::DOUBLE;
CREATE OR REPLACE MACRO csgA() AS (24.0::DOUBLE, 12.0::DOUBLE, 17.5::DOUBLE);
CREATE OR REPLACE MACRO csgAr() AS 6.0::DOUBLE;
CREATE OR REPLACE MACRO csgB() AS (24.0::DOUBLE, 6.6::DOUBLE, 17.0::DOUBLE);
CREATE OR REPLACE MACRO csgBr() AS 4.4::DOUBLE;
CREATE OR REPLACE MACRO exposure() AS 1.15::DOUBLE;
CREATE OR REPLACE MACRO gamma() AS 2.2::DOUBLE;
CREATE OR REPLACE MACRO eps() AS 0.002::DOUBLE;
CREATE OR REPLACE MACRO va(a, b) AS tuplePlus(a, b);
CREATE OR REPLACE MACRO vs(a, b) AS tupleMinus(a, b);
CREATE OR REPLACE MACRO vm(a, s) AS tupleMultiplyByNumber(a, s);
CREATE OR REPLACE MACRO vd(a, b) AS dotProduct(a, b);
CREATE OR REPLACE MACRO vn(a) AS L2Normalize(a);
CREATE OR REPLACE MACRO vc(a, b) AS (a[2]*b[3] - a[3]*b[2], a[3]*b[1] - a[1]*b[3], a[1]*b[2] - a[2]*b[1]);
CREATE OR REPLACE MACRO vref(d, n) AS tupleMinus(d, tupleMultiplyByNumber(n, 2.0::DOUBLE * dotProduct(d, n)));
CREATE OR REPLACE MACRO spheres() AS list_transform(list_filter(flatten(list_transform(range(bannerH()), gy__spheres -> list_transform(range(bannerW()), gx__spheres -> (gx__spheres, gy__spheres)))), c__spheres -> substring(banner()[c__spheres[2] + 1], c__spheres[1] + 1, 1) = '#'), c__spheres -> (c__spheres[1] * spacing(), 0.0::DOUBLE, (ch_toFloat64(bannerH() - 1) - c__spheres[2]) * spacing() + z0()));
CREATE OR REPLACE MACRO camFwd() AS L2Normalize(tupleMinus(target(), eye()));
CREATE OR REPLACE MACRO camRight() AS L2Normalize(vc(L2Normalize(tupleMinus(target(), eye())), (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE)));
CREATE OR REPLACE MACRO camUp() AS vc(camRight(), camFwd());
CREATE OR REPLACE MACRO tanHalf() AS tan(fovDeg() * 0.5::DOUBLE * pi() / 180.0::DOUBLE);
CREATE OR REPLACE MACRO aspect() AS imgW() / imgH();
CREATE OR REPLACE MACRO r2() AS radius() * radius();
CREATE OR REPLACE MACRO sphereT(p, d) AS if(
        dotProduct(p, d) * dotProduct(p, d) - (dotProduct(p, p) - r2()) > 0.0::DOUBLE
        AND -dotProduct(p, d) - sqrt(greatest(dotProduct(p, d) * dotProduct(p, d) - (dotProduct(p, p) - r2()), 0.0::DOUBLE)) > eps(),
        -dotProduct(p, d) - sqrt(greatest(dotProduct(p, d) * dotProduct(p, d) - (dotProduct(p, p) - r2()), 0.0::DOUBLE)),
        1e9);
CREATE OR REPLACE MACRO sphereHits(o, d) AS list_transform(spheres(), c__sphereHits -> sphereT(vs(o, c__sphereHits), d));
CREATE OR REPLACE MACRO sphPair(o, d, c, rad) AS if(
        dotProduct(vs(o,c), d) * dotProduct(vs(o,c), d) - (dotProduct(vs(o,c), vs(o,c)) - rad*rad) > 0.0::DOUBLE,
        (-dotProduct(vs(o,c), d) - sqrt(greatest(dotProduct(vs(o,c), d) * dotProduct(vs(o,c), d) - (dotProduct(vs(o,c), vs(o,c)) - rad*rad), 0.0::DOUBLE)),
         -dotProduct(vs(o,c), d) + sqrt(greatest(dotProduct(vs(o,c), d) * dotProduct(vs(o,c), d) - (dotProduct(vs(o,c), vs(o,c)) - rad*rad), 0.0::DOUBLE))),
        (1e9, -1e9));
CREATE OR REPLACE MACRO planeT(o, d) AS if(d[3] < -eps() AND o[3] > 0.0::DOUBLE, -o[3] / d[3], 1e9);
CREATE OR REPLACE MACRO occluded(o, d, maxd) AS list_bool_or(list_transform(spheres(), c__occluded -> sphereT(vs(o, c__occluded), d) < maxd));
CREATE OR REPLACE MACRO sky(d) AS tupleMultiplyByNumber(skyColor(), 0.35::DOUBLE + 0.65::DOUBLE * pow(greatest(1.0::DOUBLE - d[3], 0.0::DOUBLE), 3.0::DOUBLE));
CREATE OR REPLACE MACRO checker(p) AS (toInt64(floor(p[1] / tile())) + toInt64(floor(p[2] / tile()))) % 2;
CREATE OR REPLACE MACRO rnd(a, b, c) AS (cityHash64(a, b, c) % 1048576) / 1048576.0::DOUBLE;
-- ============================================================================
--  A path tracer in one ClickHouse SQL query.
--
--  Renders the word "ClickHouse" as a cluster of mirrored spheres floating over
--  an infinite checkerboard floor under a hazy sky -- a homage to Andrew
--  Kensler's famous Pixar business-card ray tracer (which spells "aek"/"PIXAR").
--
--  Everything is pure SQL, and the scene is Constructive Solid Geometry (CSG):
--    * The letters are the UNION of ~110 sphere primitives, whose centers are
--      decoded from a 7-row ASCII-art bitmap of the word.
--    * A chrome "planet" hovering over the word is the DIFFERENCE of two spheres
--      (sphere A minus sphere B) -- the carved-out cavity is genuine boolean CSG.
--    * 3D vector math is done on Tuple(DOUBLE, DOUBLE, DOUBLE) via the
--      built-in tuple/vector functions (tuplePlus, dotProduct, L2Normalize, ...),
--      wrapped in short lambda aliases (va, vs, vm, vd, vn, vc, vref).
--    * Light transport is a WITH RECURSIVE CTE: each iteration advances every
--      live ray by exactly one bounce (mirror reflection off the letters and the
--      CSG object), so the query stays a single stage no matter how deep it goes.
--    * Pixels are emitted straight to a PNG via ClickHouse's `PNG` output format
--      (columns r, g, b in [0, 1], one row per pixel in scanline order).
--
--  Run it (image size and samples are runtime parameters):
--      clickhouse local --output_format_image_width 640 --output_format_image_height 256 \
--                       --param_SAMPLES 24 --queries-file clickhouse_raytracer.sql > clickhouse.png
--
--  Quality / speed knobs:
--    --output_format_image_width / _height . resolution (read in SQL with `getSetting`)
--    --param_SAMPLES ....................... samples per pixel (anti-aliasing +
--                                            soft shadows; more = less noise)
--    maxDepth .............................. number of ray bounces (in the WITH prelude)
-- ============================================================================
WITH RECURSIVE
    trace AS (
  (
    SELECT
      toInt32(0)                               AS depth,
      pixel, sample, lpos,
      eye()                                      AS o,
      vn(va(camFwd(), va(vm(camRight(), ndcx), vm(camUp(), ndcy)))) AS d,
      CAST(1.0::DOUBLE AS DOUBLE)                     AS thr,
      CAST((0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE) AS TUPLE(DOUBLE, DOUBLE, DOUBLE)) AS acc,
      toUInt8(1)                               AS alive
    FROM (
      SELECT
        number AS pixel,
        sample,
        va(lightPos(), (lightR() * (rnd(pixel, sample, 3) - 0.5::DOUBLE),
                      lightR() * (rnd(pixel, sample, 4) - 0.5::DOUBLE),
                      lightR() * (rnd(pixel, sample, 5) - 0.5::DOUBLE))) AS lpos,
        (((pixel % W()) + rnd(pixel, sample, 1)) / imgW() * 2.0::DOUBLE - 1.0::DOUBLE) * aspect() * tanHalf() AS ndcx,
        (1.0::DOUBLE - (ch_intDiv(pixel, W()) + rnd(pixel, sample, 2)) / imgH() * 2.0::DOUBLE) * tanHalf()      AS ndcy
      FROM ch_numbers_n(W() * H())
      CROSS JOIN unnest(range(CAST(2 AS UINTEGER))) AS aj(sample)
    )
  )
      UNION ALL
  (
    SELECT
      depth + 1                                AS depth,
      pixel, sample, lpos,
      if(isMirror, shOrig, o)                  AS o,
      if(isMirror, rdir, d)                    AS d,
      if(isMirror, thr * reflect_k(), thr)       AS thr,
      va(acc,
         va(if(isMirror, vm(specColor(), if(isMirror, thr * reflect_k(), thr) * spec), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
            va(if(isFloor, vm(floorCol, if(isMirror, thr * reflect_k(), thr)), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
               if(isMiss, vm(sky(if(isMirror, rdir, d)), if(isMirror, thr * reflect_k(), thr)), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE))))) AS acc,
      if(isMirror, toUInt8(1), toUInt8(0))     AS alive
    FROM (
      /* level 3: shading (occluded computed once, as a column) */
      SELECT
        depth, pixel, sample, lpos, o, d, thr, acc,
        isMirror, isFloor, isMiss, hp, rdir,
        va(hp, vm(nrm, eps() * 2.0::DOUBLE))             AS shOrig,
        occluded(shOrig, ldir, distL - 0.05::DOUBLE)   AS shadowed,
        pow(greatest(vd(ldir, rdir), 0.0::DOUBLE), shininess()) * if(shadowed, 0.0::DOUBLE, 1.0::DOUBLE) AS spec,
        greatest(vd(ldir, (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE)), 0.0::DOUBLE) * if(shadowed, 0.0::DOUBLE, 1.0::DOUBLE)      AS lambert,
        least(1.0::DOUBLE, greatest(0.0::DOUBLE, (tHit - fogStart()) / fogRange())) AS fog,
        va(vm(vm(if(checker(hp) = 0, floorA(), floorB()), floorAmb() + floorDiff() * lambert), 1.0::DOUBLE - fog),
           vm(hazeColor(), fog))                 AS floorCol
      FROM (
        /* level 2: nearest hit, geometry, surface vectors. Mirror surfaces are
           the text spheres OR the CSG object (sphere csgA minus sphere csgB). */
        SELECT
          depth, pixel, sample, lpos, o, d, thr, acc, alive, tP,
          list_min(hits)                       AS tS,
          /* CSG A-B: enter A where not inside B, or exit B (the cavity wall) where inside A */
          if(pairA[1] > eps() AND pairA[1] < 1e8 AND NOT (pairB[1] <= pairA[1] AND pairA[1] <= pairB[2]),
             pairA[1], 1e9)                     AS cEnter,
          if(pairB[1] < 1e8 AND pairB[2] > eps() AND pairA[1] <= pairB[2] AND pairB[2] <= pairA[2],
             pairB[2], 1e9)                     AS cCav,
          least(cEnter, cCav)                  AS tC,
          least(tS, tC)                        AS tM,
          least(tM, tP)                        AS tHit,
          va(o, vm(d, tHit))                   AS hp,
          /* normal of the chosen mirror surface */
          if(tC < tS,
             if(cEnter <= cCav, vn(vs(hp, csgA())), vn(vs(csgB(), hp))),
             vn(vs(hp, spheres()[indexOf(hits, tS)]))) AS sn,
          alive AND (tM < tP) AND (tM < 1e8)   AS isMirror,
          alive AND (tP <= tM) AND (tP < 1e8)  AS isFloor,
          alive AND (tM >= 1e8) AND (tP >= 1e8) AS isMiss,
          if(isFloor, (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE), sn)     AS nrm,
          vref(d, sn)                          AS rdir,
          vn(vs(lpos, hp))                     AS ldir,
          L2Norm(vs(lpos, hp))                 AS distL
        FROM (
          /* level 1: ray-vs-scene distances */
          SELECT
            depth, pixel, sample, lpos, o, d, thr, acc, alive,
            sphereHits(o, d)                   AS hits,
            planeT(o, d)                       AS tP,
            sphPair(o, d, csgA(), csgAr())         AS pairA,
            sphPair(o, d, csgB(), csgBr())         AS pairB
          FROM trace
          WHERE alive AND depth < maxDepth()
        )
      )
    )
  )
    )
SELECT
    clamp(pow(greatest(avg(col[1]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS r,
    clamp(pow(greatest(avg(col[2]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS g,
    clamp(pow(greatest(avg(col[3]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS b,
    pixel % W()      AS x,                            /* explicit PNG coordinates: no ORDER BY needed */
    ch_intDiv(pixel, W()) AS y
FROM (
  /* per sample, the terminal ray (max depth) holds the fully accumulated color */
  SELECT pixel, arg_max(va(acc, if(alive, vm(sky(d), thr), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE))), depth) AS col
  FROM trace
  GROUP BY pixel, sample
)
GROUP BY pixel
