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
CREATE OR REPLACE MACRO ch_intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO ch_toFloat64(x) AS CAST(x AS DOUBLE);
CREATE OR REPLACE MACRO banner() AS ['.###..##...#.......#....#...#....................', '#...#..#...........#....#...#....................', '#......#...#...###.#.##.#...#..##..#..#..###..##.', '#......#...#..#....#.#..#####.#..#.#..#.#....#..#', '#......#...#..#....##...#...#.#..#.#..#..##..####', '#...#..#...#..#....#.#..#...#.#..#.#..#....#.#...', '.###...##..#...###.#..#.#...#..##...###.###...###'];
CREATE OR REPLACE MACRO bannerW() AS 49;
CREATE OR REPLACE MACRO bannerH() AS 7;
CREATE OR REPLACE MACRO spacing() AS 1.0::DOUBLE;
CREATE OR REPLACE MACRO radius() AS 0.92::DOUBLE;
CREATE OR REPLACE MACRO z0() AS 3.0::DOUBLE;
CREATE OR REPLACE MACRO target() AS (24.0::DOUBLE, 0.0::DOUBLE, 6.0::DOUBLE);
CREATE OR REPLACE MACRO t() AS CAST(12 AS UINTEGER)::DOUBLE / CAST(48 AS UINTEGER)::DOUBLE;
CREATE OR REPLACE MACRO theta() AS 0.610865::DOUBLE * sin(2.0::DOUBLE * pi() * t());
CREATE OR REPLACE MACRO orbitR() AS 42.0::DOUBLE;
CREATE OR REPLACE MACRO slideX() AS 0.0::DOUBLE;
CREATE OR REPLACE MACRO slideY() AS 0.0::DOUBLE;
CREATE OR REPLACE MACRO slideZ() AS 0.0::DOUBLE;
CREATE OR REPLACE MACRO eye() AS (24.0::DOUBLE + orbitR() * sin(theta()), -orbitR() * cos(theta()), 12.0::DOUBLE);
CREATE OR REPLACE MACRO fovDeg() AS 34.0::DOUBLE;
CREATE OR REPLACE MACRO W() AS 80::UBIGINT;
CREATE OR REPLACE MACRO H() AS 32::UBIGINT;
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
CREATE OR REPLACE MACRO csgA() AS (24.0::DOUBLE + slideX(), 12.0::DOUBLE + slideY(), 17.5::DOUBLE + slideZ());
CREATE OR REPLACE MACRO csgAr() AS 6.0::DOUBLE;
CREATE OR REPLACE MACRO csgB() AS (24.0::DOUBLE + slideX(), 6.6::DOUBLE + slideY(), 17.0::DOUBLE + slideZ());
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
CREATE OR REPLACE MACRO spheres() AS list_transform(list_filter(flatten(list_transform(range(bannerH()), gy__spheres -> list_transform(range(bannerW()), gx__spheres -> (gx__spheres, gy__spheres)))), c__spheres -> substring(banner()[c__spheres[2] + 1], c__spheres[1] + 1, 1) = '#'), c__spheres -> (c__spheres[1] * spacing() + slideX(), 0.0::DOUBLE + slideY(), (ch_toFloat64(bannerH() - 1) - c__spheres[2]) * spacing() + z0() + slideZ()));
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
CREATE OR REPLACE MACRO rnd(a, b, c) AS (hash_city64(a, b, c) % 1048576) / 1048576.0::DOUBLE;
CREATE OR REPLACE MACRO oneBounce(s, lpos) AS list_transform([s[1]], o__oneBounce -> list_transform([s[2]], d__oneBounce -> list_transform([s[3]], thr__oneBounce -> list_transform([s[4]], acc__oneBounce -> list_transform([sphereHits(o__oneBounce, d__oneBounce)], hits__oneBounce -> list_transform([planeT(o__oneBounce, d__oneBounce)], tP__oneBounce -> list_transform([sphPair(o__oneBounce, d__oneBounce, csgA(), csgAr())], pairA__oneBounce -> list_transform([sphPair(o__oneBounce, d__oneBounce, csgB(), csgBr())], pairB__oneBounce -> list_transform([list_min(hits__oneBounce)], tS__oneBounce -> list_transform([if(pairA__oneBounce[1] > eps() AND pairA__oneBounce[1] < 1e8 AND NOT (pairB__oneBounce[1] <= pairA__oneBounce[1] AND pairA__oneBounce[1] <= pairB__oneBounce[2]), pairA__oneBounce[1], 1e9)], cEnter__oneBounce -> list_transform([if(pairB__oneBounce[1] < 1e8 AND pairB__oneBounce[2] > eps() AND pairA__oneBounce[1] <= pairB__oneBounce[2] AND pairB__oneBounce[2] <= pairA__oneBounce[2], pairB__oneBounce[2], 1e9)], cCav__oneBounce -> list_transform([least(cEnter__oneBounce, cCav__oneBounce)], tC__oneBounce -> list_transform([least(tS__oneBounce, tC__oneBounce)], tM__oneBounce -> list_transform([least(tM__oneBounce, tP__oneBounce)], tHit__oneBounce -> list_transform([va(o__oneBounce, vm(d__oneBounce, tHit__oneBounce))], hp__oneBounce -> list_transform([if(tC__oneBounce < tS__oneBounce, if(cEnter__oneBounce <= cCav__oneBounce, vn(vs(hp__oneBounce, csgA())), vn(vs(csgB(), hp__oneBounce))), vn(vs(hp__oneBounce, spheres()[indexOf(hits__oneBounce, tS__oneBounce)])))], sn__oneBounce -> list_transform([(tM__oneBounce < tP__oneBounce) AND (tM__oneBounce < 1e8)], isMirror__oneBounce -> list_transform([(tP__oneBounce <= tM__oneBounce) AND (tP__oneBounce < 1e8)], isFloor__oneBounce -> list_transform([(tM__oneBounce >= 1e8) AND (tP__oneBounce >= 1e8)], isMiss__oneBounce -> list_transform([if(isFloor__oneBounce, (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE), sn__oneBounce)], nrm__oneBounce -> list_transform([va(hp__oneBounce, vm(nrm__oneBounce, eps() * 2.0::DOUBLE))], shOrig__oneBounce -> list_transform([vref(d__oneBounce, sn__oneBounce)], rdir__oneBounce -> list_transform([vn(vs(lpos, hp__oneBounce))], ldir__oneBounce -> list_transform([L2Norm(vs(lpos, hp__oneBounce))], distL__oneBounce -> list_transform([occluded(shOrig__oneBounce, ldir__oneBounce, distL__oneBounce - 0.05::DOUBLE)], shadowed__oneBounce -> list_transform([pow(greatest(vd(ldir__oneBounce, rdir__oneBounce), 0.0::DOUBLE), shininess()) * if(shadowed__oneBounce, 0.0::DOUBLE, 1.0::DOUBLE)], spec__oneBounce -> list_transform([greatest(vd(ldir__oneBounce, (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE)), 0.0::DOUBLE) * if(shadowed__oneBounce, 0.0::DOUBLE, 1.0::DOUBLE)], lambert__oneBounce -> list_transform([least(1.0::DOUBLE, greatest(0.0::DOUBLE, (tHit__oneBounce - fogStart()) / fogRange()))], fog__oneBounce -> list_transform([va(vm(vm(if(checker(hp__oneBounce) = 0, floorA(), floorB()), floorAmb() + floorDiff() * lambert__oneBounce), 1.0::DOUBLE - fog__oneBounce), vm(hazeColor(), fog__oneBounce))], floorCol__oneBounce -> (
        if(isMirror__oneBounce, shOrig__oneBounce, o__oneBounce),
        if(isMirror__oneBounce, rdir__oneBounce, d__oneBounce),
        if(isMirror__oneBounce, thr__oneBounce * reflect_k(), thr__oneBounce),
        va(acc__oneBounce, va(if(isMirror__oneBounce, vm(specColor(), thr__oneBounce * spec__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
                   va(if(isFloor__oneBounce, vm(floorCol__oneBounce, thr__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
                      if(isMiss__oneBounce, vm(sky(d__oneBounce), thr__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE))))),
        if(isMirror__oneBounce, 1.0::DOUBLE, 0.0::DOUBLE)))[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
SELECT
    clamp(pow(greatest(avg(col[1]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS r,
    clamp(pow(greatest(avg(col[2]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS g,
    clamp(pow(greatest(avg(col[3]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS b,
    pixel % W()      AS x,                          /* explicit PNG coordinates: no ORDER BY needed */
    ch_intDiv(pixel, W()) AS y
FROM (
  SELECT pixel, va(st[4], if(st[5] > 0.5::DOUBLE, vm(sky(st[2]), st[3]), (0.0::DOUBLE,0.0::DOUBLE,0.0::DOUBLE))) AS col
  FROM (
    SELECT pixel,
      /* the bounce loop: a fold over range(maxDepth), not recursion */
      list_reduce(range(maxDepth()), (s, i) -> if(s[5] > 0.5::DOUBLE, oneBounce(s, lpos), s), st0) AS st
    FROM (
      SELECT
        number AS pixel, sample,
        va(lightPos(), (lightR() * (rnd(number, sample, 3) - 0.5::DOUBLE),
                      lightR() * (rnd(number, sample, 4) - 0.5::DOUBLE),
                      lightR() * (rnd(number, sample, 5) - 0.5::DOUBLE))) AS lpos,
        CAST((eye(),
              vn(va(camFwd(), va(vm(camRight(), (((number % W()) + rnd(number, sample, 1)) / imgW() * 2.0::DOUBLE - 1.0::DOUBLE) * aspect() * tanHalf()),
                                vm(camUp(),    (1.0::DOUBLE - (ch_intDiv(number, W()) + rnd(number, sample, 2)) / imgH() * 2.0::DOUBLE) * tanHalf())))),
              1.0::DOUBLE, (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE), 1.0::DOUBLE) AS TUPLE(TUPLE(DOUBLE,DOUBLE,DOUBLE), TUPLE(DOUBLE,DOUBLE,DOUBLE), DOUBLE, TUPLE(DOUBLE,DOUBLE,DOUBLE), DOUBLE)) AS st0
      FROM ch_numbers_n(W() * H())
      CROSS JOIN unnest(range(CAST(2 AS UINTEGER))) AS aj(sample)
    )
  )
)
GROUP BY pixel
