CREATE OR REPLACE MACRO tuplePlus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] + ch_ly[1], ch_lx[2] + ch_ly[2], ch_lx[3] + ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMinus(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> (ch_lx[1] - ch_ly[1], ch_lx[2] - ch_ly[2], ch_lx[3] - ch_ly[3]))[1])[1];
CREATE OR REPLACE MACRO tupleMultiplyByNumber(a, s) AS list_transform([a], ch_lx -> list_transform([s], ch_lk -> (ch_lx[1] * ch_lk, ch_lx[2] * ch_lk, ch_lx[3] * ch_lk))[1])[1];
CREATE OR REPLACE MACRO dotProduct(a, b) AS list_transform([a], ch_lx -> list_transform([b], ch_ly -> ch_lx[1] * ch_ly[1] + ch_lx[2] * ch_ly[2] + ch_lx[3] * ch_ly[3])[1])[1];
CREATE OR REPLACE MACRO L2Norm(a) AS list_transform([a], ch_lx -> sqrt(ch_lx[1] * ch_lx[1] + ch_lx[2] * ch_lx[2] + ch_lx[3] * ch_lx[3]))[1];
CREATE OR REPLACE MACRO L2Normalize(a) AS list_transform([a], ch_lx -> list_transform([sqrt(ch_lx[1] * ch_lx[1] + ch_lx[2] * ch_lx[2] + ch_lx[3] * ch_lx[3])], ch_ln -> (ch_lx[1] / ch_ln, ch_lx[2] / ch_ln, ch_lx[3] / ch_ln))[1])[1];
CREATE OR REPLACE MACRO clamp(x, lo, hi) AS greatest(lo, least(hi, x));
CREATE OR REPLACE MACRO assumeNotNull(x) AS x;
CREATE OR REPLACE MACRO indexOf(a, x) AS coalesce(list_position(a, x), 0);
CREATE OR REPLACE MACRO arrayConcat(a, b) AS list_concat(a, b);
CREATE OR REPLACE MACRO ch_numbers_n(n) AS TABLE SELECT range AS number FROM range(CAST(n AS BIGINT));
CREATE OR REPLACE MACRO ch_u64(x) AS list_transform([CAST(x AS HUGEINT)], ch_lv -> CAST(CASE WHEN ch_lv < 0 THEN ch_lv + CAST('18446744073709551616' AS HUGEINT) ELSE ch_lv END AS UBIGINT))[1];
CREATE OR REPLACE MACRO ch_mul64(a, c) AS CAST((CAST(a AS UHUGEINT) * CAST(c AS UHUGEINT)) % CAST('18446744073709551616' AS UHUGEINT) AS UBIGINT);
CREATE OR REPLACE MACRO ch_shr_xor(x, s) AS list_transform([x], ch_lv -> xor(ch_lv, ch_lv >> s))[1];
CREATE OR REPLACE MACRO ch_intHash64(x) AS ch_shr_xor(ch_mul64(ch_shr_xor(ch_mul64(ch_shr_xor(xor(ch_u64(x), CAST(5544725790478674055 AS UBIGINT)), 33), 18397679294719823053), 33), 14181476777654086739), 33);
CREATE OR REPLACE MACRO ch_hash128to64(lo, hi) AS list_transform([hi], ch_lh -> list_transform([ch_shr_xor(ch_mul64(xor(lo, ch_lh), 11376068507788127593), 47)], ch_la -> ch_mul64(ch_shr_xor(ch_mul64(xor(ch_lh, ch_la), 11376068507788127593), 47), 11376068507788127593))[1])[1];
CREATE OR REPLACE MACRO cityHash64(a, b) AS ch_hash128to64(ch_intHash64(a), ch_intHash64(b)), (a, b, c) AS ch_hash128to64(ch_hash128to64(ch_intHash64(a), ch_intHash64(b)), ch_intHash64(c));
CREATE OR REPLACE MACRO ch_intDiv(a, b) AS a // b;
CREATE OR REPLACE MACRO ch_toFloat64(x) AS CAST(x AS DOUBLE);
CREATE OR REPLACE MACRO banner() AS ['.###.....#.........###......###.....#...#....#...#.....###.....#...#.....####....#####', '#...#....#..........#......#...#....#..#.....#...#....#...#....#...#....#........#....', '#........#..........#......#........#.#......#...#....#...#....#...#....#........#....', '#........#..........#......#........##.......#####....#...#....#...#.....###.....####.', '#........#..........#......#........#.#......#...#....#...#....#...#........#....#....', '#...#....#..........#......#...#....#..#.....#...#....#...#....#...#........#....#....', '.###.....#####.....###......###.....#...#....#...#.....###......###.....####.....#####'];
CREATE OR REPLACE MACRO bannerW() AS 86;
CREATE OR REPLACE MACRO bannerH() AS 7;
CREATE OR REPLACE MACRO spacing() AS 1.0::DOUBLE;
CREATE OR REPLACE MACRO radius() AS 0.92::DOUBLE;
CREATE OR REPLACE MACRO z0() AS 3.0::DOUBLE;
CREATE OR REPLACE MACRO baseW() AS 1.4::DOUBLE;
CREATE OR REPLACE MACRO target() AS (42.5::DOUBLE, 0.0::DOUBLE, 6.0::DOUBLE);
CREATE OR REPLACE MACRO t() AS CAST(30 AS UINTEGER)::DOUBLE / CAST(60 AS UINTEGER)::DOUBLE;
CREATE OR REPLACE MACRO p1() AS assumeNotNull((greatest(0.0::DOUBLE, least(1.0::DOUBLE, t() / 0.26::DOUBLE))));
CREATE OR REPLACE MACRO p2() AS assumeNotNull((greatest(0.0::DOUBLE, least(1.0::DOUBLE, (t() - 0.26::DOUBLE) / 0.18::DOUBLE))));
CREATE OR REPLACE MACRO p3() AS assumeNotNull((greatest(0.0::DOUBLE, least(1.0::DOUBLE, (t() - 0.44::DOUBLE) / 0.3::DOUBLE))));
CREATE OR REPLACE MACRO p4() AS assumeNotNull((greatest(0.0::DOUBLE, least(1.0::DOUBLE, (t() - 0.74::DOUBLE) / 0.12::DOUBLE))));
CREATE OR REPLACE MACRO p5() AS assumeNotNull((greatest(0.0::DOUBLE, least(1.0::DOUBLE, (t() - 0.86::DOUBLE) / 0.14::DOUBLE))));
CREATE OR REPLACE MACRO bz() AS assumeNotNull((CASE WHEN t() < 0.26::DOUBLE THEN 5.0::DOUBLE * abs(sin(3 * pi() * p1())) WHEN t() < 0.44::DOUBLE THEN (0.0::DOUBLE + ((0.0::DOUBLE) - (0.0::DOUBLE)) * (p2()) + 11.0::DOUBLE * 4.0::DOUBLE * (p2()) * (1.0::DOUBLE - (p2()))) WHEN t() < 0.74::DOUBLE THEN 0.0::DOUBLE WHEN t() < 0.86::DOUBLE THEN 0.0::DOUBLE ELSE 0.12::DOUBLE * sin(2.0::DOUBLE*pi()*p5()) END));
CREATE OR REPLACE MACRO air() AS assumeNotNull((greatest(0.0::DOUBLE, least(1.0::DOUBLE, bz() / 6.0::DOUBLE))));
CREATE OR REPLACE MACRO comp() AS assumeNotNull((greatest(0.0::DOUBLE, least(1.0::DOUBLE, CASE WHEN t() < 0.26::DOUBLE THEN 0.5::DOUBLE * (1.0::DOUBLE - air()) WHEN t() < 0.44::DOUBLE THEN 0.0::DOUBLE WHEN t() < 0.74::DOUBLE THEN 0.5::DOUBLE * greatest(0.0::DOUBLE, 1.0::DOUBLE - p3() / 0.10::DOUBLE) ELSE 0.0::DOUBLE END))));
CREATE OR REPLACE MACRO reach() AS assumeNotNull((CASE WHEN t() < 0.44::DOUBLE THEN 0.0::DOUBLE WHEN t() < 0.74::DOUBLE THEN (0.0::DOUBLE + ((0.55::DOUBLE) - (0.0::DOUBLE)) * ((least(1.0::DOUBLE, p3() / 0.25::DOUBLE))*(least(1.0::DOUBLE, p3() / 0.25::DOUBLE))*(3.0::DOUBLE - 2.0::DOUBLE*(least(1.0::DOUBLE, p3() / 0.25::DOUBLE))))) WHEN t() < 0.86::DOUBLE THEN (0.55::DOUBLE + ((1.0::DOUBLE) - (0.55::DOUBLE)) * ((p4())*(p4())*(3.0::DOUBLE - 2.0::DOUBLE*(p4())))) ELSE 1.0::DOUBLE END));
CREATE OR REPLACE MACRO yaw() AS assumeNotNull((CASE WHEN t() < 0.26::DOUBLE THEN pi() WHEN t() < 0.44::DOUBLE THEN (pi() + ((1.5::DOUBLE*pi()) - (pi())) * ((p2())*(p2())*(3.0::DOUBLE - 2.0::DOUBLE*(p2())))) WHEN t() < 0.74::DOUBLE THEN 1.5::DOUBLE*pi() + 0.7::DOUBLE * sin(2.0::DOUBLE*pi()*p3()) WHEN t() < 0.86::DOUBLE THEN 1.5::DOUBLE*pi() ELSE 1.5::DOUBLE*pi() + 0.12217304763960307::DOUBLE * sin(3.0::DOUBLE*pi()*p5()) END));
CREATE OR REPLACE MACRO bx() AS assumeNotNull((CASE WHEN t() < 0.26::DOUBLE THEN (94.0::DOUBLE + ((42.5::DOUBLE) - (94.0::DOUBLE)) * ((p1())*(p1())*(3.0::DOUBLE - 2.0::DOUBLE*(p1())))) ELSE 42.5::DOUBLE END));
CREATE OR REPLACE MACRO "by"() AS assumeNotNull((CASE WHEN t() < 0.26::DOUBLE THEN (-11.0::DOUBLE + ((-4.0::DOUBLE) - (-11.0::DOUBLE)) * ((p1())*(p1())*(3.0::DOUBLE - 2.0::DOUBLE*(p1())))) WHEN t() < 0.44::DOUBLE THEN (-4.0::DOUBLE + ((4.5::DOUBLE) - (-4.0::DOUBLE)) * ((p2())*(p2())*(3.0::DOUBLE - 2.0::DOUBLE*(p2())))) WHEN t() < 0.74::DOUBLE THEN 4.5::DOUBLE WHEN t() < 0.86::DOUBLE THEN (4.5::DOUBLE + ((3.0::DOUBLE) - (4.5::DOUBLE)) * ((p4())*(p4())*(3.0::DOUBLE - 2.0::DOUBLE*(p4())))) ELSE 3.0::DOUBLE END));
CREATE OR REPLACE MACRO cosY() AS assumeNotNull((cos(yaw())));
CREATE OR REPLACE MACRO sinY() AS assumeNotNull((sin(yaw())));
CREATE OR REPLACE MACRO a1() AS assumeNotNull(((0.24434609527920614::DOUBLE + comp() * 0.3141592653589793::DOUBLE + (0.9075712110370514::DOUBLE - 0.24434609527920614::DOUBLE) * reach())));
CREATE OR REPLACE MACRO a2() AS assumeNotNull(((0.4188790204786391::DOUBLE + comp() * 0.3839724354387525::DOUBLE + (1.1519173063162575::DOUBLE - 0.4188790204786391::DOUBLE) * reach())));
CREATE OR REPLACE MACRO a3() AS assumeNotNull((CASE WHEN t() < 0.26::DOUBLE THEN 2.4085543677521746::DOUBLE WHEN t() < 0.44::DOUBLE THEN (2.4085543677521746::DOUBLE + ((1.6580627893946132::DOUBLE) - (2.4085543677521746::DOUBLE)) * ((p2())*(p2())*(3.0::DOUBLE - 2.0::DOUBLE*(p2())))) WHEN t() < 0.74::DOUBLE THEN 1.6755160819145565::DOUBLE + 0.20943951023931956::DOUBLE * sin(2.0::DOUBLE*pi()*p3()) WHEN t() < 0.86::DOUBLE THEN (1.6755160819145565::DOUBLE + ((1.5707963267948966::DOUBLE) - (1.6755160819145565::DOUBLE)) * ((p4())*(p4())*(3.0::DOUBLE - 2.0::DOUBLE*(p4())))) ELSE 1.5707963267948966::DOUBLE - 0.24434609527920614::DOUBLE * (p5())*(p5())*(3.0::DOUBLE - 2.0::DOUBLE*(p5())) END));
CREATE OR REPLACE MACRO L1c() AS assumeNotNull((3.6::DOUBLE * (1.0::DOUBLE + 0.12::DOUBLE*air() - 0.25::DOUBLE*comp())));
CREATE OR REPLACE MACRO L2c() AS assumeNotNull((3.4::DOUBLE * (1.0::DOUBLE + 0.12::DOUBLE*air() - 0.25::DOUBLE*comp())));
CREATE OR REPLACE MACRO sinA1() AS assumeNotNull((sin(a1())));
CREATE OR REPLACE MACRO cosA1() AS assumeNotNull((cos(a1())));
CREATE OR REPLACE MACRO sinA2() AS assumeNotNull((sin(a2())));
CREATE OR REPLACE MACRO cosA2() AS assumeNotNull((cos(a2())));
CREATE OR REPLACE MACRO sinA3() AS assumeNotNull((sin(a3())));
CREATE OR REPLACE MACRO cosA3() AS assumeNotNull((cos(a3())));
CREATE OR REPLACE MACRO eu() AS assumeNotNull((L1c() * sinA1()));
CREATE OR REPLACE MACRO ew() AS assumeNotNull((1.4::DOUBLE + L1c() * cosA1()));
CREATE OR REPLACE MACRO hu() AS assumeNotNull((eu() + L2c() * sinA2()));
CREATE OR REPLACE MACRO hw() AS assumeNotNull((ew() + L2c() * cosA2()));
CREATE OR REPLACE MACRO eye() AS (42.5::DOUBLE, -69.1785::DOUBLE, 20.7536::DOUBLE);
CREATE OR REPLACE MACRO fovDeg() AS 34.0::DOUBLE;
CREATE OR REPLACE MACRO W() AS 120::UBIGINT;
CREATE OR REPLACE MACRO H() AS 54::UBIGINT;
CREATE OR REPLACE MACRO imgW() AS W()::DOUBLE;
CREATE OR REPLACE MACRO imgH() AS H()::DOUBLE;
CREATE OR REPLACE MACRO maxDepth() AS 3;
CREATE OR REPLACE MACRO lightPos() AS (bx() + (hu() + 2.9::DOUBLE * sinA3()) * cosY(), "by"() + (hu() + 2.9::DOUBLE * sinA3()) * sinY(), bz() + (hw() + 2.9::DOUBLE * cosA3()));
CREATE OR REPLACE MACRO lightR() AS 3.0::DOUBLE;
CREATE OR REPLACE MACRO skyColor() AS (0.0026::DOUBLE, 0.0026::DOUBLE, 0.0023::DOUBLE);
CREATE OR REPLACE MACRO hazeColor() AS (0.0026::DOUBLE, 0.0026::DOUBLE, 0.0023::DOUBLE);
CREATE OR REPLACE MACRO fogStart() AS 35.0::DOUBLE;
CREATE OR REPLACE MACRO fogRange() AS 120.0::DOUBLE;
CREATE OR REPLACE MACRO floorPB() AS (0.010::DOUBLE, 0.010::DOUBLE, 0.010::DOUBLE);
CREATE OR REPLACE MACRO floorAmb() AS 0.55::DOUBLE;
CREATE OR REPLACE MACRO floorDiff() AS 0.45::DOUBLE;
CREATE OR REPLACE MACRO specColor() AS (1.0::DOUBLE, 1.0::DOUBLE, 0.45::DOUBLE);
CREATE OR REPLACE MACRO shininess() AS 60.0::DOUBLE;
CREATE OR REPLACE MACRO reflect_k() AS 0.72::DOUBLE;
CREATE OR REPLACE MACRO warmLight() AS (1.0::DOUBLE, 1.0::DOUBLE, 0.42::DOUBLE);
CREATE OR REPLACE MACRO poolGain() AS 1.8::DOUBLE;
CREATE OR REPLACE MACRO bulbEmit() AS 4.0::DOUBLE;
CREATE OR REPLACE MACRO lightAtten() AS 0.006::DOUBLE;
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
CREATE OR REPLACE MACRO letterSpheres() AS (list_transform(list_filter(flatten(list_transform(range(bannerH()), gy__letterSpheres -> list_transform(range(bannerW()), gx__letterSpheres -> (gx__letterSpheres, gy__letterSpheres)))), c__letterSpheres -> substring(banner()[c__letterSpheres[2] + 1], c__letterSpheres[1] + 1, 1) = '#'), c__letterSpheres -> (
        c__letterSpheres[1] * spacing(),
        0.0::DOUBLE,
        z0() + (ch_toFloat64(bannerH() - 1) - c__letterSpheres[2]) * spacing(),
        radius())))::TUPLE(DOUBLE, DOUBLE, DOUBLE, DOUBLE)[];
CREATE OR REPLACE MACRO lampSpheres() AS ([(bx() + 1.7::DOUBLE, "by"() + 0.0::DOUBLE, bz() + 1.0::DOUBLE, 0.8::DOUBLE), (bx() + 1.0599::DOUBLE, "by"() + 1.3291::DOUBLE, bz() + 1.0::DOUBLE, 0.8::DOUBLE), (bx() + -0.3783::DOUBLE, "by"() + 1.6574::DOUBLE, bz() + 1.0::DOUBLE, 0.8::DOUBLE), (bx() + -1.5316::DOUBLE, "by"() + 0.7376::DOUBLE, bz() + 1.0::DOUBLE, 0.8::DOUBLE), (bx() + -1.5316::DOUBLE, "by"() + -0.7376::DOUBLE, bz() + 1.0::DOUBLE, 0.8::DOUBLE), (bx() + -0.3783::DOUBLE, "by"() + -1.6574::DOUBLE, bz() + 1.0::DOUBLE, 0.8::DOUBLE), (bx() + 1.0599::DOUBLE, "by"() + -1.3291::DOUBLE, bz() + 1.0::DOUBLE, 0.8::DOUBLE), (bx(), "by"(), bz() + 1.2::DOUBLE, 1.0::DOUBLE), (bx() + (0.16::DOUBLE * eu()) * cosY(), "by"() + (0.16::DOUBLE * eu()) * sinY(), bz() + (1.4::DOUBLE + 0.16::DOUBLE * (ew() - 1.4::DOUBLE)), 0.5::DOUBLE), (bx() + (0.32::DOUBLE * eu()) * cosY(), "by"() + (0.32::DOUBLE * eu()) * sinY(), bz() + (1.4::DOUBLE + 0.32::DOUBLE * (ew() - 1.4::DOUBLE)), 0.5::DOUBLE), (bx() + (0.48::DOUBLE * eu()) * cosY(), "by"() + (0.48::DOUBLE * eu()) * sinY(), bz() + (1.4::DOUBLE + 0.48::DOUBLE * (ew() - 1.4::DOUBLE)), 0.5::DOUBLE), (bx() + (0.64::DOUBLE * eu()) * cosY(), "by"() + (0.64::DOUBLE * eu()) * sinY(), bz() + (1.4::DOUBLE + 0.64::DOUBLE * (ew() - 1.4::DOUBLE)), 0.5::DOUBLE), (bx() + (0.8::DOUBLE * eu()) * cosY(), "by"() + (0.8::DOUBLE * eu()) * sinY(), bz() + (1.4::DOUBLE + 0.8::DOUBLE * (ew() - 1.4::DOUBLE)), 0.5::DOUBLE), (bx() + (0.95::DOUBLE * eu()) * cosY(), "by"() + (0.95::DOUBLE * eu()) * sinY(), bz() + (1.4::DOUBLE + 0.95::DOUBLE * (ew() - 1.4::DOUBLE)), 0.5::DOUBLE), (bx() + (eu()) * cosY(), "by"() + (eu()) * sinY(), bz() + (ew()), 0.75::DOUBLE), (bx() + (eu() + 0.2::DOUBLE * (hu() - eu())) * cosY(), "by"() + (eu() + 0.2::DOUBLE * (hu() - eu())) * sinY(), bz() + (ew() + 0.2::DOUBLE * (hw() - ew())), 0.5::DOUBLE), (bx() + (eu() + 0.4::DOUBLE * (hu() - eu())) * cosY(), "by"() + (eu() + 0.4::DOUBLE * (hu() - eu())) * sinY(), bz() + (ew() + 0.4::DOUBLE * (hw() - ew())), 0.5::DOUBLE), (bx() + (eu() + 0.6::DOUBLE * (hu() - eu())) * cosY(), "by"() + (eu() + 0.6::DOUBLE * (hu() - eu())) * sinY(), bz() + (ew() + 0.6::DOUBLE * (hw() - ew())), 0.5::DOUBLE), (bx() + (eu() + 0.8::DOUBLE * (hu() - eu())) * cosY(), "by"() + (eu() + 0.8::DOUBLE * (hu() - eu())) * sinY(), bz() + (ew() + 0.8::DOUBLE * (hw() - ew())), 0.5::DOUBLE), (bx() + (eu() + 0.95::DOUBLE * (hu() - eu())) * cosY(), "by"() + (eu() + 0.95::DOUBLE * (hu() - eu())) * sinY(), bz() + (ew() + 0.95::DOUBLE * (hw() - ew())), 0.5::DOUBLE), (bx() + (hu()) * cosY(), "by"() + (hu()) * sinY(), bz() + (hw()), 0.65::DOUBLE), (bx() + (hu() + 0.8::DOUBLE * sinA3()) * cosY(), "by"() + (hu() + 0.8::DOUBLE * sinA3()) * sinY(), bz() + (hw() + 0.8::DOUBLE * cosA3()), 0.55::DOUBLE), (bx() + (hu() + 1.7::DOUBLE * sinA3()) * cosY(), "by"() + (hu() + 1.7::DOUBLE * sinA3()) * sinY(), bz() + (hw() + 1.7::DOUBLE * cosA3()), 0.85::DOUBLE), (bx() + (hu() + 2.5::DOUBLE * sinA3()) * cosY(), "by"() + (hu() + 2.5::DOUBLE * sinA3()) * sinY(), bz() + (hw() + 2.5::DOUBLE * cosA3()), 1.1::DOUBLE), (bx() + (hu() + 3.3::DOUBLE * sinA3()) * cosY(), "by"() + (hu() + 3.3::DOUBLE * sinA3()) * sinY(), bz() + (hw() + 3.3::DOUBLE * cosA3()), 1.4::DOUBLE), (bx() + (hu() + 2.9::DOUBLE * sinA3()) * cosY(), "by"() + (hu() + 2.9::DOUBLE * sinA3()) * sinY(), bz() + (hw() + 2.9::DOUBLE * cosA3()), 0.85::DOUBLE)])::TUPLE(DOUBLE, DOUBLE, DOUBLE, DOUBLE)[];
CREATE OR REPLACE MACRO spheres() AS arrayConcat(letterSpheres(), lampSpheres());
CREATE OR REPLACE MACRO camFwd() AS L2Normalize(tupleMinus(target(), eye()));
CREATE OR REPLACE MACRO camRight() AS L2Normalize(vc(L2Normalize(tupleMinus(target(), eye())), (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE)));
CREATE OR REPLACE MACRO camUp() AS vc(camRight(), camFwd());
CREATE OR REPLACE MACRO tanHalf() AS tan(fovDeg() * 0.5::DOUBLE * pi() / 180.0::DOUBLE);
CREATE OR REPLACE MACRO aspect() AS imgW() / imgH();
CREATE OR REPLACE MACRO sphTnear(p, d, rad) AS list_transform([dotProduct(p, d) * dotProduct(p, d)], b__sphTnear -> if(
        b__sphTnear - (dotProduct(p, p) - rad*rad) > 0.0::DOUBLE
        AND -dotProduct(p, d) - sqrt(greatest(b__sphTnear - (dotProduct(p, p) - rad*rad), 0.0::DOUBLE)) > eps(),
        -dotProduct(p, d) - sqrt(greatest(b__sphTnear - (dotProduct(p, p) - rad*rad), 0.0::DOUBLE)),
        1e9))[1];
CREATE OR REPLACE MACRO sphereHits(o, d) AS list_transform(spheres(), c__sphereHits -> sphTnear(vs(o, (c__sphereHits[1], c__sphereHits[2], c__sphereHits[3])), d, c__sphereHits[4]));
CREATE OR REPLACE MACRO planeT(o, d) AS if(d[3] < -eps() AND o[3] > 0.0::DOUBLE, -o[3] / d[3], 1e9);
CREATE OR REPLACE MACRO occluded(o, d, maxd) AS list_bool_or(list_transform(spheres(), c__occluded -> sphTnear(vs(o, (c__occluded[1], c__occluded[2], c__occluded[3])), d, c__occluded[4]) < maxd));
CREATE OR REPLACE MACRO sky(d) AS skyColor();
CREATE OR REPLACE MACRO rnd(a, b, c) AS (cityHash64(a, b, c) % 1048576) / 1048576.0::DOUBLE;
CREATE OR REPLACE MACRO oneBounce(s, lpos) AS list_transform([s[1]], o__oneBounce -> list_transform([s[2]], d__oneBounce -> list_transform([s[3]], thr__oneBounce -> list_transform([s[4]], acc__oneBounce -> list_transform([sphereHits(o__oneBounce, d__oneBounce)], hits__oneBounce -> list_transform([planeT(o__oneBounce, d__oneBounce)], tP__oneBounce -> list_transform([list_min(hits__oneBounce)], tS__oneBounce -> list_transform([spheres()[indexOf(hits__oneBounce, tS__oneBounce)]], hc__oneBounce -> list_transform([least(tS__oneBounce, tP__oneBounce)], tHit__oneBounce -> list_transform([va(o__oneBounce, vm(d__oneBounce, tHit__oneBounce))], hp__oneBounce -> list_transform([vn(vs(hp__oneBounce, (hc__oneBounce[1], hc__oneBounce[2], hc__oneBounce[3])))], sn__oneBounce -> list_transform([(tS__oneBounce < tP__oneBounce) AND (tS__oneBounce < 1e8)], isMirror__oneBounce -> list_transform([(tP__oneBounce <= tS__oneBounce) AND (tP__oneBounce < 1e8)], isFloor__oneBounce -> list_transform([(tS__oneBounce >= 1e8) AND (tP__oneBounce >= 1e8)], isMiss__oneBounce -> list_transform([if(isFloor__oneBounce, (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE), sn__oneBounce)], nrm__oneBounce -> list_transform([va(hp__oneBounce, vm(nrm__oneBounce, eps() * 2.0::DOUBLE))], shOrig__oneBounce -> list_transform([vref(d__oneBounce, sn__oneBounce)], rdir__oneBounce -> list_transform([vn(vs(lpos, hp__oneBounce))], ldir__oneBounce -> list_transform([L2Norm(vs(lpos, hp__oneBounce))], distL__oneBounce -> list_transform([occluded(shOrig__oneBounce, ldir__oneBounce, distL__oneBounce - 0.05::DOUBLE)], shadowed__oneBounce -> list_transform([if(shadowed__oneBounce, 0.0::DOUBLE, 1.0::DOUBLE)], lit__oneBounce -> list_transform([1.0::DOUBLE / (1.0::DOUBLE + distL__oneBounce * distL__oneBounce * lightAtten())], att__oneBounce -> list_transform([L2Norm(vs((hc__oneBounce[1], hc__oneBounce[2], hc__oneBounce[3]), lightPos())) < 1.2::DOUBLE], isBulb__oneBounce -> list_transform([pow(greatest(vd(ldir__oneBounce, rdir__oneBounce), 0.0::DOUBLE), shininess()) * lit__oneBounce * (1.0::DOUBLE + 2.5::DOUBLE * att__oneBounce)], spec__oneBounce -> list_transform([greatest(vd(ldir__oneBounce, (0.0::DOUBLE, 0.0::DOUBLE, 1.0::DOUBLE)), 0.0::DOUBLE) * lit__oneBounce], lambert__oneBounce -> list_transform([least(1.0::DOUBLE, greatest(0.0::DOUBLE, (tHit__oneBounce - fogStart()) / fogRange()))], fog__oneBounce -> list_transform([va(va(vm(vm(floorPB(), floorAmb() + floorDiff() * lambert__oneBounce), 1.0::DOUBLE - fog__oneBounce), vm(hazeColor(), fog__oneBounce)), vm(warmLight(), poolGain() * att__oneBounce * lit__oneBounce * (1.0::DOUBLE - fog__oneBounce)))], floorCol__oneBounce -> (
        if(isMirror__oneBounce, shOrig__oneBounce, o__oneBounce),
        if(isMirror__oneBounce, rdir__oneBounce, d__oneBounce),
        if(isMirror__oneBounce, thr__oneBounce * reflect_k(), thr__oneBounce),
        va(acc__oneBounce, va(if(isMirror__oneBounce, va(vm(specColor(), thr__oneBounce * spec__oneBounce),
                                   if(isBulb__oneBounce, vm(warmLight(), thr__oneBounce * bulbEmit()), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE))), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
                   va(if(isFloor__oneBounce, vm(floorCol__oneBounce, thr__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE)),
                      if(isMiss__oneBounce, vm(sky(d__oneBounce), thr__oneBounce), (0.0::DOUBLE, 0.0::DOUBLE, 0.0::DOUBLE))))),
        if(isMirror__oneBounce, 1.0::DOUBLE, 0.0::DOUBLE)))[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1])[1];
SELECT
    clamp(pow(greatest(avg(col[1]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS r,
    clamp(pow(greatest(avg(col[2]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS g,
    clamp(pow(greatest(avg(col[3]), 0.0::DOUBLE) * exposure(), 1.0::DOUBLE / gamma()), 0.0::DOUBLE, 1.0::DOUBLE) AS b,
    pixel % W()      AS x,
    ch_intDiv(pixel, W()) AS y
FROM (
  SELECT pixel, va(st[4], if(st[5] > 0.5::DOUBLE, vm(sky(st[2]), st[3]), (0.0::DOUBLE,0.0::DOUBLE,0.0::DOUBLE))) AS col
  FROM (
    SELECT pixel,
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
