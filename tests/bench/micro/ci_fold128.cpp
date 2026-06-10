// Fastest 16-byte ASCII A-Z->a-z fold. Candidates operate on a 16-byte unit;
// measured as throughput over a 4 KiB buffer. Each is correctness-checked against
// a scalar reference over all 256 byte values before timing.
//
//   taskset -c N ./serenedb-bench-micro-ci_fold128 --benchmark_min_time=0.3s \
//     --benchmark_repetitions=12 --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>
#include <immintrin.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

namespace {

// reference
inline uint8_t RefLower(uint8_t c) { return (c >= 'A' && c <= 'Z') ? uint8_t(c + 32) : c; }

// A: current -- two signed range compares (5 ops + 3 consts)
inline __m128i FoldA(__m128i v) {
	__m128i ge = _mm_cmpgt_epi8(v, _mm_set1_epi8('A' - 1));
	__m128i le = _mm_cmpgt_epi8(_mm_set1_epi8('Z' + 1), v);
	return _mm_or_si128(v, _mm_and_si128(_mm_and_si128(ge, le), _mm_set1_epi8(0x20)));
}
// B: bias + single signed compare (4 ops + 3 consts).
// add 0x3F maps 'A'..'Z' (0x41..0x5A) to 0x80..0x99 = -128..-103 (signed); no other
// byte lands <= -103, so one cmpgt isolates the upper-case set.
inline __m128i FoldB(__m128i v) {
	// 'A'..'Z' (0x41..0x5A) + 0x3F -> 0x80..0x99 = -128..-103 (signed); threshold is
	// the byte just past 'Z': 'Z' + 0x3F + 1 = 0x9A = -102, so biased < -102 selects [A,Z].
	__m128i biased = _mm_add_epi8(v, _mm_set1_epi8(int8_t(0x80 - 'A')));
	__m128i is_up = _mm_cmpgt_epi8(_mm_set1_epi8(int8_t('Z' + 0x80 - 'A' + 1)), biased);
	return _mm_or_si128(v, _mm_and_si128(is_up, _mm_set1_epi8(0x20)));
}
// C: __uint128_t SWAR (folly bit-trick at 128 bits; lowers to ~2x 64-bit on x86)
inline __uint128_t FoldC(__uint128_t c) {
	constexpr __uint128_t k7f = (__uint128_t(0x7f7f7f7f7f7f7f7fULL) << 64) | 0x7f7f7f7f7f7f7f7fULL;
	constexpr __uint128_t k25 = (__uint128_t(0x2525252525252525ULL) << 64) | 0x2525252525252525ULL;
	constexpr __uint128_t k1a = (__uint128_t(0x1a1a1a1a1a1a1a1aULL) << 64) | 0x1a1a1a1a1a1a1a1aULL;
	constexpr __uint128_t k20 = (__uint128_t(0x2020202020202020ULL) << 64) | 0x2020202020202020ULL;
	__uint128_t r = c & k7f;
	r += k25;
	r &= k7f;
	r += k1a;
	r &= ~c;
	r >>= 2;
	r &= k20;
	return c + r;
}

bool CheckSimd(__m128i (*fn)(__m128i)) {
	alignas(16) uint8_t in[16], out[16];
	for (int base = 0; base < 256; base += 16) {
		for (int i = 0; i < 16; ++i) {
			in[i] = uint8_t(base + i);
		}
		__m128i r = fn(_mm_load_si128(reinterpret_cast<const __m128i *>(in)));
		_mm_store_si128(reinterpret_cast<__m128i *>(out), r);
		for (int i = 0; i < 16; ++i) {
			if (out[i] != RefLower(in[i])) {
				return false;
			}
		}
	}
	return true;
}
bool CheckSwar() {
	for (int base = 0; base < 256; base += 16) {
		uint8_t in[16];
		for (int i = 0; i < 16; ++i) {
			in[i] = uint8_t(base + i);
		}
		__uint128_t c;
		std::memcpy(&c, in, 16);
		__uint128_t r = FoldC(c);
		uint8_t out[16];
		std::memcpy(out, &r, 16);
		for (int i = 0; i < 16; ++i) {
			if (out[i] != RefLower(in[i])) {
				return false;
			}
		}
	}
	return true;
}

std::vector<char> Buf(size_t n) {
	std::vector<char> b(n);
	const char *al = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_!";
	for (size_t i = 0; i < n; ++i) {
		b[i] = al[(i * 7 + 3) % 64];
	}
	return b;
}
constexpr size_t kN = 4096;

template <__m128i (*Fn)(__m128i)>
void BmSimd(benchmark::State &s) {
	if (!CheckSimd(Fn)) {
		s.SkipWithError("fold incorrect");
		return;
	}
	std::vector<char> buf = Buf(kN);
	for (auto _ : s) {
		benchmark::DoNotOptimize(buf.data());
		for (size_t i = 0; i < kN; i += 16) {
			__m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i *>(buf.data() + i));
			_mm_storeu_si128(reinterpret_cast<__m128i *>(buf.data() + i), Fn(v));
		}
		benchmark::DoNotOptimize(buf.data());
	}
}
void BmSwar(benchmark::State &s) {
	if (!CheckSwar()) {
		s.SkipWithError("fold incorrect");
		return;
	}
	std::vector<char> buf = Buf(kN);
	for (auto _ : s) {
		benchmark::DoNotOptimize(buf.data());
		for (size_t i = 0; i < kN; i += 16) {
			__uint128_t v;
			std::memcpy(&v, buf.data() + i, 16);
			v = FoldC(v);
			std::memcpy(buf.data() + i, &v, 16);
		}
		benchmark::DoNotOptimize(buf.data());
	}
}
} // namespace

// load + fold, NO store-back, THROUGHPUT: 8 independent accumulator lanes so
// consecutive folds don't form a serial dependency (mimics the hash's 4 parallel
// AES states consuming independent folds). Isolates load+fold throughput.
template <__m128i (*Fn)(__m128i)>
void BmLoadFold(benchmark::State &s) {
	if (!CheckSimd(Fn)) {
		s.SkipWithError("fold incorrect");
		return;
	}
	std::vector<char> buf = Buf(kN);
	for (auto _ : s) {
		benchmark::DoNotOptimize(buf.data());
		const char *p = buf.data();
		__m128i a0 = _mm_setzero_si128(), a1 = a0, a2 = a0, a3 = a0, a4 = a0, a5 = a0, a6 = a0, a7 = a0;
		for (size_t i = 0; i < kN; i += 128) { // 8 lanes per 128 bytes
			auto L = [&](size_t off) { return Fn(_mm_loadu_si128(reinterpret_cast<const __m128i *>(p + i + off))); };
			a0 = _mm_xor_si128(a0, L(0));
			a1 = _mm_xor_si128(a1, L(16));
			a2 = _mm_xor_si128(a2, L(32));
			a3 = _mm_xor_si128(a3, L(48));
			a4 = _mm_xor_si128(a4, L(64));
			a5 = _mm_xor_si128(a5, L(80));
			a6 = _mm_xor_si128(a6, L(96));
			a7 = _mm_xor_si128(a7, L(112));
		}
		__m128i acc = _mm_xor_si128(_mm_xor_si128(_mm_xor_si128(a0, a1), _mm_xor_si128(a2, a3)),
		                            _mm_xor_si128(_mm_xor_si128(a4, a5), _mm_xor_si128(a6, a7)));
		benchmark::DoNotOptimize(acc);
	}
}

BENCHMARK_TEMPLATE(BmSimd, FoldA)->Name("A_two_cmpgt_loadfoldstore");
BENCHMARK_TEMPLATE(BmSimd, FoldB)->Name("B_bias_loadfoldstore");
BENCHMARK(BmSwar)->Name("C_uint128_swar_loadfoldstore");
BENCHMARK_TEMPLATE(BmLoadFold, FoldA)->Name("A_two_cmpgt_loadfold_NOSTORE");
BENCHMARK_TEMPLATE(BmLoadFold, FoldB)->Name("B_bias_loadfold_NOSTORE");

BENCHMARK_MAIN();
