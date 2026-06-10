// ASCII A-Z->a-z fold throughput by unit width.
//
// The CI hash folds each loaded word before mixing. This compares fold approaches
// at 1/2/4/8/16/32-byte granularity to see which instruction sequence is fastest
// for the fused path. Folds an L-byte runtime buffer in place; reports ns/buffer.
//
//   taskset -c N ./serenedb-bench-micro-ci_lower --benchmark_min_time=0.3s \
//     --benchmark_repetitions=12 --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>
#include <immintrin.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace {

// ---- 1-byte ----
inline char Fold1Branch(char c) { return (c >= 'A' && c <= 'Z') ? char(c + 32) : c; }
inline char Fold1Branchless(char c) {
	return char(c + ((uint8_t(uint8_t(c) - 'A') < 26u) << 5));
}
// ---- SWAR (folly bit-trick) ----
inline uint16_t Fold16(uint16_t c) {
	uint16_t r = c & 0x7f7f;
	r += 0x2525;
	r &= 0x7f7f;
	r += 0x1a1a;
	r &= uint16_t(~c);
	r >>= 2;
	r &= 0x2020;
	return uint16_t(c + r);
}
inline uint32_t Fold32(uint32_t c) {
	uint32_t r = c & 0x7f7f7f7fu;
	r += 0x25252525u;
	r &= 0x7f7f7f7fu;
	r += 0x1a1a1a1au;
	r &= ~c;
	r >>= 2;
	r &= 0x20202020u;
	return c + r;
}
inline uint64_t Fold64(uint64_t c) {
	uint64_t r = c & 0x7f7f7f7f7f7f7f7full;
	r += 0x2525252525252525ull;
	r &= 0x7f7f7f7f7f7f7f7full;
	r += 0x1a1a1a1a1a1a1a1aull;
	r &= ~c;
	r >>= 2;
	r &= 0x2020202020202020ull;
	return c + r;
}
// ---- SIMD range-compare fold (the CILower128 style) ----
inline __m128i Fold128(__m128i v) {
	__m128i ge = _mm_cmpgt_epi8(v, _mm_set1_epi8('A' - 1));
	__m128i le = _mm_cmpgt_epi8(_mm_set1_epi8('Z' + 1), v);
	return _mm_or_si128(v, _mm_and_si128(_mm_and_si128(ge, le), _mm_set1_epi8(0x20)));
}
inline __m256i Fold256(__m256i v) {
	__m256i ge = _mm256_cmpgt_epi8(v, _mm256_set1_epi8('A' - 1));
	__m256i le = _mm256_cmpgt_epi8(_mm256_set1_epi8('Z' + 1), v);
	return _mm256_or_si256(v, _mm256_and_si256(_mm256_and_si256(ge, le), _mm256_set1_epi8(0x20)));
}

// fold an L-byte buffer in place (L assumed multiple of the unit width here; we use L=64)
template <int W>
void FoldBuf(char *p, size_t n) {
	for (size_t i = 0; i < n; i += W) {
		if constexpr (W == 0) { // per-byte branch
			p[i] = Fold1Branch(p[i]);
		} else if constexpr (W == -1) { // per-byte branchless
			p[i] = Fold1Branchless(p[i]);
		} else if constexpr (W == 2) {
			uint16_t w;
			std::memcpy(&w, p + i, 2);
			w = Fold16(w);
			std::memcpy(p + i, &w, 2);
		} else if constexpr (W == 4) {
			uint32_t w;
			std::memcpy(&w, p + i, 4);
			w = Fold32(w);
			std::memcpy(p + i, &w, 4);
		} else if constexpr (W == 8) {
			uint64_t w;
			std::memcpy(&w, p + i, 8);
			w = Fold64(w);
			std::memcpy(p + i, &w, 8);
		} else if constexpr (W == 16) {
			__m128i w = _mm_loadu_si128(reinterpret_cast<const __m128i *>(p + i));
			_mm_storeu_si128(reinterpret_cast<__m128i *>(p + i), Fold128(w));
		} else if constexpr (W == 32) {
			__m256i w = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p + i));
			_mm256_storeu_si256(reinterpret_cast<__m256i *>(p + i), Fold256(w));
		}
	}
}
// per-byte specializations need a step of 1
template <>
void FoldBuf<0>(char *p, size_t n) {
	for (size_t i = 0; i < n; ++i) p[i] = Fold1Branch(p[i]);
}
template <>
void FoldBuf<-1>(char *p, size_t n) {
	for (size_t i = 0; i < n; ++i) p[i] = Fold1Branchless(p[i]);
}

std::vector<char> MakeBuf(size_t n) {
	std::vector<char> b(n);
	const char *al = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_!";
	for (size_t i = 0; i < n; ++i) b[i] = al[(i * 7 + 3) % 64];
	return b;
}

template <int W>
void Bm(benchmark::State &s) {
	const size_t n = static_cast<size_t>(s.range(0));
	std::vector<char> buf = MakeBuf(n);
	for (auto _ : s) {
		benchmark::DoNotOptimize(buf.data());
		FoldBuf<W>(buf.data(), n);
		benchmark::DoNotOptimize(buf.data());
	}
}
} // namespace

// per-byte (branch / branchless), then SWAR 2/4/8, then SIMD 16/32, over buffer sizes
BENCHMARK_TEMPLATE(Bm, 0)->Arg(8)->Arg(16)->Arg(32)->Arg(64)->Name("fold_perbyte_branch");
BENCHMARK_TEMPLATE(Bm, -1)->Arg(8)->Arg(16)->Arg(32)->Arg(64)->Name("fold_perbyte_branchless");
BENCHMARK_TEMPLATE(Bm, 2)->Arg(8)->Arg(16)->Arg(32)->Arg(64)->Name("fold_swar2");
BENCHMARK_TEMPLATE(Bm, 4)->Arg(8)->Arg(16)->Arg(32)->Arg(64)->Name("fold_swar4");
BENCHMARK_TEMPLATE(Bm, 8)->Arg(8)->Arg(16)->Arg(32)->Arg(64)->Name("fold_swar8");
BENCHMARK_TEMPLATE(Bm, 16)->Arg(16)->Arg(32)->Arg(64)->Name("fold_sse16");
BENCHMARK_TEMPLATE(Bm, 32)->Arg(32)->Arg(64)->Name("fold_avx32");

BENCHMARK_MAIN();
