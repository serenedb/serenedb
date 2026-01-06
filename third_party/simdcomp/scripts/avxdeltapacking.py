#!/usr/bin/env python
import sys
def howmany(bit):
    """ how many values are we going to pack? """
    return 256

def howmanywords(bit):
    return (howmany(bit) * bit + 255)//256

def howmanybytes(bit):
    return howmanywords(bit) * 16

print("""

#ifdef __AVX2__

/** avxpacking with delta encoding **/
#include <immintrin.h>
#include <stdint.h>
#include <string.h>
#include <simdcomputil.h>
#include <avxdeltabitpacking.h>

""")

print("""
static inline uint32_t avxmaxbitas32int(__m256i accumulator) {
    __m128i lo = _mm256_castsi256_si128(accumulator);
    __m128i hi = _mm256_extracti128_si256(accumulator, 1);
    __m128i combined = _mm_or_si128(lo, hi);
    combined = _mm_or_si128(combined, _mm_srli_si128(combined, 8));
    combined = _mm_or_si128(combined, _mm_srli_si128(combined, 4));
    return bits(_mm_cvtsi128_si32(combined));
}

/* Compute max bits needed for delta-encoded block (SIMD version) */
uint32_t avxmaxbitsd1(uint32_t initvalue, const uint32_t *in) {
    const __m256i *pin = (const __m256i *)in;
    __m256i prev_vec = _mm256_set1_epi32(initvalue);
    __m256i newvec = _mm256_loadu_si256(pin++);
    __m256i accumulator = avx_delta(newvec, prev_vec);
    prev_vec = newvec;

    for (uint32_t k = 1; k < 32; ++k) {
        newvec = _mm256_loadu_si256(pin++);
        accumulator = _mm256_or_si256(accumulator, avx_delta(newvec, prev_vec));
        prev_vec = newvec;
    }
    return avxmaxbitas32int(accumulator);
}
""")

def plurial(number):
    if(number != 1):
        return "s"
    else :
        return ""

print("")
print("static void avxipackblock0(const uint32_t * pin, __m256i * compressed, uint32_t prev) {");
print("  (void)compressed;");
print("  (void)prev;");
print("  (void) pin; /* we consumed {0} 32-bit integer{1} */ ".format(howmany(0),plurial(howmany(0))));
print("}");
print("")

for bit in range(1,33):
    print("")
    print("/* we are going to pack {0} {1}-bit values, touching {2} 256-bit words, using {3} bytes */ ".format(howmany(bit),bit,howmanywords(bit),howmanybytes(bit)))
    print("static void avxipackblock{0}(const uint32_t * pin, __m256i * compressed, uint32_t init_value) {{".format(bit));
    print("  const __m256i * in = (const __m256i *)  pin;");
    print("  /* we are going to touch  {0} 256-bit word{1} */ ".format(howmanywords(bit),plurial(howmanywords(bit))));
    if(howmanywords(bit) == 1):
      print("  __m256i w0;")
    else:
      print("  __m256i w0, w1;")
    print("  __m256i prev = _mm256_set1_epi32(init_value);")
    print("  __m256i curr, delta;")
    oldword = 0
    for j in range(howmany(bit)//8):
      firstword = j * bit // 32
      if(firstword > oldword):
        print("  _mm256_storeu_si256(compressed++, w{0});".format(oldword%2))
        oldword = firstword
      secondword = (j * bit + bit - 1)//32
      firstshift = (j*bit) % 32
      print("  curr = _mm256_lddqu_si256(in++);")
      print("  delta = avx_delta(curr, prev);")
      if( firstword == secondword):
          if(firstshift == 0):
            print("  w{0} = delta;".format(firstword%2))
          else:
            print("  w{0} = _mm256_or_si256(w{0},_mm256_slli_epi32(delta , {1}));".format(firstword%2,firstshift))
      else:
          print("  w{0} = _mm256_or_si256(w{0},_mm256_slli_epi32(delta, {1}));".format(firstword%2,firstshift))
          secondshift = 32-firstshift
          print("  w{0} = _mm256_srli_epi32(delta,{1});".format(secondword%2,secondshift))
      print("  prev = curr;")
    print("  _mm256_storeu_si256(compressed, w{0});".format(secondword%2))
    print("}");
    print("")


print("static void avxiunpackblock0(const __m256i * compressed, uint32_t * pout, uint32_t prev) {");
print("  (void) compressed;");
print("  (void) prev;");
print("  memset(pout,0,{0});".format(howmany(0)));
print("}");
print("")

for bit in range(1,33):
    print("")
    print("/* we packed {0} {1}-bit values, touching {2} 256-bit words, using {3} bytes */ ".format(howmany(bit),bit,howmanywords(bit),howmanybytes(bit)))
    print("static void avxiunpackblock{0}(const __m256i * compressed, uint32_t * pout, uint32_t init_value) {{".format(bit));
    print("  /* we are going to access  {0} 256-bit word{1} */ ".format(howmanywords(bit),plurial(howmanywords(bit))));
    if(howmanywords(bit) == 1):
      print("  __m256i w0;")
    else:
      print("  __m256i w0, w1;")
    print("  __m256i * out = (__m256i *) pout;");
    if(bit < 32): print("  const __m256i mask = _mm256_set1_epi32({0});".format((1<<bit)-1));
    print("  __m256i prev = _mm256_set1_epi32(init_value);")
    print("  __m256i delta;")
    maskstr = " _mm256_and_si256 ( mask, {0}) "
    if (bit == 32) : maskstr = " {0} " # no need
    oldword = 0
    print("  w0 = _mm256_lddqu_si256(compressed++);")
    for j in range(howmany(bit)//8):
      firstword = j * bit // 32
      secondword = (j * bit + bit - 1)//32
      if(secondword > oldword):
        print("  w{0} = _mm256_lddqu_si256(compressed++);".format(secondword%2))
        oldword = secondword
      firstshift = (j*bit) % 32
      firstshiftstr = "_mm256_srli_epi32( w{0} , "+str(firstshift)+") "
      if(firstshift == 0):
          firstshiftstr =" w{0} " # no need
      wfirst = firstshiftstr.format(firstword%2)
      if( firstword == secondword):
          if(firstshift + bit != 32):
            wfirst  = maskstr.format(wfirst)
          print("  delta = {0};".format(wfirst))
      else:
          secondshift = (32-firstshift)
          wsecond = "_mm256_slli_epi32( w{0} , {1} ) ".format((firstword+1)%2,secondshift)
          wfirstorsecond = " _mm256_or_si256 ({0},{1}) ".format(wfirst,wsecond)
          wfirstorsecond = maskstr.format(wfirstorsecond)
          print("  delta = {0};".format(wfirstorsecond))
      print("  prev = avx_prefix_sum(delta, prev);")
      print("  _mm256_storeu_si256(out++, prev);")
    print("}");
    print("")

print("""
void avxpackwithoutmaskd1(uint32_t initvalue, const uint32_t *in, __m256i *out, uint32_t bit) {
    switch (bit) {""")
for bit in range(33):
    print(f"    case {bit}: avxipackblock{bit}(in, out, initvalue); break;")
print("""    default: break;
    }
}

void avxunpackd1(uint32_t initvalue, const __m256i *in, uint32_t *out, uint32_t bit) {
    switch (bit) {""")
for bit in range(33):
    print(f"    case {bit}: avxiunpackblock{bit}(in, out, initvalue); break;")
print("""    default: break;
    }
}

#endif /* __AVX2__ */

""")
