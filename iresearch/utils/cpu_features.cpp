////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/utils/cpu_features.hpp"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

#if defined(__x86_64__)
#include <cpuid.h>
#elif defined(__aarch64__) && defined(__linux__)
#include <sys/auxv.h>
#endif

namespace irs {

void CheckCpuFeatures(std::string_view program) {
  std::string missing;
  [[maybe_unused]] const auto require = [&](bool supported,
                                            std::string_view name) {
    if (!supported) {
      absl::StrAppend(&missing, " ", name);
    }
  };
#if defined(__x86_64__)
#ifdef __SSE3__
  require(__builtin_cpu_supports("sse3"), "SSE3");
#endif
#ifdef __SSSE3__
  require(__builtin_cpu_supports("ssse3"), "SSSE3");
#endif
#ifdef __SSE4_1__
  require(__builtin_cpu_supports("sse4.1"), "SSE4.1");
#endif
#ifdef __SSE4_2__
  require(__builtin_cpu_supports("sse4.2"), "SSE4.2");
#endif
#ifdef __POPCNT__
  require(__builtin_cpu_supports("popcnt"), "POPCNT");
#endif
#ifdef __GCC_HAVE_SYNC_COMPARE_AND_SWAP_16
  require(__builtin_cpu_supports("cx16"), "CMPXCHG16B");
#endif
#ifdef __LAHF_SAHF__
  uint32_t eax = 0, ebx = 0, ecx = 0, edx = 0;
  require(
    __get_cpuid(0x80000001, &eax, &ebx, &ecx, &edx) && (ecx & bit_LAHF_LM),
    "LAHF-SAHF");
#endif
#ifdef __PCLMUL__
  require(__builtin_cpu_supports("pclmul"), "PCLMULQDQ");
#endif
#ifdef __AES__
  require(__builtin_cpu_supports("aes"), "AES");
#endif
#ifdef __XSAVE__
  require(__builtin_cpu_supports("xsave"), "XSAVE");
#endif
#ifdef __AVX__
  require(__builtin_cpu_supports("avx"), "AVX");
#endif
#ifdef __AVX2__
  require(__builtin_cpu_supports("avx2"), "AVX2");
#endif
#ifdef __BMI__
  require(__builtin_cpu_supports("bmi"), "BMI1");
#endif
#ifdef __BMI2__
  require(__builtin_cpu_supports("bmi2"), "BMI2");
#endif
#ifdef __F16C__
  require(__builtin_cpu_supports("f16c"), "F16C");
#endif
#ifdef __FMA__
  require(__builtin_cpu_supports("fma"), "FMA");
#endif
#ifdef __LZCNT__
  require(__builtin_cpu_supports("lzcnt"), "LZCNT");
#endif
#ifdef __MOVBE__
  require(__builtin_cpu_supports("movbe"), "MOVBE");
#endif
#ifdef __AVX512F__
  require(__builtin_cpu_supports("avx512f"), "AVX512F");
#endif
#ifdef __AVX512BW__
  require(__builtin_cpu_supports("avx512bw"), "AVX512BW");
#endif
#ifdef __AVX512CD__
  require(__builtin_cpu_supports("avx512cd"), "AVX512CD");
#endif
#ifdef __AVX512DQ__
  require(__builtin_cpu_supports("avx512dq"), "AVX512DQ");
#endif
#ifdef __AVX512VL__
  require(__builtin_cpu_supports("avx512vl"), "AVX512VL");
#endif
#ifdef __AVX512VBMI__
  require(__builtin_cpu_supports("avx512vbmi"), "AVX512VBMI");
#endif
#ifdef __AVX512VBMI2__
  require(__builtin_cpu_supports("avx512vbmi2"), "AVX512VBMI2");
#endif
#elif defined(__aarch64__) && defined(__linux__)
  const auto hwcap = getauxval(AT_HWCAP);
#ifdef __ARM_FEATURE_CRC32
  require(hwcap & HWCAP_CRC32, "CRC32");
#endif
#ifdef __ARM_FEATURE_AES
  require(hwcap & HWCAP_AES, "AES");
  require(hwcap & HWCAP_PMULL, "PMULL");
#endif
#ifdef __ARM_FEATURE_SHA2
  require(hwcap & HWCAP_SHA1, "SHA1");
  require(hwcap & HWCAP_SHA2, "SHA2");
#endif
#ifdef __ARM_FEATURE_ATOMICS
  require(hwcap & HWCAP_ATOMICS, "LSE");
#endif
#ifdef __ARM_FEATURE_QRDMX
  require(hwcap & HWCAP_ASIMDRDM, "RDM");
#endif
#ifdef __ARM_FEATURE_DOTPROD
  require(hwcap & HWCAP_ASIMDDP, "DOTPROD");
#endif
#ifdef __ARM_FEATURE_FP16_SCALAR_ARITHMETIC
  require(hwcap & HWCAP_FPHP, "FP16");
#endif
#ifdef __ARM_FEATURE_FP16_VECTOR_ARITHMETIC
  require(hwcap & HWCAP_ASIMDHP, "ASIMDHP");
#endif
#ifdef __ARM_FEATURE_RCPC
  require(hwcap & HWCAP_LRCPC, "RCPC");
#endif
#endif
  if (missing.empty()) {
    return;
  }
  program.remove_prefix(program.rfind('/') + 1);
  absl::FPrintF(stderr, "%s cannot run on this CPU: it lacks%s\n", program,
                missing);
  std::exit(EXIT_FAILURE);
}

}  // namespace irs
