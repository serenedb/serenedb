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

#pragma once

#include <stringzilla/utf8_graphemes/serial.h>
#include <stringzilla/utf8_norm/serial.h>
#include <stringzilla/utf8_sentences/serial.h>
#include <stringzilla/utf8_tokens/serial.h>
#include <stringzilla/utf8_uncased_fold/serial.h>
#if defined(__x86_64__)
#include <stringzilla/utf8_graphemes/haswell.h>
#include <stringzilla/utf8_graphemes/icelake.h>
#include <stringzilla/utf8_norm/haswell.h>
#include <stringzilla/utf8_sentences/haswell.h>
#include <stringzilla/utf8_sentences/icelake.h>
#include <stringzilla/utf8_tokens/haswell.h>
#include <stringzilla/utf8_tokens/icelake.h>
#include <stringzilla/utf8_uncased_fold/haswell.h>
#include <stringzilla/utf8_uncased_fold/icelake.h>
#elif defined(__aarch64__)
#include <stringzilla/utf8_graphemes/neon.h>
#include <stringzilla/utf8_norm/neon.h>
#include <stringzilla/utf8_sentences/neon.h>
#include <stringzilla/utf8_tokens/neon.h>
#include <stringzilla/utf8_uncased_fold/neon.h>
#endif

#include <cstddef>

#include "iresearch/analysis/text/classify/block_masks.hpp"

namespace irs::analysis::sz {

using SegmentFn = sz_size_t (*)(sz_cptr_t, sz_size_t, sz_size_t*, sz_size_t*,
                                sz_size_t, sz_size_t*);

#ifdef __x86_64__
inline bool HasAvx512() noexcept {
  static const bool kHas = __builtin_cpu_supports("avx512bw") &&
                           __builtin_cpu_supports("avx512vl") &&
                           __builtin_cpu_supports("avx512dq") &&
                           __builtin_cpu_supports("avx512vbmi") &&
                           __builtin_cpu_supports("avx512vbmi2");
  return kHas;
}
#endif

inline size_t Norm(const char* in, size_t n, sz_normal_form_t form,
                   char* out) noexcept {
#ifdef __x86_64__
  return sz_utf8_norm_haswell(in, n, form, out);
#elif defined(__aarch64__)
  return sz_utf8_norm_neon(in, n, form, out);
#else
  return sz_utf8_norm_serial(in, n, form, out);
#endif
}

[[gnu::noinline]] inline bool SegmentDenormalized(
  const char* segment, size_t length, sz_normal_form_t form) noexcept {
  const auto* begin = reinterpret_cast<const sz_u8_t*>(segment);
  const auto* end = begin + length;
  if (form == sz_normal_form_nfc_k || form == sz_normal_form_nfkc_k) {
    sz_rune_t starter;
    const auto starter_length =
      sz_rune_decode(segment, segment + length, &starter);
    sz_rune_t next;
    if (starter_length != sz_rune_invalid_k && starter_length < length &&
        sz_rune_decode(segment + starter_length, segment + length, &next) !=
          sz_rune_invalid_k &&
        sz_utf8_norm_lookup_(starter).canonical_combining_class == 0 &&
        sz_utf8_norm_compose_pair_(starter, next) != 0) {
      return true;
    }
  }
  const auto* position = begin;
  sz_u8_t ccc = 0;
  if (sz_utf8_norm_verify_block_(&position, end, end,
                                 sz_utf8_norm_form_flag_(form),
                                 &ccc) == nullptr) {
    return false;
  }
  sz_utf8_norm_out_t out{.dst = nullptr,
                         .cmp = begin,
                         .cmp_end = end,
                         .written = 0,
                         .matches = sz_true_k};
  sz_utf8_norm_run_(segment, length, form, &out);
  return !out.matches || out.cmp != end;
}

inline constexpr size_t kFoldGrowth = 3;

#ifdef __x86_64__
inline constexpr size_t kSerialFoldBytes = 32;
inline constexpr size_t kSerialFoldBytesAvx512 = 6;
#endif

inline size_t Fold(const char* in, size_t n, char* out) noexcept {
#ifdef __x86_64__
  if (HasAvx512()) {
    if (n < kSerialFoldBytesAvx512 &&
        (n == 0 || static_cast<uint8_t>(in[0]) < 0xE0)) {
      return sz_utf8_uncased_fold_serial(in, n, out);
    }
    return sz_utf8_uncased_fold_icelake(in, n, out);
  }
  if (n < kSerialFoldBytes) {
    return sz_utf8_uncased_fold_serial(in, n, out);
  }
  return sz_utf8_uncased_fold_haswell(in, n, out);
#elif defined(__aarch64__)
  return sz_utf8_uncased_fold_neon(in, n, out);
#else
  return sz_utf8_uncased_fold_serial(in, n, out);
#endif
}

#ifdef __x86_64__
template<auto Haswell, auto Icelake>
inline size_t Dispatch(const char* text, size_t length, size_t* starts,
                       size_t* lengths, size_t capacity,
                       size_t* consumed) noexcept {
  if (HasAvx512()) {
    return Icelake(text, length, starts, lengths, capacity, consumed);
  }
  return Haswell(text, length, starts, lengths, capacity, consumed);
}

inline constexpr size_t kSerialSentenceBytes = 48;

inline size_t Sentences(const char* text, size_t length, size_t* starts,
                        size_t* lengths, size_t capacity,
                        size_t* consumed) noexcept {
  if (!HasAvx512() && length < kSerialSentenceBytes) {
    return sz_utf8_sentences_serial(text, length, starts, lengths, capacity,
                                    consumed);
  }
  return Dispatch<sz_utf8_sentences_haswell, sz_utf8_sentences_icelake>(
    text, length, starts, lengths, capacity, consumed);
}

inline size_t Newlines(const char* text, size_t length, size_t* offsets,
                       size_t* lengths, size_t capacity,
                       size_t* consumed) noexcept {
  return Dispatch<sz_utf8_newlines_haswell, sz_utf8_newlines_icelake>(
    text, length, offsets, lengths, capacity, consumed);
}

inline constexpr size_t kSerialGraphemeBytes = 16;

inline bool HasFourByteLead(const char* text, size_t length) noexcept {
  const auto* bytes = reinterpret_cast<const byte_type*>(text);
  size_t i = 0;
  for (; i + classify::kClassifyBlock <= length;
       i += classify::kClassifyBlock) {
    if (classify::MoveMask(classify::Load(bytes + i) >= uint8_t{0xF0}) != 0) {
      return true;
    }
  }
  for (; i < length; ++i) {
    if (bytes[i] >= 0xF0) {
      return true;
    }
  }
  return false;
}

inline SegmentFn GraphemesFor(const char* text, size_t length) noexcept {
  if (length < kSerialGraphemeBytes) {
    return sz_utf8_graphemes_serial;
  }
  if (HasAvx512() && HasFourByteLead(text, length)) {
    return sz_utf8_graphemes_icelake;
  }
  return sz_utf8_graphemes_haswell;
}
#elif defined(__aarch64__)
inline size_t Sentences(const char* text, size_t length, size_t* starts,
                        size_t* lengths, size_t capacity,
                        size_t* consumed) noexcept {
  return sz_utf8_sentences_neon(text, length, starts, lengths, capacity,
                                consumed);
}

inline size_t Newlines(const char* text, size_t length, size_t* offsets,
                       size_t* lengths, size_t capacity,
                       size_t* consumed) noexcept {
  return sz_utf8_newlines_neon(text, length, offsets, lengths, capacity,
                               consumed);
}

inline SegmentFn GraphemesFor(const char*, size_t) noexcept {
  return sz_utf8_graphemes_neon;
}
#else
inline size_t Sentences(const char* text, size_t length, size_t* starts,
                        size_t* lengths, size_t capacity,
                        size_t* consumed) noexcept {
  return sz_utf8_sentences_serial(text, length, starts, lengths, capacity,
                                  consumed);
}

inline size_t Newlines(const char* text, size_t length, size_t* offsets,
                       size_t* lengths, size_t capacity,
                       size_t* consumed) noexcept {
  return sz_utf8_newlines_serial(text, length, offsets, lengths, capacity,
                                 consumed);
}

inline SegmentFn GraphemesFor(const char*, size_t) noexcept {
  return sz_utf8_graphemes_serial;
}
#endif

}  // namespace irs::analysis::sz
