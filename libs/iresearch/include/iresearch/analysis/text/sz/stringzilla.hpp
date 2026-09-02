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

#include <stringzilla/utf8_norm/serial.h>
#include <stringzilla/utf8_sentences/serial.h>
#include <stringzilla/utf8_tokens/serial.h>
#if defined(__x86_64__)
#include <stringzilla/utf8_norm/haswell.h>
#include <stringzilla/utf8_sentences/haswell.h>
#include <stringzilla/utf8_sentences/icelake.h>
#include <stringzilla/utf8_tokens/haswell.h>
#include <stringzilla/utf8_tokens/icelake.h>
#elif defined(__aarch64__)
#include <stringzilla/utf8_norm/neon.h>
#include <stringzilla/utf8_sentences/neon.h>
#include <stringzilla/utf8_tokens/neon.h>
#endif

#include <cstddef>

namespace irs::analysis::sz {

#ifdef __x86_64__
inline bool HasAvx512() noexcept {
  static const bool kHas = __builtin_cpu_supports("avx512bw") &&
                           __builtin_cpu_supports("avx512vl") &&
                           __builtin_cpu_supports("avx512vbmi");
  return kHas;
}
#endif

// norm stays on haswell: icelake measured equal-to-slower on every corpus
// (transform -9%, multilingual scan -13%), unlike sentences/newlines.
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

inline size_t Sentences(const char* text, size_t length, size_t* starts,
                        size_t* lengths, size_t capacity,
                        size_t* consumed) noexcept {
  return Dispatch<sz_utf8_sentences_haswell, sz_utf8_sentences_icelake>(
    text, length, starts, lengths, capacity, consumed);
}

inline size_t Newlines(const char* text, size_t length, size_t* offsets,
                       size_t* lengths, size_t capacity,
                       size_t* consumed) noexcept {
  return Dispatch<sz_utf8_newlines_haswell, sz_utf8_newlines_icelake>(
    text, length, offsets, lengths, capacity, consumed);
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
#endif

}  // namespace irs::analysis::sz
