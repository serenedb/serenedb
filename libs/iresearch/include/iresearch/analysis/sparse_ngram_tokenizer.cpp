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

#include "sparse_ngram_tokenizer.hpp"

#include <algorithm>
#include <limits>

#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace irs::analysis {
namespace {

constexpr size_t kBatch = 8 * 1024;

constexpr uint64_t kMul1 = 0xc6a4a7935bd1e995ULL;
constexpr uint64_t kMul2 = 0x228876a7198b743ULL;

inline uint32_t HashBigram(const char* begin) {
  uint64_t a = begin[0] * kMul1 + begin[1] * kMul2;
  return a + (~a >> 47);
}

void FillHashesScalar(const char* data, size_t count, uint32_t* out) {
  for (size_t j = 0; j < count; ++j) {
    out[j] = HashBigram(data + j);
  }
}

#if defined(__x86_64__)
__attribute__((target("avx512f,avx512dq"))) void FillHashesAvx512(
  const char* data, size_t count, uint32_t* out) {
  const __m512i k1 = _mm512_set1_epi64(kMul1);
  const __m512i k2 = _mm512_set1_epi64(kMul2);
  const __m512i ones = _mm512_set1_epi64(-1);
  size_t j = 0;
  for (; j + 8 <= count; j += 8) {
    const __m512i b0 = _mm512_cvtepi8_epi64(
      _mm_loadl_epi64(reinterpret_cast<const __m128i*>(data + j)));
    const __m512i b1 = _mm512_cvtepi8_epi64(
      _mm_loadl_epi64(reinterpret_cast<const __m128i*>(data + j + 1)));
    const __m512i a =
      _mm512_add_epi64(_mm512_mullo_epi64(b0, k1), _mm512_mullo_epi64(b1, k2));
    const __m512i not_a = _mm512_xor_si512(a, ones);
    const __m512i h = _mm512_add_epi64(a, _mm512_srli_epi64(not_a, 47));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + j),
                        _mm512_cvtepi64_epi32(h));
  }
  FillHashesScalar(data + j, count - j, out + j);
}
#endif

using FillHashesFn = void (*)(const char*, size_t, uint32_t*);

FillHashesFn ResolveFillHashes() {
#if defined(__x86_64__)
  if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512dq")) {
    return FillHashesAvx512;
  }
#endif
  return FillHashesScalar;
}

const FillHashesFn kFillHashes = ResolveFillHashes();

}  // namespace

Analyzer::ptr SparseNGramTokenizer::Make(Options opts) {
  return std::make_unique<SparseNGramTokenizer>(std::move(opts));
}

SparseNGramTokenizer::SparseNGramTokenizer(Options options)
  : _options(options) {
  _options.max_ngram_length = std::max<size_t>(_options.max_ngram_length, 3);
}

bool SparseNGramTokenizer::reset(std::string_view value) {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  _data = ViewCast<byte_type>(value);
  std::get<IncAttr>(_attrs).value = 1;
  _stack.clear();
  _head = 0;
  _pending_size = 0;
  _pending_idx = 0;
  _pos = 0;
  _hash_base = 0;
  _hash_end = 0;
  _finalized = !_options.covering;
  return true;
}

void SparseNGramTokenizer::FillHashes() {
  const auto* data = reinterpret_cast<const char*>(_data.data());
  const size_t end = std::min(_data.size() - 1, _pos + kBatch);
  _hashes.resize(end - _pos);
  kFillHashes(data + _pos, end - _pos, _hashes.data());
  _hash_base = _pos;
  _hash_end = end;
}

bool SparseNGramTokenizer::Advance() {
  _pending_idx = 0;
  const size_t pos_end = _data.size() >= 2 ? _data.size() - 1 : 0;
  if (_pending.size() < 2 * StackSize() + kBatch) {
    _pending.resize(2 * StackSize() + kBatch);
  }
  _pending_out = _pending.data();
  while (_pending_out == _pending.data()) {
    if (_pos < pos_end) {
      if (_pos >= _hash_end) {
        FillHashes();
      }
      const size_t budget = (_pending.size() - 2 * StackSize()) / 3;
      if (budget == 0) {
        _pending.resize(_pending.size() + 2 * StackSize() + kBatch);
        _pending_out = _pending.data();
        continue;
      }
      const size_t end_i = std::min({pos_end, _hash_end, _pos + budget});
      const uint32_t* hashes = _hashes.data() - _hash_base;
      if (_options.covering) {
        for (size_t i = _pos; i < end_i; ++i) {
          StepCovering(i, hashes[i]);
        }
      } else {
        for (size_t i = _pos; i < end_i; ++i) {
          StepAll(i, hashes[i]);
        }
      }
      _pos = end_i;
    } else if (!_finalized) {
      _finalized = true;
      while (StackSize() > 1) {
        const size_t last = _stack.back().pos + 2;
        _stack.pop_back();
        Emit(_stack.back().pos, last);
      }
    } else {
      break;
    }
  }
  _pending_size = _pending_out - _pending.data();
  return _pending_size != 0;
}

void SparseNGramTokenizer::StepAll(size_t i, uint32_t hash) {
  const HashAndPos p{hash, i};
  const size_t min_pos = i + 2 - std::min(i + 2, _options.max_ngram_length);
  while (!_stack.empty() && p.hash > _stack.back().hash) {
    if (_stack.back().pos < min_pos) {
      _stack.clear();
      break;
    }
    Emit(_stack.back().pos, i + 2);
    while (_stack.size() > 1 &&
           _stack.back().hash == _stack[_stack.size() - 2].hash) {
      _stack.pop_back();
    }
    _stack.pop_back();
  }
  if (!_stack.empty() && _stack.back().pos >= min_pos) {
    Emit(_stack.back().pos, i + 2);
  }
  _stack.push_back(p);
}

void SparseNGramTokenizer::StepCovering(size_t i, uint32_t hash) {
  const HashAndPos p{hash, i};
  if (StackSize() > 1 &&
      i - _stack[_head].pos + 3 >= _options.max_ngram_length) {
    Emit(_stack[_head].pos, _stack[_head + 1].pos + 2);
    if (++_head >= 64) {
      _stack.erase(_stack.begin(), _stack.begin() + _head);
      _head = 0;
    }
  }
  while (StackSize() > 0 && p.hash > _stack.back().hash) {
    if (_stack[_head].hash == _stack.back().hash) {
      Emit(_stack.back().pos, i + 2);
      while (StackSize() > 1) {
        const size_t last = _stack.back().pos + 2;
        _stack.pop_back();
        Emit(_stack.back().pos, last);
      }
    }
    _stack.pop_back();
    if (_head == _stack.size()) {
      _stack.clear();
      _head = 0;
    }
  }
  _stack.push_back(p);
}

}  // namespace irs::analysis
