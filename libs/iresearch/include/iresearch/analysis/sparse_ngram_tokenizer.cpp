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

#include "iresearch/analysis/token_batch.hpp"

namespace irs::analysis {
namespace {

constexpr size_t kBatch = 8 * 1024;

constexpr uint64_t kMul1 = 0xc6a4a7935bd1e995ULL;
constexpr uint64_t kMul2 = 0x228876a7198b743ULL;

inline uint32_t HashBigram(const char* begin) {
  const uint64_t a = static_cast<uint8_t>(begin[0]) * kMul1 +
                     static_cast<uint8_t>(begin[1]) * kMul2;
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
  for (size_t j = 0; j < count; ++j) {
    out[j] = HashBigram(data + j);
  }
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

Tokenizer::ptr SparseNGramTokenizer::Make(Options opts) {
  return std::make_unique<SparseNGramTokenizer>(std::move(opts));
}

SparseNGramTokenizer::SparseNGramTokenizer(Options options)
  : _options(options) {
  _options.max_ngram_length = std::max<size_t>(_options.max_ngram_length, 3);
}

void SparseNGramTokenizer::FillHashes(Cursor& ctx) {
  const auto* data = reinterpret_cast<const char*>(ctx.data.data());
  const size_t end = std::min(ctx.data.size() - 1, ctx.pos + kBatch);
  _hashes.resize(end - ctx.pos);
  kFillHashes(data + ctx.pos, end - ctx.pos, _hashes.data());
  ctx.hash_base = ctx.pos;
  ctx.hash_end = end;
}

bool SparseNGramTokenizer::Advance(Cursor& ctx) {
  const size_t pos_end = ctx.data.size() >= 2 ? ctx.data.size() - 1 : 0;
  if (_pending.size() < 2 * StackSize(ctx) + kBatch) {
    _pending.resize(2 * StackSize(ctx) + kBatch);
  }
  ctx.pending_out = _pending.data();
  while (ctx.pending_out == _pending.data()) {
    if (ctx.pos < pos_end) {
      if (ctx.pos >= ctx.hash_end) {
        FillHashes(ctx);
      }
      const size_t budget = (_pending.size() - 2 * StackSize(ctx)) / 3;
      if (budget == 0) {
        _pending.resize(_pending.size() + 2 * StackSize(ctx) + kBatch);
        ctx.pending_out = _pending.data();
        continue;
      }
      const size_t end_i = std::min({pos_end, ctx.hash_end, ctx.pos + budget});
      const uint32_t* hashes = _hashes.data() - ctx.hash_base;
      if (_options.covering) {
        for (size_t i = ctx.pos; i < end_i; ++i) {
          StepCovering(ctx, i, hashes[i]);
        }
      } else {
        for (size_t i = ctx.pos; i < end_i; ++i) {
          StepAll(ctx, i, hashes[i]);
        }
      }
      ctx.pos = end_i;
    } else if (_options.covering && StackSize(ctx) > 1) {
      while (StackSize(ctx) > 1) {
        const size_t last = _stack.back().pos + 2;
        _stack.pop_back();
        Emit(ctx, _stack.back().pos, last);
      }
    } else {
      break;
    }
  }
  ctx.pending_size = ctx.pending_out - _pending.data();
  return ctx.pending_size != 0;
}

void SparseNGramTokenizer::StepAll(Cursor& ctx, size_t i, uint32_t hash) {
  const HashAndPos p{hash, static_cast<uint32_t>(i)};
  const size_t min_pos = i + 2 - std::min(i + 2, _options.max_ngram_length);
  while (!_stack.empty() && p.hash > _stack.back().hash) {
    if (_stack.back().pos < min_pos) {
      _stack.clear();
      break;
    }
    Emit(ctx, _stack.back().pos, i + 2);
    while (_stack.size() > 1 &&
           _stack.back().hash == _stack[_stack.size() - 2].hash) {
      _stack.pop_back();
    }
    _stack.pop_back();
  }
  if (!_stack.empty() && _stack.back().pos >= min_pos) {
    Emit(ctx, _stack.back().pos, i + 2);
  }
  _stack.push_back(p);
}

void SparseNGramTokenizer::StepCovering(Cursor& ctx, size_t i, uint32_t hash) {
  const HashAndPos p{hash, static_cast<uint32_t>(i)};
  if (StackSize(ctx) > 1 &&
      i - _stack[ctx.head].pos + 3 >= _options.max_ngram_length) {
    Emit(ctx, _stack[ctx.head].pos, _stack[ctx.head + 1].pos + 2);
    if (++ctx.head >= 64) {
      _stack.erase(_stack.begin(), _stack.begin() + ctx.head);
      ctx.head = 0;
    }
  }
  while (StackSize(ctx) > 0 && p.hash > _stack.back().hash) {
    if (_stack[ctx.head].hash == _stack.back().hash) {
      Emit(ctx, _stack.back().pos, i + 2);
      while (StackSize(ctx) > 1) {
        const size_t last = _stack.back().pos + 2;
        _stack.pop_back();
        Emit(ctx, _stack.back().pos, last);
      }
    }
    _stack.pop_back();
    if (ctx.head == _stack.size()) {
      _stack.clear();
      ctx.head = 0;
    }
  }
  _stack.push_back(p);
}

template<TokenLayout Layout>
bool SparseNGramTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  _stack.clear();
  Cursor ctx{
    .data = {reinterpret_cast<const byte_type*>(raw.GetData()), raw.GetSize()}};

  while (Advance(ctx)) {
    sink.EmitK<Layout>(ctx.pending_size, ctx.data.data(),
                       ctx.data.data() + ctx.data.size(),
                       [&](size_t j) IRS_FORCE_INLINE {
                         const uint64_t entry = _pending[j];
                         return EmitKSlot{static_cast<uint32_t>(entry),
                                          static_cast<uint32_t>(entry >> 32)};
                       });
  }
  return true;
}

template class TypedTokenizer<SparseNGramTokenizer>;

}  // namespace irs::analysis
