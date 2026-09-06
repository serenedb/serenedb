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
#include <cstring>

#include "basics/assert.h"
#include "iresearch/analysis/token_batch.hpp"

namespace irs::analysis {
namespace {

constexpr size_t kBatch = 8 * 1024;
constexpr size_t kHeadSlack = 64;

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

void SparseNGramTokenizer::EnsureScratch() {
  if (!_hashes.empty()) [[likely]] {
    return;
  }
  _stack.resize(_options.max_ngram_length + kHeadSlack + 2);
  _pending.resize(2 * (kBatch + _stack.size()));
  _hashes.resize(kBatch);
}

void SparseNGramTokenizer::FillHashes(Cursor& ctx) {
  const auto* data = reinterpret_cast<const char*>(ctx.data.data());
  const size_t end = std::min(ctx.data.size() - 1, ctx.pos + kBatch);
  kFillHashes(data + ctx.pos, end - ctx.pos, _hashes.data());
  ctx.hash_base = ctx.pos;
  ctx.hash_end = end;
}

bool SparseNGramTokenizer::Advance(Cursor& ctx) {
  const size_t pos_end = ctx.data.size() >= 2 ? ctx.data.size() - 1 : 0;
  HashAndPos* const base = _stack.data();
  HashAndPos* const limit = base + _stack.size();
  HashAndPos* top = base + ctx.top;
  size_t head = ctx.head;
  uint64_t* const pending = _pending.data();
  uint64_t* const pending_end = pending + _pending.size();
  uint64_t* out = pending;
  while (out == pending) {
    if (ctx.pos < pos_end) {
      if (ctx.pos >= ctx.hash_end) {
        FillHashes(ctx);
      }
      const uint32_t* hashes = _hashes.data() - ctx.hash_base;
      const size_t end_i = std::min(pos_end, ctx.hash_end);
      const size_t depth = static_cast<size_t>(top - (base + head));
      const size_t room = static_cast<size_t>(pending_end - out);
      SDB_ASSERT(room > depth);
      const size_t stop_i = std::min(end_i, ctx.pos + (room - depth) / 2);
      if (_options.covering) {
        for (size_t i = ctx.pos; i < stop_i; ++i) {
          StepCovering(base, top, head, out, i, hashes[i]);
        }
      } else {
        for (size_t i = ctx.pos; i < stop_i; ++i) {
          StepAll(base, limit, top, out, i, hashes[i]);
        }
      }
      SDB_ASSERT(top <= limit);
      ctx.pos = stop_i;
      if (stop_i < end_i) {
        break;
      }
    } else if (_options.covering && top - (base + head) > 1) {
      while (top - (base + head) > 1) {
        const size_t last = top[-1].pos + 2;
        --top;
        Emit(out, top[-1].pos, last);
      }
    } else {
      break;
    }
  }
  ctx.top = static_cast<size_t>(top - base);
  ctx.head = head;
  ctx.pending_size = static_cast<size_t>(out - pending);
  return ctx.pending_size != 0;
}

void SparseNGramTokenizer::StepAll(HashAndPos* base, HashAndPos* limit,
                                   HashAndPos*& top, uint64_t*& out, size_t i,
                                   uint32_t hash) const {
  const size_t min_pos = i + 2 - std::min(i + 2, _options.max_ngram_length);
  while (top != base && hash > top[-1].hash) {
    if (top[-1].pos < min_pos) {
      top = base;
      break;
    }
    Emit(out, top[-1].pos, i + 2);
    while (top - base > 1 && top[-1].hash == top[-2].hash) {
      --top;
    }
    --top;
  }
  if (top != base && top[-1].pos >= min_pos) {
    Emit(out, top[-1].pos, i + 2);
  }
  *top++ = {hash, static_cast<uint32_t>(i)};
  if (top == limit) [[unlikely]] {
    HashAndPos* live = base;
    while (live != top && live->pos < min_pos) {
      ++live;
    }
    const size_t keep = static_cast<size_t>(top - live);
    std::memmove(base, live, keep * sizeof *base);
    top = base + keep;
  }
}

void SparseNGramTokenizer::StepCovering(HashAndPos* base, HashAndPos*& top,
                                        size_t& head, uint64_t*& out, size_t i,
                                        uint32_t hash) const {
  HashAndPos* live = base + head;
  if (top - live > 1 && i - live->pos + 3 >= _options.max_ngram_length) {
    Emit(out, live->pos, live[1].pos + 2);
    if (++head >= kHeadSlack) {
      std::memmove(base, base + head,
                   static_cast<size_t>(top - (base + head)) * sizeof *base);
      top -= head;
      head = 0;
    }
    live = base + head;
  }
  while (top != live && hash > top[-1].hash) {
    if (live->hash == top[-1].hash) {
      Emit(out, top[-1].pos, i + 2);
      while (top - live > 1) {
        const size_t last = top[-1].pos + 2;
        --top;
        Emit(out, top[-1].pos, last);
      }
    }
    --top;
    if (top == live) {
      top = base;
      head = 0;
      live = base;
    }
  }
  *top++ = {hash, static_cast<uint32_t>(i)};
}

template<TokenLayout Layout>
bool SparseNGramTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const size_t size = raw.GetSize();
  EnsureScratch();
  const uint64_t* const pending = _pending.data();
  Cursor ctx{.data = {reinterpret_cast<const byte_type*>(raw.GetData()), size}};
  while (Advance(ctx)) {
    sink.EmitK<Layout>(ctx.pending_size, ctx.data.data(),
                       ctx.data.data() + ctx.data.size(),
                       [&](size_t j) IRS_FORCE_INLINE {
                         const uint64_t entry = pending[j];
                         return EmitKSlot{static_cast<uint32_t>(entry),
                                          static_cast<uint32_t>(entry >> 32)};
                       });
  }
  return true;
}

template class TypedTokenizer<SparseNGramTokenizer>;

}  // namespace irs::analysis
