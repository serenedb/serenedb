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
#include <limits>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis {
namespace {

constexpr size_t kBatch = 8 * 1024;
constexpr size_t kHeadSlack = 64;
constexpr size_t kNoStop = std::numeric_limits<size_t>::max();

constexpr uint64_t kMul1 = 0xc6a4a7935bd1e995ULL;
constexpr uint64_t kMul2 = 0x228876a7198b743ULL;

IRS_FORCE_INLINE uint32_t HashPair(uint32_t a0, uint32_t a1) {
  const uint64_t a = a0 * kMul1 + a1 * kMul2;
  return a + (~a >> 47);
}

IRS_FORCE_INLINE uint64_t FillHashesScalar(const char* data, size_t count,
                                           uint32_t* out) {
  uint64_t acc = 0;
  for (size_t j = 0; j < count; ++j) {
    const auto lead = static_cast<uint8_t>(data[j]);
    acc |= lead;
    out[j] = HashPair(lead, static_cast<uint8_t>(data[j + 1]));
  }
  return acc;
}

#if defined(__x86_64__)
__attribute__((target("avx512f,avx512dq"))) uint64_t
FillHashesAvx512(const char* data, size_t count, uint32_t* out) {
  return FillHashesScalar(data, count, out);
}
#endif

void FillHashesCp(const byte_type* data, const byte_type* end,
                  const uint32_t* bounds, size_t count, uint32_t* out) {
  const auto* it = data + bounds[0];
  uint32_t prev = utf8_utils::ToChar32(it, end);
  for (size_t j = 0; j < count; ++j) {
    const auto* next = data + bounds[j + 1];
    const uint32_t cur = utf8_utils::ToChar32(next, end);
    out[j] = HashPair(prev, cur);
    prev = cur;
  }
}

using FillHashesFn = uint64_t (*)(const char*, size_t, uint32_t*);

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

template<bool Symbols>
uint64_t SparseNGramTokenizer::FillHashes(Cursor& ctx) {
  const size_t end = std::min(ctx.units - 1, ctx.pos + kBatch);
  uint64_t acc = 0;
  if constexpr (Symbols) {
    FillHashesCp(ctx.data.data(), ctx.data.data() + ctx.data.size(),
                 _bounds.data() + ctx.pos, end - ctx.pos, _hashes.data());
  } else {
    const auto* data = reinterpret_cast<const char*>(ctx.data.data());
    acc = kFillHashes(data + ctx.pos, end - ctx.pos, _hashes.data());
  }
  ctx.hash_base = ctx.pos;
  ctx.hash_end = end;
  return acc;
}

template<bool Symbols>
bool SparseNGramTokenizer::Next(Cursor& ctx) {
  const size_t pos_end = ctx.units >= 2 ? ctx.units - 1 : 0;
  HashAndPos* const base = _stack.data();
  HashAndPos* const limit = base + _stack.size();
  HashAndPos* top = base + ctx.top;
  size_t head = ctx.head;
  EmitKSlot* const pending = _pending.data();
  EmitKSlot* const pending_end = pending + _pending.size();
  EmitKSlot* out = pending;
  while (out == pending) {
    if (ctx.pos < pos_end) {
      if (ctx.pos >= ctx.hash_end) {
        FillHashes<Symbols>(ctx);
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
                                   HashAndPos*& top, EmitKSlot*& out, size_t i,
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
                                        size_t& head, EmitKSlot*& out, size_t i,
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

template<TokenLayout Layout, bool Detect>
bool SparseNGramTokenizer::FillBytes(duckdb::string_t raw, TokenSink& sink) {
  const size_t size = raw.GetSize();
  const EmitKSlot* const pending = _pending.data();
  Cursor ctx{.data = {reinterpret_cast<const byte_type*>(raw.GetData()), size},
             .units = size};
  if constexpr (Detect) {
    if (size > kBatch) {
      if (!classify::IsAsciiValue(raw.GetData(), size)) {
        return false;
      }
    } else if (size >= 2) {
      const uint64_t bytes = FillHashes<false>(ctx) | ctx.data.back();
      if ((bytes & 0x80) != 0) {
        return false;
      }
    }
  }
  const size_t stop = _options.covering ? kNoStop : (size >= 2 ? size - 1 : 0);
  while (Next<false>(ctx)) {
    sink.EmitK<Layout>(ctx.pending_size, ctx.data.data(),
                       ctx.data.data() + ctx.data.size(),
                       [&](size_t j) IRS_FORCE_INLINE { return pending[j]; });
    if (ctx.pos >= stop) {
      break;
    }
  }
  return true;
}

template<TokenLayout Layout>
bool SparseNGramTokenizer::FillSymbols(duckdb::string_t raw, TokenSink& sink) {
  const size_t size = raw.GetSize();
  const EmitKSlot* const pending = _pending.data();
  const auto* const data = reinterpret_cast<const byte_type*>(raw.GetData());
  const size_t units = classify::BuildUtf8CpBounds(data, size, true, _bounds);
  Cursor ctx{.data = {data, size}, .units = units};
  const uint32_t* const bounds = _bounds.data();
  while (Next<true>(ctx)) {
    sink.EmitK<Layout>(ctx.pending_size, ctx.data.data(),
                       ctx.data.data() + ctx.data.size(),
                       [&](size_t j) IRS_FORCE_INLINE {
                         const auto slot = pending[j];
                         return EmitKSlot{bounds[slot.begin], bounds[slot.end]};
                       });
  }
  return true;
}

template<TokenLayout Layout, bool KnownAscii>
bool SparseNGramTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  EnsureScratch();
  if constexpr (KnownAscii) {
    return FillBytes<Layout, false>(raw, sink);
  } else if (FillBytes<Layout, true>(raw, sink)) {
    return true;
  } else {
    return FillSymbols<Layout>(raw, sink);
  }
}

template class TypedTokenizer<SparseNGramTokenizer>;

}  // namespace irs::analysis
