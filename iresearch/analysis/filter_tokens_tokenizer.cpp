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

#include "filter_tokens_tokenizer.hpp"

#include <simdutf.h>

#include <algorithm>
#include <limits>

#include "iresearch/analysis/sql_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"

namespace irs::analysis {

Tokenizer::ptr FilterTokensTokenizer::Make(Options opts) {
  return std::make_unique<FilterTokensTokenizer>(std::move(opts));
}

FilterTokensTokenizer::FilterTokensTokenizer(Options opts)
  : _min{opts.min_length},
    _max{opts.max_length == 0 ? std::numeric_limits<size_t>::max()
                              : opts.max_length},
    _by_length{opts.min_length != 0 || opts.max_length != 0} {
  if (!opts.predicate.empty()) {
    _predicate = std::make_unique<SqlPredicate>(type_name(), opts.predicate);
  }
}

FilterTokensTokenizer::~FilterTokensTokenizer() = default;

void FilterTokensTokenizer::Bind(duckdb::ClientContext& ctx) {
  if (_predicate) {
    _predicate->Bind(ctx);
  }
}

void FilterTokensTokenizer::Unbind() noexcept {
  if (_predicate) {
    _predicate->Unbind();
  }
}

size_t FilterTokensTokenizer::MemoryUsage() const noexcept {
  return _predicate ? _predicate->MemoryUsage() : 0;
}

template<bool KnownAscii>
bool FilterTokensTokenizer::FitsLength(
  const duckdb::string_t& term) const noexcept {
  const size_t size = term.GetSize();
  if constexpr (KnownAscii) {
    return size >= _min && size <= _max;
  } else {
    const size_t least = (size + 3) / 4;
    if (size < _min || least > _max) {
      return false;
    }
    if (size <= _max && least >= _min) {
      return true;
    }
    const size_t chars = simdutf::count_utf8(term.GetData(), size);
    return chars >= _min && chars <= _max;
  }
}

template<bool KnownAscii>
bool FilterTokensTokenizer::DropByLength(TokenBatch& batch,
                                         uint64_t* valid) const noexcept {
  bool all_kept = true;
  for (uint32_t base = 0, n = batch.count; base < n; base += 64) {
    const auto end = std::min<uint32_t>(n, base + 64);
    const uint64_t word = valid[base >> 6];
    uint64_t marks = 0;
    for (uint32_t i = base; i < end; ++i) {
      if (((word >> (i & 63)) & 1) == 0) {
        continue;
      }
      marks |= static_cast<uint64_t>(!FitsLength<KnownAscii>(batch.terms[i]))
               << (i & 63);
    }
    if (marks != 0) {
      valid[base >> 6] = word & ~marks;
      all_kept = false;
    }
  }
  return all_kept;
}

bool FilterTokensTokenizer::ProcessTokens(TokenBatch& batch, BatchCtx& ctx) {
  SDB_ASSERT(ctx.valid);
  bool all_kept = true;
  if (_by_length) {
    all_kept = ctx.traits.ascii ? DropByLength<true>(batch, ctx.valid)
                                : DropByLength<false>(batch, ctx.valid);
  }
  if (_predicate) {
    all_kept &= _predicate->Apply(batch.terms, batch.count, ctx.valid);
  }
  return all_kept;
}

template<TokenLayout Layout, bool KnownAscii>
bool FilterTokensTokenizer::DoFill(duckdb::string_t value, TokenSink& sink) {
  if (_by_length && !FitsLength<KnownAscii>(value)) {
    return true;
  }
  if (_predicate && !_predicate->Test(value)) {
    return true;
  }
  sink.Emit<Layout>(value);
  return true;
}

template class TypedTokenizer<FilterTokensTokenizer>;

}  // namespace irs::analysis
