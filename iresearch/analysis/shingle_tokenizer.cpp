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

#include "iresearch/analysis/shingle_tokenizer.hpp"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cstring>

#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::analysis {

Tokenizer::ptr ShingleTokenizer::Make(Options opts,
                                      duckdb::SharedObjectCache& cache) {
  Tokenizer::ptr base;
  if (opts.base_analyzer) {
    base = CreateTokenizer(std::move(*opts.base_analyzer), cache);
  }
  return std::make_unique<ShingleTokenizer>(std::move(base), std::move(opts));
}

ShingleTokenizer::ShingleTokenizer(Tokenizer::ptr base, Options&& options)
  : _analyzer{std::move(base)},
    _min{options.min_shingle_size},
    _max{options.max_shingle_size},
    _output_unigrams{options.output_unigrams},
    _fallback_unigrams{options.fallback_unigrams},
    _separator{std::move(options.token_separator)} {
  if (!_analyzer) {
    _analyzer = std::make_unique<KeywordTokenizer>();
  }
  _producer_dense = !_analyzer->Traits().explicit_pos;
  for (const auto& word : options.frequent_words) {
    _frequent.Insert(std::string{ViewCast<char>(bytes_view{word})});
  }
  _has_frequent = !_frequent.Empty();
  if (_has_frequent) {
    _output_unigrams = true;
  }
  SDB_ASSERT(_min >= 1 && _max >= _min);
}

bstring ShingleTokenizer::Join(std::span<const bytes_view> tokens) const {
  const auto joined =
    absl::StrJoin(tokens, ViewCast<char>(bytes_view{_separator}),
                  [](std::string* out, bytes_view token) {
                    absl::StrAppend(out, ViewCast<char>(token));
                  });
  return bstring{ViewCast<byte_type>(std::string_view{joined})};
}

bool ShingleTokenizer::DrainBase(duckdb::string_t raw) {
  return _sub->analyzer.Analyze(*_analyzer, raw, _sub->tokens);
}

template<bool HasFrequent>
void ShingleTokenizer::BuildTables(uint32_t n) {
  const auto tok = _sub->tokens.terms();
  _tok_psum.resize(n + 1);
  _tok_psum[0] = 0;
  if constexpr (HasFrequent) {
    _freq.resize(n);
  }
  for (uint32_t k = 0; k < n; ++k) {
    _tok_psum[k + 1] = _tok_psum[k] + tok[k].GetSize();
    if constexpr (HasFrequent) {
      _freq[k] = _frequent.Contains(tok[k]) ? 1 : 0;
    }
  }
}

template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent>
void ShingleTokenizer::EmitRuns(const duckdb::string_t* raw, TokenSink& sink,
                                uint32_t n, bool no_shingles) {
  const auto* const tok = _sub->tokens.terms().data();
  const auto* const tpos = _sub->tokens.pos().data();
  const auto emit_unigram = [&](uint32_t i, uint32_t pos) {
    const auto& term = tok[i];
    sink.Emit<Layout>(raw ? *raw : term, term.GetData(),
                      static_cast<uint32_t>(term.GetSize()), pos);
  };

  const auto* const sep = _separator.data();
  const auto sep_size = static_cast<uint32_t>(_separator.size());
  const auto* const psum = _tok_psum.data();
  const auto window_len = [=](uint32_t i, uint32_t s) IRS_FORCE_INLINE {
    return psum[i + s] - psum[i] + (s - 1) * sep_size;
  };
  const auto emit_shingles = [&](uint32_t i, uint32_t reach, uint32_t pos) {
    size_t count;
    uint32_t span;
    if constexpr (HasFrequent) {
      auto& ends = _shingle_ends;
      ends.clear();
      bool orv = false;
      for (uint32_t k = 0; k < _min; ++k) {
        orv |= _freq[i + k] != 0;
      }
      for (uint32_t s = _min; s <= reach; ++s) {
        if (s == _min || orv) {
          ends.push_back(window_len(i, s));
        }
        if (i + s < n) {
          orv |= _freq[i + s] != 0;
        }
      }
      count = ends.size();
      span = count == 1 ? _min : reach;
    } else {
      count = reach - _min + 1;
      span = reach;
    }
    const auto stage = [=](byte_type* mem) IRS_FORCE_INLINE {
      const auto first = tok[i];
      const uint32_t first_size = first.GetSize();
      std::memcpy(mem, first.GetData(), first_size);
      byte_type* w = mem + first_size;
      for (uint32_t j = 1; j < span; ++j) {
        std::memcpy(w, sep, sep_size);
        w += sep_size;
        const auto t = tok[i + j];
        const uint32_t size = t.GetSize();
        std::memcpy(w, t.GetData(), size);
        w += size;
      }
    };
    sink.EmitK<Layout>(count + (OutputUnigrams ? 1 : 0), window_len(i, span),
                       stage, [&](size_t j, byte_type*) IRS_FORCE_INLINE {
                         if constexpr (OutputUnigrams) {
                           if (j == 0) {
                             return EmitKSlotPos{0, window_len(i, 1), pos};
                           }
                           --j;
                         }
                         if constexpr (HasFrequent) {
                           return EmitKSlotPos{0, _shingle_ends[j], pos};
                         } else {
                           return EmitKSlotPos{
                             0, window_len(i, _min + static_cast<uint32_t>(j)),
                             pos};
                         }
                       });
  };

  const bool unigrams = OutputUnigrams || (_fallback_unigrams && no_shingles);
  uint32_t run_end = 0;
  for (uint32_t i = 0; i < n; ++i) {
    const uint32_t pos = tpos[i];
    if (run_end <= i) {
      run_end = i + 1;
    }
    while (run_end - i < _max && run_end < n &&
           tpos[run_end] - tpos[run_end - 1] == 1) {
      ++run_end;
    }
    const uint32_t reach = run_end - i;
    if (reach < _min) {
      if (unigrams) {
        emit_unigram(i, pos);
      }
      continue;
    }
    emit_shingles(i, reach, pos);
  }
}

template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent>
void ShingleTokenizer::EmitBaseTokens(const duckdb::string_t* raw,
                                      TokenSink& sink) {
  const uint32_t n = static_cast<uint32_t>(_sub->tokens.terms().size());
  const bool no_shingles = n < _min;
  if (!no_shingles) {
    BuildTables<HasFrequent>(n);
  }
  EmitRuns<Layout, OutputUnigrams, HasFrequent>(raw, sink, n, no_shingles);
}

template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent>
bool ShingleTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  if (!DrainBase(raw)) {
    return false;
  }
  EmitBaseTokens<Layout, OutputUnigrams, HasFrequent>(&raw, sink);
  return true;
}

bool ShingleTokenizer::FillTokens(std::span<const duckdb::string_t> tokens,
                                  TokenSink& sink, FillCtx ctx) {
  return DispatchFill(
    *this, ctx.layout, ctx.traits,
    [&](auto layout_tag, auto unigrams_tag, auto frequent_tag)
      IRS_FORCE_INLINE {
        _sub->tokens.Assign(tokens);
        EmitBaseTokens<layout_tag(), unigrams_tag(), frequent_tag()>(nullptr,
                                                                     sink);
        return true;
      });
}

template class TypedTokenizer<ShingleTokenizer>;

}  // namespace irs::analysis
