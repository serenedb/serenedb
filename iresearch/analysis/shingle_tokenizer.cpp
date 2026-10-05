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
  _producer = _analyzer->Traits();
  for (const auto& word : options.frequent_words) {
    _frequent.Insert(std::string{ViewCast<char>(bytes_view{word})});
  }
  if (HasFrequentWords()) {
    _output_unigrams = true;
  }
  SDB_ASSERT(_min >= 1 && _max >= _min);
}

template<bool HasFrequent>
void ShingleTokenizer::BuildTables(std::span<const duckdb::string_t> tok) {
  const auto n = static_cast<uint32_t>(tok.size());
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

template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent,
         typename Base>
void ShingleTokenizer::EmitBaseTokens(const duckdb::string_t* raw,
                                      TokenSink& sink, const Base& base) {
  constexpr bool kOffs = Base::kLayout == TokenLayout::TermsPosOffs;
  const auto terms = base.terms();
  const auto n = static_cast<uint32_t>(terms.size());
  const bool no_shingles = n < _min;
  if (!no_shingles) {
    BuildTables<HasFrequent>(terms);
  }
  const auto* const tok = terms.data();
  const auto* const tpos = base.pos().data();
  const uint32_t* starts = nullptr;
  const uint32_t* ends = nullptr;
  if constexpr (kOffs) {
    starts = base.offs_start().data();
    ends = base.offs_end().data();
  }
  const auto emit_unigram = [&](uint32_t i, uint32_t pos) {
    const auto& term = tok[i];
    const auto size = static_cast<uint32_t>(term.GetSize());
    if constexpr (kOffs) {
      sink.Emit<Layout>(raw ? *raw : term, term.GetData(), size, pos,
                        Offs{starts[i], ends[i]});
    } else {
      sink.Emit<Layout>(raw ? *raw : term, term.GetData(), size, pos);
    }
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
      auto& sizes = _shingle_sizes;
      sizes.clear();
      bool orv = false;
      for (uint32_t k = 0; k < _min; ++k) {
        orv |= _freq[i + k] != 0;
      }
      for (uint32_t s = _min; s <= reach; ++s) {
        if (s == _min || orv) {
          sizes.push_back(s);
        }
        if (i + s < n) {
          orv |= _freq[i + s] != 0;
        }
      }
      count = sizes.size();
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
    const auto slot = [&](uint32_t s) IRS_FORCE_INLINE {
      if constexpr (kOffs) {
        return EmitKSlotPosOffs{0, window_len(i, s), pos,
                                Offs{starts[i], ends[i + s - 1]}};
      } else {
        return EmitKSlotPos{0, window_len(i, s), pos};
      }
    };
    sink.EmitK<Layout>(count + (OutputUnigrams ? 1 : 0), window_len(i, span),
                       stage, [&](size_t j, byte_type*) IRS_FORCE_INLINE {
                         if constexpr (OutputUnigrams) {
                           if (j == 0) {
                             return slot(1);
                           }
                           --j;
                         }
                         if constexpr (HasFrequent) {
                           return slot(_shingle_sizes[j]);
                         } else {
                           return slot(_min + static_cast<uint32_t>(j));
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
bool ShingleTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const auto fill = [&](auto& base) IRS_FORCE_INLINE {
    if (!_sub->analyzer.Analyze(*_analyzer, raw, base)) {
      return false;
    }
    EmitBaseTokens<Layout, OutputUnigrams, HasFrequent>(&raw, sink, base);
    return true;
  };
  if constexpr (Layout == TokenLayout::TermsPosOffs) {
    if (_producer.offsets) {
      return fill(_sub->offs_tokens);
    }
  }
  return fill(_sub->tokens);
}

bool ShingleTokenizer::FillTokens(std::span<const duckdb::string_t> tokens,
                                  TokenSink& sink, FillCtx ctx) {
  return DispatchFill(
    *this, ctx.layout, ctx.traits,
    [&](auto layout_tag, auto unigrams_tag, auto frequent_tag)
      IRS_FORCE_INLINE {
        _sub->tokens.Assign(tokens);
        EmitBaseTokens<layout_tag(), unigrams_tag(), frequent_tag()>(
          nullptr, sink, _sub->tokens);
        return true;
      });
}

template class TypedTokenizer<ShingleTokenizer>;

}  // namespace irs::analysis
