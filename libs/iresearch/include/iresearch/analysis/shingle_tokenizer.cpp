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

namespace {

void WriteTokenLength(uint32_t n, bstring& out) {
  SDB_ASSERT(n <= ShingleTokenizer::kMaxTokenSize);
  if (n <= 0x3F) {
    out.push_back(static_cast<byte_type>(n));
  } else if (n <= 0x3FFF) {
    out.push_back(static_cast<byte_type>(0x40 | (n >> 8)));
    out.push_back(static_cast<byte_type>(n & 0xFF));
  } else {
    out.push_back(static_cast<byte_type>(0x80 | (n >> 24)));
    out.push_back(static_cast<byte_type>((n >> 16) & 0xFF));
    out.push_back(static_cast<byte_type>((n >> 8) & 0xFF));
    out.push_back(static_cast<byte_type>(n & 0xFF));
  }
}

template<bool Checked>
const byte_type* ReadTokenImpl(const byte_type* p, const byte_type* end,
                               bytes_view& token) noexcept {
  if constexpr (Checked) {
    if (p >= end) {
      return nullptr;
    }
  }
  const uint32_t head = *p++;
  uint32_t n = head & 0x3F;
  switch (head >> 6) {
    case 0:
      break;
    case 1:
      if constexpr (Checked) {
        if (end - p < 1) {
          return nullptr;
        }
      }
      n = (n << 8) | uint32_t{*p++};
      break;
    default:
      if constexpr (Checked) {
        if ((head >> 6) != 2 || end - p < 3) {
          return nullptr;
        }
      }
      n <<= 24;
      n |= uint32_t{*p++} << 16;
      n |= uint32_t{*p++} << 8;
      n |= uint32_t{*p++};
      break;
  }
  if constexpr (Checked) {
    if (static_cast<size_t>(end - p) < n) {
      return nullptr;
    }
  }
  token = bytes_view{p, n};
  return p + n;
}

}  // namespace

void ShingleTokenizer::WriteToken(bytes_view token, bstring& out) {
  WriteTokenLength(static_cast<uint32_t>(token.size()), out);
  out.append(token.data(), token.size());
}

const byte_type* ShingleTokenizer::ReadTokenChecked(
  const byte_type* p, const byte_type* end, bytes_view& token) noexcept {
  return ReadTokenImpl<true>(p, end, token);
}

const byte_type* ShingleTokenizer::ReadToken(const byte_type* p,
                                             bytes_view& token) noexcept {
  return ReadTokenImpl<false>(p, nullptr, token);
}

ShingleTokenizer::ShingleTokenizer(Tokenizer::ptr base, Options&& options)
  : _analyzer{std::move(base)},
    _min{options.min_shingle_size},
    _max{options.max_shingle_size},
    _output_unigrams{options.output_unigrams},
    _output_unigrams_if_no_shingles{options.output_unigrams_if_no_shingles},
    _store_tokens{options.store_tokens},
    _separator{std::move(options.token_separator)},
    _filler{std::move(options.filler_token)} {
  if (!_analyzer) {
    _analyzer = std::make_unique<KeywordTokenizer>();
  }
  _producer_dense = !_analyzer->Traits().explicit_pos;
  if (_separator.empty()) {
    _separator.push_back(kDefaultSeparator);
  }
  if (_filler.empty()) {
    _filler.push_back(static_cast<byte_type>('_'));
  }
  for (const auto& word : options.frequent_words) {
    _frequent.Insert(std::string{ViewCast<char>(bytes_view{word})});
  }
  _has_frequent = !_frequent.Empty();
  if (_has_frequent) {
    _output_unigrams = true;
  }
  SDB_ASSERT(_min >= 1 && _max >= _min);
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

void ShingleTokenizer::StoreBlob(TokenSink& sink, uint32_t n) {
  const auto tok = _sub->tokens.terms();
  const auto tpos = _sub->tokens.pos();
  _blob.clear();
  const auto write_fillers = [&](uint32_t k) {
    for (; k != 0; --k) {
      WriteToken(_filler, _blob);
    }
  };
  uint32_t prev = 0;
  for (uint32_t i = 0; i < n; ++i) {
    write_fillers(tpos[i] > prev ? tpos[i] - prev - 1 : 0);
    prev = tpos[i];
    WriteToken(AsBytesView(tok[i]), _blob);
  }
  sink.Store(_blob);
}

template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent>
void ShingleTokenizer::EmitRuns(duckdb::string_t raw, TokenSink& sink,
                                uint32_t n, bool no_shingles) {
  const auto* const tok = _sub->tokens.terms().data();
  const auto* const tpos = _sub->tokens.pos().data();
  const auto emit_unigram = [&](uint32_t i, uint32_t pos) {
    const auto& term = tok[i];
    sink.Emit<Layout>(raw, term.GetData(),
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

  const bool unigrams =
    OutputUnigrams || (_output_unigrams_if_no_shingles && no_shingles);
  // One token per position. A base that stacks alternatives at one position
  // (posInc 0, e.g. synonyms) is not supported: the delta-0 token ends the
  // run here, so only the last alternative shingles forward, and StoreBlob
  // writes the stack flat, breaking its index-equals-position layout.
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

template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent,
         bool StoreTokens>
bool ShingleTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  if (!DrainBase(raw)) {
    return false;
  }

  const uint32_t n = static_cast<uint32_t>(_sub->tokens.terms().size());
  const bool no_shingles = n < _min;
  if (!no_shingles) {
    BuildTables<HasFrequent>(n);
  }

  EmitRuns<Layout, OutputUnigrams, HasFrequent>(raw, sink, n, no_shingles);

  if constexpr (StoreTokens) {
    StoreBlob(sink, n);
  }
  return true;
}

template class TypedTokenizer<ShingleTokenizer>;

}  // namespace irs::analysis
