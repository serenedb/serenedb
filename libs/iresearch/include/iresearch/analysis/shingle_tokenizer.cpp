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

Tokenizer::ptr ShingleTokenizer::Make(Options opts) {
  Tokenizer::ptr base;
  if (opts.base_analyzer) {
    base = CreateTokenizer(std::move(*opts.base_analyzer));
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

}  // namespace

void ShingleTokenizer::WriteToken(bytes_view token, bstring& out) {
  WriteTokenLength(static_cast<uint32_t>(token.size()), out);
  out.append(token.data(), token.size());
}

void ShingleTokenizer::WriteToken(duckdb::string_t token, bstring& out) {
  const uint32_t size = token.GetSize();
  WriteTokenLength(size, out);
  out.append(reinterpret_cast<const byte_type*>(token.GetData()), size);
}

const byte_type* ShingleTokenizer::ReadTokenChecked(
  const byte_type* p, const byte_type* end, bytes_view& token) noexcept {
  if (p >= end) {
    return nullptr;
  }
  const uint32_t head = *p++;
  uint32_t n = head & 0x3F;
  switch (head >> 6) {
    case 0:
      break;
    case 1:
      if (end - p < 1) {
        return nullptr;
      }
      n = (n << 8) | uint32_t{*p++};
      break;
    case 2:
      if (end - p < 3) {
        return nullptr;
      }
      n <<= 24;
      n |= uint32_t{*p++} << 16;
      n |= uint32_t{*p++} << 8;
      n |= uint32_t{*p++};
      break;
    default:
      return nullptr;
  }
  if (static_cast<size_t>(end - p) < n) {
    return nullptr;
  }
  token = bytes_view{p, n};
  return p + n;
}

const byte_type* ShingleTokenizer::ReadToken(const byte_type* p,
                                             bytes_view& token) noexcept {
  const uint32_t head = *p++;
  uint32_t n = head & 0x3F;
  switch (head >> 6) {
    case 0:
      break;
    case 1:
      n = (n << 8) | uint32_t{*p++};
      break;
    default:
      n <<= 24;
      n |= uint32_t{*p++} << 16;
      n |= uint32_t{*p++} << 8;
      n |= uint32_t{*p++};
      break;
  }
  token = bytes_view{p, n};
  return p + n;
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
    _frequent.emplace(ViewCast<char>(bytes_view{word}));
  }
  _has_frequent = !_frequent.empty();
  if (_has_frequent) {
    _output_unigrams = true;
  }
  SDB_ASSERT(_min >= 1 && _max >= _min);
}

template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent,
         bool StoreTokens>
bool ShingleTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  _tok.clear();
  _pos.clear();
  _needs_intern.clear();
  if constexpr (HasFrequent) {
    _freq.clear();
  }
  _arena.Reset();

  _accumulator.Bind(_tok, _pos, _producer_dense, nullptr, nullptr, raw,
                    &_needs_intern);
  _scratch_writer->Bind(_accumulator, nullptr);
  if (!_analyzer->Fill(raw, *_scratch_writer, TokenLayout::TermsPos)) {
    return false;
  }
  _scratch_writer->Finish();

  const uint32_t n = static_cast<uint32_t>(_tok.size());
  const bool no_shingles = n < _min;
  _tok_psum.resize(n + 1);
  _tok_psum[0] = 0;
  for (uint32_t k = 0; k < n; ++k) {
    _tok_psum[k + 1] = _tok_psum[k] + _tok[k].GetSize();
  }

  if constexpr (HasFrequent) {
    _freq.resize(n);
    for (uint32_t i = 0; i < n; ++i) {
      _freq[i] = _frequent.contains(
                   std::string_view{_tok[i].GetData(), _tok[i].GetSize()})
                   ? 1
                   : 0;
    }
  }

  const auto emit_unigram = [&](uint32_t i, uint32_t pos) {
    if (_needs_intern[i]) {
      const auto term = _tok[i];
      const uint32_t size = term.GetSize();
      if (size <= duckdb::string_t::INLINE_LENGTH) {
        sink.Emit<Layout>(term, pos);
      } else {
        const char* const data = term.GetData();
        sink.Emit<Layout>(
          size,
          [&](byte_type* mem) IRS_FORCE_INLINE {
            std::memcpy(mem, data, size);
            return size;
          },
          pos);
      }
    } else {
      sink.Emit<Layout>(_tok[i], pos);
    }
  };

  // Every shingle of window i is a strict prefix of the next longer one
  // (fixed separator): the wanted prefix lengths are collected first
  // (`want(s)` gates sizes per the frequent-words rules), then the sink
  // stages the full window per wave and each size is a prefix view of it --
  // O(reach) bytes copied instead of O(reach^2).
  const auto append = [&](byte_type* w, uint32_t i, uint32_t j) {
    if (j != 0) {
      std::memcpy(w, _separator.data(), _separator.size());
      w += _separator.size();
    }
    const auto t = _tok[i + j];
    const uint32_t size = t.GetSize();
    std::memcpy(w, t.GetData(), size);
    return w + size;
  };
  const auto window_len = [&](uint32_t i, uint32_t s) IRS_FORCE_INLINE {
    return _tok_psum[i + s] - _tok_psum[i] +
           (s - 1) * static_cast<uint32_t>(_separator.size());
  };
  const auto emit_shingles = [&](uint32_t i, uint32_t reach, uint32_t pos,
                                 auto&& want) {
    const uint32_t total = window_len(i, reach);
    const auto stage = [&](byte_type* mem, size_t) IRS_FORCE_INLINE {
      byte_type* w = mem;
      for (uint32_t j = 0; j < reach; ++j) {
        w = append(w, i, j);
      }
    };
    if constexpr (HasFrequent) {
      auto& ends = _shingle_ends;
      ends.clear();
      for (uint32_t s = _min; s <= reach; ++s) {
        if (want(s)) {
          ends.push_back(window_len(i, s));
        }
      }
      sink.EmitK<Layout>(
        ends.size(), total, stage,
        [&](size_t j, byte_type*)
          IRS_FORCE_INLINE { return EmitKSlot{0, ends[j]}; },
        pos);
    } else {
      sink.EmitK<Layout>(
        reach - _min + 1, total, stage,
        [&](size_t j, byte_type*) IRS_FORCE_INLINE {
          return EmitKSlot{0, window_len(i, _min + static_cast<uint32_t>(j))};
        },
        pos);
    }
  };

  for (uint32_t i = 0; i < n; ++i) {
    const uint32_t pos = _pos[i];
    if constexpr (OutputUnigrams) {
      emit_unigram(i, pos);
    } else {
      if (_output_unigrams_if_no_shingles && no_shingles) {
        emit_unigram(i, pos);
      }
    }
    uint32_t reach = 1;
    while (reach < _max && i + reach < n &&
           _pos[i + reach] - _pos[i + reach - 1] == 1) {
      ++reach;
    }
    if (reach < _min) {
      continue;
    }
    if constexpr (HasFrequent) {
      bool orv = false;
      for (uint32_t k = 0; k < _min; ++k) {
        orv |= _freq[i + k] != 0;
      }
      emit_shingles(i, reach, pos, [&](uint32_t s) {
        const bool take = s == _min || orv;
        if (i + s < n) {
          orv |= _freq[i + s] != 0;
        }
        return take;
      });
    } else {
      emit_shingles(i, reach, pos, [](uint32_t) { return true; });
    }
  }

  if constexpr (StoreTokens) {
    _blob.clear();
    const auto write_fillers = [&](uint32_t k) {
      for (; k != 0; --k) {
        WriteToken(_filler, _blob);
      }
    };
    for (uint32_t i = 0; i < n; ++i) {
      write_fillers(i == 0 ? _pos[0] - 1 : _pos[i] - _pos[i - 1] - 1);
      WriteToken(_tok[i], _blob);
    }
    sink.Store(_blob);
  }
  return true;
}

template class TypedTokenizer<ShingleTokenizer>;

}  // namespace irs::analysis
