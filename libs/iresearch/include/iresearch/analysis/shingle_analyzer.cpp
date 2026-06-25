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

#include "iresearch/analysis/shingle_analyzer.hpp"

#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::analysis {
namespace {

// Default token separator: a single 0xFF byte cannot appear inside a UTF-8
// token, so shingle terms are unambiguous for text columns.

}  // namespace

Analyzer::ptr ShingleAnalyzer::Make(Options opts) {
  Analyzer::ptr base;
  if (opts.base_analyzer) {
    base = CreateAnalyzer(std::move(*opts.base_analyzer));
  }
  // If `base_analyzer` is absent the ctor falls back to StringTokenizer.
  return std::make_unique<ShingleAnalyzer>(std::move(base), std::move(opts));
}

void ShingleAnalyzer::AppendShingle(std::span<const bytes_view> tokens,
                                    bytes_view separator, bstring& out) {
  out.clear();
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (i != 0) {
      out.append(separator.data(), separator.size());
    }
    out.append(tokens[i].data(), tokens[i].size());
  }
}

void ShingleAnalyzer::WriteToken(bytes_view token, std::string& out) {
  const auto n = static_cast<uint32_t>(token.size());
  SDB_ASSERT(n <= kMaxTokenSize);
  if (n <= 0x3F) {  // 00xxxxxx
    out.push_back(static_cast<char>(n));
  } else if (n <= 0x3FFF) {  // 01xxxxxx xxxxxxxx
    out.push_back(static_cast<char>(0x40 | (n >> 8)));
    out.push_back(static_cast<char>(n & 0xFF));
  } else {  // 10xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
    out.push_back(static_cast<char>(0x80 | (n >> 24)));
    out.push_back(static_cast<char>((n >> 16) & 0xFF));
    out.push_back(static_cast<char>((n >> 8) & 0xFF));
    out.push_back(static_cast<char>(n & 0xFF));
  }
  const auto bytes = ViewCast<char>(token);
  out.append(bytes.data(), bytes.size());
}

const byte_type* ShingleAnalyzer::ReadTokenChecked(const byte_type* p,
                                                   const byte_type* end,
                                                   bytes_view& token) noexcept {
  if (p >= end) {
    return nullptr;
  }
  const uint32_t head = *p++;
  uint32_t n = head & 0x3F;
  switch (head >> 6) {
    case 0:  // 1-byte length
      break;
    case 1:  // 2-byte length
      if (end - p < 1) {
        return nullptr;
      }
      n = (n << 8) | uint32_t{*p++};
      break;
    case 2:  // 4-byte length
      if (end - p < 3) {
        return nullptr;
      }
      n <<= 24;
      n |= uint32_t{*p++} << 16;
      n |= uint32_t{*p++} << 8;
      n |= uint32_t{*p++};
      break;
    default:  // selector 3 is never written
      return nullptr;
  }
  if (static_cast<size_t>(end - p) < n) {
    return nullptr;
  }
  token = bytes_view{p, n};
  return p + n;
}

const byte_type* ShingleAnalyzer::ReadToken(const byte_type* p,
                                            bytes_view& token) noexcept {
  const uint32_t head = *p++;
  uint32_t n = head & 0x3F;
  switch (head >> 6) {
    case 0:  // 1-byte length
      break;
    case 1:  // 2-byte length
      n = (n << 8) | uint32_t{*p++};
      break;
    default:  // 4-byte length (selector 2; 3 is unused)
      n <<= 24;
      n |= uint32_t{*p++} << 16;
      n |= uint32_t{*p++} << 8;
      n |= uint32_t{*p++};
      break;
  }
  token = bytes_view{p, n};
  return p + n;
}

ShingleAnalyzer::ShingleAnalyzer(Analyzer::ptr base, Options&& options) noexcept
  : _analyzer{std::move(base)},
    _min{options.min_shingle_size},
    _max{options.max_shingle_size},
    _output_unigrams{options.output_unigrams},
    _output_unigrams_if_no_shingles{options.output_unigrams_if_no_shingles},
    _store_tokens{options.store_tokens},
    _separator{std::move(options.token_separator)} {
  if (!_analyzer) {
    _analyzer = std::make_unique<StringTokenizer>();
  }
  if (_separator.empty()) {
    _separator.push_back(kDefaultSeparator);
  }
  _filler = std::move(options.filler_token);
  if (_filler.empty()) {
    _filler.push_back(static_cast<byte_type>('_'));  // Lucene's default filler
  }
  for (const auto& word : options.frequent_words) {
    _frequent.emplace(ViewCast<char>(bytes_view{word}));
  }
  if (!_frequent.empty()) {
    // A bounded vocabulary needs a term at every base position so a phrase over
    // a rare-only span can fall back to unigrams -- force unigram emission.
    _output_unigrams = true;
  }
  SDB_ASSERT(_min >= 1 && _max >= _min);
  _ring.resize(_max);
  _ring_filler.resize(_max, 0);
  _base_term = irs::get<TermAttr>(*_analyzer);
  _base_inc = irs::get<IncAttr>(*_analyzer);  // null -> assume gap-free (inc=1)
  SDB_ASSERT(_base_term);
}

Attribute* ShingleAnalyzer::GetMutable(TypeInfo::type_id type) noexcept {
  if (type == Type<TermAttr>::id()) {
    return &_shingle_term;
  }
  if (type == Type<IncAttr>::id()) {
    return &_inc;
  }
  if (type == Type<StoreAttr>::id()) {
    // Hidden when token storage is off: the indexer keys its synthetic store
    // column on this attribute's presence. The query side reads TokenBlob()
    // directly, so decomposition is unaffected.
    return _store_tokens ? &_store : nullptr;
  }
  return nullptr;
}

bytes_view ShingleAnalyzer::TokenAt(uint32_t offset) const noexcept {
  bytes_view token;
  ReadToken(reinterpret_cast<const byte_type*>(_terms.data()) + offset, token);
  return token;
}

void ShingleAnalyzer::AppendToken(bytes_view token, bool is_filler) {
  if (token.size() > kMaxTokenSize) {
    SDB_WARN(IRESEARCH, "too long input for shingle analyzer: ", token.size());
    return;
  }
  const auto offset = static_cast<uint32_t>(_terms.size());
  WriteToken(token, _terms);
  const auto slot = (_ring_head + _ring_len) % _max;
  _ring[slot] = offset;
  _ring_filler[slot] = is_filler;
  ++_ring_len;
  ++_emitted_total;
  _store.value = ViewCast<byte_type>(std::string_view{_terms});
}

void ShingleAnalyzer::PopFront() noexcept {
  SDB_ASSERT(_ring_len > 0);
  _ring_head = (_ring_head + 1) % _max;
  --_ring_len;
}

void ShingleAnalyzer::EmitPosition() noexcept {
  // The first term at a window front advances the position; co-located terms
  // share it. A silent front (a filler whose shingles were all skipped) still
  // occupies a position, carried into the next emission via _pending_inc.
  _inc.value = _front_emitted ? 0 : _pending_inc;
  _front_emitted = true;
  _pending_inc = 1;
}

bytes_view ShingleAnalyzer::BuildShingle(uint32_t size) {
  _join.clear();
  for (uint32_t j = 0; j < size; ++j) {
    _join.push_back(TokenAt(_ring[(_ring_head + j) % _max]));
  }
  AppendShingle(_join, _separator, _scratch);
  return _scratch;
}

bool ShingleAnalyzer::WindowHasFiller(uint32_t size) const noexcept {
  for (uint32_t j = 0; j < size; ++j) {
    if (_ring_filler[(_ring_head + j) % _max]) {
      return true;
    }
  }
  return false;
}

bool ShingleAnalyzer::WindowHasFrequent(uint32_t size) const noexcept {
  for (uint32_t j = 0; j < size; ++j) {
    if (IsFrequent(TokenAt(_ring[(_ring_head + j) % _max]))) {
      return true;
    }
  }
  return false;
}

bool ShingleAnalyzer::reset(std::string_view data) {
  _terms.clear();
  _ring_head = 0;
  _ring_len = 0;
  _emitted_total = 0;
  _emit_size = 0;
  _base_exhausted = false;
  _front_emitted = false;
  _pending_inc = 1;
  _pending_fillers = 0;
  _has_pending = false;
  _store.value = {};
  return _analyzer->reset(data);
}

void ShingleAnalyzer::DrainTokens(std::string_view data) {
  // Build the StoreAttr blob without constructing any shingle terms: walk
  // every base token (and filler) through AppendToken, which is all the blob
  // needs, recycling the window ring as it fills. Used by the query-side
  // decomposition, which derives its own shingles from the tokens.
  if (!reset(data)) {
    return;
  }
  const auto append = [&](bytes_view token, bool is_filler) {
    if (_ring_len == _max) {
      PopFront();
    }
    AppendToken(token, is_filler);
  };
  while (_analyzer->next()) {
    // Mirror next()'s gap handling: a base position increment > 1 (e.g. a
    // stopword the base filtered out) inserts that many fillers before the
    // real token.
    const uint32_t inc = _base_inc != nullptr ? _base_inc->value : 1;
    for (uint32_t i = 1; i < inc; ++i) {
      append(_filler, /*is_filler=*/true);
    }
    append(_base_term->value, /*is_filler=*/false);
  }
}

bool ShingleAnalyzer::next() {
  for (;;) {
    // Prime the window: a full window (max tokens) lets the front position
    // emit every shingle size; at end-of-stream a partial window emits the
    // sizes that still fit.
    while (!_base_exhausted && _ring_len < _max) {
      // Lucene-style gap handling: a base position increment > 1 (e.g. a
      // stopword the base filtered out) inserts that many filler tokens before
      // the real one, buffered across iterations so the ring never overflows.
      // With inc == 1 (no gaps) this reduces to a plain AppendToken.
      if (_pending_fillers != 0) {
        AppendToken(_filler, /*is_filler=*/true);
        --_pending_fillers;
      } else if (_has_pending) {
        AppendToken(_pending_token);
        _has_pending = false;
      } else if (_analyzer->next()) {
        const uint32_t inc = _base_inc != nullptr ? _base_inc->value : 1;
        if (inc > 1) {
          _pending_fillers = inc - 1;
          _pending_token.assign(_base_term->value);  // value may change on next()
          _has_pending = true;
        } else {
          AppendToken(_base_term->value);
        }
      } else {
        _base_exhausted = true;
      }
    }
    if (_ring_len == 0) {
      _store.value = ViewCast<byte_type>(std::string_view{_terms});
      return false;
    }
    if (_emit_size == 0) {
      _emit_size = _min;
      const bool no_shingles = _base_exhausted && _emitted_total < _min;
      // Fillers are never standalone terms (their posting list would span
      // nearly every document); they appear only inside shingles and in the
      // stored blob, and still occupy a position via _pending_inc.
      if ((_output_unigrams ||
           (_output_unigrams_if_no_shingles && no_shingles)) &&
          !_ring_filler[_ring_head]) {
        _shingle_term.value = TokenAt(_ring[_ring_head]);
        EmitPosition();
        return true;
      }
    }
    if (_emit_size >= _min && _emit_size <= _ring_len) {
      // A shingle never spans a filler: a removed token means "any single
      // token here" to every query path, which a baked-in filler byte would
      // overconstrain -- the gap instead surfaces as a position increment.
      // A frequent-words list escalates width adaptively: min-size shingles
      // are always dense (a rare one is already selective enough), wider
      // sizes exist only where the narrow ones are poor -- windows containing
      // a frequent word.
      if (!WindowHasFiller(_emit_size) &&
          (_frequent.empty() || _emit_size == _min ||
           WindowHasFrequent(_emit_size))) {
        _shingle_term.value = BuildShingle(_emit_size);
        ++_emit_size;
        EmitPosition();
        return true;
      }
      ++_emit_size;  // not in the bounded vocabulary: skip this shingle size
      continue;
    }
    if (!_front_emitted) {
      ++_pending_inc;  // silent front: its position carries into the next term
    }
    PopFront();
    _emit_size = 0;
    _front_emitted = false;
  }
}

}  // namespace irs::analysis
