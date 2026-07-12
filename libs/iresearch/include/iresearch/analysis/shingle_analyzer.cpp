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

#include <algorithm>
#include <limits>

#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::analysis {

Analyzer::ptr ShingleAnalyzer::Make(Options opts) {
  Analyzer::ptr base;
  if (opts.base_analyzer) {
    base = CreateAnalyzer(std::move(*opts.base_analyzer));
    if (!base) {
      return nullptr;
    }
  }
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

ShingleAnalyzer::ShingleAnalyzer(Analyzer::ptr base, Options&& options)
  : _analyzer{std::move(base)},
    _min{std::max(options.min_shingle_size, 1u)},
    _max{std::max(options.max_shingle_size, _min)},
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
    _filler.push_back(static_cast<byte_type>('_'));
  }
  for (const auto& word : options.frequent_words) {
    _frequent.emplace(ViewCast<char>(bytes_view{word}));
  }
  if (!_frequent.empty()) {
    _output_unigrams = true;
  }
  _ring.resize(_max);
  _base_term = irs::get<TermAttr>(*_analyzer);
  _base_inc = irs::get<IncAttr>(*_analyzer);
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
    // Hidden when token storage is off: the indexer keys the synthetic store
    // column on this attribute's presence.
    return _store_tokens ? &_store : nullptr;
  }
  return nullptr;
}

void ShingleAnalyzer::TrimBlob() {
  if (_ring_len == 0) {
    _terms.clear();
    return;
  }
  const auto& front = WindowAt(0);
  const auto dead = front.offset - PrefixSize(front.size);
  _terms.erase(0, dead);
  for (uint32_t j = 0; j < _ring_len; ++j) {
    _ring[(_ring_head + j) % _max].offset -= dead;
  }
}

void ShingleAnalyzer::AppendToken(bytes_view token, bool is_filler) {
  if (token.size() > kMaxTokenSize) {
    SDB_WARN(IRESEARCH, "too long input for shingle analyzer: ", token.size());
    token = _filler;
    is_filler = true;
  }
  if (_trim_blob && _terms.size() >= kTrimThreshold) {
    TrimBlob();
  }
  SDB_ASSERT(_terms.size() + token.size() + sizeof(uint32_t) <=
             std::numeric_limits<uint32_t>::max());
  WriteToken(token, _terms);
  auto& slot = _ring[(_ring_head + _ring_len) % _max];
  slot.offset = static_cast<uint32_t>(_terms.size() - token.size());
  slot.size = static_cast<uint32_t>(token.size());
  slot.filler = is_filler;
  slot.frequent = !is_filler && !_frequent.empty() && IsFrequent(token);
  ++_ring_len;
  _real_tokens += !is_filler;
  _store.value = ViewCast<byte_type>(std::string_view{_terms});
}

void ShingleAnalyzer::PopFront() noexcept {
  SDB_ASSERT(_ring_len > 0);
  _ring_head = (_ring_head + 1) % _max;
  --_ring_len;
}

void ShingleAnalyzer::EmitPosition() noexcept {
  _inc.value = _front_emitted ? 0 : _pending_inc;
  _front_emitted = true;
  _pending_inc = 1;
}

bytes_view ShingleAnalyzer::BuildShingle(uint32_t size) {
  _join.clear();
  for (uint32_t j = 0; j < size; ++j) {
    _join.push_back(TokenAt(WindowAt(j)));
  }
  AppendShingle(_join, _separator, _scratch);
  return _scratch;
}

bool ShingleAnalyzer::WindowHasFiller(uint32_t size) const noexcept {
  for (uint32_t j = 0; j < size; ++j) {
    if (WindowAt(j).filler) {
      return true;
    }
  }
  return false;
}

bool ShingleAnalyzer::WindowHasFrequent(uint32_t size) const noexcept {
  for (uint32_t j = 0; j < size; ++j) {
    if (WindowAt(j).frequent) {
      return true;
    }
  }
  return false;
}

bool ShingleAnalyzer::reset(std::string_view data) {
  _terms.clear();
  _ring_head = 0;
  _ring_len = 0;
  _real_tokens = 0;
  _emit_size = 0;
  _base_exhausted = false;
  _front_emitted = false;
  _pending_inc = 1;
  _pending_fillers = 0;
  _has_pending = false;
  _trim_blob = !_store_tokens;
  _store.value = {};
  return _analyzer->reset(data);
}

void ShingleAnalyzer::DrainTokens(std::string_view data) {
  if (!reset(data)) {
    return;
  }
  _trim_blob = false;
  const auto append = [&](bytes_view token, bool is_filler) {
    if (_ring_len == _max) {
      PopFront();
    }
    AppendToken(token, is_filler);
  };
  while (_analyzer->next()) {
    const uint32_t inc = _base_inc ? _base_inc->value : 1;
    for (uint32_t i = 1; i < inc; ++i) {
      append(_filler, true);
    }
    append(_base_term->value, false);
  }
}

bool ShingleAnalyzer::next() {
  for (;;) {
    while (!_base_exhausted && _ring_len < _max) {
      if (_pending_fillers != 0) {
        AppendToken(_filler, true);
        --_pending_fillers;
      } else if (_has_pending) {
        AppendToken(_pending_token);
        _has_pending = false;
      } else if (_analyzer->next()) {
        const uint32_t inc = _base_inc ? _base_inc->value : 1;
        if (inc > 1) {
          _pending_fillers = inc - 1;
          _pending_token.assign(_base_term->value);
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
      const bool no_shingles = _base_exhausted && _real_tokens < _min;
      if ((_output_unigrams ||
           (_output_unigrams_if_no_shingles && no_shingles)) &&
          !WindowAt(0).filler) {
        _shingle_term.value = TokenAt(WindowAt(0));
        EmitPosition();
        return true;
      }
    }
    if (_emit_size >= _min && _emit_size <= _ring_len) {
      if (!WindowHasFiller(_emit_size) &&
          (_frequent.empty() || _emit_size == _min ||
           WindowHasFrequent(_emit_size))) {
        _shingle_term.value = BuildShingle(_emit_size);
        ++_emit_size;
        EmitPosition();
        return true;
      }
      ++_emit_size;
      continue;
    }
    if (!_front_emitted) {
      ++_pending_inc;
    }
    PopFront();
    _emit_size = 0;
    _front_emitted = false;
  }
}

}  // namespace irs::analysis
