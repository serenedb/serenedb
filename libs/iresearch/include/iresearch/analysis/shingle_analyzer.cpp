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

#include <vpack/builder.h>

#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kMinSize = "minShingleSize";
constexpr std::string_view kMaxSize = "maxShingleSize";
constexpr std::string_view kOutputUnigrams = "outputUnigrams";
constexpr std::string_view kOutputUnigramsIfNoShingles =
  "outputUnigramsIfNoShingles";
constexpr std::string_view kParseError =
  ", failed to parse options for shingle analyzer";

constexpr uint32_t kMinShingle = 2;
constexpr uint32_t kMaxShingle = 16;  // bounds the sliding window

// Default token separator: a single 0xFF byte cannot appear inside a UTF-8
// token, so shingle terms are unambiguous for text columns.
constexpr byte_type kDefaultSeparator{0xFF};

bool ParseUInt(vpack::Slice obj, std::string_view key, uint32_t def,
               uint32_t& out) {
  auto slice = obj.get(key);
  if (slice.isNone()) {
    out = def;
    return true;
  }
  if (!slice.isNumber<uint32_t>()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, key, " attribute must be uint32",
              kParseError);
    return false;
  }
  out = slice.getNumber<uint32_t>();
  return true;
}

bool ParseBool(vpack::Slice obj, std::string_view key, bool def, bool& out) {
  auto slice = obj.get(key);
  if (slice.isNone()) {
    out = def;
    return true;
  }
  if (!slice.isBool()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, key, " attribute must be bool",
              kParseError);
    return false;
  }
  out = slice.getBool();
  return true;
}

bool ParseSizes(vpack::Slice slice, uint32_t& min, uint32_t& max) {
  SDB_ASSERT(slice.isObject());
  if (!ParseUInt(slice, kMinSize, kMinShingle, min) ||
      !ParseUInt(slice, kMaxSize, kMinShingle, max)) {
    return false;
  }
  if (min < kMinShingle) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, kMinSize, " must be at least ",
              kMinShingle, kParseError);
    return false;
  }
  if (max < min) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, kMaxSize, " must be >= ", kMinSize,
              kParseError);
    return false;
  }
  if (max > kMaxShingle) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, kMaxSize, " must be at most ",
              kMaxShingle, kParseError);
    return false;
  }
  return true;
}

bool ParseOptions(vpack::Slice slice, ShingleAnalyzer::Options& options) {
  if (!slice.isObject()) {
    return false;
  }
  if (!ParseSizes(slice, options.min_shingle_size, options.max_shingle_size)) {
    return false;
  }
  if (!ParseBool(slice, kOutputUnigrams, true, options.output_unigrams)) {
    return false;
  }
  if (!ParseBool(slice, kOutputUnigramsIfNoShingles, false,
                 options.output_unigrams_if_no_shingles)) {
    return false;
  }
  if (!analyzers::MakeAnalyzer(slice, options.base_analyzer)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Invalid analyzer definition in ",
              slice_to_string(slice), kParseError);
    return false;
  }
  return true;
}

Analyzer::ptr MakeImpl(vpack::Slice slice) {
  ShingleAnalyzer::Options opts;
  if (ParseOptions(slice, opts)) {
    return std::make_unique<ShingleAnalyzer>(std::move(opts));
  }
  return {};
}

bool NormalizeImpl(vpack::Slice input, vpack::Builder& output) {
  if (!input.isObject()) {
    return false;
  }
  uint32_t min = 0;
  uint32_t max = 0;
  if (!ParseSizes(input, min, max)) {
    return false;
  }
  bool output_unigrams = true;
  bool output_unigrams_if_no_shingles = false;
  if (!ParseBool(input, kOutputUnigrams, true, output_unigrams) ||
      !ParseBool(input, kOutputUnigramsIfNoShingles, false,
                 output_unigrams_if_no_shingles)) {
    return false;
  }
  vpack::ObjectBuilder scope{&output};
  output.add(kMinSize, min);
  output.add(kMaxSize, max);
  output.add(kOutputUnigrams, output_unigrams);
  output.add(kOutputUnigramsIfNoShingles, output_unigrams_if_no_shingles);
  if (!analyzers::NormalizeAnalyzer(input, output)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Invalid analyzer definition in ",
              slice_to_string(input), kParseError);
    return false;
  }
  return true;
}

}  // namespace

bool ShingleAnalyzer::normalize(std::string_view args, std::string& definition) {
  if (args.empty()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Empty arguments", kParseError);
    return false;
  }
  vpack::Slice input{reinterpret_cast<const uint8_t*>(args.data())};
  vpack::Builder output;
  if (!NormalizeImpl(input, output)) {
    return false;
  }
  definition.assign(output.slice().startAs<char>(), output.slice().byteSize());
  return true;
}

Analyzer::ptr ShingleAnalyzer::make(std::string_view args) {
  if (args.empty()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Empty arguments", kParseError);
    return {};
  }
  vpack::Slice slice{reinterpret_cast<const uint8_t*>(args.data())};
  return MakeImpl(slice);
}

void ShingleAnalyzer::init() {
  REGISTER_ANALYZER_VPACK(ShingleAnalyzer, ShingleAnalyzer::make,
                          ShingleAnalyzer::normalize);
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

ShingleAnalyzer::ShingleAnalyzer(Options&& options) noexcept
  : _analyzer{std::move(options.base_analyzer)},
    _min{options.min_shingle_size},
    _max{options.max_shingle_size},
    _output_unigrams{options.output_unigrams},
    _output_unigrams_if_no_shingles{options.output_unigrams_if_no_shingles},
    _separator{std::move(options.token_separator)} {
  if (!_analyzer) {
    _analyzer = std::make_unique<StringTokenizer>();
  }
  if (_separator.empty()) {
    _separator.push_back(kDefaultSeparator);
  }
  SDB_ASSERT(_min >= 1 && _max >= _min);
  _ring.resize(_max);
  _base_term = irs::get<TermAttr>(*_analyzer);
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
    return &_store;
  }
  return nullptr;
}

bytes_view ShingleAnalyzer::TokenAt(uint32_t offset) const noexcept {
  bytes_view token;
  ReadToken(reinterpret_cast<const byte_type*>(_terms.data()) + offset, token);
  return token;
}

void ShingleAnalyzer::AppendToken(bytes_view token) {
  if (token.size() > kMaxTokenSize) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "too long input for shingle analyzer: ", token.size());
    return;
  }
  const auto offset = static_cast<uint32_t>(_terms.size());
  WriteToken(token, _terms);
  _ring[(_ring_head + _ring_len) % _max] = offset;
  ++_ring_len;
  ++_emitted_total;
  _store.value = ViewCast<byte_type>(std::string_view{_terms});
}

void ShingleAnalyzer::PopFront() noexcept {
  SDB_ASSERT(_ring_len > 0);
  _ring_head = (_ring_head + 1) % _max;
  --_ring_len;
}

bytes_view ShingleAnalyzer::BuildShingle(uint32_t size) {
  _join.clear();
  for (uint32_t j = 0; j < size; ++j) {
    _join.push_back(TokenAt(_ring[(_ring_head + j) % _max]));
  }
  AppendShingle(_join, _separator, _scratch);
  return _scratch;
}

bool ShingleAnalyzer::reset(std::string_view data) {
  _terms.clear();
  _ring_head = 0;
  _ring_len = 0;
  _emitted_total = 0;
  _emit_size = 0;
  _base_exhausted = false;
  _store.value = {};
  return _analyzer->reset(data);
}

bool ShingleAnalyzer::next() {
  for (;;) {
    // Prime the window: a full window (max tokens) lets the front position
    // emit every shingle size; at end-of-stream a partial window emits the
    // sizes that still fit.
    while (!_base_exhausted && _ring_len < _max) {
      if (_analyzer->next()) {
        AppendToken(_base_term->value);
      } else {
        _base_exhausted = true;
      }
    }
    if (_ring_len == 0) {
      _store.value = ViewCast<byte_type>(std::string_view{_terms});
      return false;
    }
    _inc.value = 1;
    if (_emit_size == 0) {
      _emit_size = _min;
      const bool no_shingles = _base_exhausted && _emitted_total < _min;
      if (_output_unigrams || (_output_unigrams_if_no_shingles && no_shingles)) {
        _shingle_term.value = TokenAt(_ring[_ring_head]);
        return true;
      }
    }
    if (_emit_size >= _min && _emit_size <= _ring_len) {
      _shingle_term.value = BuildShingle(_emit_size);
      ++_emit_size;
      return true;
    }
    PopFront();
    _emit_size = 0;
  }
}

}  // namespace irs::analysis
