////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "analyzer.h"

#include <vpack/builder.h>

#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <iresearch/utils/vpack_utils.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/logger/logger.h"
#include "basics/utf8_utils.hpp"

namespace sdb::search::wildcard {
namespace {

constexpr std::string_view kNGramSize = "ngramSize";
constexpr std::string_view kParseError =
  ", failed to parse options for wildcard analyzer";
constexpr size_t kMinNGram = 2;

bool ParseNGramSize(vpack::Slice input, size_t& ngram_size) {
  SDB_ASSERT(input.isObject());
  input = input.get(kNGramSize);
  if (!input.isNumber<size_t>()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, kNGramSize,
              " attribute must be size_t", kParseError);
    return false;
  }
  ngram_size = input.getNumber<size_t>();
  if (ngram_size < kMinNGram) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, kNGramSize,
              " attribute must be at least ", kMinNGram, kParseError);
    return false;
  }
  return true;
}

bool ParseOptions(vpack::Slice slice, Analyzer::Options& options) {
  if (!slice.isObject()) {
    return false;
  }
  if (!ParseNGramSize(slice, options.ngram_size)) {
    return false;
  }
  if (!irs::analysis::analyzers::MakeAnalyzer(slice, options.base_analyzer)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Invalid analyzer definition in ", irs::slice_to_string(slice),
              kParseError);
    return false;
  }
  return true;
}

irs::analysis::Analyzer::ptr MakeImpl(vpack::Slice slice) {
  if (Analyzer::Options opts; ParseOptions(slice, opts)) {
    return std::make_unique<Analyzer>(std::move(opts));
  }
  return {};
}

bool NormalizeImpl(vpack::Slice input, vpack::Builder& output) {
  if (!input.isObject()) {
    return false;
  }
  size_t ngram_size = 0;
  if (!ParseNGramSize(input, ngram_size)) {
    return false;
  }
  vpack::ObjectBuilder scope{&output};
  output.add(kNGramSize, ngram_size);
  if (!irs::analysis::analyzers::NormalizeAnalyzer(input, output)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Invalid analyzer definition in ", irs::slice_to_string(input),
              kParseError);
    return false;
  }
  return true;
}

static constexpr std::string_view kFill = "00000\xFF";

constexpr std::string_view Fill(size_t len) noexcept {
  return {kFill.data() + kFill.size() - len, len};
}

}  // namespace

bool Analyzer::normalize(std::string_view args, std::string& definition) {
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

irs::analysis::Analyzer::ptr Analyzer::make(std::string_view args) {
  if (args.empty()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Empty arguments", kParseError);
    return {};
  }
  vpack::Slice slice{reinterpret_cast<const uint8_t*>(args.data())};
  return MakeImpl(slice);
}

irs::Attribute* Analyzer::GetMutable(irs::TypeInfo::type_id type) noexcept {
  if (type == irs::Type<irs::OffsAttr>::id()) {
    return nullptr;
  }
  return _ngram->GetMutable(type);
}

bool Analyzer::reset(std::string_view data) {
  if (!_analyzer->reset(data)) {
    return false;
  }
  _terms.clear();
  while (_analyzer->next()) {
    const auto size = _term->value.size();
    if (size > std::numeric_limits<int32_t>::max()) {
      // TODO(mbkkt) icu doesn't support more
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "too long input for wildcard analyzer: ", size);
      continue;
    }
    // TODO(mbkkt) We can add offsets here
    const auto idx = _terms.size();
    absl::StrAppend(
      &_terms,
      Fill(irs::bytes_io<uint32_t>::vsize(static_cast<uint32_t>(size)) + 1),
      irs::ViewCast<char>(_term->value), Fill(1));
    auto* data = begin() + idx;
    irs::WriteVarint(static_cast<uint32_t>(size), data);
  }
  _terms_begin = begin();
  _terms_end = _terms_begin + _terms.size();
  return _terms_begin != _terms_end;
}

irs::bytes_view Analyzer::store(irs::Tokenizer* ctx, vpack::Slice slice) {
  auto& impl = basics::downCast<Analyzer>(*ctx);
  return irs::ViewCast<irs::byte_type>(std::string_view{impl._terms});
}

bool Analyzer::next() {
  if (_ngram->next()) [[likely]] {
    return true;
  }
  if (auto size = _ngram_term->value.size(); size > 1) {
    auto* begin = _ngram_term->value.data();
    auto* end = begin + size;
    begin = irs::utf8_utils::Next(begin, end);
    _ngram_term->value = {begin, end};
    if (_ngram_term->value.size() > 1) {
      return true;
    }
  }
  if (_terms_begin == _terms_end) {
    return false;
  }
  const auto size = irs::vread<uint32_t>(_terms_begin) + 2U;
  irs::bytes_view term{_terms_begin, size};
  _ngram->reset(irs::ViewCast<char>(term));
  _terms_begin += size;
  if (!_ngram->next()) {
    _ngram_term->value = term;
  }
  return true;
}

Analyzer::Analyzer(Options&& options) noexcept
  : _analyzer{std::move(options.base_analyzer)} {
  if (!_analyzer) {
    // Fallback to default implementation
    _analyzer = std::make_unique<irs::StringTokenizer>();
  }
  auto ptr = NGram::make({
    options.ngram_size,
    options.ngram_size,
    false,
    NGram::InputType::UTF8,
    {},
    {},
  });
  _ngram = decltype(_ngram){basics::downCast<NGram>(ptr.release())};
  SDB_ASSERT(_ngram);
  _term = irs::get<irs::TermAttr>(*_analyzer);
  SDB_ASSERT(_term);
  _ngram_term = irs::GetMutable<irs::TermAttr>(_ngram.get());
  SDB_ASSERT(_ngram_term);
}

}  // namespace sdb::search::wildcard
