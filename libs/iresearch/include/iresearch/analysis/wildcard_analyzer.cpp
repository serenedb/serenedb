////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2024 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/analysis/wildcard_analyzer.hpp"

#include <vpack/builder.h>

#include <iresearch/utils/attribute_provider.hpp>

#include "basics/down_cast.h"
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kNgramSize = "ngramSize";
constexpr std::string_view kParseError =
  ", failed to parse options for wildcard analyzer";
constexpr size_t kMinNgram = 2;

bool ParseNgramSize(vpack::Slice input, size_t& ngram_size) {
  SDB_ASSERT(input.isObject());
  input = input.get(kNgramSize);
  if (!input.isNumber<size_t>()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, kNgramSize,
              " attribute must be size_t", kParseError);
    return false;
  }
  ngram_size = input.getNumber<size_t>();
  if (ngram_size < kMinNgram) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, kNgramSize,
              " attribute must be at least ", kMinNgram, kParseError);
    return false;
  }
  return true;
}

bool ParseOptions(vpack::Slice slice, WildcardAnalyzer::Options& options) {
  if (!slice.isObject()) {
    return false;
  }
  if (!ParseNgramSize(slice, options.ngram_size)) {
    return false;
  }
  if (!analyzers::MakeAnalyzer(slice, options.base_analyzer)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Invalid analyzer definition in ", slice_to_string(slice),
              kParseError);
    return false;
  }
  return true;
}

Analyzer::ptr MakeImpl(vpack::Slice slice) {
  WildcardAnalyzer::Options opts;
  if (ParseOptions(slice, opts)) {
    return std::make_unique<WildcardAnalyzer>(std::move(opts));
  }
  return {};
}

bool NormalizeImpl(vpack::Slice input, vpack::Builder& output) {
  if (!input.isObject()) {
    return false;
  }
  size_t ngram_size = 0;
  if (!ParseNgramSize(input, ngram_size)) {
    return false;
  }
  vpack::ObjectBuilder scope{&output};
  output.add(kNgramSize, ngram_size);
  if (!analyzers::NormalizeAnalyzer(input, output)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Invalid analyzer definition in ", slice_to_string(input),
              kParseError);
    return false;
  }
  return true;
}

constexpr std::string_view kFill = "00000\xFF";

constexpr std::string_view Fill(size_t len) noexcept {
  return {kFill.data() + kFill.size() - len, len};
}

}  // namespace

bool WildcardAnalyzer::normalize(std::string_view args,
                                 std::string& definition) {
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

Analyzer::ptr WildcardAnalyzer::make(std::string_view args) {
  if (args.empty()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Empty arguments", kParseError);
    return {};
  }
  vpack::Slice slice{reinterpret_cast<const uint8_t*>(args.data())};
  return MakeImpl(slice);
}

Attribute* WildcardAnalyzer::GetMutable(TypeInfo::type_id type) noexcept {
  if (type == Type<OffsAttr>::id()) {
    return nullptr;
  }
  return _ngram->GetMutable(type);
}

bool WildcardAnalyzer::reset(std::string_view data) {
  if (!_analyzer->reset(data)) {
    return false;
  }
  _terms.clear();
  while (_analyzer->next()) {
    auto size = _term->value.size();
    if (size > std::numeric_limits<int32_t>::max()) {
      // icu doesn't support more
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "too long input for wildcard analyzer: ", size);
      continue;
    }
    auto idx = _terms.size();
    absl::StrAppend(
      &_terms, Fill(bytes_io<uint32_t>::vsize(static_cast<uint32_t>(size)) + 1),
      ViewCast<char>(_term->value), Fill(1));
    auto* data = begin() + idx;
    WriteVarint<uint32_t>(static_cast<uint32_t>(size), data);
  }
  _terms_begin = begin();
  _terms_end = _terms_begin + _terms.size();
  return _terms_begin != _terms_end;
}

bytes_view WildcardAnalyzer::store(Tokenizer* ctx, vpack::Slice) {
  auto& impl = sdb::basics::downCast<WildcardAnalyzer>(*ctx);
  return ViewCast<byte_type>(std::string_view{impl._terms});
}

bool WildcardAnalyzer::next() {
  if (_ngram->next()) [[likely]] {
    return true;
  }
  if (auto size = _ngram_term->value.size(); size > 1) {
    auto* begin = _ngram_term->value.data();
    auto* end = begin + size;
    begin = utf8_utils::Next(begin, end);
    _ngram_term->value = {begin, end};
    if (_ngram_term->value.size() > 1) {
      return true;
    }
  }
  if (_terms_begin == _terms_end) {
    return false;
  }
  auto size = vread<uint32_t>(_terms_begin) + 2U;
  bytes_view term{_terms_begin, size};
  _ngram->reset(ViewCast<char>(term));
  _terms_begin += size;
  if (!_ngram->next()) {
    _ngram_term->value = term;
  }
  return true;
}

WildcardAnalyzer::WildcardAnalyzer(Options&& options) noexcept
  : _analyzer{std::move(options.base_analyzer)} {
  if (!_analyzer) {
    _analyzer = std::make_unique<StringTokenizer>();
  }
  auto ptr = Ngram::make({
    options.ngram_size,
    options.ngram_size,
    false,
    NGramTokenizerBase::InputType::UTF8,
    {},
    {},
  });
  _ngram = decltype(_ngram){sdb::basics::downCast<Ngram>(ptr.release())};
  SDB_ASSERT(_ngram);
  _term = irs::get<TermAttr>(*_analyzer);
  SDB_ASSERT(_term);
  _ngram_term = irs::GetMutable<TermAttr>(_ngram.get());
  SDB_ASSERT(_ngram_term);
}

}  // namespace irs::analysis
