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

#include <iresearch/utils/attribute_provider.hpp>

#include "basics/down_cast.h"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kFill = "00000\xFF";

constexpr std::string_view Fill(size_t len) noexcept {
  return {kFill.data() + kFill.size() - len, len};
}

}  // namespace

Analyzer::ptr WildcardAnalyzer::Make(Options opts) {
  Analyzer::ptr base;
  if (opts.base_analyzer) {
    base = CreateAnalyzer(std::move(*opts.base_analyzer));
  }
  // If `base_analyzer` is absent the ctor falls back to StringTokenizer.
  return std::make_unique<WildcardAnalyzer>(std::move(base), opts.ngram_size);
}

Attribute* WildcardAnalyzer::GetMutable(TypeInfo::type_id type) noexcept {
  if (type == Type<OffsAttr>::id()) {
    return nullptr;
  }
  if (type == Type<StoreAttr>::id()) {
    return &_store;
  }
  return _ngram->GetMutable(type);
}

bool WildcardAnalyzer::reset(std::string_view data) {
  _ngram->reset({});
  if (!_analyzer->reset(data)) {
    return false;
  }
  _terms.clear();
  while (_analyzer->next()) {
    auto size = _term->value.size();
    if (size > std::numeric_limits<int32_t>::max()) {
      // icu doesn't support more
      SDB_WARN(IRESEARCH, "too long input for wildcard analyzer: ", size);
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
  _store.value = ViewCast<byte_type>(std::string_view{_terms});
  return _terms_begin != _terms_end;
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

WildcardAnalyzer::WildcardAnalyzer(Analyzer::ptr base_analyzer,
                                   size_t ngram_size) noexcept
  : _analyzer{std::move(base_analyzer)} {
  if (!_analyzer) {
    _analyzer = std::make_unique<StringTokenizer>();
  }
  auto ptr = Ngram::make({
    ngram_size,
    ngram_size,
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
