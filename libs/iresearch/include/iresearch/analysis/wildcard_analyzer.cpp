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

#include <simdutf.h>

#include "basics/down_cast.h"
#include "iresearch/analysis/classify.hpp"
#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kFill = "00000\xFF";

[[maybe_unused]] constexpr std::string_view FillPad(size_t len) noexcept {
  return {kFill.data() + kFill.size() - len, len};
}

}  // namespace

Tokenizer::ptr WildcardAnalyzer::Make(Options opts) {
  Tokenizer::ptr base;
  if (opts.base_analyzer) {
    base = CreateTokenizer(std::move(*opts.base_analyzer));
  }
  // If `base_analyzer` is absent the ctor falls back to KeywordTokenizer.
  return std::make_unique<WildcardAnalyzer>(std::move(base), opts.ngram_size);
}

namespace {

void AppendEncodedTerm(bstring& terms, duckdb::string_t term) {
  const size_t size = term.GetSize();
  if (size > std::numeric_limits<int32_t>::max()) {
    // icu doesn't support more
    SDB_WARN(IRESEARCH, "too long input for wildcard analyzer: ", size);
    return;
  }
  const auto vlen = bytes_io<uint32_t>::vsize(static_cast<uint32_t>(size));
  const auto idx = terms.size();
  terms.resize(idx + vlen + 1 + size + 1, byte_type{0xFF});
  auto* data = terms.data() + idx;
  WriteVarint<uint32_t>(static_cast<uint32_t>(size), data);
  std::memcpy(terms.data() + idx + vlen + 1, term.GetData(), size);
}

class EncodeConsumer final : public TokenConsumer {
 public:
  explicit EncodeConsumer(bstring& terms) noexcept : _terms{&terms} {}

  void Consume(TokenBatch& batch, DocRuns) final {
    for (const auto& term : batch.Terms()) {
      AppendEncodedTerm(*_terms, term);
    }
  }

 private:
  bstring* _terms;
};

}  // namespace

template<TokenLayout Layout>
bool WildcardAnalyzer::DoFill(duckdb::string_t raw, TokenSink& out) {
  _terms.clear();
  EncodeConsumer consumer{_terms};
  if (!_encode_writer) {
    _encode_writer = std::make_unique<TokenSink>();
  }
  _encode_writer->Bind(consumer, nullptr);
  if (!_analyzer->Fill(raw, *_encode_writer, TokenLayout::Terms)) {
    return false;
  }
  _encode_writer->Finish();

  if (_terms.empty()) {
    return false;
  }
  Emit<Layout>(out);
  out.Store(_terms);
  return true;
}

void WildcardAnalyzer::BuildTermBounds(bytes_view term) {
  const auto* p = term.data();
  const size_t size = term.size();
  BuildUtf8CpBounds(
    p, size,
    simdutf::validate_utf8(reinterpret_cast<const char*>(p) + 1, size - 2),
    _fill_bounds);
}

template<bool Identity, TokenLayout Layout>
void WildcardAnalyzer::EmitTermGrams(TokenSink& sink, bytes_view term,
                                     const uint32_t* bounds, uint32_t nsym) {
  const auto* p = term.data();
  const auto size = static_cast<uint32_t>(term.size());
  const uint32_t n = _ngram_size;
  const auto bnd = [&](uint32_t i) -> uint32_t {
    if constexpr (Identity) {
      return i;
    } else {
      return bounds[i];
    }
  };

  uint32_t count = nsym >= n ? nsym - n + 1 : 0;
  while (count < nsym && bnd(count) < size - 1) {
    ++count;
  }
  // grams are overlapping windows of one term: the sink stages the term once
  // per slot-guaranteed wave and the gen emits window views into it
  sink.EmitK<Layout>(
    count, size,
    [&](byte_type* mem, size_t) IRS_FORCE_INLINE { std::memcpy(mem, p, size); },
    [&](size_t s, byte_type*) IRS_FORCE_INLINE {
      const auto b0 = bnd(static_cast<uint32_t>(s));
      const auto b1 = bnd(std::min(static_cast<uint32_t>(s) + n, nsym));
      return EmitKSlot{b0, b1};
    });
}

template<TokenLayout Layout>
void WildcardAnalyzer::Emit(TokenSink& sink) {
  const auto* it = _terms.data();
  const auto* end = it + _terms.size();
  while (it != end) {
    const auto size = vread<uint32_t>(it) + 2U;
    const bytes_view term{it, size};
    it += size;
    const auto* body = reinterpret_cast<const char*>(term.data()) + 1;
    const size_t body_size = term.size() - 2;
    if (body_size <= 16 ? IsAsciiShort(body, body_size)
                        : simdutf::validate_ascii(body, body_size)) {
      EmitTermGrams<true, Layout>(sink, term, nullptr,
                                  static_cast<uint32_t>(term.size()));
    } else {
      BuildTermBounds(term);
      EmitTermGrams<false, Layout>(
        sink, term, _fill_bounds.data(),
        static_cast<uint32_t>(_fill_bounds.size() - 1));
    }
  }
}

WildcardAnalyzer::WildcardAnalyzer(Tokenizer::ptr base_analyzer,
                                   size_t ngram_size) noexcept
  : _analyzer{std::move(base_analyzer)},
    _ngram_size{static_cast<uint32_t>(std::max<size_t>(ngram_size, 1))} {
  if (!_analyzer) {
    _analyzer = std::make_unique<KeywordTokenizer>();
  }
  _ngram = std::make_unique<Ngram>(NGramTokenizerBase::Options{
    ngram_size,
    ngram_size,
    false,
    NGramTokenizerBase::InputType::UTF8,
    {},
    {},
  });
  SDB_ASSERT(_ngram);
}

template class TypedTokenizer<WildcardAnalyzer>;

}  // namespace irs::analysis
