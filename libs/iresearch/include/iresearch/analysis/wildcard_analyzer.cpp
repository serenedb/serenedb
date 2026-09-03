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

#include "basics/log.h"
#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::analysis {

Tokenizer::ptr WildcardAnalyzer::Make(Options opts,
                                      duckdb::SharedObjectCache& cache) {
  Tokenizer::ptr base;
  if (opts.base_analyzer) {
    base = CreateTokenizer(std::move(*opts.base_analyzer), cache);
  }
  return std::make_unique<WildcardAnalyzer>(std::move(base), opts.ngram_size);
}

namespace {

void AppendEncodedTerm(bstring& terms, duckdb::string_t term) {
  const size_t size = term.GetSize();
  if (size > std::numeric_limits<int32_t>::max()) {
    SDB_WARN(IRESEARCH, "too long input for wildcard analyzer: ", size);
    return;
  }
  const auto vlen = bytes_io<uint32_t>::vsize(static_cast<uint32_t>(size));
  const auto idx = terms.size();
  terms.resize_and_overwrite(
    idx + vlen + 1 + size + 1, [&](byte_type* p, size_t n) {
      auto* data = p + idx;
      WriteVarint<uint32_t>(static_cast<uint32_t>(size), data);
      *data++ = byte_type{0xFF};
      std::memcpy(data, term.GetData(), size);
      data[size] = byte_type{0xFF};
      return n;
    });
}

struct EncodeConsumer final : TokenConsumer {
  explicit EncodeConsumer(bstring& terms) noexcept : terms{&terms} {}

  void Consume(TokenBatch& batch, DocRuns) final {
    for (const auto& term : batch.Terms()) {
      AppendEncodedTerm(*terms, term);
      if (track_ascii) {
        ascii &= classify::IsAsciiValue(term.GetData(), term.GetSize());
      }
    }
  }

  bstring* terms;
  bool track_ascii = true;
  bool ascii = true;
};

}  // namespace

struct WildcardAnalyzer::SubSink {
  explicit SubSink(bstring& terms) : consumer{terms} {
    writer.Bind(consumer, nullptr);
  }

  EncodeConsumer consumer;
  TokenSink writer;
};

WildcardAnalyzer::~WildcardAnalyzer() = default;

std::tuple<bool> WildcardAnalyzer::PrepareBatch(BlockTraits traits) {
  if (!_sub_sink) {
    _sub_sink = std::make_unique<SubSink>(_terms);
  }
  return {traits.ascii};
}

template<TokenLayout Layout, bool KnownAscii>
bool WildcardAnalyzer::DoFill(duckdb::string_t raw, TokenSink& out) {
  _terms.clear();
  auto& consumer = _sub_sink->consumer;
  consumer.track_ascii = !(KnownAscii && _base_stable);
  consumer.ascii = true;
  if (!_analyzer->Fill(
        raw, _sub_sink->writer,
        {TokenLayout::Terms, BlockTraits{.ascii = KnownAscii}})) {
    _sub_sink->writer.Discard();
    return false;
  }
  _sub_sink->writer.Finish();

  if (_terms.empty()) {
    return true;
  }
  if (consumer.ascii) {
    EmitTerms<true, Layout>(out);
  } else {
    EmitTerms<false, Layout>(out);
  }
  out.Store(_terms);
  return true;
}

template<bool Identity, TokenLayout Layout>
void WildcardAnalyzer::EmitTermGrams(TokenSink& sink, const byte_type* term,
                                     uint32_t size) {
  const uint32_t* bounds = nullptr;
  uint32_t nsym = size;
  if constexpr (!Identity) {
    nsym = static_cast<uint32_t>(classify::BuildUtf8CpBounds(
      term, size,
      simdutf::validate_utf8(reinterpret_cast<const char*>(term) + 1, size - 2),
      _fill_bounds));
    bounds = _fill_bounds.data();
  }
  const auto n = static_cast<uint32_t>(_ngram.min_gram());
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
  sink.EmitK<Layout>(
    count, size,
    [&](byte_type* mem) IRS_FORCE_INLINE { std::memcpy(mem, term, size); },
    [&](size_t s, byte_type*) IRS_FORCE_INLINE {
      const auto b0 = bnd(static_cast<uint32_t>(s));
      const auto b1 = bnd(std::min(static_cast<uint32_t>(s) + n, nsym));
      return EmitKSlot{b0, b1};
    });
}

template<bool Identity, TokenLayout Layout>
void WildcardAnalyzer::EmitTerms(TokenSink& sink) {
  const auto* it = _terms.data();
  const auto* end = it + _terms.size();
  while (it != end) {
    const auto size = vread<uint32_t>(it) + 2U;
    const auto* term = it;
    it += size;
    EmitTermGrams<Identity, Layout>(sink, term, size);
  }
}

WildcardAnalyzer::WildcardAnalyzer(Tokenizer::ptr base_analyzer,
                                   size_t ngram_size)
  : _analyzer{std::move(base_analyzer)},
    _ngram{NGramTokenizerBase::Options{
      ngram_size,
      ngram_size,
      false,
      NGramTokenizerBase::InputType::UTF8,
      {},
      {},
    }} {
  if (!_analyzer) {
    _analyzer = std::make_unique<KeywordTokenizer>();
  }
  _base_stable = _analyzer->Traits().stable;
}

template class TypedTokenizer<WildcardAnalyzer>;

}  // namespace irs::analysis
