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

constexpr std::string_view FillPad(size_t len) noexcept {
  return {kFill.data() + kFill.size() - len, len};
}

}  // namespace

Tokenizer::ptr WildcardAnalyzer::Make(Options opts) {
  Tokenizer::ptr base;
  if (opts.base_analyzer) {
    base = CreateTokenizer(std::move(*opts.base_analyzer));
  }
  // If `base_analyzer` is absent the ctor falls back to StringTokenizer.
  return std::make_unique<WildcardAnalyzer>(std::move(base), opts.ngram_size);
}

void WildcardAnalyzer::AppendEncodedTerm(bytes_view term) {
  const auto size = term.size();
  if (size > std::numeric_limits<int32_t>::max()) {
    // icu doesn't support more
    SDB_WARN(IRESEARCH, "too long input for wildcard analyzer: ", size);
    return;
  }
  const auto idx = _terms.size();
  absl::StrAppend(
    &_terms,
    FillPad(bytes_io<uint32_t>::vsize(static_cast<uint32_t>(size)) + 1),
    ViewCast<char>(term), FillPad(1));
  auto* data = begin() + idx;
  WriteVarint<uint32_t>(static_cast<uint32_t>(size), data);
}

namespace {

class EncodeConsumer final : public TokenConsumer,
                             private EncodeConsumerAccess {
 public:
  explicit EncodeConsumer(WildcardAnalyzer& self) noexcept : _self{&self} {}

  void Consume(TokenBatch& batch, std::span<const DocRun>) final {
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      Append(*_self, bytes_view{reinterpret_cast<const byte_type*>(t.GetData()),
                                t.GetSize()});
    }
  }

 private:
  WildcardAnalyzer* _self;
};

}  // namespace

template<TokenLayout Layout>
bool WildcardAnalyzer::DoFill(std::string_view data, TokenEmitter& out) {
  _terms.clear();
  EncodeConsumer consumer{*this};
  if (!_encode_writer) {
    _encode_writer = std::make_unique<TokenWriter>(consumer);
  } else {
    _encode_writer->Bind(consumer);
  }
  if (!_analyzer->Fill(data, *_encode_writer, TokenLayout::Terms)) {
    return false;
  }
  _encode_writer->Finish();

  if (_terms.empty()) {
    return false;
  }
  Emit(out);
  out.Store(ViewCast<byte_type>(std::string_view{_terms}));
  return true;
}

void WildcardAnalyzer::BuildTermBounds(bytes_view term) {
  _fill_bounds.clear();
  const auto* p = term.data();
  const size_t size = term.size();
  if (simdutf::validate_utf8(reinterpret_cast<const char*>(p) + 1, size - 2)) {
    size_t offset = 0;
    while (size - offset >= 32) {
      uint32_t mask = 0;
      for (int b = 0; b < 32; ++b) {
        mask |= static_cast<uint32_t>((p[offset + b] & 0xC0) != 0x80) << b;
      }
      while (mask != 0) {
        _fill_bounds.push_back(
          static_cast<uint32_t>(offset + std::countr_zero(mask)));
        mask &= mask - 1;
      }
      offset += 32;
    }
    for (; offset < size; ++offset) {
      if ((p[offset] & 0xC0) != 0x80) {
        _fill_bounds.push_back(static_cast<uint32_t>(offset));
      }
    }
  } else {
    const auto* end = p + size;
    for (const auto* it = p; it != end; it = utf8_utils::Next(it, end)) {
      _fill_bounds.push_back(static_cast<uint32_t>(it - p));
    }
  }
  _fill_bounds.push_back(static_cast<uint32_t>(size));
}

template<bool Identity>
void WildcardAnalyzer::EmitTermGrams(TokenEmitter& sink, bytes_view term,
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

  uint32_t done = 0;
  while (done < count) {
    const auto slots = sink.Next(count - done);
    for (size_t j = 0; j < slots.size(); ++j) {
      const auto s = static_cast<uint32_t>(done + j);
      const uint32_t b0 = bnd(s);
      const uint32_t b1 = bnd(std::min(s + n, nsym));
      slots[j] = sink.Intern(bytes_view{p + b0, b1 - b0});
    }
    done += static_cast<uint32_t>(slots.size());
  }
}

void WildcardAnalyzer::Emit(TokenEmitter& sink) {
  const auto* it = reinterpret_cast<const byte_type*>(_terms.data());
  const auto* end = it + _terms.size();
  while (it != end) {
    const auto size = vread<uint32_t>(it) + 2U;
    const bytes_view term{it, size};
    it += size;
    if (simdutf::validate_ascii(reinterpret_cast<const char*>(term.data()) + 1,
                                term.size() - 2)) {
      EmitTermGrams<true>(sink, term, nullptr,
                          static_cast<uint32_t>(term.size()));
    } else {
      BuildTermBounds(term);
      EmitTermGrams<false>(sink, term, _fill_bounds.data(),
                           static_cast<uint32_t>(_fill_bounds.size() - 1));
    }
  }
}

WildcardAnalyzer::WildcardAnalyzer(Tokenizer::ptr base_analyzer,
                                   size_t ngram_size) noexcept
  : _analyzer{std::move(base_analyzer)},
    _ngram_size{static_cast<uint32_t>(std::max<size_t>(ngram_size, 1))} {
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
}

template class TypedTokenizer<WildcardAnalyzer>;

}  // namespace irs::analysis
