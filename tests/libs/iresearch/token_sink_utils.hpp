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

#pragma once

#include <cctype>
#include <cstring>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace tests {

class EmptyTokenizer final
  : public irs::analysis::TypedTokenizer<EmptyTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "empty_tokenizer";
  }

  template<irs::TokenLayout>
  bool DoFill(duckdb::string_t, irs::TokenSink&) noexcept {
    return false;
  }
};


// One analyzed token in batch convention: `pos` is the prefix sum of legacy
// increments within the value (first token with inc=1 has pos 1); offsets are
// value-relative, 0/0 when the analyzer carries none.
struct AnalyzerToken {
  std::string term;
  uint32_t pos;
  uint32_t offs_start;
  uint32_t offs_end;

  bool operator==(const AnalyzerToken&) const = default;
};

// Analyzes one value through the push API. nullopt = value rejected. The
// default layout requests everything the analyzer produces, like a driver
// whose field features were validated against the analyzer's traits.
inline std::optional<std::vector<AnalyzerToken>> Analyze(
  irs::analysis::Tokenizer& a, std::string_view value,
  std::optional<irs::TokenLayout> layout_opt = std::nullopt) {
  const auto layout =
    layout_opt.value_or(a.Traits().offsets ? irs::TokenLayout::TermsPosOffs
                                           : irs::TokenLayout::TermsPos);
  irs::TokenCollector collector{layout};
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    return std::nullopt;
  }
  const duckdb::string_t handle =
    value.empty()
      ? duckdb::string_t{}
      : duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())};
  if (!irs::AnalyzeValue(a, handle, collector)) {
    return std::nullopt;
  }
  std::vector<AnalyzerToken> out;
  out.reserve(collector.tokens.size());
  for (auto& t : collector.tokens) {
    out.push_back(
      {std::string{reinterpret_cast<const char*>(t.term.data()), t.term.size()},
       t.pos, t.offs_start, t.offs_end});
  }
  return out;
}

// Terms only, for assertions that don't care about pos/offs.
inline std::optional<std::vector<std::string>> AnalyzeTerms(
  irs::analysis::Tokenizer& a, std::string_view value) {
  auto tokens = Analyze(a, value, irs::TokenLayout::Terms);
  if (!tokens) {
    return std::nullopt;
  }
  std::vector<std::string> out;
  out.reserve(tokens->size());
  for (auto& t : *tokens) {
    out.push_back(std::move(t.term));
  }
  return out;
}

inline duckdb::string_t ToStringT(std::string_view v) noexcept {
  return {v.data(), static_cast<uint32_t>(v.size())};
}

// Copy-emit for stub kernels holding bytes in test-local storage.
template<irs::TokenLayout L, typename... Tags>
void EmitCopy(irs::TokenSink& sink, irs::bytes_view term, Tags... tags) {
  sink.Emit<L>(
    term.size(),
    [&](irs::byte_type* mem) {
      std::memcpy(mem, term.data(), term.size());
      return static_cast<uint32_t>(term.size());
    },
    tags...);
}

// Test-side consumer running the given callable on every consume cycle; feed
// via `writer` and hand over the final partial batch with writer.Finish().
template<typename F>
class FnTokenSink final : public irs::TokenConsumer {
 public:
  FnTokenSink(irs::TokenLayout layout, F fn)
    : layout{layout}, _fn(std::move(fn)) {
    writer.Bind(*this, this);
  }

  void Consume(irs::TokenBatch& batch,
               irs::DocRuns runs) final {
    _fn(batch, runs);
  }

  irs::TokenSink writer;
  irs::TokenLayout layout;

 private:
  F _fn;
};

// Ascii-vs-unicode differential for analyzers with an ascii fast tier: the
// unicode path is input-selected by appending a non-ascii sentinel word, and
// its output must be the ascii-tier output plus the sentinel's own tokens.
inline void AssertAsciiMatchesUnicode(irs::analysis::Tokenizer& stream,
                                      std::string_view value) {
  const auto fast = Analyze(stream, value);
  ASSERT_TRUE(fast.has_value());
  std::string unicode_value{value};
  if (!unicode_value.empty() &&
      std::isgraph(static_cast<unsigned char>(unicode_value.back()))) {
    unicode_value += ' ';
  }
  unicode_value += "\xCF\x89\xCF\x89\xCF\x89";
  const auto slow = Analyze(stream, unicode_value);
  ASSERT_TRUE(slow.has_value());
  ASSERT_GT(slow->size(), fast->size());
  for (size_t i = 0; i < fast->size(); ++i) {
    SCOPED_TRACE(testing::Message() << "token=" << i);
    ASSERT_EQ((*slow)[i].term, (*fast)[i].term);
    ASSERT_EQ((*slow)[i].pos, (*fast)[i].pos);
    ASSERT_EQ((*slow)[i].offs_start, (*fast)[i].offs_start);
    ASSERT_EQ((*slow)[i].offs_end, (*fast)[i].offs_end);
  }
}

}  // namespace tests
