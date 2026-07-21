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

#include <iresearch/analysis/batch/token_batch.hpp>
#include <iresearch/analysis/batch/token_sinks.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace tests {

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
  if (!irs::AnalyzeValue(a, value, collector)) {
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

// Test-side consumer running the given callable on every consume cycle; feed
// via `writer` and hand over the final partial batch with writer.Finish().
template<typename F>
class FnTokenSink final : public irs::TokenConsumer {
 public:
  FnTokenSink(irs::TokenLayout layout, F fn)
    : writer{*this}, layout{layout}, _fn(std::move(fn)) {}

  void Consume(irs::TokenBatch& batch,
               std::span<const irs::DocRun> runs) final {
    _fn(batch, runs);
  }

  irs::TokenWriter writer;
  irs::TokenLayout layout;

 private:
  F _fn;
};

// Sink for fills expected to stay within one batch: a consume cycle is a test
// failure surfaced by the caller checking flushed() == false. Inspect the
// staged tokens via writer.buf and the staged runs via writer.runs() WITHOUT
// calling writer.Finish().
class OneBatchSink final : public irs::TokenConsumer {
 public:
  explicit OneBatchSink(irs::TokenLayout layout)
    : writer{*this}, layout{layout} {}

  void Consume(irs::TokenBatch&, std::span<const irs::DocRun>) final {
    _flushed = true;
  }

  bool flushed() const noexcept { return _flushed; }

  irs::TokenWriter writer;
  irs::TokenLayout layout;

 private:
  bool _flushed = false;
};

}  // namespace tests
