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

#include <cstring>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tests {

class EmptyTokenizer final
  : public irs::analysis::TypedTokenizer<EmptyTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "empty_tokenizer";
  }

  irs::TokenTraits Traits() const noexcept final { return {}; }

  template<irs::TokenLayout>
  bool DoFill(duckdb::string_t, irs::TokenSink&) noexcept {
    return false;
  }
};

struct AnalyzerToken {
  std::string term;
  uint32_t pos;
  uint32_t offs_start;
  uint32_t offs_end;

  bool operator==(const AnalyzerToken&) const = default;
};

inline std::optional<std::vector<AnalyzerToken>> Analyze(
  irs::analysis::Tokenizer& a, std::string_view value,
  std::optional<irs::TokenLayout> layout_opt = std::nullopt) {
  const auto layout =
    layout_opt.value_or(a.Traits().offsets ? irs::TokenLayout::TermsPosOffs
                                           : irs::TokenLayout::TermsPos);
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    return std::nullopt;
  }
  const duckdb::string_t handle =
    value.empty()
      ? duckdb::string_t{}
      : duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())};
  return irs::ResolveLayout(
    layout,
    [&]<irs::TokenLayout L>() -> std::optional<std::vector<AnalyzerToken>> {
      irs::ValueAnalyzer analyzer;
      auto tokens = [&] {
        if constexpr (L == irs::TokenLayout::Terms) {
          return irs::ValueTokens<L>{};
        } else {
          return irs::ValueTokens<L>{a.Traits()};
        }
      }();
      if (!analyzer.Analyze(a, handle, tokens)) {
        return std::nullopt;
      }
      const auto terms = tokens.terms();
      std::vector<AnalyzerToken> out;
      out.reserve(terms.size());
      for (size_t i = 0; i < terms.size(); ++i) {
        AnalyzerToken tok{std::string{terms[i].GetData(), terms[i].GetSize()},
                          0, 0, 0};
        if constexpr (L != irs::TokenLayout::Terms) {
          tok.pos = tokens.pos()[i];
        }
        if constexpr (L == irs::TokenLayout::TermsPosOffs) {
          if (!tokens.offs_start().empty()) {
            tok.offs_start = tokens.offs_start()[i];
            tok.offs_end = tokens.offs_end()[i];
          }
        }
        out.push_back(std::move(tok));
      }
      return out;
    });
}

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

inline void FillColumn(irs::analysis::Tokenizer& tokenizer,
                       std::span<const duckdb::string_t> values,
                       irs::doc_id_t first_doc, irs::TokenSink& sink,
                       irs::TokenLayout layout) {
  duckdb::UnifiedVectorFormat fmt;
  fmt.sel = duckdb::FlatVector::IncrementalSelectionVector();
  fmt.data = reinterpret_cast<duckdb::const_data_ptr_t>(values.data());
  fmt.physical_type = duckdb::PhysicalType::VARCHAR;
  tokenizer.Fill(fmt, static_cast<uint32_t>(values.size()), first_doc, sink,
                 {layout});
}

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

template<typename F>
class FnTokenSink final : public irs::TokenConsumer {
 public:
  FnTokenSink(irs::TokenLayout layout, F fn)
    : layout{layout}, _fn(std::move(fn)) {
    writer.Bind(*this, nullptr);
  }

  void Consume(irs::TokenBatch& batch, irs::DocRuns runs) final {
    _fn(batch, runs);
  }

  irs::TokenSink writer;
  irs::TokenLayout layout;

 private:
  F _fn;
};

}  // namespace tests
