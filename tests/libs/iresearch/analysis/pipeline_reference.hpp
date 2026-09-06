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

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/tokenizer.hpp"
#include "token_sink_utils.hpp"

namespace tests {

inline std::optional<std::vector<AnalyzerToken>> ChainReference(
  std::span<const irs::analysis::Tokenizer::ptr> children,
  std::string_view value, irs::TokenLayout layout) {
  SDB_ASSERT(!children.empty());
  auto toks = Analyze(*children.front(), value, layout);
  if (!toks) {
    return std::nullopt;
  }
  for (auto it = std::next(children.begin()); it != children.end(); ++it) {
    auto& child = **it;
    if (child.Traits().keyword) {
      continue;
    }
    std::vector<AnalyzerToken> next;
    uint32_t out = 0;
    uint32_t prev = 0;
    for (const auto& p : *toks) {
      const uint32_t inc_p = p.pos - prev;
      prev = p.pos;
      const auto ctoks = Analyze(child, p.term, layout);
      if (!ctoks) {
        continue;
      }
      uint32_t last = 0;
      bool first = true;
      for (const auto& c : *ctoks) {
        AnalyzerToken t{c.term, 0, 0, 0};
        if (layout != irs::TokenLayout::Terms) {
          uint32_t inc = c.pos - last;
          last = c.pos;
          if (first) {
            inc += inc_p;
            --inc;
            first = false;
          }
          out += inc;
          t.pos = out;
        }
        if (layout == irs::TokenLayout::TermsPosOffs) {
          t.offs_start = p.offs_start + c.offs_start;
          t.offs_end = c.offs_end == p.term.size() ? p.offs_end
                                                   : p.offs_start + c.offs_end;
        }
        next.push_back(std::move(t));
      }
    }
    toks = std::move(next);
  }
  return toks;
}

inline std::optional<std::vector<std::string>> ChainReferenceTerms(
  std::span<const irs::analysis::Tokenizer::ptr> children,
  std::string_view value) {
  auto toks = ChainReference(children, value, irs::TokenLayout::Terms);
  if (!toks) {
    return std::nullopt;
  }
  std::vector<std::string> out;
  out.reserve(toks->size());
  for (auto& t : *toks) {
    out.push_back(std::move(t.term));
  }
  return out;
}

}  // namespace tests
