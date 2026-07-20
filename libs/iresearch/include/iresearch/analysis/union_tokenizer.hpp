////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <tuple>

#include "basics/serializer.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

struct TokenizerConfig;

// Runs multiple sub-tokenizers independently on the same input and interleaves
// their token streams by position. Tokens from different sub-tokenizers at the
// same logical position overlap (inc=0). This enables indexing a single field
// with multiple analysis strategies simultaneously.
//
// Position alignment is by each child tokenizer's own position counter.
// Cross-tokenizer positional relationships are intentionally approximate for
// heterogeneous analyzers (e.g. text + ngram).
//
// OffsAttr is not exposed because interleaving tokens from independent
// tokenizers over the same input violates the monotonic offset invariant
// required by the indexer.
class UnionTokenizer final : public TypedAnalyzer<UnionTokenizer>,
                             private util::Noncopyable {
 public:
  struct Options {
    using Owner = UnionTokenizer;
    std::vector<std::unique_ptr<TokenizerConfig>> children;
  };
  static Analyzer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "union"; }

  explicit UnionTokenizer(std::vector<Analyzer::ptr> children);

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    // Only return union-owned attributes. Do not delegate to sub-analyzers:
    // the "active" sub changes on each next() call, so any delegated pointer
    // would be unstable.
    return irs::GetMutable(_attrs, id);
  }

  bool next() final;
  bool reset(std::string_view data) final;

  /// @brief calls visitor on union members in respective order.
  /// Visiting is interrupted on first visitor returning false.
  /// @return true if all visits returned true, false otherwise
  template<typename Visitor>
  bool VisitMembers(Visitor&& visitor) const {
    for (const auto& sub : _subs) {
      const auto& stream = sub.GetStream();
      if (stream.type() == type()) {
        // union inside union - forward visiting
        const auto& sub_union = sdb::basics::downCast<UnionTokenizer>(stream);
        if (!sub_union.VisitMembers(visitor)) {
          return false;
        }
      } else if (!visitor(sub.GetStream())) {
        return false;
      }
    }
    return true;
  }

 private:
  struct SubAnalyzer {
    explicit SubAnalyzer(Analyzer::ptr a);
    SubAnalyzer();

    bool DoReset(std::string_view data);
    bool Advance();

    const Analyzer& GetStream() const noexcept {
      SDB_ASSERT(_analyzer);
      return *_analyzer;
    }

    const TermAttr* term{nullptr};
    const IncAttr* inc{nullptr};
    const PayAttr* pay{nullptr};  // nullptr if sub has no payload
    bool has_token{false};
    uint32_t position{0};

   private:
    Analyzer::ptr _analyzer;
  };

  uint32_t FindMinPosition() const noexcept;

  using SubAnalyzers = std::vector<SubAnalyzer>;
  using Attributes = std::tuple<IncAttr, TermAttr, AttributePtr<PayAttr>>;

  SubAnalyzers _subs;
  size_t _emit_index = 0;
  uint32_t _current_min_pos = std::numeric_limits<uint32_t>::max();
  uint32_t _last_emitted_pos = 0;
  PayAttr _payload;
  bstring _term_buf;
  bstring _payload_buf;
  Attributes _attrs;
};

template<typename Context>
void SerdeWrite(Context ctx, const UnionTokenizer::Options& o) {
  if constexpr (std::is_same_v<typename Context::Format,
                               sdb::basics::ObjectFormat>) {
    sdb::basics::WriteObject(ctx.io(), std::tie(o.children), ctx.arg());
  } else {
    sdb::basics::WriteTuple(ctx.io(), std::tie(o.children), ctx.arg());
  }
}

template<typename Context>
void SerdeRead(Context ctx, UnionTokenizer::Options& o) {
  auto refs = std::tie(o.children);
  if constexpr (std::is_same_v<typename Context::Format,
                               sdb::basics::ObjectFormat>) {
    sdb::basics::ReadObject(ctx.io(), refs, ctx.arg());
  } else {
    sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
  }
}

}  // namespace irs::analysis
