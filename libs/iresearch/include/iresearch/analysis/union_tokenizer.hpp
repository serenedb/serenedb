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

#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
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
class UnionTokenizer final : public TypedTokenizer<UnionTokenizer>,
                             private util::Noncopyable {
 public:
  struct Options {
    using Owner = UnionTokenizer;
    std::vector<std::unique_ptr<TokenizerConfig>> children;
  };
  static Tokenizer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "union"; }

  explicit UnionTokenizer(std::vector<Tokenizer::ptr> children);

  TokenTraits Traits() const noexcept final {
    return {.dense_pos = false, .offsets = false};
  }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  using TypedTokenizer<UnionTokenizer>::Fill;

  void Fill(std::span<const duckdb::string_t> values,
            std::span<const doc_id_t> docs, TokenWriter& sink,
            TokenLayout layout) final;

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
    explicit SubAnalyzer(Tokenizer::ptr a);
    SubAnalyzer();

    const Tokenizer& GetStream() const noexcept {
      SDB_ASSERT(_analyzer);
      return *_analyzer;
    }

    Tokenizer& GetMutableStream() noexcept {
      SDB_ASSERT(_analyzer);
      return *_analyzer;
    }

   private:
    Tokenizer::ptr _analyzer;
  };

  void CollectSubs(std::string_view data);
  template<TokenLayout Layout, bool Copy>
  void EmitMerged(TokenEmitter& sink);

  struct FillScratchT;
  struct FillScratchDeleterT {
    void operator()(FillScratchT* p) const noexcept;
  };

  using SubAnalyzers = std::vector<SubAnalyzer>;

  SubAnalyzers _subs;
  std::unique_ptr<FillScratchT, FillScratchDeleterT> _scratch;
};

template<typename Context>
void SerdeWrite(Context ctx, const UnionTokenizer::Options& o) {
  sdb::basics::WriteTuple(ctx.io(), std::tie(o.children), ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, UnionTokenizer::Options& o) {
  auto refs = std::tie(o.children);
  sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
}

extern template class TypedTokenizer<UnionTokenizer>;

}  // namespace irs::analysis
