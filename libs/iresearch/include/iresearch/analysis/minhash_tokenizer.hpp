////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <tuple>

#include "basics/noncopyable.hpp"
#include "basics/serializer.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/minhash_utils.hpp"

namespace irs::analysis {

struct TokenizerConfig;

class MinHashTokenizer final : public TypedAnalyzer<MinHashTokenizer>,
                               private util::Noncopyable {
 public:
  struct Options {
    using Owner = MinHashTokenizer;
    std::unique_ptr<TokenizerConfig> analyzer;
    uint32_t num_hashes{1};
  };
  static analysis::Analyzer::ptr Make(Options opts);

  // Return analyzer type name.
  static constexpr std::string_view type_name() noexcept { return "minhash"; }

  explicit MinHashTokenizer(analysis::Analyzer::ptr analyzer,
                            uint32_t num_hashes);

  // Advance stream to the next token.
  bool next() final;

  // Reset stream to a specified value.
  bool reset(std::string_view data) final;

  // Return a stream attribute denoted by `id`.
  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  // Return number of MinHash hashes.
  uint32_t num_hashes() const noexcept { return _num_hashes; }

  // Return accumulated MinHash signature.
  const MinHash& signature() const noexcept { return _minhash; }

 private:
  using attributes = std::tuple<TermAttr, IncAttr, OffsAttr>;
  using iterator = std::vector<uint64_t>::const_iterator;

  void ComputeSignature();

  analysis::Analyzer::ptr _analyzer;
  uint32_t _num_hashes{1};
  MinHash _minhash;
  attributes _attrs;
  IncAttr _next_inc;
  const TermAttr* _term{};
  const OffsAttr* _offset{};
  iterator _begin{};
  iterator _end{};
  std::array<char, 11> _buf{};
};

template<typename Context>
void SerdeWrite(Context ctx, const MinHashTokenizer::Options& o) {
  if constexpr (std::is_same_v<typename Context::Format,
                               sdb::basics::ObjectFormat>) {
    sdb::basics::WriteObject(ctx.io(), std::tie(o.analyzer, o.num_hashes),
                             ctx.arg());
  } else {
    sdb::basics::WriteTuple(ctx.io(), std::tie(o.analyzer, o.num_hashes),
                            ctx.arg());
  }
}

template<typename Context>
void SerdeRead(Context ctx, MinHashTokenizer::Options& o) {
  auto refs = std::tie(o.analyzer, o.num_hashes);
  if constexpr (std::is_same_v<typename Context::Format,
                               sdb::basics::ObjectFormat>) {
    sdb::basics::ReadObject(ctx.io(), refs, ctx.arg());
  } else {
    sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
  }
}

}  // namespace irs::analysis
