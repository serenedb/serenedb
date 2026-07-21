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
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/minhash_utils.hpp"

namespace irs::analysis {

struct TokenizerConfig;

class MinHashTokenizer final : public TypedTokenizer<MinHashTokenizer>,
                               private util::Noncopyable {
 public:
  struct Options {
    using Owner = MinHashTokenizer;
    std::unique_ptr<TokenizerConfig> analyzer;
    uint32_t num_hashes{1};
  };
  static analysis::Tokenizer::ptr Make(Options opts);

  // Return analyzer type name.
  static constexpr std::string_view type_name() noexcept { return "minhash"; }

  explicit MinHashTokenizer(analysis::Tokenizer::ptr analyzer,
                            uint32_t num_hashes);

  TokenTraits Traits() const noexcept final {
    return {.dense_pos = false, .offsets = false};
  }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  // Return number of MinHash hashes.
  uint32_t num_hashes() const noexcept { return _num_hashes; }

  // Return accumulated MinHash signature.
  const MinHash& signature() const noexcept { return _minhash; }

 private:
  template<TokenLayout Layout>
  void EmitSignature(TokenEmitter& sink);

  struct FillScratchT;
  struct FillScratchDeleterT {
    void operator()(FillScratchT* p) const noexcept;
  };

  analysis::Tokenizer::ptr _analyzer;
  uint32_t _num_hashes{1};
  MinHash _minhash;
  std::unique_ptr<FillScratchT, FillScratchDeleterT> _scratch;
};

template<typename Context>
void SerdeWrite(Context ctx, const MinHashTokenizer::Options& o) {
  sdb::basics::WriteTuple(ctx.io(), std::tie(o.analyzer, o.num_hashes),
                          ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, MinHashTokenizer::Options& o) {
  auto refs = std::tie(o.analyzer, o.num_hashes);
  sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
}

extern template class TypedTokenizer<MinHashTokenizer>;

}  // namespace irs::analysis
