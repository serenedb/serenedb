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
#include "iresearch/utils/minhash_utils.hpp"

namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

struct TokenizerConfig;

class MinHashTokenizer final : public TypedTokenizer<MinHashTokenizer>,
                               private TokenConsumer,
                               private util::Noncopyable {
 public:
  struct Options {
    using Owner = MinHashTokenizer;
    std::unique_ptr<TokenizerConfig> analyzer;
    uint32_t num_hashes{1};
  };
  static analysis::Tokenizer::ptr Make(Options opts,
                                       duckdb::SharedObjectCache& cache);

  static constexpr std::string_view type_name() noexcept { return "minhash"; }

  explicit MinHashTokenizer(analysis::Tokenizer::ptr analyzer,
                            uint32_t num_hashes);

  TokenTraits Traits() const noexcept final { return {.explicit_pos = true}; }
  std::tuple<> PrepareBatch(BlockTraits);

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

  void Bind(duckdb::ClientContext& ctx) final { _analyzer->Bind(ctx); }

  void Unbind() noexcept final { _analyzer->Unbind(); }
  size_t MemoryUsage() const noexcept final {
    return _analyzer->MemoryUsage() + (_sub_writer ? sizeof(TokenSink) : 0);
  }

  uint32_t num_hashes() const noexcept { return _num_hashes; }

 private:
  void Consume(TokenBatch& batch, DocRuns) final;

  template<TokenLayout Layout>
  void EmitSignature(TokenSink& sink);

  analysis::Tokenizer::ptr _analyzer;
  uint32_t _num_hashes{1};
  MinHash _minhash;
  std::unique_ptr<TokenSink> _sub_writer;
};

template<typename Context>
void SerdeWrite(Context ctx, const MinHashTokenizer::Options& o) {
  sdb::basics::WriteTupleOrObject(ctx, std::tie(o.analyzer, o.num_hashes));
}

template<typename Context>
void SerdeRead(Context ctx, MinHashTokenizer::Options& o) {
  auto refs = std::tie(o.analyzer, o.num_hashes);
  sdb::basics::ReadTupleOrObject(ctx, refs);
}

extern template class TypedTokenizer<MinHashTokenizer>;

}  // namespace irs::analysis
