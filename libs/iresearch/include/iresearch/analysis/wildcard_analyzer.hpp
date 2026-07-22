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

#pragma once

#include <tuple>
#include <vector>

#include "basics/serializer.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis {

struct TokenizerConfig;

class WildcardAnalyzer final : public TypedTokenizer<WildcardAnalyzer>,
                               private util::Noncopyable {
  using Ngram = NGramTokenizer<NGramTokenizerBase::InputType::UTF8>;

 public:
  struct Options {
    using Owner = WildcardAnalyzer;
    std::unique_ptr<TokenizerConfig> base_analyzer;
    size_t ngram_size = 3;
  };
  static Tokenizer::ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "wildcard"; }

  explicit WildcardAnalyzer(Tokenizer::ptr base_analyzer,
                            size_t ngram_size) noexcept;

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  TokenTraits Traits() const noexcept final {
    return {.offsets = false, .store = true};
  }

  auto& ngram() const noexcept {
    SDB_ASSERT(_ngram);
    return *_ngram;
  }

 private:
  byte_type* begin() noexcept {
    return reinterpret_cast<byte_type*>(_terms.data());
  }

  void Emit(TokenEmitter& sink);
  void AppendEncodedTerm(bytes_view term);
  void BuildTermBounds(bytes_view term);
  template<bool Identity>
  void EmitTermGrams(TokenEmitter& sink, bytes_view term,
                     const uint32_t* bounds, uint32_t nsym);

  Tokenizer::ptr _analyzer;
  std::unique_ptr<Ngram> _ngram;
  std::string _terms;

  uint32_t _ngram_size = 3;
  std::vector<uint32_t> _fill_bounds;
  std::unique_ptr<TokenWriter> _encode_writer;

  friend class EncodeConsumerAccess;
};

extern template class TypedTokenizer<WildcardAnalyzer>;

class EncodeConsumerAccess {
 protected:
  static void Append(WildcardAnalyzer& self, bytes_view term) {
    self.AppendEncodedTerm(term);
  }
};

template<typename Context>
void SerdeWrite(Context ctx, const WildcardAnalyzer::Options& o) {
  sdb::basics::WriteTuple(ctx.io(), std::tie(o.base_analyzer, o.ngram_size),
                          ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, WildcardAnalyzer::Options& o) {
  auto refs = std::tie(o.base_analyzer, o.ngram_size);
  sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
}

}  // namespace irs::analysis
