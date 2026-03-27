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

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"

namespace irs::analysis {

class WildcardAnalyzer final : public TypedAnalyzer<WildcardAnalyzer>,
                               private util::Noncopyable {
  using Ngram = NGramTokenizer<NGramTokenizerBase::InputType::UTF8>;

 public:
  struct Options {
    Analyzer::ptr base_analyzer;
    size_t ngram_size = 3;
  };

  static constexpr std::string_view type_name() noexcept { return "wildcard"; }
  static bool normalize(std::string_view args, std::string& definition);
  static Analyzer::ptr make(std::string_view args);

  explicit WildcardAnalyzer(Options&& options) noexcept;

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  bool reset(std::string_view data) final;

  static bytes_view store(Tokenizer* ctx, vpack::Slice slice);

  bool next() final;

  auto& ngram() const noexcept {
    SDB_ASSERT(_ngram);
    return *_ngram;
  }

 private:
  byte_type* begin() noexcept {
    return reinterpret_cast<byte_type*>(_terms.data());
  }

  Analyzer::ptr _analyzer;
  std::unique_ptr<Ngram> _ngram;
  const TermAttr* _term{};
  TermAttr* _ngram_term{};
  std::string _terms;
  const byte_type* _terms_begin{};
  const byte_type* _terms_end{};
};

}  // namespace irs::analysis
