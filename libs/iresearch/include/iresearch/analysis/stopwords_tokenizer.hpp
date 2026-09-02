////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include <vector>

#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/analysis/stopword_set.hpp"
#include "tokenizer.hpp"

namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

// An analyzer capable of masking the input, treated as a single token,
// if it is present in the configured list
class StopwordsTokenizer final : public TypedTokenizer<StopwordsTokenizer>,
                                 public TypedTokenStage<StopwordsTokenizer>,
                                 private util::Noncopyable {
 public:
  struct Options {
    using Owner = StopwordsTokenizer;
    std::vector<std::string> mask;
    std::string stopwords_path;
  };
  static ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  static constexpr std::string_view type_name() noexcept { return "stopwords"; }

  explicit StopwordsTokenizer(
    duckdb::shared_ptr<const StopwordSet> stopwords) noexcept;
  TokenTraits Traits() const noexcept final {
    return {.unique = true, .offsets = true, .stable = true};
  }

  template<TokenLayout Layout, typename Sink>
  IRS_FORCE_INLINE bool DoFill(const duckdb::string_t& value, Sink& sink);

 private:
  duckdb::shared_ptr<const StopwordSet> _stopwords;
};

extern template class TypedTokenizer<StopwordsTokenizer>;
extern template class TypedTokenStage<StopwordsTokenizer>;

}  // namespace irs::analysis
