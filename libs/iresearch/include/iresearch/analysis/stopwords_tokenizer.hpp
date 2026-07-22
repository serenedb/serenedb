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

#include <absl/container/flat_hash_set.h>

#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/first_len_filter.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

// An analyzer capable of masking the input, treated as a single token,
// if it is present in the configured list
class StopwordsTokenizer final : public TypedTokenizer<StopwordsTokenizer>,
                                 private util::Noncopyable {
 public:
  using stopwords_set = absl::flat_hash_set<std::string>;

  struct Options {
    using Owner = StopwordsTokenizer;
    stopwords_set mask;
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "stopwords"; }

  explicit StopwordsTokenizer(stopwords_set&& mask);

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  bool IsStopword(std::string_view value) const noexcept {
    if (!_prefilter.MayContain(value)) [[likely]] {
      return false;
    }
    return _stopwords.contains(value);
  }

 private:
  stopwords_set _stopwords;
  FirstLenFilter _prefilter;
};

extern template class TypedTokenizer<StopwordsTokenizer>;

}  // namespace irs::analysis
