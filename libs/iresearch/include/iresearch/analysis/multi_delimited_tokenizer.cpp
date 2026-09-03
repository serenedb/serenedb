////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "multi_delimited_tokenizer.hpp"

#include <algorithm>
#include <string_view>

#include "iresearch/analysis/text/delim/split.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

template<typename Finder>
class MultiDelimitedTokenizerImpl final
  : public TypedTokenizer<MultiDelimitedTokenizerImpl<Finder>>,
    public MultiDelimitedTokenizer {
 public:
  template<typename... Args>
  explicit MultiDelimitedTokenizerImpl(Args&&... args)
    : _finder{std::forward<Args>(args)...} {}

  TokenTraits Traits() const noexcept final {
    return {
      .offsets = true,
      .stable = true,
    };
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    delim::SplitValue<Layout>(sink, raw, _finder);
    return true;
  }

 private:
  [[no_unique_address]] Finder _finder;
};

}  // namespace irs::analysis
namespace irs {

template<typename Finder>
struct Type<analysis::MultiDelimitedTokenizerImpl<Finder>>
  : Type<analysis::MultiDelimitedTokenizer> {};

}  // namespace irs
namespace irs::analysis {
namespace {

Tokenizer::ptr MakeImpl(std::vector<bstring>&& delimiters) {
  const bool single_character_case = absl::c_all_of(
    delimiters, [](const auto& delim) { return delim.size() == 1; });
  if (single_character_case) {
    switch (delimiters.size()) {
      case 0:
        return std::make_unique<
          MultiDelimitedTokenizerImpl<delim::NoDelimFinder>>();
      case 1:
        return std::make_unique<
          MultiDelimitedTokenizerImpl<delim::OneCharFinder>>(delimiters[0][0]);
      default: {
        delim::ManyCharsFinder finder;
        for (const auto& d : delimiters) {
          SDB_ASSERT(d.size() == 1);
          finder.Add(d[0]);
        }
        return std::make_unique<
          MultiDelimitedTokenizerImpl<delim::ManyCharsFinder>>(
          std::move(finder));
      }
    }
  }
  if (delimiters.size() == 1) {
    if (delimiters[0].size() > delim::kHorspoolNeedleThreshold) {
      return std::make_unique<
        MultiDelimitedTokenizerImpl<delim::OneLongStringFinder>>(
        std::move(delimiters[0]));
    }
    return std::make_unique<
      MultiDelimitedTokenizerImpl<delim::OneStringFinder>>(
      std::move(delimiters[0]));
  }
  return std::make_unique<
    MultiDelimitedTokenizerImpl<delim::MultiStringFinder>>(
    std::move(delimiters));
}

}  // namespace

Tokenizer::ptr MultiDelimitedTokenizer::Make(
  MultiDelimitedTokenizer::Options opts) {
  for (size_t i = 0; i < opts.delimiters.size(); ++i) {
    const bytes_view view{opts.delimiters[i]};
    if (view.empty()) {
      THROW_SQL_ERROR(ERR_MSG("multi_delimited: empty delimiter"));
    }
    for (size_t j = 0; j < i; ++j) {
      const bytes_view known{opts.delimiters[j]};
      if (view.starts_with(known) || known.starts_with(view)) {
        THROW_SQL_ERROR(
          ERR_MSG("multi_delimited: delimiters must not be prefixes of one "
                  "another"));
      }
    }
  }
  return MakeImpl(std::move(opts.delimiters));
}

}  // namespace irs::analysis
