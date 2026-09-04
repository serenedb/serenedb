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

#include "stopwords_tokenizer.hpp"

#include "iresearch/analysis/text/dict/stopwords_loader.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

StopwordsTokenizer::StopwordsTokenizer(
  duckdb::shared_ptr<const StopwordSet> stopwords) noexcept
  : _stopwords{std::move(stopwords)} {
  SDB_ASSERT(_stopwords);
}

Tokenizer::ptr StopwordsTokenizer::Make(Options opts,
                                        duckdb::SharedObjectCache& cache) {
  auto stopwords = duckdb::make_uniq<StopwordSet>(std::move(opts.mask));
  if (!opts.stopwords_path.empty() &&
      !dict::LoadStopwords(*stopwords, {}, opts.stopwords_path)) {
    THROW_SQL_ERROR(ERR_MSG("stopwords: failed to load stopwords"));
  }
  stopwords->ShrinkToFit();

  return std::make_unique<StopwordsTokenizer>(
    StopwordSet::GetOrBuild(cache, std::move(stopwords)));
}

template<TokenLayout Layout, typename Sink>
bool StopwordsTokenizer::DoFill(const duckdb::string_t& raw, Sink& sink) {
  if (!_stopwords->Contains(raw)) {
    sink.template Emit<Layout>(raw);
  }
  return true;
}

template class TypedTokenizer<StopwordsTokenizer>;
template class TypedTokenStage<StopwordsTokenizer>;

}  // namespace irs::analysis
