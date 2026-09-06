////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Alex Geenen
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "classification_tokenizer.hpp"

#include <fasttext.h>

#include <string_view>

#include "iresearch/analysis/fast_text_model.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/store/store_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

Tokenizer::ptr ClassificationTokenizer::Make(Options opts,
                                             duckdb::SharedObjectCache& cache) {
  if (opts.model_location.empty()) {
    THROW_SQL_ERROR(ERR_MSG("classification: empty model location"));
  }
  if (opts.top_k <= 0) {
    THROW_SQL_ERROR(ERR_MSG("classification: top_k must be positive"));
  }
  if (opts.threshold < 0.0 || opts.threshold > 1.0) {
    THROW_SQL_ERROR(ERR_MSG("classification: threshold must be in [0, 1]"));
  }
  auto model = sdb::fast_text::GetOrBuildModel<fasttext::FastText>(
    cache, opts.model_location);
  return std::make_unique<ClassificationTokenizer>(opts, std::move(model));
}

ClassificationTokenizer::ClassificationTokenizer(const Options& options,
                                                 model_ptr model) noexcept
  : _model{std::move(model)},
    _threshold{options.threshold},
    _top_k{options.top_k} {
  SDB_ASSERT(_model);
}

template<TokenLayout Layout>
bool ClassificationTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const auto size = static_cast<uint32_t>(raw.GetSize());
  BytesViewInput s_input{
    bytes_view{reinterpret_cast<const byte_type*>(raw.GetData()), size}};
  InputBuf in_buf{&s_input};
  std::istream ss{&in_buf};
  _predictions.clear();
  _model->predictLine(ss, _predictions, _top_k, static_cast<float>(_threshold));

  for (const auto& [score, label] : _predictions) {
    sink.Emit<Layout>(label.data(), static_cast<uint32_t>(label.size()), 1);
  }
  return true;
}

template class TypedTokenizer<ClassificationTokenizer>;

}  // namespace irs::analysis
