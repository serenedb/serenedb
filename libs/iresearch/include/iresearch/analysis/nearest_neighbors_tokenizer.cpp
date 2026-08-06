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

#include "nearest_neighbors_tokenizer.hpp"

#include <fasttext.h>

#include <string_view>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/fasttext_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

std::atomic<NearestNeighborsTokenizer::model_provider_f> gModelProvider =
  nullptr;

}  // namespace

Tokenizer::ptr NearestNeighborsTokenizer::Make(Options opts) {
  if (opts.model_location.empty()) {
    THROW_SQL_ERROR(ERR_MSG("nearest_neighbors: empty model location"));
  }
  if (opts.top_k <= 0) {
    THROW_SQL_ERROR(ERR_MSG("nearest_neighbors: top_k must be positive"));
  }
  auto provider = gModelProvider.load(std::memory_order_relaxed);

  model_ptr model;
  try {
    if (provider) {
      model = provider(opts.model_location);
    } else {
      auto new_model = std::make_shared<fasttext::ImmutableFastText>();
      new_model->loadModel(opts.model_location);
      model = new_model;
    }
  } catch (const std::exception& e) {
    THROW_SQL_ERROR(
      ERR_MSG("nearest_neighbors: failed to load fasttext kNN "
              "model from: ",
              opts.model_location, ", error: ", e.what()));
  } catch (...) {
    THROW_SQL_ERROR(
      ERR_MSG("nearest_neighbors: failed to load fasttext kNN "
              "model from: ",
              opts.model_location));
  }

  if (!model) {
    THROW_SQL_ERROR(
      ERR_MSG("nearest_neighbors: failed to load fasttext kNN "
              "model from: ",
              opts.model_location));
  }

  return std::make_unique<NearestNeighborsTokenizer>(opts, std::move(model));
}

NearestNeighborsTokenizer::model_provider_f
NearestNeighborsTokenizer::set_model_provider(
  model_provider_f provider) noexcept {
  return gModelProvider.exchange(provider, std::memory_order_relaxed);
}

NearestNeighborsTokenizer::NearestNeighborsTokenizer(const Options& options,
                                                     model_ptr model) noexcept
  : _model{std::move(model)}, _top_k{options.top_k} {
  SDB_ASSERT(_model);

  _model_dict = _model->getDictionary();
  SDB_ASSERT(_model_dict);
}

template<TokenLayout Layout>
bool NearestNeighborsTokenizer::DoFill(duckdb::string_t raw,
                                       TokenSink& sink) {
  const auto size = static_cast<uint32_t>(raw.GetSize());
  BytesViewInput s_input{
    bytes_view{reinterpret_cast<const byte_type*>(raw.GetData()), size}};
  InputBuf in_buf{&s_input};
  std::istream ss{&in_buf};

  _model_dict->getLine(ss, _line_token_ids, _line_token_label_ids);

  uint32_t pos = 0;
  for (const auto token_id : _line_token_ids) {
    const auto neighbors =
      _model->getNN(_model_dict->getWord(token_id), _top_k);
    if (neighbors.empty()) {
      continue;
    }
    ++pos;
    for (const auto& [score, word] : neighbors) {
      sink.Emit<Layout>(
        word.size(),
        [&](byte_type* mem) IRS_FORCE_INLINE {
          std::memcpy(mem, word.data(), word.size());
          return static_cast<uint32_t>(word.size());
        },
        pos);
    }
  }
  return true;
}

template class TypedTokenizer<NearestNeighborsTokenizer>;

}  // namespace irs::analysis
