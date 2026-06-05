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

#include "basics/exceptions.h"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/fasttext_utils.hpp"

namespace irs::analysis {
namespace {

std::atomic<NearestNeighborsTokenizer::model_provider_f> gModelProvider =
  nullptr;

}  // namespace

Analyzer::ptr NearestNeighborsTokenizer::Make(Options opts) {
  if (opts.model_location.empty()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "nearest_neighbors: empty model location");
  }
  if (opts.top_k <= 0) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "nearest_neighbors: top_k must be positive");
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
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              absl::StrCat("nearest_neighbors: failed to load fasttext kNN "
                           "model from: ",
                           opts.model_location, ", error: ", e.what()));
  } catch (...) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              absl::StrCat("nearest_neighbors: failed to load fasttext kNN "
                           "model from: ",
                           opts.model_location));
  }

  if (!model) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              absl::StrCat("nearest_neighbors: failed to load fasttext kNN "
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
  : _model{std::move(model)},
    _neighbors_it{_neighbors.end()},
    _top_k{options.top_k} {
  SDB_ASSERT(_model);

  _model_dict = _model->getDictionary();
  SDB_ASSERT(_model_dict);
}

bool NearestNeighborsTokenizer::next() {
  if (_neighbors_it == _neighbors.end()) {
    if (_current_token_ind == _n_tokens) {
      return false;
    }
    _neighbors = _model->getNN(
      _model_dict->getWord(_line_token_ids[_current_token_ind]), _top_k);
    _neighbors_it = _neighbors.begin();
    ++_current_token_ind;
  }

  auto& term = std::get<TermAttr>(_attrs);
  term.value = {
    reinterpret_cast<const byte_type*>(_neighbors_it->second.c_str()),
    _neighbors_it->second.size()};

  auto& inc = std::get<IncAttr>(_attrs);
  inc.value = static_cast<uint32_t>(_neighbors_it == _neighbors.begin());

  ++_neighbors_it;

  return true;
}

bool NearestNeighborsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());

  BytesViewInput s_input{ViewCast<byte_type>(data)};
  InputBuf buf{&s_input};
  std::istream ss{&buf};

  _model_dict->getLine(ss, _line_token_ids, _line_token_label_ids);
  _n_tokens = static_cast<int32_t>(_line_token_ids.size());
  _current_token_ind = 0;

  _neighbors.clear();
  _neighbors_it = _neighbors.end();

  return true;
}

}  // namespace irs::analysis
