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

#include "basics/exceptions.h"
#include "iresearch/store/store_utils.hpp"

namespace irs::analysis {
namespace {

std::atomic<ClassificationTokenizer::model_provider_f> gModelProvider = nullptr;

Analyzer::ptr Construct(const ClassificationTokenizer::Options& options) {
  auto provider = gModelProvider.load(std::memory_order_relaxed);

  ClassificationTokenizer::model_ptr model;

  try {
    if (provider) {
      model = provider(options.model_location);
    } else {
      auto new_model = std::make_shared<fasttext::FastText>();
      new_model->loadModel(options.model_location);

      model = new_model;
    }
  } catch (const std::exception& e) {
    SDB_THROW(
      sdb::ERROR_BAD_PARAMETER,
      absl::StrCat("classification: failed to load fasttext model from: ",
                   options.model_location, ", error: ", e.what()));
  } catch (...) {
    SDB_THROW(
      sdb::ERROR_BAD_PARAMETER,
      absl::StrCat("classification: failed to load fasttext model from: ",
                   options.model_location));
  }

  if (!model) {
    SDB_THROW(
      sdb::ERROR_BAD_PARAMETER,
      absl::StrCat("classification: failed to load fasttext model from: ",
                   options.model_location));
  }

  return std::make_unique<ClassificationTokenizer>(options, std::move(model));
}

}  // namespace

Analyzer::ptr ClassificationTokenizer::Make(Options opts) {
  if (opts.model_location.empty()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "classification: empty model location");
  }
  if (opts.top_k <= 0) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "classification: top_k must be positive");
  }
  if (opts.threshold < 0.0 || opts.threshold > 1.0) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "classification: threshold must be in [0, 1]");
  }
  return Construct(opts);
}

ClassificationTokenizer::model_provider_f
ClassificationTokenizer::set_model_provider(
  model_provider_f provider) noexcept {
  return gModelProvider.exchange(provider, std::memory_order_relaxed);
}

ClassificationTokenizer::ClassificationTokenizer(const Options& options,
                                                 model_ptr model) noexcept
  : _model{std::move(model)},
    _predictions_it{_predictions.end()},
    _threshold{options.threshold},
    _top_k{options.top_k} {
  SDB_ASSERT(_model);
}

bool ClassificationTokenizer::next() {
  if (_predictions_it == _predictions.end()) {
    return false;
  }

  auto& term = std::get<TermAttr>(_attrs);
  term.value = {
    reinterpret_cast<const byte_type*>(_predictions_it->second.c_str()),
    _predictions_it->second.size()};

  auto& inc = std::get<IncAttr>(_attrs);
  inc.value = static_cast<uint32_t>(_predictions_it == _predictions.begin());

  ++_predictions_it;

  return true;
}

bool ClassificationTokenizer::reset(std::string_view data) {
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());

  BytesViewInput s_input{ViewCast<byte_type>(data)};
  InputBuf buf{&s_input};
  std::istream ss{&buf};
  _predictions.clear();
  _model->predictLine(ss, _predictions, _top_k, static_cast<float>(_threshold));
  _predictions_it = _predictions.begin();

  return true;
}

}  // namespace irs::analysis
