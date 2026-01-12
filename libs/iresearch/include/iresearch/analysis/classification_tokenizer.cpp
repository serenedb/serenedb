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
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "iresearch/store/store_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kModelLocationParamName = "model_location";
constexpr std::string_view kTopKParamName = "top_k";
constexpr std::string_view kThresholdParamName = "threshold";

std::atomic<ClassificationTokenizer::model_provider_f> gModelProvider = nullptr;

bool ParseVPackOptions(const vpack::Slice slice,
                       ClassificationTokenizer::Options& options,
                       const char* action) {
  if (vpack::ValueType::Object == slice.type()) {
    auto model_location_slice = slice.get(kModelLocationParamName);
    if (!model_location_slice.isString()) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Invalid vpack while ", action,
                     " classification_tokenizer from VPack arguments. ",
                     kModelLocationParamName, " value should be a string."));
      return false;
    }
    options.model_location = model_location_slice.stringView();
    auto top_k_slice = slice.get(kTopKParamName);
    if (!top_k_slice.isNone()) {
      if (!top_k_slice.isNumber<int32_t>()) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Invalid value provided while ", action,
                       " classification_tokenizer from VPack arguments. ",
                       kTopKParamName,
                       " value should be an non-negative int32_t."));
        return false;
      }
      options.top_k = top_k_slice.getNumber<int32_t>();
      if (options.top_k < 0) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Invalid value provided while ", action,
                       " classification_tokenizer from VPack arguments. ",
                       kTopKParamName,
                       " value should be an non-negative int32_t."));
        return false;
      }
    }

    auto threshold_slice = slice.get(kThresholdParamName);
    if (!threshold_slice.isNone()) {
      if (!threshold_slice.isNumber()) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Invalid vpack while ", action,
                       " classification_tokenizer from VPack arguments. ",
                       kThresholdParamName, " value should be a double."));
        return false;
      }
      const auto threshold = threshold_slice.getNumber<double>();
      if (threshold < 0.0 || threshold > 1.0) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Invalid value provided while ", action,
                       " classification_tokenizer from VPack arguments. ",
                       kTopKParamName,
                       " value should be between 0.0 and 1.0 (inclusive)."));
        return false;
      }
      options.threshold = threshold;
    }
    return true;
  }

  SDB_ERROR(
    "xxxxx", sdb::Logger::IRESEARCH,
    absl::StrCat(
      "Invalid vpack while ", action,
      " classification_tokenizer from VPack arguments. Object was expected."));

  return false;
}

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
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Failed to load fasttext classification model from: ",
                   options.model_location, ", error: ", e.what()));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Failed to load fasttext classification model from: ",
                   options.model_location));
  }

  if (!model) {
    return nullptr;
  }

  return std::make_unique<ClassificationTokenizer>(options, std::move(model));
}

Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  ClassificationTokenizer::Options options{};
  if (ParseVPackOptions(slice, options, "constructing")) {
    return Construct(options);
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice{reinterpret_cast<const uint8_t*>(args.data())};
  return MakeVPack(slice);
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while constructing classification_tokenizer ");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing classification_tokenizer from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing classification_tokenizer from JSON");
  }
  return nullptr;
}

bool MakeVPackConfig(const ClassificationTokenizer::Options& options,
                     vpack::Builder* builder) {
  vpack::ObjectBuilder object{builder};
  {
    builder->add(kModelLocationParamName, options.model_location);
    builder->add(kTopKParamName, options.top_k);
    builder->add(kThresholdParamName, options.threshold);
  }
  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  ClassificationTokenizer::Options options{};
  if (ParseVPackOptions(slice, options, "normalizing")) {
    return MakeVPackConfig(options, builder);
  }
  return false;
}

bool NormalizeVPackConfig(std::string_view args, std::string& config) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  vpack::Builder builder;
  if (NormalizeVPackConfig(slice, &builder)) {
    config.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
    return true;
  }
  return false;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing classification_tokenizer ");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while normalizing classification_tokenizer from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while normalizing classification_tokenizer from JSON");
  }
  return false;
}

}  // namespace

void ClassificationTokenizer::init() {
  REGISTER_ANALYZER_JSON(ClassificationTokenizer, MakeJson,
                         NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(ClassificationTokenizer, MakeVPack,
                          NormalizeVPackConfig);
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
