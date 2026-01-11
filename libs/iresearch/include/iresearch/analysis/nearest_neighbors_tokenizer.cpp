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
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/fasttext_utils.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kModelLocationParamName = "model_location";
constexpr std::string_view kTopKParamName = "top_k";

std::atomic<NearestNeighborsTokenizer::model_provider_f> gModelProvider =
  nullptr;

bool ParseVPackOptions(const vpack::Slice slice,
                       NearestNeighborsTokenizer::Options& options,
                       const char* action) {
  if (vpack::ValueType::Object == slice.type()) {
    auto model_location_slice = slice.get(kModelLocationParamName);
    if (!model_location_slice.isString()) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Invalid vpack while ", action,
                     " nearest_neighbors_tokenizer from VPack arguments. ",
                     kModelLocationParamName, " value should be a string."));
      return false;
    }
    options.model_location = model_location_slice.stringView();
    auto top_k_slice = slice.get(kTopKParamName);
    if (!top_k_slice.isNone()) {
      if (!top_k_slice.isNumber()) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Invalid vpack while ", action,
                       " nearest_neighbors_tokenizer from VPack arguments. ",
                       kTopKParamName, " value should be an integer."));
        return false;
      }
      const auto top_k = top_k_slice.getNumber<size_t>();
      if (top_k > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Invalid value provided while ", action,
                       " nearest_neighbors_tokenizer from VPack arguments. ",
                       kTopKParamName, " value should be an int32_t."));
        return false;
      }
      options.top_k = static_cast<uint32_t>(top_k);
    }

    return true;
  }

  SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
            absl::StrCat("Invalid vpack while ", action,
                         " nearest_neighbors_tokenizer from VPack arguments. "
                         "Object was expected."));

  return false;
}

Analyzer::ptr Construct(const NearestNeighborsTokenizer::Options& options) {
  auto provider = gModelProvider.load(std::memory_order_relaxed);

  NearestNeighborsTokenizer::model_ptr model;

  try {
    if (provider) {
      model = provider(options.model_location);
    } else {
      auto new_model = std::make_shared<fasttext::ImmutableFastText>();
      new_model->loadModel(options.model_location);

      model = new_model;
    }
  } catch (const std::exception& e) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to load fasttext kNN model from: ",
                           options.model_location, ", error: ", e.what()));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to load fasttext kNN model from: ",
                           options.model_location));
  }

  if (!model) {
    return nullptr;
  }

  return std::make_unique<NearestNeighborsTokenizer>(options, std::move(model));
}

Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  NearestNeighborsTokenizer::Options options{};
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
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Null arguments while constructing nearest_neighbors_tokenizer ");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat(
                "Caught error '", ex.what(),
                "' while constructing nearest_neighbors_tokenizer from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing nearest_neighbors_tokenizer from JSON");
  }
  return nullptr;
}

bool MakeVPackConfig(const NearestNeighborsTokenizer::Options& options,
                     vpack::Builder* builder) {
  vpack::ObjectBuilder object{builder};
  {
    builder->add(kModelLocationParamName, options.model_location);
    builder->add(kTopKParamName, options.top_k);
  }
  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  NearestNeighborsTokenizer::Options options{};
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
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Null arguments while normalizing nearest_neighbors_tokenizer ");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat(
                "Caught error '", ex.what(),
                "' while normalizing nearest_neighbors_tokenizer from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while normalizing nearest_neighbors_tokenizer from JSON");
  }
  return false;
}

}  // namespace

void NearestNeighborsTokenizer::init() {
  REGISTER_ANALYZER_JSON(NearestNeighborsTokenizer, MakeJson,
                         NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(NearestNeighborsTokenizer, MakeVPack,
                          NormalizeVPackConfig);
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
