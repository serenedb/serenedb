////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "catalog/search_analyzer_impl.h"

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/index/norm.hpp>

#include "basics/containers/trivial_map.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/serializer.h"
#include "catalog/mangling.h"

namespace sdb::search {

[[maybe_unused]] std::tuple<FunctionValueType, FunctionValueType, char>
GetAnalyzerMeta(const irs::analysis::Analyzer* analyzer) noexcept {
  SDB_ASSERT(analyzer);
  const auto type = analyzer->type();

  if (type == irs::Type<irs::analysis::GeoJsonAnalyzer>::id()) {
    return {FunctionValueType::JsonCompound, FunctionValueType::String,
            mangling::kAnalyzer};
  }

  if (type == irs::Type<irs::analysis::GeoPointAnalyzer>::id()) {
    return {FunctionValueType::JsonCompound, FunctionValueType::String,
            mangling::kAnalyzer};
  }

  if (type == irs::Type<irs::analysis::WildcardAnalyzer>::id()) {
    return {FunctionValueType::String, FunctionValueType::String,
            mangling::kString};
  }

#ifdef SDB_GTEST
  if ("iresearch-vpack-analyzer" == type().name()) {
    return {FunctionValueType::JsonCompound, FunctionValueType::String,
            mangling::kString};
  }
#endif

  const auto* value_type = irs::get<AnalyzerReturnTypeAttr>(*analyzer);
  if (value_type) {
    // TODO(gnusi): returning mangling::kString is not always correct
    return {FunctionValueType::String, value_type->value, mangling::kString};
  }

  return {FunctionValueType::String, FunctionValueType::String,
          mangling::kString};
}

namespace {

constexpr containers::TrivialSet kGeoAnalyzers = [](auto selector) {
  return selector()
    .Case(irs::analysis::GeoJsonAnalyzer::type_name())
    .Case(irs::analysis::GeoPointAnalyzer::type_name());
};

}  // namespace

void Features::Visit(std::function<void(std::string_view)> visitor) const {
  if (HasFeatures(irs::IndexFeatures::Freq)) {
    visitor(irs::Type<irs::FreqAttr>::name());
  }
  if (HasFeatures(irs::IndexFeatures::Pos)) {
    visitor(irs::Type<irs::PosAttr>::name());
  }
  if (HasFeatures(irs::IndexFeatures::Offs)) {
    visitor(irs::Type<irs::OffsAttr>::name());
  }
  if (HasFeatures(irs::IndexFeatures::Norm)) {
    visitor(irs::Type<irs::Norm>::name());
  }
}

bool Features::Add(std::string_view feature_name) {
  if (feature_name == irs::Type<irs::PosAttr>::name()) {
    _index_features |= irs::IndexFeatures::Pos;
  } else if (feature_name == irs::Type<irs::FreqAttr>::name()) {
    _index_features |= irs::IndexFeatures::Freq;
  } else if (feature_name == irs::Type<irs::OffsAttr>::name()) {
    _index_features |= irs::IndexFeatures::Offs;
  } else if (feature_name == irs::Type<irs::Norm>::name()) {
    _index_features |= irs::IndexFeatures::Norm;
  } else {
    return false;
  }
  return true;
}

Result Features::Validate(std::string_view type) const {
  if (HasFeatures(irs::IndexFeatures::Offs) &&
      !HasFeatures(irs::IndexFeatures::Pos)) {
    return {
      ERROR_BAD_PARAMETER,
      "missing feature 'position' required when 'offset' feature is specified"};
  }

  if (HasFeatures(irs::IndexFeatures::Pos) &&
      !HasFeatures(irs::IndexFeatures::Freq)) {
    return {ERROR_BAD_PARAMETER,
            "missing feature 'frequency' required when 'position' feature is "
            "specified"};
  }

  if (HasFeatures(irs::IndexFeatures::Norm) &&
      !HasFeatures(irs::IndexFeatures::Freq)) {
    return {
      ERROR_BAD_PARAMETER,
      "missing feature 'frequency' required when 'norm' feature is specified"};
  }

  const auto supported_features = [&] {
    if (type == irs::analysis::WildcardAnalyzer::type_name()) {
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
    }
    if (IsGeoAnalyzer(type)) {
      return irs::IndexFeatures::None;
    }
    if (type == irs::analysis::UnionTokenizer::type_name()) {
      // Union does not expose OffsAttr; interleaving tokens from independent
      // sub-tokenizers over the same input breaks the monotonic offset
      // invariant required by the indexer.
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
    }
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Norm | irs::IndexFeatures::Offs;
  }();

  if (!irs::IsSubsetOf(_index_features, supported_features)) {
    return {ERROR_BAD_PARAMETER, "Unsupported index features are specified: ",
            std::to_underlying(_index_features)};
  }

  return {};
}

bool IsGeoAnalyzer(std::string_view type) noexcept {
  return kGeoAnalyzers.Contains(type);
}

AnalyzerImpl::Builder::ptr AnalyzerImpl::Builder::make(StringStreamTag) {
  return std::make_unique<irs::StringTokenizer>();
}

AnalyzerImpl::Builder::ptr AnalyzerImpl::Builder::make(NumberStreamTag) {
  return std::make_unique<irs::NumericTokenizer>();
}

AnalyzerImpl::Builder::ptr AnalyzerImpl::Builder::make(BoolStreamTag) {
  return std::make_unique<irs::BooleanTokenizer>();
}

AnalyzerImpl::Builder::ptr AnalyzerImpl::Builder::make(NullStreamTag) {
  return std::make_unique<irs::NullTokenizer>();
}

AnalyzerImpl::Builder::ptr AnalyzerImpl::Builder::make(std::string_view bytes) {
  if (bytes.empty()) {
    return {};
  }
  try {
    auto stream = catalog::ReadStream(bytes);
    duckdb::BinaryDeserializer src{stream};
    irs::analysis::TokenizerConfig cfg;
    basics::ReadTuple(src, cfg);
    return irs::analysis::CreateAnalyzer(std::move(cfg));
  } catch (...) {
    return {};
  }
}

Result AnalyzerImpl::init(std::string_view type,
                          std::string_view properties_bytes, Features features,
                          FunctionValueType input_type,
                          FunctionValueType return_type) {
  return basics::SafeCall([&] -> Result {
    if (type.empty()) {
      return {ERROR_BAD_PARAMETER, "analyzer type is empty"};
    }
    if (auto r = features.Validate(type); !r.ok()) {
      return r;
    }

    _config.assign(properties_bytes.data(), properties_bytes.size());
    const auto properties_size = _config.size();
    // ensure no reallocations will happen when appending the type tail below
    _config.reserve(_config.size() + type.size());

    _cache.clear();  // reset for new type/properties
    auto instance =
      _cache.emplace(std::string_view{_config.data(), properties_size});
    if (!instance) {
      return {ERROR_BAD_PARAMETER,
              "failed to create analyzer instance, type: ",
              type,
              ", properties: ",
              properties_size,
              " bytes"};
    }

    _properties = std::string_view{_config.data(), properties_size};
    _type = {_config.data() + _config.size(), type.size()};
    _config.append(type);
    SDB_ASSERT(_type == type);

    _features = features;  // store only requested features

    bool input_invalid = (input_type == FunctionValueType::Invalid);
    bool return_invalid = (return_type == FunctionValueType::Invalid);
    SDB_ASSERT(input_invalid == return_invalid);
    if (input_invalid || return_invalid) {
      std::tie(std::ignore, std::ignore, _field_marker) =
        GetAnalyzerMeta(instance.get());
      return {};
    }

    std::tie(_input_type, _return_type, _field_marker) =
      GetAnalyzerMeta(instance.get());
    if (instance->type() != irs::Type<irs::analysis::PipelineTokenizer>::id()) {
      return {};
    }

    // pipeline needs to validate members compatibility
    const irs::analysis::Analyzer* prev = nullptr;
    const irs::analysis::Analyzer* next = nullptr;
    auto prev_output = FunctionValueType::Invalid;
    auto& pipeline =
      basics::downCast<irs::analysis::PipelineTokenizer>(*instance);
    if (!pipeline.visit_members([&](const irs::analysis::Analyzer& curr) {
          FunctionValueType curr_input;
          FunctionValueType curr_output;
          std::tie(curr_input, curr_output, std::ignore) =
            GetAnalyzerMeta(&curr);
          if (prev &&
              (curr_input & prev_output) == FunctionValueType::Invalid) {
            next = &curr;
            return false;
          }
          prev = &curr;
          prev_output = curr_output;
          return true;
        })) {
      return {ERROR_BAD_PARAMETER,
              "Failed to validate pipeline analyzer, because incompatible "
              "part found. Analyzer type: ",
              prev->type()().name(),
              ", emits output not acceptable by analyzer type: ",
              next->type()().name()};
    }
    // for pipeline we take last pipe member output type as whole pipe output
    // type
    _return_type = prev_output;
    return {};
  });
}

AnalyzerImpl::CacheType::ptr AnalyzerImpl::Get() const noexcept try {
  return _cache.emplace(_properties);
} catch (const basics::Exception& e) {
  SDB_WARN(SEARCH,
           "caught exception while instantiating an search analyzer type '",
           _type, "' (", _properties.size(),
           " bytes of properties): ", e.code(), " ", e.what());
  return {};
} catch (const std::exception& e) {
  SDB_WARN(
    SEARCH, "caught exception while instantiating an search analyzer type '",
    _type, "' (", _properties.size(), " bytes of properties): ", e.what());
  return {};
} catch (...) {
  SDB_WARN(SEARCH,
           "caught exception while instantiating an search analyzer type '",
           _type, "' (", _properties.size(), " bytes of properties)");
  return {};
}

}  // namespace sdb::search
