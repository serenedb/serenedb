#include "catalog/search_analyzer_impl.h"

#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/index/norm.hpp>

#include "app/name_validator.h"
#include "basics/down_cast.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "catalog/analyzer.h"
#include "catalog/geo_analyzer.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/identity_analyzer.h"
#include "catalog/mangling.h"
#include "catalog/search_common.h"
#include "catalog/types.h"
#include "catalog/vpack_helper.h"
#include "frozen/set.h"
#include "vpack/vpack_helper.h"

namespace sdb::search {

#ifdef SDB_CLUSTER
std::tuple<FunctionValueType, FunctionValueType, AnalyzerImpl::StoreFunc,
           char> extern GetAnalyzerMeta(const irs::analysis::Analyzer*
                                          analyzer) noexcept;
#else
[[maybe_unused]] std::tuple<FunctionValueType, FunctionValueType,
                            AnalyzerImpl::StoreFunc, char>
GetAnalyzerMeta(const irs::analysis::Analyzer* analyzer) noexcept {
  SDB_ASSERT(analyzer);
  const auto type = analyzer->type();

  if (type == irs::Type<GeoVPackAnalyzer>::id()) {
    return {FunctionValueType::JsonCompound, FunctionValueType::String,
            &GeoVPackAnalyzer::store, mangling::kAnalyzer};
  }

  if (type == irs::Type<GeoPointAnalyzer>::id()) {
    return {FunctionValueType::JsonCompound, FunctionValueType::String,
            &GeoPointAnalyzer::store, mangling::kAnalyzer};
  }

  if (type == irs::Type<wildcard::Analyzer>::id()) {
    return {FunctionValueType::String, FunctionValueType::String,
            &wildcard::Analyzer::store, mangling::kString};
  }

#ifdef SDB_GTEST
  if ("iresearch-vpack-analyzer" == type().name()) {
    return {FunctionValueType::JsonCompound, FunctionValueType::String, nullptr,
            mangling::kString};
  }
#endif

  const auto* value_type = irs::get<AnalyzerReturnTypeAttr>(*analyzer);
  if (value_type) {
    // TODO(gnusi): returning mangling::kString is not always correct
    return {FunctionValueType::String, value_type->value, nullptr,
            mangling::kString};
  }

  return {FunctionValueType::String, FunctionValueType::String, nullptr,
          mangling::kString};
}
#endif

namespace {

bool Normalize(std::string& out, std::string_view type,
               vpack::Slice properties) {
  if (type.empty()) {
    // in iresearch we don't allow to have analyzers with empty type string
    return false;
  }
  // for API consistency we only support analyzers configurable via vpack
  return irs::analysis::analyzers::Normalize(
    out, type, irs::Type<irs::text_format::VPack>::get(),
    ToStr<char>(properties), false);
}

constexpr auto kGeoAnalyzers = frozen::make_set<std::string_view>({
  GeoJsonAnalyzer::type_name(),
  GeoPointAnalyzer::type_name(),
});

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

void Features::ToVPack(vpack::Builder& b) const {
  vpack::ArrayBuilder features{&b};
  Visit([&](std::string_view name) {
    SDB_ASSERT(!name.empty());
    b.add(name);
  });
}

Result Features::FromVPack(vpack::Slice s) {
  if (!s.isArray()) {
    return {ERROR_BAD_PARAMETER, "array expected"};
  }

  for (vpack::ArrayIterator it{s}; it.valid(); ++it) {
    const auto v = *it;
    if (!v.isString()) {
      return {ERROR_BAD_PARAMETER, "array entry #", it.index(),
              " is not a string"};
    }
    const auto name = v.stringView();
    if (!Add(name)) {
      return {ERROR_BAD_PARAMETER, "failed to find feature '", name,
              "' see array entry #", it.index()};
    }
  }

  return Validate();
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
    if (type == wildcard::Analyzer::type_name()) {
      // maybe we should disable norm for wildcard analyzer?
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
    }
    if (IsGeoAnalyzer(type)) {
      return irs::IndexFeatures::None;
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
  return kGeoAnalyzers.count(type) != 0;
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

AnalyzerImpl::Builder::ptr AnalyzerImpl::Builder::make(
  std::string_view type, vpack::Slice properties) {
  if (type.empty()) {
    // in Search we don't allow to have analyzers with empty type string
    return {};
  }
  // for API consistency we only support analyzers configurable via vpack
  return irs::analysis::analyzers::Get(
    type, irs::Type<irs::text_format::VPack>::get(), ToStr<char>(properties),
    false);
}

Result AnalyzerImpl::init(std::string_view type, vpack::Slice properties,
                          Features features, FunctionValueType input_type,
                          FunctionValueType return_type) {
  return basics::SafeCall([&] -> Result {
    _config.clear();
    if (!Normalize(_config, type, properties)) {
      return {ERROR_BAD_PARAMETER,
              "failed to normalize analyzer definition, type: ", type,
              ", properties: ", properties.toJson()};
    }

    if (auto r = features.Validate(type); !r.ok()) {
      return r;
    }

    SDB_ENSURE(!type.empty(), ERROR_BAD_PARAMETER);
    SDB_ENSURE(!_config.empty(), ERROR_BAD_PARAMETER);
    // ensure no reallocations will happen
    _config.reserve(_config.size() + type.size());

    _cache.clear();  // reset for new type/properties
    auto instance = _cache.emplace(type, ToSlice<char>(_config));
    if (!instance) {
      return {ERROR_BAD_PARAMETER,
              "failed to create analyzer instance, type: ", type,
              ", properties: ", properties.toJson()};
    }

    _properties = ToSlice<char>(_config);
    _type = {_config.data() + _config.size(), type.size()};
    _config.append(type);
    SDB_ASSERT(_type == type);

    _features = features;  // store only requested features

    bool input_invalid = (input_type == FunctionValueType::Invalid);
    bool return_invalid = (return_type == FunctionValueType::Invalid);
    SDB_ASSERT(input_invalid == return_invalid);
    if (input_invalid || return_invalid) {
      std::tie(std::ignore, std::ignore, _store_func, _field_marker) =
        GetAnalyzerMeta(instance.get());
      return {};
    }

    std::tie(_input_type, _return_type, _store_func, _field_marker) =
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
          std::tie(curr_input, curr_output, std::ignore, std::ignore) =
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
  return _cache.emplace(_type, _properties);
} catch (const basics::Exception& e) {
  SDB_WARN("xxxxx", Logger::SEARCH,
           "caught exception while instantiating an search analyzer type '",
           _type, "' properties '", _properties.toJson(), "': ", e.code(), " ",
           e.what());
  return {};
} catch (const std::exception& e) {
  SDB_WARN("xxxxx", Logger::SEARCH,
           "caught exception while instantiating an search analyzer type '",
           _type, "' properties '", _properties.toJson(), "': ", e.what());
  return {};
} catch (...) {
  SDB_WARN("xxxxx", Logger::SEARCH,
           "caught exception while instantiating an search analyzer type '",
           _type, "' properties '", _properties.toJson(), "'");
  return {};
}

void AnalyzerImpl::ToVPack(vpack::Builder& builder) const {
  vpack::ObjectBuilder o{&builder};

  // Maybe it's not bad, but we don't have mapping this _type to ObjectType
  builder.add(sdb::StaticStrings::kDataSourceType, _type);
  builder.add(search::StaticStrings::kAnalyzerPropertiesField, _properties);

  // add features
  builder.add(search::StaticStrings::kAnalyzerFeaturesField);
  _features.ToVPack(builder);

  builder.add(search::StaticStrings::kAnalyzerInputTypeField,
              static_cast<uint64_t>(_input_type));

  builder.add(search::StaticStrings::kAnalyzerReturnTypeField,
              static_cast<uint64_t>(_return_type));
}
Result AnalyzerImpl::FromVPack(ObjectId database, vpack::Slice slice,
                               std::unique_ptr<AnalyzerImpl>& implementation) {
  Features features;

  // TODO(mbkkt) isNone check needed only for gtest?
  if (auto s = slice.get(search::StaticStrings::kAnalyzerFeaturesField);
      !s.isNone()) {
    if (auto r = features.FromVPack(s); !r.ok()) {
      return r;
    }
  }

  auto impl = std::make_unique<AnalyzerImpl>();
  auto r = impl->init(
    basics::VPackHelper::getString(slice, sdb::StaticStrings::kDataSourceType,
                                   {}),
    slice.get(search::StaticStrings::kAnalyzerPropertiesField), features);
  if (!r.ok()) {
    return r;
  }

  if (auto s = slice.get(search::StaticStrings::kAnalyzerInputTypeField);
      s.isNumber()) {
    impl->_input_type = static_cast<FunctionValueType>(s.getNumber<uint64_t>());
  }

  if (auto s = slice.get(search::StaticStrings::kAnalyzerReturnTypeField);
      s.isNumber()) {
    impl->_return_type =
      static_cast<FunctionValueType>(s.getNumber<uint64_t>());
  }

  implementation = std::move(impl);
  return {};
}

std::shared_ptr<catalog::Function> MakeAnalyzer(ObjectId database,
                                                std::string_view name,
                                                std::string_view type,
                                                vpack::Slice options,
                                                Features features) {
  catalog::FunctionProperties properties{
    .options =
      {
        .language = catalog::FunctionLanguage::AnalyzerJson,
        .state = catalog::FunctionState::Immutable,
        .parallel = catalog::FunctionParallel::Safe,
      },
    .name = std::string{name},
    .id = ObjectId{},  // TODO(gnusi): what to do?
    .implementation = options,
  };

  auto impl = std::make_unique<AnalyzerImpl>();
  auto r = impl->init(type, options, features, FunctionValueType::Json,
                      FunctionValueType::Json);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  return std::make_shared<catalog::Function>(std::move(properties),
                                             std::move(impl), database);
}

bool AnalyzerEquals(const AnalyzerImpl& analyzer, std::string_view type,
                    vpack::Slice properties, Features features) {
  std::string normalized_properties;
  if (!Normalize(normalized_properties, type, properties)) {
    SDB_WARN("xxxxx", Logger::SEARCH,
             "failed to normalize properties for analyzer type '", type,
             "' properties '", properties.toString(), "'");
    return false;
  }

  // TODO(mbkkt) remove this?
  // first check non-normalizeable portion of analyzer definition
  // to rule out need to check normalization of properties
  if (analyzer.GetType() != type || analyzer.GetFeatures() != features) {
    return false;
  }

  // this check is not final as old-normalized definition may be present in
  // database!
  if (basics::VPackHelper::equals(analyzer.GetProperties(),
                                  ToSlice<char>(normalized_properties))) {
    return true;
  }

  // TODO(mbkkt) remove this?
  // Here could be analyzer definition with old-normalized properties (see Issue
  // #9652) To make sure properties really differ, let`s re-normalize and
  // re-check
  std::string re_normalized_properties;
  if (!Normalize(re_normalized_properties, analyzer.GetType(),
                 analyzer.GetProperties())) [[unlikely]] {
    // failed to re-normalize definition - strange. It was already normalized
    // once. Some bug in load/store?
    SDB_ASSERT(false);
    SDB_WARN("xxxxx", Logger::SEARCH,
             "failed to re-normalize properties for analyzer type '",
             analyzer.GetType(), "' properties '",
             analyzer.GetProperties().toJson(), "'");
    return false;
  }

  return basics::VPackHelper::equals(ToSlice<char>(normalized_properties),
                                     ToSlice<char>(re_normalized_properties));
}

}  // namespace sdb::search
