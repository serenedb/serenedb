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

#include "search/search_analyzer_impl.h"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/parsed_data/create_tokenizer_info.hpp>
#include <iresearch/analysis/geo_tokenizer.hpp>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_tokenizer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>

#include "catalog/catalog.h"

namespace sdb::search {
namespace {

std::string FeatureNames(irs::IndexFeatures features) {
  const std::pair<irs::IndexFeatures, std::string_view> names[] = {
    {irs::IndexFeatures::Freq, irs::Type<irs::FreqAttr>::name()},
    {irs::IndexFeatures::Pos, irs::Type<irs::PosAttr>::name()},
    {irs::IndexFeatures::Offs, irs::Type<irs::OffsAttr>::name()},
    {irs::IndexFeatures::Norm, irs::Type<irs::Norm>::name()},
  };
  std::string out;
  for (const auto& [feature, name] : names) {
    if (irs::IsSubsetOf(feature, features)) {
      absl::StrAppend(&out, out.empty() ? "" : ", ", name);
    }
  }
  return out;
}

}  // namespace

static_assert(duckdb::TOKENIZER_FEATURES[0].bit ==
                std::to_underlying(irs::IndexFeatures::Freq) &&
              duckdb::TOKENIZER_FEATURES[0].name ==
                irs::Type<irs::FreqAttr>::name());
static_assert(duckdb::TOKENIZER_FEATURES[1].bit ==
                std::to_underlying(irs::IndexFeatures::Pos) &&
              duckdb::TOKENIZER_FEATURES[1].name ==
                irs::Type<irs::PosAttr>::name());
static_assert(duckdb::TOKENIZER_FEATURES[2].bit ==
                std::to_underlying(irs::IndexFeatures::Offs) &&
              duckdb::TOKENIZER_FEATURES[2].name ==
                irs::Type<irs::OffsAttr>::name());
static_assert(duckdb::TOKENIZER_FEATURES[3].bit ==
                std::to_underlying(irs::IndexFeatures::Norm) &&
              duckdb::TOKENIZER_FEATURES[3].name ==
                irs::Type<irs::Norm>::name());

bool Features::Add(std::string_view feature_name) {
  for (const auto& feature : duckdb::TOKENIZER_FEATURES) {
    if (feature_name == feature.name) {
      _index_features |= static_cast<irs::IndexFeatures>(feature.bit);
      return true;
    }
  }
  return false;
}

std::vector<std::string_view> Features::Names() const {
  std::vector<std::string_view> names;
  for (const auto& feature : duckdb::TOKENIZER_FEATURES) {
    if (HasFeatures(static_cast<irs::IndexFeatures>(feature.bit))) {
      names.push_back(feature.name);
    }
  }
  return names;
}

void Features::Validate(std::string_view type) const {
  if (HasFeatures(irs::IndexFeatures::Offs) &&
      !HasFeatures(irs::IndexFeatures::Pos)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("missing feature 'position' required when 'offset' feature is "
              "specified"));
  }

  if (HasFeatures(irs::IndexFeatures::Pos) &&
      !HasFeatures(irs::IndexFeatures::Freq)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("missing feature 'frequency' required when 'position' feature is "
              "specified"));
  }

  if (HasFeatures(irs::IndexFeatures::Norm) &&
      !HasFeatures(irs::IndexFeatures::Freq)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("missing feature 'frequency' required when 'norm' feature is "
              "specified"));
  }

  const auto supported_features = [&] {
    if (type == irs::analysis::WildcardTokenizer::type_name()) {
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
    }
    if (IsGeoTokenizer(type)) {
      return irs::IndexFeatures::None;
    }
    if (type == irs::analysis::UnionTokenizer::type_name()) {
      // Union does not expose OffsAttr; interleaving tokens from independent
      // sub-tokenizers over the same input breaks the monotonic offset
      // invariant required by the indexer.
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
    }
    if (type == irs::analysis::SparseNGramTokenizer::type_name()) {
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
    }
    if (type == irs::analysis::SqlTokenizer::type_name()) {
      // Expression results carry no source offsets.
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
    }
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Norm | irs::IndexFeatures::Offs;
  }();

  if (!irs::IsSubsetOf(_index_features, supported_features)) {
    const auto supported = FeatureNames(supported_features);
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Unsupported index features are specified: ",
              FeatureNames(_index_features & ~supported_features)),
      ERR_HINT(type, " supports ",
               supported.empty() ? "no index features" : supported, "."));
  }
}

bool IsGeoTokenizer(std::string_view type) noexcept {
  static const irs::containers::FlatHashSet<std::string_view> kGeoTokenizers = {
    irs::analysis::GeoJsonTokenizer::type_name(),
    irs::analysis::GeoPointTokenizer::type_name(),
  };
  return kGeoTokenizers.contains(type);
}

}  // namespace sdb::search
