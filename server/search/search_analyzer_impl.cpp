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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
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

#include "catalog/entry.h"

namespace sdb::search {

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
    if (irs::analysis::GeoJsonTokenizer::IsGeoTokenizer(type)) {
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
    if (type == irs::analysis::ShingleTokenizer::type_name()) {
      // Shingle terms carry positions but no source offsets.
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
    }
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Norm | irs::IndexFeatures::Offs;
  }();

  if (!irs::IsSubsetOf(_index_features, supported_features)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Unsupported index features are specified: ",
                            std::to_underlying(_index_features)));
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
