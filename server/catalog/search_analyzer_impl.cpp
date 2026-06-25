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
#include <iresearch/analysis/shingle_analyzer.hpp>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/index/norm.hpp>

#include "basics/containers/flat_hash_set.h"
#include "basics/serializer.h"
#include "catalog/object.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

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
    if (type == irs::analysis::WildcardAnalyzer::type_name()) {
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
    }
    if (type == irs::analysis::ShingleAnalyzer::type_name()) {
      // Two phrase strategies share the analyzer, chosen by the column's
      // features. Frequency-only (Strategy A) generates shingle candidates and
      // verifies contiguity against the stored token stream. Adding Positions
      // (Strategy B) makes a phrase over the positional shingle terms exact by
      // construction, so no per-candidate verification is needed. Norm rides
      // the synthetic column, so it excludes the stored token stream
      // (storetokens = false; enforced at CREATE TEXT SEARCH DICTIONARY).
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
             irs::IndexFeatures::Norm;
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
    if (type == irs::analysis::SparseNGramTokenizer::type_name()) {
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
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

bool IsGeoAnalyzer(std::string_view type) noexcept {
  static const containers::FlatHashSet<std::string_view> kGeoAnalyzers = {
    irs::analysis::GeoJsonAnalyzer::type_name(),
    irs::analysis::GeoPointAnalyzer::type_name(),
  };
  return kGeoAnalyzers.contains(type);
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

}  // namespace sdb::search
