////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "geo/geo_json.h"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::tests {

struct StringField final {
  std::string_view Name() const { return field_name; }
  irs::field_id Id() const noexcept { return id; }

  irs::Tokenizer& GetTokens() const {
    stream.reset(value);
    return stream;
  }

  bool Write(irs::DataOutput& out) const {
    irs::WriteStr(out, value);
    return true;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::None;
  }

  mutable irs::StringTokenizer stream;
  std::string_view value;
  std::string_view field_name;
  irs::field_id id{irs::field_limits::invalid()};
};

struct GeoField final {
  std::string_view Name() const { return field_name; }
  irs::field_id Id() const noexcept { return id; }

  irs::Tokenizer& GetTokens() const {
    if (!value.empty()) {
      static_cast<irs::analysis::GeoAnalyzer&>(*stream).reset(value);
    }
    return *stream.get();
  }

  // Source coding force-includes the indexed source column, so the stored
  // value is the original GeoJSON text the filter re-parses at query time.
  bool Write(irs::DataOutput& out) const {
    if (!value.empty()) {
      out.WriteBytes(reinterpret_cast<const irs::byte_type*>(value.data()),
                     value.size());
    }
    return true;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::None;
  }

  mutable irs::analysis::Analyzer::ptr stream{
    irs::analysis::GeoJsonAnalyzer::Make(
      irs::analysis::GeoJsonAnalyzer::Options{})};
  std::string_view value;
  std::string_view field_name;
  irs::field_id id{irs::field_limits::invalid()};
};

}  // namespace irs::tests
