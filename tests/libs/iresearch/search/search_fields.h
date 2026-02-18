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

#include <vpack/slice.h>

#include "geo/geo_json.h"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::tests {

struct StringField final {
  std::string_view Name() const { return field_name; }

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
};

struct GeoField final {
  std::string_view Name() const { return field_name; }

  irs::Tokenizer& GetTokens() const {
    if (!shape_slice.isNone()) {
      stream->reset(irs::slice_to_view<char>(shape_slice));
    }
    return *stream.get();
  }

  bool Write(irs::DataOutput& out) const {
    if (!shape_slice.isNone()) {
      out.WriteBytes(shape_slice.start(), shape_slice.byteSize());
    }
    return true;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::None;
  }

  mutable irs::analysis::Analyzer::ptr stream{
    irs::analysis::GeoJsonAnalyzer::make(
      irs::slice_to_view<char>(vpack::Slice::emptyObjectSlice()))};
  vpack::Slice shape_slice;
  std::string_view field_name;
};

}  // namespace irs::tests
