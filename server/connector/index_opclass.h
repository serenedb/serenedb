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

#pragma once

#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/optional_ptr.hpp>
#include <duckdb/common/types/value.hpp>
#include <optional>
#include <string_view>

#include "catalog1/entry/inverted_index.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace irs::analysis {

class Analyzer;

}  // namespace irs::analysis
namespace sdb::catalog {

class TokenizerCatalogEntry;

}  // namespace sdb::catalog
namespace sdb::connector {

// The opclass a CREATE INDEX key was written with. `options` is nullopt when
// the name carried no parentheses, which is what separates a built-in from a
// text search dictionary of the same name: `included`/`ivf` name the built-in
// only in the parenthesised form, so a dictionary may shadow either.
struct KeyOpclass {
  std::string_view name;
  const std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>* options =
    nullptr;

  bool HasParentheses() const noexcept {
    return options != nullptr && options->has_value();
  }
  bool IsBuiltin(std::string_view builtin) const noexcept {
    return HasParentheses() && name == builtin;
  }
  bool IsTokenizer() const noexcept;
};

namespace term_dict {

// Rejects a key type the term dictionary cannot encode. A bare GEOMETRY key
// needs an opclass -- there is no default tokenization for one.
void Validate(std::string_view label, const duckdb::LogicalType& type,
              std::string_view opclass);

}  // namespace term_dict
namespace included {

void Validate(std::string_view label, const duckdb::LogicalType& type);

}  // namespace included
namespace ivf {

// The vector width of an ARRAY(FLOAT, N) key, or 0 for any other type.
uint32_t Dimension(const duckdb::LogicalType& type) noexcept;

void Validate(std::string_view label, const duckdb::LogicalType& type);

}  // namespace ivf

// Rejects a key whose declared type the opclass cannot index at all, before
// any dictionary lookup: the ivf / included built-ins by type, an unknown
// parenthesised built-in by name, everything else by term-dictionary support.
void ValidateInvertedIndexKey(std::string_view label,
                              const duckdb::LogicalType& type,
                              const KeyOpclass& opclass);

// The geo and tokenizer column-type contract: a geo analyzer wants JSON or a
// CRS84 GEOMETRY (and geopoint wants JSON outright), any other tokenizer wants
// VARCHAR / BLOB or a list of them.
void ValidateTokenizerVsColumn(std::string_view column_name,
                               const duckdb::LogicalType& col_type,
                               const irs::analysis::Analyzer& analyzer);

// Whether the analyzer reads the whole value rather than descending into it.
// A geo analyzer parses the GeoJSON object itself, so such a key gets no JSON
// leaf ids and is exempt from the object/array leaf rejection.
bool IsGeoAnalyzer(const irs::analysis::Analyzer& analyzer);

// Geo codings that keep no analyzer-side blob, so the query re-parses the
// source: every geopoint, and geojson under `coding = 'source'`. Such a key
// force-includes its column into the columnstore.
bool IsGeoSourceAnalyzer(const irs::analysis::Analyzer& analyzer);

// Folds one key's opclass into the field's config, drawing whatever sub-field
// ids that opclass turns out to need from `next_sub_id`. Merges: a column
// listed twice arrives here twice with the same `entry`.
//
// `dict` is the resolved text search dictionary, or null when the opclass is
// a built-in or empty. Throws when the opclass names neither.
void ApplyOpclassToEntry(duckdb::ClientContext& context,
                         std::string_view schema_name, std::string_view label,
                         const duckdb::LogicalType& value_type,
                         const KeyOpclass& opclass,
                         duckdb::optional_ptr<catalog::TokenizerCatalogEntry> dict,
                         irs::field_id& next_sub_id,
                         catalog::InvertedIndexField& entry);

}  // namespace sdb::connector
