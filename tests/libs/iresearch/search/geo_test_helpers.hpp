////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <simdjson.h>

#include <string>
#include <string_view>
#include <vector>

namespace irs::tests {

// A single test document: a GeoJSON geometry plus its row name. The geometry
// is kept as text so it can be re-parsed by the analyzer / geo::json parsers
// (the force-included source column stores this same text).
struct GeoDoc {
  std::string name;
  std::string geometry;
};

// Parses a JSON array of {"name": "...", "geometry": {...}} objects, copying
// each name and the raw geometry text out so callers can drive the analyzer
// and the filter from owned strings (on-demand documents are single-pass).
inline std::vector<GeoDoc> ParseGeoDocs(std::string_view json) {
  std::string buffer{json};
  buffer.append(simdjson::SIMDJSON_PADDING, '\0');
  simdjson::padded_string_view padded_view{buffer.data(), json.size(),
                                           buffer.size()};
  simdjson::dom::parser parser;
  std::vector<GeoDoc> docs;
  for (auto element : parser.parse(padded_view.data(), json.size())) {
    GeoDoc doc;
    doc.name = std::string{std::string_view{element["name"]}};
    doc.geometry = simdjson::to_string(element["geometry"]);
    docs.emplace_back(std::move(doc));
  }
  return docs;
}

// Owns a JSON text buffer plus a reusable simdjson on-demand parser/document.
// text() yields the original GeoJSON text (feed it to analyzer reset); value()
// re-iterates a fresh document each call so geo::json parsers (single-pass
// on-demand) can be invoked repeatedly on the same source.
class JsonDoc {
 public:
  explicit JsonDoc(std::string_view json) : _text{json} {
    _buffer.assign(json);
    _buffer.append(simdjson::SIMDJSON_PADDING, '\0');
  }

  std::string_view text() const noexcept { return _text; }

  simdjson::ondemand::value value() {
    simdjson::padded_string_view padded_view{_buffer.data(), _text.size(),
                                             _buffer.size()};
    _doc = _parser.iterate(padded_view).value();
    return _doc.get_value().value();
  }

  JsonDoc(JsonDoc&&) = default;
  JsonDoc& operator=(JsonDoc&&) = default;

 private:
  std::string _text;
  std::string _buffer;
  simdjson::ondemand::parser _parser;
  simdjson::ondemand::document _doc;
};

inline JsonDoc FromJson(std::string_view json) { return JsonDoc{json}; }

}  // namespace irs::tests
