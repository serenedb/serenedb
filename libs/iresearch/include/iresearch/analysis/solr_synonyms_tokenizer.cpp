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

#include "solr_synonyms_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <string_view>

#include "basics/exceptions.h"
#include "basics/result.h"

namespace irs::analysis {

namespace {
SolrSynonymsTokenizer::SynonymsList SplitLine(const std::string_view line) {
  std::vector<std::string_view> outputs(absl::StrSplit(line, ','));

  for (auto& s : outputs) {
    s = absl::StripAsciiWhitespace(s);
    if (s.empty()) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER);
    }
  }

  absl::c_sort(outputs);
  outputs.erase(std::unique(outputs.begin(), outputs.end()), outputs.end());

  return outputs;
}
}  // namespace

sdb::ResultOr<SolrSynonymsTokenizer::SynonymsLines>
SolrSynonymsTokenizer::ParseSynonymsLines(std::string_view input) {
  SynonymsLines synonyms_lines;

  std::vector<std::string_view> lines = absl::StrSplit(input, '\n');
  size_t line_number{};
  for (const auto& line : lines) {
    line_number++;
    if (line.empty() || line[0] == '#')
      continue;
    std::vector<std::string_view> sides = absl::StrSplit(line, "=>");

    SynonymsLine synonyms_line;

    if (sides.size() > 1) {
      if (sides.size() != 2) {
        return std::unexpected<sdb::Result>{
          std::in_place, sdb::ERROR_BAD_PARAMETER,
          "More than one explicit mapping specified on the line ", line_number};
      }

      try {
        synonyms_line.in = SplitLine(sides[0]);
        synonyms_line.out = SplitLine(sides[1]);
      } catch (...) {
        return std::unexpected<sdb::Result>{std::in_place,
                                            sdb::ERROR_BAD_PARAMETER,
                                            "Failed parse line ", line_number};
      }

      synonyms_lines.push_back(std::move(synonyms_line));

    } else {
      try {
        synonyms_line.out = SplitLine(sides[0]);
      } catch (...) {
        return std::unexpected<sdb::Result>{std::in_place,
                                            sdb::ERROR_BAD_PARAMETER,
                                            "Failed parse line ", line_number};
      }

      synonyms_lines.push_back(std::move(synonyms_line));
    }
  }

  return synonyms_lines;
}

sdb::ResultOr<SolrSynonymsTokenizer::SynonymsMap> SolrSynonymsTokenizer::Parse(
  const SynonymsLines& lines) {
  SynonymsMap result;
  for (const auto& synonyms_line : lines) {
    if (synonyms_line.in.empty()) {
      for (std::string_view synonym : synonyms_line.out) {
        result[synonym] = &synonyms_line.out;
      }
    } else {
      for (std::string_view synonym : synonyms_line.in) {
        result[synonym] = &synonyms_line.out;
      }
    }
  }
  return result;
}

SolrSynonymsTokenizer::SolrSynonymsTokenizer(
  SolrSynonymsTokenizer::SynonymsMap&& synonyms)
  : _synonyms(std::move(synonyms)) {}

bool SolrSynonymsTokenizer::next() {
  if (_curr == _end) {
    return false;
  }

  auto& inc = std::get<IncAttr>(_attrs);
  inc.value = (_curr == _begin) ? 1 : 0;

  auto& term = std::get<TermAttr>(_attrs);
  term.value = ViewCast<byte_type>(*_curr++);
  return true;
}

bool SolrSynonymsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = data.size();

  if (const auto it = _synonyms.find(data); it == _synonyms.end()) {
    _holder = data;
    _begin = _curr = &_holder;
    _end = _curr + 1;
  } else {
    _begin = _curr = it->second->data();
    _end = _curr + it->second->size();
  }

  return true;
}

}  // namespace irs::analysis
