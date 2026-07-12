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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <string_view>
#include <utility>

#include "basics/exceptions.h"
#include "basics/log.h"
#include "iresearch/analysis/token_attributes.hpp"

namespace irs::analysis {
namespace {

SolrSynonymsTokenizer::SynonymsList SplitLine(const std::string_view line,
                                              const size_t line_number) {
  std::vector<std::string_view> outputs(absl::StrSplit(line, ','));

  for (auto& s : outputs) {
    s = absl::StripAsciiWhitespace(s);
    if (s.empty()) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                "solr_synonyms: failed to parse synonyms: Failed parse line ",
                line_number);
    }
  }

  absl::c_sort(outputs);
  outputs.erase(std::unique(outputs.begin(), outputs.end()), outputs.end());

  return outputs;
}

}  // namespace

SolrSynonymsTokenizer::SynonymsLines SolrSynonymsTokenizer::ParseSynonymsLines(
  std::string_view input) {
  SynonymsLines synonyms_lines;

  std::vector<std::string_view> lines = absl::StrSplit(input, '\n');
  size_t line_number{};
  for (const auto& line : lines) {
    line_number++;
    if (line.empty() || line[0] == '#') {
      continue;
    }
    std::vector<std::string_view> sides = absl::StrSplit(line, "=>");

    SynonymsLine synonyms_line;

    if (sides.size() > 1) {
      if (sides.size() != 2) {
        SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                  "solr_synonyms: failed to parse synonyms: More than one "
                  "explicit mapping specified on the line ",
                  line_number);
      }

      synonyms_line.in = SplitLine(sides[0], line_number);
      synonyms_line.out = SplitLine(sides[1], line_number);
    } else {
      synonyms_line.out = SplitLine(sides[0], line_number);
    }

    synonyms_lines.push_back(std::move(synonyms_line));
  }

  return synonyms_lines;
}

SolrSynonymsTokenizer::SynonymsMap SolrSynonymsTokenizer::Parse(
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
  std::shared_ptr<const State> state) noexcept
  : _state(std::move(state)) {
  SDB_ASSERT(_state);
}

std::shared_ptr<const SolrSynonymsTokenizer::State>
SolrSynonymsTokenizer::MakeState(std::string text) {
  auto state = std::make_shared<State>();

  // Order matters: views in `lines`/`synonyms` point into `text`, and
  // `synonyms`'s value pointers reference SynonymsLine elements in `lines`.
  // Populate each buffer before deriving views from it.
  state->text = std::move(text);
  state->lines = ParseSynonymsLines(state->text);
  state->synonyms = Parse(state->lines);

  return state;
}

Analyzer::ptr SolrSynonymsTokenizer::Make(Options opts) {
  return std::make_unique<SolrSynonymsTokenizer>(
    MakeState(std::move(opts.synonyms_text)));
}

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
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = data.size();

  const auto& synonyms = _state->synonyms;
  if (const auto it = synonyms.find(data); it == synonyms.end()) {
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
