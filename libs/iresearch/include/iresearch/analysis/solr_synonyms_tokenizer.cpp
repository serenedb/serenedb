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

#include "basics/log.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

SolrSynonymsTokenizer::SynonymsList SplitLine(const std::string_view line,
                                              const size_t line_number) {
  std::vector<std::string_view> outputs(absl::StrSplit(line, ','));

  for (auto& s : outputs) {
    s = absl::StripAsciiWhitespace(s);
    if (s.empty()) {
      THROW_SQL_ERROR(
        ERR_MSG("solr_synonyms: failed to parse synonyms: Failed parse line ",
                line_number));
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
        THROW_SQL_ERROR(
          ERR_MSG("solr_synonyms: failed to parse synonyms: More than one "
                  "explicit mapping specified on the line ",
                  line_number));
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
  for (const auto& [key, list] : state->synonyms) {
    state->prefilter.Add(key);
  }

  return state;
}

Tokenizer::ptr SolrSynonymsTokenizer::Make(Options opts) {
  return std::make_unique<SolrSynonymsTokenizer>(
    MakeState(std::move(opts.synonyms_text)));
}

bool SolrSynonymsTokenizer::Bind(std::string_view value) {
  _input_size = static_cast<uint32_t>(value.size());

  if (const auto* list = Lookup(value); list == nullptr) {
    _holder = value;
    _curr = &_holder;
    _end = _curr + 1;
  } else {
    _curr = list->data();
    _end = _curr + list->size();
  }

  return true;
}

template<TokenLayout Layout>
bool SolrSynonymsTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  if (!Bind(value)) {
    return false;
  }
  auto& buf = sink.buf;
  const auto input_end = _input_size;
  size_t remaining = static_cast<size_t>(_end - _curr);
  while (remaining != 0) {
    const auto slots = sink.Next(remaining);
    const auto first = static_cast<uint32_t>(slots.data() - buf.terms);
    for (size_t j = 0; j < slots.size(); ++j) {
      const auto& synonym = *_curr++;
      slots[j] =
        duckdb::string_t{synonym.data(), static_cast<uint32_t>(synonym.size())};
      if constexpr (Layout != TokenLayout::Terms) {
        buf.pos[first + j] = 1;
      }
      if constexpr (Layout == TokenLayout::TermsPosOffs) {
        buf.offs_start[first + j] = 0;
        buf.offs_end[first + j] = input_end;
      }
    }
    remaining -= slots.size();
  }
  return true;
}

template class TypedTokenizer<SolrSynonymsTokenizer>;

}  // namespace irs::analysis
