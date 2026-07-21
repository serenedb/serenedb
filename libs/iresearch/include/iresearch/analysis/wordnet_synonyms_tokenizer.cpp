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

#include "wordnet_synonyms_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <re2/re2.h>

#include <string_view>
#include <utility>

#include "basics/log.h"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr size_t kWordnetMinCountParams = 4;
constexpr size_t kWordnetMaxCountParams = 6;

const RE2 kWordnetPattern(R"(s\(([^)]*)\)\.)");

bool RegexWordnet(const std::string_view input, std::string_view* result) {
  if (input.length() <= 2) {
    return false;
  }

  if (!RE2::FullMatch(input, kWordnetPattern, result)) {
    return false;
  }

  return true;
}

std::vector<std::string_view> ParseParams(const std::string_view line,
                                          const size_t line_number) {
  std::string_view params;

  if (!RegexWordnet(line, &params)) {
    THROW_SQL_ERROR(
      ERR_MSG("wordnet_synonyms: failed to parse synonyms: Failed parse line ",
              line_number));
  }

  std::vector<std::string_view> outputs = absl::StrSplit(params, ',');
  if (outputs.size() < kWordnetMinCountParams ||
      outputs.size() > kWordnetMaxCountParams) {
    THROW_SQL_ERROR(
      ERR_MSG("wordnet_synonyms: failed to parse synonyms: Failed parse line ",
              line_number));
  }
  return outputs;
}

}  // namespace

WordnetSynonymsTokenizer::SynonymsMap WordnetSynonymsTokenizer::Parse(
  const std::string_view input) {
  std::vector<std::string_view> lines = absl::StrSplit(input, '\n');

  size_t line_number{};

  SynonymsMap mapping;

  for (const auto& line : lines) {
    line_number++;
    if (line.empty()) {
      continue;
    }

    std::vector<std::string_view> params = ParseParams(line, line_number);

    const std::string_view syn_set_id = params[0];
    const std::string_view raw_synonym = params[2];

    if (raw_synonym.size() < 3 || raw_synonym.front() != '\'' ||
        raw_synonym.back() != '\'') {
      THROW_SQL_ERROR(
        ERR_MSG("wordnet_synonyms: failed to parse synonyms: Failed parse "
                "line ",
                line_number));
    }

    std::string synonym = absl::StrReplaceAll(
      raw_synonym.substr(1, raw_synonym.size() - 2), {{"''", "'"}});

    mapping[synonym].push_back(syn_set_id);
  }

  for (auto& [word, synset] : mapping) {
    absl::c_sort(synset);
    synset.erase(std::unique(synset.begin(), synset.end()), synset.end());
    synset.shrink_to_fit();
  }

  return mapping;
}

WordnetSynonymsTokenizer::WordnetSynonymsTokenizer(
  std::shared_ptr<const State> state) noexcept
  : _state(std::move(state)) {
  SDB_ASSERT(_state);
}

std::shared_ptr<const WordnetSynonymsTokenizer::State>
WordnetSynonymsTokenizer::MakeState(std::string text) {
  auto state = std::make_shared<State>();

  // Order matters: views in `mapping`'s values point into `text`, so the
  // backing buffer is populated before parsing builds the views over it.
  state->text = std::move(text);
  state->mapping = Parse(state->text);
  for (const auto& [key, groups] : state->mapping) {
    state->prefilter.Add(key);
  }

  return state;
}

Tokenizer::ptr WordnetSynonymsTokenizer::Make(Options opts) {
  return std::make_unique<WordnetSynonymsTokenizer>(
    MakeState(std::move(opts.synonyms_text)));
}

bool WordnetSynonymsTokenizer::Bind(const std::string_view value) {
  _input_size = static_cast<uint32_t>(value.size());

  if (const auto* groups = Lookup(value); groups == nullptr) {
    _term_exists = false;
  } else {
    _curr = groups->data();
    _end = _curr + groups->size();

    _term_exists = true;
  }

  return true;
}

template<TokenLayout Layout>
bool WordnetSynonymsTokenizer::DoFill(std::string_view value,
                                      TokenEmitter& sink) {
  if (!Bind(value)) {
    return false;
  }
  auto& buf = sink.buf;
  if (!_term_exists) {
    return true;
  }
  const auto input_end = _input_size;
  size_t remaining = static_cast<size_t>(_end - _curr);
  while (remaining != 0) {
    const auto slots = sink.Next(remaining);
    const auto first = static_cast<uint32_t>(slots.data() - buf.terms);
    for (size_t j = 0; j < slots.size(); ++j) {
      const auto& synonym = *_curr++;
      slots[j] =
        duckdb::string_t{synonym.data(), static_cast<uint32_t>(synonym.size())};
      if constexpr (Layout == TokenLayout::TermsPosOffs) {
        buf.offs_start[first + j] = 0;
        buf.offs_end[first + j] = input_end;
      }
    }
    remaining -= slots.size();
  }
  _term_exists = false;
  return true;
}

template class TypedTokenizer<WordnetSynonymsTokenizer>;

}  // namespace irs::analysis
