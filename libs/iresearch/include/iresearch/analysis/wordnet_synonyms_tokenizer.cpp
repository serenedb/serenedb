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
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <regex>
#include <string_view>
#include <utility>

namespace irs::analysis {

namespace {

constexpr static size_t kWordnetCountParams = 6;

const std::regex kWordnetPattern(R"(s\(([^)]*)\)\.)");

bool RegexWordnet(const std::string_view s_list,
                  std::match_results<std::string_view::iterator>& list_parts) {
  if (s_list.length() <= 2) {
    return false;
  }

  return std::regex_match(s_list.begin(), s_list.end(), list_parts,
                          kWordnetPattern);
}

sdb::ResultOr<std::vector<std::string_view>> ParseParams(
  const std::string_view line) {
  std::match_results<std::string_view::iterator> match;

  if (!RegexWordnet(line, match)) {
    return std::unexpected<sdb::Result>{std::in_place,
                                        sdb::ERROR_BAD_PARAMETER};
  }
  const std::string_view params(match[1].first, match[1].second);

  std::vector<std::string_view> outputs(absl::StrSplit(params, ','));
  if (outputs.size() != kWordnetCountParams) {
    return std::unexpected<sdb::Result>{std::in_place,
                                        sdb::ERROR_BAD_PARAMETER};
  }
  return outputs;
}
}  // namespace

sdb::ResultOr<WordnetSynonymsTokenizer::SynonymsMap>
WordnetSynonymsTokenizer::Parse(const std::string_view input) {
  std::vector<std::string_view> lines = absl::StrSplit(input, '\n');

  std::string_view last_syn_set_id = "";
  size_t line_number{};

  SynonymsGroups synset;
  SynonymsMap mapping;

  for (const auto& line : lines) {
    line_number++;
    if (line.empty())
      continue;

    const auto params = ParseParams(line);
    if (params.error().is(sdb::ERROR_BAD_PARAMETER)) {
      return std::unexpected<sdb::Result>{std::in_place,
                                          sdb::ERROR_BAD_PARAMETER,
                                          "Failed parse line ", line_number};
    }

    const std::string_view syn_set_id = (*params)[0];

    // I couldn't find explicit guarantees that SynSet IDs must appear
    // sequentially, but popular implementations rely on this assumption.
    if (last_syn_set_id != syn_set_id) {
      synset.push_back(syn_set_id);
    }

    const std::string_view raw_synonym = (*params)[2];

    if (raw_synonym.size() < 3 || raw_synonym.front() != '\'' ||
        raw_synonym.back() != '\'') {
      return std::unexpected<sdb::Result>{std::in_place,
                                          sdb::ERROR_BAD_PARAMETER,
                                          "Failed parse line ", line_number};
    }

    std::string synonym = absl::StrReplaceAll(
      raw_synonym.substr(1, raw_synonym.size() - 2), {{"''", "'"}});

    if (synonym.empty()) {
      return std::unexpected<sdb::Result>{std::in_place,
                                          sdb::ERROR_BAD_PARAMETER,
                                          "Failed parse line ", line_number};
    }

    mapping[synonym].push_back(synset.back());

    last_syn_set_id = syn_set_id;
  }
  return mapping;
}

WordnetSynonymsTokenizer::WordnetSynonymsTokenizer(
  WordnetSynonymsTokenizer::SynonymsMap&& mapping)
  : _mapping(std::move(mapping)) {}

bool WordnetSynonymsTokenizer::next() {
  if (!_term_exists) {
    return false;
  }

  auto& term = std::get<TermAttr>(_attrs);
  term.value = ViewCast<byte_type>(*_curr);
  _curr++;

  if (_curr == _end) {
    _term_exists = false;
  }

  return true;
}

bool WordnetSynonymsTokenizer::reset(const std::string_view data) {
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = data.size();

  if (const auto it = _mapping.find(data); it == _mapping.end()) {
    _term_exists = false;
  } else {
    _begin = _curr = it->second.data();
    _end = _curr + it->second.size();

    _term_exists = true;
  }

  return _term_exists;
}

}  // namespace irs::analysis
