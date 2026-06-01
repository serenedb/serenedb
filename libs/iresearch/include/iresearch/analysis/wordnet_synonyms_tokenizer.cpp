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
#include <vpack/builder.h>
#include <vpack/parser.h>

#include <string_view>
#include <utility>

#include "basics/log.h"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/string.hpp"

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

sdb::ResultOr<std::vector<std::string_view>> ParseParams(
  const std::string_view line) {
  std::string_view params;

  if (!RegexWordnet(line, &params)) {
    return std::unexpected<sdb::Result>{std::in_place,
                                        sdb::ERROR_BAD_PARAMETER};
  }

  std::vector<std::string_view> outputs = absl::StrSplit(params, ',');
  if (outputs.size() < kWordnetMinCountParams ||
      outputs.size() > kWordnetMaxCountParams) {
    return std::unexpected<sdb::Result>{std::in_place,
                                        sdb::ERROR_BAD_PARAMETER};
  }
  return outputs;
}

}  // namespace

sdb::ResultOr<WordnetSynonymsTokenizer::SynonymsMap>
WordnetSynonymsTokenizer::Parse(const std::string_view input) {
  std::vector<std::string_view> lines = absl::StrSplit(input, '\n');

  size_t line_number{};

  SynonymsMap mapping;

  for (const auto& line : lines) {
    line_number++;
    if (line.empty()) {
      continue;
    }

    const auto params = ParseParams(line);
    if (params.error().is(sdb::ERROR_BAD_PARAMETER)) {
      return std::unexpected<sdb::Result>{std::in_place,
                                          sdb::ERROR_BAD_PARAMETER,
                                          "Failed parse line ", line_number};
    }

    const std::string_view syn_set_id = (*params)[0];
    const std::string_view raw_synonym = (*params)[2];

    if (raw_synonym.size() < 3 || raw_synonym.front() != '\'' ||
        raw_synonym.back() != '\'') {
      return std::unexpected<sdb::Result>{std::in_place,
                                          sdb::ERROR_BAD_PARAMETER,
                                          "Failed parse line ", line_number};
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

sdb::ResultOr<std::shared_ptr<const WordnetSynonymsTokenizer::State>>
WordnetSynonymsTokenizer::MakeState(std::string text) {
  auto state = std::make_shared<State>();

  // Order matters: views in `mapping`'s values point into `text`, so the
  // backing buffer is populated before parsing builds the views over it.
  state->text = std::move(text);

  auto mapping = Parse(state->text);
  if (!mapping) {
    return std::unexpected{std::move(mapping.error())};
  }
  state->mapping = std::move(*mapping);

  return state;
}

namespace {

constexpr std::string_view kSynonymsField = "synonyms";

bool ParseVPackOptions(const vpack::Slice slice, std::string& synonyms_text) {
  if (!slice.isObject()) {
    SDB_ERROR(IRESEARCH, "Slice for wordnet_synonyms is not an object");
    return false;
  }

  const auto field = slice.get(kSynonymsField);
  if (field.isNone() || !field.isString()) {
    SDB_ERROR(IRESEARCH,
              "Missing or non-string 'synonyms' while constructing "
              "wordnet_synonyms from VPack arguments");
    return false;
  }

  synonyms_text.assign(field.stringView());
  return true;
}

Analyzer::ptr MakeVPack(vpack::Slice slice) {
  std::string synonyms_text;
  if (!ParseVPackOptions(slice, synonyms_text)) {
    return nullptr;
  }
  auto state = WordnetSynonymsTokenizer::MakeState(std::move(synonyms_text));
  if (!state) {
    SDB_ERROR(IRESEARCH,
              "Failed to parse synonyms while constructing wordnet_synonyms: ",
              state.error().errorMessage());
    return nullptr;
  }
  return std::make_unique<WordnetSynonymsTokenizer>(std::move(*state));
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR(IRESEARCH,
                "Null arguments while constructing wordnet_synonyms");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(IRESEARCH, "Caught error '", ex.what(),
              "' while constructing wordnet_synonyms from JSON");
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught error while constructing wordnet_synonyms from JSON");
  }
  return nullptr;
}

bool NormalizeVPackConfig(vpack::Slice slice, vpack::Builder* builder) {
  std::string synonyms_text;
  if (!ParseVPackOptions(slice, synonyms_text)) {
    return false;
  }
  // Fail at DDL time on a malformed synonyms text rather than letting the
  // dictionary be created and surfacing the error later at use time.
  auto state = WordnetSynonymsTokenizer::MakeState(synonyms_text);
  if (!state) {
    SDB_ERROR(IRESEARCH,
              "Failed to parse synonyms while normalizing wordnet_synonyms: ",
              state.error().errorMessage());
    return false;
  }
  vpack::ObjectBuilder object(builder);
  builder->add(kSynonymsField, synonyms_text);
  return true;
}

bool NormalizeVPackConfig(std::string_view args, std::string& config) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  vpack::Builder builder;
  if (NormalizeVPackConfig(slice, &builder)) {
    config.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
    return true;
  }
  return false;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR(IRESEARCH, "Null arguments while normalizing wordnet_synonyms");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(IRESEARCH, "Caught error '", ex.what(),
              "' while normalizing wordnet_synonyms from JSON");
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught error while normalizing wordnet_synonyms from JSON");
  }
  return false;
}

}  // namespace

void WordnetSynonymsTokenizer::init() {
  REGISTER_ANALYZER_VPACK(WordnetSynonymsTokenizer, MakeVPack,
                          NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(WordnetSynonymsTokenizer, MakeJson,
                         NormalizeJsonConfig);
}

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
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = data.size();

  const auto& mapping = _state->mapping;
  if (const auto it = mapping.find(data); it == mapping.end()) {
    _term_exists = false;
  } else {
    _begin = _curr = it->second.data();
    _end = _curr + it->second.size();

    _term_exists = true;
  }

  return true;
}

}  // namespace irs::analysis
