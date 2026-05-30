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
#include <vpack/builder.h>
#include <vpack/parser.h>

#include <expected>
#include <string_view>
#include <utility>

#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/result.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/string.hpp"

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
    if (line.empty() || line[0] == '#') {
      continue;
    }
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
  std::shared_ptr<const State> state) noexcept
  : _state(std::move(state)) {
  SDB_ASSERT(_state);
}

sdb::ResultOr<std::shared_ptr<const SolrSynonymsTokenizer::State>>
SolrSynonymsTokenizer::MakeState(std::string text) {
  auto state = std::make_shared<State>();

  // Order matters: views in `lines`/`synonyms` point into `text`, and
  // `synonyms`'s value pointers reference SynonymsLine elements in `lines`.
  // Populate each buffer before deriving views from it.
  state->text = std::move(text);

  auto lines = ParseSynonymsLines(state->text);
  if (!lines) {
    return std::unexpected{std::move(lines).error()};
  }
  state->lines = std::move(*lines);

  auto synonyms = Parse(state->lines);
  if (!synonyms) {
    return std::unexpected{std::move(synonyms).error()};
  }
  state->synonyms = std::move(*synonyms);

  return state;
}

namespace {

constexpr std::string_view kSynonymsField = "synonyms";

bool ParseVPackOptions(const vpack::Slice slice, std::string& synonyms_text) {
  if (!slice.isObject()) {
    SDB_ERROR(IRESEARCH,
              "Slice for solr_synonyms is not an object");
    return false;
  }

  const auto field = slice.get(kSynonymsField);
  if (field.isNone() || !field.isString()) {
    SDB_ERROR(IRESEARCH,
              "Missing or non-string 'synonyms' while constructing "
              "solr_synonyms from VPack arguments");
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
  auto state = SolrSynonymsTokenizer::MakeState(std::move(synonyms_text));
  if (!state) {
    SDB_ERROR(IRESEARCH,
              "Failed to parse synonyms while constructing solr_synonyms: ",
              state.error().errorMessage());
    return nullptr;
  }
  return std::make_unique<SolrSynonymsTokenizer>(std::move(*state));
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR(IRESEARCH,
                "Null arguments while constructing solr_synonyms");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(IRESEARCH, "Caught error '", ex.what(),
              "' while constructing solr_synonyms from JSON");
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught error while constructing solr_synonyms from JSON");
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
  auto state = SolrSynonymsTokenizer::MakeState(synonyms_text);
  if (!state) {
    SDB_ERROR(IRESEARCH,
              "Failed to parse synonyms while normalizing solr_synonyms: ",
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
      SDB_ERROR(IRESEARCH,
                "Null arguments while normalizing solr_synonyms");
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
              "' while normalizing solr_synonyms from JSON");
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              "Caught error while normalizing solr_synonyms from JSON");
  }
  return false;
}

}  // namespace

void SolrSynonymsTokenizer::init() {
  REGISTER_ANALYZER_VPACK(SolrSynonymsTokenizer, MakeVPack,
                          NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(SolrSynonymsTokenizer, MakeJson, NormalizeJsonConfig);
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
