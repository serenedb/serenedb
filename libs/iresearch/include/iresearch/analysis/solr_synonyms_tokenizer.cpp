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

#include <duckdb/common/crypto/md5.hpp>
#include <string_view>
#include <utility>

#include "iresearch/analysis/token_batch.hpp"
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

  size_t line_number{};
  for (std::string_view line : absl::StrSplit(input, '\n')) {
    line_number++;
    if (line.ends_with('\r')) {
      line.remove_suffix(1);
    }
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
  auto visit = [&](auto&& visitor) -> void {
    for (const auto& line : lines) {
      auto& list = line.in.empty() ? line.out : line.in;
      for (const std::string_view synonym : list) {
        visitor(synonym, line.out);
      }
    }
  };
  size_t inline_keys = 0;
  size_t long_keys = 0;
  visit([&](const std::string_view synonym, const auto&) {
    ++(synonym.size() <= duckdb::string_t::INLINE_LENGTH ? inline_keys
                                                         : long_keys);
  });
  SynonymsMap result;
  result.Reserve(inline_keys, long_keys);
  visit([&](const std::string_view synonym, const auto& list) {
    result[synonym] = &list;
  });
  return result;
}

SolrSynonymsTokenizer::SolrSynonymsTokenizer(
  duckdb::shared_ptr<const State> state) noexcept
  : _state(std::move(state)) {
  SDB_ASSERT(_state);
}

duckdb::unique_ptr<SolrSynonymsTokenizer::State>
SolrSynonymsTokenizer::MakeState(std::string text) {
  auto state = duckdb::make_uniq<State>();

  state->text = std::move(text);
  state->lines = ParseSynonymsLines(state->text);
  state->synonyms = Parse(state->lines);

  return state;
}

Tokenizer::ptr SolrSynonymsTokenizer::Make(Options opts,
                                           duckdb::SharedObjectCache& cache) {
  duckdb::MD5Context digest;
  digest.Add(opts.synonyms_text);
  char hex[duckdb::MD5Context::MD5_HASH_LENGTH_TEXT];
  digest.FinishHex(hex);
  auto state = cache.GetOrBuild<State>(std::string_view{hex, sizeof(hex)}, [&] {
    return MakeState(std::move(opts.synonyms_text));
  });
  return std::make_unique<SolrSynonymsTokenizer>(std::move(state));
}

template<TokenLayout Layout, typename Sink>
bool SolrSynonymsTokenizer::DoFill(const duckdb::string_t& raw, Sink& sink) {
  if (const auto* list = _state->synonyms.Find(raw); list && *list) {
    for (const std::string_view synonym : **list) {
      sink.template Emit<Layout>(MakeTermView(synonym), 1);
    }
  } else {
    sink.template Emit<Layout>(raw, 1);
  }
  return true;
}

template class TypedTokenizer<SolrSynonymsTokenizer>;
template class TypedTokenExpander<SolrSynonymsTokenizer>;

}  // namespace irs::analysis
