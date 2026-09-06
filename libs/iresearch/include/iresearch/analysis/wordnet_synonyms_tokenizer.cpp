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
#include <absl/strings/str_split.h>

#include <cstring>
#include <duckdb/common/crypto/md5.hpp>
#include <string_view>
#include <utility>

#include "iresearch/analysis/token_batch.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr size_t kWordnetMinCountParams = 4;
constexpr size_t kWordnetMaxCountParams = 6;

bool RegexWordnet(const std::string_view input, std::string_view* result) {
  if (!input.starts_with("s(") || !input.ends_with(").")) {
    return false;
  }
  const auto params = input.substr(2, input.size() - 4);
  if (params.find(')') != std::string_view::npos) {
    return false;
  }
  *result = params;
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
  std::string& input) {
  size_t line_number{};

  SynonymsMap mapping;

  for (std::string_view line : absl::StrSplit(std::string_view{input}, '\n')) {
    line_number++;
    if (line.ends_with('\r')) {
      line.remove_suffix(1);
    }
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

    std::string_view synonym = raw_synonym.substr(1, raw_synonym.size() - 2);
    if (size_t quote = synonym.find("''"); quote != std::string_view::npos) {
      char* dst = input.data() + (synonym.data() - input.data());
      size_t size = quote + 1;
      std::string_view tail = synonym.substr(quote + 2);
      for (quote = tail.find("''"); quote != std::string_view::npos;
           quote = tail.find("''")) {
        std::memmove(dst + size, tail.data(), quote + 1);
        size += quote + 1;
        tail = tail.substr(quote + 2);
      }
      std::memmove(dst + size, tail.data(), tail.size());
      size += tail.size();
      synonym = {dst, size};
    }

    mapping[synonym].emplace_back(syn_set_id);
  }

  mapping.ForEachMapped([](SynonymsGroups& synset) {
    absl::c_sort(synset);
    synset.erase(std::unique(synset.begin(), synset.end()), synset.end());
    synset.shrink_to_fit();
  });

  return mapping;
}

WordnetSynonymsTokenizer::WordnetSynonymsTokenizer(
  duckdb::shared_ptr<const State> state) noexcept
  : _state(std::move(state)) {
  SDB_ASSERT(_state);
}

duckdb::unique_ptr<WordnetSynonymsTokenizer::State>
WordnetSynonymsTokenizer::MakeState(std::string text) {
  auto state = duckdb::make_uniq<State>();

  state->text = std::move(text);
  state->mapping = Parse(state->text);

  return state;
}

Tokenizer::ptr WordnetSynonymsTokenizer::Make(
  Options opts, duckdb::SharedObjectCache& cache) {
  duckdb::MD5Context digest;
  digest.Add(opts.synonyms_text);
  char hex[duckdb::MD5Context::MD5_HASH_LENGTH_TEXT];
  digest.FinishHex(hex);
  auto state = cache.GetOrBuild<State>(std::string_view{hex, sizeof(hex)}, [&] {
    return MakeState(std::move(opts.synonyms_text));
  });
  return std::make_unique<WordnetSynonymsTokenizer>(std::move(state));
}

template<TokenLayout Layout, typename Sink>
bool WordnetSynonymsTokenizer::DoFill(const duckdb::string_t& raw, Sink& sink) {
  if (const auto* groups = _state->mapping.Find(raw); groups) {
    for (const std::string_view group : *groups) {
      sink.template Emit<Layout>(MakeTermView(group));
    }
  }
  return true;
}

template class TypedTokenizer<WordnetSynonymsTokenizer>;
template class TypedTokenExpander<WordnetSynonymsTokenizer>;

}  // namespace irs::analysis
