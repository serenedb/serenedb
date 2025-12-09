#include "wordnet_synonyms_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <string_view>
#include <utility>

namespace irs::analysis {

sdb::ResultOr<WordnetSynonymsTokenizer::WordnetInput>
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

    const std::string_view syn_set_id = line.substr(2, 9);

    // I couldn't find explicit guarantees that SynSet IDs must appear
    // sequentially, but popular implementations rely on this assumption.
    if (last_syn_set_id != syn_set_id) {
      synset.push_back(syn_set_id);
    }

    const size_t start = line.find('\'') + 1;
    const size_t end = line.rfind('\'');

    if (start == std::string::npos || end == std::string::npos ||
        start >= end) {
      return std::unexpected<sdb::Result>{std::in_place,
                                          sdb::ERROR_VALIDATION_BAD_PARAMETER,
                                          "Failed parse line ", line_number};
    }

    std::string synonym =
      absl::StrReplaceAll(line.substr(start, end - start), {{"''", "'"}});

    if (synonym.empty()) {
      return std::unexpected<sdb::Result>{std::in_place,
                                          sdb::ERROR_VALIDATION_BAD_PARAMETER,
                                          "Failed parse line ", line_number};
    }

    mapping.emplace(std::move(synonym), synset.back());

    last_syn_set_id = syn_set_id;
  }
  return WordnetInput{.groups = std::move(synset),
                      .mapping = std::move(mapping)};
}

WordnetSynonymsTokenizer::WordnetSynonymsTokenizer(
  WordnetSynonymsTokenizer::SynonymsGroups&& groups,
  WordnetSynonymsTokenizer::SynonymsMap&& mapping)
  : _groups(std::move(groups)), _mapping(std::move(mapping)) {}

bool WordnetSynonymsTokenizer::next() {
  if (!_term_exists) {
    return false;
  }
  _term_exists = false;
  return true;
}

bool WordnetSynonymsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = data.size();

  if (const auto it = _mapping.find(data); it == _mapping.end()) {
    _term_exists = false;
  } else {
    auto& term = std::get<TermAttr>(_attrs);
    term.value = ViewCast<byte_type>(it->second);
    _term_exists = true;
  }

  return true;
}

}  // namespace irs::analysis
