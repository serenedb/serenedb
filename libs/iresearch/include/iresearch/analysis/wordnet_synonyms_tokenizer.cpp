#include "wordnet_synonyms_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <string_view>

namespace irs::analysis {

WordnetSynonymsTokenizer::WordnetSynonymsTokenizer(
  WordnetSynonymsTokenizer::SynonymsGroup&& groups,
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
