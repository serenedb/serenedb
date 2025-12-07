#include "synonyms_tokenizer.hpp"

#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>

#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <string_view>

#include "maskings/parse_result.h"

namespace irs::analysis {

ParseResult<SynonymsTokenizer::synonyms_holder> SynonymsTokenizer::parse(
  std::string_view input) {
  synonyms_holder owner;

  std::vector<std::string_view> lines = absl::StrSplit(input, '\n');

  for (const auto& line : lines) {
    if (line.empty() || line[0] == '#')
      continue;
    std::vector<std::string_view> synonyms;
    for (std::string_view s : absl::StrSplit(line, ',')) {
      synonyms.push_back(absl::StripAsciiWhitespace(s));
    }

    owner.push_back(std::move(synonyms));
  }

  return ParseResult<synonyms_holder>(std::move(owner));
}

ParseResult<SynonymsTokenizer::synonyms_map> SynonymsTokenizer::parse(
  const synonyms_holder& holder) {
  synonyms_map result;
  for (const auto& line : holder) {
    for (std::string_view synonym : line) {
      result[synonym] = &line;
    }
  }
  return ParseResult<synonyms_map>(std::move(result));
}

SynonymsTokenizer::SynonymsTokenizer(SynonymsTokenizer::synonyms_map&& synonyms)
  : _synonyms(std::move(synonyms)) {}

bool SynonymsTokenizer::next() {
  if (_curr == _end) {
    return false;
  }

  auto& inc = std::get<IncAttr>(_attrs);
  inc.value = (_curr == _begin) ? 1 : 0;

  auto& term = std::get<TermAttr>(_attrs);
  term.value = ViewCast<byte_type>(*_curr++);
  return true;
}

bool SynonymsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = data.size();

  if (const auto it = _synonyms.find(data); it == _synonyms.end()) {
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
