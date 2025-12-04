#include "synonyms_tokenizer.hpp"

#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>

namespace irs::analysis {

SynonymsTokenizer::SynonymsTokenizer(SynonymsTokenizer::synonyms_map&& synonyms)
  : _synonyms(std::move(synonyms)) {}

bool SynonymsTokenizer::next() {
  if (_term_eof) {
    return false;
  }
  _term_eof = true;
  return true;
}

bool SynonymsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = uint32_t(data.size());
  auto& term = std::get<TermAttr>(_attrs);
  term.value = ViewCast<byte_type>(data);

  const bool contains = _synonyms.contains(data);
  _term_eof = contains;

  if (contains) {
    const auto synonyms = _synonyms[data];
    if (data != (*synonyms)[0]) {
      auto& inc = std::get<IncAttr>(_attrs);
      inc.value = 0;
    }
  }

  return true;
}

}  // namespace irs::analysis