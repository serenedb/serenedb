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

#include "edge_ngram_tokenizer.hpp"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/serializer.h>
#include <vpack/slice.h>

#include <limits>
#include <optional>
#include <string_view>

#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs {
namespace analysis {
namespace {

struct VPackOptionsTemp {
  size_t min = 1;
  std::optional<size_t> max;
  bool preserveOriginal = false;
};

bool ParseVPackOptions(const vpack::Slice& slice,
                       EdgeNGramTokenizer::Options& options) {
  if (!slice.isObject()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for edge_ngram_token_stream is not an object");
    return false;
  }

  VPackOptionsTemp temp;
  auto r = vpack::ReadObjectNothrow(slice, temp,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    SDB_WARN(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Failed to parse edge_ngram_token_stream options: ", r.errorMessage());
    return false;
  }

  options.min_gram = temp.min;
  if (temp.max) {
    if (temp.min > *temp.max) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "'min' must be <= 'max' for edge_ngram_token_stream");
      return false;
    }
    options.max_gram = *temp.max;
    options.max_gram_set = true;
  } else {
    options.max_gram = std::numeric_limits<size_t>::max();
    options.max_gram_set = false;
  }
  options.preserve_original = temp.preserveOriginal;

  return true;
}

Analyzer::ptr MakeVPack(const vpack::Slice& args) {
  EdgeNGramTokenizer::Options options;
  if (ParseVPackOptions(args, options)) {
    return EdgeNGramTokenizer::make(std::move(options));
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  EdgeNGramTokenizer::Options options;
  if (ParseVPackOptions(slice, options)) {
    options.min_gram = std::max<size_t>(options.min_gram, 1);
    if (options.max_gram_set) {
      options.max_gram = std::max(options.max_gram, options.min_gram);
    }
    vpack::ObjectBuilder object(builder);
    builder->add("min", static_cast<uint64_t>(options.min_gram));
    if (options.max_gram_set) {
      builder->add("max", static_cast<uint64_t>(options.max_gram));
    }
    builder->add("preserveOriginal", options.preserve_original);
    return true;
  }
  return false;
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

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while constructing edge_ngram_token_stream");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Caught error '", ex.what(),
              "' while constructing edge_ngram_token_stream "
              "from JSON");
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing edge_ngram_token_stream "
              "from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing edge_ngram_token_stream");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Caught error '", ex.what(),
                           "' while normalizing edge_ngram_token_stream "
                           "from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing edge_ngram_token_stream "
              "from JSON");
  }
  return false;
}

}  // namespace

Analyzer::ptr EdgeNGramTokenizer::make(Options&& options) {
  options.min_gram = std::max<size_t>(options.min_gram, 1);
  if (options.max_gram_set) {
    options.max_gram = std::max(options.max_gram, options.min_gram);
  }
  return std::make_unique<EdgeNGramTokenizer>(std::move(options));
}

EdgeNGramTokenizer::EdgeNGramTokenizer(Options&& options) noexcept
  : _options{std::move(options)}, _term_eof{true} {}

EdgeNGramTokenizer::~EdgeNGramTokenizer() = default;

void EdgeNGramTokenizer::init() {
  REGISTER_ANALYZER_JSON(EdgeNGramTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(EdgeNGramTokenizer, MakeVPack, NormalizeVPackConfig);
}

Attribute* EdgeNGramTokenizer::GetMutable(TypeInfo::type_id type) noexcept {
  return irs::GetMutable(_attrs, type);
}

bool EdgeNGramTokenizer::reset(std::string_view data) {
  _data = ViewCast<byte_type>(data);
  _term_eof = data.empty();
  _code_points = 0;
  return true;
}

bool EdgeNGramTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  const auto* begin = _data.data();
  const auto* end = _data.data() + _data.size();
  auto& inc = std::get<IncAttr>(_attrs);

  // if there are no ngrams yet then a new word started
  if (_code_points == 0) {
    _prefix_end = begin;
    inc.value = 1;
    // find the first ngram > min
    do {
      _prefix_end = utf8_utils::Next(_prefix_end, end);
    } while (++_code_points < _options.min_gram && _prefix_end != end);
  } else {
    // not first ngram in a word
    inc.value = 0;  // staying on the current pos
    _prefix_end = utf8_utils::Next(_prefix_end, end);
    ++_code_points;
  }

  // if a word has finished
  bool finished = (_prefix_end == end);

  // if length > max
  if (_options.max_gram_set && _code_points > _options.max_gram) {
    // no unwatched ngrams in a word
    finished = true;
    if (_options.preserve_original) {
      _prefix_end = end;
    } else {
      _code_points = 0;
      _term_eof = true;
      return false;
    }
  }

  // if length >= min or preserveOriginal
  if (_code_points >= _options.min_gram || _options.preserve_original) {
    const auto byte_len = static_cast<uint32_t>(_prefix_end - begin);
    std::get<TermAttr>(_attrs).value = {begin, byte_len};

    auto& offset = std::get<OffsAttr>(_attrs);
    offset.start = 0;
    offset.end = byte_len;

    if (finished) {
      _code_points = 0;
      _term_eof = true;
    }
    return true;
  }

  if (finished) {
    _code_points = 0;
    _term_eof = true;
  }
  return false;
}

}  // namespace analysis
}  // namespace irs
