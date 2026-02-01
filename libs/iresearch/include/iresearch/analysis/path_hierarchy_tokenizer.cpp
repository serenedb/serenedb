////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "path_hierarchy_tokenizer.hpp"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>
#include <vector>

#include "basics/logger/logger.h"
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {
namespace {

bool ParseVPackOptions(const vpack::Slice slice,
                       PathHierarchyTokenizer::OptionsT& options) {
  if (!slice.isObject()) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Slice for path_hierarchy_token_stream is not an object or string");
    return false;
  }

  bool has_delimiter = false;
  {
    auto it = slice.get("delimiter");
    if (!it.isNone()) {
      if (!it.isString()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                 "Invalid type 'delimiter' (string expected) for "
                 "path_hierarchy_token_stream from VPack arguments");
        return false;
      }
      std::string delimiter_str = it.copyString();
      SDB_ASSERT(delimiter_str.size() == 1);
      options.delimiter = delimiter_str[0];
      has_delimiter = true;
    }
  }

  bool has_replacement = false;
  {
    auto it = slice.get("replacement");
    if (!it.isNone()) {
      if (!it.isString()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                 "Invalid type 'replacement' (string expected) for "
                 "path_hierarchy_token_stream from VPack arguments");
        return false;
      }
      std::string replacement_str = it.copyString();
      SDB_ASSERT(replacement_str.size() == 1);
      options.replacement = replacement_str[0];
      has_replacement = true;
    }
  }

  if (has_delimiter && !has_replacement) {
    options.replacement = options.delimiter;
  }

  {
    auto it = slice.get("buffer_size");
    if (!it.isNone()) {
      if (!it.isNumber()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                 "Invalid type 'buffer_size' (number expected) for "
                 "path_hierarchy_token_stream from VPack arguments");
        return false;
      }
      options.buffer_size = it.getNumber<decltype(options.buffer_size)>();
    }
  }

  {
    auto it = slice.get("reverse");
    if (!it.isNone()) {
      if (!it.isBool()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                 "Invalid type 'reverse' (bool expected) for "
                 "path_hierarchy_token_stream from VPack arguments");
        return false;
      }
      options.reverse = it.getBool();
    }
  }

  {
    auto it = slice.get("skip");
    if (!it.isNone()) {
      if (!it.isNumber()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                 "Invalid type 'skip' (number expected) for "
                 "path_hierarchy_token_stream from VPack arguments");
        return false;
      }
      options.skip = it.getNumber<decltype(options.skip)>();
    }
  }

  return true;
}

Analyzer::ptr MakeVPack(const vpack::Slice& args) {
  PathHierarchyTokenizer::OptionsT options;
  if (ParseVPackOptions(args, options)) {
    return std::make_unique<PathHierarchyTokenizer>(options);
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Null arguments while constructing path_hierarchy_token_stream");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Caught error '", ex.what(),
                           "' while constructing path_hierarchy_token_stream "
                           "from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing path_hierarchy_token_stream "
              "from JSON");
  }
  return nullptr;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  PathHierarchyTokenizer::OptionsT options;
  if (ParseVPackOptions(slice, options)) {
    vpack::ObjectBuilder object(builder);
    builder->add("delimiter", std::string(1, options.delimiter));
    builder->add("replacement", std::string(1, options.replacement));
    builder->add("buffer_size", options.buffer_size);
    builder->add("reverse", options.reverse);
    builder->add("skip", options.skip);
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

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing path_hierarchy_token_stream");
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
                           "' while normalizing path_hierarchy_token_stream "
                           "from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing path_hierarchy_token_stream "
              "from JSON");
  }
  return false;
}

}  // namespace

struct PathHierarchyTokenizer::StateT {
  std::string data;                     // input text to tokenize
  char delimiter;                       // path separator
  char replacement;                     // replacement character for delimiter
  bool reverse;                         // reverse mode for domains
  size_t skip;                          // tokens to skip
  std::vector<size_t> delim_positions;  // positions of all delimiters
  size_t current_token{0};              // current token index
  size_t num_tokens{0};                 // number of tokens

  void findDelimiters() {
    delim_positions.clear();
    if (data.empty()) {
      return;
    }

    size_t pos = 0;
    while ((pos = data.find(delimiter, pos)) != std::string::npos) {
      delim_positions.emplace_back(pos);
      ++pos;
    }
  }
};

void PathHierarchyTokenizer::StateDeleterT::operator()(
  StateT* state) const noexcept {
  delete state;
}

PathHierarchyTokenizer::PathHierarchyTokenizer(const OptionsT& options)
  : _term_eof(true), _options(options) {
  if (_options.buffer_size > 0) {
    _replace_buffer.reserve(_options.buffer_size);
  }
}

void PathHierarchyTokenizer::init() {
  REGISTER_ANALYZER_JSON(PathHierarchyTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(PathHierarchyTokenizer, MakeVPack,
                          NormalizeVPackConfig);
}

Attribute* PathHierarchyTokenizer::GetMutable(TypeInfo::type_id type) noexcept {
  return irs::GetMutable(_attrs, type);
}

bool PathHierarchyTokenizer::reset(std::string_view data) {
  _term_eof = false;

  auto state = std::make_unique<StateT>();
  state->data = data;
  state->delimiter = _options.delimiter;
  state->replacement = _options.replacement;
  state->reverse = _options.reverse;
  state->skip = _options.skip;
  state->current_token = 0;
  state->num_tokens = 0;

  state->data.assign(data.data(), data.size());

  state->findDelimiters();

  state->current_token = std::min(
    state->skip,
    state->reverse
      ? state->delim_positions.size() + 1
      : (state->delim_positions.empty() ? 1 : state->delim_positions.size()));

  // Calculate total number of tokens
  // For forward mode "/a/b/c" with delims at [0, 2, 4]: 3 tokens
  // For reverse mode "www.example.com" with delims at [3, 11]: 3 tokens (1 +
  // num_delims)
  if (state->reverse) {
    state->num_tokens = state->delim_positions.size() + 1;
  } else {
    state->num_tokens =
      state->delim_positions.empty() ? 1 : state->delim_positions.size();
  }

  _state.reset(state.release());

  return true;
}

bool PathHierarchyTokenizer::next() {
  if (!_state || _term_eof) {
    return false;
  }

  auto& state = *_state;
  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  if (state.data.empty()) {
    _term_eof = true;
    return false;
  }

  if (state.reverse) {
    // Reverse mode: domain-like hierarchies
    // e.g., "www.example.com" with delims at [3, 11]
    // Token 0: start=0 "www.example.com"
    // Token 1: start=4 "example.com"
    // Token 2: start=12 "com"
    if (state.current_token >= state.num_tokens) {
      _term_eof = true;
      return false;
    }

    size_t start_pos = 0;
    if (state.current_token > 0 &&
        state.current_token <= state.delim_positions.size()) {
      size_t delim_idx = state.current_token - 1;
      start_pos = state.delim_positions[delim_idx] + 1;
    }

    std::string_view token_str(state.data.data() + start_pos);

    if (state.delimiter != state.replacement) {
      apply_replacement(token_str, term_attr, state.delimiter,
                        state.replacement);
    } else {
      term_attr.value = bytes_view(ViewCast<byte_type>(token_str));
    }

    offset_attr.start = start_pos;
    offset_attr.end = state.data.length();

  } else {
    // Forward mode: full paths at each level
    // e.g., "/a/b/c" with delims at [0, 4, 9]
    // Token 0: end at delim[1] = 4 → "/a" (exclude trailing delimiter)
    // Token 1: end at delim[2] = 9 → "/a/b" (exclude trailing delimiter)
    // Token 2: end at data.length() = 14 → "/a/b/c" (full text)
    if (state.current_token >= state.num_tokens) {
      _term_eof = true;
      return false;
    }

    size_t end_pos;
    if (state.delim_positions.empty()) {
      end_pos = state.data.length();
    } else if (state.current_token + 1 < state.delim_positions.size()) {
      size_t next_delim_idx = state.current_token + 1;
      end_pos = state.delim_positions[next_delim_idx];
    } else {
      end_pos = state.data.length();
    }

    std::string_view token_str(state.data.data(), end_pos);

    if (state.delimiter != state.replacement) {
      apply_replacement(token_str, term_attr, state.delimiter,
                        state.replacement);
    } else {
      term_attr.value = bytes_view(ViewCast<byte_type>(token_str));
    }

    offset_attr.start = 0;
    offset_attr.end = end_pos;
  }

  inc_attr.value = 1;
  state.current_token++;
  return true;
}

void PathHierarchyTokenizer::apply_replacement(std::string_view input,
                                               TermAttr& term_attr,
                                               char delimiter,
                                               char replacement) {
  if (input.find(delimiter) == std::string_view::npos) {
    term_attr.value = bytes_view(ViewCast<byte_type>(input));
    return;
  }

  _replace_buffer.clear();
  _replace_buffer.reserve(input.size());

  for (char c : input) {
    _replace_buffer.push_back(c == delimiter ? replacement : c);
  }

  term_attr.value = bytes_view(ViewCast<byte_type>(
    std::string_view(_replace_buffer.data(), _replace_buffer.size())));
}

}  // namespace irs::analysis
