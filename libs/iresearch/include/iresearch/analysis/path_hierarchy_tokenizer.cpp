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

#include "path_hierarchy_tokenizer.hpp"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/serializer.h>
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

  struct VPackOptionsTemp {
    std::string delimiter = "/";
    std::string replacement = "";
    size_t buffer_size = 1024;
    bool reverse = false;
    size_t skip = 0;
  } temp;

  auto r = vpack::ReadObjectNothrow(slice, temp,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "Failed to parse path_hierarchy_token_stream options: ",
             r.errorMessage());
    return false;
  }

  if (temp.delimiter.size() != 1) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "Invalid type 'delimiter' (single character string expected) for "
             "path_hierarchy_token_stream from VPack arguments");
    return false;
  }

  options.delimiter = temp.delimiter[0];

  if (temp.replacement.empty()) {
    options.replacement = options.delimiter;
  } else {
    if (temp.replacement.size() != 1) {
      SDB_WARN(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Invalid type 'replacement' (single character string expected) for "
        "path_hierarchy_token_stream from VPack arguments");
      return false;
    }
    options.replacement = temp.replacement[0];
  }

  options.buffer_size = temp.buffer_size;
  options.reverse = temp.reverse;
  options.skip = temp.skip;

  return true;
}

Analyzer::ptr MakeVPack(const vpack::Slice& args) {
  PathHierarchyTokenizer::OptionsT options;
  if (ParseVPackOptions(args, options)) {
    return PathHierarchyTokenizer::make(std::move(options));
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
    {
      vpack::ObjectBuilder object(builder);
      builder->add("delimiter", std::string(1, options.delimiter));
      builder->add("replacement", std::string(1, options.replacement));
      builder->add("buffer_size", options.buffer_size);
      builder->add("reverse", options.reverse);
      builder->add("skip", options.skip);
    }
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

PathHierarchyTokenizer::PathHierarchyTokenizer(OptionsT&& options) noexcept
  : _options{std::move(options)}, _term_eof{true} {
  if (_options.buffer_size > 0) {
    _replace_buffer.reserve(_options.buffer_size);
  }
}

PathHierarchyTokenizer::~PathHierarchyTokenizer() = default;

void PathHierarchyTokenizer::init() {
  REGISTER_ANALYZER_JSON(PathHierarchyTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(PathHierarchyTokenizer, MakeVPack,
                          NormalizeVPackConfig);
}

Attribute* PathHierarchyTokenizer::GetMutable(TypeInfo::type_id type) noexcept {
  return irs::GetMutable(_attrs, type);
}

void PathHierarchyTokenizer::apply_replacement(std::string_view input,
                                               TermAttr& term_attr,
                                               char delimiter, char replacement,
                                               std::string& buffer) const {
  if (input.find(delimiter) == std::string_view::npos) {
    term_attr.value = ViewCast<byte_type>(input);
    return;
  }

  buffer.resize(input.size());

  const char* src = input.data();
  char* dst = buffer.data();
  size_t remaining = input.size();
  size_t pos = 0;

  while (true) {
    size_t next_delim = input.find(delimiter, pos);
    if (next_delim == std::string_view::npos) {
      if (remaining > pos) {
        std::memcpy(dst + pos, src + pos, remaining - pos);
      }
      break;
    }

    if (next_delim > pos) {
      std::memcpy(dst + pos, src + pos, next_delim - pos);
    }

    dst[next_delim] = replacement;

    pos = next_delim + 1;
  }
  term_attr.value =
    ViewCast<byte_type>(std::string_view(buffer.data(), buffer.size()));
}

class ForwardPathHierarchyTokenizer final : public PathHierarchyTokenizer {
 public:
  explicit ForwardPathHierarchyTokenizer(OptionsT&& options) noexcept
    : PathHierarchyTokenizer(std::move(options)) {}

  bool reset(std::string_view data) final;
  bool next() final;

 private:
  struct State {
    std::string_view data;
    std::vector<size_t> delim_positions;
    size_t num_tokens = 0;
    size_t current_token = 0;
    bool starts_with_delimiter = false;
    size_t first_token_start = 0;
  };

  std::unique_ptr<State> _state;
};

bool ForwardPathHierarchyTokenizer::reset(std::string_view data) {
  _term_eof = false;

  auto state = std::make_unique<State>();
  state->data = data;
  state->delim_positions.clear();
  state->current_token = 0;
  state->num_tokens = 0;
  state->starts_with_delimiter = false;
  state->first_token_start = 0;

  // Find all delimiter positions
  if (!data.empty()) {
    size_t pos = 0;
    while ((pos = data.find(_options.delimiter, pos)) !=
           std::string_view::npos) {
      state->delim_positions.emplace_back(pos);
      ++pos;
    }
  }

  if (state->delim_positions.empty()) {
    state->num_tokens = data.empty() ? 0 : 1;
  } else {
    state->starts_with_delimiter = (state->delim_positions[0] == 0);

    size_t total_tokens =
      state->delim_positions.size() + !state->starts_with_delimiter;

    if (_options.skip >= total_tokens) {
      state->num_tokens = 0;
    } else {
      state->num_tokens = total_tokens - _options.skip;
    }

    if (_options.skip > 0 && _options.skip < total_tokens) {
      size_t skip_position =
        _options.skip - (!state->starts_with_delimiter ? 1 : 0);
      state->first_token_start = state->delim_positions[skip_position];
    } else {
      state->first_token_start = 0;
    }
  }

  _state = std::move(state);
  return true;
}

bool ForwardPathHierarchyTokenizer::next() {
  if (!_state || _term_eof) {
    return false;
  }

  auto& state = *_state;

  if (state.data.empty() || state.current_token >= state.num_tokens) {
    _term_eof = true;
    return false;
  }

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  if (state.delim_positions.empty()) {
    // Single token (no delimiters)
    std::string_view token_str(state.data);

    if (_options.delimiter != _options.replacement) {
      apply_replacement(token_str, term_attr, _options.delimiter,
                        _options.replacement, _replace_buffer);
    } else {
      term_attr.value = ViewCast<byte_type>(token_str);
    }

    offset_attr.start = 0;
    offset_attr.end = state.data.length();
    inc_attr.value = 1;
    ++state.current_token;
    return true;
  }

  // Compute global token index (before skip)
  size_t global_token_idx = state.current_token + _options.skip;

  // Determine end offset
  size_t end_pos = 0;
  if (global_token_idx + state.starts_with_delimiter <
      state.delim_positions.size()) {
    end_pos =
      state.delim_positions[global_token_idx + state.starts_with_delimiter];
  } else {
    end_pos = state.data.length();
  }

  size_t start_pos = state.first_token_start;
  std::string_view token_str(state.data.data() + start_pos,
                             end_pos - start_pos);

  // Skip empty tokens (e.g. from consecutive delimiters)
  if (token_str.empty()) {
    ++state.current_token;
    return next();
  }

  if (_options.delimiter != _options.replacement) {
    apply_replacement(token_str, term_attr, _options.delimiter,
                      _options.replacement, _replace_buffer);
  } else {
    term_attr.value = ViewCast<byte_type>(token_str);
  }

  offset_attr.start = start_pos;
  offset_attr.end = end_pos;
  inc_attr.value = 1;
  ++state.current_token;
  return true;
}

class ReversePathHierarchyTokenizer final : public PathHierarchyTokenizer {
 public:
  explicit ReversePathHierarchyTokenizer(OptionsT&& options) noexcept
    : PathHierarchyTokenizer(std::move(options)) {}

  bool reset(std::string_view data) final;
  bool next() final;

 private:
  struct State {
    std::string_view data;
    std::vector<size_t> delim_positions;
    std::vector<size_t> token_starts;
    size_t num_tokens = 0;
    size_t current_token = 0;
    size_t end_pos = 0;
  };

  std::unique_ptr<State> _state;
};

bool ReversePathHierarchyTokenizer::reset(std::string_view data) {
  _term_eof = false;

  auto state = std::make_unique<State>();
  state->data = data;
  state->current_token = 0;

  // Find all delimiter positions
  if (!data.empty()) {
    size_t pos = 0;
    while ((pos = data.find(_options.delimiter, pos)) !=
           std::string_view::npos) {
      state->delim_positions.emplace_back(pos);
      ++pos;
    }
  }

  // Build list of start positions for all possible tokens
  std::vector<size_t> all_starts;
  if (!data.empty()) {
    all_starts.push_back(0);
  }

  for (size_t pos : state->delim_positions) {
    size_t start = pos + 1;
    if (start < data.length()) {
      all_starts.push_back(start);
    }
  }

  size_t total_tokens = all_starts.size();

  // Apply skip
  if (_options.skip >= total_tokens) {
    state->token_starts.clear();
  } else {
    all_starts.resize(total_tokens - _options.skip);
    state->token_starts = std::move(all_starts);
  }

  state->num_tokens = state->token_starts.size();

  if (state->num_tokens > 0) {
    size_t last_kept_idx = total_tokens - 1 - _options.skip;
    if (last_kept_idx < state->delim_positions.size()) {
      state->end_pos = state->delim_positions[last_kept_idx] + 1;
    } else {
      state->end_pos = data.length();
    }
  }

  _state = std::move(state);
  return true;
}

bool ReversePathHierarchyTokenizer::next() {
  if (!_state || _term_eof) {
    return false;
  }

  auto& state = *_state;

  if (state.data.empty() || state.current_token >= state.token_starts.size()) {
    _term_eof = true;
    return false;
  }

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  size_t start_pos = state.token_starts[state.current_token];
  size_t end_pos = state.end_pos;

  std::string_view token_str(state.data.data() + start_pos,
                             end_pos - start_pos);

  if (_options.delimiter != _options.replacement) {
    apply_replacement(token_str, term_attr, _options.delimiter,
                      _options.replacement, _replace_buffer);
  } else {
    term_attr.value = ViewCast<byte_type>(token_str);
  }

  offset_attr.start = start_pos;
  offset_attr.end = end_pos;
  inc_attr.value = 1;
  ++state.current_token;
  return true;
}

Analyzer::ptr PathHierarchyTokenizer::make(OptionsT&& options) {
  if (options.reverse) {
    return std::make_unique<ReversePathHierarchyTokenizer>(std::move(options));
  } else {
    return std::make_unique<ForwardPathHierarchyTokenizer>(std::move(options));
  }
}

}  // namespace irs::analysis
