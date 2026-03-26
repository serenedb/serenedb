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

#include <cstring>
#include <string_view>
#include <vector>

#include "basics/logger/logger.h"
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {
namespace {

bool ParseVPackOptions(const vpack::Slice slice,
                       PathHierarchyTokenizer::Options& options) {
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

  if (temp.delimiter.empty()) {
    temp.delimiter = "/";
  }
  options.delimiter = std::move(temp.delimiter);

  if (temp.replacement.empty()) {
    options.replacement = options.delimiter;
  } else {
    options.replacement = std::move(temp.replacement);
  }

  options.buffer_size = temp.buffer_size;
  options.reverse = temp.reverse;
  options.skip = temp.skip;

  return true;
}

Analyzer::ptr MakeVPack(const vpack::Slice& args) {
  PathHierarchyTokenizer::Options options;
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
  PathHierarchyTokenizer::Options options;
  if (ParseVPackOptions(slice, options)) {
    vpack::ObjectBuilder object(builder);
    builder->add("delimiter", options.delimiter);
    builder->add("replacement", options.replacement);
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

PathHierarchyTokenizer::PathHierarchyTokenizer(Options&& options) noexcept
  : _options{std::move(options)}, _term_eof{true} {}

PathHierarchyTokenizer::~PathHierarchyTokenizer() = default;

void PathHierarchyTokenizer::init() {
  REGISTER_ANALYZER_JSON(PathHierarchyTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(PathHierarchyTokenizer, MakeVPack,
                          NormalizeVPackConfig);
}

Attribute* PathHierarchyTokenizer::GetMutable(TypeInfo::type_id type) noexcept {
  return irs::GetMutable(_attrs, type);
}

class ForwardPathHierarchyTokenizer final : public PathHierarchyTokenizer {
 public:
  explicit ForwardPathHierarchyTokenizer(Options&& options) noexcept
    : PathHierarchyTokenizer(std::move(options)) {}

  bool reset(std::string_view data) final;
  bool next() final;

 private:
  struct State {
    std::string_view data;
    std::string result_token;
    size_t acc_len = 0;
    size_t chars_read = 0;
    size_t start_position = 0;
    size_t skipped = 0;
    bool end_delimiter = false;
  };

  bool next_slice();
  bool next_replace();

  size_t FindNextDelim(std::string_view data, std::string_view delim,
                       size_t from);
  bool MatchesDelimAt(std::string_view data, size_t pos,
                      std::string_view delim);

  State _state;
  bool _has_state{false};
  bool _slice_mode{false};
};

size_t ForwardPathHierarchyTokenizer::FindNextDelim(std::string_view data,
                                                    std::string_view delim,
                                                    size_t from) {
  if (delim.empty() || from >= data.size()) {
    return data.size();
  }
  if (delim.size() == 1) {
    const char* ptr = data.data() + from;
    const char* next =
      static_cast<const char*>(std::memchr(ptr, delim[0], data.size() - from));
    return next ? static_cast<size_t>(next - data.data()) : data.size();
  }
  const size_t pos = data.find(delim, from);
  return pos == std::string_view::npos ? data.size() : pos;
}

bool ForwardPathHierarchyTokenizer::MatchesDelimAt(std::string_view data,
                                                   size_t pos,
                                                   std::string_view delim) {
  if (delim.empty()) {
    return false;
  }
  return pos + delim.size() <= data.size() &&
         data.substr(pos, delim.size()) == delim;
}

bool ForwardPathHierarchyTokenizer::reset(std::string_view data) {
  _slice_mode = (_options.delimiter == _options.replacement);
  _state.data = data;
  _state.chars_read = 0;
  _state.start_position = 0;
  _state.skipped = 0;
  _state.end_delimiter = false;
  _state.acc_len = 0;
  _state.result_token.clear();
  if (!_slice_mode) {
    _state.result_token.reserve(data.size());
  }
  _has_state = true;
  _term_eof = data.empty();
  return true;
}

bool ForwardPathHierarchyTokenizer::next() {
  if (_slice_mode) {
    return next_slice();
  }
  return next_replace();
}

bool ForwardPathHierarchyTokenizer::next_slice() {
  if (_term_eof || !_has_state) {
    return false;
  }

  const std::string_view delim{_options.delimiter};
  const std::string_view repl{_options.replacement};

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  auto& state = _state;

  const size_t prefix_len = state.acc_len;
  size_t length = 0;
  bool added = false;

  if (state.end_delimiter) {
    state.acc_len += repl.size();
    length += repl.size();
    state.end_delimiter = false;
    added = true;
  }

  while (true) {
    if (state.chars_read >= state.data.size()) {
      if (state.skipped > _options.skip && added) {
        const size_t total_len = prefix_len + length;
        term_attr.value = ViewCast<byte_type>(
          state.data.substr(state.start_position, total_len));
        offset_attr.start = state.start_position;
        offset_attr.end = state.start_position + total_len;
        inc_attr.value = 1;
        _term_eof = true;
        return true;
      }
      _term_eof = true;
      return false;
    }

    if (!added) {
      added = true;
      ++state.skipped;
      if (state.skipped > _options.skip) {
        if (MatchesDelimAt(state.data, state.chars_read, delim)) {
          state.acc_len += repl.size();
          length += repl.size();
          state.chars_read += delim.size();
        } else {
          state.acc_len += 1;
          length += 1;
          state.chars_read += 1;
        }
      } else {
        ++state.start_position;
        state.chars_read += 1;
      }
      continue;
    }

    if (MatchesDelimAt(state.data, state.chars_read, delim)) {
      if (state.skipped > _options.skip) {
        state.end_delimiter = true;
        state.chars_read += delim.size();
        break;
      }
      ++state.skipped;
      if (state.skipped > _options.skip) {
        state.acc_len += repl.size();
        length += repl.size();
        state.chars_read += delim.size();
      } else {
        state.start_position += delim.size();
        state.chars_read += delim.size();
      }
      continue;
    }

    const size_t component_start = state.chars_read;
    const size_t component_end =
      FindNextDelim(state.data, delim, component_start + 1);
    const size_t component_len = component_end - component_start;

    if (state.skipped > _options.skip) {
      state.acc_len += component_len;
      length += component_len;
    } else {
      state.start_position += component_len;
    }
    state.chars_read = component_end;
  }

  const size_t total_len = prefix_len + length;
  term_attr.value =
    ViewCast<byte_type>(state.data.substr(state.start_position, total_len));
  offset_attr.start = state.start_position;
  offset_attr.end = state.start_position + total_len;
  inc_attr.value = 1;
  return true;
}

bool ForwardPathHierarchyTokenizer::next_replace() {
  if (_term_eof || !_has_state) {
    return false;
  }

  const std::string_view delim{_options.delimiter};
  const std::string_view repl{_options.replacement};

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  auto& state = _state;

  const size_t prefix_len = state.result_token.size();
  size_t length = 0;
  bool added = false;

  if (state.end_delimiter) {
    state.result_token.append(repl);
    length += repl.size();
    state.end_delimiter = false;
    added = true;
  }

  while (true) {
    if (state.chars_read >= state.data.size()) {
      if (state.skipped > _options.skip && added) {
        const size_t total_len = prefix_len + length;
        term_attr.value =
          ViewCast<byte_type>(std::string_view(state.result_token));
        offset_attr.start = state.start_position;
        offset_attr.end = state.start_position + total_len;
        inc_attr.value = 1;
        _term_eof = true;
        return true;
      }
      _term_eof = true;
      return false;
    }

    if (!added) {
      added = true;
      ++state.skipped;
      if (state.skipped > _options.skip) {
        if (MatchesDelimAt(state.data, state.chars_read, delim)) {
          state.result_token.append(repl);
          length += repl.size();
          state.chars_read += delim.size();
        } else {
          state.result_token.push_back(state.data[state.chars_read]);
          length += 1;
          state.chars_read += 1;
        }
      } else {
        ++state.start_position;
        state.chars_read += 1;
      }
      continue;
    }

    if (MatchesDelimAt(state.data, state.chars_read, delim)) {
      if (state.skipped > _options.skip) {
        state.end_delimiter = true;
        state.chars_read += delim.size();
        break;
      }
      ++state.skipped;
      if (state.skipped > _options.skip) {
        state.result_token.append(repl);
        length += repl.size();
        state.chars_read += delim.size();
      } else {
        state.start_position += delim.size();
        state.chars_read += delim.size();
      }
      continue;
    }

    const size_t component_start = state.chars_read;
    const size_t component_end =
      FindNextDelim(state.data, delim, component_start + 1);
    const size_t component_len = component_end - component_start;

    if (state.skipped > _options.skip) {
      state.result_token.append(state.data.data() + component_start,
                                component_len);
      length += component_len;
    } else {
      state.start_position += component_len;
    }
    state.chars_read = component_end;
  }

  const size_t total_len = prefix_len + length;
  term_attr.value = ViewCast<byte_type>(std::string_view(state.result_token));
  offset_attr.start = state.start_position;
  offset_attr.end = state.start_position + total_len;
  inc_attr.value = 1;
  return true;
}

class ReversePathHierarchyTokenizer final : public PathHierarchyTokenizer {
 public:
  explicit ReversePathHierarchyTokenizer(Options&& options) noexcept
    : PathHierarchyTokenizer(std::move(options)) {}

  bool reset(std::string_view data) final;
  bool next() final;

 private:
  // Replaces delimiter with replacement; writes result to `buffer`/term.
  // Optionally collects component start offsets after each delimiter.
  void ApplyReplacement(std::string_view input, TermAttr& term_attr,
                        std::string_view delimiter,
                        std::string_view replacement, std::string& buffer,
                        std::vector<size_t>* component_starts);

  size_t FindNextDelim(std::string_view data, std::string_view delim,
                       size_t from);

  std::string_view _data;
  std::vector<size_t> _delimiter_positions;
  std::string _buffer;
  int _delimiters_count = -1;
  size_t _end_position = 0;
  size_t _final_offset = 0;
  int _skipped = 0;
};

size_t ReversePathHierarchyTokenizer::FindNextDelim(std::string_view data,
                                                    std::string_view delim,
                                                    size_t from) {
  if (delim.empty() || from >= data.size()) {
    return data.size();
  }
  if (delim.size() == 1) {
    const char* ptr = data.data() + from;
    const char* next =
      static_cast<const char*>(std::memchr(ptr, delim[0], data.size() - from));
    return next ? static_cast<size_t>(next - data.data()) : data.size();
  }
  const size_t pos = data.find(delim, from);
  return pos == std::string_view::npos ? data.size() : pos;
}

void ReversePathHierarchyTokenizer::ApplyReplacement(
  std::string_view input, TermAttr& term_attr, std::string_view delimiter,
  std::string_view replacement, std::string& buffer,
  std::vector<size_t>* component_starts) {
  if (delimiter.empty()) {
    term_attr.value = ViewCast<byte_type>(input);
    if (component_starts) {
      component_starts->clear();
      component_starts->push_back(0);
      if (component_starts->back() < input.size()) {
        component_starts->push_back(input.size());
      }
    }
    return;
  }

  if (delimiter == replacement) {
    term_attr.value = ViewCast<byte_type>(input);
    if (component_starts) {
      component_starts->clear();
      component_starts->push_back(0);
      size_t pos = 0;
      while (pos < input.size()) {
        const size_t next =
          ReversePathHierarchyTokenizer::FindNextDelim(input, delimiter, pos);
        if (next == input.size()) {
          break;
        }
        pos = next + delimiter.size();
        component_starts->push_back(pos);
      }
      if (component_starts->empty() ||
          component_starts->back() < input.size()) {
        component_starts->push_back(input.size());
      }
    }
    return;
  }

  buffer.clear();
  buffer.reserve(input.size() + 8);

  if (component_starts) {
    component_starts->clear();
    component_starts->push_back(0);
  }

  size_t pos = 0;
  while (pos < input.size()) {
    const size_t next =
      ReversePathHierarchyTokenizer::FindNextDelim(input, delimiter, pos);
    buffer.append(input.data() + pos, next - pos);
    if (next == input.size()) {
      break;
    }
    buffer.append(replacement);
    pos = next + delimiter.size();
    if (component_starts) {
      component_starts->push_back(buffer.size());
    }
  }

  if (component_starts && !component_starts->empty() &&
      component_starts->back() < buffer.size()) {
    component_starts->push_back(buffer.size());
  }
  term_attr.value =
    ViewCast<byte_type>(std::string_view(buffer.data(), buffer.size()));
}

bool ReversePathHierarchyTokenizer::reset(std::string_view data) {
  _data = data;
  _term_eof = data.empty();
  _delimiter_positions.clear();
  _delimiter_positions.reserve(data.size());
  _buffer.clear();
  _delimiters_count = -1;  // lazy init on first next()
  _end_position = 0;
  _final_offset = 0;
  _skipped = 0;
  return true;
}

bool ReversePathHierarchyTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  // one-time initialization on first call to next():
  // build replaced string in _buffer and delimiter start positions
  if (_delimiters_count == -1) {
    ApplyReplacement(_data, term_attr, _options.delimiter, _options.replacement,
                     _buffer, &_delimiter_positions);
    if (_buffer.empty()) {
      _buffer.assign(_data.begin(), _data.end());
    }

    _delimiters_count = _delimiter_positions.size();
    int idx = _delimiters_count - 1 - _options.skip;
    if (idx >= 0) {
      _end_position = _delimiter_positions[idx];
    }
    _final_offset = _data.size();
  }

  inc_attr.value = 1;

  int limit = _delimiters_count - 1 - _options.skip;
  if (_skipped < limit) {
    size_t start = _delimiter_positions[_skipped];
    size_t len = _end_position - start;

    const char* base = (_options.delimiter == _options.replacement)
                         ? _data.data()
                         : _buffer.data();
    term_attr.value = ViewCast<byte_type>(std::string_view(base + start, len));

    offset_attr.start = start;
    offset_attr.end = _end_position;

    ++_skipped;
    return true;
  }

  _term_eof = true;
  return false;
}

Analyzer::ptr PathHierarchyTokenizer::make(Options&& options) {
  if (options.reverse) {
    return std::make_unique<ReversePathHierarchyTokenizer>(std::move(options));
  }
  return std::make_unique<ForwardPathHierarchyTokenizer>(std::move(options));
}

}  // namespace irs::analysis
