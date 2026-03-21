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

PathHierarchyTokenizer::PathHierarchyTokenizer(Options&& options) noexcept
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
    size_t chars_read = 0;
    size_t start_position = 0;
    size_t skipped = 0;
    bool end_delimiter = false;
  };

  static size_t NextDelimPos(std::string_view data, char delimiter,
                             size_t from) noexcept;

  State _state;
  bool _has_state{false};
};

size_t ForwardPathHierarchyTokenizer::NextDelimPos(std::string_view data,
                                                   char delimiter,
                                                   size_t from) noexcept {
  const char* ptr = data.data() + from;
  const char* data_end = data.data() + data.size();
  const char* next = static_cast<const char*>(
    std::memchr(ptr, delimiter, static_cast<size_t>(data_end - ptr)));
  return next ? static_cast<size_t>(next - data.data()) : data.size();
}

bool ForwardPathHierarchyTokenizer::reset(std::string_view data) {
  _state.data = data;
  _state.chars_read = 0;
  _state.start_position = 0;
  _state.skipped = 0;
  _state.end_delimiter = false;
  _state.result_token.clear();
  _state.result_token.reserve(data.size());
  _has_state = true;
  _term_eof = data.empty();
  return true;
}

bool ForwardPathHierarchyTokenizer::next() {
  if (_term_eof || !_has_state)
    return false;

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  auto& state = _state;

  size_t prefix_len = state.result_token.size();
  size_t length = 0;  // number of newly added characters
  bool added = false;

  if (state.end_delimiter) {
    state.result_token.push_back(_options.replacement);
    ++length;
    state.end_delimiter = false;
    added = true;
  }

  while (true) {
    if (state.chars_read >= state.data.size()) {
      if (state.skipped > _options.skip && added) {
        size_t total_len = prefix_len + length;
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

    char ch = state.data[state.chars_read++];

    if (!added) {
      // First character of the current component
      added = true;
      ++state.skipped;
      if (state.skipped > _options.skip) {
        state.result_token.push_back(
          ch == _options.delimiter ? _options.replacement : ch);
        ++length;
      } else {
        ++state.start_position;
      }
      continue;
    }

    if (ch == _options.delimiter) {
      if (state.skipped > _options.skip) {
        state.end_delimiter = true;
        break;
      }
      ++state.skipped;
      if (state.skipped > _options.skip) {
        state.result_token.push_back(_options.replacement);
        ++length;
      } else {
        ++state.start_position;
      }
      continue;
    }

    // Not a delimiter: extend current component quickly until next delimiter
    size_t component_start = state.chars_read - 1;  // includes 'ch'
    size_t component_end =
      NextDelimPos(state.data, _options.delimiter, state.chars_read);
    size_t component_len = component_end - component_start;

    if (state.skipped > _options.skip) {
      state.result_token.append(state.data.data() + component_start,
                                component_len);
      length += component_len;
    } else {
      state.start_position += component_len;
    }
    state.chars_read = component_end;
  }

  size_t total_len = prefix_len + length;
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
  static void ApplyReplacement(std::string_view input, TermAttr& term_attr,
                               char delimiter, char replacement,
                               std::string& buffer,
                               std::vector<size_t>* component_starts);

  std::string_view _data;
  std::vector<size_t> _delimiter_positions;
  std::string _buffer;
  int _delimiters_count = -1;
  size_t _end_position = 0;
  size_t _final_offset = 0;
  int _skipped = 0;
};

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

void ReversePathHierarchyTokenizer::ApplyReplacement(
  std::string_view input, TermAttr& term_attr, char delimiter, char replacement,
  std::string& buffer, std::vector<size_t>* component_starts) {
  if (delimiter == replacement) {
    if (component_starts) {
      component_starts->push_back(0);
      const char* data = input.data();
      const char* pos = data;
      const char* data_end = data + input.size();
      while (pos != data_end) {
        const char* next = static_cast<const char*>(
          std::memchr(pos, delimiter, static_cast<size_t>(data_end - pos)));
        if (!next)
          break;
        pos = next + 1;
        component_starts->push_back(static_cast<size_t>(pos - data));
      }
      if (component_starts->empty() ||
          component_starts->back() < input.size()) {
        component_starts->push_back(input.size());
      }
    }
    term_attr.value = ViewCast<byte_type>(input);
    return;
  }

  const char* data = input.data();
  size_t len = input.size();
  const char* const data_end = data + len;

  if (component_starts)
    component_starts->push_back(0);

  const char* first =
    static_cast<const char*>(std::memchr(data, delimiter, len));
  const char* pos = first ? first : data_end;
  if (pos == data_end) {
    term_attr.value = ViewCast<byte_type>(input);
    if (component_starts && component_starts->back() < len) {
      component_starts->push_back(len);
    }
    return;
  }

  buffer.resize(len);
  char* buf_start = buffer.data();
  char* dst = buf_start;

  size_t prefix_len = static_cast<size_t>(pos - data);
  if (prefix_len > 0) {
    std::memcpy(dst, data, prefix_len);
    dst += prefix_len;
  }

  const char* src = pos;
  const char* end = data_end;
  while (src != end) {
    if (*src == delimiter) {
      *dst++ = replacement;
      ++src;
      if (component_starts) {
        component_starts->push_back(static_cast<size_t>(dst - buf_start));
      }
      continue;
    }

    const char* next = static_cast<const char*>(
      std::memchr(src, delimiter, static_cast<size_t>(end - src)));
    const char* component_end = next ? next : end;
    size_t component_len = static_cast<size_t>(component_end - src);
    std::memcpy(dst, src, component_len);
    dst += component_len;
    src = component_end;
  }

  if (component_starts && component_starts->back() < len) {
    component_starts->push_back(len);
  }
  term_attr.value = ViewCast<byte_type>(std::string_view(buf_start, len));
}

bool ReversePathHierarchyTokenizer::next() {
  if (_term_eof)
    return false;

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
  } else {
    return std::make_unique<ForwardPathHierarchyTokenizer>(std::move(options));
  }
}

}  // namespace irs::analysis
