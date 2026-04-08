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
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "path_hierarchy_token_stream delimiter must not be empty");
    return false;
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

  SDB_ASSERT(!options.delimiter.empty());
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
  std::string_view _data;  // input
  std::string _buffer;     // buffer for case delimiter != replacement
  size_t _prefix_start_in_input =
    0;  // left edge of prefix; every token's offset.start
  size_t _prefix_end_in_input =
    0;  // input merged into _buffer ends before this index
  size_t _delimiter_search_from = 0;  // find(next delimiter) starts here
  size_t _delimiter_size = 0;         // delimiter length
  bool _need_replacement = false;     // delimiter != replacement, using _buffer
};

bool ForwardPathHierarchyTokenizer::reset(std::string_view data) {
  _data = data;
  _term_eof = data.empty();

  if (_term_eof) {
    return true;
  }

  _delimiter_size = _options.delimiter.size();
  _need_replacement = _options.delimiter != _options.replacement;
  SDB_ASSERT(_delimiter_size > 0);

  _prefix_start_in_input = 0;
  if (_options.skip > 0) {
    size_t skip_step_idx = 0;
    size_t scan_from = 0;
    // leading delimiter counts as one skip step: (/a/b/c equal a/b/c)
    // without +1, skip would line up wrong on paths that start with a delimiter
    size_t delimiter_steps_to_skip =
      _options.skip + (_data.find(_options.delimiter) == 0);

    while (skip_step_idx < delimiter_steps_to_skip) {
      size_t next_delimiter_position =
        _data.find(_options.delimiter, scan_from);
      if (next_delimiter_position == std::string_view::npos) {
        _term_eof = true;
        return true;
      }

      _prefix_start_in_input = next_delimiter_position;
      scan_from = next_delimiter_position + _delimiter_size;
      ++skip_step_idx;
    }
  }

  _prefix_end_in_input = _prefix_start_in_input;
  _delimiter_search_from = _prefix_start_in_input;

  // for leading delimiter: /a/b/c
  // bump search cursor past it so the first segment token isn't empty
  if (_delimiter_search_from + _delimiter_size <= _data.size() &&
      _data.substr(_delimiter_search_from, _delimiter_size) ==
        _options.delimiter) {
    _delimiter_search_from += _delimiter_size;
  }

  if (_need_replacement) {
    _buffer.clear();
    _buffer.reserve(_options.buffer_size);
  }

  return true;
}

bool ForwardPathHierarchyTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  size_t token_end_position = _data.size();
  size_t next_delimiter_position =
    _data.find(_options.delimiter, _delimiter_search_from);

  if (next_delimiter_position != std::string_view::npos) {
    token_end_position = next_delimiter_position;
    _delimiter_search_from = next_delimiter_position + _delimiter_size;
  } else {
    _term_eof = true;
  }

  SDB_ASSERT(_prefix_start_in_input <= token_end_position);
  SDB_ASSERT(token_end_position <= _data.size());

  if (!_need_replacement) {
    term_attr.value = ViewCast<byte_type>(_data.substr(
      _prefix_start_in_input, token_end_position - _prefix_start_in_input));
  } else {
    SDB_ASSERT(_prefix_end_in_input <= token_end_position);

    // when first character isn't delimiter: a/b/c
    // if cursor is on delimiter, emit replacement then tail; else append input
    if (_data.substr(_prefix_end_in_input, _delimiter_size) ==
        _options.delimiter) {
      _buffer.append(_options.replacement);

      size_t segment_len = _prefix_end_in_input + _delimiter_size;
      _buffer.append(_data.data() + segment_len,
                     token_end_position - segment_len);
    } else {
      _buffer.append(_data.data() + _prefix_end_in_input,
                     token_end_position - _prefix_end_in_input);
    }

    _prefix_end_in_input = token_end_position;
    term_attr.value = ViewCast<byte_type>(std::string_view(_buffer));
  }

  offset_attr.start = static_cast<uint32_t>(_prefix_start_in_input);
  offset_attr.end = static_cast<uint32_t>(token_end_position);
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
  std::string_view _data;  // input
  std::string _buffer;     // buffer for case delimiter != replacement
  size_t _suffix_start_in_buffer = 0;  // current token's left edge in _buffer
  size_t _suffix_start_in_input =
    0;  // current token's left edge in input (for offset.start)
  size_t _suffix_window_end =
    0;  // path ends here after skip-from-right (past last byte)
  size_t _delimiter_size = 0;      // delimiter length
  bool _need_replacement = false;  // delimiter != replacement, using _buffer
};

bool ReversePathHierarchyTokenizer::reset(std::string_view data) {
  _data = data;
  _term_eof = data.empty();

  if (_term_eof) {
    return true;
  }

  _delimiter_size = _options.delimiter.size();
  _need_replacement = _options.delimiter != _options.replacement;
  SDB_ASSERT(_delimiter_size > 0);

  // walk right-to-left: each skip drops one trailing segment
  size_t trimmed_window_end = data.size();
  for (size_t skip_idx = 0; skip_idx < _options.skip; ++skip_idx) {
    if (trimmed_window_end <= _delimiter_size) {
      _term_eof = true;
      return true;
    }

    size_t rfind_delimiter_position =
      data.rfind(_options.delimiter, trimmed_window_end - _delimiter_size - 1);
    if (rfind_delimiter_position == std::string_view::npos) {
      _term_eof = true;
      return true;
    }
    trimmed_window_end = rfind_delimiter_position + _delimiter_size;
  }
  _suffix_window_end = trimmed_window_end;

  _suffix_start_in_input = 0;
  _suffix_start_in_buffer = 0;

  if (!_need_replacement) {
    if (_suffix_window_end == 0) {
      _term_eof = true;
    }
    return true;
  }

  _buffer.clear();
  _buffer.reserve(_options.buffer_size);

  // left-to-right: segments joined with replacement (offsets still use input
  // indices)
  size_t scan_from = 0;
  while (scan_from < _suffix_window_end) {
    size_t next_delimiter_position = data.find(_options.delimiter, scan_from);
    if (next_delimiter_position == std::string_view::npos) {
      SDB_ASSERT(scan_from <= _suffix_window_end);
      _buffer.append(data.data() + scan_from, _suffix_window_end - scan_from);
      break;
    }
    SDB_ASSERT(next_delimiter_position + _delimiter_size <= _suffix_window_end);

    _buffer.append(data.data() + scan_from,
                   next_delimiter_position - scan_from);
    _buffer.append(_options.replacement);

    scan_from = next_delimiter_position + _delimiter_size;
  }

  if (_buffer.size() == 0) {
    _term_eof = true;
    return true;
  }
  return true;
}

bool ReversePathHierarchyTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  SDB_ASSERT(_suffix_window_end <= _data.size());
  SDB_ASSERT(_suffix_start_in_input <= _suffix_window_end);

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  if (!_need_replacement) {
    term_attr.value = ViewCast<byte_type>(_data.substr(
      _suffix_start_in_input, _suffix_window_end - _suffix_start_in_input));
  } else {
    SDB_ASSERT(_suffix_start_in_buffer <= _buffer.size());
    term_attr.value = ViewCast<byte_type>(
      std::string_view(_buffer).substr(_suffix_start_in_buffer));
  }

  offset_attr.start = static_cast<uint32_t>(_suffix_start_in_input);
  offset_attr.end = static_cast<uint32_t>(_suffix_window_end);
  inc_attr.value = 1;

  size_t next_delimiter_position =
    _data.find(_options.delimiter, _suffix_start_in_input);

  if (next_delimiter_position == std::string_view::npos ||
      next_delimiter_position >= _suffix_window_end) {
    _term_eof = true;
  } else {
    SDB_ASSERT(next_delimiter_position >= _suffix_start_in_input);
    SDB_ASSERT(next_delimiter_position + _delimiter_size <= _suffix_window_end);

    size_t segment_len = next_delimiter_position - _suffix_start_in_input;
    _suffix_start_in_input = next_delimiter_position + _delimiter_size;

    if (_need_replacement) {
      _suffix_start_in_buffer += segment_len + _options.replacement.size();
    }

    if (_suffix_start_in_input >= _suffix_window_end) {
      _term_eof = true;
    }
  }

  return true;
}

Analyzer::ptr PathHierarchyTokenizer::make(Options&& options) {
  if (options.replacement.empty()) {
    options.replacement = options.delimiter;
  }
  if (options.reverse) {
    return std::make_unique<ReversePathHierarchyTokenizer>(std::move(options));
  }
  return std::make_unique<ForwardPathHierarchyTokenizer>(std::move(options));
}

}  // namespace irs::analysis
