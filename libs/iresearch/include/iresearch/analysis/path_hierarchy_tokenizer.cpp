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
  std::string_view _data = "";
  std::string _buffer = "";
  size_t _start = 0;
  size_t _curr_start_raw = 0;
  size_t _last_raw_end = 0;
  size_t _delim_size = 0;
  bool _passthrough = false;
};

bool ForwardPathHierarchyTokenizer::reset(std::string_view data) {
  _data = data;
  _term_eof = data.empty();

  if (_term_eof) {
    return true;
  }

  std::string_view delim = _options.delimiter;
  std::string_view repl = _options.replacement;

  _delim_size = delim.size();
  _passthrough = delim == repl;

  _start = 0;
  if (_options.skip > 0) {
    size_t pos = 0;
    size_t hit = 0;

    bool leading = (_data.find(delim) == 0);
    size_t needed = _options.skip + leading;

    while (hit < needed) {
      size_t find_pos = _data.find(delim, pos);
      if (find_pos == std::string_view::npos) {
        _term_eof = true;
        return true;
      }
      _start = find_pos;
      pos = find_pos + _delim_size;
      ++hit;
    }
  }

  _last_raw_end = _start;
  _curr_start_raw = _start;

  if (_curr_start_raw + _delim_size <= _data.size() &&
      _data.compare(_curr_start_raw, _delim_size, delim) == 0) {
    _curr_start_raw += _delim_size;
  }

  if (!_passthrough) {
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

  std::string_view delim = _options.delimiter;
  std::string_view repl = _options.replacement;

  size_t end_in = _data.size();
  size_t find_pos = _data.find(delim, _curr_start_raw);

  if (find_pos != std::string_view::npos) {
    end_in = find_pos;
    _curr_start_raw = find_pos + _delim_size;
  } else {
    _term_eof = true;
  }

  if (_passthrough) {
    term_attr.value =
      ViewCast<byte_type>(_data.substr(_start, end_in - _start));
  } else {
    if (_last_raw_end < end_in) {
      bool delim_at_from = false;
      if (_last_raw_end + _delim_size <= end_in) {
        delim_at_from = (_data.compare(_last_raw_end, _delim_size, delim) == 0);
      }

      if (delim_at_from) {
        _buffer.append(repl);
        _buffer.append(_data.data() + _last_raw_end + _delim_size,
                       end_in - _last_raw_end - _delim_size);
      } else {
        _buffer.append(_data.data() + _last_raw_end, end_in - _last_raw_end);
      }
    }

    _last_raw_end = end_in;
    term_attr.value = ViewCast<byte_type>(std::string_view(_buffer));
  }

  offset_attr.start = static_cast<uint32_t>(_start);
  offset_attr.end = static_cast<uint32_t>(end_in);
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
  std::string_view _data = "";
  std::string _buffer = "";
  size_t _trim_end = 0;
  size_t _curr_start_buf = 0;
  size_t _curr_start_raw = 0;
  size_t _delim_size = 0;
  bool _passthrough = false;
};

bool ReversePathHierarchyTokenizer::reset(std::string_view data) {
  _data = data;
  _term_eof = data.empty();

  if (_term_eof) {
    return true;
  }

  std::string_view delim = _options.delimiter;
  std::string_view repl = _options.replacement;

  _delim_size = delim.size();
  _passthrough = delim == repl;

  size_t end = data.size();
  for (size_t s = 0; s < _options.skip; ++s) {
    if (end <= _delim_size) {
      _term_eof = true;
      return true;
    }

    size_t find_pos = data.rfind(delim, end - _delim_size - 1);
    if (find_pos == std::string_view::npos) {
      _term_eof = true;
      return true;
    }
    end = find_pos + _delim_size;
  }
  _trim_end = end;

  _curr_start_raw = 0;
  _curr_start_buf = 0;

  if (_passthrough) {
    if (_trim_end == 0) {
      _term_eof = true;
    }
    return true;
  }

  _buffer.clear();
  _buffer.reserve(_options.buffer_size);

  size_t pos = 0;
  while (pos < _trim_end) {
    size_t find_pos = data.find(delim, pos);
    if (find_pos == std::string_view::npos) {
      _buffer.append(data.data() + pos, _trim_end - pos);
      break;
    }
    _buffer.append(data.data() + pos, find_pos - pos);
    _buffer.append(repl);
    pos = find_pos + _delim_size;
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

  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  std::string_view delim = _options.delimiter;

  if (_passthrough) {
    term_attr.value = ViewCast<byte_type>(
      _data.substr(_curr_start_raw, _trim_end - _curr_start_raw));
  } else {
    term_attr.value =
      ViewCast<byte_type>(std::string_view(_buffer).substr(_curr_start_buf));
  }

  offset_attr.start = static_cast<uint32_t>(_curr_start_raw);
  offset_attr.end = static_cast<uint32_t>(_trim_end);
  inc_attr.value = 1;

  size_t find_pos = _data.find(delim, _curr_start_raw);

  if (find_pos == std::string_view::npos || find_pos >= _trim_end) {
    _term_eof = true;
  } else {
    size_t raw_segment_len = find_pos - _curr_start_raw;
    _curr_start_raw = find_pos + delim.size();

    if (!_passthrough) {
      _curr_start_buf += (raw_segment_len + _options.replacement.size());
    }

    if (_curr_start_raw >= _trim_end) {
      _term_eof = true;
    }
  }

  return true;
}

Analyzer::ptr PathHierarchyTokenizer::make(Options&& options) {
  if (options.reverse) {
    return std::make_unique<ReversePathHierarchyTokenizer>(std::move(options));
  }
  return std::make_unique<ForwardPathHierarchyTokenizer>(std::move(options));
}

}  // namespace irs::analysis
