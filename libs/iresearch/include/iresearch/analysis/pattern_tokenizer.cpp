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

#include "pattern_tokenizer.hpp"

#include <re2/re2.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/serializer.h>
#include <vpack/slice.h>

#include <string_view>

namespace irs::analysis {
namespace {

constexpr std::string_view kPatternParamName = "pattern";
constexpr std::string_view kGroupParamName = "group";

bool ParseVPackOptions(const vpack::Slice slice,
                       PatternTokenizer::Options& options) {
  if (!slice.isObject()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for pattern_token_stream is not an object");
    return false;
  }

  auto r = vpack::ReadObjectNothrow(slice, options,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    SDB_WARN(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Failed to parse pattern_token_stream options: ", r.errorMessage());
    return false;
  }

  if (options.pattern.empty()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Missing 'pattern' while constructing pattern_token_stream from "
              "VPack arguments");
    return false;
  }

  return true;
}

Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  PatternTokenizer::Options options;
  if (ParseVPackOptions(slice, options)) {
    return PatternTokenizer::make(options.pattern, options.group);
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

bool MakeVPackConfig(std::string_view pattern, int group,
                     vpack::Builder* vpack_builder) {
  vpack::ObjectBuilder object(vpack_builder);
  vpack_builder->add(kPatternParamName, pattern);
  vpack_builder->add(kGroupParamName, group);
  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice,
                          vpack::Builder* vpack_builder) {
  PatternTokenizer::Options options;
  if (ParseVPackOptions(slice, options)) {
    return MakeVPackConfig(options.pattern, options.group, vpack_builder);
  }
  return false;
}

bool NormalizeVPackConfig(std::string_view args, std::string& definition) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  vpack::Builder builder;
  bool res = NormalizeVPackConfig(slice, &builder);
  if (res) {
    definition.assign(builder.slice().startAs<char>(),
                      builder.slice().byteSize());
  }
  return res;
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while constructing pattern_token_stream");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing pattern_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing pattern_token_stream from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing pattern_token_stream");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    vpack::Builder vpack_builder;
    if (NormalizeVPackConfig(vpack->slice(), &vpack_builder)) {
      definition = vpack_builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while normalizing pattern_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing pattern_token_stream from JSON");
  }
  return false;
}

}  // namespace

PatternTokenizer::PatternTokenizer(std::string_view pattern, int group)
  : _pattern(pattern, re2::RE2::Quiet),
    _group(group),
    _current_pos(0),
    _exhausted(false),
    _num_groups(_pattern.NumberOfCapturingGroups()) {
  _matches.resize(std::max(1, _num_groups + 1));
}
PatternTokenizer::~PatternTokenizer() = default;

Analyzer::ptr PatternTokenizer::make(std::string_view pattern, int group) {
  re2::RE2 re(pattern, re2::RE2::Quiet);
  if (!re.ok()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Invalid regex while constructing pattern_token_stream");
    return nullptr;
  }
  return std::make_unique<PatternTokenizer>(pattern, group);
}

void PatternTokenizer::init() {
  REGISTER_ANALYZER_VPACK(PatternTokenizer, MakeVPack, NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(PatternTokenizer, MakeJson, NormalizeJsonConfig);
}

bool PatternTokenizer::reset(std::string_view data) {
  _data = data;
  _current_pos = 0;
  _exhausted = false;
  return true;
}

bool PatternTokenizer::next() {
  if (_exhausted || _data.empty()) {
    return false;
  }

  auto& offset_attr = std::get<OffsAttr>(_attrs);
  auto& term_attr = std::get<TermAttr>(_attrs);
  auto& inc_attr = std::get<IncAttr>(_attrs);

  const char* const data_base = _data.data();
  const size_t data_len = _data.size();

  while (_current_pos <= data_len) {
    re2::StringPiece input(data_base + _current_pos, data_len - _current_pos);

    if (!_pattern.Match(input, 0, input.size(), re2::RE2::UNANCHORED,
                        _matches.data(), _matches.size())) {
      if (_group < 0 && _current_pos < data_len) {
        const size_t start = _current_pos;
        const size_t end = data_len;

        offset_attr.start = static_cast<uint32_t>(start);
        offset_attr.end = static_cast<uint32_t>(end);
        term_attr.value =
          ViewCast<byte_type>(std::string_view(data_base + start, end - start));
        inc_attr.value = 1;

        _exhausted = true;
        return true;
      }

      _exhausted = true;
      return false;
    }

    const auto& match = _matches[0];

    size_t match_start = _current_pos + (match.data() - input.data());
    size_t match_end = match_start + match.length();

    if (_group >= 0) {
      if (_group <= _num_groups) {
        const auto& g = _matches[_group];

        if (!g.empty()) {
          const size_t start = _current_pos + (g.data() - input.data());
          const size_t end = start + g.length();

          offset_attr.start = static_cast<uint32_t>(start);
          offset_attr.end = static_cast<uint32_t>(end);
          term_attr.value = ViewCast<byte_type>(g);
          inc_attr.value = 1;

          _current_pos = match_end;
          return true;
        }
      }

      _current_pos = (match.length() == 0) ? _current_pos + 1 : match_end;
      continue;
    }

    if (match_start > _current_pos) {
      size_t start = _current_pos;
      size_t end = match_start;

      offset_attr.start = static_cast<uint32_t>(start);
      offset_attr.end = static_cast<uint32_t>(end);
      term_attr.value =
        ViewCast<byte_type>(std::string_view(data_base + start, end - start));
      inc_attr.value = 1;

      _current_pos = match_end;
      return true;
    }

    _current_pos = (match.length() == 0) ? _current_pos + 1 : match_end;
  }

  return false;
}

}  // namespace irs::analysis
