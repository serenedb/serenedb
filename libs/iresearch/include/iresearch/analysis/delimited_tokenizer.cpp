////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "delimited_tokenizer.hpp"

#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

namespace irs::analysis {
namespace {

bytes_view EvalTerm(bstring& buf, bytes_view data) {
  if (data.empty() || '"' != data[0]) {
    return data;  // not a quoted term (even if quotes inside
  }

  buf.clear();

  bool escaped = false;
  size_t start = 1;

  for (size_t i = 1, count = data.size(); i < count; ++i) {
    if ('"' == data[i]) {
      if (escaped && start == i) {  // an escaped quote
        escaped = false;

        continue;
      }

      if (escaped) {
        break;  // mismatched quote
      }

      buf.append(&data[start], i - start);
      escaped = true;
      start = i + 1;
    }
  }

  return start != 1 && start == data.size()
           ? bytes_view(buf)
           : data;  // return identity for mismatched quotes
}

size_t FindDelimiter(bytes_view data, bytes_view delim) {
  if (IsNull(delim)) {
    return data.size();
  }

  bool quoted = false;

  for (size_t i = 0, count = data.size(); i < count; ++i) {
    if (quoted) {
      if ('"' == data[i]) {
        quoted = false;
      }

      continue;
    }

    if (data.size() - i < delim.size()) {
      break;  // no more delimiters in data
    }

    if (0 == memcmp(data.data() + i, delim.data(), delim.size()) &&
        (i || delim.size())) {  // do not match empty delim at data start
      return i;  // delimiter match takes precedence over '"' match
    }

    if ('"' == data[i]) {
      quoted = true;
    }
  }

  return data.size();
}

constexpr std::string_view kDelimiterParamName{"delimiter"};

bool ParseVPackOptions(const vpack::Slice slice, std::string& delimiter) {
  if (!slice.isObject() && !slice.isString()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for delimited_token_stream is not an object or string");
    return false;
  }

  switch (slice.type()) {
    case vpack::ValueType::String:
      delimiter = slice.stringView();
      return true;
    case vpack::ValueType::Object:
      if (auto delim_type_slice = slice.get(kDelimiterParamName);
          !delim_type_slice.isNone()) {
        if (!delim_type_slice.isString()) {
          SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid type '",
                   kDelimiterParamName,
                   "' (string expected) for delimited_token_stream from "
                   "VPack arguments");
          return false;
        }
        delimiter = delim_type_slice.stringView();
        return true;
      }
      break;
    default:
      break;
  }

  SDB_ERROR(
    "xxxxx", sdb::Logger::IRESEARCH,
    absl::StrCat(
      "Missing '", kDelimiterParamName,
      "' while constructing delimited_token_stream from VPack arguments"));

  return false;
}

// args is a jSON encoded object with the following attributes:
// "delimiter"(string): the delimiter to use for tokenization <required>
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  std::string delimiter;
  if (ParseVPackOptions(slice, delimiter)) {
    return DelimitedTokenizer::make(delimiter);
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

// builds analyzer config from internal options in json format
// delimiter reference to analyzer options storage
// definition string for storing json document with config
bool MakeVPackConfig(std::string_view delimiter,
                     vpack::Builder* vpack_builder) {
  vpack::ObjectBuilder object(vpack_builder);
  {
    // delimiter
    vpack_builder->add(kDelimiterParamName, delimiter);
  }

  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice,
                          vpack::Builder* vpack_builder) {
  std::string delimiter;
  if (ParseVPackOptions(slice, delimiter)) {
    return MakeVPackConfig(delimiter, vpack_builder);
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
                "Null arguments while constructing delimited_token_stream");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing delimited_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing delimited_token_stream from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing delimited_token_stream");
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
                   "' while normalizing delimited_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while normalizing delimited_token_stream from JSON");
  }
  return false;
}

// args is a delimiter to use for tokenization
Analyzer::ptr MakeText(std::string_view args) {
  return std::make_unique<DelimitedTokenizer>(args);
}

bool NormalizeTextConfig(std::string_view delimiter, std::string& definition) {
  definition = delimiter;
  return true;
}

}  // namespace

DelimitedTokenizer::DelimitedTokenizer(std::string_view delimiter)
  : _delim(ViewCast<byte_type>(delimiter)) {
  if (!IsNull(_delim)) {
    _delim_buf = _delim;  // keep a local copy of the delimiter
    _delim = _delim_buf;  // update the delimter to point at the local copy
  }
}

Analyzer::ptr DelimitedTokenizer::make(std::string_view delimiter) {
  return MakeText(delimiter);
}

void DelimitedTokenizer::init() {
  REGISTER_ANALYZER_VPACK(DelimitedTokenizer, MakeVPack, NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(DelimitedTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_TEXT(DelimitedTokenizer, MakeText, NormalizeTextConfig);
}

bool DelimitedTokenizer::next() {
  if (IsNull(_data)) {
    return false;
  }

  auto& offset = std::get<OffsAttr>(_attrs);

  auto size = FindDelimiter(_data, _delim);
  auto next = std::max<size_t>(1, size + _delim.size());
  auto start = offset.end + static_cast<uint32_t>(_delim.size());
  // value is allowed to overflow, will only produce invalid result
  auto end = start + size;

  if (std::numeric_limits<uint32_t>::max() < end) {
    return false;  // cannot fit the next token into offset calculation
  }

  auto& term = std::get<TermAttr>(_attrs);

  offset.start = start;
  offset.end = static_cast<uint32_t>(end);
  term.value = IsNull(_delim)
                 ? bytes_view{_data.data(), size}
                 : EvalTerm(_term_buf, bytes_view(_data.data(), size));
  _data = size >= _data.size()
            ? bytes_view{}
            : bytes_view{_data.data() + next, _data.size() - next};

  return true;
}

bool DelimitedTokenizer::reset(std::string_view data) {
  _data = ViewCast<byte_type>(data);

  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  // counterpart to computation in next() above
  offset.end = 0 - static_cast<uint32_t>(_delim.size());

  return true;
}

}  // namespace irs::analysis
