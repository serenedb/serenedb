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
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "stopwords_tokenizer.hpp"

#include <absl/strings/escaping.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <cctype>  // for std::isspace(...)
#include <string_view>

#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

bool HexDecode(std::string& buf, std::string_view value) {
  buf.clear();
  if (absl::HexStringToBytes(value, &buf)) {
    return true;
  }
  SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
           "Invalid HEX masked token: ", value);
  return false;
}

Analyzer::ptr Construct(const vpack::ArrayIterator& mask, bool hex) {
  size_t offset = 0;
  StopwordsTokenizer::stopwords_set tokens;

  std::string token;
  for (auto itr = mask.begin(); itr.valid(); ++itr, ++offset) {
    if (!(*itr).isString()) {
      SDB_WARN(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Non-string value in 'mask' at offset '", offset,
        "' while constructing token_stopwords_stream from VPack arguments");

      return nullptr;
    }
    auto value = (*itr).stringView();
    if (!hex) {
      // interpret verbatim
      tokens.emplace(value);
    } else if (HexDecode(token, value)) {
      tokens.emplace(std::move(token));
    } else {
      return nullptr;  // hex-decoding failed
    }
  }
  return std::make_unique<StopwordsTokenizer>(std::move(tokens));
}

constexpr std::string_view kStopwordsParamName = "stopwords";
constexpr std::string_view kHexParamName = "hex";

// args is a jSON encoded object with the following attributes:
// "mask"(string-list): the HEX encoded token values to mask <required>
// if HEX conversion fails for any token then it is matched verbatim
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  switch (slice.type()) {
    case vpack::ValueType::Array:
      return Construct(vpack::ArrayIterator(slice),
                       false);  // arrays are always verbatim
    case vpack::ValueType::Object: {
      auto hex_slice = slice.get(kHexParamName);
      if (!hex_slice.isBool() && !hex_slice.isNone()) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Invalid vpack while constructing "
                               "token_stopwords_stream from VPack arguments. ",
                               kHexParamName, " value should be boolean."));
        return nullptr;
      }
      bool hex = hex_slice.isTrue();
      auto mask_slice = slice.get(kStopwordsParamName);
      if (mask_slice.isArray()) {
        return Construct(vpack::ArrayIterator(mask_slice), hex);
      }
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Invalid vpack while constructing "
                             "token_stopwords_stream from VPack arguments. ",
                             kStopwordsParamName, " value should be array."));
      return nullptr;
    }
    default: {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Invalid vpack while constructing token_stopwords_stream from VPack "
        "arguments. Array or Object was expected.");
    }
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
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while constructing token_stopwords_stream ");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing token_stopwords_stream from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing token_stopwords_stream from JSON");
  }
  return nullptr;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  switch (slice.type()) {
    case vpack::ValueType::Array: {  // always normalize to object for
                                     // consistency reasons
      builder->openObject();
      builder->add(kStopwordsParamName, slice);
      builder->add(kHexParamName, false);
      builder->close();
      return true;
    }
    case vpack::ValueType::Object: {
      auto hex_slice = slice.get(kHexParamName);
      if (!hex_slice.isBool() && !hex_slice.isNone()) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Invalid vpack while normalizing token_stopwords_stream "
                       "from VPack arguments. ",
                       kHexParamName, " value should be boolean."));
        return false;
      }
      bool hex = hex_slice.isTrue();
      auto mask_slice = slice.get(kStopwordsParamName);
      if (mask_slice.isArray()) {
        builder->openObject();
        builder->add(kStopwordsParamName, mask_slice);
        builder->add(kHexParamName, hex);
        builder->close();
        return true;
      }
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Invalid vpack while constructing "
                             "token_stopwords_stream from VPack arguments. ",
                             kStopwordsParamName, " value should be array."));
      return false;
    }
    default: {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        "Invalid vpack while normalizing token_stopwords_stream from VPack "
        "arguments. Array or Object was expected.");
    }
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
                "Null arguments while normalizing token_stopwords_stream");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return true;
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while normalizing token_stopwords_stream from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while normalizing token_stopwords_stream from JSON");
  }
  return false;
}

}  // namespace

StopwordsTokenizer::StopwordsTokenizer(
  StopwordsTokenizer::stopwords_set&& stopwords)
  : _stopwords{std::move(stopwords)} {}

void StopwordsTokenizer::init() {
  REGISTER_ANALYZER_VPACK(StopwordsTokenizer, MakeVPack, NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(StopwordsTokenizer, MakeJson, NormalizeJsonConfig);
}

bool StopwordsTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  _term_eof = true;

  return true;
}

bool StopwordsTokenizer::reset(std::string_view data) {
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = uint32_t(data.size());
  auto& term = std::get<TermAttr>(_attrs);
  term.value = ViewCast<byte_type>(data);
  _term_eof = _stopwords.contains(data);
  return true;
}

}  // namespace irs::analysis
