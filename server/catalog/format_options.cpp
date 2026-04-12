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

#include "catalog/format_options.h"

#include "query/utils.h"

namespace sdb {

void TextFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", std::to_underlying(_format));
  b.add("delim", _delim);
  b.add("escape", _escape);
  b.add("null_string", std::string_view{_null_string});
  b.add("header", _header);
}

void ParquetFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", std::to_underlying(_format));
}

void DwrfFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", std::to_underlying(_format));
}

void OrcFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", std::to_underlying(_format));
}

std::shared_ptr<FormatOptions> FormatOptions::fromVPack(vpack::Slice slice) {
  if (!slice.isObject()) {
    return nullptr;
  }
  auto format_slice = slice.get("format");
  if (!format_slice.isNumber()) {
    return nullptr;
  }
  switch (static_cast<FileFormat>(format_slice.getNumber<uint8_t>())) {
    case FileFormat::Text: {
      uint8_t delim = '\t';
      if (auto s = slice.get("delim"); s.isNumber()) {
        delim = s.getNumber<uint8_t>();
      }
      uint8_t escape = '\\';
      if (auto s = slice.get("escape"); s.isNumber()) {
        escape = s.getNumber<uint8_t>();
      }
      std::string null_string = "\\N";
      if (auto s = slice.get("null_string"); s.isString()) {
        null_string = std::string{s.stringView()};
      }
      uint8_t header = 0;
      if (auto s = slice.get("header"); s.isNumber()) {
        header = s.getNumber<uint8_t>();
      }
      return std::make_shared<TextFormatOptions>(
        delim, escape, std::move(null_string), header);
    }
    case FileFormat::Parquet:
      return std::make_shared<ParquetFormatOptions>();
    case FileFormat::Dwrf:
      return std::make_shared<DwrfFormatOptions>();
    case FileFormat::Orc:
      return std::make_shared<OrcFormatOptions>();
    case FileFormat::None:
      break;
  }
  return nullptr;
}

}  // namespace sdb
