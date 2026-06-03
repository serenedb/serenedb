////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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

#include "common_defines.h"

#include <ostream>
#include <string>

#include "basics/static_strings.h"

namespace sdb::rest {

std::string ContentTypeToString(ContentType type) {
  switch (type) {
    case ContentType::Text:
      return StaticStrings::kMimeTypeText;
    case ContentType::Html:
      return StaticStrings::kMimeTypeHtml;
    case ContentType::Dump:
      return StaticStrings::kMimeTypeDump;
    case ContentType::Custom:
      return {};  // use value from headers
    case ContentType::Unset:
    case ContentType::Json:
    default:
      return StaticStrings::kMimeTypeJson;
  }
}

ContentType StringToContentType(const std::string& val, ContentType def) {
  if (val.size() >= StaticStrings::kMimeTypeJsonNoEncoding.size() &&
      val.compare(0, StaticStrings::kMimeTypeJsonNoEncoding.size(),
                  StaticStrings::kMimeTypeJsonNoEncoding) == 0) {
    return ContentType::Json;
  }
  if (val.starts_with(StaticStrings::kMimeTypeDumpNoEncoding)) {
    return ContentType::Dump;
  }
  if (val.starts_with(StaticStrings::kMimeTypeTextNoEncoding)) {
    return ContentType::Text;
  }
  if (val.starts_with(StaticStrings::kMimeTypeHtmlNoEncoding)) {
    return ContentType::Html;
  }
  return def;
}

}  // namespace sdb::rest
