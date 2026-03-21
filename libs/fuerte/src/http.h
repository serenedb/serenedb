////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// @author Jan Christoph Uhde
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <fuerte/message.h>
#include <fuerte/types.h>

#include <string>
#include <string_view>

namespace sdb::fuerte::http {

void UrlDecode(std::string& out, std::string_view str);

/// url-decodes str and returns it - convenience function
inline std::string UrlDecode(std::string_view str) {
  std::string result;
  UrlDecode(result, str);
  return result;
}

/// url-encodes str into out - convenience function
void UrlEncode(std::string& out, std::string_view str);

/// url-encodes str and returns it - convenience function
inline std::string UrlEncode(std::string_view str) {
  std::string result;
  UrlEncode(result, str);
  return result;
}

void AppendPath(const Request& req, std::string& target);

}  // namespace sdb::fuerte::http
