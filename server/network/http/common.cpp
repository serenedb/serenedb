////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "network/http/common.h"

namespace sdb::network::http {

std::string SqlLiteral(std::string_view text) {
  std::string out;
  out.reserve(text.size() + 2);
  out.push_back('\'');
  for (const char c : text) {
    if (c == '\'') {
      out.push_back('\'');
    }
    out.push_back(c);
  }
  out.push_back('\'');
  return out;
}

std::string SqlIdentifier(std::string_view name) {
  std::string out;
  out.reserve(name.size() + 2);
  out.push_back('"');
  for (const char c : name) {
    if (c == '"') {
      out.push_back('"');
    }
    out.push_back(c);
  }
  out.push_back('"');
  return out;
}

std::string FlattenBody(const message::SequenceView& body) {
  std::string out;
  for (const auto buffer : body) {
    out.append(reinterpret_cast<const char*>(buffer.data()), buffer.size());
  }
  return out;
}

}  // namespace sdb::network::http
