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

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>

#include "connector/duckdb_copy_filesystem.h"
#include "network/http/response_writer.h"

namespace sdb::network::http {

class CopyResponse final : public connector::CopyOutput {
 public:
  CopyResponse(HttpResponseWriter& writer, std::string_view content_type,
               std::string headers, size_t held)
    : _writer{writer},
      _content_type{content_type},
      _headers{std::move(headers)},
      _held{held} {}

  size_t Write(std::string_view bytes) final;
  void Finish();

 private:
  void Send(std::string_view bytes);

  HttpResponseWriter& _writer;
  std::string_view _content_type;
  std::string _headers;
  size_t _held;
  std::string _buffer;
  bool _streaming = false;
};

}  // namespace sdb::network::http
