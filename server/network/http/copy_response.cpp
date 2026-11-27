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

#include "network/http/copy_response.h"

namespace sdb::network::http {

size_t CopyResponse::Write(std::string_view bytes) {
  if (_streaming) {
    Send(bytes);
  } else {
    _buffer.append(bytes);
    if (_buffer.size() > _held) {
      _streaming = true;
      _writer.WriteHeadChunked(HttpStatus::Ok, _content_type, _headers);
      Send(_buffer);
      _buffer = {};
    }
  }
  return _writer.Committed();
}

void CopyResponse::Send(std::string_view bytes) {
  if (!_writer.Broken()) {
    _writer.Write(bytes);
  }
}

void CopyResponse::Finish() {
  if (_streaming) {
    _writer.Finish();
  } else {
    _writer.Fixed(HttpStatus::Ok, _content_type, _buffer, _headers);
  }
}

}  // namespace sdb::network::http
