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
#include <cstring>
#include <string>
#include <string_view>

namespace sdb::network::pg {

class CopyEodScanner {
 public:
  struct Result {
    std::string_view carry;
    std::string_view data;
  };

  Result Scan(std::string_view chunk) {
    Result r;
    if (_ended) {
      return r;
    }
    while (!_pending.empty() && !chunk.empty()) {
      _pending.push_back(chunk.front());
      if (_pending == "\\.\n" || _pending == "\\.\r\n") {
        _ended = true;
        _pending.clear();
        return r;
      }
      if (IsMarkerPrefix(_pending)) {
        chunk.remove_prefix(1);
        continue;
      }
      _pending.pop_back();
      _escaped = _pending == "\\";
      _at_line_start = false;
      _carry = std::move(_pending);
      _pending.clear();
      r.carry = _carry;
      break;
    }
    if (!_pending.empty()) {
      return r;
    }
    r.data = ScanChunk(chunk);
    return r;
  }

  std::string_view Finish() {
    if (_ended || _pending == "\\.") {
      _ended = true;
      _pending.clear();
      return {};
    }
    _carry = std::move(_pending);
    _pending.clear();
    return _carry;
  }

  bool Ended() const noexcept { return _ended; }

 private:
  static constexpr size_t kNone = std::string_view::npos;

  static bool IsMarkerPrefix(std::string_view s) noexcept {
    return s == "\\" || s == "\\." || s == "\\.\r";
  }

  std::string_view Hold(std::string_view chunk, size_t at) {
    _pending = chunk.substr(at);
    return chunk.substr(0, at);
  }

  std::string_view ScanChunk(std::string_view chunk) {
    const char* const data = chunk.data();
    const size_t size = chunk.size();
    size_t escaped = _escaped ? 0 : kNone;
    for (size_t pos = 0; pos < size;) {
      const auto* hit =
        static_cast<const char*>(std::memchr(data + pos, '\\', size - pos));
      if (!hit) {
        break;
      }
      const auto begin = static_cast<size_t>(hit - data);
      auto end = begin + 1;
      while (end < size && data[end] == '\\') {
        ++end;
      }
      const bool line_start =
        begin == 0 ? _at_line_start
                   : data[begin - 1] == '\n' && escaped != begin - 1;
      if (line_start && end - begin == 1) {
        if (end == size || (data[end] == '.' && end + 1 == size)) {
          return Hold(chunk, begin);
        }
        if (data[end] == '.') {
          if (data[end + 1] == '\n') {
            _ended = true;
            return chunk.substr(0, begin);
          }
          if (data[end + 1] == '\r') {
            if (end + 2 == size) {
              return Hold(chunk, begin);
            }
            if (data[end + 2] == '\n') {
              _ended = true;
              return chunk.substr(0, begin);
            }
          }
        }
      }
      const bool odd = ((end - begin) % 2 != 0) != (begin == escaped);
      escaped = odd ? end : kNone;
      pos = end;
    }
    if (size != 0) {
      _escaped = escaped == size;
      _at_line_start = data[size - 1] == '\n' && escaped != size - 1;
    }
    return chunk;
  }

  std::string _pending;
  std::string _carry;
  bool _at_line_start = true;
  bool _escaped = false;
  bool _ended = false;
};

}  // namespace sdb::network::pg
