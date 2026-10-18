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
#include <memory>
#include <string>
#include <string_view>

namespace bench {

class LineSource {
 public:
  explicit LineSource(int fd);
  explicit LineSource(const std::string& path);
  ~LineSource();

  LineSource(const LineSource&) = delete;
  LineSource& operator=(const LineSource&) = delete;

  bool Ok() const noexcept { return _fd >= 0; }

  bool Stable() const noexcept { return _map != nullptr; }

  const char* End() const noexcept { return _end; }

  bool Next(std::string_view& line);

 private:
  void Map();
  bool Read();

  int _fd = -1;
  bool _owns = false;
  char* _map = nullptr;
  size_t _map_size = 0;
  const char* _pos = nullptr;
  const char* _end = nullptr;
  std::unique_ptr<char[]> _buf;
  size_t _cap = 0;
  size_t _size = 0;
  size_t _begin = 0;
  bool _eof = false;
};

}  // namespace bench
