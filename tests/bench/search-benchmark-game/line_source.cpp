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

#include "line_source.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

namespace bench {
namespace {

constexpr size_t kChunk = size_t{1} << 20;

}  // namespace

LineSource::LineSource(int fd) : _fd{fd} { Map(); }

LineSource::LineSource(const std::string& path)
  : _fd{::open(path.c_str(), O_RDONLY | O_CLOEXEC)}, _owns{true} {
  if (_fd >= 0) {
    Map();
  }
}

LineSource::~LineSource() {
  if (_map != nullptr) {
    ::munmap(_map, _map_size);
  }
  if (_owns && _fd >= 0) {
    ::close(_fd);
  }
}

void LineSource::Map() {
  struct stat st{};
  if (::fstat(_fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size == 0) {
    return;
  }
  const auto offset = ::lseek(_fd, 0, SEEK_CUR);
  if (offset < 0 || offset >= st.st_size) {
    return;
  }
  const auto size = static_cast<size_t>(st.st_size);
  auto* map = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, _fd, 0);
  if (map == MAP_FAILED) {
    return;
  }
  ::madvise(map, size, MADV_SEQUENTIAL);
  _map = static_cast<char*>(map);
  _map_size = size;
  _pos = _map + offset;
  _end = _map + size;
}

bool LineSource::Read() {
  if (_begin != 0) {
    std::memmove(_buf.get(), _buf.get() + _begin, _size - _begin);
    _size -= _begin;
    _begin = 0;
  }
  if (_size == _cap) {
    const auto cap = std::max(2 * _cap, kChunk);
    auto buf = std::make_unique_for_overwrite<char[]>(cap);
    if (_size != 0) {
      std::memcpy(buf.get(), _buf.get(), _size);
    }
    _buf = std::move(buf);
    _cap = cap;
  }
  ssize_t n;
  do {
    n = ::read(_fd, _buf.get() + _size, _cap - _size);
  } while (n < 0 && errno == EINTR);
  if (n > 0) {
    _size += static_cast<size_t>(n);
  }
  _end = _buf.get() + _size;
  if (n <= 0) {
    _eof = true;
    return false;
  }
  return true;
}

bool LineSource::Next(std::string_view& line) {
  if (_map != nullptr) {
    if (_pos == _end) {
      return false;
    }
    const auto* nl =
      static_cast<const char*>(std::memchr(_pos, '\n', _end - _pos));
    const auto* stop = nl != nullptr ? nl : _end;
    line = {_pos, static_cast<size_t>(stop - _pos)};
    _pos = nl != nullptr ? nl + 1 : _end;
    return true;
  }
  if (_fd < 0) {
    return false;
  }
  size_t scanned = _begin;
  for (;;) {
    if (const auto* nl = static_cast<const char*>(
          std::memchr(_buf.get() + scanned, '\n', _size - scanned))) {
      const auto at = static_cast<size_t>(nl - _buf.get());
      line = {_buf.get() + _begin, at - _begin};
      _begin = at + 1;
      return true;
    }
    scanned = _size - _begin;
    if (_eof || !Read()) {
      if (_begin == _size) {
        return false;
      }
      line = {_buf.get() + _begin, _size - _begin};
      _begin = _size;
      return true;
    }
  }
}

}  // namespace bench
