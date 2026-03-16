////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "memory_directory.hpp"

#include <algorithm>
#include <cstring>

#include "basics/assert.h"
#include "basics/crc.hpp"
#include "basics/logger/logger.h"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

class SingleInstanceLock : public IndexLock {
 public:
  SingleInstanceLock(std::string_view name, MemoryDirectory* parent)
    : _name{name}, _parent{parent} {
    SDB_ASSERT(parent);
  }

  bool lock() final {
    std::lock_guard lock{_parent->_llock};
    return _parent->_locks.insert(_name).second;
  }

  bool is_locked(bool& result) const noexcept final {
    std::lock_guard lock{_parent->_llock};
    result = _parent->_locks.contains(_name);
    return true;
  }

  bool unlock() noexcept final {
    std::lock_guard lock{_parent->_llock};
    return _parent->_locks.erase(_name) != 0;
  }

 private:
  std::string _name;
  MemoryDirectory* _parent;
};

const byte_type* MemoryIndexInput::ReadData(uint64_t count) noexcept {
  const auto* begin = _begin + count;

  if (begin > _end) {
    return nullptr;
  }

  std::swap(begin, _begin);
  return begin;
}

const byte_type* MemoryIndexInput::ReadData(uint64_t offset,
                                            uint64_t count) noexcept {
  const auto idx = _file->buffer_offset(offset);
  SDB_ASSERT(idx < _file->buffer_count());
  const auto& buf = _file->get_buffer(idx);
  const auto begin = buf.data + offset - buf.offset;
  const auto end = begin + count;
  const auto buf_end =
    buf.data + std::min(buf.size, _file->Length() - buf.offset);

  if (end <= buf_end) {
    _buf = buf.data;
    _begin = end;
    _end = buf_end;
    _start = buf.offset;
    return begin;
  }

  return nullptr;
}

size_t MemoryIndexInput::ReadBytes(byte_type* b, size_t count) {
  const auto bytes_left = count;  // initial length
  while (count) {
    if (_begin >= _end) {
      if (IsEOF()) {
        break;
      }
      SwitchBuffer(Position());
    }

    auto copied = std::min<size_t>(std::distance(_begin, _end), count);
    std::memcpy(b, _begin, sizeof(byte_type) * copied);

    count -= copied;
    _begin += copied;
    b += copied;
  }
  return bytes_left - count;
}

void MemoryIndexInput::Seek(uint64_t pos) {
  // allow seeking past eof(), set to eof
  if (pos >= _file->Length()) {
    _buf = nullptr;
    _begin = nullptr;
    _start = _file->Length();
    return;
  }

  SwitchBuffer(pos);
}

uint32_t MemoryIndexInput::Checksum(uint64_t offset) const {
  if (!_file->Length()) {
    return 0;
  }

  Crc32c crc;

  auto buffer_idx = _file->buffer_offset(Position());
  size_t to_process;

  // process current buffer if exists
  if (_begin) {
    to_process = std::min(offset, Remain());
    crc.process_bytes(_begin, to_process);
    offset -= to_process;
    ++buffer_idx;
  }

  // process intermediate buffers
  auto last_buffer_idx = _file->buffer_count();

  if (last_buffer_idx) {
    --last_buffer_idx;
  }

  for (; offset && buffer_idx < last_buffer_idx; ++buffer_idx) {
    auto& buf = _file->get_buffer(buffer_idx);
    to_process = std::min(offset, buf.size);
    crc.process_bytes(buf.data, to_process);
    offset -= to_process;
  }

  // process the last buffer
  if (offset && buffer_idx == last_buffer_idx) {
    auto& buf = _file->get_buffer(last_buffer_idx);
    to_process = std::min(offset, _file->Length() - buf.offset);
    crc.process_bytes(buf.data, to_process);
  }

  return crc.checksum();
}

void MemoryIndexInput::SwitchBuffer(size_t pos) {
  auto idx = _file->buffer_offset(pos);
  SDB_ASSERT(idx < _file->buffer_count());
  auto& buf = _file->get_buffer(idx);

  if (buf.data != _buf) {
    _buf = buf.data;
    _start = buf.offset;
    _end = _buf + std::min(buf.size, _file->Length() - _start);
  }

  SDB_ASSERT(_start <= pos && pos < _start + std::distance(_buf, _end));
  _begin = _buf + (pos - _start);
}

void MemoryIndexOutput::FlushBuffer() {
  SDB_ASSERT(Remain() == 0);
  auto idx = _file.buffer_offset(Position());
  auto buf =
    idx < _file.buffer_count() ? _file.get_buffer(idx) : _file.push_buffer();
  _offset = buf.offset;
  _buf = buf.data;
  _pos = _buf;
  _end = _buf + buf.size;
}

MemoryIndexOutput::MemoryIndexOutput(MemoryFile& file) noexcept
  : IndexOutput{nullptr, nullptr}, _file{file} {}

void MemoryIndexOutput::Truncate(size_t pos) noexcept {
  auto idx = _file.buffer_offset(pos);
  SDB_ASSERT(idx < _file.buffer_count());
  auto buf = _file.get_buffer(idx);
  _offset = buf.offset;
  _buf = buf.data;
  _pos = _buf + pos - _offset;
  _end = _buf + buf.size;
}

void MemoryIndexOutput::WriteDirect(const byte_type* b, size_t len) {
  Write(*this, b, len);
}

class ChecksumMemoryIndexOutput final : public MemoryIndexOutput {
 public:
  explicit ChecksumMemoryIndexOutput(MemoryFile& file) noexcept
    : MemoryIndexOutput{file}, _crc_begin{_pos} {}

  uint32_t Checksum() noexcept final {
    Flush();

    FlushBuffer<true>();

    return _crc.checksum();
  }

 private:
  friend class BufferedOutput;

  template<bool CrcOnly = false>
  void FlushBuffer() {
    _crc.process_bytes(_crc_begin, _pos - _crc_begin);
    if constexpr (!CrcOnly) {
      MemoryIndexOutput::FlushBuffer();
    }
    _crc_begin = _pos;
  }

  void WriteBuffer(const byte_type* b, size_t len) {
    SDB_ASSERT(_pos);
    SDB_ASSERT(b);
    SDB_ASSERT(len <= Remain());
    if (_crc_begin == _pos) [[unlikely]] {
      _crc.copy_bytes(_pos, b, len);
      _crc_begin += len;
    } else {
      std::memcpy(_pos, b, len);
    }
    _pos += len;
  }

  void WriteDirect(const byte_type* b, size_t len) final {
    Write(*this, b, len);
  }

  byte_type* _crc_begin;
  Crc32c _crc;
};

MemoryDirectory::MemoryDirectory(DirectoryAttributes attrs,
                                 const ResourceManagementOptions& rm)
  : Directory{rm},
    _attrs{std::move(attrs)},
    _files{FilesAllocator{*rm.readers}} {}

MemoryDirectory::~MemoryDirectory() noexcept {
  std::lock_guard lock{_flock};

  _files.clear();
}

bool MemoryDirectory::exists(bool& result,
                             std::string_view name) const noexcept {
  absl::ReaderMutexLock lock{&_flock};

  result = _files.contains(name);

  return true;
}

IndexOutput::ptr MemoryDirectory::create(std::string_view name) noexcept try {
  std::lock_guard lock{_flock};

  auto it = _files.try_emplace(name).first;
  auto& file = it->second;

  if (file) {
    file->Reset();
  } else {
    irs::Finally cleanup = [&]() noexcept {
      if (!file) {
        _files.erase(it);
      }
    };
    file = std::make_unique<MemoryFile>(_files.get_allocator().Manager());
  }

  return IndexOutput::ptr{new ChecksumMemoryIndexOutput{*file}};
} catch (...) {
  return nullptr;
}

bool MemoryDirectory::length(uint64_t& result,
                             std::string_view name) const noexcept {
  absl::ReaderMutexLock lock{&_flock};

  const auto it = _files.find(name);

  if (it == _files.end()) {
    return false;
  }

  result = it->second->Length();

  return true;
}

IndexLock::ptr MemoryDirectory::make_lock(std::string_view name) noexcept try {
  return IndexLock::ptr{new SingleInstanceLock{name, this}};
} catch (...) {
  SDB_ASSERT(false);
  return nullptr;
}

bool MemoryDirectory::mtime(std::time_t& result,
                            std::string_view name) const noexcept {
  absl::ReaderMutexLock lock{&_flock};
  const auto it = _files.find(name);
  if (it == _files.end()) {
    return false;
  }
  result = it->second->mtime();
  return true;
}

IndexInput::ptr MemoryDirectory::open(std::string_view name,
                                      IOAdvice /*advice*/) const noexcept try {
  absl::ReaderMutexLock lock{&_flock};
  const auto it = _files.find(name);
  if (it != _files.end()) {
    return std::make_unique<MemoryIndexInput>(*it->second);
  }
  SDB_ERROR(
    "xxxxx", sdb::Logger::IRESEARCH,
    absl::StrCat("Failed to open input file, error: File not found, path: ",
                 name));
  return nullptr;
} catch (...) {
  SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
            absl::StrCat("Failed to open input file, path: ", name));
  return nullptr;
}

bool MemoryDirectory::remove(std::string_view name) noexcept {
  std::lock_guard lock{_flock};

  return _files.erase(name) != 0;
}

bool MemoryDirectory::rename(std::string_view src,
                             std::string_view dst) noexcept {
  try {
    std::lock_guard lock{_flock};

    const auto res = _files.try_emplace(dst);
    auto it = _files.find(src);

    if (it != _files.end()) [[likely]] {
      if (it != res.first) [[likely]] {
        res.first->second = std::move(it->second);
        _files.erase(it);
      }
      return true;
    }

    if (res.second) {
      _files.erase(res.first);
    }
  } catch (...) {
  }

  return false;
}

bool MemoryDirectory::visit(const Directory::visitor_f& visitor) const {
  std::vector<std::string> files;
  // take a snapshot of existing files in directory
  // to avoid potential recursive read locks in visitor
  {
    absl::ReaderMutexLock lock{&_flock};
    files.append_range(_files | std::views::keys);
  }
  for (auto& file : files) {
    if (!visitor(file)) {
      return false;
    }
  }
  return true;
}

}  // namespace irs
