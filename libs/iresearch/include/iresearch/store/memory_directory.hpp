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

#pragma once

#include <absl/container/flat_hash_map.h>

#include "basics/resource_manager.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/directory_attributes.hpp"

namespace irs {

class MemoryFile : public container_utils::RawBlockVector<16, 8> {
  // total number of levels and size of the first level 2^8
  using Base = container_utils::RawBlockVector<16, 8>;

 public:
  explicit MemoryFile(IResourceManager& rm) noexcept : Base{rm} {
    _meta.mtime = now();
  }

  MemoryFile(MemoryFile&& rhs) noexcept
    : Base{static_cast<Base&&>(rhs)},
      _meta{rhs._meta},
      _len{std::exchange(rhs._len, 0)} {}

  MemoryFile& operator>>(DataOutput& out) {
    Visit([&](const byte_type* b, size_t len) {
      out.WriteBytes(b, len);
      return true;
    });
    return *this;
  }

  size_t Length() const noexcept { return _len; }

  void Length(size_t length) noexcept {
    _len = length;
    _meta.mtime = now();
  }

  std::time_t mtime() const noexcept { return _meta.mtime; }

  void Reset() noexcept { _len = 0; }

  void Clear() noexcept {
    Base::clear();
    Reset();
  }

  template<typename Visitor>
  bool Visit(const Visitor& visitor) {
    auto len = _len;
    for (const auto& buffer : _buffers) {
      const auto to_visit = std::min(len, buffer.size);
      if (!visitor(buffer.data, to_visit)) {
        return false;
      }
      len -= to_visit;
    }
    return true;
  }

 private:
  // metadata for a memory_file
  struct Meta {
    std::time_t mtime;
  };

  static std::time_t now() noexcept {
    return std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());
  }

  Meta _meta;
  size_t _len = 0;
};

class MemoryIndexInput final : public IndexInput {
 public:
  explicit MemoryIndexInput(const MemoryFile& file) noexcept : _file{&file} {}

  const byte_type* ReadData(uint64_t count) noexcept final;
  const byte_type* ReadData(uint64_t offset, uint64_t count) noexcept final;

  const byte_type* ReadView(uint64_t count) noexcept final {
    return ReadData(count);
  }
  const byte_type* ReadView(uint64_t offset, uint64_t count) noexcept final {
    return ReadData(offset, count);
  }

  byte_type ReadByte() final {
    if (_begin >= _end) {
      SwitchBuffer(Position());
    }
    return *_begin++;
  }
  size_t ReadBytes(byte_type* b, size_t count) final;
  size_t ReadBytes(uint64_t offset, byte_type* b, size_t count) final {
    Seek(offset);
    return ReadBytes(b, count);
  }

  int16_t ReadI16() final {
    return Remain() < sizeof(uint16_t) ? irs::read<uint16_t>(*this)
                                       : irs::read<uint16_t>(_begin);
  }
  int32_t ReadI32() final {
    return Remain() < sizeof(uint32_t) ? irs::read<uint32_t>(*this)
                                       : irs::read<uint32_t>(_begin);
  }
  int64_t ReadI64() final {
    return Remain() < sizeof(uint64_t) ? irs::read<uint64_t>(*this)
                                       : irs::read<uint64_t>(_begin);
  }
  uint32_t ReadV32() final {
    return Remain() < bytes_io<uint32_t>::kMaxVSize
             ? irs::vread<uint32_t>(*this)
             : irs::vread<uint32_t>(_begin);
  }
  uint64_t ReadV64() final {
    return Remain() < bytes_io<uint64_t>::kMaxVSize
             ? irs::vread<uint64_t>(*this)
             : irs::vread<uint64_t>(_begin);
  }

  uint64_t Position() const noexcept final {
    return _start + std::distance(_buf, _begin);
  }
  uint64_t Length() const noexcept final { return _file->Length(); }
  bool IsEOF() const noexcept final { return Position() >= Length(); }

  void Skip(uint64_t count) final { Seek(Position() + count); }
  void Seek(uint64_t pos) final;

  IndexInput::ptr Dup() const final {
    return std::make_unique<MemoryIndexInput>(*this);
  }
  IndexInput::ptr Reopen() const final { return Dup(); }

  uint32_t Checksum(uint64_t offset) const final;

  // TODO(mbkkt) remove this
  byte_type operator*() { return ReadByte(); }
  MemoryIndexInput& operator++() noexcept { return *this; }
  MemoryIndexInput& operator++(int) noexcept { return *this; }

 private:
  void SwitchBuffer(size_t pos);

  // returns number of reamining bytes in the buffer
  IRS_FORCE_INLINE uint64_t Remain() const {
    return std::distance(_begin, _end);
  }

  const MemoryFile* _file;        // underline file
  const byte_type* _buf{};        // current buffer
  const byte_type* _begin{_buf};  // current position
  const byte_type* _end{_buf};    // end of the valid bytes
  size_t _start{};                // buffer offset in file
};

class MemoryIndexOutput : public IndexOutput {
 public:
  explicit MemoryIndexOutput(MemoryFile& file) noexcept;

  void Reset() noexcept {
    _buf = _pos = _end = nullptr;
    _offset = 0;
  }

  void Truncate(size_t pos) noexcept;

  void Flush() noexcept final { _file.Length(Position()); }

  uint32_t Checksum() override { throw NotSupported{}; }

  uint64_t CloseImpl() noexcept final {
    // TODO(mbkkt) maybe we need reset?
    Flush();
    return Position();
  }

 protected:
  friend class BufferedOutput;

  void FlushBuffer();

 private:
  void WriteDirect(const byte_type* b, size_t len) override;

  MemoryFile& _file;
};

class MemoryDirectory final : public Directory {
 public:
  explicit MemoryDirectory(
    DirectoryAttributes attributes = DirectoryAttributes{},
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);

  ~MemoryDirectory() noexcept final;

  DirectoryAttributes& attributes() noexcept final { return _attrs; }

  IndexOutput::ptr create(std::string_view name) noexcept final;

  bool exists(bool& result, std::string_view name) const noexcept final;

  bool length(uint64_t& result, std::string_view name) const noexcept final;

  IndexLock::ptr make_lock(std::string_view name) noexcept final;

  bool mtime(std::time_t& result, std::string_view name) const noexcept final;

  IndexInput::ptr open(std::string_view name,
                       IOAdvice advice) const noexcept final;

  bool remove(std::string_view name) noexcept final;

  bool rename(std::string_view src, std::string_view dst) noexcept final;

  bool sync(std::span<const std::string_view>) noexcept final { return true; }

  bool visit(const visitor_f& visitor) const final;

 private:
  friend class SingleInstanceLock;

  using FilesAllocator = ManagedTypedAllocator<
    std::pair<const std::string, std::unique_ptr<MemoryFile>>>;
  using FileMap = absl::flat_hash_map<
    std::string, std::unique_ptr<MemoryFile>,
    absl::container_internal::hash_default_hash<std::string>,
    absl::container_internal::hash_default_eq<std::string>,
    FilesAllocator>;  // unique_ptr because of rename
  using LockMap = absl::flat_hash_set<std::string>;

  DirectoryAttributes _attrs;
  mutable absl::Mutex _flock;
  absl::Mutex _llock;
  FileMap _files;
  LockMap _locks;
};

struct MemoryOutput {
  explicit MemoryOutput(IResourceManager& rm) noexcept : file{rm} {}

  MemoryOutput(MemoryOutput&& rhs) noexcept : file{std::move(rhs.file)} {}

  void Reset() noexcept {
    file.Reset();
    stream.Reset();
  }

  MemoryFile file;
  MemoryIndexOutput stream{file};
};

}  // namespace irs
