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

#include "fs_directory.hpp"

#include <absl/strings/str_cat.h>

#include "basics/crc.hpp"
#include "basics/file_utils_ext.hpp"
#include "basics/logger/logger.h"
#include "basics/object_pool.hpp"
#include "basics/shared.hpp"
#include "basics/system-compiler.h"
#include "iresearch/error/error.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/store/directory_cleaner.hpp"

#ifdef _WIN32
#include <Windows.h>  // for GetLastError()
#endif

namespace irs {
namespace {

// Converts the specified IOAdvice to corresponding posix fadvice
inline int GetPosixFadvice(IOAdvice advice) noexcept {
  switch (advice) {
    case IOAdvice::NORMAL:
    case IOAdvice::DirectRead:
      return IR_FADVICE_NORMAL;
    case IOAdvice::SEQUENTIAL:
      return IR_FADVICE_SEQUENTIAL;
    case IOAdvice::RANDOM:
      return IR_FADVICE_RANDOM;
    case IOAdvice::READONCE:
      return IR_FADVICE_DONTNEED;
    case IOAdvice::ReadonceSequential:
      return IR_FADVICE_SEQUENTIAL | IR_FADVICE_NOREUSE;
    case IOAdvice::ReadonceRandom:
      return IR_FADVICE_RANDOM | IR_FADVICE_NOREUSE;
  }

  SDB_ERROR(
    "xxxxx", sdb::Logger::IRESEARCH,
    absl::StrCat("fadvice '", static_cast<uint32_t>(advice),
                 "' is not valid (RANDOM|SEQUENTIAL), fallback to NORMAL"));

  return IR_FADVICE_NORMAL;
}

inline file_utils::OpenMode GetReadMode(IOAdvice advice) {
  if (IOAdvice::DirectRead == (advice & IOAdvice::DirectRead)) {
    return file_utils::OpenMode::Read | file_utils::OpenMode::Direct;
  }
  return file_utils::OpenMode::Read;
}

}  // namespace

class FsLock : public IndexLock {
 public:
  FsLock(const std::filesystem::path& dir, std::string_view file)
    : _dir{dir}, _file{file} {}

  bool lock() final {
    if (_handle) {
      // don't allow self obtaining
      return false;
    }

    bool exists;

    if (!file_utils::Exists(exists, _dir.c_str())) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Error caught");
      return false;
    }

    // create directory if it is not exists
    if (!exists && !file_utils::Mkdir(_dir.c_str(), true)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Error caught");
      return false;
    }

    const auto path = _dir / _file;

    // create lock file
    if (!file_utils::VerifyLockFile(path.c_str())) {
      if (!file_utils::Exists(exists, path.c_str()) ||
          (exists && !file_utils::Remove(path.c_str()))) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Error caught");
        return false;
      }

      _handle = file_utils::CreateLockFile(path.c_str());
    }

    return _handle != nullptr;
  }

  bool is_locked(bool& result) const noexcept final {
    if (_handle != nullptr) {
      result = true;
      return true;
    }

    try {
      const auto path = _dir / _file;

      result = file_utils::VerifyLockFile(path.c_str());
      return true;
    } catch (...) {
    }

    return false;
  }

  bool unlock() noexcept final {
    if (_handle != nullptr) {
      _handle = nullptr;
#ifdef _WIN32
      // file will be automatically removed on close
      return true;
#else
      const auto path = _dir / _file;

      return file_utils::Remove(path.c_str());
#endif
    }

    return false;
  }

 private:
  std::filesystem::path _dir;
  std::string _file;
  file_utils::lock_handle_t _handle;
};

class FSIndexOutput final : public IndexOutput {
 public:
  static ptr Open(const path_char_t* name,
                  const ResourceManagementOptions& rm) noexcept {
    SDB_ASSERT(name);
    size_t descriptors = 0;
    size_t memory = 0;
    try {
      rm.file_descriptors->Increase(1);
      descriptors = 1;

      auto handle =
        file_utils::Open(name, file_utils::OpenMode::Write, IR_FADVICE_NORMAL);
      if (handle) [[likely]] {
        rm.transactions->Increase(sizeof(FSIndexOutput));
        memory = sizeof(FSIndexOutput);

        ptr ptr{new FSIndexOutput{std::move(handle), rm}};
        descriptors = 0;
        memory = 0;
        return ptr;
      }

      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to open output file, error: ", GET_ERROR(),
                             ", path: ", file_utils::ToStr(name)));
    } catch (...) {
    }
    rm.file_descriptors->DecreaseChecked(descriptors);
    rm.transactions->DecreaseChecked(memory);
    return nullptr;
  }

 private:
  void Flush() final {
    WriteHandle(_buf, Length());
    _pos = _buf;
  }

  uint32_t Checksum() final {
    Flush();
    return _crc.checksum();
  }

  uint64_t CloseImpl() final {
    Flush();
    const auto size = Position();

    _handle.reset();
    _rm.file_descriptors->Decrease(1);
    _crc = Crc32c{};
    _offset = 0;

    return size;
  }

  void WriteDirect(const byte_type* b, size_t len) final {
    auto remain = Remain();
    SDB_ASSERT(len >= remain);
    const auto option =
      static_cast<size_t>(Length() != 0) * 2 + static_cast<size_t>(remain != 0);
    switch (option) {
      case 2 + 1:
        WriteBuffer(b, remain);
        b += remain;
        len -= remain;
        [[fallthrough]];
      case 2 + 0:
        Flush();
        remain = Remain();
        [[fallthrough]];
      case 0 + 1:
        if (remain <= len) {
          const auto buffer_len = len % remain;
          const auto direct_len = len - buffer_len;
          WriteHandle(b, direct_len);
          if (buffer_len == 0) {
            return;
          }
          b += direct_len;
          len = buffer_len;
        }
        WriteBuffer(b, len);
        break;
      default:
        SDB_UNREACHABLE();
    }
  }

  void WriteHandle(const byte_type* b, size_t len) {
    SDB_ASSERT(_handle);
    const auto written = file_utils::Fwrite(_handle.get(), b, len);
    _crc.process_bytes(b, written);
    _offset += written;
    if (len != written) [[unlikely]] {
      throw IoError{
        absl::StrCat("Failed to FSIndexOutput::WriteDirect, written '", written,
                     "' out of '", len, "' bytes")};
    }
  }

  FSIndexOutput(file_utils::handle_t handle,
                const ResourceManagementOptions& rm) noexcept
    : IndexOutput{_buf, std::end(_buf)}, _rm{rm}, _handle{std::move(handle)} {
    SDB_ASSERT(_handle);
  }

  ~FSIndexOutput() final {
    SDB_ASSERT(!_handle);
    _rm.transactions->Decrease(sizeof(FSIndexOutput));
  }

  // TODO(mbkkt) larger buf_ size?
  byte_type _buf[1024];
  const ResourceManagementOptions& _rm;
  file_utils::handle_t _handle;
  Crc32c _crc;
};

class PooledFsIndexInput;

class FsIndexInput : public BufferedIndexInput {
 public:
  uint64_t CountMappedMemory() const final { return 0; }

  using BufferedIndexInput::ReadInternal;

  uint32_t Checksum(size_t offset) const final {
    // "ReadInternal" modifies pos_
    Finally restore_position = [pos = this->_pos, this]() noexcept {
      const_cast<FsIndexInput*>(this)->_pos = pos;
    };

    const auto begin = _pos;
    const auto end = (std::min)(begin + offset, _handle->size);

    Crc32c crc;
    byte_type buf[sizeof _buf];

    for (auto pos = begin; pos < end;) {
      const auto to_read = (std::min)(end - pos, sizeof buf);
      pos += const_cast<FsIndexInput*>(this)->ReadInternal(buf, to_read);
      crc.process_bytes(buf, to_read);
    }

    return crc.checksum();
  }

  ptr Dup() const override { return ptr(new FsIndexInput(*this)); }

  static IndexInput::ptr Open(const path_char_t* name, size_t pool_size,
                              IOAdvice advice,
                              const ResourceManagementOptions& rm) noexcept
    try {
    SDB_ASSERT(name);

    size_t descriptors = 1;
    rm.file_descriptors->Increase(descriptors);
    Finally cleanup = [&]() noexcept {
      rm.file_descriptors->DecreaseChecked(descriptors);
    };

    auto handle = FileHandle::make(rm);
    handle->io_advice = advice;
    handle->handle = file_utils::Open(name, GetReadMode(handle->io_advice),
                                      GetPosixFadvice(handle->io_advice));

    if (nullptr == handle->handle) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to open input file, error: ", GET_ERROR(),
                             ", path: ", file_utils::ToStr(name)));
      return nullptr;
    }
    uint64_t size;
    if (!file_utils::ByteSize(size, *handle)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to get stat for input file, error: ",
                             GET_ERROR(), ", path: ", file_utils::ToStr(name)));
      return nullptr;
    }

    handle->size = size;

    ptr input{new FsIndexInput(std::move(handle), pool_size)};
    descriptors = 0;
    return input;
  } catch (...) {
    return nullptr;
  }

  uint64_t Length() const final { return _handle->size; }

  ptr Reopen() const override;

 protected:
  void SeekInternal(size_t pos) final {
    if (pos > _handle->size) {
      throw IoError{absl::StrCat("seek out of range for input file, length '",
                                 _handle->size, "', position '", pos, "'")};
    }

    _pos = pos;
  }

  size_t ReadInternal(byte_type* b, size_t len) final {
    SDB_ASSERT(b);
    SDB_ASSERT(_handle->handle);

    void* fd = *_handle;

    if (_handle->pos != _pos) {
      if (irs::file_utils::Fseek(fd, static_cast<long>(_pos), SEEK_SET) != 0) {
        throw IoError{absl::StrCat("failed to seek to '", _pos,
                                   "' for input file, error '",
                                   irs::file_utils::Ferror(fd), "'")};
      }
      _handle->pos = _pos;
    }

    const size_t read = irs::file_utils::Fread(fd, b, sizeof(byte_type) * len);
    _pos = _handle->pos += read;

    SDB_ASSERT(_handle->pos == _pos);
    return read;
  }

 private:
  friend PooledFsIndexInput;

  /* use shared wrapper here since we don't want to
   * call "ftell" every time we need to know current
   * position */
  struct FileHandle {
    using ptr = std::shared_ptr<FileHandle>;

    static ptr make(const ResourceManagementOptions& rm) {
      return std::make_shared<FileHandle>(rm);
    }

    FileHandle(const ResourceManagementOptions& rm) noexcept
      : resource_manager{rm} {}

    ~FileHandle() {
      const auto released = static_cast<size_t>(handle != nullptr);
      handle.reset();
      resource_manager.file_descriptors->DecreaseChecked(released);
    }
    operator void*() const { return handle.get(); }

    file_utils::handle_t handle; /* native file handle */
    size_t size{};               /* file size */
    size_t pos{};                /* current file position*/
    IOAdvice io_advice{IOAdvice::NORMAL};
    const ResourceManagementOptions& resource_manager;
  };

  FsIndexInput(FileHandle::ptr&& handle, size_t pool_size);

  FsIndexInput(const FsIndexInput& rhs);

  ~FsIndexInput() override;

  FsIndexInput& operator=(const FsIndexInput&) = delete;

  byte_type _buf[1024];
  FileHandle::ptr _handle;  // shared file handle
  size_t _pool_size;  // size of pool for instances of pooled_fs_index_input
  size_t _pos;        // current input stream position
};

class PooledFsIndexInput final : public FsIndexInput {
 public:
  explicit PooledFsIndexInput(const FsIndexInput& in);
  ~PooledFsIndexInput() noexcept final;
  IndexInput::ptr Dup() const final {
    return ptr(new PooledFsIndexInput(*this));
  }
  IndexInput::ptr Reopen() const final;

 private:
  struct Builder {
    using ptr = std::unique_ptr<FileHandle>;

    static std::unique_ptr<FileHandle> make(
      const ResourceManagementOptions& rm) {
      return std::make_unique<FileHandle>(rm);
    }
  };

  using FdPoolT = UnboundedObjectPool<Builder>;
  std::shared_ptr<FdPoolT> _fd_pool;

  PooledFsIndexInput(const PooledFsIndexInput& in) = default;
  FileHandle::ptr Reopen(const FileHandle& src) const;
};

FsIndexInput::FsIndexInput(FileHandle::ptr&& handle, size_t pool_size)
  : _handle(std::move(handle)), _pool_size(pool_size), _pos(0) {
  SDB_ASSERT(_handle);
  _handle->resource_manager.readers->Increase(sizeof(PooledFsIndexInput));
  BufferedIndexInput::reset(_buf, sizeof _buf, 0);
}

FsIndexInput::FsIndexInput(const FsIndexInput& rhs)
  : _handle(rhs._handle), _pool_size(rhs._pool_size), _pos(rhs.Position()) {
  SDB_ASSERT(_handle);
  _handle->resource_manager.readers->Increase(sizeof(PooledFsIndexInput));
  BufferedIndexInput::reset(_buf, sizeof _buf, _pos);
}

FsIndexInput::~FsIndexInput() {
  if (_handle) {
    auto& r = *_handle->resource_manager.readers;
    _handle.reset();
    r.Decrease(sizeof(PooledFsIndexInput));
  }
}

IndexInput::ptr FsIndexInput::Reopen() const {
  return std::make_unique<PooledFsIndexInput>(*this);
}

PooledFsIndexInput::PooledFsIndexInput(const FsIndexInput& in)
  : FsIndexInput(in), _fd_pool(std::make_shared<FdPoolT>(_pool_size)) {
  _handle = Reopen(*_handle);
}

PooledFsIndexInput::~PooledFsIndexInput() noexcept {
  SDB_ASSERT(_handle);
  auto& r = *_handle->resource_manager.readers;
  _handle.reset();  // release handle before the fs_pool_ is deallocated
  r.Decrease(sizeof(PooledFsIndexInput));
}

IndexInput::ptr PooledFsIndexInput::Reopen() const {
  auto ptr = Dup();
  SDB_ASSERT(ptr);

  auto& in = static_cast<PooledFsIndexInput&>(*ptr);
  in._handle = Reopen(*_handle);  // reserve a new handle from pool
  SDB_ASSERT(in._handle && in._handle->handle);

  return ptr;
}

FsIndexInput::FileHandle::ptr PooledFsIndexInput::Reopen(
  const FileHandle& src) const {
  // reserve a new handle from the pool
  std::shared_ptr<FsIndexInput::FileHandle> handle{
    const_cast<PooledFsIndexInput*>(this)->_fd_pool->emplace(
      src.resource_manager)};
  size_t descriptors{0};
  irs::Finally cleanup = [&]() noexcept {
    src.resource_manager.file_descriptors->DecreaseChecked(descriptors);
  };
  if (!handle->handle) {
    src.resource_manager.file_descriptors->Increase(1);
    descriptors = 1;
    handle->handle = irs::file_utils::Open(
      src, GetReadMode(src.io_advice),
      GetPosixFadvice(
        src.io_advice));  // same permission as in fs_index_input::open(...)

    if (!handle->handle) {
      // even win32 uses 'errno' for error codes in calls to file_open(...)
      throw IoError{
        absl::StrCat("Failed to reopen input file, error: ", GET_ERROR())};
    }
    descriptors = 0;
    handle->io_advice = src.io_advice;
  }

  const auto pos = irs::file_utils::Ftell(
    handle->handle.get());  // match position of file descriptor

  if (pos < 0) {
    throw IoError{absl::StrCat(
      "Failed to obtain current position of input file, error: ", GET_ERROR())};
  }

  handle->pos = pos;
  handle->size = src.size;
  return handle;
}

FSDirectory::FSDirectory(std::filesystem::path dir, DirectoryAttributes attrs,
                         const ResourceManagementOptions& rm,
                         size_t fd_pool_size)
  : Directory{rm},
    _attrs{std::move(attrs)},
    _dir{std::move(dir)},
    _fd_pool_size{fd_pool_size} {}

IndexOutput::ptr FSDirectory::create(std::string_view name) noexcept {
  try {
    const auto path = _dir / name;

    auto out = FSIndexOutput::Open(path.c_str(), ResourceManager());

    if (!out) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to open output file, path: ", name));
    }

    return out;
  } catch (...) {
  }

  return nullptr;
}

const std::filesystem::path& FSDirectory::path() const noexcept { return _dir; }

bool FSDirectory::exists(bool& result, std::string_view name) const noexcept {
  const auto path = _dir / name;

  return file_utils::Exists(result, path.c_str());
}

bool FSDirectory::length(uint64_t& result,
                         std::string_view name) const noexcept {
  const auto path = _dir / name;

  return file_utils::ByteSize(result, path.c_str());
}

IndexLock::ptr FSDirectory::make_lock(std::string_view name) noexcept {
  return IndexLock::ptr{new FsLock{_dir, name}};
}

bool FSDirectory::mtime(std::time_t& result,
                        std::string_view name) const noexcept {
  const auto path = _dir / name;

  return file_utils::Mtime(result, path.c_str());
}

bool FSDirectory::remove(std::string_view name) noexcept {
  try {
    const auto path = _dir / name;

    return file_utils::Remove(path.c_str());
  } catch (...) {
  }

  return false;
}

IndexInput::ptr FSDirectory::open(std::string_view name,
                                  IOAdvice advice) const noexcept {
  try {
    const auto path = _dir / name;

    return FsIndexInput::Open(path.c_str(), _fd_pool_size, advice,
                              ResourceManager());
  } catch (...) {
  }

  return nullptr;
}

bool FSDirectory::rename(std::string_view src, std::string_view dst) noexcept {
  try {
    const auto src_path = _dir / src;
    const auto dst_path = _dir / dst;

    return file_utils::Move(src_path.c_str(), dst_path.c_str());
  } catch (...) {
  }

  return false;
}

bool FSDirectory::visit(const Directory::visitor_f& visitor) const {
  bool exists;

  if (!file_utils::ExistsDirectory(exists, _dir.c_str()) || !exists) {
    return false;
  }

#ifdef _WIN32
  std::filesystem::path path;
  auto dir_visitor = [&path, &visitor](const path_char_t* name) {
    path = name;

    auto filename = path.string();
    return visitor(filename);
  };
#else
  std::string filename;
  auto dir_visitor = [&filename, &visitor](const path_char_t* name) {
    filename.assign(name);
    return visitor(filename);
  };
#endif

  return file_utils::VisitDirectory(_dir.c_str(), dir_visitor, false);
}

bool FSDirectory::sync(std::string_view name) noexcept {
  try {
    const auto path = _dir / name;

    if (file_utils::FileSync(path.c_str())) {
      return true;
    }

    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to sync file, error: ", GET_ERROR(),
                           ", path: ", path.string()));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to sync file, name: ", name));
  }

  return false;
}

bool FSDirectory::sync(std::span<const std::string_view> files) noexcept {
  return absl::c_all_of(files, [this](std::string_view name) mutable noexcept {
    return this->sync(name);
  });
}

bool CachingFSDirectory::length(uint64_t& result,
                                std::string_view name) const noexcept {
  if (_cache.Visit(name, [&](const auto length) noexcept {
        result = length;
        return true;
      })) {
    return true;
  }

  return FSDirectory::length(result, name);
}

IndexInput::ptr CachingFSDirectory::open(std::string_view name,
                                         IOAdvice advice) const noexcept {
  auto stream = FSDirectory::open(name, advice);

  if ((IOAdvice::READONCE != (advice & IOAdvice::READONCE)) && stream) {
    _cache.Put(name, [&]() noexcept { return stream->Length(); });
  }

  return stream;
}

}  // namespace irs
