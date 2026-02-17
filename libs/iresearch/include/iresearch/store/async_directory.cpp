////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "async_directory.hpp"

#include <absl/strings/str_cat.h>
#include <liburing.h>

#include "basics/crc.hpp"
#include "basics/file_utils_ext.hpp"
#include "iresearch/error/error.hpp"

namespace irs {
namespace {

constexpr size_t kNumPages = 128;
constexpr size_t kPageSize = 4096;
constexpr size_t kPageAlignment = 4096;

struct BufferDeleter {
  void operator()(byte_type* memory) const noexcept { ::free(memory); }
};

std::unique_ptr<byte_type, BufferDeleter> Allocate() {
  constexpr size_t kBufSize = kNumPages * kPageSize;

  void* mem = nullptr;
  if (posix_memalign(&mem, kPageAlignment, kBufSize) != 0) {
    throw std::bad_alloc();
  }

  return std::unique_ptr<byte_type, BufferDeleter>{
    static_cast<byte_type*>(mem)};
}

class SegregatedBuffer {
 public:
  using NodeType = ConcurrentStack<byte_type*>::NodeType;

  SegregatedBuffer();

  NodeType* Pop() noexcept { return _free_pages.pop(); }

  void Push(NodeType& node) noexcept { _free_pages.push(node); }

  constexpr size_t Size() const noexcept { return kNumPages * kPageSize; }

  byte_type* Data() const noexcept { return _buffer.get(); }

 private:
  std::unique_ptr<byte_type, BufferDeleter> _buffer;
  NodeType _pages[kNumPages];
  ConcurrentStack<byte_type*> _free_pages;
};

SegregatedBuffer::SegregatedBuffer() : _buffer{Allocate()} {
  auto* begin = _buffer.get();
  for (auto& page : _pages) {
    page.value = begin;
    _free_pages.push(page);
    begin += kPageSize;
  }
}

class URing {
 public:
  URing(size_t queue_size, unsigned flags) {
    if (io_uring_queue_init(queue_size, &ring, flags) != 0) {
      throw NotSupported{};
    }
  }

  ~URing() { io_uring_queue_exit(&ring); }

  void RegBuffer(byte_type* b, size_t size);

  size_t Size() const noexcept { return ring.sq.ring_sz; }

  io_uring_sqe* GetSqe() noexcept { return io_uring_get_sqe(&ring); }

  size_t SqReady() noexcept { return io_uring_sq_ready(&ring); }

  void Submit();
  bool Deque(bool wait, uint64_t* data);

  io_uring ring;
};

void URing::RegBuffer(byte_type* b, size_t size) {
  iovec iovec{.iov_base = b, .iov_len = size};
  if (io_uring_register_buffers(&ring, &iovec, 1) != 0) {
    throw IllegalState("failed to register buffer");
  }
}

void URing::Submit() {
  const int ret = io_uring_submit(&ring);
  if (ret < 0) {
    throw IoError{absl::StrCat("failed to submit write request, error ", -ret)};
  }
}

bool URing::Deque(bool wait, uint64_t* data) {
  io_uring_cqe* cqe = nullptr;
  const int ret =
    wait ? io_uring_wait_cqe(&ring, &cqe) : io_uring_peek_cqe(&ring, &cqe);

  if (ret < 0) {
    if (ret != -EAGAIN) {
      throw IoError{absl::StrCat("Failed to peek a request, error ", -ret)};
    }

    return false;
  }

  SDB_ASSERT(cqe != nullptr);
  if (cqe->res < 0) {
    throw IoError{
      absl::StrCat("Async i/o operation failed, error ", -cqe->res)};
  }

  *data = cqe->user_data;
  io_uring_cqe_seen(&ring, cqe);

  return true;
}

}  // namespace

class AsyncFile {
 public:
  AsyncFile(size_t queue_size, unsigned flags) : _ring(queue_size, flags) {
    _ring.RegBuffer(_buffer.Data(), _buffer.Size());
  }

  io_uring_sqe* GetSqe();
  void Submit() { return _ring.Submit(); }
  void Drain(bool wait);

  SegregatedBuffer::NodeType* GetBuffer();
  void ReleaseBuffer(SegregatedBuffer::NodeType& node) noexcept {
    _buffer.Push(node);
  }

 private:
  SegregatedBuffer _buffer;
  URing _ring;
};

AsyncFileBuilder::ptr AsyncFileBuilder::make(size_t queue_size,
                                             unsigned flags) {
  return AsyncFileBuilder::ptr(new AsyncFile(queue_size, flags));
}

void AsyncFileDeleter::operator()(AsyncFile* p) noexcept { delete p; }

SegregatedBuffer::NodeType* AsyncFile::GetBuffer() {
  uint64_t data = 0;
  while (_ring.Deque(false, &data) && data == 0) {
  }

  if (data != 0) {
    return reinterpret_cast<SegregatedBuffer::NodeType*>(data);
  }

  auto* node = _buffer.Pop();

  if (node != nullptr) {
    return node;
  }

  while (_ring.Deque(true, &data) && data == 0) {
  }
  return reinterpret_cast<SegregatedBuffer::NodeType*>(data);
}

io_uring_sqe* AsyncFile::GetSqe() {
  io_uring_sqe* sqe = _ring.GetSqe();

  if (sqe == nullptr) {
    uint64_t data = 0;

    if (!_ring.Deque(false, &data)) {
      SDB_ASSERT(_ring.SqReady());
      _ring.Submit();
      _ring.Deque(true, &data);
    }

    sqe = _ring.GetSqe();

    if (data != 0) {
      _buffer.Push(*reinterpret_cast<SegregatedBuffer::NodeType*>(&data));
    }
  }

  SDB_ASSERT(sqe);
  return sqe;
}

void AsyncFile::Drain(bool wait) {
  io_uring_sqe* sqe = GetSqe();
  SDB_ASSERT(sqe);

  io_uring_prep_nop(sqe);
  sqe->flags |= (IOSQE_IO_LINK | IOSQE_IO_DRAIN);
  sqe->user_data = 0;

  _ring.Submit();

  if (!wait) {
    return;
  }

  uint64_t data = 0;
  for (;;) {
    _ring.Deque(true, &data);

    if (data == 0) {
      return;
    }

    _buffer.Push(*reinterpret_cast<SegregatedBuffer::NodeType*>(data));
  }
}

class AsyncIndexOutput final : public IndexOutput {
 public:
  static IndexOutput::ptr Open(const path_char_t* name,
                               AsyncFilePtr&& async) noexcept;

  void Flush() final;

  uint32_t Checksum() final {
    Flush();
    return _crc.checksum();
  }

 private:
  size_t CloseImpl() final {
    Finally reset = [this]() noexcept {
      _async->ReleaseBuffer(*_node);
      _handle.reset();
    };

    Flush();

    // FIXME(gnusi): we can avoid waiting here in case
    // if we'll keep track of all unsynced files
    _async->Drain(true);
    return Position();
  }

  void WriteDirect(const byte_type* b, size_t len) final;

  using NodeType = ConcurrentStack<byte_type*>::NodeType;

  AsyncIndexOutput(AsyncFilePtr&& async, file_utils::handle_t&& handle) noexcept
    : IndexOutput{nullptr, nullptr},
      _async{std::move(async)},
      _handle{std::move(handle)} {
    Reset(_async->GetBuffer());
  }

  void Reset(NodeType* node) noexcept {
    SDB_ASSERT(node);
    _node = node;
    _buf = _pos = node->value;
    _end = _buf + kPageSize;
    SDB_ASSERT(_buf);
  }

  void PrepareSQE(size_t len) {
    auto* sqe = _async->GetSqe();
    io_uring_prep_write_fixed(sqe, HANDLE_CAST(_handle.get()), _buf, len,
                              _offset, 0);
    sqe->user_data = reinterpret_cast<uint64_t>(_node);
    _offset += len;
  }

  AsyncFilePtr _async;
  file_utils::handle_t _handle;
  NodeType* _node = nullptr;
  Crc32c _crc;
};

IndexOutput::ptr AsyncIndexOutput::Open(const path_char_t* name,
                                        AsyncFilePtr&& async) noexcept {
  SDB_ASSERT(name);

  if (!async) {
    return nullptr;
  }

  file_utils::handle_t handle(
    file_utils::Open(name, file_utils::OpenMode::Write, IR_FADVICE_NORMAL));

  if (nullptr == handle) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to open output file, error: ", GET_ERROR(),
                           ", path: ", file_utils::ToStr(name)));

    return nullptr;
  }

  try {
    return IndexOutput::ptr{
      new AsyncIndexOutput{std::move(async), std::move(handle)}};
  } catch (...) {
  }

  return nullptr;
}

void AsyncIndexOutput::Flush() {
  SDB_ASSERT(_handle);
  const auto len = Length();
  if (len == 0) {
    return;
  }
  _crc.process_bytes(_buf, len);
  PrepareSQE(len);

  _async->Submit();

  Reset(_async->GetBuffer());
}

void AsyncIndexOutput::WriteDirect(const byte_type* b, size_t len) {
  SDB_ASSERT(_handle);
  auto remain = Remain();
  SDB_ASSERT(len >= remain);

  WriteBuffer(b, remain);
  _crc.process_bytes(_buf, kPageSize);
  PrepareSQE(kPageSize);
  b += remain;
  len -= remain;

  while (len >= kPageSize) {
    Reset(_async->GetBuffer());
    _crc.copy_bytes(_pos, b, kPageSize);
    PrepareSQE(kPageSize);
    b += kPageSize;
    len -= kPageSize;
  }

  _async->Submit();

  Reset(_async->GetBuffer());

  WriteBuffer(b, len);
}

AsyncDirectory::AsyncDirectory(std::filesystem::path dir,
                               DirectoryAttributes attrs,
                               const ResourceManagementOptions& rm,
                               size_t pool_size, size_t queue_size,
                               unsigned flags)
  : MMapDirectory{std::move(dir), std::move(attrs), rm},
    _async_pool{pool_size},
    _queue_size{queue_size},
    _flags{flags} {}

IndexOutput::ptr AsyncDirectory::create(std::string_view name) noexcept {
  std::filesystem::path path;

  try {
    (path /= this->path()) /= name;

    return AsyncIndexOutput::Open(path.c_str(),
                                  _async_pool.emplace(_queue_size, _flags));
  } catch (...) {
  }

  return nullptr;
}

bool AsyncDirectory::sync(std::span<const std::string_view> names) noexcept {
  std::filesystem::path path;

  try {
    std::vector<file_utils::handle_t> handles(names.size());
    path /= this->path();

    auto async = _async_pool.emplace(_queue_size, _flags);

    for (auto name = names.begin(); auto& handle : handles) {
      std::filesystem::path full_path(path);
      full_path /= (*name);
      ++name;

      const int fd = ::open(full_path.c_str(), O_WRONLY, S_IRWXU);

      if (fd < 0) {
        return false;
      }

      handle.reset(reinterpret_cast<void*>(fd));

      auto* sqe = async->GetSqe();

      while (sqe == nullptr) {
        async->Submit();
        sqe = async->GetSqe();
      }

      io_uring_prep_fsync(sqe, fd, 0);
      sqe->user_data = 0;
    }

    // FIXME(gnusi): or submit one-by-one?
    async->Submit();
    async->Drain(true);
  } catch (...) {
    return false;
  }

  return true;
}

}  // namespace irs
