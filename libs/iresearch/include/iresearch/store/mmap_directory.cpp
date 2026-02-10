////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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

#include "iresearch/store/mmap_directory.hpp"

#include "basics/file_utils_ext.hpp"
#include "iresearch/store/mmap_index_input.hpp"
#include "iresearch/utils/mmap_utils.hpp"

namespace irs {
namespace {

using mmap_utils::MMapHandle;

// Converts the specified IOAdvice to corresponding posix madvice
inline int GetPosixMadvice(IOAdvice advice) {
  switch (advice) {
    case IOAdvice::NORMAL:
    case IOAdvice::DirectRead:
      return IR_MADVICE_NORMAL;
    case IOAdvice::SEQUENTIAL:
      return IR_MADVICE_SEQUENTIAL;
    case IOAdvice::RANDOM:
      return IR_MADVICE_RANDOM;
    case IOAdvice::READONCE:
      return IR_MADVICE_NORMAL;
    case IOAdvice::ReadonceSequential:
      return IR_MADVICE_SEQUENTIAL;
    case IOAdvice::ReadonceRandom:
      return IR_MADVICE_RANDOM;
  }

  SDB_ERROR(
    "xxxxx", sdb::Logger::IRESEARCH,
    absl::StrCat("madvice '", static_cast<uint32_t>(advice),
                 "' is not valid (RANDOM|SEQUENTIAL), fallback to NORMAL"));

  return IR_MADVICE_NORMAL;
}

std::shared_ptr<MMapHandle> OpenHandle(const path_char_t* file, IOAdvice advice,
                                       IResourceManager& rm) noexcept {
  SDB_ASSERT(file);

  std::shared_ptr<MMapHandle> handle;

  try {
    handle = std::make_shared<MMapHandle>(rm);
  } catch (...) {
    return nullptr;
  }

  if (!handle->open(file)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to open mmapped input file, path: ",
                           file_utils::ToStr(file)));
    return nullptr;
  }

  if (0 == handle->size()) {
    return handle;
  }

  const int padvice = GetPosixMadvice(advice);

  if (IR_MADVICE_NORMAL != padvice && !handle->advise(padvice)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to madvise input file, path: ",
                           file_utils::ToStr(file), ", error ", errno));
  }

  handle->dontneed(bool(advice & IOAdvice::READONCE));

  return handle;
}

std::shared_ptr<MMapHandle> OpenHandle(const std::filesystem::path& dir,
                                       std::string_view name, IOAdvice advice,
                                       IResourceManager& rm) noexcept {
  try {
    const auto path = dir / name;

    return OpenHandle(path.c_str(), advice, rm);
  } catch (...) {
  }

  return nullptr;
}

#ifdef __linux__
size_t BytesInCache(uint8_t* addr, size_t length) {
  static const size_t kPageSize = sysconf(_SC_PAGESIZE);
  SDB_ASSERT(reinterpret_cast<uintptr_t>(addr) % kPageSize == 0);
  std::vector<uint8_t> pages(
    std::min(8 * kPageSize, (length + kPageSize - 1) / kPageSize), 0);
  size_t bytes = 0;
  auto count = [&](uint8_t* data, size_t bytes_size, size_t pages_size) {
    mincore(static_cast<void*>(data), bytes_size, pages.data());
    auto it = pages.begin();
    auto end = it + pages_size;
    for (; it != end; ++it) {
      if (*it != 0) {
        bytes += kPageSize;
      }
    }
  };

  const auto available_pages = pages.size();
  const auto available_space = available_pages * kPageSize;

  const auto* end = addr + length;
  while (addr + available_space < end) {
    count(addr, available_space, available_pages);
    addr += available_space;
  }
  if (addr != end) {
    const size_t bytes_size = end - addr;
    count(addr, bytes_size, (bytes_size + kPageSize - 1) / kPageSize);
  }
  return bytes;
}
#endif

}  // namespace

uint64_t MMapIndexInput::CountMappedMemory() const {
#ifdef __linux__
  return _handle ? BytesInCache(static_cast<uint8_t*>(_handle->addr()),
                                _handle->size())
                 : 0;
#else
  return 0;
#endif
}

MMapDirectory::MMapDirectory(std::filesystem::path path,
                             DirectoryAttributes attrs,
                             const ResourceManagementOptions& rm)
  : FSDirectory{std::move(path), std::move(attrs), rm} {}

IndexInput::ptr MMapDirectory::open(std::string_view name,
                                    IOAdvice advice) const noexcept {
  if (IOAdvice::DirectRead == (advice & IOAdvice::DirectRead)) {
    return FSDirectory::open(name, advice);
  }

  auto handle =
    OpenHandle(path(), name, advice, *ResourceManager().file_descriptors);

  if (!handle) {
    return nullptr;
  }

  try {
    return std::make_unique<MMapIndexInput>(std::move(handle));
  } catch (...) {
  }

  return nullptr;
}

bool CachingMMapDirectory::length(uint64_t& result,
                                  std::string_view name) const noexcept {
  if (_cache.Visit(name, [&](const auto& cached) noexcept {
        result = cached->size();
        return true;
      })) {
    return true;
  }

  return MMapDirectory::length(result, name);
}

IndexInput::ptr CachingMMapDirectory::open(std::string_view name,
                                           IOAdvice advice) const noexcept {
  if (bool(advice & (IOAdvice::READONCE | IOAdvice::DirectRead))) {
    return MMapDirectory::open(name, advice);
  }

  auto make_stream = [](auto&& handle) noexcept -> IndexInput::ptr {
    SDB_ASSERT(handle);

    try {
      return std::make_unique<MMapIndexInput>(std::move(handle));
    } catch (...) {
    }

    return nullptr;
  };

  std::shared_ptr<mmap_utils::MMapHandle> handle;

  if (_cache.Visit(name, [&](const auto& cached) noexcept {
        handle = cached;
        return handle != nullptr;
      })) {
    return make_stream(std::move(handle));
  }

  handle =
    OpenHandle(path(), name, advice, *ResourceManager().file_descriptors);
  if (handle) {
    _cache.Put(name, [&]() noexcept { return handle; });

    return make_stream(std::move(handle));
  }

  return nullptr;
}

}  // namespace irs
