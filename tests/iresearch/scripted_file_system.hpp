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

#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/memory_mapped_file.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>
#include <vector>

#include "gtest/gtest.h"
#include "hook_file_system.hpp"

namespace tests {

struct PrefetchRange {
  uint64_t offset = 0;
  uint64_t size = 0;
};

class ScriptedMapping;

void RegisterFaulting(ScriptedMapping& mapping);
void UnregisterFaulting(ScriptedMapping& mapping) noexcept;
void DropFaultingMappings();

class ScriptedMapping final : public duckdb::MemoryMappedFile {
 public:
  ScriptedMapping(irs::bstring bytes, bool faults)
    : MemoryMappedFile{{},
                       duckdb::FileOpenFlags::FILE_FLAGS_READ,
                       Reserve(bytes.size()),
                       static_cast<duckdb::idx_t>(bytes.size())},
      _bytes{std::move(bytes)},
      _faults{faults} {
    KeepUntil(size);
    if (_faults) {
      RegisterFaulting(*this);
    }
  }
  ~ScriptedMapping() final {
    if (_faults) {
      UnregisterFaulting(*this);
    }
    ::munmap(data, Reserved(size));
  }

  void Sync() final {}
  bool Trim(duckdb::idx_t /*offset*/, duckdb::idx_t /*length*/) final {
    return false;
  }
  void Close() final {}
  void Advise(duckdb::MappedFileAccess /*access*/) final {}
  void Prefetch(duckdb::idx_t location, duckdb::idx_t nr_bytes) final {
    prefetched.emplace_back(location, nr_bytes);
    Load(location / Page(), Pages(location + nr_bytes));
  }
  void Evict(duckdb::idx_t /*location*/, duckdb::idx_t /*nr_bytes*/) final {}

  void Drop(uint64_t offset, uint64_t length) {
    const auto first = offset / Page();
    const auto end = Pages(std::min<uint64_t>(offset + length, size));
    ASSERT_EQ(
      ::madvise(data + first * Page(), (end - first) * Page(), MADV_DONTNEED),
      0);
    if (_faults) {
      ASSERT_EQ(
        ::mprotect(data + first * Page(), (end - first) * Page(), PROT_NONE),
        0);
    }
  }

  void DropAll() { Drop(0, size); }

  bool FaultIn(const void* at) noexcept {
    const auto* const byte = static_cast<const duckdb::data_t*>(at);
    if (byte < data || byte >= data + Reserved(size)) {
      return false;
    }
    const auto page = static_cast<uint64_t>(byte - data) / Page();
    Load(page, page + 1);
    return true;
  }

  void KeepUntil(uint64_t end) {
    const auto all = Pages(size);
    const auto kept = Pages(std::min<uint64_t>(end, size));
    Load(0, kept);
    if (kept != all) {
      ASSERT_EQ(
        ::madvise(data + kept * Page(), (all - kept) * Page(), MADV_DONTNEED),
        0);
    }
  }

  std::vector<PrefetchRange> prefetched;

 private:
  static uint64_t Page() noexcept {
    return static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
  }
  static uint64_t Pages(uint64_t bytes) noexcept {
    return (bytes + Page() - 1) / Page();
  }
  static uint64_t Reserved(uint64_t bytes) noexcept {
    return std::max<uint64_t>(Pages(bytes), 1) * Page();
  }
  static duckdb::data_ptr_t Reserve(uint64_t bytes) {
    auto* const mapped =
      ::mmap(nullptr, Reserved(bytes), PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    SDB_ENSURE(mapped != MAP_FAILED, "ScriptedMapping: mmap failed");
    ::madvise(mapped, Reserved(bytes), MADV_NOHUGEPAGE);
    return static_cast<duckdb::data_ptr_t>(mapped);
  }
  void Load(uint64_t first, uint64_t end) noexcept {
    const auto from = first * Page();
    const auto to = std::min<uint64_t>(end * Page(), size);
    if (from >= to) {
      return;
    }
    if (_faults) {
      ::mprotect(data + from, Pages(to) * Page() - from,
                 PROT_READ | PROT_WRITE);
    }
    std::memcpy(data + from, _bytes.data() + from, to - from);
  }

  irs::bstring _bytes;
  bool _faults;
};

class ScriptedFileSystem final : public HookFileSystem {
 public:
  explicit ScriptedFileSystem(duckdb::FileSystem& inner, bool faults = false)
    : HookFileSystem{inner}, _faults{faults} {}

  duckdb::shared_ptr<duckdb::InputFile> OpenBlockFile(
    const std::string& /*path*/) final {
    return nullptr;
  }
  duckdb::unique_ptr<duckdb::MemoryMappedFile> MemoryMapFile(
    const duckdb::OpenFileInfo& path, duckdb::FileOpenFlags /*flags*/,
    const duckdb::MMapOptions& /*options*/,
    duckdb::optional_ptr<duckdb::FileOpener> /*opener*/) final {
    auto mapping = duckdb::make_uniq<ScriptedMapping>(
      irs::ReadWhole<irs::bstring>(duckdb::QueryContext{}, *this, path.path),
      _faults);
    mapped = mapping.get();
    return mapping;
  }

  ScriptedMapping* mapped = nullptr;

 private:
  bool _faults;
};

}  // namespace tests
