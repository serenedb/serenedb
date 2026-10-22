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

#include <duckdb/common/memory_mapped_file.hpp>
#include <memory>

#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

class MappedFile {
 public:
  MappedFile(std::unique_ptr<duckdb::MemoryMappedFile> mapping,
             IResourceManager* file_descriptors);
  MappedFile(const MappedFile&) = delete;
  MappedFile& operator=(const MappedFile&) = delete;
  ~MappedFile();

  bytes_view Bytes() const noexcept;
  duckdb::MemoryMappedFile& Mapping() noexcept { return *_mapping; }
  const file_utils::ResidencyMap& Residency() const noexcept {
    return _residency;
  }

 private:
  std::unique_ptr<duckdb::MemoryMappedFile> _mapping;
  IResourceManager* _file_descriptors;
  file_utils::ResidencyMap _residency;
};

class MappedInput final : public BytesViewInput {
 public:
  explicit MappedInput(std::shared_ptr<MappedFile> file) noexcept;

  ptr Dup() const final { return std::make_unique<MappedInput>(*this); }

  void Prefetch(uint64_t offset, uint64_t count) const noexcept final;
  void Advise(IOAdvice advice) noexcept final;
  bool Warm(uint64_t offset, uint64_t count) const noexcept final;
  void MarkWarm(uint64_t offset, uint64_t count) const noexcept final;

 private:
  uint64_t LastPage(uint64_t offset, uint64_t count) const noexcept;

  std::shared_ptr<MappedFile> _file;
};

duckdb::MappedFileAccess ToMappedFileAccess(IOAdvice advice) noexcept;

}  // namespace irs
