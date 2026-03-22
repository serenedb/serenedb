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

#pragma once

#include "iresearch/store/caching_directory.hpp"
#include "iresearch/store/fs_directory.hpp"

namespace irs {
namespace mmap_utils {

class MMapHandle;
}

class MMapDirectory : public FSDirectory {
 public:
  explicit MMapDirectory(
    std::filesystem::path dir,
    DirectoryAttributes attrs = DirectoryAttributes{},
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault);

  IndexInput::ptr open(std::string_view name,
                       IOAdvice advice) const noexcept override;
};

class CachingMMapDirectory
  : public CachingDirectoryBase<MMapDirectory,
                                std::shared_ptr<mmap_utils::MMapHandle>> {
 public:
  using CachingDirectoryBase::CachingDirectoryBase;

  bool length(uint64_t& result, std::string_view name) const noexcept final;

  IndexInput::ptr open(std::string_view name,
                       IOAdvice advice) const noexcept final;
};

}  // namespace irs
