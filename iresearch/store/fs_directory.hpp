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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <filesystem>

#include "iresearch/store/directory.hpp"
#include "iresearch/store/directory_attributes.hpp"

namespace irs {

class FSDirectory : public Directory {
 public:
  static constexpr size_t kDefaultPoolSize = 8;

  explicit FSDirectory(
    std::filesystem::path dir,
    DirectoryAttributes attrs = DirectoryAttributes{},
    const ResourceManagementOptions& rm = ResourceManagementOptions::gDefault,
    size_t fd_pool_size = kDefaultPoolSize);

  const std::filesystem::path& path() const noexcept;

  using Directory::attributes;
  DirectoryAttributes& attributes() noexcept final { return _attrs; }

  IndexOutput::ptr create(std::string_view name) noexcept override;

  bool exists(bool& result, std::string_view name) const noexcept override;

  bool length(uint64_t& result, std::string_view name) const noexcept override;

  IndexLock::ptr make_lock(std::string_view name) noexcept final;

  bool mtime(std::time_t& result, std::string_view name) const noexcept final;

  IndexInput::ptr open(std::string_view name,
                       IOAdvice advice) const noexcept override;

  bool remove(std::string_view name) noexcept override;

  bool rename(std::string_view src, std::string_view dst) noexcept override;

  bool sync(std::span<const std::string_view> files) noexcept override;

  bool visit(const visitor_f& visitor) const final;

 private:
  bool sync(std::string_view name) noexcept;

  DirectoryAttributes _attrs;
  std::filesystem::path _dir;
  size_t _fd_pool_size;
};

}  // namespace irs
