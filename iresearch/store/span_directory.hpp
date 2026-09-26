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

#include <ctime>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "iresearch/store/directory.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

class SpanDirectory final : public Directory {
 public:
  using Files = containers::FlatHashMap<std::string, bytes_view>;

  SpanDirectory(Files files,
                const ResourceManagementOptions& resource_manager) noexcept
    : Directory{resource_manager}, _files{std::move(files)} {}

  IndexInput::ptr open(std::string_view name,
                       IOAdvice /*advice*/) const noexcept final {
    const auto it = _files.find(name);
    if (it == _files.end()) {
      return nullptr;
    }
    try {
      return std::make_unique<BytesViewInput>(it->second);
    } catch (...) {
      return nullptr;
    }
  }

  bool exists(bool& result, std::string_view name) const noexcept final {
    result = _files.contains(name);
    return true;
  }

  bool length(uint64_t& result, std::string_view name) const noexcept final {
    const auto it = _files.find(name);
    if (it == _files.end()) {
      return false;
    }
    result = it->second.size();
    return true;
  }

  bool visit(const visitor_f& visitor) const final {
    for (const auto& [name, _] : _files) {
      if (!visitor(name)) {
        return false;
      }
    }
    return true;
  }

  DirectoryAttributes& attributes() noexcept final { return _attributes; }

  IndexOutput::ptr create(std::string_view /*name*/) noexcept final {
    return nullptr;
  }

  IndexLock::ptr make_lock(std::string_view /*name*/) noexcept final {
    return nullptr;
  }

  bool mtime(std::time_t& /*result*/,
             std::string_view /*name*/) const noexcept final {
    return false;
  }

  bool remove(std::string_view /*name*/) noexcept final { return false; }

  bool rename(std::string_view /*src*/,
              std::string_view /*dst*/) noexcept final {
    return false;
  }

  bool sync(std::span<const std::string_view> /*files*/) noexcept final {
    return true;
  }

 private:
  Files _files;
  DirectoryAttributes _attributes;
};

}  // namespace irs
