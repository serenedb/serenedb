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

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <string>

#include "iresearch/store/directory.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace irs {

class OverlayDirectory final : public Directory {
 public:
  explicit OverlayDirectory(Directory::ptr base);

  DirectoryAttributes& attributes() noexcept final {
    return _base->attributes();
  }

  IndexOutput::ptr create(std::string_view name) final;

  bool exists(bool& result, std::string_view name) const noexcept final {
    return Layer(name).exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept final {
    return Layer(name).length(result, name);
  }

  bool mtime(std::time_t& result, std::string_view name) const noexcept final {
    return Layer(name).mtime(result, name);
  }

  IndexInput::ptr open(std::string_view name, IOAdvice advice) const final {
    return Layer(name).open(name, advice);
  }

  bool remove(std::string_view name) noexcept final;

  bool rename(std::string_view src, std::string_view dst) noexcept final;

  bool sync(std::span<const std::string_view>) noexcept final { return true; }

  bool visit(const visitor_f& visitor) const final;

 private:
  bool OnTop(std::string_view name) const noexcept;
  bool Hidden(std::string_view name) const noexcept;
  const Directory& Layer(std::string_view name) const noexcept;

  Directory::ptr _base;
  MemoryDirectory _top;
  mutable absl::Mutex _mutex;
  absl::flat_hash_set<std::string> _hidden ABSL_GUARDED_BY(_mutex);
};

}  // namespace irs
