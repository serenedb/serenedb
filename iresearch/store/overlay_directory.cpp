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

#include "iresearch/store/overlay_directory.hpp"

namespace irs {

OverlayDirectory::OverlayDirectory(Directory::ptr base)
  : Directory{base->ResourceManager()},
    _base{std::move(base)},
    _top{DirectoryAttributes{}, _base->ResourceManager()} {}

bool OverlayDirectory::OnTop(std::string_view name) const noexcept {
  bool result = false;
  return _top.exists(result, name) && result;
}

bool OverlayDirectory::Hidden(std::string_view name) const noexcept {
  absl::ReaderMutexLock lock{&_mutex};
  return _hidden.contains(name);
}

const Directory& OverlayDirectory::Layer(std::string_view name) const noexcept {
  if (OnTop(name) || Hidden(name)) {
    return _top;
  }
  return *_base;
}

IndexOutput::ptr OverlayDirectory::create(std::string_view name) {
  {
    absl::MutexLock lock{&_mutex};
    _hidden.emplace(name);
  }
  return _top.create(name);
}

bool OverlayDirectory::remove(std::string_view name) noexcept {
  if (OnTop(name)) {
    return _top.remove(name);
  }
  absl::MutexLock lock{&_mutex};
  bool in_base = false;
  if (_hidden.contains(name) || !_base->exists(in_base, name) || !in_base) {
    return false;
  }
  _hidden.emplace(name);
  return true;
}

bool OverlayDirectory::rename(std::string_view src,
                              std::string_view dst) noexcept {
  if (!OnTop(src)) {
    return false;
  }
  {
    absl::MutexLock lock{&_mutex};
    _hidden.emplace(dst);
  }
  return _top.rename(src, dst);
}

bool OverlayDirectory::visit(const visitor_f& visitor) const {
  if (!_top.visit(visitor)) {
    return false;
  }
  return _base->visit(
    [&](std::string_view name) { return Hidden(name) || visitor(name); });
}

}  // namespace irs
