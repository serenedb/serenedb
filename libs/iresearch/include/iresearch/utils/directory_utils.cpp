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

#include "iresearch/utils/directory_utils.hpp"

#include "basics/log.h"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/utils/attributes.hpp"

namespace irs {
namespace directory_utils {

// Return a reference to a file or empty() if not found
IndexFileRefs::ref_t Reference(const Directory& dir, std::string_view name) {
  auto& refs = dir.attributes().refs();

  bool exists;

  // Do not add an attribute if the file definitly does not exist
  if (!dir.exists(exists, name) || !exists) {
    return nullptr;
  }

  auto ref = refs.add(name);

  return dir.exists(exists, name) && exists ? ref
                                            : IndexFileRefs::ref_t{nullptr};
}

bool RemoveAllUnreferenced(Directory& dir) {
  auto& refs = dir.attributes().refs();

  dir.visit([&refs](std::string_view name) {
    refs.add(name);  // Ensure all files in dir are tracked
    return true;
  });

  DirectoryCleaner::clean(dir);
  return true;
}

}  // namespace directory_utils

TrackingDirectory::TrackingDirectory(Directory& impl) noexcept
  : Directory{impl.ResourceManager()}, _impl{impl} {}

IndexOutput::ptr TrackingDirectory::create(std::string_view name) noexcept try {
  _files.emplace(name);
  if (auto result = _impl.create(name)) [[likely]] {
    result->SetOnClose({absl::FunctionValue{},
                        [&](uint64_t size) noexcept { _files_size += size; }});
    return result;
  }
  _files.erase(name);  // Revert change
  return nullptr;
} catch (...) {
  return nullptr;
}

bool TrackingDirectory::remove(std::string_view name) noexcept {
  if (!_impl.remove(name)) {
    return false;
  }
  _files.erase(name);
  return true;
}

bool TrackingDirectory::rename(std::string_view src,
                               std::string_view dst) noexcept {
  if (!_impl.rename(src, dst)) {
    return false;
  }

  try {
    _files.emplace(dst);
    _files.erase(src);

    return true;
  } catch (...) {
    _impl.rename(dst, src);  // revert
  }

  return false;
}

void TrackingDirectory::ClearTracked() noexcept {
  _files_size = 0;
  _files.clear();
}

std::vector<std::string> TrackingDirectory::FlushTracked(uint64_t& files_size) {
  std::vector<std::string> files(_files.size());
  auto files_begin = files.begin();
  for (auto begin = _files.begin(), end = _files.end(); begin != end;
       ++files_begin) {
    auto to_extract = begin++;
    *files_begin = std::move(_files.extract(to_extract).value());
  }
  files_size = std::exchange(_files_size, uint64_t{0});
  return files;
}

RefTrackingDirectory::RefTrackingDirectory(Directory& impl,
                                           bool track_open /*= false*/)
  : Directory{impl.ResourceManager()},
    _attribute{impl.attributes().refs()},
    _impl{impl},
    _track_open{track_open} {}

RefTrackingDirectory::RefTrackingDirectory(
  RefTrackingDirectory&& other) noexcept
  : Directory{other.ResourceManager()},
    _attribute{other._attribute},  // references do not require std::move(...)
    _impl{other._impl},            // references do not require std::move(...)
    _refs{std::move(other._refs)},
    _track_open{other._track_open} {}

void RefTrackingDirectory::clear_refs() const {
  std::lock_guard lock{_mutex};
  _refs.clear();
}

IndexOutput::ptr RefTrackingDirectory::create(std::string_view name) noexcept
  try {
  // Do not change the order of calls!
  // The cleaner should "see" the file in directory
  // ONLY if there is a tracking reference present!
  auto ref = _attribute.add(name);
  auto result = _impl.create(name);

  // only track ref on successful call to impl_
  if (result) {
    std::lock_guard lock{_mutex};
    _refs.insert(std::move(ref));
  } else {
    _attribute.remove(name);
  }

  return result;
} catch (...) {
  return nullptr;
}

IndexInput::ptr RefTrackingDirectory::open(std::string_view name,
                                           IOAdvice advice) const noexcept try {
  if (!_track_open) {
    return _impl.open(name, advice);
  }
  // Do not change the order of calls!
  // The cleaner should "see" the file in directory
  // ONLY if there is a tracking reference present!
  auto ref = _attribute.add(name);
  auto result = _impl.open(name, advice);

  // only track ref on successful call to impl_
  if (result) {
    std::lock_guard lock{_mutex};
    _refs.emplace(std::move(ref));
  } else {
    _attribute.remove(name);
  }

  return result;
} catch (...) {
  return nullptr;
}

bool RefTrackingDirectory::remove(std::string_view name) noexcept try {
  if (!_impl.remove(name)) {
    return false;
  }
  _attribute.remove(name);

  std::lock_guard lock{_mutex};
  _refs.erase(name);
  return true;
} catch (...) {
  // ignore failure since removal from impl_ was sucessful
  return false;
}

bool RefTrackingDirectory::rename(std::string_view src,
                                  std::string_view dst) noexcept try {
  if (!_impl.rename(src, dst)) {
    return false;
  }
  auto ref = _attribute.add(dst);

  {
    std::lock_guard lock{_mutex};
    _refs.emplace(std::move(ref));

    _refs.erase(src);
  }

  _attribute.remove(src);
  return true;
} catch (...) {
  return false;
}

std::vector<IndexFileRefs::ref_t> RefTrackingDirectory::GetRefs() const {
  std::lock_guard lock{_mutex};

  return {_refs.begin(), _refs.end()};
}

}  // namespace irs
