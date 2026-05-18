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

#pragma once

#include "basics/shared.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/directory_cleaner.hpp"

namespace irs {
namespace directory_utils {

// return a reference to a file or empty() if not found
IndexFileRefs::ref_t Reference(const Directory& dir, std::string_view name);

// Remove all (tracked and non-tracked) files if they are unreferenced
// return success
bool RemoveAllUnreferenced(Directory& dir);

}  // namespace directory_utils

// Track files created/opened via file names
struct TrackingDirectory final : public Directory {
  explicit TrackingDirectory(Directory& impl) noexcept;

  Directory& GetImpl() noexcept { return _impl; }

  DirectoryAttributes& attributes() noexcept final {
    return _impl.attributes();
  }

  IndexOutput::ptr create(std::string_view name) noexcept final;

  bool exists(bool& result, std::string_view name) const noexcept final {
    return _impl.exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept final {
    return _impl.length(result, name);
  }

  IndexLock::ptr make_lock(std::string_view name) noexcept final {
    return _impl.make_lock(name);
  }

  bool mtime(std::time_t& result, std::string_view name) const noexcept final {
    return _impl.mtime(result, name);
  }

  IndexInput::ptr open(std::string_view name,
                       IOAdvice advice) const noexcept final {
    return _impl.open(name, advice);
  }

  bool remove(std::string_view name) noexcept final;

  bool rename(std::string_view src, std::string_view dst) noexcept final;

  bool sync(std::span<const std::string_view> files) noexcept final {
    return _impl.sync(files);
  }

  bool visit(const visitor_f& visitor) const final {
    return _impl.visit(visitor);
  }

  std::vector<std::string> FlushTracked(uint64_t& files_size);

  void ClearTracked() noexcept;

 private:
  using FileSet = absl::flat_hash_set<std::string>;

  Directory& _impl;
  uint64_t _files_size{};
  FileSet _files;
};

// Track files created/opened via file refs instead of file names
struct RefTrackingDirectory : public Directory {
 public:
  using ptr = std::unique_ptr<RefTrackingDirectory>;

  // @param track_open - track file refs for calls to open(...)
  explicit RefTrackingDirectory(Directory& impl, bool track_open = false);
  RefTrackingDirectory(RefTrackingDirectory&& other) noexcept;

  Directory& operator*() noexcept { return _impl; }

  DirectoryAttributes& attributes() noexcept final {
    return _impl.attributes();
  }

  void clear_refs() const;

  IndexOutput::ptr create(std::string_view name) noexcept final;

  bool exists(bool& result, std::string_view name) const noexcept final {
    return _impl.exists(result, name);
  }

  bool length(uint64_t& result, std::string_view name) const noexcept final {
    return _impl.length(result, name);
  }

  IndexLock::ptr make_lock(std::string_view name) noexcept final {
    return _impl.make_lock(name);
  }

  bool mtime(std::time_t& result, std::string_view name) const noexcept final {
    return _impl.mtime(result, name);
  }

  IndexInput::ptr open(std::string_view name,
                       IOAdvice advice) const noexcept final;

  bool remove(std::string_view name) noexcept final;

  bool rename(std::string_view src, std::string_view dst) noexcept final;

  bool sync(std::span<const std::string_view> names) noexcept final {
    return _impl.sync(names);
  }

  bool visit(const visitor_f& visitor) const final {
    return _impl.visit(visitor);
  }

  std::vector<IndexFileRefs::ref_t> GetRefs() const;

 private:
  using refs_t =
    absl::flat_hash_set<IndexFileRefs::ref_t, IndexFileRefs::counter_t::Hash,
                        IndexFileRefs::counter_t::EqualTo>;

  IndexFileRefs& _attribute;
  Directory& _impl;
  mutable absl::Mutex _mutex;  // for use with refs_
  mutable refs_t _refs;
  bool _track_open;
};

}  // namespace irs
