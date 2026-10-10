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

#include "iresearch/utils/container_utils.hpp"
#include "iresearch/utils/ref_counter.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

// Represents a reference counter for index related files
class IndexFileRefs final {
 public:
  using counter_t = RefCounter<std::string>;
  using ref_t = counter_t::ref_t;

  IndexFileRefs() = default;
  ref_t add(std::string_view key) { return _refs.add(key); }
  bool remove(std::string_view key) { return _refs.remove(key); }

  counter_t& refs() noexcept { return _refs; }

 private:
  counter_t _refs;
};

using FileRefs = std::vector<IndexFileRefs::ref_t>;

// Represents common directory attributes
class DirectoryAttributes {
 public:
  DirectoryAttributes();
  virtual ~DirectoryAttributes() = default;

  DirectoryAttributes(DirectoryAttributes&&) = default;
  DirectoryAttributes& operator=(DirectoryAttributes&&) = default;

  IndexFileRefs& refs() const noexcept { return *_refs; }

 private:
  std::unique_ptr<IndexFileRefs> _refs;
};

}  // namespace irs
