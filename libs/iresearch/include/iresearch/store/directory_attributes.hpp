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
#include "iresearch/utils/container_utils.hpp"
#include "iresearch/utils/ref_counter.hpp"

namespace irs {

// Directory encryption provider
struct Encryption {
  // FIXME check if it's possible to rename to irs::encryption?
  static constexpr std::string_view type_name() noexcept {
    return "encryption";
  }

  virtual ~Encryption() = default;

  struct Stream {
    using ptr = std::unique_ptr<Stream>;

    virtual ~Stream() = default;

    // Returns size of the block supported by stream
    virtual size_t block_size() const = 0;

    // Decrypt specified data at a provided offset
    virtual bool Decrypt(uint64_t offset, byte_type* data, size_t size) = 0;

    // Encrypt specified data at a provided offset
    virtual bool Encrypt(uint64_t offset, byte_type* data, size_t size) = 0;
  };

  // Returns the length of the header that is added to every file
  // and used for storing encryption options
  virtual size_t header_length() = 0;

  // Creates cipher header in an allocated block for a new file
  virtual bool create_header(std::string_view filename, byte_type* header) = 0;

  // Returns a cipher stream for a file given file name
  virtual Stream::ptr create_stream(std::string_view filename,
                                    byte_type* header) = 0;
};

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
  // 0 == pool_size -> use global allocator, noexcept
  explicit DirectoryAttributes(std::unique_ptr<irs::Encryption> enc = nullptr);
  virtual ~DirectoryAttributes() = default;

  DirectoryAttributes(DirectoryAttributes&&) = default;
  DirectoryAttributes& operator=(DirectoryAttributes&&) = default;

  irs::Encryption* encryption() const noexcept { return _enc.get(); }
  IndexFileRefs& refs() const noexcept { return *_refs; }

 private:
  std::unique_ptr<irs::Encryption> _enc;
  std::unique_ptr<IndexFileRefs> _refs;
};

}  // namespace irs
