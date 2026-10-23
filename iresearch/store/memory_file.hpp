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

#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/container_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs {

class MemoryFile : public container_utils::RawBlockVector<16, 8> {
  // total number of levels and size of the first level 2^8
  using Base = container_utils::RawBlockVector<16, 8>;

 public:
  explicit MemoryFile(IResourceManager& rm) noexcept : Base{rm} {}

  MemoryFile(MemoryFile&& rhs) noexcept
    : Base{static_cast<Base&&>(rhs)}, _len{std::exchange(rhs._len, 0)} {}

  MemoryFile& operator>>(DataOutput& out) {
    Visit([&](const byte_type* b, size_t len) {
      out.WriteData(b, len);
      return true;
    });
    return *this;
  }

  size_t Length() const noexcept { return _len; }

  void Length(size_t length) noexcept { _len = length; }

  void Reset() noexcept { _len = 0; }

  void Clear() noexcept {
    Base::clear();
    Reset();
  }

  template<typename Visitor>
  bool Visit(const Visitor& visitor) {
    auto len = _len;
    for (const auto& buffer : _buffers) {
      const auto to_visit = std::min(len, buffer.size);
      if (!visitor(buffer.data, to_visit)) {
        return false;
      }
      len -= to_visit;
    }
    return true;
  }

 private:
  size_t _len = 0;
};

class MemoryIndexOutput : public IndexOutput {
 public:
  explicit MemoryIndexOutput(MemoryFile& file) noexcept;

  void Reset() noexcept {
    _buf = _pos = _end = nullptr;
    _offset = 0;
  }

  void Flush() noexcept final { _file.Length(Position()); }

  uint32_t Checksum() override {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("this memory output keeps no checksum"));
  }

  uint64_t CloseImpl() noexcept final {
    // TODO(mbkkt) maybe we need reset?
    Flush();
    return Position();
  }

 protected:
  friend class BufferedOutput;

  void FlushBuffer();

 private:
  void WriteDirect(const byte_type* b, size_t len) override;

  MemoryFile& _file;
};

struct MemoryOutput {
  explicit MemoryOutput(IResourceManager& rm) noexcept : file{rm} {}

  MemoryOutput(MemoryOutput&& rhs) noexcept : file{std::move(rhs.file)} {}

  void Reset() noexcept {
    file.Reset();
    stream.Reset();
  }

  MemoryFile file;
  MemoryIndexOutput stream{file};
};

}  // namespace irs
