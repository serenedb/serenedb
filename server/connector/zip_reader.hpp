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

#include <zlib.h>

#include <array>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace sdb::connector::zip {

inline constexpr uint16_t kStore = 0;
inline constexpr uint16_t kDeflate = 8;

struct Path {
  std::string_view archive;
  std::string_view member;
};

Path ParsePath(std::string_view path);

class Source {
 public:
  virtual ~Source() = default;

  virtual uint64_t Size() const = 0;

  virtual void ReadAt(uint64_t offset, std::span<std::byte> output) = 0;

  virtual void Check() = 0;
};

inline constexpr size_t kMetadataCacheLimit = 50 * 1024 * 1024;

// Manages the reading of a single file in a ZIP archive
class MemberReader {
 public:
  MemberReader(Source& source, std::string_view member);

  ~MemberReader() noexcept;

  MemberReader(const MemberReader&) = delete;

  MemberReader& operator=(const MemberReader&) = delete;

  uint64_t Size() const { return _size; }

  uint64_t Position() const { return _position; }

  size_t Read(std::span<std::byte> output);

  void ReadAt(uint64_t offset, std::span<std::byte> output);

  void Seek(uint64_t offset);

 private:
  void Start();

  void Stop() noexcept;

  void Advance(uint64_t target);

  size_t Inflate(std::span<std::byte> output);

  size_t ReadStored(std::span<std::byte> output);

  void Finish();

  void CheckState();

  Source& _source;
  std::vector<std::byte> _input;
  std::vector<std::byte> _discard;
  z_stream _stream{};
  uint64_t _size = 0;
  uint64_t _compressed_size = 0;
  uint64_t _data_offset = 0;
  uint64_t _position = 0;
  uint64_t _decoded = 0;
  uint64_t _loaded = 0;
  uint64_t _crc_position = 0;
  uint32_t _expected_crc = 0;
  uint32_t _crc = 0;
  uint16_t _method = kStore;
  bool _started = false;
  bool _ended = false;
  bool _failed = false;
  bool _crc_valid = true;
};

}  // namespace sdb::connector::zip
