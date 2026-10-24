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

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "connector/zip_reader.hpp"

namespace zip_test {

inline void Put(std::string& output, uint64_t value, unsigned width) {
  for (unsigned i = 0; i < width; ++i) {
    output += static_cast<char>(value >> (8 * i));
  }
}

inline void Patch(std::string& output, size_t offset, uint64_t value,
                  unsigned width) {
  for (unsigned i = 0; i < width; ++i) {
    output.at(offset + i) = static_cast<char>(value >> (8 * i));
  }
}

inline std::string Deflate(const std::string& input) {
  z_stream stream{};

  if (deflateInit2(&stream, 6, Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY) !=
      Z_OK) {
    throw std::runtime_error{"deflateInit2 failed"};
  }

  std::string output(deflateBound(&stream, input.size()), '\0');
  stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
  stream.avail_in = input.size();
  stream.next_out = reinterpret_cast<Bytef*>(output.data());
  stream.avail_out = output.size();
  const auto result = deflate(&stream, Z_FINISH);
  output.resize(stream.total_out);
  deflateEnd(&stream);

  if (result != Z_STREAM_END) {
    throw std::runtime_error{"deflate failed"};
  }

  return output;
}

struct Entry {
  std::string name;
  std::string data;
  uint16_t method = sdb::connector::zip::kDeflate;
  uint16_t flags = 0x800;
  uint32_t attributes = 0100644U << 16;
  std::string comment{};
};

struct Archive {
  std::string bytes;
  std::vector<size_t> central;
  std::vector<size_t> data;

  explicit Archive(const std::vector<Entry>& entries, bool zip64 = false,
                   bool descriptor = false) {
    std::string directory;

    for (const auto& entry : entries) {
      const auto offset = bytes.size();
      data.push_back(offset + 30 + entry.name.size() + (zip64 ? 20 : 0));

      const auto compressed = entry.method == sdb::connector::zip::kDeflate
                                ? Deflate(entry.data)
                                : entry.data;
      const auto crc =
        crc32(0, reinterpret_cast<const Bytef*>(entry.data.data()),
              entry.data.size());
      const auto flags = entry.flags | (descriptor ? 8 : 0);

      Put(bytes, 0x04034b50, 4);
      Put(bytes, zip64 ? 45 : 20, 2);
      Put(bytes, flags, 2);
      Put(bytes, entry.method, 2);
      Put(bytes, 0, 4);
      Put(bytes, descriptor ? 0 : crc, 4);
      Put(bytes, zip64 ? UINT32_MAX : descriptor ? 0 : compressed.size(), 4);
      Put(bytes, zip64 ? UINT32_MAX : descriptor ? 0 : entry.data.size(), 4);
      Put(bytes, entry.name.size(), 2);
      Put(bytes, zip64 ? 20 : 0, 2);
      bytes += entry.name;

      if (zip64) {
        Put(bytes, 1, 2);
        Put(bytes, 16, 2);
        Put(bytes, entry.data.size(), 8);
        Put(bytes, compressed.size(), 8);
      }

      bytes += compressed;

      if (descriptor) {
        Put(bytes, 0x08074b50, 4);
        Put(bytes, crc, 4);
        Put(bytes, compressed.size(), zip64 ? 8 : 4);
        Put(bytes, entry.data.size(), zip64 ? 8 : 4);
      }

      central.push_back(directory.size());
      Put(directory, 0x02014b50, 4);
      Put(directory, 0x32d, 2);
      Put(directory, zip64 ? 45 : 20, 2);
      Put(directory, flags, 2);
      Put(directory, entry.method, 2);
      Put(directory, 0, 4);
      Put(directory, crc, 4);
      Put(directory, zip64 ? UINT32_MAX : compressed.size(), 4);
      Put(directory, zip64 ? UINT32_MAX : entry.data.size(), 4);
      Put(directory, entry.name.size(), 2);
      Put(directory, zip64 ? 28 : 0, 2);
      Put(directory, entry.comment.size(), 2);
      Put(directory, 0, 2);
      Put(directory, 0, 2);
      Put(directory, entry.attributes, 4);
      Put(directory, zip64 ? UINT32_MAX : offset, 4);
      directory += entry.name;

      if (zip64) {
        Put(directory, 1, 2);
        Put(directory, 24, 2);
        Put(directory, entry.data.size(), 8);
        Put(directory, compressed.size(), 8);
        Put(directory, offset, 8);
      }

      directory += entry.comment;
    }

    const auto cd_offset = bytes.size();

    for (auto& offset : central) {
      offset += cd_offset;
    }

    bytes += directory;

    if (zip64) {
      const auto end_offset = bytes.size();
      Put(bytes, 0x06064b50, 4);
      Put(bytes, 44, 8);
      Put(bytes, 45, 2);
      Put(bytes, 45, 2);
      Put(bytes, 0, 4);
      Put(bytes, 0, 4);
      Put(bytes, entries.size(), 8);
      Put(bytes, entries.size(), 8);
      Put(bytes, directory.size(), 8);
      Put(bytes, cd_offset, 8);
      Put(bytes, 0x07064b50, 4);
      Put(bytes, 0, 4);
      Put(bytes, end_offset, 8);
      Put(bytes, 1, 4);
    }

    Put(bytes, 0x06054b50, 4);
    Put(bytes, 0, 4);
    Put(bytes, zip64 ? UINT16_MAX : entries.size(), 2);
    Put(bytes, zip64 ? UINT16_MAX : entries.size(), 2);
    Put(bytes, zip64 ? UINT32_MAX : directory.size(), 4);
    Put(bytes, zip64 ? UINT32_MAX : cd_offset, 4);
    Put(bytes, 0, 2);
  }
};

}  // namespace zip_test
