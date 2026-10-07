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

#include "search/source_files.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <filesystem>

#include "search/frame.h"
#include "server/utils/file_utils.h"

namespace sdb::search {
namespace {

constexpr duckdb::field_id_t kFrameFiles = 0;
constexpr duckdb::field_id_t kFileId = 0;
constexpr duckdb::field_id_t kFilePath = 1;
constexpr duckdb::field_id_t kFileVersion = 2;

void WriteFilesFrame(duckdb::BufferedFileWriter& writer,
                     std::span<const SourceFile> files) {
  duckdb::MemoryStream payload;
  duckdb::BinarySerializer out{payload};
  out.Begin();
  out.WriteList(kFrameFiles, "files", files.size(),
                [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                  list.WriteObject([&](duckdb::BinarySerializer& file) {
                    file.WriteProperty<uint64_t>(kFileId, "id", files[i].id);
                    file.WriteProperty(kFilePath, "path", files[i].path);
                    file.WriteProperty(kFileVersion, "version",
                                       files[i].version);
                  });
                });
  out.End();
  WriteFrame(writer, payload.GetData(), payload.GetPosition());
}

void ReadFilesFrame(std::vector<uint8_t>& payload,
                    std::vector<SourceFile>& files) {
  duckdb::MemoryStream stream{payload.data(), payload.size()};
  duckdb::BinaryDeserializer in{stream};
  in.Begin();
  in.ReadList(
    kFrameFiles, "files",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& file) {
        files.push_back(
          {.id = file.ReadProperty<uint64_t>(kFileId, "id"),
           .path = file.ReadProperty<std::string>(kFilePath, "path"),
           .version = file.ReadProperty<std::string>(kFileVersion, "version")});
      });
    });
  in.End();
}

}  // namespace

SourceFiles::SourceFiles(std::vector<SourceFile> files)
  : _files{std::move(files)} {
  absl::c_sort(_files, [](const SourceFile& lhs, const SourceFile& rhs) {
    return lhs.id < rhs.id;
  });
}

const SourceFile* SourceFiles::Find(uint64_t id) const noexcept {
  const auto it = absl::c_lower_bound(
    _files, id,
    [](const SourceFile& file, uint64_t key) { return file.id < key; });
  return it != _files.end() && it->id == id ? &*it : nullptr;
}

uint64_t SourceFiles::NextId() const noexcept {
  return _files.empty() ? 0 : _files.back().id + 1;
}

StoredSourceFiles ReadSourceFiles(duckdb::FileSystem& fs,
                                  const std::string& path) {
  StoredSourceFiles stored;
  duckdb::BufferedFileReader reader{fs, path.c_str()};
  std::vector<uint8_t> payload;
  uint64_t intact = 0;
  while (ReadFrame(reader, payload)) {
    ReadFilesFrame(payload, stored.files);
    intact = reader.CurrentOffset();
  }
  stored.torn = intact != reader.FileSize();
  return stored;
}

void AppendSourceFiles(duckdb::FileSystem& fs, const std::string& path,
                       std::span<const SourceFile> files) {
  duckdb::BufferedFileWriter writer{
    fs, path,
    duckdb::FileOpenFlags::FILE_FLAGS_WRITE |
      duckdb::FileOpenFlags::FILE_FLAGS_FILE_CREATE |
      duckdb::FileOpenFlags::FILE_FLAGS_APPEND};
  WriteFilesFrame(writer, files);
  writer.Sync();
  writer.Close();
}

void WriteSourceFiles(duckdb::FileSystem& fs, const std::string& tmp_path,
                      const std::string& path,
                      std::span<const SourceFile> files) {
  {
    duckdb::BufferedFileWriter writer{
      fs, tmp_path,
      duckdb::FileOpenFlags::FILE_FLAGS_WRITE |
        duckdb::FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW};
    WriteFilesFrame(writer, files);
    writer.Sync();
    writer.Close();
  }
  fs.MoveFile(tmp_path, path);
  utils::file_utils::SyncDirectory(
    std::filesystem::path{path}.parent_path().string());
}

}  // namespace sdb::search
