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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/functional/function_ref.h>

#include <duckdb/common/constants.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <duckdb/common/serializer/serialization_traits.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>

#include "iresearch/formats/flush_state.hpp"

namespace irs::format_utils {

inline constexpr uint64_t kFooterCrcLen = sizeof(uint32_t);

inline constexpr duckdb::field_id_t kMetaFieldPacked =
  duckdb::MESSAGE_TERMINATOR_FIELD_ID - 1;

struct Footer {
  uint64_t data_len = 0;
  uint32_t data_expected_crc32c = 0;
};

using FooterWriter = absl::FunctionRef<void(duckdb::BinarySerializer&)>;
using FooterReader =
  absl::FunctionRef<void(duckdb::BinaryDeserializer&, uint64_t)>;

uint32_t WriteFooter(duckdb::BufferedFileWriter& out);

uint32_t WriteFooter(duckdb::BufferedFileWriter& out, FooterWriter write);

Footer ReadFooter(const duckdb::InputFile& file, std::string_view name,
                  FileSizes sizes);

Footer ReadFooter(const duckdb::InputFile& file, std::string_view name,
                  FileSizes sizes, FooterReader read);

duckdb::shared_ptr<duckdb::InputFile> EmptyInput(
  duckdb::DatabaseInstance& db, std::string path,
  const duckdb::InputFile* sibling);

std::unique_ptr<duckdb::BufferedFileWriter> OpenOutput(duckdb::FileSystem& fs,
                                                       std::string_view root,
                                                       std::string_view name);

std::unique_ptr<duckdb::BufferedFileWriter> PrepareOutput(FileTracker& tracker,
                                                          std::string_view ext,
                                                          uint8_t kind);

duckdb::shared_ptr<duckdb::InputFile> CloseOutput(
  duckdb::BufferedFileWriter& out);

duckdb::shared_ptr<duckdb::InputFile> CloseOutput(
  duckdb::BufferedFileWriter& out, FileTracker& tracker, uint8_t kind,
  uint32_t footer);

class LazyOutput {
 public:
  LazyOutput(std::string_view ext, uint8_t kind) noexcept
    : _ext{ext}, _kind{kind} {}

  void Prepare(FileTracker& tracker) noexcept {
    SDB_ASSERT(!_out);
    _tracker = &tracker;
  }

  bool Opened() const noexcept { return _out != nullptr; }
  uint64_t Position() const noexcept { return _out ? _out->Position() : 0; }

  duckdb::BufferedFileWriter& Get() {
    if (!_out) [[unlikely]] {
      Open();
    }
    return *_out;
  }

  duckdb::shared_ptr<duckdb::InputFile> Close(uint32_t footer);

 private:
  void Open();

  std::string_view _ext;
  uint8_t _kind;
  FileTracker* _tracker = nullptr;
  std::unique_ptr<duckdb::BufferedFileWriter> _out;
};

}  // namespace irs::format_utils
