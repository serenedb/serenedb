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

#include <absl/functional/function_ref.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/formats/index_meta_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_reader_options.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/data_input.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/store/index_files.hpp>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace tests {

class IndexDir {
 public:
  IndexDir(duckdb::DatabaseInstance& db, std::string_view root);
  IndexDir(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
           std::string_view root);

  irs::IndexOutput::ptr Create(std::string_view name) const;
  std::unique_ptr<irs::InputFile> Open(std::string_view name,
                                       duckdb::MappedFileAccess access) const;
  irs::bstring ReadWhole(std::string_view name) const;
  bool Has(std::string_view name) const;
  bool Remove(std::string_view name) const;
  void Remove(std::span<const std::string> names) const;
  void Rename(std::string_view src, std::string_view dst) const;
  void Sync(std::span<const std::string_view> names) const;
  void SyncDirectory() const;
  void Compact(std::string_view name) const;
  void Visit(absl::FunctionRef<void(std::string_view)> visitor) const;

  duckdb::DatabaseInstance& Db() const noexcept { return _state->db; }
  duckdb::FileSystem& Fs() const noexcept { return _state->fs; }
  const std::string& Root() const noexcept { return _state->root; }
  irs::StorageKind Kind() const noexcept { return _state->kind; }
  irs::IndexFileRefs& Refs() const noexcept { return *_state->refs; }
  const irs::FileObservers& Observers() const noexcept { return _observers; }

  irs::IndexReaderOptions ReaderOptions(
    irs::IndexReaderOptions options = {}) const;
  irs::IndexWriterOptions WriterOptions(
    irs::IndexWriterOptions options = {}) const;

  IndexDir Observed(irs::FileObserver& observer) const;
  IndexDir Unobserved() const;

 private:
  struct State {
    ~State();

    duckdb::DatabaseInstance& db;
    duckdb::FileSystem& fs;
    std::string root;
    irs::StorageKind kind;
    std::shared_ptr<irs::IndexFileRefs> refs;
  };

  std::shared_ptr<State> _state;
  irs::FileObservers _observers;
};

irs::DirectoryReader OpenReader(const IndexDir& dir,
                                const irs::IndexReaderOptions& options = {},
                                irs::MetaPayloadReader payload = {});

irs::IndexWriter::ptr MakeWriter(const IndexDir& dir, irs::OpenMode mode,
                                 irs::IndexWriterOptions options = {});

}  // namespace tests
