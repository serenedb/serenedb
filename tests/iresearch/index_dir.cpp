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

#include "index_dir.hpp"

#include <duckdb/storage/block_cache/block_cache.hpp>

namespace tests {

IndexDir::State::~State() {
  if (kind != irs::StorageKind::Memory) {
    return;
  }
  try {
    fs.RemoveDirectory(root);
  } catch (...) {
  }
}

IndexDir::IndexDir(duckdb::DatabaseInstance& db, std::string_view root)
  : IndexDir{db, duckdb::FileSystem::GetFileSystem(db), root} {}

IndexDir::IndexDir(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
                   std::string_view root)
  : _state{std::make_shared<State>(db, fs, std::string{root},
                                   irs::StorageKindOf(root),
                                   std::make_shared<irs::IndexFileRefs>())} {}

std::unique_ptr<irs::FileOutput> IndexDir::Create(std::string_view name) const {
  return irs::FileOutput::Open(Fs(), Root(), name, _observers,
                               &duckdb::BlockCache::Get(Db()));
}

std::unique_ptr<irs::InputFile> IndexDir::Open(
  std::string_view name, duckdb::MappedFileAccess access) const {
  return irs::OpenInput(duckdb::BlockCache::Get(Db()), Fs(),
                        irs::PathOf(Root(), name), access);
}

irs::bstring IndexDir::ReadWhole(std::string_view name) const {
  return irs::ReadWhole(Fs(), irs::PathOf(Root(), name));
}

bool IndexDir::Has(std::string_view name) const {
  return Fs().FileExists(irs::PathOf(Root(), name));
}

bool IndexDir::Remove(std::string_view name) const {
  if (!Fs().TryRemoveFile(irs::PathOf(Root(), name))) {
    return false;
  }
  for (auto* observer : _observers) {
    observer->Removed(name);
  }
  return true;
}

void IndexDir::Remove(std::span<const std::string> names) const {
  if (names.empty()) {
    return;
  }
  duckdb::vector<std::string> paths;
  paths.reserve(names.size());
  for (const auto& name : names) {
    paths.push_back(irs::PathOf(Root(), name));
  }
  Fs().RemoveFiles(paths);
  for (const auto& name : names) {
    for (auto* observer : _observers) {
      observer->Removed(name);
    }
  }
}

void IndexDir::Rename(std::string_view src, std::string_view dst) const {
  Fs().MoveFile(irs::PathOf(Root(), src), irs::PathOf(Root(), dst));
  for (auto* observer : _observers) {
    observer->Renamed(src, dst);
  }
}

void IndexDir::Sync(std::span<const std::string_view> names) const {
  irs::SyncFiles(Fs(), Root(), names);
}

void IndexDir::SyncDirectory() const { irs::SyncDirectory(Fs(), Root()); }

void IndexDir::Compact(std::string_view name) const {
  Fs().Compact(irs::PathOf(Root(), name));
}

void IndexDir::Visit(absl::FunctionRef<void(std::string_view)> visitor) const {
  irs::ListIndexFiles(Fs(), Root(), visitor);
}

irs::IndexReaderOptions IndexDir::ReaderOptions(
  irs::IndexReaderOptions options) const {
  if (!options.db) {
    options.db = &Db();
  }
  if (!options.refs) {
    options.refs = _state->refs;
  }
  return options;
}

irs::IndexWriterOptions IndexDir::WriterOptions(
  irs::IndexWriterOptions options) const {
  if (!options.db) {
    options.db = &Db();
  }
  options.reader_options = ReaderOptions(std::move(options.reader_options));
  return options;
}

IndexDir IndexDir::Observed(irs::FileObserver& observer) const {
  auto result = *this;
  result._observers.push_back(&observer);
  return result;
}

IndexDir IndexDir::Unobserved() const {
  auto result = *this;
  result._observers.clear();
  return result;
}

irs::DirectoryReader OpenReader(const IndexDir& dir,
                                const irs::IndexReaderOptions& options,
                                irs::MetaPayloadReader payload) {
  return irs::DirectoryReader{dir.Fs(), dir.Root(), dir.ReaderOptions(options),
                              std::move(payload)};
}

irs::IndexWriter::ptr MakeWriter(const IndexDir& dir, irs::OpenMode mode,
                                 irs::IndexWriterOptions options) {
  return irs::IndexWriter::Make(dir.Fs(), dir.Root(), mode,
                                dir.WriterOptions(std::move(options)));
}

}  // namespace tests
