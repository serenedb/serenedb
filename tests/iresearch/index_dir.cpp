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

#include <absl/algorithm/container.h>

#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/main/database_file_opener.hpp>
#include <iresearch/formats/format_utils.hpp>
#include <iresearch/index/file_names.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iterator>

namespace tests {
namespace {

StorageKind KindOf(std::string_view root) {
  if (root.starts_with(duckdb::MemoryFileSystem::PREFIX)) {
    return StorageKind::Memory;
  }
  return duckdb::DatabaseFileSystem::IsRemote(root) ? StorageKind::Remote
                                                    : StorageKind::Local;
}

}  // namespace

IndexDir::State::~State() {
  if (kind != StorageKind::Memory) {
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
  : _state{std::make_shared<State>(db, fs, std::string{root}, KindOf(root),
                                   std::make_shared<Segments>())} {}

std::unique_ptr<duckdb::BufferedFileWriter> IndexDir::Create(
  std::string_view name) const {
  return irs::format_utils::OpenOutput(Fs(), Root(), name, nullptr);
}

duckdb::shared_ptr<duckdb::InputFile> IndexDir::Open(
  std::string_view name, duckdb::MappedFileAccess access) const {
  return Fs().OpenInputFile(Fs().JoinPath(Root(), name), access);
}

irs::bstring IndexDir::ReadWhole(std::string_view name) const {
  return irs::ReadWhole<irs::bstring>(duckdb::QueryContext{}, Fs(),
                                      Fs().JoinPath(Root(), name));
}

bool IndexDir::Has(std::string_view name) const {
  return Fs().FileExists(Fs().JoinPath(Root(), name));
}

bool IndexDir::Remove(std::string_view name) const {
  return Fs().TryRemoveFile(Fs().JoinPath(Root(), name));
}

void IndexDir::Rename(std::string_view src, std::string_view dst) const {
  Fs().MoveFile(Fs().JoinPath(Root(), src), Fs().JoinPath(Root(), dst));
}

void IndexDir::Sync(std::span<const std::string_view> names) const {
  duckdb::vector<std::string> paths;
  for (const auto name : names) {
    paths.push_back(Fs().JoinPath(Root(), name));
  }
  Fs().SyncFiles(paths);
}

void IndexDir::Compact(std::string_view name) const {
  Fs().Compact({Fs().JoinPath(Root(), name)});
}

std::vector<std::string> IndexDir::Files() const {
  std::vector<std::string> files;
  Fs().ListFiles(Root(),
                 [&](duckdb::OpenFileInfo& info) {
                   if (!duckdb::FileSystem::IsDirectory(info)) {
                     files.push_back(std::move(info.path));
                   }
                 },
                 nullptr, {.strict = true});
  return files;
}

irs::FileTracker& IndexDir::Tracker(std::string_view segment) const {
  absl::MutexLock lock{&_state->trackers_mutex};
  auto& tracker = _state->trackers[segment];
  if (!tracker) {
    tracker = std::make_unique<irs::FileTracker>(Fs(), Root(), segment);
  }
  return *tracker;
}

std::shared_ptr<const irs::FileTracker> IndexDir::Files(
  const irs::SegmentMeta& meta) const {
  absl::MutexLock lock{&_state->segments->mutex};
  for (const auto& segment : _state->segments->committed) {
    if (segment.meta.name == meta.name) {
      return segment.files;
    }
  }
  return nullptr;
}

std::vector<irs::SegmentMeta> IndexDir::Committed() const {
  std::vector<irs::SegmentMeta> metas;
  absl::MutexLock lock{&_state->segments->mutex};
  for (const auto& segment : _state->segments->committed) {
    metas.push_back(segment.meta);
  }
  return metas;
}

std::vector<CommittedSegment> IndexDir::CommittedSegments() const {
  absl::MutexLock lock{&_state->segments->mutex};
  return _state->segments->committed;
}

void IndexDir::Publish(std::vector<CommittedSegment> segments) const {
  absl::MutexLock lock{&_state->segments->mutex};
  auto& published = _state->segments->published;
  for (const auto& [meta, files] : segments) {
    if (files && absl::c_none_of(published, [&](const Published& known) {
          return known.segment.name == meta.name;
        })) {
      published.push_back(
        {.segment = {.name = meta.name, .kinds = meta.kinds}, .files = files});
    }
  }
  _state->segments->committed.swap(segments);
}

std::vector<std::string> IndexDir::HeldFiles() const {
  std::vector<std::string> files;
  absl::MutexLock lock{&_state->segments->mutex};
  auto& published = _state->segments->published;
  std::erase_if(published,
                [](const Published& known) { return known.files.expired(); });
  for (const auto& known : published) {
    absl::c_move(irs::SegmentFiles(known.segment.name, known.segment.kinds),
                 std::back_inserter(files));
  }
  return files;
}

size_t IndexDir::RemoveReleased() const {
  const auto before = Files();
  irs::RemoveReleased(Fs(), Root(), *Released());
  const auto after = Files();
  const irs::containers::FlatHashSet<std::string_view> kept{after.begin(),
                                                            after.end()};
  return static_cast<size_t>(absl::c_count_if(
    before, [&](const std::string& name) { return !kept.contains(name); }));
}

uint64_t IndexDir::TopSegmentId() const {
  absl::MutexLock lock{&_state->segments->mutex};
  return _state->segments->top_segment_id;
}

void IndexDir::RaiseTopSegmentId(uint64_t id) const {
  absl::MutexLock lock{&_state->segments->mutex};
  auto& top = _state->segments->top_segment_id;
  top = std::max(top, id);
}

IndexDir IndexDir::Over(duckdb::FileSystem& fs) const {
  SDB_ASSERT(Kind() != StorageKind::Memory);
  auto result = *this;
  result._state =
    std::make_shared<State>(Db(), fs, Root(), Kind(), _state->segments);
  return result;
}

IndexDir IndexDir::Reopened() const {
  SDB_ASSERT(Kind() != StorageKind::Memory);
  IndexDir reopened{Db(), Fs(), Root()};
  std::vector<CommittedSegment> segments;
  for (auto& meta : Committed()) {
    auto files = std::make_shared<const irs::FileTracker>(
      Fs(), Root(), meta.name, meta.kinds, reopened.Released());
    segments.push_back({.meta = std::move(meta), .files = std::move(files)});
  }
  reopened.Publish(std::move(segments));
  return reopened;
}

}  // namespace tests
