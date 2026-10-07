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

#include "connector/source_file.h"

#include <absl/base/internal/endian.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/multi_file/multi_file_list.hpp>
#include <duckdb/common/types/hash.hpp>
#include <duckdb/main/client_context.hpp>
#include <limits>

#include "connector/primary_key.h"
#include "connector/term_dict.h"
#include "planning/iceberg_multi_file_list.hpp"

namespace sdb::connector {
namespace {

std::string FileVersion(duckdb::ClientContext& context,
                        const duckdb::OpenFileInfo& file) {
  if (const auto& ext = file.extended_info) {
    const auto& options = ext->options;
    const auto etag = options.find("etag");
    if (etag != options.end() && !etag->second.IsNull()) {
      auto version = duckdb::StringValue::Get(etag->second);
      if (!version.empty()) {
        return version;
      }
    }
    const auto mtime = options.find("last_modified");
    if (mtime != options.end() && !mtime->second.IsNull()) {
      return absl::StrCat(
        mtime->second.DefaultCastAs(duckdb::LogicalType::TIMESTAMP)
          .GetValue<duckdb::timestamp_t>()
          .value);
    }
  }
  auto& fs = duckdb::FileSystem::GetFileSystem(context);
  auto handle = fs.OpenFile(file.path, duckdb::FileFlags::FILE_FLAGS_READ);
  auto version = fs.GetVersionTag(*handle);
  if (!version.empty()) {
    return version;
  }
  return absl::StrCat(fs.GetLastModifiedTime(*handle).value);
}

bool HasLiveDoc(irs::SeekTermIterator& terms, irs::bytes_view prefix,
                const irs::DocumentMask::Iterator& masked, bool& more) {
  while (more && terms.value().starts_with(prefix)) {
    auto postings = terms.postings(irs::IndexFeatures::None);
    for (auto doc = postings->Next(); !irs::doc_limits::eof(doc);
         doc = postings->Next()) {
      if (!masked.Contains(doc)) {
        return true;
      }
    }
    more = terms.next();
  }
  return false;
}

void CollectLiveFileIds(const irs::SubReader& segment,
                        absl::flat_hash_set<uint64_t>& live) {
  const auto* field = segment.field(term_dict::kPKFieldId);
  if (!field) {
    return;
  }
  const auto masked = segment.MaskedDocs();
  auto terms = field->iterator();
  bool more = terms->next();
  while (more) {
    const auto id = absl::big_endian::Load64(terms->value().data());
    const auto prefix = primary_key::PkFilePrefix(id);
    if (masked.Empty() ||
        HasLiveDoc(*terms,
                   irs::ViewCast<irs::byte_type>(std::string_view{prefix}),
                   masked, more)) {
      live.insert(id);
    }
    if (!more || id == std::numeric_limits<uint64_t>::max()) {
      return;
    }
    const auto next = primary_key::PkFilePrefix(id + 1);
    more =
      terms->seek_ge(irs::ViewCast<irs::byte_type>(std::string_view{next})) !=
      irs::SeekResult::End;
  }
}

}  // namespace

SourceListing ListSource(duckdb::ClientContext& context,
                         const duckdb::MultiFileList& list, bool versioned) {
  SourceListing listing;
  if (const auto* iceberg_list =
        dynamic_cast<const duckdb::IcebergMultiFileList*>(&list)) {
    const auto& planner = iceberg_list->GetScanPlanner();
    for (duckdb::idx_t i = 0;; ++i) {
      auto file = planner.GetDataFileDescriptor(i);
      if (!file) {
        break;
      }
      listing.files.emplace_back(std::move(file->file_path));
    }
  } else {
    listing.files = list.GetAllFiles();
  }
  listing.versions.reserve(listing.files.size());
  duckdb::hash_t digest = 0;
  for (const auto& file : listing.files) {
    auto version = versioned ? FileVersion(context, file) : std::string{};
    digest = duckdb::CombineHash(
      digest, duckdb::Hash(file.path.data(), file.path.size()));
    digest =
      duckdb::CombineHash(digest, duckdb::Hash(version.data(), version.size()));
    listing.versions.push_back(std::move(version));
  }
  listing.digest = digest;
  return listing;
}

std::vector<search::SourceFile> ListedFiles(const SourceListing& listing,
                                            uint64_t first_id) {
  std::vector<search::SourceFile> files;
  files.reserve(listing.files.size());
  for (size_t i = 0; i < listing.files.size(); ++i) {
    files.push_back({.id = first_id + i,
                     .path = listing.files[i].path,
                     .version = listing.versions[i]});
  }
  return files;
}

HeldFiles CollectHeldFiles(const irs::IndexReader& reader,
                           const search::SourceFiles& files) {
  absl::flat_hash_set<uint64_t> live;
  for (const auto& segment : reader) {
    CollectLiveFileIds(segment, live);
  }
  HeldFiles held;
  for (const auto id : live) {
    const auto* file = files.Find(id);
    if (!file) {
      held.complete = false;
      continue;
    }
    auto& entry = held.by_path[file->path];
    entry.ids.push_back(id);
    entry.version = file->version;
  }
  return held;
}

HeldFiles KnownFiles(const search::SourceFiles& files) {
  HeldFiles held;
  for (const auto& file : files.Files()) {
    held.by_path[file.path];
  }
  return held;
}

}  // namespace sdb::connector
