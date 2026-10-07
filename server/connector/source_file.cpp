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

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/multi_file/multi_file_list.hpp>
#include <duckdb/common/types/hash.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/assert.hpp>

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

}  // namespace

std::string SourceFileTerm(uint64_t id, std::string_view path,
                           std::string_view version) {
  auto term = primary_key::PkFilePrefix(id);
  absl::StrAppend(&term, path, std::string_view{"\0", 1}, version);
  return term;
}

uint64_t SourceFileTermId(std::string_view term) {
  SDB_ASSERT(term.size() > sizeof(uint64_t));
  return absl::big_endian::Load64(term.data());
}

SourceFile ParseSourceFileTerm(irs::bytes_view term) {
  const auto bytes = irs::ViewCast<char>(term);
  const auto rest = bytes.substr(sizeof(uint64_t));
  const auto separator = rest.find('\0');
  SDB_ASSERT(separator != std::string_view::npos);
  return {.id = SourceFileTermId(bytes),
          .path = rest.substr(0, separator),
          .version = rest.substr(separator + 1)};
}

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

std::vector<std::string> SourceFileTerms(const SourceListing& listing) {
  std::vector<std::string> terms;
  terms.reserve(listing.files.size());
  for (size_t i = 0; i < listing.files.size(); ++i) {
    terms.push_back(
      SourceFileTerm(i, listing.files[i].path, listing.versions[i]));
  }
  return terms;
}

HeldFiles CollectHeldFiles(const irs::IndexReader& reader) {
  HeldFiles held;
  for (const auto& segment : reader) {
    const auto* field = segment.field(term_dict::kSourceFileFieldId);
    if (!field) {
      continue;
    }
    held.next_id = std::max(
      held.next_id, SourceFileTermId(irs::ViewCast<char>(field->max())) + 1);
    const auto masked = segment.MaskedDocs();
    auto terms = field->iterator();
    while (terms->next()) {
      if (!masked.Empty()) {
        auto postings = terms->postings(irs::IndexFeatures::None);
        auto doc = postings->Next();
        while (!irs::doc_limits::eof(doc) && masked.Contains(doc)) {
          doc = postings->Next();
        }
        if (irs::doc_limits::eof(doc)) {
          continue;
        }
      }
      const auto file = ParseSourceFileTerm(terms->value());
      auto& entry = held.by_path[std::string{file.path}];
      if (!absl::c_linear_search(entry.ids, file.id)) {
        entry.ids.push_back(file.id);
      }
      entry.version = file.version;
    }
  }
  return held;
}

std::optional<std::string> FindSourceFilePath(const irs::IndexReader& reader,
                                              uint64_t id) {
  const auto prefix = primary_key::PkFilePrefix(id);
  const irs::bytes_view key{
    reinterpret_cast<const irs::byte_type*>(prefix.data()), prefix.size()};
  for (const auto& segment : reader) {
    const auto* field = segment.field(term_dict::kSourceFileFieldId);
    if (!field) {
      continue;
    }
    auto terms = field->iterator();
    if (terms->seek_ge(key) == irs::SeekResult::End ||
        !terms->value().starts_with(key)) {
      continue;
    }
    return std::string{ParseSourceFileTerm(terms->value()).path};
  }
  return std::nullopt;
}

}  // namespace sdb::connector
