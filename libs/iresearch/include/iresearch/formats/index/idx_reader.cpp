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

#include "iresearch/formats/index/idx_reader.hpp"

#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/types.hpp>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/math_utils.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/utils/encryption.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

constexpr duckdb::field_id_t kFooterSlotTermDict = 100;
constexpr duckdb::field_id_t kFooterSlotIvf = 101;

IndexInput::ptr OpenAndCheckHeader(const Directory& dir,
                                   std::string_view filename) {
  auto in = dir.open(filename, IOAdvice::SEQUENTIAL);
  if (!in) {
    throw IoError{absl::StrCat("Failed to open index file, path: ", filename)};
  }
  format_utils::CheckHeader(*in, kIdxFormatName, kIdxFormatVersion,
                            kIdxFormatVersion);
  return in;
}

constexpr uint64_t kTrailerLen = sizeof(uint64_t) + format_utils::kFooterLen;

}  // namespace

struct IdxReader::Impl {
  Encryption::Stream::ptr cipher;
  IndexInput::ptr in;
  uint64_t body_start{};
  std::vector<std::pair<field_id, CentroidsTree>> ivf_entries;
  sdb::containers::FlatHashMap<field_id, size_t> ivf_by_id;
  std::vector<std::pair<field_id, TermDictMeta>> term_dicts;
};

IdxReader::IdxReader(const Directory& dir, std::string_view segment_name)
  : _impl{std::make_unique<Impl>()} {
  const auto filename = absl::StrCat(segment_name, ".", kIdxFormatExt);
  bool exists = false;
  if (!dir.exists(exists, filename)) {
    throw IoError{
      absl::StrCat("Failed to check existence of file, path: ", filename)};
  }
  if (!exists) {
    return;
  }

  auto raw_in = OpenAndCheckHeader(dir, filename);
  auto* enc = dir.attributes().encryption();
  if (Decrypt(filename, *raw_in, enc, _impl->cipher)) {
    SDB_ENSURE(_impl->cipher && _impl->cipher->block_size(),
               "IdxReader: Decrypt returned true but cipher / block_size() "
               "is null for ",
               filename);
  }
  const auto raw_len = raw_in->Length();
  SDB_ENSURE(raw_len >= raw_in->Position() + kTrailerLen,
             "idx: truncated `.idx` file ", filename, " (length ", raw_len,
             " is not large enough to contain header + footer offset + "
             "iresearch footer)");
  const uint64_t body_start = raw_in->Position();
  raw_in->Seek(raw_len - kTrailerLen);
  const uint64_t footer_offset = static_cast<uint64_t>(raw_in->ReadI64());
  raw_in->Seek(body_start);

  if (_impl->cipher) {
    const auto blocks_in_buffer = math::DivCeil64(kDefaultEncryptionBufferSize,
                                                  _impl->cipher->block_size());
    _impl->in = std::make_unique<EncryptedInput>(
      std::move(raw_in), *_impl->cipher, blocks_in_buffer, kTrailerLen);
    _impl->body_start = 0;
  } else {
    _impl->in = std::move(raw_in);
    _impl->body_start = body_start;
  }

  const uint64_t body_len =
    _impl->cipher ? _impl->in->Length() : raw_len - kTrailerLen;
  SDB_ENSURE(footer_offset <= body_len, "idx: corrupted `.idx` file ", filename,
             ": footer offset ", footer_offset, " is out of body range [0, ",
             body_len, "]");
  const auto footer_in = _impl->in->Dup();
  footer_in->Seek(footer_offset);

  duckdb::BinaryDeserializer deserializer{*footer_in};
  deserializer.Begin();
  deserializer.ReadList(
    kFooterSlotTermDict, "term_dict",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        TermDictMeta meta;
        const auto id = obj.ReadProperty<uint64_t>(0, "id");
        meta.features =
          static_cast<IndexFeatures>(obj.ReadProperty<uint32_t>(1, "features"));
        meta.term_count = obj.ReadProperty<uint64_t>(2, "term_count");
        meta.doc_count = obj.ReadProperty<uint64_t>(3, "doc_count");
        meta.total_doc_freq = obj.ReadProperty<uint64_t>(4, "total_doc_freq");
        meta.total_term_freq = obj.ReadProperty<uint64_t>(5, "total_term_freq");
        meta.has_wand = obj.ReadProperty<bool>(6, "has_wand");
        meta.body_offset = obj.ReadProperty<uint64_t>(7, "body_offset");
        meta.norm = obj.ReadProperty<uint64_t>(8, "norm");
        _impl->term_dicts.emplace_back(id, std::move(meta));
      });
    });
  deserializer.ReadList(
    kFooterSlotIvf, "ivf",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        const auto id = obj.ReadProperty<uint64_t>(0, "id");
        const auto tree_offset = obj.ReadProperty<uint64_t>(1, "tree_offset");
        const auto tree_byte_size =
          obj.ReadProperty<uint64_t>(2, "tree_byte_size");
        const auto stats_offset = obj.ReadProperty<uint64_t>(3, "stats_offset");
        const auto stats_byte_size =
          obj.ReadProperty<uint64_t>(4, "stats_byte_size");

        auto body = _impl->in->Dup();
        body->Seek(tree_offset);
        auto entry = CentroidsTree::Deserialize(*body, tree_byte_size);

        if (stats_byte_size != 0) {
          body->Seek(stats_offset);
          const size_t stats_size = static_cast<size_t>(body->ReadI64());
          bstring stats(stats_size, 0);
          body->ReadData(stats.data(), stats_size);
          entry.SetQuantStats(std::move(stats));
        }

        const size_t idx = _impl->ivf_entries.size();
        _impl->ivf_entries.emplace_back(id, std::move(entry));
        _impl->ivf_by_id.emplace(id, idx);
      });
    });
  deserializer.End();
}

IdxReader::~IdxReader() = default;

bool IdxReader::HasIvf(field_id id) const noexcept {
  return _impl->ivf_by_id.contains(id);
}

const CentroidsTree* IdxReader::Ivf(field_id id) const noexcept {
  auto it = _impl->ivf_by_id.find(id);
  return it == _impl->ivf_by_id.end() ? nullptr
                                      : &_impl->ivf_entries[it->second].second;
}

std::span<const std::pair<field_id, TermDictMeta>> IdxReader::TermDicts()
  const noexcept {
  return _impl->term_dicts;
}

IndexInput::ptr IdxReader::ReopenIn() const {
  return _impl->in ? _impl->in->Reopen() : IndexInput::ptr{};
}

uint64_t IdxReader::BodyStart() const noexcept { return _impl->body_start; }

}  // namespace irs
