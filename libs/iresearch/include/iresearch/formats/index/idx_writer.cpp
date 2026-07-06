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

#include "iresearch/formats/index/idx_writer.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types.hpp>
#include <optional>
#include <utility>
#include <vector>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/math_utils.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "iresearch/utils/encryption.hpp"

namespace irs {
namespace {

constexpr duckdb::field_id_t kFooterSlotTermDict = 100;
constexpr duckdb::field_id_t kFooterSlotIvf = 101;
constexpr duckdb::field_id_t kFooterSlotTermsBodyStart = 102;

}  // namespace

struct IvfCentroidEntry {
  field_id column_id;
  uint64_t offset = 0;
  uint64_t byte_size = 0;
};

struct TermDictEntry {
  field_id id;
  TermDictMeta meta;
};

struct IdxWriter::Impl {
  Directory* dir;
  std::string filename;
  duckdb::DatabaseInstance* db;
  Encryption::Stream::ptr cipher;
  IndexOutput::ptr out;
  std::vector<IvfCentroidEntry> ivf_entries;
  std::vector<TermDictEntry> term_dict_entries;
  std::optional<uint64_t> terms_body_start;
};

IdxWriter::IdxWriter(Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db)
  : _impl{std::make_unique<Impl>()} {
  _impl->dir = &dir;
  _impl->db = &db;
  _impl->filename = absl::StrCat(segment_name, ".", kIdxFormatExt);
}

IdxWriter::~IdxWriter() {
  if (_impl && _impl->out) {
    Rollback();
  }
}

void IdxWriter::EnsureOut() {
  if (_impl->out) {
    return;
  }
  auto out = _impl->dir->create(_impl->filename);
  if (!out) {
    throw IoError{
      absl::StrCat("Failed to create index file, path: ", _impl->filename)};
  }
  format_utils::WriteHeader(*out, kIdxFormatName, kIdxFormatVersion);
  auto* enc = _impl->dir->attributes().encryption();
  bstring enc_header;
  const bool encrypted =
    Encrypt(_impl->filename, *out, enc, enc_header, _impl->cipher);
  SDB_ENSURE(!encrypted || (_impl->cipher && _impl->cipher->block_size()),
             sdb::ERROR_INTERNAL,
             "IdxWriter::EnsureOut: Encrypt returned true but cipher / "
             "block_size() is null for ",
             _impl->filename);
  if (encrypted) {
    const auto blocks_in_buffer = math::DivCeil64(kDefaultEncryptionBufferSize,
                                                  _impl->cipher->block_size());
    out = IndexOutput::ptr{
      new EncryptedOutput{std::move(out), *_impl->cipher, blocks_in_buffer}};
  }
  _impl->out = std::move(out);
}

IndexOutput& IdxWriter::BlocksOut() {
  EnsureOut();
  return *_impl->out;
}

void IdxWriter::AddCentroidsEntry(field_id id, uint64_t offset,
                                  uint64_t byte_size) {
  _impl->ivf_entries.push_back(IvfCentroidEntry{
    .column_id = id, .offset = offset, .byte_size = byte_size});
}

void IdxWriter::AddTermDictEntry(field_id id, TermDictMeta meta) {
  _impl->term_dict_entries.push_back(
    TermDictEntry{.id = id, .meta = std::move(meta)});
}

void IdxWriter::SetTermsBodyStart(uint64_t offset) noexcept {
  if (!_impl->terms_body_start.has_value()) {
    _impl->terms_body_start = offset;
  }
}

bool IdxWriter::Empty() const noexcept {
  return _impl->ivf_entries.empty() && _impl->term_dict_entries.empty();
}

void IdxWriter::Commit() {
  if (Empty() && !_impl->out) {
    return;
  }

  EnsureOut();

  const uint64_t footer_offset = _impl->out->Position();

  duckdb::BinarySerializer serializer{*_impl->out};
  serializer.Begin();
  serializer.WriteList(
    kFooterSlotTermDict, "term_dict", _impl->term_dict_entries.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& e = _impl->term_dict_entries[i];
      list.WriteObject([&](duckdb::Serializer& obj) {
        obj.WriteProperty<uint64_t>(0, "id", e.id);
        obj.WriteProperty<uint32_t>(1, "features",
                                    static_cast<uint32_t>(e.meta.features));
        obj.WriteProperty<uint64_t>(2, "term_count", e.meta.term_count);
        obj.WriteProperty<uint64_t>(3, "doc_count", e.meta.doc_count);
        obj.WriteProperty<uint64_t>(4, "total_doc_freq", e.meta.total_doc_freq);
        obj.WriteProperty<uint64_t>(5, "total_term_freq",
                                    e.meta.total_term_freq);
        obj.WriteProperty<bool>(6, "has_wand", e.meta.has_wand);
        obj.WriteProperty<uint64_t>(7, "body_offset", e.meta.body_offset);
        obj.WriteProperty<uint64_t>(8, "norm", e.meta.norm);
      });
    });
  serializer.WriteList(kFooterSlotIvf, "ivf", _impl->ivf_entries.size(),
                       [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                         const auto& e = _impl->ivf_entries[i];
                         list.WriteObject([&](duckdb::Serializer& obj) {
                           obj.WriteProperty<uint64_t>(0, "id", e.column_id);
                           obj.WriteProperty<uint64_t>(1, "offset", e.offset);
                           obj.WriteProperty<uint64_t>(2, "byte_size",
                                                       e.byte_size);
                         });
                       });
  if (_impl->terms_body_start.has_value()) {
    serializer.WriteProperty<uint64_t>(kFooterSlotTermsBodyStart, "terms_start",
                                       *_impl->terms_body_start);
  }
  serializer.End();

  IndexOutput* trailer_out = _impl->out.get();
  IndexOutput::ptr raw_out;
  if (_impl->cipher) {
    auto& enc_out = static_cast<EncryptedOutput&>(*_impl->out);
    enc_out.Flush();
    raw_out = enc_out.Release();
    _impl->out = std::move(raw_out);
    trailer_out = _impl->out.get();
  }
  trailer_out->WriteU64(footer_offset);
  format_utils::WriteFooter(*trailer_out);
  _impl->out.reset();
  _impl->cipher.reset();
}

void IdxWriter::Rollback() noexcept {
  _impl->out.reset();
  _impl->cipher.reset();
  _impl->ivf_entries.clear();
  _impl->term_dict_entries.clear();
  _impl->terms_body_start.reset();
}

}  // namespace irs
