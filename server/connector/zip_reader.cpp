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

#include "connector/zip_reader.hpp"

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>
#include <string>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace sdb::connector::zip {
namespace {

struct EntryInfo {
  uint64_t size;
  uint64_t compressed_size;
  uint64_t data_offset;
  uint32_t crc;
  uint16_t method;
};

EntryInfo FindEntry(Source& source, std::string_view member);

constexpr size_t kChunk = 64 * 1024;
constexpr uint32_t kMax32 = UINT32_MAX;

void InvalidException(const char* message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR), ERR_MSG(message));
}

// Loads width bytes at the given offset as a little-endian integer
uint64_t GetNumber(std::span<const std::byte> bytes, size_t offset,
                   size_t width) {
  if (offset > bytes.size() || width > bytes.size() - offset) {
    InvalidException("Truncated ZIP header");
  }

  switch (width) {
    case 2:
      return absl::little_endian::Load16(bytes.data() + offset);
    case 4:
      return absl::little_endian::Load32(bytes.data() + offset);
    case 8:
      return absl::little_endian::Load64(bytes.data() + offset);
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("Unsupported ZIP integer width: ", width));
  }
}

void CheckRange(uint64_t offset, uint64_t length, uint64_t end) {
  if (offset > end || length > end - offset) {
    InvalidException("ZIP range exceeds file bounds");
  }
}

void ValidateZipMemberName(std::string_view name) {
  if (name.empty() || name.front() == '/' || name.find('\0') != name.npos ||
      name.find('\\') != name.npos || name.find(':') != name.npos) {
    InvalidException("Unsafe or unsupported ZIP member name");
  }

  bool has_component = false;

  while (!name.empty()) {
    const auto slash = name.find('/');
    const auto part = name.substr(0, slash);

    if (part == "..") {
      InvalidException("ZIP member traversal is not allowed");
    }

    has_component |= !part.empty() && part != ".";

    if (slash == name.npos) {
      break;
    }

    name.remove_prefix(slash + 1);
  }

  if (!has_component) {
    InvalidException("Empty ZIP member name");
  }
}

std::string NormalizeZipMemberName(std::string_view name) {
  ValidateZipMemberName(name);
  std::string result;

  while (!name.empty()) {
    auto slash = name.find('/');
    auto part = name.substr(0, slash);

    if (!part.empty() && part != ".") {
      if (!result.empty()) {
        result += '/';
      }
      result.append(part);
    }

    if (slash == name.npos) {
      break;
    }

    name.remove_prefix(slash + 1);
  }

  return result;
}

// Reads ZIP64 extra fields to replace size, offset, and disk placeholders
void ReadZip64ExtraField(std::span<const std::byte> extra, uint64_t& size,
                         uint64_t& compressed, uint64_t& offset,
                         uint64_t& disk) {
  bool found = false;
  size_t pos = 0;

  while (pos < extra.size()) {
    auto type = GetNumber(extra, pos, 2);
    auto length = GetNumber(extra, pos + 2, 2);
    pos += 4;
    CheckRange(pos, length, extra.size());

    if (type == 1) {
      if (found) {
        InvalidException("Duplicate ZIP64 extra field");
      }

      found = true;
      auto values = extra.subspan(pos, length);
      size_t cursor = 0;

      for (auto* value : {&size, &compressed, &offset}) {
        if (*value == kMax32) {
          *value = GetNumber(values, cursor, 8);
          cursor += 8;
        }
      }

      if (disk == UINT16_MAX) {
        disk = GetNumber(values, cursor, 4);
      }
    }

    pos += length;
  }

  if (!found && (size == kMax32 || compressed == kMax32 || offset == kMax32 ||
                 disk == UINT16_MAX)) {
    InvalidException("Missing ZIP64 extra field");
  }
}

bool ZipMemberNameLess(std::string_view lhs, std::string_view rhs) {
  while (true) {
    const auto lslash = lhs.find('/');
    const auto rslash = rhs.find('/');
    const auto lpart = lhs.substr(0, lslash);
    const auto rpart = rhs.substr(0, rslash);

    if (lpart != rpart) {
      return lpart < rpart;
    }

    if (lslash == lhs.npos || rslash == rhs.npos) {
      return lslash == lhs.npos && rslash != rhs.npos;
    }

    lhs.remove_prefix(lslash + 1);
    rhs.remove_prefix(rslash + 1);
  }
}

// Caches central directory metadata up to 50 MiB inclusive; larger directories
// are read from storage on each Read call
class CentralDirectory {
 public:
  CentralDirectory(Source& source, uint64_t offset, uint64_t size)
    : _source{source}, _offset{offset}, _size{size} {
    if (size <= kMetadataCacheLimit) {
      _cache.resize(size);

      for (size_t cursor = 0; cursor < _cache.size(); cursor += kChunk) {
        const auto count = std::min(kChunk, _cache.size() - cursor);
        _source.Check();
        _source.ReadAt(offset + cursor,
                       std::span{_cache}.subspan(cursor, count));
      }
    }
  }

  void Read(uint64_t offset, std::span<std::byte> output) {
    _source.Check();
    CheckRange(offset, output.size(), _offset + _size);

    if (offset < _offset) {
      InvalidException("ZIP central directory read precedes directory");
    }

    if (!_cache.empty()) {
      std::memcpy(output.data(), _cache.data() + (offset - _offset),
                  output.size());
    } else {
      _source.ReadAt(offset, output);
    }
  }

  void Add(uint64_t offset) { _entries.push_back(offset); }

  void ValidateZipMemberNames() {
    std::sort(_entries.begin(), _entries.end(),
              [&](uint64_t lhs, uint64_t rhs) {
                return ZipMemberNameLess(ZipMemberNameAt(lhs).first,
                                         ZipMemberNameAt(rhs).first);
              });
    std::string previous;
    bool previous_directory = true;

    for (auto offset : _entries) {
      auto [name, directory] = ZipMemberNameAt(offset);

      if (name == previous ||
          (!previous_directory && name.starts_with(previous) &&
           name.size() > previous.size() && name[previous.size()] == '/')) {
        InvalidException("Duplicate ZIP name or file/directory conflict");
      }

      previous = std::move(name);
      previous_directory = directory;
    }
  }

 private:
  std::pair<std::string, bool> ZipMemberNameAt(uint64_t offset) {
    constexpr size_t kHeaderSize = 46;
    constexpr size_t kNameLengthOffset = 28;
    constexpr size_t kExternalAttributesOffset = 38;
    constexpr uint32_t kDosDirectoryAttribute = 0x10;
    constexpr unsigned kUnixModeShift = 16;
    constexpr uint32_t kUnixFileTypeMask = 0170000;
    constexpr uint32_t kUnixDirectoryType = 0040000;

    std::array<std::byte, kHeaderSize> header;
    Read(offset, header);

    std::vector<std::byte> name_bytes(
      GetNumber(header, kNameLengthOffset, sizeof(uint16_t)));
    Read(offset + kHeaderSize, name_bytes);

    const std::string_view raw{reinterpret_cast<const char*>(name_bytes.data()),
                               name_bytes.size()};
    auto name = NormalizeZipMemberName(raw);
    const auto attributes =
      GetNumber(header, kExternalAttributesOffset, sizeof(uint32_t));
    const bool directory =
      raw.back() == '/' || (attributes & kDosDirectoryAttribute) ||
      ((attributes >> kUnixModeShift) & kUnixFileTypeMask) ==
        kUnixDirectoryType;

    return {std::move(name), directory};
  }

  Source& _source;
  const uint64_t _offset;
  const uint64_t _size;
  std::vector<std::byte> _cache;
  std::vector<uint64_t> _entries;
};

}  // namespace

Path ParsePath(std::string_view path) {
  if (!path.starts_with("zip://")) {
    InvalidException("ZIP path must start with zip://");
  }

  path.remove_prefix(6);

  if (path.find_first_of("*?[") != path.npos) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("ZIP glob patterns are not supported yet"));
  }

  const auto split = path.find(".zip");
  if (split == path.npos || split + 4 >= path.size() ||
      path[split + 4] != '/') {
    InvalidException("Expected zip://archive.zip/member path");
  }

  Path result{path.substr(0, split + 4), path.substr(split + 5)};

  if (result.archive.find(':') != path.npos ||
      result.archive.find('\0') != path.npos ||
      result.archive.find('\\') != path.npos ||
      result.archive.starts_with("//")) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("ZIP supports only local archive files"));
  }

  if (result.member.empty() || result.member.back() == '/') {
    InvalidException(
      "ZIP path must select one file, not an archive or directory");
  }

  ValidateZipMemberName(result.member);

  return result;
}

MemberReader::MemberReader(Source& source, std::string_view member)
  : _source{source} {
  const auto entry = FindEntry(source, member);
  _size = entry.size;
  _compressed_size = entry.compressed_size;
  _data_offset = entry.data_offset;
  _expected_crc = entry.crc;
  _method = entry.method;
}

MemberReader::~MemberReader() noexcept { Stop(); }

namespace {

// Finds the requested archive member and returns its validated metadata
EntryInfo FindEntry(Source& source, std::string_view member) {
  constexpr size_t kEocdSize = 22;
  constexpr size_t kEocdCommentLengthOffset = 20;
  constexpr size_t kEocdDiskOffset = 4;
  constexpr size_t kEocdDirectoryDiskOffset = 6;
  constexpr size_t kEocdDiskEntryCountOffset = 8;
  constexpr size_t kEocdEntryCountOffset = 10;
  constexpr size_t kEocdDirectorySizeOffset = 12;
  constexpr size_t kEocdDirectoryOffset = 16;

  constexpr size_t kZip64LocatorSize = 20;
  constexpr size_t kZip64LocatorDiskOffset = 4;
  constexpr size_t kZip64LocatorDiskCountOffset = 16;
  constexpr size_t kZip64LocatorRecordOffset = 8;

  constexpr size_t kZip64EndSize = 56;
  constexpr size_t kZip64EndLengthOffset = 4;
  constexpr size_t kZip64EndMinPayloadSize = 44;
  constexpr size_t kZip64EndPrefixSize = 12;
  constexpr size_t kZip64EndDiskOffset = 16;
  constexpr size_t kZip64EndDirectoryDiskOffset = 20;
  constexpr size_t kZip64EndDiskEntryCountOffset = 24;
  constexpr size_t kZip64EndEntryCountOffset = 32;
  constexpr size_t kZip64EndDirectorySizeOffset = 40;
  constexpr size_t kZip64EndDirectoryOffset = 48;

  constexpr size_t kCentralHeaderSize = 46;
  constexpr size_t kCentralHeaderNameLengthOffset = 28;
  constexpr size_t kCentralHeaderExtraLengthOffset = 30;
  constexpr size_t kCentralHeaderCommentLengthOffset = 32;
  constexpr size_t kCentralHeaderFlagsOffset = 8;
  constexpr size_t kCentralHeaderAttributesOffset = 38;
  constexpr size_t kCentralHeaderSizeOffset = 24;
  constexpr size_t kCentralHeaderCompressedSizeOffset = 20;
  constexpr size_t kCentralHeaderLocalHeaderOffset = 42;
  constexpr size_t kCentralHeaderDiskOffset = 34;
  constexpr size_t kCentralHeaderMethodOffset = 10;
  constexpr size_t kCentralHeaderCrcOffset = 16;

  constexpr size_t kLocalHeaderSize = 30;
  constexpr size_t kLocalHeaderFlagsOffset = 6;
  constexpr size_t kLocalHeaderMethodOffset = 8;
  constexpr size_t kLocalHeaderNameLengthOffset = 26;
  constexpr size_t kLocalHeaderExtraLengthOffset = 28;
  constexpr size_t kLocalHeaderSizeOffset = 22;
  constexpr size_t kLocalHeaderCompressedSizeOffset = 18;
  constexpr size_t kLocalHeaderCrcOffset = 14;

  constexpr size_t kMaxCommentSize = UINT16_MAX;
  constexpr size_t kSignatureOffset = 0;
  constexpr uint32_t kEocdSignature = 0x06054b50;
  constexpr uint32_t kZip64LocatorSignature = 0x07064b50;
  constexpr uint32_t kZip64EndSignature = 0x06064b50;
  constexpr uint32_t kCentralHeaderSignature = 0x02014b50;
  constexpr uint32_t kLocalHeaderSignature = 0x04034b50;
  constexpr uint32_t kDescriptorSignature = 0x08074b50;
  constexpr uint32_t kSingleDiskCount = 1;

  constexpr uint32_t kDosDirectoryAttribute = 0x10;
  constexpr unsigned kUnixModeShift = 16;
  constexpr uint32_t kUnixFileTypeMask = 0170000;
  constexpr uint32_t kUnixRegularFileType = 0100000;
  constexpr uint32_t kUnixDirectoryType = 0040000;
  constexpr uint16_t kEncryptedFlag = 1;
  constexpr uint16_t kPatchedDataFlag = 32;
  constexpr uint16_t kStrongEncryptionFlag = 64;
  constexpr uint16_t kMaskedHeaderFlag = 8192;
  constexpr uint16_t kDataDescriptorFlag = 8;

  constexpr size_t kDescriptorMaxSize = 24;
  constexpr size_t kDescriptorCrcSize = sizeof(uint32_t);
  constexpr size_t kDescriptorSizeFieldCount = 2;
  constexpr size_t kDescriptorSignatureSize = sizeof(uint32_t);

  source.Check();

  EntryInfo entry_info{};
  const auto file_size = source.Size();
  const auto wanted = NormalizeZipMemberName(member);
  std::vector<std::byte> tail(
    std::min<uint64_t>(file_size, kMaxCommentSize + kEocdSize));
  source.ReadAt(file_size - tail.size(), tail);

  // 1. Locate the central directory and read its size and entry count
  size_t eocd = tail.size();
  // 1.1. Find the end-of-central-directory record offset within the tail buffer
  if (tail.size() >= kEocdSize) {
    for (size_t i = tail.size() - kEocdSize + 1; i-- > 0;) {
      if (GetNumber(tail, i, sizeof(uint32_t)) == kEocdSignature &&
          GetNumber(tail, i + kEocdCommentLengthOffset, sizeof(uint16_t)) ==
            tail.size() - i - kEocdSize) {
        eocd = i;
        break;
      }
    }
  }

  if (eocd == tail.size()) {
    InvalidException("ZIP end of central directory not found");
  }

  if (GetNumber(tail, eocd + kEocdDiskOffset, sizeof(uint16_t)) ||
      GetNumber(tail, eocd + kEocdDirectoryDiskOffset, sizeof(uint16_t)) ||
      GetNumber(tail, eocd + kEocdDiskEntryCountOffset, sizeof(uint16_t)) !=
        GetNumber(tail, eocd + kEocdEntryCountOffset, sizeof(uint16_t))) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("Multi-disk ZIP is not supported"));
  }

  uint64_t count =
    GetNumber(tail, eocd + kEocdEntryCountOffset, sizeof(uint16_t));
  uint64_t cd_size =
    GetNumber(tail, eocd + kEocdDirectorySizeOffset, sizeof(uint32_t));
  uint64_t cd_offset =
    GetNumber(tail, eocd + kEocdDirectoryOffset, sizeof(uint32_t));
  uint64_t cd_bound = file_size - tail.size() + eocd;

  // 1.2. Read ZIP64 directory metadata if required
  if (count == UINT16_MAX || cd_size == kMax32 || cd_offset == kMax32) {
    if (cd_bound < kZip64LocatorSize) {
      InvalidException("Missing ZIP64 locator");
    }

    std::array<std::byte, kZip64LocatorSize> locator;
    source.ReadAt(cd_bound - kZip64LocatorSize, locator);

    if (GetNumber(locator, kSignatureOffset, sizeof(uint32_t)) !=
          kZip64LocatorSignature ||
        GetNumber(locator, kZip64LocatorDiskOffset, sizeof(uint32_t)) ||
        GetNumber(locator, kZip64LocatorDiskCountOffset, sizeof(uint32_t)) !=
          kSingleDiskCount) {
      InvalidException("Invalid or multi-disk ZIP64 locator");
    }

    auto zip64_offset =
      GetNumber(locator, kZip64LocatorRecordOffset, sizeof(uint64_t));
    CheckRange(zip64_offset, kZip64EndSize, cd_bound - kZip64LocatorSize);
    std::array<std::byte, kZip64EndSize> header;
    source.ReadAt(zip64_offset, header);
    auto length = GetNumber(header, kZip64EndLengthOffset, sizeof(uint64_t));

    if (GetNumber(header, kSignatureOffset, sizeof(uint32_t)) !=
          kZip64EndSignature ||
        length < kZip64EndMinPayloadSize ||
        GetNumber(header, kZip64EndDiskOffset, sizeof(uint32_t)) ||
        GetNumber(header, kZip64EndDirectoryDiskOffset, sizeof(uint32_t)) ||
        GetNumber(header, kZip64EndDiskEntryCountOffset, sizeof(uint64_t)) !=
          GetNumber(header, kZip64EndEntryCountOffset, sizeof(uint64_t))) {
      InvalidException("Invalid ZIP64 end record");
    }

    CheckRange(zip64_offset + kZip64EndPrefixSize, length,
               cd_bound - kZip64LocatorSize);

    if (zip64_offset + kZip64EndPrefixSize + length !=
        cd_bound - kZip64LocatorSize) {
      InvalidException("Invalid ZIP64 end record length");
    }

    count = GetNumber(header, kZip64EndEntryCountOffset, sizeof(uint64_t));
    cd_size = GetNumber(header, kZip64EndDirectorySizeOffset, sizeof(uint64_t));
    cd_offset = GetNumber(header, kZip64EndDirectoryOffset, sizeof(uint64_t));
    cd_bound = zip64_offset;
  }

  CheckRange(cd_offset, cd_size, cd_bound);

  if (count > cd_size / kCentralHeaderSize) {
    InvalidException("Invalid ZIP entry count");
  }

  // 2. Scan and validate the central directory to find the requested member
  CentralDirectory directory_index{source, cd_offset, cd_size};
  std::vector<std::byte> variable;
  std::string selected_name;
  uint64_t selected_offset = 0;
  uint16_t selected_flags = 0;
  bool found = false;
  uint64_t cursor = cd_offset;

  // 2.1. Read and validate each central directory entry
  for (uint64_t i = 0; i < count; ++i) {
    source.Check();
    std::array<std::byte, kCentralHeaderSize> header;
    directory_index.Read(cursor, header);

    if (GetNumber(header, kSignatureOffset, sizeof(uint32_t)) !=
        kCentralHeaderSignature) {
      InvalidException("Invalid ZIP central directory header");
    }

    auto name_size =
      GetNumber(header, kCentralHeaderNameLengthOffset, sizeof(uint16_t));
    auto extra_size =
      GetNumber(header, kCentralHeaderExtraLengthOffset, sizeof(uint16_t));
    auto comment_size =
      GetNumber(header, kCentralHeaderCommentLengthOffset, sizeof(uint16_t));
    CheckRange(cursor + kCentralHeaderSize,
               name_size + extra_size + comment_size, cd_offset + cd_size);
    variable.resize(name_size + extra_size);
    directory_index.Read(cursor + kCentralHeaderSize, variable);
    std::string_view raw{reinterpret_cast<const char*>(variable.data()),
                         name_size};
    auto name = NormalizeZipMemberName(raw);
    const auto flags =
      GetNumber(header, kCentralHeaderFlagsOffset, sizeof(uint16_t));
    const auto attributes =
      GetNumber(header, kCentralHeaderAttributesOffset, sizeof(uint32_t));
    const auto unix_type = (attributes >> kUnixModeShift) & kUnixFileTypeMask;
    bool directory = raw.back() == '/' ||
                     (attributes & kDosDirectoryAttribute) ||
                     unix_type == kUnixDirectoryType;

    if (unix_type && unix_type != kUnixRegularFileType &&
        unix_type != kUnixDirectoryType) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("ZIP symlink and special entries are not supported"));
    }

    directory_index.Add(cursor);
    uint64_t size =
      GetNumber(header, kCentralHeaderSizeOffset, sizeof(uint32_t));
    uint64_t compressed =
      GetNumber(header, kCentralHeaderCompressedSizeOffset, sizeof(uint32_t));
    uint64_t offset =
      GetNumber(header, kCentralHeaderLocalHeaderOffset, sizeof(uint32_t));
    uint64_t disk =
      GetNumber(header, kCentralHeaderDiskOffset, sizeof(uint16_t));
    ReadZip64ExtraField(std::span{variable}.subspan(name_size), size,
                        compressed, offset, disk);

    if (disk) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Multi-disk ZIP is not supported"));
    }

    CheckRange(offset, kLocalHeaderSize, cd_offset);

    // 2.2. Save metadata for the requested member
    if (name == wanted) {
      if (directory) {
        InvalidException("ZIP member is a directory");
      }

      if (flags & (kEncryptedFlag | kPatchedDataFlag | kStrongEncryptionFlag |
                   kMaskedHeaderFlag)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("Encrypted or patched ZIP entries are not supported"));
      }

      entry_info.method =
        GetNumber(header, kCentralHeaderMethodOffset, sizeof(uint16_t));

      if (entry_info.method != kStore && entry_info.method != kDeflate) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("Unsupported ZIP compression method"));
      }

      if (size > INT64_MAX || compressed > INT64_MAX) {
        InvalidException("ZIP member size exceeds supported range");
      }

      entry_info.size = size;
      entry_info.compressed_size = compressed;
      entry_info.crc =
        GetNumber(header, kCentralHeaderCrcOffset, sizeof(uint32_t));
      selected_offset = offset;
      selected_flags = flags;
      selected_name.assign(raw);
      found = true;
    }

    cursor += kCentralHeaderSize + name_size + extra_size + comment_size;
  }

  // 2.3. Validate directory size and member names, and require a match
  if (cursor != cd_offset + cd_size) {
    InvalidException("ZIP central directory size mismatch");
  }

  directory_index.ValidateZipMemberNames();

  if (!found) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FILE),
                    ERR_MSG("ZIP member not found"));
  }

  // 3. Validate the selected member's local header and locate its data
  std::array<std::byte, kLocalHeaderSize> local;
  source.ReadAt(selected_offset, local);

  if (GetNumber(local, kSignatureOffset, sizeof(uint32_t)) !=
        kLocalHeaderSignature ||
      GetNumber(local, kLocalHeaderFlagsOffset, sizeof(uint16_t)) !=
        selected_flags ||
      GetNumber(local, kLocalHeaderMethodOffset, sizeof(uint16_t)) !=
        entry_info.method) {
    InvalidException("ZIP local header disagrees with central directory");
  }

  auto name_size =
    GetNumber(local, kLocalHeaderNameLengthOffset, sizeof(uint16_t));
  auto extra_size =
    GetNumber(local, kLocalHeaderExtraLengthOffset, sizeof(uint16_t));
  CheckRange(selected_offset + kLocalHeaderSize, name_size + extra_size,
             cd_offset);
  variable.resize(name_size + extra_size);
  source.ReadAt(selected_offset + kLocalHeaderSize, variable);

  if (std::string_view{reinterpret_cast<const char*>(variable.data()),
                       name_size} != selected_name) {
    InvalidException("ZIP local filename disagrees with central directory");
  }

  entry_info.data_offset =
    selected_offset + kLocalHeaderSize + name_size + extra_size;
  CheckRange(entry_info.data_offset, entry_info.compressed_size, cd_offset);
  uint64_t local_size =
    GetNumber(local, kLocalHeaderSizeOffset, sizeof(uint32_t));
  uint64_t local_compressed =
    GetNumber(local, kLocalHeaderCompressedSizeOffset, sizeof(uint32_t));
  uint64_t unused_offset = 0;
  uint64_t unused_disk = 0;
  bool descriptor64 = local_size == kMax32 || local_compressed == kMax32 ||
                      entry_info.size >= kMax32 ||
                      entry_info.compressed_size >= kMax32;
  ReadZip64ExtraField(std::span{variable}.subspan(name_size), local_size,
                      local_compressed, unused_offset, unused_disk);

  // 4. Validate sizes and CRC against the local header or data descriptor
  if (!(selected_flags & kDataDescriptorFlag)) {
    if (local_size != entry_info.size ||
        local_compressed != entry_info.compressed_size ||
        GetNumber(local, kLocalHeaderCrcOffset, sizeof(uint32_t)) !=
          entry_info.crc) {
      InvalidException(
        "ZIP local sizes or CRC disagree with central directory");
    }
  } else {
    if ((local_size && local_size != entry_info.size) ||
        (local_compressed && local_compressed != entry_info.compressed_size) ||
        (GetNumber(local, kLocalHeaderCrcOffset, sizeof(uint32_t)) &&
         GetNumber(local, kLocalHeaderCrcOffset, sizeof(uint32_t)) !=
           entry_info.crc)) {
      InvalidException("ZIP local header disagrees with data descriptor");
    }

    const auto descriptor_offset =
      entry_info.data_offset + entry_info.compressed_size;
    std::array<std::byte, kDescriptorMaxSize> descriptor;
    const size_t width = descriptor64 ? sizeof(uint64_t) : sizeof(uint32_t);
    CheckRange(descriptor_offset,
               kDescriptorCrcSize + kDescriptorSizeFieldCount * width,
               cd_offset);
    const auto available =
      std::min<uint64_t>(descriptor.size(), cd_offset - descriptor_offset);
    source.ReadAt(descriptor_offset, std::span{descriptor}.first(available));
    const auto matches = [&](size_t prefix) {
      return prefix + kDescriptorCrcSize + kDescriptorSizeFieldCount * width <=
               available &&
             GetNumber(descriptor, prefix, sizeof(uint32_t)) ==
               entry_info.crc &&
             GetNumber(descriptor, prefix + kDescriptorCrcSize, width) ==
               entry_info.compressed_size &&
             GetNumber(descriptor, prefix + kDescriptorCrcSize + width,
                       width) == entry_info.size;
    };

    if (!matches(0) && !(GetNumber(descriptor, kSignatureOffset,
                                   sizeof(uint32_t)) == kDescriptorSignature &&
                         matches(kDescriptorSignatureSize))) {
      InvalidException("ZIP data descriptor disagrees with central directory");
    }
  }

  if (entry_info.method == kStore &&
      entry_info.size != entry_info.compressed_size) {
    InvalidException("Stored ZIP member has inconsistent sizes");
  }

  return entry_info;
}

}  // namespace

void MemberReader::Stop() noexcept {
  if (_started) {
    inflateEnd(&_stream);
    _started = false;
  }
}

void MemberReader::Start() {
  Stop();
  _input.resize(kChunk);
  _discard.resize(kChunk);
  _stream = {};
  const int status = inflateInit2(&_stream, -MAX_WBITS);

  if (status != Z_OK) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("ZIP decoder allocation failed"));
  }

  _started = true;
  _decoded = 0;
  _loaded = 0;
  _crc = 0;
  _ended = false;
}

void MemberReader::CheckState() {
  if (_failed) {
    InvalidException("ZIP reader is in failed state; reopen the file");
  }

  _source.Check();
}

// Decompresses DEFLATE data into output, producing at most kChunk bytes per
// call
size_t MemberReader::Inflate(std::span<std::byte> output) {
  _source.Check();

  // 1. Prepare data for decompression
  if (!_stream.avail_in && _loaded < _compressed_size) {
    const auto count = std::min<uint64_t>(kChunk, _compressed_size - _loaded);
    _source.ReadAt(_data_offset + _loaded, std::span{_input}.first(count));
    _loaded += count;
    _stream.next_in = reinterpret_cast<Bytef*>(_input.data());
    _stream.avail_in = count;
  }

  const auto count = std::min(output.size(), kChunk);
  _stream.next_out = reinterpret_cast<Bytef*>(output.data());
  _stream.avail_out = count;

  // 2. Decompress using zlib and check the result
  const auto status = inflate(&_stream, Z_NO_FLUSH);

  if (status == Z_MEM_ERROR) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("ZIP decoder allocation failed"));
  }

  if (status != Z_OK && status != Z_STREAM_END) {
    InvalidException("Invalid or truncated ZIP DEFLATE stream");
  }

  const size_t produced = count - _stream.avail_out;
  if (produced > _size - _decoded) {
    InvalidException("ZIP decompressed size exceeds declared size");
  }

  _decoded += produced;
  _crc = crc32(_crc, reinterpret_cast<const Bytef*>(output.data()), produced);
  _ended = status == Z_STREAM_END;

  if (_ended &&
      (_decoded != _size || _loaded - _stream.avail_in != _compressed_size ||
       _crc != _expected_crc)) {
    InvalidException("ZIP stream size or CRC mismatch");
  }

  return produced;
}

// Fills output with uncompressed data in portions of at most kChunk bytes
size_t MemberReader::ReadStored(std::span<std::byte> output) {
  if (_position != _crc_position) {
    _crc_valid = false;
  }

  size_t done = 0;
  while (done < output.size()) {
    _source.Check();
    auto part = output.subspan(done, std::min(kChunk, output.size() - done));
    _source.ReadAt(_data_offset + _position + done, part);

    if (_crc_valid) {
      _crc =
        crc32(_crc, reinterpret_cast<const Bytef*>(part.data()), part.size());
      _crc_position += part.size();
    }

    done += part.size();
  }

  return done;
}

void MemberReader::Advance(uint64_t target) {
  if (!_started || target < _decoded) {
    Start();
  }

  while (_decoded < target) {
    const auto count = std::min<uint64_t>(_discard.size(), target - _decoded);
    Inflate(std::span{_discard}.first(count));
  }
}

void MemberReader::Finish() {
  if (_method == kDeflate && !_ended) {
    std::array<std::byte, 1> probe;
    while (!_ended) {
      Inflate(probe);
    }
  } else if (_method == kStore && _crc_valid && _crc != _expected_crc) {
    InvalidException("ZIP CRC mismatch");
  }
}

// Reads member data at the current position and advances it by the bytes read
size_t MemberReader::Read(std::span<std::byte> output) {
  try {
    CheckState();

    if (output.empty()) {
      return 0;
    }

    size_t count = std::min<uint64_t>(output.size(), _size - _position);

    if (_method == kDeflate) {
      Advance(_position);
      size_t done = 0;

      while (done < count) {
        done += Inflate(output.subspan(done, count - done));
      }
    } else {
      count = ReadStored(output.first(count));
    }

    _position += count;
    if (_position == _size) {
      Finish();
    }

    return count;
  } catch (...) {
    _failed = true;
    throw;
  }
}

void MemberReader::Seek(uint64_t offset) {
  CheckState();
  CheckRange(offset, 0, _size);
  _position = offset;

  if (!offset && _method == kStore) {
    _crc_position = 0;
    _crc_valid = true;
    _crc = 0;
  }
}

void MemberReader::ReadAt(uint64_t offset, std::span<std::byte> output) {
  CheckRange(offset, output.size(), _size);
  Seek(offset);
  Read(output);
}

}  // namespace sdb::connector::zip
