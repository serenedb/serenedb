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

#include <gtest/gtest.h>

#include <random>

#include "connector/zip_reader.hpp"
#include "iresearch/utils/pg/sql_exception.hpp"
#include "zip_fixture.h"

namespace {

using sdb::connector::zip::kDeflate;
using sdb::connector::zip::kStore;

using sdb::connector::zip::MemberReader;

struct Source final : sdb::connector::zip::Source {
  explicit Source(const std::string& bytes) : bytes{bytes} {}

  uint64_t Size() const final { return bytes.size(); }

  void ReadAt(uint64_t offset, std::span<std::byte> output) final {
    if (offset > bytes.size() || output.size() > bytes.size() - offset) {
      throw std::runtime_error{"short source"};
    }

    std::memcpy(output.data(), bytes.data() + offset, output.size());
    read += output.size();
    largest_read = std::max(largest_read, output.size());
  }

  void Check() final {
    if (cancel_after && ++checks >= cancel_after) {
      throw std::runtime_error{"cancelled"};
    }
  }

  const std::string& bytes;
  size_t read = 0;
  size_t largest_read = 0;
  size_t checks = 0;
  size_t cancel_after = 0;
};

std::string ReadAll(MemberReader& reader) {
  std::string result;
  std::array<std::byte, 113> buffer;

  while (auto count = reader.Read(buffer)) {
    result.append(reinterpret_cast<char*>(buffer.data()), count);
  }
  return result;
}

TEST(ZipReader, StoreDeflateZip64AndDescriptors) {
  std::string input;
  for (size_t i = 0; i < 200000; ++i) {
    input += char(i * 17);
  }

  for (auto method : {kStore, kDeflate}) {
    for (bool zip64 : {false, true}) {
      for (bool descriptor : {false, true}) {
        zip_test::Archive archive{
          {{"dir/data", input, method}}, zip64, descriptor};
        Source source{archive.bytes};

        {
          MemberReader reader{source, "dir/data"};
          std::array<std::byte, 137> buffer;
          std::mt19937 random{12};
          EXPECT_EQ(ReadAll(reader), input);

          for (int i = 0; i < 20; ++i) {
            auto offset = random() % (input.size() - buffer.size());
            reader.ReadAt(offset, buffer);
            EXPECT_EQ(
              std::memcmp(buffer.data(), input.data() + offset, buffer.size()),
              0);
            EXPECT_EQ(reader.Position(), offset + buffer.size());
          }

          reader.Seek(0);
          EXPECT_EQ(ReadAll(reader), input);
          EXPECT_THROW(reader.Seek(input.size() + 1), irs::SqlException);
          EXPECT_THROW(reader.ReadAt(input.size(), buffer), irs::SqlException);
        }
      }
    }
  }
}

TEST(ZipReader, EmptyAndMissingMembers) {
  for (auto method : {kStore, kDeflate}) {
    zip_test::Archive archive{{{"empty", "", method}}};
    Source source{archive.bytes};

    MemberReader reader{source, "empty"};
    EXPECT_EQ(ReadAll(reader), "");
    EXPECT_THROW((MemberReader{source, "missing"}), irs::SqlException);
  }

  zip_test::Archive archive{{}};
  Source source{archive.bytes};

  EXPECT_THROW((MemberReader{source, "missing"}), irs::SqlException);
}

TEST(ZipReader, RejectsDangerousAndAmbiguousNames) {
  for (auto name :
       {"../evil", "/absolute", "C:/file", "a/../../file", "a\\file"}) {
    zip_test::Archive archive{{{"good", "ok"}, {name, "bad"}}};
    Source source{archive.bytes};

    EXPECT_THROW((MemberReader{source, "good"}), irs::SqlException) << name;
  }

  for (auto names : {std::vector<std::string>{"a", "./a"},
                     {"a", "a/b"},
                     {"a/b", "a"},
                     {"dir/", "dir/"}}) {
    zip_test::Archive archive{{{names[0], ""}, {names[1], ""}}};
    Source source{archive.bytes};

    EXPECT_THROW((MemberReader{source, names[0]}), irs::SqlException);
  }

  zip_test::Archive archive{
    {{"good", "ok"}, {"link", "good", kStore, 0x800, 0120777U << 16}}};
  Source source{archive.bytes};

  EXPECT_THROW((MemberReader{source, "good"}), irs::SqlException);
}

TEST(ZipReader, FullNamesNormalizationAndUnsupportedUnselectedMethod) {
  std::string name(600, 'x');
  name += "/café.csv";
  const std::string raw_name = "raw/\xff.csv";
  zip_test::Archive archive{{{name, "ok"},
                             {"other", "opaque", 99},
                             {"folder/", "", kStore},
                             {"folder/x", "x"},
                             {raw_name, "raw"}}};
  Source source{archive.bytes};

  MemberReader reader{source, "./" + name};
  EXPECT_EQ(ReadAll(reader), "ok");
  MemberReader raw_reader{source, raw_name};
  EXPECT_EQ(ReadAll(raw_reader), "raw");
  EXPECT_THROW((MemberReader{source, "other"}), irs::SqlException);
  EXPECT_THROW((MemberReader{source, "folder"}), irs::SqlException);
  EXPECT_THROW((MemberReader{source, "FOLDER/x"}), irs::SqlException);
}

TEST(ZipReader, CorruptionAndTerminalError) {
  for (auto method : {kStore, kDeflate}) {
    zip_test::Archive archive{{{"data", "hello world", method}}};
    zip_test::Patch(archive.bytes, archive.central[0] + 16, 123, 4);
    zip_test::Patch(archive.bytes, 14, 123, 4);
    Source source{archive.bytes};

    MemberReader reader{source, "data"};
    EXPECT_THROW(ReadAll(reader), irs::SqlException);
    EXPECT_THROW(reader.Seek(0), irs::SqlException);
  }
  for (auto offset : {size_t{0}, size_t{6}, size_t{18}}) {
    zip_test::Archive archive{{{"data", "hello world"}}};
    archive.bytes[offset] ^= 1;
    Source source{archive.bytes};

    EXPECT_THROW((MemberReader{source, "data"}), irs::SqlException);
  }
  for (auto delta : {-1, 1}) {
    zip_test::Archive archive{{{"data", "hello world"}}};
    zip_test::Patch(archive.bytes, archive.central[0] + 24, 11 + delta, 4);
    zip_test::Patch(archive.bytes, 22, 11 + delta, 4);
    Source source{archive.bytes};

    MemberReader reader{source, "data"};
    EXPECT_THROW(ReadAll(reader), irs::SqlException);
  }
}

TEST(ZipReader, CancellationDuringSeek) {
  zip_test::Archive archive{{{"data", std::string(1024 * 1024, 'a')}}};
  Source source{archive.bytes};
  MemberReader reader{source, "data"};
  source.cancel_after = source.checks + 4;
  reader.Seek(reader.Size() - 1);
  EXPECT_THROW(ReadAll(reader), std::runtime_error);
}

TEST(ZipReader, BoundedBuffersAndLazyRead) {
  for (auto method : {kStore, kDeflate}) {
    zip_test::Archive archive{
      {{"data", std::string(2 * 1024 * 1024, 'a'), method},
       {"other", std::string(2 * 1024 * 1024, 'b'), method}}};
    Source source{archive.bytes};

    size_t read_at_close;
    {
      MemberReader reader{source, "data"};
      EXPECT_LT(source.read, 70000);
      std::array<std::byte, 1> byte;
      EXPECT_EQ(reader.Read(byte), 1);
      read_at_close = source.read;
    }
    EXPECT_EQ(source.read, read_at_close);
  }
}

TEST(ZipReader, PathSyntax) {
  using sdb::connector::zip::ParsePath;
  EXPECT_EQ(ParsePath("zip://a.zip/dir/a.csv").member, "dir/a.csv");
  EXPECT_EQ(ParsePath("zip:///tmp/a.zip/a.csv").archive, "/tmp/a.zip");
  for (auto path :
       {"a.zip/a.csv", "zip://a.ZIP/a.csv", "zip://a.zip/", "zip://a.zip",
        "zip://a.zip!!a.csv", "zip://s3://b/a.zip/a.csv",
        "zip://https://host/a.zip/a.csv", "zip://zip://a.zip/b.zip/c.csv"}) {
    EXPECT_THROW(ParsePath(path), irs::SqlException) << path;
  }
}

TEST(ZipReader, TruncationAndMutatedHeaders) {
  const zip_test::Archive original{
    {{"data", std::string(1024, 'a')}}, true, true};
  for (size_t length = 0; length < original.bytes.size(); ++length) {
    auto bytes = original.bytes.substr(0, length);
    Source source{bytes};

    EXPECT_THROW((MemberReader{source, "data"}), irs::SqlException);
  }

  std::mt19937 random{43};
  for (int i = 0; i < 500; ++i) {
    auto bytes = original.bytes;
    bytes[random() % bytes.size()] ^= char(1 + random() % 255);
    Source source{bytes};

    try {
      MemberReader reader{source, "data"};
      ReadAll(reader);
    } catch (const irs::SqlException&) {
    }
  }
}

TEST(ZipReader, RejectsTrailingCompressedBytesAndInvalidEmptyStream) {
  for (const auto& data : {std::string{}, std::string{"abc"}}) {
    zip_test::Archive archive{{{"data", data}}};
    const auto compressed_size = archive.central[0] - archive.data[0];
    archive.bytes[archive.data[0]] = char(0xff);
    Source source{archive.bytes};

    MemberReader reader{source, "data"};
    EXPECT_THROW(ReadAll(reader), irs::SqlException);
    EXPECT_GT(compressed_size, 0);
  }
  zip_test::Archive archive{{{"data", "abc"}}};
  auto cd = archive.central[0];
  const auto compressed_size = cd - archive.data[0];
  archive.bytes.insert(cd, 1, '\0');
  ++cd;
  zip_test::Patch(archive.bytes, 18, compressed_size + 1, 4);
  zip_test::Patch(archive.bytes, cd + 20, compressed_size + 1, 4);
  zip_test::Patch(archive.bytes, archive.bytes.size() - 6, cd, 4);
  Source source{archive.bytes};

  MemberReader reader{source, "data"};
  EXPECT_THROW(ReadAll(reader), irs::SqlException);
}

TEST(ZipReader, MetadataCacheThresholdAndDiskOffsets) {
  using sdb::connector::zip::kMetadataCacheLimit;
  for (const auto size : {kMetadataCacheLimit - 1, kMetadataCacheLimit,
                          kMetadataCacheLimit + 1}) {
    std::vector<zip_test::Entry> entries;
    size_t base = 0;
    for (size_t i = 0; i < 801; ++i) {
      entries.push_back({"file-" + std::to_string(i), "ok", kStore});
      base += 46 + entries.back().name.size();
    }

    size_t remaining = size - base;
    for (auto& entry : entries) {
      const auto count = std::min<size_t>(UINT16_MAX, remaining);
      entry.comment.assign(count, 'x');
      remaining -= count;
    }
    ASSERT_EQ(remaining, 0);

    zip_test::Archive archive{entries};
    ASSERT_EQ(archive.bytes.size() - 22 - archive.central.front(), size);

    Source source{archive.bytes};
    {
      MemberReader reader{source, "file-800"};
      if (size <= kMetadataCacheLimit) {
        EXPECT_GE(source.read, size);
      } else {
        EXPECT_LT(source.read, size / 2);
      }
      EXPECT_LE(source.largest_read, 65535 + 22);
      EXPECT_EQ(ReadAll(reader), "ok");
    }

    std::memcpy(archive.bytes.data() + archive.central[799] + 46, "file-800",
                8);
    EXPECT_THROW((MemberReader{source, "file-800"}), irs::SqlException);
  }
}

TEST(ZipReader, ComponentOrderingDetectsNonAdjacentFileConflicts) {
  for (const auto& entries : {
         std::vector<zip_test::Entry>{{"a", ""}, {"a!", ""}, {"a/b", ""}},
         std::vector<zip_test::Entry>{{"a/b", ""}, {"a!", ""}, {"a", ""}},
         std::vector<zip_test::Entry>{{"dir/x", ""}, {"./dir//x", ""}},
       }) {
    zip_test::Archive archive{entries};
    Source source{archive.bytes};
    EXPECT_THROW((MemberReader{source, "a!"}), irs::SqlException);
  }
}

}  // namespace
