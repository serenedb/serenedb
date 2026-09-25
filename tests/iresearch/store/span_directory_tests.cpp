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

#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/span_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/string_utils.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

constexpr irs::field_id kName = 0;
constexpr std::string_view kTerm = "doc";
constexpr irs::doc_id_t kDocs = 8;

struct Image {
  std::vector<std::string> bytes;
  irs::SpanDirectory::Files files;
};

Image ReadImage(irs::Directory& src) {
  Image image;
  std::vector<std::string> names;
  src.visit([&](std::string_view name) {
    names.emplace_back(name);
    return true;
  });
  image.bytes.reserve(names.size());
  for (const auto& name : names) {
    uint64_t length = 0;
    EXPECT_TRUE(src.length(length, name));
    auto in = src.open(name, irs::IOAdvice::NORMAL);
    EXPECT_NE(in, nullptr);
    auto& buffer = image.bytes.emplace_back(length, '\0');
    if (length != 0) {
      in->ReadData(0, reinterpret_cast<irs::byte_type*>(buffer.data()),
                   buffer.size());
    }
    image.files.emplace(
      name,
      irs::bytes_view{reinterpret_cast<const irs::byte_type*>(buffer.data()),
                      buffer.size()});
  }
  return image;
}

void WriteIndex(irs::Directory& dir, const irs::Format::ptr& codec) {
  irs::IndexWriterOptions options;
  options.lock_repository = false;
  options.db = &irs::DuckDBEngine::Instance().instance();
  options.reader_options.db = options.db;
  auto writer = irs::IndexWriter::Make(dir, codec, irs::OpenMode::kOmCreate,
                                       std::move(options));
  ASSERT_NE(writer, nullptr);
  {
    auto trx = writer->GetBatch();
    const duckdb::string_t value{kTerm.data(),
                                 static_cast<uint32_t>(kTerm.size())};
    for (irs::doc_id_t i = 0; i < kDocs; ++i) {
      auto doc = trx.Insert();
      const auto id = doc.DocId();
      ASSERT_TRUE(doc.WithField(
        kName, irs::IndexFeatures::Freq, [&](irs::FieldInverter& fld) {
          return fld.InvertKeywords([&](auto&& emit) { emit(value, id); });
        }));
    }
    trx.Commit();
  }
  writer->RefreshCommit();
}

TEST(SpanDirectory, ServesFilesWithoutCopying) {
  const irs::byte_type payload[] = {'a', 'b', 'c'};
  irs::SpanDirectory::Files files;
  files.emplace("segments_1", irs::bytes_view{payload, sizeof(payload)});
  irs::ResourceManagementOptions resource_manager;
  irs::SpanDirectory dir{std::move(files), resource_manager};

  bool present = false;
  ASSERT_TRUE(dir.exists(present, "segments_1"));
  EXPECT_TRUE(present);
  ASSERT_TRUE(dir.exists(present, "absent"));
  EXPECT_FALSE(present);

  uint64_t length = 0;
  ASSERT_TRUE(dir.length(length, "segments_1"));
  EXPECT_EQ(length, sizeof(payload));
  EXPECT_FALSE(dir.length(length, "absent"));

  auto in = dir.open("segments_1", irs::IOAdvice::NORMAL);
  ASSERT_NE(in, nullptr);
  EXPECT_EQ(in->Length(), sizeof(payload));
  EXPECT_EQ(in->ReadStable(sizeof(payload)), payload);
  EXPECT_EQ(dir.open("absent", irs::IOAdvice::NORMAL), nullptr);

  size_t visited = 0;
  EXPECT_TRUE(dir.visit([&](std::string_view) {
    ++visited;
    return true;
  }));
  EXPECT_EQ(visited, 1U);
}

TEST(SpanDirectory, RefusesTheWriteSurface) {
  irs::ResourceManagementOptions resource_manager;
  irs::SpanDirectory dir{irs::SpanDirectory::Files{}, resource_manager};

  EXPECT_EQ(dir.create("f"), nullptr);
  EXPECT_EQ(dir.make_lock("f"), nullptr);
  std::time_t mtime = 0;
  EXPECT_FALSE(dir.mtime(mtime, "f"));
  EXPECT_FALSE(dir.remove("f"));
  EXPECT_FALSE(dir.rename("a", "b"));
}

TEST(SpanDirectory, DirectoryReaderOpensAnIndexServedFromSpans) {
  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(codec, nullptr);

  irs::MemoryDirectory source;
  ASSERT_NO_FATAL_FAILURE(WriteIndex(source, codec));

  auto image = ReadImage(source);
  ASSERT_FALSE(image.files.empty());

  irs::ResourceManagementOptions resource_manager;
  irs::SpanDirectory dir{std::move(image.files), resource_manager};

  irs::IndexReaderOptions reader_options;
  reader_options.db = &irs::DuckDBEngine::Instance().instance();
  irs::DirectoryReader reader{dir, codec, reader_options};

  ASSERT_TRUE(static_cast<bool>(reader));
  ASSERT_EQ(reader.size(), 1U);
  EXPECT_EQ(reader.live_docs_count(), kDocs);

  const auto* field = reader[0].field(kName);
  ASSERT_NE(field, nullptr);
  EXPECT_EQ(field->Lookup(irs::ViewCast<irs::byte_type>(kTerm)).docs_count,
            kDocs);
}

}  // namespace
