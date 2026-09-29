////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <iresearch/error/error.hpp>
#include <iresearch/formats/format_utils.hpp>
#include <iresearch/formats/index_meta_reader.hpp>
#include <iresearch/formats/index_meta_writer.hpp>
#include <iresearch/formats/segment_meta_writer.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/serialization.hpp>
#include <iresearch/utils/type_limits.hpp>

#include "tests_shared.hpp"

using namespace irs;

TEST(index_meta_tests, memory_directory_read_write_15) {
  irs::MemoryDirectory dir;
  irs::IndexMetaWriter writer{[](uint64_t tick, duckdb::BinarySerializer& out) {
    EXPECT_EQ(42, tick);
    out.WriteProperty<std::string>(0, "payload", "payload");
  }};

  // check that there are no files in a directory
  std::vector<std::string> files;
  auto list_files = [&files](std::string_view name) {
    files.emplace_back(name);
    return true;
  };
  ASSERT_TRUE(dir.visit(list_files));
  ASSERT_TRUE(files.empty());

  // create index metadata and write it into the specified directory
  irs::IndexMeta meta_orig;
  std::string filename;
  std::string tmp_filename;

  ASSERT_TRUE(writer.prepare(dir, meta_orig, tmp_filename, filename, 42));
  ASSERT_EQ("segments_1", filename);
  ASSERT_EQ("pending_segments_1", tmp_filename);

  // we should increase meta generation after we write to directory
  EXPECT_EQ(1, meta_orig.gen);

  // check that files were successfully
  // written to directory
  files.clear();
  ASSERT_TRUE(dir.visit(list_files));
  EXPECT_EQ(1, files.size());
  EXPECT_EQ(files[0], std::string_view("pending_segments_1"));

  writer.commit();

  // create index metadata and read it from the specified  directory
  irs::IndexMeta meta_read;
  std::string payload;
  {
    std::string segments_file;

    const bool index_exists = irs::index_meta::LastFile(dir, segments_file);

    ASSERT_TRUE(index_exists);
    irs::index_meta::Read(
      dir, meta_read, segments_file, [&](duckdb::BinaryDeserializer& in) {
        payload = in.ReadProperty<std::string>(0, "payload");
      });
  }

  EXPECT_EQ(meta_orig, meta_read);
  EXPECT_EQ("payload", payload);
}

TEST(index_meta_tests, invisible_count_round_trip) {
  irs::MemoryDirectory dir;

  auto make_segment = [&](std::string_view name, irs::doc_id_t visible_end) {
    irs::IndexSegment segment;
    segment.meta.name = name;
    segment.meta.version = 1;
    segment.meta.docs_count = 10;
    segment.meta.byte_size = 42;
    segment.meta.visible_end = visible_end;
    segment.meta.docs_mask = std::make_shared<irs::DocumentMask>([] {
      irs::DocumentMask mask;
      mask.Add(irs::doc_limits::min() + 1);
      mask.Trim();
      return mask;
    }());
    segment.meta.live_docs_count =
      segment.meta.docs_count - irs::RemovalCount(segment.meta);
    irs::segment_meta::Write(dir, segment.filename, segment.meta);
    return segment;
  };

  irs::IndexMeta meta_orig;
  meta_orig.segments.emplace_back(
    make_segment("tailed", irs::doc_limits::min() + 7));
  meta_orig.segments.emplace_back(
    make_segment("whole", irs::doc_limits::eof()));

  std::string filename;
  std::string tmp_filename;
  irs::IndexMetaWriter writer;
  ASSERT_TRUE(writer.prepare(dir, meta_orig, tmp_filename, filename, 0));
  ASSERT_TRUE(writer.commit());

  irs::IndexMeta meta_read;
  irs::index_meta::Read(dir, meta_read, filename);
  ASSERT_EQ(2, meta_read.segments.size());

  const auto& tailed = meta_read.segments[0].meta;
  EXPECT_EQ(10, tailed.docs_count);
  EXPECT_EQ(6, tailed.live_docs_count);
  EXPECT_EQ(irs::doc_limits::min() + 7, tailed.visible_end);
  EXPECT_EQ(3, irs::InvisibleCount(tailed));

  const auto& whole = meta_read.segments[1].meta;
  EXPECT_EQ(10, whole.docs_count);
  EXPECT_EQ(9, whole.live_docs_count);
  EXPECT_EQ(irs::doc_limits::eof(), whole.visible_end);
}

TEST(index_meta_tests, ctor) {
  irs::IndexMeta meta;
  EXPECT_EQ(0, meta.seg_counter);
  EXPECT_EQ(0, meta.segments.size());
  EXPECT_EQ(irs::index_gen_limits::invalid(), meta.gen);
}

TEST(index_meta_tests, last_generation) {
  const char prefix[] = "segments_";

  std::vector<std::string> names;
  names.emplace_back("segments_387");
  names.emplace_back("segments_622");
  names.emplace_back("segments_314");
  names.emplace_back("segments_933");
  names.emplace_back("segments_660");
  names.emplace_back("segments_966");
  names.emplace_back("segments_074");
  names.emplace_back("segments_057");
  names.emplace_back("segments_836");
  names.emplace_back("segments_282");
  names.emplace_back("segments_882");
  names.emplace_back("segments_191");
  names.emplace_back("segments_965");
  names.emplace_back("segments_164");
  names.emplace_back("segments_117");

  // get max value
  uint64_t max = 0;
  for (const auto& s : names) {
    uint64_t num = atoi(s.c_str() + sizeof(prefix) - 1);
    if (num > max) {
      max = num;
    }
  }

  // populate directory
  irs::MemoryDirectory dir;
  for (auto& name : names) {
    auto out = dir.create(name);
    ASSERT_FALSE(!out);
  }

  std::string last_seg_file;

  const bool index_exists = irs::index_meta::LastFile(dir, last_seg_file);
  const std::string expected_seg_file = "segments_" + std::to_string(max);

  ASSERT_TRUE(index_exists);
  EXPECT_EQ(expected_seg_file, last_seg_file);
}

TEST(index_meta_tests, rejects_unknown_fields) {
  irs::MemoryDirectory dir;
  const auto name = irs::index_meta::FileName(1);
  {
    auto out = dir.create(name);
    ASSERT_NE(nullptr, out);
    irs::format_utils::WriteFooter(*out, [](duckdb::BinarySerializer& meta) {
      meta.WriteProperty<uint64_t>(
        irs::index_meta::kFieldStorageVersion, "storage_version",
        static_cast<uint64_t>(duckdb::StorageVersion::SERENEDB_LATEST));
      meta.WriteProperty<uint64_t>(irs::index_meta::kFieldSegCounter,
                                   "seg_counter", 0);
      meta.WriteList(irs::index_meta::kFieldSegments, "segments", 0,
                     [](duckdb::BinarySerializer::List&, duckdb::idx_t) {});
      meta.WriteProperty<uint64_t>(irs::index_meta::kFieldPayload + 1,
                                   "from_the_future", 1);
    });
  }

  std::string message;
  try {
    irs::IndexMeta meta;
    irs::index_meta::Read(dir, meta, name);
  } catch (const irs::IndexError& e) {
    message = e.what();
  }
  EXPECT_NE(std::string::npos, message.find("written by a newer release"))
    << message;
}

TEST(index_meta_tests, payload_is_read_whole) {
  irs::MemoryDirectory dir;
  irs::IndexMetaWriter writer{[](uint64_t tick, duckdb::BinarySerializer& out) {
    out.WriteProperty<uint64_t>(0, "tick", tick);
    out.WriteProperty<std::string>(1, "name", "payload");
  }};
  irs::IndexMeta meta;
  std::string pending_filename;
  std::string filename;
  ASSERT_TRUE(writer.prepare(dir, meta, pending_filename, filename, 7));
  ASSERT_TRUE(writer.commit());

  std::string message;
  try {
    irs::IndexMeta read;
    irs::index_meta::Read(dir, read, filename);
  } catch (const irs::IndexError& e) {
    message = e.what();
  }
  EXPECT_NE(std::string::npos,
            message.find("has a payload but no reader for it"))
    << message;

  {
    irs::IndexMeta read;
    ASSERT_THROW(irs::index_meta::Read(dir, read, filename,
                                       [](duckdb::BinaryDeserializer& in) {
                                         in.ReadProperty<uint64_t>(0, "tick");
                                       }),
                 irs::IndexError);
  }

  irs::IndexMeta read;
  uint64_t tick = 0;
  std::string name;
  irs::index_meta::Read(dir, read, filename,
                        [&](duckdb::BinaryDeserializer& in) {
                          tick = in.ReadProperty<uint64_t>(0, "tick");
                          name = in.ReadProperty<std::string>(1, "name");
                        });
  EXPECT_EQ(7, tick);
  EXPECT_EQ("payload", name);
  EXPECT_EQ(meta, read);
}
