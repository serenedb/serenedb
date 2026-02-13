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

#include "iresearch/formats/formats.hpp"
#include "tests_shared.hpp"

TEST(formats_tests, duplicate_register) {
  struct DummyFormat final : public irs::Format {
    static ptr Make() { return ptr(new DummyFormat()); }
    irs::ColumnstoreWriter::ptr get_columnstore_writer(
      bool, irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::ColumnstoreReader::ptr get_columnstore_reader() const final {
      return nullptr;
    }
    irs::FieldWriter::ptr get_field_writer(bool,
                                           irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::FieldReader::ptr get_field_reader(irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::PostingsWriter::ptr get_postings_writer(
      bool, irs::IResourceManager&) const final {
      return nullptr;
    }
    irs::PostingsReader::ptr get_postings_reader() const final {
      return nullptr;
    }
    irs::IndexMetaWriter::ptr get_index_meta_writer() const final {
      return nullptr;
    }
    irs::IndexMetaReader::ptr get_index_meta_reader() const final {
      return nullptr;
    }
    irs::SegmentMetaWriter::ptr get_segment_meta_writer() const final {
      return nullptr;
    }
    irs::SegmentMetaReader::ptr get_segment_meta_reader() const final {
      return nullptr;
    }
    irs::TypeInfo::type_id type() const noexcept final {
      return irs::Type<DummyFormat>::id();
    }
  };

  static bool gInitialExpected = true;

  // check required for tests with repeat (static maps are not cleared between
  // runs)
  if (gInitialExpected) {
    ASSERT_FALSE(irs::formats::Exists(irs::Type<DummyFormat>::name()));
    ASSERT_EQ(nullptr, irs::formats::Get(irs::Type<DummyFormat>::name()));

    irs::FormatRegistrar initial(irs::Type<DummyFormat>::get(),
                                 &DummyFormat::Make);
    ASSERT_EQ(!gInitialExpected, !initial);
  }

  gInitialExpected =
    false;  // next test iteration will not be able to register the same format
  irs::FormatRegistrar duplicate(irs::Type<DummyFormat>::get(),
                                 &DummyFormat::Make);
  ASSERT_TRUE(!duplicate);

  ASSERT_TRUE(irs::formats::Exists(irs::Type<DummyFormat>::name()));
  ASSERT_NE(nullptr, irs::formats::Get(irs::Type<DummyFormat>::name()));
}
