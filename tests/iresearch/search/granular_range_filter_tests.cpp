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

#include <iresearch/search/filters/granular_range_filter.hpp>
#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <random>

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "tests_shared.hpp"

namespace {

// Stable per-name field ids, sourced from `tests::FieldIdFor` so the
// canonical JSON factories and these tests agree on the id-per-name.
[[maybe_unused]] inline constexpr irs::field_id kKey =
  tests::FieldIdFor("_key");
[[maybe_unused]] inline constexpr irs::field_id kSeq = tests::FieldIdFor("seq");
[[maybe_unused]] inline constexpr irs::field_id kName =
  tests::FieldIdFor("name");
[[maybe_unused]] inline constexpr irs::field_id kValue =
  tests::FieldIdFor("value");
[[maybe_unused]] inline constexpr irs::field_id kField =
  tests::FieldIdFor("field");
[[maybe_unused]] inline constexpr irs::field_id kField1 =
  tests::FieldIdFor("field1");
[[maybe_unused]] inline constexpr irs::field_id kA = tests::FieldIdFor("a");
[[maybe_unused]] inline constexpr irs::field_id kSame =
  tests::FieldIdFor("same");
[[maybe_unused]] inline constexpr irs::field_id kDuplicated =
  tests::FieldIdFor("duplicated");
[[maybe_unused]] inline constexpr irs::field_id kPrefix =
  tests::FieldIdFor("prefix");
[[maybe_unused]] inline constexpr irs::field_id kInvalidField =
  irs::field_limits::invalid();
[[maybe_unused]] inline constexpr irs::field_id kInvalidName =
  irs::field_limits::invalid();

inline irs::field_id FieldIdForName(std::string_view name) {
  return tests::FieldIdFor(name);
}

void SetGranularTerm(irs::bstring& boundary, irs::bytes_view value) {
  boundary.assign(value);
}

class GranularFloatField : public tests::FloatField {};

class GranularDoubleField : public tests::DoubleField {};

class GranularIntField : public tests::IntField {};

class GranularLongField : public tests::LongField {};

class GranularRangeFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  static void ByRangeJsonFieldFactory(
    tests::Document& doc, const std::string& name,
    const tests::JsonDocGenerator::JsonValue& data) {
    const irs::field_id fid = FieldIdForName(name);
    if (data.is_string()) {
      auto sf = std::make_shared<tests::StringField>(name, data.str);
      sf->id = fid;
      doc.insert(std::move(sf));
    } else if (data.is_null()) {
      doc.insert(std::make_shared<tests::BinaryField>());
      auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
      field.Name(name);
      field.id = fid;
      field.value(irs::ViewCast<irs::byte_type>(irs::kNullTerm));
    } else if (data.is_bool() && data.b) {
      doc.insert(std::make_shared<tests::BinaryField>());
      auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
      field.Name(name);
      field.id = fid;
      field.value(irs::ViewCast<irs::byte_type>(irs::kTrueTerm));
    } else if (data.is_bool() && !data.b) {
      doc.insert(std::make_shared<tests::BinaryField>());
      auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
      field.Name(name);
      field.id = fid;
      field.value(irs::ViewCast<irs::byte_type>(irs::kTrueTerm));
    } else if (data.is_number()) {
      // 'value' can be interpreted as a double
      {
        doc.insert(std::make_shared<GranularDoubleField>());
        const auto d_value = data.as_number<double_t>();
        auto& field = (doc.indexed.end() - 1).as<tests::DoubleField>();
        field.Name(name);
        field.id = fid;
        field.value(d_value);
      }

      // 'value' can be interpreted as a float
      {
        doc.insert(std::make_shared<GranularFloatField>());
        auto f_value = data.as_number<float_t>();
        auto& field = (doc.indexed.end() - 1).as<tests::FloatField>();
        field.Name(name);
        field.id = fid;
        field.value(f_value);
      }

      // 'value' can be interpreted as int64
      {
        doc.insert(std::make_shared<GranularLongField>());
        const auto li_value = data.as_number<int64_t>();
        auto& field = (doc.indexed.end() - 1).as<tests::LongField>();
        field.Name(name);
        field.id = fid;
        field.value(li_value);
      }

      // 'value' can be interpreted as int32
      {
        doc.insert(std::make_shared<GranularIntField>());
        auto l_value = data.as_number<int32_t>();
        auto& field = (doc.indexed.end() - 1).as<tests::IntField>();
        field.Name(name);
        field.id = fid;
        field.value(l_value);
      }
    }
  }

  void ByRangeGranularityBoost() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("granular_sequential.json"),
                                  &ByRangeJsonFieldFactory);

      add_segment(gen, irs::kOmCreate);
    }

    auto rdr = open_reader();
    ASSERT_EQ(1, rdr->size());

    auto& segment = (*rdr)[0];

    MaxMemoryCounter counter;

    // without boost
    {
      irs::ByGranularRange q;
      *q.mutable_field_id() = kSeq;
      SetGranularTerm(q.mutable_options()->range.min,
                      irs::ViewCast<irs::byte_type>(std::string_view("A")));
      SetGranularTerm(q.mutable_options()->range.max,
                      irs::ViewCast<irs::byte_type>(std::string_view("M")));
      q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      q.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{q, irs::SubReader::empty(), nullptr,
                                     counter};
      ASSERT_TRUE(irs::QueryBuilder::IsEmpty(*prepared.Query(0)));
      ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    counter.Reset();

    // with boost
    {
      irs::score_t boost = 1.5f;
      irs::ByGranularRange q;
      *q.mutable_field_id() = kName;
      SetGranularTerm(q.mutable_options()->range.min,
                      irs::ViewCast<irs::byte_type>(std::string_view("A")));
      SetGranularTerm(q.mutable_options()->range.max,
                      irs::ViewCast<irs::byte_type>(std::string_view("M")));
      q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      q.SetBoost(boost);

      tests::PreparedFilter prepared{q, segment};
      ASSERT_EQ(boost, prepared.Query(0)->Boost());
    }
  }

  void ByRangeGranularityLevel() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("granular_sequential.json"),
                                  &ByRangeJsonFieldFactory);

      add_segment(gen, irs::kOmCreate);
    }

    auto rdr = open_reader();

    // range under same granularity value for topmost element, (i.e. last value
    // from numeric_token_stream)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  INT32_C(0));
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  INT32_C(1000));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // range under different granularity value for topmost element, (i.e. last
    // value from numeric_token_stream)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  INT32_C(-1000));
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  INT32_C(+1000));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = [-20000..+20000]
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  double_t(-20000));
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  double_t(+20000));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6, 7, 10, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value > 100
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  double_t(100));
      SetGranularTerm(query.mutable_options()->range.max,
                      irs::numeric_utils::numeric_traits<double_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value => 100
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  double_t(100));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value => 20007 (largest value)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  double_t(20007));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value < 10000.123
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min,
                      irs::numeric_utils::numeric_traits<double_t>::ninf());
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  double_t(10000.123));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 9, 10, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value <= 10000.123
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  double_t(10000.123));
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 9, 10, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // all documents
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4,  5,  6,
                                          7, 8, 9, 10, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }
  }

  void ByRangeSequentialNumeric() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &ByRangeJsonFieldFactory);

      add_segment(gen, irs::kOmCreate);
    }

    auto rdr = open_reader();

    // long - seq = [7..7]
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT64_C(7));

      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, INT64_C(7));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq = [1..7]
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT64_C(1));

      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, INT64_C(7));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  INT64_C(31));
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  INT64_C(32));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq > 28
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT64_C(28));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      (irs::numeric_utils::numeric_traits<int64_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq >= 31 (match largest value)
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT64_C(31));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      (irs::numeric_utils::numeric_traits<int64_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq <= 5
    {
      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, INT64_C(5));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min,
                      (irs::numeric_utils::numeric_traits<int64_t>::min)());
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq = [7..7]
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT32_C(7));

      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, INT32_C(7));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq = [1..7]
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT32_C(1));

      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, INT32_C(7));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  INT32_C(31));
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  INT32_C(32));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq > 28
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT32_C(28));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      (irs::numeric_utils::numeric_traits<int32_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq >= 31 (match largest value)
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, INT32_C(31));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      (irs::numeric_utils::numeric_traits<int32_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq <= 5
    {
      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, INT32_C(5));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min,
                      (irs::numeric_utils::numeric_traits<int32_t>::min)());
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value = [123..123]
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, (float_t)123.f);

      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, (float_t)123.f);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value = [91.524..123)
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, (float_t)91.524f);

      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, (float_t)123.f);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 10, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  float_t(31));
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  float_t(32));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value < 91.565
    {
      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, (float_t)90.565f);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min,
                      irs::numeric_utils::numeric_traits<float_t>::ninf());
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{4, 11, 13, 14, 15, 16, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value > 91.565
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, (float_t)90.565f);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      irs::numeric_utils::numeric_traits<float_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 5, 6, 7, 8, 9, 10, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value >= 31 (largest value)
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, float_t(31));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      irs::numeric_utils::numeric_traits<float_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = [123...123]
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, (double_t)123.);
      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, (double_t)123.);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = (-40; 90.564]
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, (double_t)-40.);
      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, (double_t)90.564);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{4, 11, 13, 14, 15, 16, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularNumericTerm(query.mutable_options()->range.min,
                                  double_t(31));
      irs::SetGranularNumericTerm(query.mutable_options()->range.max,
                                  double_t(32));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value < 5;
    {
      irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto max_term =
        irs::numeric_utils::EncodeNumericTerm(max_stream_buf, (double_t)5.);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min,
                      irs::numeric_utils::numeric_traits<double_t>::ninf());
      SetGranularTerm(query.mutable_options()->range.max, max_term);
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{14, 15, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value > 90.543;
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, (double_t)90.543);

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      irs::numeric_utils::numeric_traits<double_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 13};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value >= 31 (largest value)
    {
      irs::byte_type min_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
      const auto min_term =
        irs::numeric_utils::EncodeNumericTerm(min_stream_buf, double_t(31));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      SetGranularTerm(query.mutable_options()->range.min, min_term);
      SetGranularTerm(query.mutable_options()->range.max,
                      irs::numeric_utils::numeric_traits<double_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->Next());) {
          actual.push_back(docs->Value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }
  }

  void ByRangeRandomizedOracle() {
    constexpr irs::field_id kPk = tests::FieldIdFor("rnd_pk");
    constexpr irs::field_id kFields[]{
      tests::FieldIdFor("rnd_i32"),
      tests::FieldIdFor("rnd_i64"),
      tests::FieldIdFor("rnd_f32"),
      tests::FieldIdFor("rnd_f64"),
    };
    constexpr size_t kRows = 24000;
    constexpr size_t kSegments = 4;
    constexpr auto kInf = std::numeric_limits<double_t>::infinity();
    constexpr auto kNan = std::numeric_limits<double_t>::quiet_NaN();
    std::mt19937_64 rng{7};
    const auto value = [&](size_t field) -> double_t {
      const bool special = rng() % 10 == 0;
      switch (field) {
        case 0: {
          constexpr double_t kSpecial[]{std::numeric_limits<int32_t>::min(),
                                        std::numeric_limits<int32_t>::max(), -1,
                                        0, 1};
          return special ? kSpecial[rng() % std::size(kSpecial)]
                         : static_cast<double_t>(rng() % 2001) - 1000;
        }
        case 1:
          return rng() % 100 < 85
                   ? 0
                   : static_cast<double_t>(rng() % 2000000000001) -
                       1000000000000;
        case 2: {
          constexpr double_t kSpecial[]{
            -kInf,
            kInf,
            kNan,
            -0.0,
            0.0,
            std::numeric_limits<float_t>::min(),
            std::numeric_limits<float_t>::lowest(),
            std::numeric_limits<float_t>::max(),
          };
          return special ? kSpecial[rng() % std::size(kSpecial)]
                         : (static_cast<double_t>(rng() % 801) - 400) / 4;
        }
        default: {
          constexpr double_t kSpecial[]{
            -kInf,
            kInf,
            kNan,
            -0.0,
            0.0,
            std::numeric_limits<double_t>::min(),
            std::numeric_limits<double_t>::lowest(),
            std::numeric_limits<double_t>::max(),
          };
          return special
                   ? kSpecial[rng() % std::size(kSpecial)]
                   : (static_cast<double_t>(rng() % 4000001) - 2000000) / 2;
        }
      }
    };
    const auto encode = [](size_t field, double_t value) {
      irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
      switch (field) {
        case 0:
          return irs::bstring{irs::numeric_utils::EncodeNumericTerm(
            buf, static_cast<int32_t>(value))};
        case 1:
          return irs::bstring{irs::numeric_utils::EncodeNumericTerm(
            buf, static_cast<int64_t>(value))};
        case 2:
          return irs::bstring{irs::numeric_utils::EncodeNumericTerm(
            buf, static_cast<float_t>(value))};
        default:
          return irs::bstring{
            irs::numeric_utils::EncodeNumericTerm(buf, value)};
      }
    };

    std::vector<std::array<std::vector<double_t>, 4>> rows(kRows);
    for (auto& row : rows) {
      for (size_t field = 0; field != 4; ++field) {
        for (size_t n = field == 1 ? 1 + rng() % 2 : rng() % 3; n != 0; --n) {
          row[field].push_back(value(field));
        }
      }
    }

    auto writer = open_writer(irs::kOmCreate);
    for (size_t segment = 0; segment != kSegments; ++segment) {
      for (size_t pk = segment * kRows / kSegments;
           pk != (segment + 1) * kRows / kSegments; ++pk) {
        tests::Document doc;
        auto key = std::make_shared<tests::LongField>();
        key->Name("rnd_pk");
        key->id = kPk;
        key->value(static_cast<int64_t>(pk));
        doc.insert(key, true, false);
        const auto add = [&]<typename F>(size_t field, auto v) {
          auto numeric = std::make_shared<F>();
          numeric->Name("rnd");
          numeric->id = kFields[field];
          numeric->value(static_cast<typename F::value_t>(v));
          doc.insert(numeric, true, false);
        };
        for (const auto v : rows[pk][0]) {
          add.template operator()<tests::IntField>(0, v);
        }
        for (const auto v : rows[pk][1]) {
          add.template operator()<tests::LongField>(1, v);
        }
        for (const auto v : rows[pk][2]) {
          add.template operator()<tests::FloatField>(2, v);
        }
        for (const auto v : rows[pk][3]) {
          add.template operator()<tests::DoubleField>(3, v);
        }
        ASSERT_TRUE(Insert(*writer, doc.indexed.begin(), doc.indexed.end()));
      }
      writer->RefreshCommit();
    }

    std::vector<bool> live(kRows, true);
    const auto check = [&](uint64_t seed) {
      auto reader = open_reader();
      std::vector<std::vector<int64_t>> pks;
      for (const auto& segment : reader) {
        auto& map =
          pks.emplace_back(segment.docs_count() + irs::doc_limits::min(), -1);
        for (const auto id : kFields) {
          const auto* numeric = segment.field(id);
          ASSERT_NE(nullptr, numeric);
          ASSERT_TRUE(numeric->meta().ranges);
        }
        const auto* terms = segment.field(kPk);
        ASSERT_NE(nullptr, terms);
        auto it = terms->iterator();
        while (it->next()) {
          const auto pk = irs::numeric_utils::numeric_traits<int64_t>::decode(
            it->value().data());
          terms->ReadDocs(it->value(), [&](irs::doc_id_t doc) {
            map[doc] = pk;
            return true;
          });
        }
      }
      std::mt19937_64 gen{seed};
      for (size_t round = 0; round != 300; ++round) {
        SCOPED_TRACE(testing::Message("round ") << round);
        const size_t field = gen() % 4;
        const auto bound = [&] {
          const auto& values = rows[gen() % kRows][field];
          return encode(field, values.empty() || gen() % 4 == 0
                                 ? value(field)
                                 : values[gen() % values.size()]);
        };
        irs::ByGranularRange query;
        *query.mutable_field_id() = kFields[field];
        auto& range = query.mutable_options()->range;
        range.min_type = static_cast<irs::BoundType>(gen() % 3);
        range.max_type = static_cast<irs::BoundType>(gen() % 3);
        if (range.min_type != irs::BoundType::Unbounded) {
          range.min = bound();
        }
        if (range.max_type != irs::BoundType::Unbounded) {
          range.max = bound();
        }
        const auto matches = [&](irs::bytes_view term) {
          switch (range.min_type) {
            case irs::BoundType::Inclusive:
              if (term < irs::bytes_view{range.min}) {
                return false;
              }
              break;
            case irs::BoundType::Exclusive:
              if (term <= irs::bytes_view{range.min}) {
                return false;
              }
              break;
            case irs::BoundType::Unbounded:
              break;
          }
          switch (range.max_type) {
            case irs::BoundType::Inclusive:
              return term <= irs::bytes_view{range.max};
            case irs::BoundType::Exclusive:
              return term < irs::bytes_view{range.max};
            case irs::BoundType::Unbounded:
              return true;
          }
          return true;
        };
        std::vector<int64_t> expected;
        for (size_t pk = 0; pk != kRows; ++pk) {
          if (live[pk] && absl::c_any_of(rows[pk][field], [&](double_t v) {
                const auto term = encode(field, v);
                return matches(term);
              })) {
            expected.push_back(static_cast<int64_t>(pk));
          }
        }
        tests::PreparedFilter prepared{query, reader};
        std::vector<int64_t> actual;
        for (size_t i = 0; i != reader.size(); ++i) {
          const auto mask = reader[i].MaskedDocs();
          auto docs = prepared.Execute(i);
          while (!irs::doc_limits::eof(docs->Next())) {
            if (!mask.Contains(docs->Value())) {
              actual.push_back(pks[i][docs->Value()]);
            }
          }
        }
        absl::c_sort(actual);
        ASSERT_EQ(expected, actual);
      }
    };

    check(1);
    {
      auto trx = writer->GetBatch();
      auto removal = std::make_unique<irs::ByGranularRange>();
      *removal->mutable_field_id() = kPk;
      irs::SetGranularNumericTerm(removal->mutable_options()->range.min,
                                  int64_t{3000});
      irs::SetGranularNumericTerm(removal->mutable_options()->range.max,
                                  int64_t{4000});
      removal->mutable_options()->range.min_type = irs::BoundType::Inclusive;
      removal->mutable_options()->range.max_type = irs::BoundType::Exclusive;
      trx.Remove(irs::Filter::ptr{std::move(removal)});
      trx.Commit();
    }
    writer->RefreshCommit();
    for (size_t pk = 3000; pk != 4000; ++pk) {
      live[pk] = false;
    }
    check(2);
    ASSERT_TRUE(writer->Compact(
      irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
    writer->RefreshCommit();
    ASSERT_EQ(1, open_reader().size());
    check(2);
  }
};

TEST(by_granular_range_test, options) {
  irs::ByGranularRangeOptions opts;
  ASSERT_TRUE(opts.range.min.empty());
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.min_type);
  ASSERT_TRUE(opts.range.max.empty());
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.max_type);
}

TEST(by_granular_range_test, ctor) {
  irs::ByGranularRange q;
  ASSERT_EQ(irs::Type<irs::ByGranularRange>::id(), q.type());
  ASSERT_EQ(irs::ByGranularRangeOptions{}, q.options());
  ASSERT_EQ(irs::kNoBoost, q.GetBoost());
}

TEST(by_granular_range_test, equal) {
  irs::ByGranularRange q0;
  *q0.mutable_field_id() = kField;
  SetGranularTerm(q0.mutable_options()->range.min,
                  irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  SetGranularTerm(q0.mutable_options()->range.max,
                  irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q0.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q0.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  irs::ByGranularRange q1;
  *q1.mutable_field_id() = kField;
  SetGranularTerm(q1.mutable_options()->range.min,
                  irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  SetGranularTerm(q1.mutable_options()->range.max,
                  irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_EQ(q0, q1);

  irs::ByGranularRange q2;
  *q2.mutable_field_id() = kField1;
  SetGranularTerm(q2.mutable_options()->range.min,
                  irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  SetGranularTerm(q2.mutable_options()->range.max,
                  irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q2.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q2.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q2);

  irs::ByGranularRange q3;
  *q3.mutable_field_id() = kField;
  SetGranularTerm(q3.mutable_options()->range.min,
                  irs::ViewCast<irs::byte_type>(std::string_view("min_term1")));
  SetGranularTerm(q3.mutable_options()->range.max,
                  irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q3.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q3.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q3);

  irs::ByGranularRange q4;
  *q4.mutable_field_id() = kField;
  SetGranularTerm(q4.mutable_options()->range.min,
                  irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  SetGranularTerm(q4.mutable_options()->range.max,
                  irs::ViewCast<irs::byte_type>(std::string_view("max_term1")));
  q4.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q4.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q4);

  irs::ByGranularRange q5;
  *q5.mutable_field_id() = kField;
  SetGranularTerm(q5.mutable_options()->range.min,
                  irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  SetGranularTerm(q5.mutable_options()->range.max,
                  irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q5.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  q5.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q5);

  irs::ByGranularRange q6;
  *q6.mutable_field_id() = kField;
  SetGranularTerm(q6.mutable_options()->range.min,
                  irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  SetGranularTerm(q6.mutable_options()->range.max,
                  irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q6.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q6.mutable_options()->range.max_type = irs::BoundType::Exclusive;

  ASSERT_NE(q0, q6);

  ASSERT_NE(q0, q6);
}

TEST(by_granular_range_test, boost) {
  // no boost
  {
    irs::ByGranularRange q;
    *q.mutable_field_id() = kField;
    SetGranularTerm(
      q.mutable_options()->range.min,
      irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
    SetGranularTerm(
      q.mutable_options()->range.max,
      irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_TRUE(irs::QueryBuilder::IsEmpty(*prepared.Query(0)));
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  // a boost on a query that matches nothing is not observable
  {
    irs::score_t boost = 1.5f;
    irs::ByGranularRange q;
    *q.mutable_field_id() = kField;
    SetGranularTerm(
      q.mutable_options()->range.min,
      irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
    SetGranularTerm(
      q.mutable_options()->range.max,
      irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    q.SetBoost(boost);

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_TRUE(irs::QueryBuilder::IsEmpty(*prepared.Query(0)));
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }
}

TEST_P(GranularRangeFilterTestCase, by_range_randomized_oracle) {
  ByRangeRandomizedOracle();
}

TEST_P(GranularRangeFilterTestCase, by_range_granularity) {
  ByRangeGranularityLevel();
}

TEST_P(GranularRangeFilterTestCase, by_range_granularity_boost) {
  ByRangeGranularityBoost();
}

TEST_P(GranularRangeFilterTestCase, by_range_numeric) {
  ByRangeSequentialNumeric();
}

TEST_P(GranularRangeFilterTestCase, by_range_order) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &ByRangeJsonFieldFactory);

    add_segment(gen, irs::kOmCreate);
  }

  auto rdr = open_reader();

  // empty query
  CheckQuery(irs::ByGranularRange(), Docs{}, rdr);

  // name = (..;..) test collector call count for field/term/finish
  {
    Docs docs{};
    Costs costs{docs.size()};

    size_t finish_count = 0;
    size_t field_docs = 0;

    std::array<irs::Scorer::ptr, 1> scorers{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*scorers.front());

    scorer.collectors_collect = [&finish_count, &field_docs](
                                  irs::byte_type*,
                                  const irs::FieldCollector* field,
                                  const irs::TermCollector*) -> void {
      ++finish_count;
      if (field) {
        field_docs += field->docs_with_field;
      }
    };

    irs::ByGranularRange q;
    *q.mutable_field_id() = kInvalidField;
    SetGranularTerm(q.mutable_options()->range.min,
                    irs::numeric_utils::numeric_traits<double_t>::ninf());
    SetGranularTerm(q.mutable_options()->range.max,
                    irs::numeric_utils::numeric_traits<double_t>::inf());
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    q.SetScorer(scorers.front().get());
    CheckQuery(*tests::Optimized(q, scorers.front().get()), scorers, docs, rdr,
               false);
    ASSERT_EQ(0, field_docs);
    ASSERT_EQ(0, finish_count);
  }

  // value = (..;..) test collector call count for field/term/finish
  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
    Costs costs{docs.size()};

    size_t finish_count = 0;
    uint64_t finish_docs_with_field = 0;
    uint64_t finish_docs_with_term = 0;

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

    scorer.collectors_collect = [&](irs::byte_type*,
                                    const irs::FieldCollector* field,
                                    const irs::TermCollector* term) -> void {
      ++finish_count;
      ASSERT_NE(nullptr, field);
      ASSERT_NE(nullptr, term);
      finish_docs_with_field += field->docs_with_field;
      finish_docs_with_term += term->docs_with_term;
    };

    irs::ByGranularRange q;
    *q.mutable_field_id() = kValue;
    SetGranularTerm(q.mutable_options()->range.min,
                    irs::numeric_utils::numeric_traits<double_t>::ninf());
    SetGranularTerm(q.mutable_options()->range.max,
                    irs::numeric_utils::numeric_traits<double_t>::inf());
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    q.SetScorer(order.front().get());
    CheckQuery(tests::FilterWrapper{q}, order, docs, rdr);
    ASSERT_EQ(11, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
  }

  // value = (..;..)
  {
    Docs docs{1, 5, 7, 9, 10, 3, 4, 8, 11, 2, 6, 12, 13, 14, 15, 16, 17};
    Costs costs{docs.size()};
    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    irs::ByGranularRange q;
    *q.mutable_field_id() = kValue;
    SetGranularTerm(q.mutable_options()->range.min,
                    irs::numeric_utils::numeric_traits<double_t>::ninf());
    SetGranularTerm(q.mutable_options()->range.max,
                    irs::numeric_utils::numeric_traits<double_t>::inf());
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    q.SetScorer(order.front().get());
    CheckQuery(*tests::Optimized(q, order.front().get()), order, docs, rdr);
  }

  // value = (..;100)
  {
    Docs docs{4, 11, 12, 13, 14, 15, 16, 17};
    Costs costs{docs.size()};
    irs::byte_type max_stream_buf[irs::numeric_utils::kNumericTermMaxSize];
    const auto max_term =
      irs::numeric_utils::EncodeNumericTerm(max_stream_buf, (double_t)100.);

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    irs::ByGranularRange q;
    *q.mutable_field_id() = kValue;
    SetGranularTerm(q.mutable_options()->range.min,
                    irs::numeric_utils::numeric_traits<double_t>::ninf());
    SetGranularTerm(q.mutable_options()->range.max, max_term);
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    q.SetScorer(order.front().get());
    CheckQuery(*tests::Optimized(q, order.front().get()), order, docs, rdr);
  }
}

TEST_P(GranularRangeFilterTestCase, by_range_order_multiple_sorts) {
  {
    auto writer = open_writer(irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    // add segment
    index().emplace_back();
    auto& segment = index().back();

    {
      const auto* data =
        "[                                 \
           { \"seq\": -6, \"value\": 2  }, \
           { \"seq\": -5, \"value\": 1  }, \
           { \"seq\": -4, \"value\": 1  }, \
           { \"seq\": -3, \"value\": 3  }, \
           { \"seq\": -2, \"value\": 1  }, \
           { \"seq\": -1, \"value\": 56 }  \
         ]";

      tests::JsonDocGenerator gen(data, &ByRangeJsonFieldFactory);
      write_segment(*writer, segment, gen);
    }

    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &ByRangeJsonFieldFactory);

      write_segment(*writer, segment, gen);
    }
    writer->RefreshCommit();
    AssertSnapshotEquality(*writer);
  }

  auto rdr = open_reader();

  // value = [...;...)
  const int seed = -6;
  for (int begin = seed, end = begin + int(rdr->docs_count()); begin != end;
       ++begin) {
    Docs docs;
    docs.resize(size_t(end - begin));
    std::iota(docs.begin(), docs.end(),
              size_t(begin - seed + irs::doc_limits::min()));
    Costs costs{docs.size()};

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    irs::ByGranularRange q;
    *q.mutable_field_id() = kSeq;
    irs::SetGranularNumericTerm(q.mutable_options()->range.min,
                                (double_t)begin);
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;

    q.SetScorer(order.front().get());
    CheckQuery(*tests::Optimized(q, order.front().get()), order, docs, rdr);
  }
}

TEST_P(GranularRangeFilterTestCase, by_range_numeric_sequence) {
  // add segment
  tests::JsonDocGenerator gen(
    resource("numeric_sequence.json"),
    [](tests::Document& doc, const std::string& name,
       const tests::JsonDocGenerator::JsonValue& data) {
      const irs::field_id fid = FieldIdForName(name);
      if (data.is_string()) {
        auto sf = std::make_shared<tests::StringField>(name, data.str);
        sf->id = fid;
        doc.insert(std::move(sf));
      } else if (data.is_null()) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.id = fid;
        field.value(irs::ViewCast<irs::byte_type>(irs::kNullTerm));
      } else if (data.is_bool() && data.b) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.id = fid;
        field.value(irs::ViewCast<irs::byte_type>(irs::kTrueTerm));
      } else if (data.is_bool() && !data.b) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.id = fid;
        field.value(irs::ViewCast<irs::byte_type>(irs::kTrueTerm));
      } else if (data.is_number()) {
        // 'value' can be interpreted as a double
        const auto d_value = data.as_number<double_t>();
        {
          doc.insert(std::make_shared<GranularDoubleField>());
          auto& field = (doc.indexed.end() - 1).as<tests::DoubleField>();
          field.Name(name);
          field.id = fid;
          field.value(d_value);
        }
      }
    });

  auto store_key = [](irs::IndexWriter::Document& doc,
                      const tests::Document& src) {
    const auto* key =
      dynamic_cast<const tests::StringField*>(src.indexed.get_by_id(kKey));
    ASSERT_NE(nullptr, key);
    auto* cs = doc.GetColWriter();
    ASSERT_NE(nullptr, cs);
    irs::tests::StoreFieldAt(*cs, kKey, doc.DocId(), *key);
  };
  add_segment(gen, irs::kOmCreate, irs::tests::DefaultWriterOptions(),
              store_key);

  auto reader = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, reader->size());
  auto& segment = reader[0];

  // a > -inf && a < 30.
  {
    std::set<std::string> expected;

    // fill expected values
    {
      gen.reset();
      while (auto* doc = gen.next()) {
        auto* numeric_field =
          dynamic_cast<GranularDoubleField*>(doc->indexed.get("a"));
        ASSERT_NE(nullptr, numeric_field);

        if (numeric_field->value() < 30.) {
          auto* key_field =
            dynamic_cast<tests::StringField*>(doc->indexed.get("_key"));
          ASSERT_NE(nullptr, key_field);

          expected.emplace(std::string(key_field->value()));
        }
      }
    }

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    SetGranularTerm(query.mutable_options()->range.min,
                    irs::numeric_utils::numeric_traits<double_t>::ninf());
    irs::SetGranularNumericTerm(query.mutable_options()->range.max, 30.);
    query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{query, reader};
    ASSERT_NE(nullptr, prepared.Query(0));
    const auto* column = segment.Column(kKey);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    std::set<std::string> actual;

    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->Next())) {
      const auto doc = docs->Value();
      const auto bytes = values.Get(doc);
      irs::BytesViewInput in;
      in.reset(bytes);
      actual.emplace(irs::ReadString<std::string>(in));
    }
    ASSERT_EQ(expected, actual);
  }

  // a < 30.
  {
    std::set<std::string> expected;

    // fill expected values
    {
      gen.reset();
      while (auto* doc = gen.next()) {
        auto* numeric_field =
          dynamic_cast<GranularDoubleField*>(doc->indexed.get("a"));
        ASSERT_NE(nullptr, numeric_field);

        if (numeric_field->value() < 30.) {
          auto* key_field =
            dynamic_cast<tests::StringField*>(doc->indexed.get("_key"));
          ASSERT_NE(nullptr, key_field);

          expected.emplace(std::string(key_field->value()));
        }
      }
    }

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    irs::SetGranularNumericTerm(query.mutable_options()->range.max, 30.);
    query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{query, reader};
    ASSERT_NE(nullptr, prepared.Query(0));
    const auto* column = segment.Column(kKey);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    std::set<std::string> actual;

    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->Next())) {
      const auto doc = docs->Value();
      const auto bytes = values.Get(doc);
      irs::BytesViewInput in;
      in.reset(bytes);
      actual.emplace(irs::ReadString<std::string>(in));
    }
    ASSERT_EQ(expected, actual);
  }

  // a > 30. && a < inf
  {
    std::set<std::string> expected;

    // fill expected values
    {
      gen.reset();
      while (auto* doc = gen.next()) {
        auto* numeric_field =
          dynamic_cast<GranularDoubleField*>(doc->indexed.get("a"));
        ASSERT_NE(nullptr, numeric_field);

        if (numeric_field->value() > 30.) {
          auto* key_field =
            dynamic_cast<tests::StringField*>(doc->indexed.get("_key"));
          ASSERT_NE(nullptr, key_field);

          expected.emplace(std::string(key_field->value()));
        }
      }
    }

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    irs::SetGranularNumericTerm(query.mutable_options()->range.min, 30.);
    SetGranularTerm(query.mutable_options()->range.max,
                    irs::numeric_utils::numeric_traits<double_t>::inf());
    query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{query, reader};
    ASSERT_NE(nullptr, prepared.Query(0));
    const auto* column = segment.Column(kKey);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    std::set<std::string> actual;

    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->Next())) {
      const auto doc = docs->Value();
      const auto bytes = values.Get(doc);
      irs::BytesViewInput in;
      in.reset(bytes);
      actual.emplace(irs::ReadString<std::string>(in));
    }
    ASSERT_EQ(expected, actual);
  }

  // a > 30.
  {
    std::set<std::string> expected;

    // fill expected values
    {
      gen.reset();
      while (auto* doc = gen.next()) {
        auto* numeric_field =
          dynamic_cast<GranularDoubleField*>(doc->indexed.get("a"));
        ASSERT_NE(nullptr, numeric_field);

        if (numeric_field->value() > 30.) {
          auto* key_field =
            dynamic_cast<tests::StringField*>(doc->indexed.get("_key"));
          ASSERT_NE(nullptr, key_field);

          expected.emplace(std::string(key_field->value()));
        }
      }
    }

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    irs::SetGranularNumericTerm(query.mutable_options()->range.min, 30.);
    query.mutable_options()->range.min_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{query, reader};
    ASSERT_NE(nullptr, prepared.Query(0));
    const auto* column = segment.Column(kKey);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    std::set<std::string> actual;

    auto docs = prepared.Execute(0);
    while (!irs::doc_limits::eof(docs->Next())) {
      const auto doc = docs->Value();
      const auto bytes = values.Get(doc);
      irs::BytesViewInput in;
      in.reset(bytes);
      actual.emplace(irs::ReadString<std::string>(in));
    }
    ASSERT_EQ(expected, actual);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(granular_range_filter_test,
                         GranularRangeFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         GranularRangeFilterTestCase::to_string);

}  // namespace
