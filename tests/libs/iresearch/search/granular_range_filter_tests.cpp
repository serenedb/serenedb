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

#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/search/granular_range_filter.hpp"
#include "iresearch/store/store_utils.hpp"
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
      field.value(
        irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
    } else if (data.is_bool() && data.b) {
      doc.insert(std::make_shared<tests::BinaryField>());
      auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
      field.Name(name);
      field.id = fid;
      field.value(
        irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
    } else if (data.is_bool() && !data.b) {
      doc.insert(std::make_shared<tests::BinaryField>());
      auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
      field.Name(name);
      field.id = fid;
      field.value(
        irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
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
      irs::SetGranularTerm(
        q.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        q.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("M")));
      q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      q.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{q, irs::SubReader::empty(), nullptr,
                                     counter};
      ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // with boost
    {
      irs::score_t boost = 1.5f;
      irs::ByGranularRange q;
      *q.mutable_field_id() = kName;
      irs::SetGranularTerm(
        q.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        q.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("M")));
      q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      q.mutable_options()->is_granular = false;
      q.boost(boost);

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
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT32_C(0));

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT32_C(1000));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      ASSERT_EQ(2, query.options().range.min.size());
      ASSERT_EQ(2, query.options().range.max.size());

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // range under different granularity value for topmost element, (i.e. last
    // value from numeric_token_stream)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT32_C(-1000));

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT32_C(+1000));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      ASSERT_EQ(2, query.options().range.min.size());
      ASSERT_EQ(2, query.options().range.max.size());

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = [-20000..+20000]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(double_t(-20000));

      irs::NumericTokenizer max_stream;
      max_stream.reset(double_t(+20000));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6, 7, 10, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value > 100
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(double_t(100));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max,
                           irs::numeric_utils::numeric_traits<double_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value => 100
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(double_t(100));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value => 20007 (largest value)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(double_t(20007));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value < 10000.123
    {
      irs::NumericTokenizer max_stream;
      max_stream.reset(double_t(10000.123));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::numeric_utils::numeric_traits<double_t>::ninf());
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 9, 10, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value <= 10000.123
    {
      irs::NumericTokenizer max_stream;
      max_stream.reset(double_t(10000.123));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 9, 10, 11, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
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
        for (; docs->next();) {
          actual.push_back(docs->value());
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
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT64_C(7));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT64_C(7));
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq = [1..7]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT64_C(1));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT64_C(7));
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT64_C(31));

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT64_C(32));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq > 28
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT64_C(28));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        (irs::numeric_utils::numeric_traits<int64_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq >= 31 (match largest value)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT64_C(31));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        (irs::numeric_utils::numeric_traits<int64_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // long - seq <= 5
    {
      irs::NumericTokenizer max_stream;
      max_stream.reset(INT64_C(5));
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        (irs::numeric_utils::numeric_traits<int64_t>::min)());
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq = [7..7]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT32_C(7));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT32_C(7));
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq = [1..7]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT32_C(1));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT32_C(7));
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT32_C(31));

      irs::NumericTokenizer max_stream;
      max_stream.reset(INT32_C(32));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq > 28
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT32_C(28));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        (irs::numeric_utils::numeric_traits<int32_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq >= 31 (match largest value)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(INT32_C(31));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        (irs::numeric_utils::numeric_traits<int32_t>::max)());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // int - seq <= 5
    {
      irs::NumericTokenizer max_stream;
      max_stream.reset(INT32_C(5));
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        (irs::numeric_utils::numeric_traits<int32_t>::min)());
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value = [123..123]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset((float_t)123.f);
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::NumericTokenizer max_stream;
      max_stream.reset((float_t)123.f);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value = [91.524..123)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset((float_t)91.524f);
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::NumericTokenizer max_stream;
      max_stream.reset((float_t)123.f);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 10, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(float_t(31));

      irs::NumericTokenizer max_stream;
      max_stream.reset(float_t(32));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value < 91.565
    {
      irs::NumericTokenizer max_stream;
      max_stream.reset((float_t)90.565f);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min,
                           irs::numeric_utils::numeric_traits<float_t>::ninf());
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{4, 11, 13, 14, 15, 16, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value > 91.565
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset((float_t)90.565f);
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max,
                           irs::numeric_utils::numeric_traits<float_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 5, 6, 7, 8, 9, 10, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // float - value >= 31 (largest value)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(float_t(31));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max,
                           irs::numeric_utils::numeric_traits<float_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = [123...123]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset((double_t)123.);
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());
      irs::NumericTokenizer max_stream;
      max_stream.reset((double_t)123.);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = (-40; 90.564]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset((double_t)-40.);
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());
      irs::NumericTokenizer max_stream;
      max_stream.reset((double_t)90.564);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{4, 11, 13, 14, 15, 16, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = [31 .. 32] with same-level granularity (last value in
    // segment)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(double_t(31));

      irs::NumericTokenizer max_stream;
      max_stream.reset(double_t(32));

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
      irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value < 5;
    {
      irs::NumericTokenizer max_stream;
      max_stream.reset((double_t)5.);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::numeric_utils::numeric_traits<double_t>::ninf());
      irs::SetGranularTerm(query.mutable_options()->range.max, max_term->value);
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{14, 15, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value > 90.543;
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset((double_t)90.543);
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kValue;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max,
                           irs::numeric_utils::numeric_traits<double_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 13};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value >= 31 (largest value)
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset(double_t(31));
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());

      irs::ByGranularRange query;
      *query.mutable_field_id() = kSeq;
      irs::SetGranularTerm(query.mutable_options()->range.min, min_term->value);
      irs::SetGranularTerm(query.mutable_options()->range.max,
                           irs::numeric_utils::numeric_traits<double_t>::inf());
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; docs->next();) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }
  }

  void ByRangeSequentialCost() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &ByRangeJsonFieldFactory);

      add_segment(gen, irs::kOmCreate);
    }

    auto rdr = open_reader();

    // empty query
    CheckQuery(irs::ByGranularRange(), Docs{}, rdr);

    // name = (..;..)
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // invalid_name = (..;..)
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kInvalidName;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), Docs{}, rdr);
    }

    // name = [A;..)
    // result: A .. Z, ~
    {
      Docs docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14,
                15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (A;..)
    // result: A .. Z, ~
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14,
                15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (..;C)
    // result: A, B, !, @, #, $, %
    {
      Docs docs{1, 2, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("C")));
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (..;C]
    // result: A, B, C, !, @, #, $, %
    {
      Docs docs{1, 2, 3, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("C")));
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = [A;C]
    // result: A, B, C
    {
      Docs docs{1, 2, 3};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("C")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = [A;B]
    // result: A, B
    {
      Docs docs{1, 2};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("B")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = [A;B)
    // result: A
    {
      Docs docs{1};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("B")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (A;B]
    // result: A
    {
      Docs docs{2};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("B")));
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (A;B)
    // result:
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("B")));
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), Docs{}, Costs{0}, rdr);
    }

    // name = [A;C)
    // result: A, B
    {
      Docs docs{1, 2};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("C")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (A;C]
    // result: B, C
    {
      Docs docs{2, 3};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("C")));
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (A;C)
    // result: B
    {
      Docs docs{2};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("C")));
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = [C;A]
    // result:
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("C")));
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("A")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), Docs{}, Costs{0}, rdr);
    }

    // name = [~;..]
    // result: ~
    {
      Docs docs{27};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("~")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = (~;..]
    // result:
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("~")));
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), Docs{}, Costs{0}, rdr);
    }

    // name = (a;..]
    // result: ~
    {
      Docs docs{27};
      Costs costs{1};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("a")));
      query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = [..;a]
    // result: !, @, #, $, %, A..Z
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("a")));
      query.mutable_options()->range.max_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = [..;a)
    // result: !, @, #, $, %, A..Z
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.max,
        irs::ViewCast<irs::byte_type>(std::string_view("a")));
      query.mutable_options()->range.max_type = irs::BoundType::Exclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), docs, costs, rdr);
    }

    // name = [DEL;..]
    // result:
    {
      irs::ByGranularRange query;
      *query.mutable_field_id() = kName;
      irs::SetGranularTerm(
        query.mutable_options()->range.min,
        irs::ViewCast<irs::byte_type>(std::string_view("\x7f")));
      query.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      query.mutable_options()->is_granular = false;

      CheckQuery(*tests::Optimized(query), Docs{}, Costs{0}, rdr);
    }
  }
};

TEST(by_granular_range_test, options) {
  irs::ByGranularRangeOptions opts;
  ASSERT_TRUE(opts.range.min.empty());
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.min_type);
  ASSERT_TRUE(opts.range.max.empty());
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.max_type);
  ASSERT_EQ(1024, opts.scored_terms_limit);
}

TEST(by_granular_range_test, ctor) {
  irs::ByGranularRange q;
  ASSERT_EQ(irs::Type<irs::ByGranularRange>::id(), q.type());
  ASSERT_EQ(irs::ByGranularRangeOptions{}, q.options());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_granular_range_test, equal) {
  irs::ByGranularRange q0;
  *q0.mutable_field_id() = kField;
  irs::SetGranularTerm(
    q0.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  irs::SetGranularTerm(
    q0.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q0.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q0.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  irs::ByGranularRange q1;
  *q1.mutable_field_id() = kField;
  irs::SetGranularTerm(
    q1.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  irs::SetGranularTerm(
    q1.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_EQ(q0, q1);

  irs::ByGranularRange q2;
  *q2.mutable_field_id() = kField1;
  irs::SetGranularTerm(
    q2.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  irs::SetGranularTerm(
    q2.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q2.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q2.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q2);

  irs::ByGranularRange q3;
  *q3.mutable_field_id() = kField;
  irs::SetGranularTerm(
    q3.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term1")));
  irs::SetGranularTerm(
    q3.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q3.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q3.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q3);

  irs::ByGranularRange q4;
  *q4.mutable_field_id() = kField;
  irs::SetGranularTerm(
    q4.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  irs::SetGranularTerm(
    q4.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term1")));
  q4.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q4.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q4);

  irs::ByGranularRange q5;
  *q5.mutable_field_id() = kField;
  irs::SetGranularTerm(
    q5.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  irs::SetGranularTerm(
    q5.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q5.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  q5.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q5);

  irs::ByGranularRange q6;
  *q6.mutable_field_id() = kField;
  irs::SetGranularTerm(
    q6.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  irs::SetGranularTerm(
    q6.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q6.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q6.mutable_options()->range.max_type = irs::BoundType::Exclusive;

  ASSERT_NE(q0, q6);

  irs::ByGranularRange q7;
  *q7.mutable_field_id() = kField;
  irs::SetGranularTerm(
    q7.mutable_options()->range.min,
    irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
  irs::SetGranularTerm(
    q7.mutable_options()->range.max,
    irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
  q7.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q7.mutable_options()->range.max_type = irs::BoundType::Inclusive;
  q7.mutable_options()->scored_terms_limit = 100;

  ASSERT_NE(q0, q7);

  ASSERT_NE(q0, q6);
}

TEST(by_granular_range_test, boost) {
  // no boost
  {
    irs::ByGranularRange q;
    *q.mutable_field_id() = kField;
    irs::SetGranularTerm(
      q.mutable_options()->range.min,
      irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
    irs::SetGranularTerm(
      q.mutable_options()->range.max,
      irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  // with boost, empty query
  {
    irs::score_t boost = 1.5f;
    irs::ByGranularRange q;
    *q.mutable_field_id() = kField;
    irs::SetGranularTerm(
      q.mutable_options()->range.min,
      irs::ViewCast<irs::byte_type>(std::string_view("min_term")));
    irs::SetGranularTerm(
      q.mutable_options()->range.max,
      irs::ViewCast<irs::byte_type>(std::string_view("max_term")));
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    q.boost(boost);

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(boost, prepared.Query(0)->Boost());
  }
}

TEST_P(GranularRangeFilterTestCase, by_range) { ByRangeSequentialCost(); }

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
    irs::SetGranularTerm(q.mutable_options()->range.min,
                         irs::numeric_utils::numeric_traits<double_t>::ninf());
    irs::SetGranularTerm(q.mutable_options()->range.max,
                         irs::numeric_utils::numeric_traits<double_t>::inf());
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    CheckQuery(*tests::Optimized(q), scorers, docs, rdr, false);
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
    irs::SetGranularTerm(q.mutable_options()->range.min,
                         irs::numeric_utils::numeric_traits<double_t>::ninf());
    irs::SetGranularTerm(q.mutable_options()->range.max,
                         irs::numeric_utils::numeric_traits<double_t>::inf());
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

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
    irs::SetGranularTerm(q.mutable_options()->range.min,
                         irs::numeric_utils::numeric_traits<double_t>::ninf());
    irs::SetGranularTerm(q.mutable_options()->range.max,
                         irs::numeric_utils::numeric_traits<double_t>::inf());
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    CheckQuery(*tests::Optimized(q), order, docs, rdr);
  }

  // value = (..;..) + scored_terms_limit
  {
    Docs docs{2, 4, 6, 11, 12, 13, 14, 15, 16, 17, 1, 5, 7, 9, 10, 3, 8};
    Costs costs{docs.size()};
    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    irs::ByGranularRange q;
    *q.mutable_field_id() = kValue;
    irs::SetGranularTerm(q.mutable_options()->range.min,
                         irs::numeric_utils::numeric_traits<double_t>::ninf());
    irs::SetGranularTerm(q.mutable_options()->range.max,
                         irs::numeric_utils::numeric_traits<double_t>::inf());
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;
    q.mutable_options()->scored_terms_limit = 2;

    CheckQuery(*tests::Optimized(q), order, docs, rdr);
  }

  // value = (..;100)
  {
    Docs docs{4, 11, 12, 13, 14, 15, 16, 17};
    Costs costs{docs.size()};
    irs::NumericTokenizer max_stream;
    max_stream.reset((double_t)100.);
    auto* max_term = irs::get<irs::TermAttr>(max_stream);

    ASSERT_TRUE(max_stream.next());

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    irs::ByGranularRange q;
    *q.mutable_field_id() = kValue;
    irs::SetGranularTerm(q.mutable_options()->range.min,
                         irs::numeric_utils::numeric_traits<double_t>::ninf());
    irs::SetGranularTerm(q.mutable_options()->range.max, max_term->value);
    q.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    q.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    CheckQuery(*tests::Optimized(q), order, docs, rdr);
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
    irs::NumericTokenizer min_stream;
    min_stream.reset((double_t)begin);

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    irs::ByGranularRange q;
    *q.mutable_field_id() = kSeq;
    irs::SetGranularTerm(q.mutable_options()->range.min, min_stream);
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;

    CheckQuery(*tests::Optimized(q), order, docs, rdr);
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
        field.value(
          irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
      } else if (data.is_bool() && data.b) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.id = fid;
        field.value(
          irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
      } else if (data.is_bool() && !data.b) {
        doc.insert(std::make_shared<tests::BinaryField>());
        auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
        field.Name(name);
        field.id = fid;
        field.value(
          irs::ViewCast<irs::byte_type>(irs::BooleanTokenizer::value_true()));
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

    irs::NumericTokenizer max_stream;
    max_stream.reset(30.);

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    irs::SetGranularTerm(query.mutable_options()->range.min,
                         irs::numeric_utils::numeric_traits<double_t>::ninf());
    irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
    query.mutable_options()->range.min_type = irs::BoundType::Exclusive;
    query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{query, reader};
    ASSERT_NE(nullptr, prepared.Query(0));
    const auto* column = segment.Column(kKey);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    std::set<std::string> actual;

    auto docs = prepared.Execute(0);
    while (docs->next()) {
      const auto doc = docs->value();
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

    irs::NumericTokenizer max_stream;
    max_stream.reset(30.);

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    irs::SetGranularTerm(query.mutable_options()->range.max, max_stream);
    query.mutable_options()->range.max_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{query, reader};
    ASSERT_NE(nullptr, prepared.Query(0));
    const auto* column = segment.Column(kKey);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    std::set<std::string> actual;

    auto docs = prepared.Execute(0);
    while (docs->next()) {
      const auto doc = docs->value();
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

    irs::NumericTokenizer min_stream;
    min_stream.reset(30.);

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
    irs::SetGranularTerm(query.mutable_options()->range.max,
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
    while (docs->next()) {
      const auto doc = docs->value();
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

    irs::NumericTokenizer min_stream;
    min_stream.reset(30.);

    irs::ByGranularRange query;
    *query.mutable_field_id() = kA;
    irs::SetGranularTerm(query.mutable_options()->range.min, min_stream);
    query.mutable_options()->range.min_type = irs::BoundType::Exclusive;

    tests::PreparedFilter prepared{query, reader};
    ASSERT_NE(nullptr, prepared.Query(0));
    const auto* column = segment.Column(kKey);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};

    std::set<std::string> actual;

    auto docs = prepared.Execute(0);
    while (docs->next()) {
      const auto doc = docs->value();
      const auto bytes = values.Get(doc);
      irs::BytesViewInput in;
      in.reset(bytes);
      actual.emplace(irs::ReadString<std::string>(in));
    }
    ASSERT_EQ(expected, actual);
  }
}

TEST_P(GranularRangeFilterTestCase, visit) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &ByRangeJsonFieldFactory);

    add_segment(gen, irs::kOmCreate);
  }

  tests::EmptyFilterVisitor visitor;
  irs::ByGranularRange::options_type opts;
  opts.is_granular = false;
  auto& rng = opts.range;
  rng.min = {static_cast<irs::bstring>(
    irs::ViewCast<irs::byte_type>(std::string_view("abc")))};
  rng.max = {static_cast<irs::bstring>(
    irs::ViewCast<irs::byte_type>(std::string_view("abcd")))};
  rng.min_type = irs::BoundType::Inclusive;
  rng.max_type = irs::BoundType::Inclusive;

  // read segment
  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];
  // get term dictionary for field
  const auto* reader = segment.field(kPrefix);
  ASSERT_TRUE(reader != nullptr);

  irs::ByGranularRange::visit(segment, *reader, opts, visitor);
  ASSERT_EQ(2, visitor.prepare_calls_counter());
  ASSERT_EQ(2, visitor.visit_calls_counter());
  ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
              {"abc", irs::kNoBoost}, {"abcd", irs::kNoBoost}}),
            visitor.term_refs<char>());

  visitor.reset();
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(granular_range_filter_test,
                         GranularRangeFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         GranularRangeFilterTestCase::to_string);

}  // namespace
