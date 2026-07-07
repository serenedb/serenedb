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
#include "iresearch/search/range_filter.hpp"
#include "tests_shared.hpp"

namespace {

// Per-file field_id constants used by all fixtures + filter-side calls.
// Sourced from `tests::FieldIdFor` so the shared JSON factories and these
// tests agree on which id a name maps to.
[[maybe_unused]] inline constexpr irs::field_id kSeqFieldId =
  tests::FieldIdFor("seq");
[[maybe_unused]] inline constexpr irs::field_id kValueFieldId =
  tests::FieldIdFor("value");
[[maybe_unused]] inline constexpr irs::field_id kNameFieldId =
  tests::FieldIdFor("name");
[[maybe_unused]] inline constexpr irs::field_id kInvalidNameFieldId =
  tests::FieldIdFor("invalid_name");
[[maybe_unused]] inline constexpr irs::field_id kFieldFieldId =
  tests::FieldIdFor("field");
[[maybe_unused]] inline constexpr irs::field_id kField1FieldId =
  tests::FieldIdFor("field1");
[[maybe_unused]] inline constexpr irs::field_id kPrefixFieldId =
  tests::FieldIdFor("prefix");

irs::ByRange MakeFilter(irs::field_id field, const irs::bytes_view& min,
                        irs::BoundType min_type, const irs::bytes_view& max,
                        irs::BoundType max_type) {
  irs::ByRange filter;
  *filter.mutable_field_id() = field;

  auto& range = filter.mutable_options()->range;
  range.min = min;
  range.min_type = min_type;
  range.max = max;
  range.max_type = max_type;
  return filter;
}

class RangeFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  void ByRangeSequentialNumeric() {
    /* add segment */
    {
      tests::JsonDocGenerator gen(
        resource("simple_sequential.json"),
        [](tests::Document& doc, const std::string& name,
           const tests::JsonDocGenerator::JsonValue& data) {
          const auto fid = tests::FieldIdFor(name);
          if (data.is_string()) {
            auto f = std::make_shared<tests::StringField>(name, data.str);
            f->id = fid;
            doc.insert(std::move(f));
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
            field.value(irs::ViewCast<irs::byte_type>(
              irs::BooleanTokenizer::value_true()));
          } else if (data.is_bool() && !data.b) {
            doc.insert(std::make_shared<tests::BinaryField>());
            auto& field = (doc.indexed.end() - 1).as<tests::BinaryField>();
            field.Name(name);
            field.id = fid;
            field.value(irs::ViewCast<irs::byte_type>(
              irs::BooleanTokenizer::value_true()));
          } else if (data.is_number()) {
            const double d_value = data.as_number<double_t>();
            // 'value' can be interpreted as a double
            {
              doc.insert(std::make_shared<tests::DoubleField>());
              auto& field = (doc.indexed.end() - 1).as<tests::DoubleField>();
              field.Name(name);
              field.id = fid;
              field.value(d_value);
            }

            const float f_value = data.as_number<float_t>();
            {
              // 'value' can be interpreted as a float
              doc.insert(std::make_shared<tests::FloatField>());
              auto& field = (doc.indexed.end() - 1).as<tests::FloatField>();
              field.Name(name);
              field.id = fid;
              field.value(f_value);
            }

            const auto value = std::ceil(d_value);
            {
              doc.insert(std::make_shared<tests::LongField>());
              auto& field = (doc.indexed.end() - 1).as<tests::LongField>();
              field.Name(name);
              field.id = fid;
              field.value(static_cast<int64_t>(value));
            }
            {
              doc.insert(std::make_shared<tests::IntField>());
              auto& field = (doc.indexed.end() - 1).as<tests::IntField>();
              field.Name(name);
              field.id = fid;
              field.value(static_cast<int32_t>(value));
            }
          }
        });
      add_segment(gen);
    }

    auto rdr = open_reader();

    MaxMemoryCounter counter;

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

      irs::ByRange query =
        MakeFilter(kSeqFieldId, min_term->value, irs::BoundType::Inclusive,
                   max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr, nullptr, counter};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

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

      irs::ByRange query =
        MakeFilter(kSeqFieldId, min_term->value, irs::BoundType::Inclusive,
                   max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kSeqFieldId, min_term->value, irs::BoundType::Exclusive,
                   (irs::numeric_utils::numeric_traits<int64_t>::max)(),
                   irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query = MakeFilter(
        kSeqFieldId, (irs::numeric_utils::numeric_traits<int64_t>::min)(),
        irs::BoundType::Inclusive, max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kSeqFieldId, min_term->value, irs::BoundType::Inclusive,
                   max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kSeqFieldId, min_term->value, irs::BoundType::Inclusive,
                   max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{2, 3, 4, 5, 6, 7, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kSeqFieldId, min_term->value, irs::BoundType::Exclusive,
                   (irs::numeric_utils::numeric_traits<int32_t>::max)(),
                   irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{30, 31, 32};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query = MakeFilter(
        kSeqFieldId, (irs::numeric_utils::numeric_traits<int32_t>::min)(),
        irs::BoundType::Inclusive, max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 4, 5, 6};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kValueFieldId, min_term->value, irs::BoundType::Inclusive,
                   max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kValueFieldId, min_term->value, irs::BoundType::Inclusive,
                   max_term->value, irs::BoundType::Exclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 5, 7, 9, 10, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query = MakeFilter(
        kValueFieldId, irs::numeric_utils::numeric_traits<float_t>::ninf(),
        irs::BoundType::Inclusive, max_term->value, irs::BoundType::Exclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{4, 11, 13, 14, 15, 16, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kValueFieldId, min_term->value, irs::BoundType::Exclusive,
                   irs::numeric_utils::numeric_traits<float_t>::inf(),
                   irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 5, 6, 7, 8, 9, 10, 12};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }

    // double - value = [123..123]
    {
      irs::NumericTokenizer min_stream;
      min_stream.reset((double_t)123.);
      auto* min_term = irs::get<irs::TermAttr>(min_stream);
      ASSERT_TRUE(min_stream.next());
      irs::NumericTokenizer max_stream;
      max_stream.reset((double_t)123.);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);
      ASSERT_TRUE(max_stream.next());

      irs::ByRange query =
        MakeFilter(kValueFieldId, min_term->value, irs::BoundType::Inclusive,
                   max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{3, 8};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kValueFieldId, min_term->value, irs::BoundType::Exclusive,
                   max_term->value, irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{4, 11, 13, 14, 15, 16, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query = MakeFilter(
        kValueFieldId, irs::numeric_utils::numeric_traits<double_t>::ninf(),
        irs::BoundType::Exclusive, max_term->value, irs::BoundType::Exclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{14, 15, 17};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
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

      irs::ByRange query =
        MakeFilter(kValueFieldId, min_term->value, irs::BoundType::Exclusive,
                   irs::numeric_utils::numeric_traits<double_t>::inf(),
                   irs::BoundType::Inclusive);

      tests::PreparedFilter prepared{query, rdr};

      std::vector<irs::doc_id_t> expected{1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 13};
      std::vector<irs::doc_id_t> actual;

      for (size_t i = 0; [[maybe_unused]] const auto& sub : rdr) {
        auto docs = prepared.Execute(i);
        for (; !irs::doc_limits::eof(docs->advance());) {
          actual.push_back(docs->value());
        }
        ++i;
      }
      ASSERT_EQ(expected, actual);
    }
  }

  void ByRangeSequentialCost() {
    /* add segment */
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // empty query
    CheckQuery(irs::ByRange(), Docs{}, rdr);

    // name = (..;..)
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // invalid_name = (..;..)
    {
      irs::ByRange filter;
      *filter.mutable_field_id() = kInvalidNameFieldId;

      CheckQuery(*tests::Optimized(filter), Docs{}, rdr);
    }

    // name = ["";..)
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = ("";..]
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = ["";""]
    {
      Docs docs{};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [A;..)
    // result: A .. Z, ~
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14,
                15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (A;..)
    // result: A .. Z, ~
    {
      Docs docs{2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14,
                15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (..;C)
    // result: A, B, !, @, #, $, %
    {
      Docs docs{1, 2, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("C"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (..;C]
    // result: A, B, C, !, @, #, $, %
    {
      Docs docs{1, 2, 3, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("C"));
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [A;C]
    // result: A, B, C
    {
      Docs docs{1, 2, 3};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("C"));
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [A;B]
    // result: A, B
    {
      Docs docs{1, 2};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("B"));
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [A;B)
    // result: A
    {
      Docs docs{1};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("B"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (A;B]
    // result: A
    {
      Docs docs{2};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("B"));
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (A;B)
    // result:
    {
      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("B"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), Docs{}, Costs{0}, rdr);
    }

    // name = [A;C)
    // result: A, B
    {
      Docs docs{1, 2};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("C"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (A;C]
    // result: B, C
    {
      Docs docs{2, 3};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("C"));
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (A;C)
    // result: B
    {
      Docs docs{2};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("C"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [C;A]
    // result:
    {
      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("C"));
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("A"));
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), Docs{}, Costs{0}, rdr);
    }

    // name = [~;..]
    // result: ~
    {
      Docs docs{27};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("~"));
      filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("~"));
      filter.mutable_options()->range.max_type = irs::BoundType::Unbounded;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = (~;..]
    // result:
    {
      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("~"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), Docs{}, Costs{0}, rdr);
    }

    // name = (a;..]
    // result: ~
    {
      Docs docs{27};
      Costs costs{1};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("a"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("a"));
      filter.mutable_options()->range.max_type = irs::BoundType::Unbounded;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [..;a]
    // result: !, @, #, $, %, A..Z
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("a"));
      filter.mutable_options()->range.min_type = irs::BoundType::Unbounded;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("a"));
      filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [..;a)
    // result: !, @, #, $, %, A..Z
    {
      Docs docs{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 28, 29, 30, 31, 32};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.max =
        irs::ViewCast<irs::byte_type>(std::string_view("a"));
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), docs, costs, rdr);
    }

    // name = [DEL;..]
    // result:
    {
      irs::ByRange filter;
      *filter.mutable_field_id() = kNameFieldId;
      filter.mutable_options()->range.min =
        irs::ViewCast<irs::byte_type>(std::string_view("\x7f"));
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;

      CheckQuery(*tests::Optimized(filter), Docs{}, Costs{0}, rdr);
    }
  }

  void ByRangeSequentialOrder() {
    // add segment
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }

    auto rdr = open_reader();

    // empty query
    CheckQuery(irs::ByRange(), Docs{}, rdr);

    // value = (..;..) test collector call count for field/term/finish
    {
      Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
      Costs costs{docs.size()};

      size_t finish_count = 0;
      uint64_t finish_docs_with_field = 0;
      uint64_t finish_docs_with_term = 0;

      irs::Scorer::ptr sort{std::make_unique<tests::sort::CustomSort>()};
      auto& scorer = static_cast<tests::sort::CustomSort&>(*sort);

      scorer.collectors_collect = [&](irs::byte_type*,
                                      const irs::FieldCollector* field,
                                      const irs::TermCollector* term) -> void {
        ++finish_count;
        ASSERT_NE(nullptr, field);
        ASSERT_NE(nullptr, term);
        finish_docs_with_field += field->docs_with_field;
        finish_docs_with_term += term->docs_with_term;
      };

      irs::ByRange filter;
      *filter.mutable_field_id() = kValueFieldId;
      filter.mutable_options()->range.min =
        irs::numeric_utils::numeric_traits<double_t>::ninf();
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max =
        irs::numeric_utils::numeric_traits<double_t>::inf();
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      CheckQuery(tests::FilterWrapper{filter}, std::span{&sort, 1}, docs, rdr);
      ASSERT_EQ(11, finish_count);
      ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
      ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
    }

    // value = (..;..)
    {
      Docs docs{1, 5, 7, 9, 10, 3, 4, 8, 11, 2, 6, 12, 13, 14, 15, 16, 17};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kValueFieldId;

      irs::Scorer::ptr sort{std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(*tests::Optimized(filter), std::span{&sort, 1}, docs, rdr);
    }

    // value = (..;..) + scored_terms_limit
    {
      Docs docs{2, 4, 6, 11, 12, 13, 14, 15, 16, 17, 1, 5, 7, 9, 10, 3, 8};
      Costs costs{docs.size()};

      irs::ByRange filter;
      *filter.mutable_field_id() = kValueFieldId;
      filter.mutable_options()->range.min =
        irs::numeric_utils::numeric_traits<double_t>::ninf();
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max =
        irs::numeric_utils::numeric_traits<double_t>::inf();
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
      filter.mutable_options()->scored_terms_limit = 2;

      irs::Scorer::ptr sort{std::make_unique<tests::sort::FrequencySort>()};

      CheckQuery(*tests::Optimized(filter), std::span{&sort, 1}, docs, rdr);
    }

    // value = (..;100)
    {
      Docs docs{4, 11, 12, 13, 14, 15, 16, 17};
      Costs costs{docs.size()};
      irs::NumericTokenizer max_stream;
      max_stream.reset((double_t)100.);
      auto* max_term = irs::get<irs::TermAttr>(max_stream);

      ASSERT_TRUE(max_stream.next());

      irs::ByRange filter;
      *filter.mutable_field_id() = kValueFieldId;
      filter.mutable_options()->range.min =
        irs::numeric_utils::numeric_traits<double_t>::ninf();
      filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
      filter.mutable_options()->range.max = max_term->value;
      filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

      irs::Scorer::ptr sort{std::make_unique<tests::sort::FrequencySort>()};
      CheckQuery(*tests::Optimized(filter), std::span{&sort, 1}, docs, rdr);
    }
  }
};

TEST(by_range_test, options) {
  irs::ByRangeOptions opts;
  ASSERT_TRUE(opts.range.min.empty());
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.min_type);
  ASSERT_TRUE(opts.range.max.empty());
  ASSERT_EQ(irs::BoundType::Unbounded, opts.range.max_type);
  ASSERT_EQ(1024, opts.scored_terms_limit);
}

TEST(by_range_test, ctor) {
  irs::ByRange q;
  ASSERT_EQ(irs::Type<irs::ByRange>::id(), q.type());
  ASSERT_EQ(irs::ByRangeOptions{}, q.options());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_range_test, equal) {
  irs::ByRange q0;
  *q0.mutable_field_id() = kFieldFieldId;
  q0.mutable_options()->range.min =
    irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
  q0.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q0.mutable_options()->range.max =
    irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
  q0.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  irs::ByRange q1;
  *q1.mutable_field_id() = kFieldFieldId;
  q1.mutable_options()->range.min =
    irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
  q1.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q1.mutable_options()->range.max =
    irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
  q1.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_EQ(q0, q1);

  irs::ByRange q2;
  *q2.mutable_field_id() = kField1FieldId;
  q2.mutable_options()->range.min =
    irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
  q2.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q2.mutable_options()->range.max =
    irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
  q2.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q2);

  irs::ByRange q3;
  *q3.mutable_field_id() = kFieldFieldId;
  q3.mutable_options()->range.min =
    irs::ViewCast<irs::byte_type>(std::string_view("min_term1"));
  q3.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q3.mutable_options()->range.max =
    irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
  q3.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q3);

  irs::ByRange q4;
  *q4.mutable_field_id() = kFieldFieldId;
  q4.mutable_options()->range.min =
    irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
  q4.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q4.mutable_options()->range.max =
    irs::ViewCast<irs::byte_type>(std::string_view("max_term1"));
  q4.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q4);

  irs::ByRange q5;
  *q5.mutable_field_id() = kFieldFieldId;
  q5.mutable_options()->range.min =
    irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
  q5.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  q5.mutable_options()->range.max =
    irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
  q5.mutable_options()->range.max_type = irs::BoundType::Inclusive;

  ASSERT_NE(q0, q5);

  irs::ByRange q6;
  *q6.mutable_field_id() = kFieldFieldId;
  q6.mutable_options()->range.min =
    irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
  q6.mutable_options()->range.min_type = irs::BoundType::Inclusive;
  q6.mutable_options()->range.max =
    irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
  q6.mutable_options()->range.max_type = irs::BoundType::Unbounded;

  ASSERT_NE(q0, q6);
}

TEST(by_range_test, boost) {
  // no boost
  {
    irs::ByRange q;
    *q.mutable_field_id() = kFieldFieldId;
    q.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }

  // with boost
  {
    irs::score_t boost = 1.5f;

    irs::ByRange q;
    *q.mutable_field_id() = kFieldFieldId;
    q.mutable_options()->range.min =
      irs::ViewCast<irs::byte_type>(std::string_view("min_term"));
    q.mutable_options()->range.min_type = irs::BoundType::Inclusive;
    q.mutable_options()->range.max =
      irs::ViewCast<irs::byte_type>(std::string_view("max_term"));
    q.mutable_options()->range.max_type = irs::BoundType::Inclusive;
    q.boost(boost);

    tests::PreparedFilter prepared{q, irs::SubReader::empty()};
    ASSERT_EQ(boost, prepared.Query(0)->Boost());
  }
}

TEST_P(RangeFilterTestCase, by_range) { ByRangeSequentialCost(); }

TEST_P(RangeFilterTestCase, by_range_numeric) { ByRangeSequentialNumeric(); }

TEST_P(RangeFilterTestCase, by_range_order) { ByRangeSequentialOrder(); }

TEST_P(RangeFilterTestCase, by_range_order_multi_segment_field_stats) {
  // two segments, both holding the queried field
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
    gen.reset();
    add_segment(gen, irs::kOmAppend);
  }

  auto rdr = open_reader();
  ASSERT_EQ(2, rdr.size());

  // field stats are summed across every segment that has the field
  uint64_t expected_docs_with_field = 0;
  for (const auto& segment : rdr) {
    if (const auto* field = segment.field(kValueFieldId)) {
      expected_docs_with_field += field->docs_count();
    }
  }
  ASSERT_GT(expected_docs_with_field, 0u);

  const irs::FieldCollector* shared_field = nullptr;
  size_t finish_count = 0;

  irs::Scorer::ptr sort{std::make_unique<tests::sort::CustomSort>()};
  auto& scorer = static_cast<tests::sort::CustomSort&>(*sort);
  scorer.collectors_collect = [&](irs::byte_type*,
                                  const irs::FieldCollector* field,
                                  const irs::TermCollector* term) -> void {
    ++finish_count;
    ASSERT_NE(nullptr, field);
    ASSERT_NE(nullptr, term);
    if (shared_field == nullptr) {
      shared_field = field;
    } else {
      ASSERT_EQ(shared_field, field);  // same collector reused for every term
    }
    ASSERT_EQ(expected_docs_with_field, field->docs_with_field);
  };

  irs::ByRange filter;
  *filter.mutable_field_id() = kValueFieldId;
  filter.mutable_options()->range.min =
    irs::numeric_utils::numeric_traits<double_t>::ninf();
  filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  filter.mutable_options()->range.max =
    irs::numeric_utils::numeric_traits<double_t>::inf();
  filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

  tests::PreparedFilter q{filter, rdr, sort.get()};
  ASSERT_NE(nullptr, q.Query(0));

  ASSERT_GT(finish_count, 1u);       // multiple scored terms
  ASSERT_NE(nullptr, shared_field);  // field stats were collected
}

TEST_P(RangeFilterTestCase, by_range_order_limit_field_stats) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
    gen.reset();
    add_segment(gen, irs::kOmAppend);
  }

  auto rdr = open_reader();
  ASSERT_EQ(2, rdr.size());

  const irs::FieldCollector* shared_field = nullptr;
  size_t finish_count = 0;

  irs::Scorer::ptr sort{std::make_unique<tests::sort::CustomSort>()};
  auto& scorer = static_cast<tests::sort::CustomSort&>(*sort);
  scorer.collectors_collect = [&](irs::byte_type*,
                                  const irs::FieldCollector* field,
                                  const irs::TermCollector* term) -> void {
    ++finish_count;
    ASSERT_NE(nullptr, field);
    ASSERT_NE(nullptr, term);
    if (shared_field == nullptr) {
      shared_field = field;
    } else {
      ASSERT_EQ(shared_field, field);
    }
  };

  irs::ByRange filter;
  *filter.mutable_field_id() = kValueFieldId;
  filter.mutable_options()->range.min =
    irs::numeric_utils::numeric_traits<double_t>::ninf();
  filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  filter.mutable_options()->range.max =
    irs::numeric_utils::numeric_traits<double_t>::inf();
  filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
  filter.mutable_options()->scored_terms_limit = 2;

  tests::PreparedFilter q{filter, rdr, sort.get()};
  ASSERT_NE(nullptr, q.Query(0));

  ASSERT_GT(finish_count, 0u);
  ASSERT_LE(finish_count, 2u);  // capped by scored_terms_limit
}

TEST_P(RangeFilterTestCase, by_range_order_no_match_field_stats) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
    gen.reset();
    add_segment(gen, irs::kOmAppend);
  }

  auto rdr = open_reader();
  ASSERT_EQ(2, rdr.size());

  size_t finish_count = 0;
  irs::Scorer::ptr sort{std::make_unique<tests::sort::CustomSort>()};
  auto& scorer = static_cast<tests::sort::CustomSort&>(*sort);
  scorer.collectors_collect = [&](irs::byte_type*, const irs::FieldCollector*,
                                  const irs::TermCollector*) -> void {
    ++finish_count;
  };

  irs::NumericTokenizer min_stream;
  min_stream.reset((double_t)1e9);
  auto* min_term = irs::get<irs::TermAttr>(min_stream);
  ASSERT_TRUE(min_stream.next());

  irs::ByRange filter;
  *filter.mutable_field_id() = kValueFieldId;
  filter.mutable_options()->range.min = min_term->value;
  filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
  filter.mutable_options()->range.max =
    irs::numeric_utils::numeric_traits<double_t>::inf();
  filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;

  tests::PreparedFilter q{filter, rdr, sort.get()};
  ASSERT_NE(nullptr, q.Query(0));
  ASSERT_EQ(0u, finish_count);  // nothing scored, no field stats collected
}

TEST_P(RangeFilterTestCase, visit) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  constexpr irs::field_id field = kPrefixFieldId;
  irs::ByRangeOptions::range_type range;
  range.min = irs::ViewCast<irs::byte_type>(std::string_view("abc"));
  range.max = irs::ViewCast<irs::byte_type>(std::string_view("abcd"));
  range.min_type = irs::BoundType::Inclusive;
  range.max_type = irs::BoundType::Inclusive;

  tests::EmptyFilterVisitor visitor;
  // read segment
  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];

  // get term dictionary for field
  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);
  irs::ByRange::visit(segment, *reader, range, visitor);
  ASSERT_EQ(1, visitor.prepare_calls_counter());
  ASSERT_EQ(2, visitor.visit_calls_counter());
  ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
              {"abc", irs::kNoBoost}, {"abcd", irs::kNoBoost}}),
            visitor.term_refs<char>());

  visitor.reset();
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(range_filter_test, RangeFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         RangeFilterTestCase::to_string);

}  // namespace
