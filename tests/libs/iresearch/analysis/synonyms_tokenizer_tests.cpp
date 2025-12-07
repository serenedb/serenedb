#include <iresearch/analysis/synonyms_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>

#include "gtest/gtest.h"

using SynonymsTokenizer = irs::analysis::SynonymsTokenizer;

TEST(token_synonyms_stream_tests, consts) {
  static_assert("synonyms" == irs::Type<SynonymsTokenizer>::name());
}

TEST(token_synonyms_stream_tests, test_masking) {
  // test no synonyms
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    SynonymsTokenizer::synonyms_map mask;
    SynonymsTokenizer stream(std::move(mask));
    ASSERT_EQ(irs::Type<SynonymsTokenizer>::id(), stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("abc", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("ghi", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }

  // test with synonyms
  {
    std::string_view data0("foo");
    std::string_view data1("bar");
    std::string_view data2("xyz");

    const std::vector<std::string_view> synonyms{"foo", "bar"};
    SynonymsTokenizer::synonyms_map mask = {
      {"foo", &synonyms},
      {"bar", &synonyms},
    };
    SynonymsTokenizer stream(std::move(mask));

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("foo", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
    ASSERT_EQ(0, inc->value);

    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("foo", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("bar", irs::ViewCast<char>(term->value));
    ASSERT_EQ(0, inc->value);

    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data2));

    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(3, offset->end);
    ASSERT_EQ("xyz", irs::ViewCast<char>(term->value));
    ASSERT_EQ(1, inc->value);

    ASSERT_FALSE(stream.next());
  }
}

TEST(token_synonyms_stream_tests, parsing) {
  {
    std::string_view data0("foo,bar\n");
    auto result = SynonymsTokenizer::parse(data0);
    const auto holder = result.result;
    {
      SynonymsTokenizer::synonyms_holder expected{{"foo", "bar"}};
      ASSERT_EQ(expected, holder);
    }

    {
      const auto sysnosyms_map = SynonymsTokenizer::parse(holder);
      SynonymsTokenizer::synonyms_map expected = {
        {"foo", &holder.back()},
        {"bar", &holder.back()},
      };
      ASSERT_EQ(expected, sysnosyms_map.result);
    }
  }

  {
    std::string_view data0("foo,bar\n\n#some comment\naaa, bbb, cc");
    auto result = SynonymsTokenizer::parse(data0);
    const auto holder = result.result;
    {
      SynonymsTokenizer::synonyms_holder expected{{"foo", "bar"},
                                                  {"aaa", "bbb", "cc"}};
      ASSERT_EQ(expected, holder);
    }

    {
      const auto actual = SynonymsTokenizer::parse(holder);
      SynonymsTokenizer::synonyms_map expected = {
        {"foo", &holder[0]}, {"bar", &holder[0]}, {"aaa", &holder[1]},
        {"bbb", &holder[1]}, {"cc", &holder[1]},
      };
      ASSERT_EQ(expected, actual.result);
    }
  }
}
