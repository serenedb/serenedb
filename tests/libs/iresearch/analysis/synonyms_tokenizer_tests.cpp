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
    SynonymsTokenizer::SynonymsMap mask;
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
    SynonymsTokenizer::SynonymsMap mask = {
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
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("foo,bar=>foo");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
    {
      const auto actual = SynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("foo,bar=>foo,bar\n");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines.back().out},
        {"bar", &synonyms_lines.back().out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("foo,bar\n\n#some comment\naaa, bbb, cc");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}},
        SynonymsTokenizer::SynonymsLine{{}, {"aaa", "bbb", "cc"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines[0].out}, {"bar", &synonyms_lines[0].out},
        {"aaa", &synonyms_lines[1].out}, {"bbb", &synonyms_lines[1].out},
        {"cc", &synonyms_lines[1].out},
      };
      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0(
      "foo,bar=>foo,bar\n\n#some comment\naaa, bbb, cc => aaa, bbb, cc");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
        SynonymsTokenizer::SynonymsLine{{"aaa", "bbb", "cc"},
                                        {"aaa", "bbb", "cc"}}};
      ASSERT_EQ(expected, synonyms_lines);
    }

    {
      const auto actual = SynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SynonymsTokenizer::SynonymsMap expected = {
        {"foo", &synonyms_lines[0].out}, {"bar", &synonyms_lines[0].out},
        {"aaa", &synonyms_lines[1].out}, {"bbb", &synonyms_lines[1].out},
        {"cc", &synonyms_lines[1].out},
      };

      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      const auto actual = SynonymsTokenizer::Parse(synonyms_lines);
      ASSERT_TRUE(actual);

      SynonymsTokenizer::SynonymsMap expected = {};

      ASSERT_EQ(expected, *actual);
    }
  }

  {
    std::string_view data0("aaa, bbb, cc => => aaa, bbb, cc");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(),
              "More than one explicit mapping specified on the line 1");
  }

  {
    std::string_view data0("aaa,");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("aaa=>");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("aaa,=>aaa");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("aaa,bbb=>aaa,");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }
  {
    std::string_view data0("\n#aa\naaa,,bbb=>aaa,bbb");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 3");
  }

  {
    std::string_view data0("foo,bar,foo");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }

  {
    std::string_view data0("foo,bar,foo=>foo,bar");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }

  {
    std::string_view data0("foo,bar,foo=>foo,bar");
    auto result = SynonymsTokenizer::ParseSynonymsLines(data0);
    ASSERT_TRUE(result);
    const auto synonyms_lines = *result;
    {
      SynonymsTokenizer::SynonymsLines expected{
        SynonymsTokenizer::SynonymsLine{{"bar", "foo"}, {"bar", "foo"}},
      };
      ASSERT_EQ(expected, synonyms_lines);
    }
  }
}
