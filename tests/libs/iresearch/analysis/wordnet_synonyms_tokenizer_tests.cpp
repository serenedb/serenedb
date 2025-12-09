#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>
#include <stdexcept>

#include "gtest/gtest.h"

using WordnetSynonymsTokenizer = irs::analysis::WordnetSynonymsTokenizer;

TEST(wordnet_synonyms_tests, consts) {
  static_assert("wordnet_synonyms" ==
                irs::Type<WordnetSynonymsTokenizer>::name());
}

TEST(wordnet_synonyms_tests, test_masking) {
  {
    std::string data0 = "come";
    WordnetSynonymsTokenizer::SynonymsGroups group;
    WordnetSynonymsTokenizer::SynonymsMap mapping;

    WordnetSynonymsTokenizer stream(std::move(group), std::move(mapping));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_FALSE(stream.next());
  }

  {
    std::string data0 = "come";
    std::string data1 = "advance";
    WordnetSynonymsTokenizer::SynonymsGroups group{"100000002"};
    WordnetSynonymsTokenizer::SynonymsMap mapping{{"come", "100000002"}};

    WordnetSynonymsTokenizer stream(std::move(group), std::move(mapping));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));
    ASSERT_FALSE(stream.next());
  }

  {
    std::string data0 = "come";
    std::string data1 = "advance";
    std::string data2 = "approach";
    WordnetSynonymsTokenizer::SynonymsGroups group{"100000002"};
    WordnetSynonymsTokenizer::SynonymsMap mapping{
      {"come", "100000002"},
      {"advance", "100000002"},
      {"approach", "100000002"},
    };

    WordnetSynonymsTokenizer stream(std::move(group), std::move(mapping));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    auto* offset = irs::get<irs::OffsAttr>(stream);
    auto* term = irs::get<irs::TermAttr>(stream);
    auto* inc = irs::get<irs::IncAttr>(stream);

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(4, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data1));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(7, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());

    ASSERT_TRUE(stream.reset(data2));
    ASSERT_TRUE(stream.next());
    ASSERT_EQ(0, offset->start);
    ASSERT_EQ(8, offset->end);
    ASSERT_EQ(1, inc->value);
    ASSERT_EQ("100000002", irs::ViewCast<char>(term->value));
    ASSERT_FALSE(stream.next());
  }
}

TEST(wordnet_synonyms_tests, parsing) {
  {
    std::string_view data0("");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result);
    const auto input = *result;
    {
      WordnetSynonymsTokenizer::SynonymsGroups expected{};
      ASSERT_EQ(expected, input.groups);
    }
    {
      WordnetSynonymsTokenizer::SynonymsMap expected{};
      ASSERT_EQ(expected, input.mapping);
    }
  }

  {
    std::string_view data0("s(100000002,1,'come',v,1,0).");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result);
    const auto input = *result;
    {
      WordnetSynonymsTokenizer::SynonymsGroups expected{"100000002"};
      ASSERT_EQ(expected, input.groups);
    }
    {
      WordnetSynonymsTokenizer::SynonymsMap expected{{"come", input.groups[0]}};
      ASSERT_EQ(expected, input.mapping);
    }
  }

  {
    std::string_view data0(
      "s(100000002,1,'come',v,1,0).\ns(100000002,2,'advance',v,1,0).\n\ns("
      "100000002,3,'approach',v,1,0).\ns(100000003,1,'release',v,1,0).");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result);
    const auto input = *result;
    {
      WordnetSynonymsTokenizer::SynonymsGroups expected{"100000002",
                                                        "100000003"};
      ASSERT_EQ(expected, input.groups);
    }
    {
      WordnetSynonymsTokenizer::SynonymsMap expected{
        {"come", input.groups[0]},
        {"advance", input.groups[0]},
        {"approach", input.groups[0]},
        {"release", input.groups[1]},
      };
      ASSERT_EQ(expected, input.mapping);
    }
  }

  {
    std::string_view data0("go");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("s(100000002,1,come,v,1,0).");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("s(100000002,1,'come,v,1,0).");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("s(100000002,1,come',v,1,0).");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("s(100000002,1,'',v,1,0).");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 1");
  }

  {
    std::string_view data0("a");
    ASSERT_THROW(WordnetSynonymsTokenizer::Parse(data0), std::out_of_range);
  }

  {
    std::string_view data0("s(100000002,1,'come',v,1,0).\nasd");
    auto result = WordnetSynonymsTokenizer::Parse(data0);
    ASSERT_TRUE(result.error().is(sdb::ERROR_VALIDATION_BAD_PARAMETER));
    ASSERT_EQ(result.error().errorMessage(), "Failed parse line 2");
  }
}
