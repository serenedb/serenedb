#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/wordnet_synonyms_tokenizer.hpp>

#include "gtest/gtest.h"

using WordnetSynonymsTokenizer = irs::analysis::WordnetSynonymsTokenizer;

TEST(wordnet_synonyms_tests, consts) {
  static_assert("wordnet_synonyms" ==
                irs::Type<WordnetSynonymsTokenizer>::name());
}

TEST(wordnet_synonyms_tests, test_masking) {
  {
    std::string data0 = "come";
    WordnetSynonymsTokenizer::SynonymsGroup group;
    WordnetSynonymsTokenizer::SynonymsMap mapping;

    WordnetSynonymsTokenizer stream(std::move(group), std::move(mapping));
    ASSERT_EQ(irs::Type<WordnetSynonymsTokenizer>::id(), stream.type());

    ASSERT_TRUE(stream.reset(data0));
    ASSERT_FALSE(stream.next());
  }

  {
    std::string data0 = "come";
    std::string data1 = "advance";
    WordnetSynonymsTokenizer::SynonymsGroup group{"100000002"};
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
    WordnetSynonymsTokenizer::SynonymsGroup group{"100000002"};
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
