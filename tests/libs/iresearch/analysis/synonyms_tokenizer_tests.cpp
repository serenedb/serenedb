#include <iresearch/analysis/synonyms_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>

#include "gtest/gtest.h"

TEST(token_synonyms_stream_tests, consts) {
  static_assert("synonyms" ==
                irs::Type<irs::analysis::SynonymsTokenizer>::name());
}

TEST(token_synonyms_stream_tests, test_masking) {
  // test mask nothing
  {
    std::string_view data0("abc");
    std::string_view data1("ghi");
    irs::analysis::SynonymsTokenizer::synonyms_map mask;
    irs::analysis::SynonymsTokenizer stream(std::move(mask));
    ASSERT_EQ(irs::Type<irs::analysis::SynonymsTokenizer>::id(), stream.type());

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
}