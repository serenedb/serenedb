////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "iresearch/analysis/icu_text_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "test_resources.hpp"
#include "tests_config.hpp"
#include "token_sink_utils.hpp"

namespace {

using namespace irs;
using namespace irs::analysis;

Tokenizer::ptr MakeTextEn() {
  TextTokenizer::Options o;
  o.locale = icu::Locale::createFromName("en_US.UTF-8");
  o.explicit_stopwords = {"the", "and", "of", "a"};
  o.explicit_stopwords_set = true;
  return TextTokenizer::Make(std::move(o), tests::Cache());
}

Tokenizer::ptr MakePipelineTextEn(
  const icu::Locale& seg_locale = irs::MakeBogusLocale()) {
  using Convert = SegmentationTokenizer::Options::Convert;
  const bool icu = !seg_locale.isBogus();
  std::vector<Tokenizer::ptr> subs;
  if (icu) {
    subs.push_back(IcuTextTokenizer::Make({.locale = seg_locale}));
  } else {
    subs.push_back(SegmentationTokenizer::Make({.convert = Convert::Lower}));
  }
  {
    NormalizingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    o.case_convert = icu ? Case::Lower : Case::None;
    o.accent = false;
    subs.push_back(NormalizingTokenizer::Make(std::move(o)));
  }
  {
    StopwordsTokenizer::Options s;
    s.mask = {"the", "and", "of", "a"};
    subs.push_back(StopwordsTokenizer::Make(std::move(s), tests::Cache()));
  }
  {
    StemmingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    subs.push_back(StemmingTokenizer::Make(std::move(o)));
  }
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakeTextEnNGram(size_t min_gram, size_t max_gram,
                               bool preserve_original) {
  TextTokenizer::Options o;
  o.locale = icu::Locale::createFromName("en_US.UTF-8");
  o.explicit_stopwords = {"the", "and", "of", "a"};
  o.explicit_stopwords_set = true;
  o.min_gram = min_gram;
  o.min_gram_set = true;
  o.max_gram = max_gram;
  o.max_gram_set = true;
  o.preserve_original = preserve_original;
  o.preserve_original_set = true;
  return TextTokenizer::Make(std::move(o), tests::Cache());
}

Tokenizer::ptr MakePipelineTextEnNGram(size_t min_gram, size_t max_gram,
                                       bool preserve_original) {
  using Convert = SegmentationTokenizer::Options::Convert;
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(SegmentationTokenizer::Make({.convert = Convert::Lower}));
  {
    NormalizingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    o.case_convert = Case::None;
    o.accent = false;
    subs.push_back(NormalizingTokenizer::Make(std::move(o)));
  }
  {
    StopwordsTokenizer::Options s;
    s.mask = {"the", "and", "of", "a"};
    subs.push_back(StopwordsTokenizer::Make(std::move(s), tests::Cache()));
  }
  {
    StemmingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    subs.push_back(StemmingTokenizer::Make(std::move(o)));
  }
  {
    NGramTokenizerBase::Options o;
    o.min_gram = min_gram;
    o.max_gram = max_gram;
    o.preserve_original = preserve_original;
    o.stream_bytes_type = NGramTokenizerBase::InputType::UTF8;
    o.ngram_mode = NGramTokenizerBase::NGramMode::Prefix;
    subs.push_back(NGramTokenizerBase::Make(std::move(o)));
  }
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

const std::vector<std::string>& Corpus() {
  static const std::vector<std::string> corpus = {
    "The quick brown fox jumps over the lazy dog",
    "the and of a",
    "",
    "...!!!   ",
    "running jumped stemming wizards brewing potions",
    "internationalization \xD1\x85\xD0\xB0\xD1\x80\xD0\xB0\xD0\xBA\xD1\x82"
    "\xD0\xB5\xD1\x80\xD0\xB8\xD1\x81\xD1\x82\xD0\xB8\xD0\xBA\xD0\xB0",
    "extraordinarily counterrevolutionaries antidisestablishmentarianism",
    "don't stop M.I.T. 3.14 1'000 wizard's",
    "Cafe\xCC\x81 nai\xCC\x88ve",
    "caf\xC3\xA9 na\xC3\xAFve r\xC3\xA9sum\xC3\xA9",
    "\xCF\x89\xCE\xB1\xCE\xB2 \xCE\x93\xCE\x94\xCE\x95 \xCE\xA9\xCE\x9C",
    "MiXeD CaSe WORDS lower UPPER Title",
    "a1b2c3 12345 0x1F 42nd",
    "line1\r\nline2\ttab  spaces",
    "the caf\xC3\xA9 of a wizard and the r\xC3\xA9sum\xC3\xA9",
    "\xED\x95\x9C\xEA\xB5\xAD\xEC\x96\xB4 \xED\x85\x8C\xEC\x8A\xA4\xED\x8A"
    "\xB8",
  };
  return corpus;
}

// Unspaced scripts: text always uses ICU's dictionary-based breaks; the
// default segmentation tokenizer is pure UAX#29 (divergence by POLICY, not
// bug), while a locale-set segmentation routes these through ICU and must
// match text exactly.
const std::vector<std::string>& DictScriptCorpus() {
  static const std::vector<std::string> corpus = {
    "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E\xE3\x81\xAE\xE3\x83\x86\xE3\x82"
    "\xAD\xE3\x82\xB9\xE3\x83\x88 \xE4\xB8\xAD\xE6\x96\x87\xE6\xB5\x8B"
    "\xE8\xAF\x95",
    "\xE0\xB8\xA0\xE0\xB8\xB2\xE0\xB8\xA9\xE0\xB8\xB2\xE0\xB9\x84\xE0\xB8"
    "\x97\xE0\xB8\xA2\xE0\xB8\x97\xE0\xB8\x94\xE0\xB8\xAA\xE0\xB8\xAD"
    "\xE0\xB8\x9A latin",
    "mixed\xE6\xBC\xA2字ascii words",
  };
  return corpus;
}

const std::vector<std::string>& InvalidUtf8Corpus() {
  static const std::vector<std::string> corpus = {
    "broken\xFF\xFE"
    "bytes \xC3(truncated \xE2\x82"
    "incomplete",
  };
  return corpus;
}

constexpr TokenLayout kLayouts[] = {
  TokenLayout::Terms,
  TokenLayout::TermsPos,
  TokenLayout::TermsPosOffs,
};

// allow_wider_offs_end: in ngram mode, text stamps offs_end as
// offs_start + gram bytes even for grams spanning the whole stemmed term,
// cutting mid-word ("running" -> "run" gram ends at word_start + 3); the
// pipeline's Remap maps whole-term grams to the true word end. The wider
// pipeline span is the intended semantics; only that widening is tolerated.
void ExpectTokensEq(const std::vector<tests::AnalyzerToken>& expect,
                    const std::vector<tests::AnalyzerToken>& got,
                    TokenLayout layout, bool allow_wider_offs_end = false) {
  ASSERT_EQ(expect.size(), got.size());
  for (size_t i = 0; i < expect.size(); ++i) {
    SCOPED_TRACE(testing::Message() << "token " << i);
    EXPECT_EQ(expect[i].term, got[i].term);
    if (layout != TokenLayout::Terms) {
      EXPECT_EQ(expect[i].pos, got[i].pos);
    }
    if (layout == TokenLayout::TermsPosOffs) {
      EXPECT_EQ(expect[i].offs_start, got[i].offs_start);
      if (allow_wider_offs_end && got[i].offs_end != expect[i].offs_end) {
        EXPECT_GT(got[i].offs_end, expect[i].offs_end);
      } else {
        EXPECT_EQ(expect[i].offs_end, got[i].offs_end);
      }
    }
  }
}

TEST(TextPipelineEquivalenceTest, per_value_streams_match) {
  auto text = MakeTextEn();
  auto pipe = MakePipelineTextEn();
  for (const auto layout : kLayouts) {
    for (const auto& v : Corpus()) {
      SCOPED_TRACE(testing::Message()
                   << "layout=" << static_cast<int>(layout) << " value=" << v);
      const auto expect = tests::Analyze(*text, v, layout);
      const auto got = tests::Analyze(*pipe, v, layout);
      ASSERT_EQ(expect.has_value(), got.has_value());
      if (!expect.has_value()) {
        continue;
      }
      ExpectTokensEq(*expect, *got, layout);
    }
  }
}

TEST(TextPipelineEquivalenceTest, ngram_streams_match) {
  struct Config {
    size_t min_gram;
    size_t max_gram;
    bool preserve_original;
  };
  constexpr Config kConfigs[] = {
    {2, 4, true},
    {2, 4, false},
    {3, 3, true},
    {1, 8, false},
  };
  for (const auto& cfg : kConfigs) {
    auto text =
      MakeTextEnNGram(cfg.min_gram, cfg.max_gram, cfg.preserve_original);
    auto pipe = MakePipelineTextEnNGram(cfg.min_gram, cfg.max_gram,
                                        cfg.preserve_original);
    for (const auto layout : kLayouts) {
      for (const auto& v : Corpus()) {
        SCOPED_TRACE(testing::Message()
                     << "min=" << cfg.min_gram << " max=" << cfg.max_gram
                     << " orig=" << cfg.preserve_original << " layout="
                     << static_cast<int>(layout) << " value=" << v);
        const auto expect = tests::Analyze(*text, v, layout);
        const auto got = tests::Analyze(*pipe, v, layout);
        ASSERT_EQ(expect.has_value(), got.has_value());
        if (!expect.has_value()) {
          continue;
        }
        ExpectTokensEq(*expect, *got, layout, true);
      }
    }
  }
}

TEST(TextPipelineEquivalenceTest, loaded_stopwords_match_text) {
  const auto dir =
    std::filesystem::temp_directory_path() / "sdb_equiv_stopwords" / "en";
  std::filesystem::create_directories(dir);
  {
    std::ofstream out{dir / "list"};
    out << "the\nand\nof\na\nover\nunder\n";
  }
  const std::string root = dir.parent_path().string();

  TextTokenizer::Options to;
  to.locale = icu::Locale::createFromName("en_US.UTF-8");
  to.stopwords_path = root;
  auto text = TextTokenizer::Make(std::move(to), tests::Cache());

  using Convert = SegmentationTokenizer::Options::Convert;
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(SegmentationTokenizer::Make({.convert = Convert::Lower}));
  {
    NormalizingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    o.case_convert = Case::None;
    o.accent = false;
    subs.push_back(NormalizingTokenizer::Make(std::move(o)));
  }
  {
    StopwordsTokenizer::Options s;
    s.stopwords_path = dir.string();
    subs.push_back(StopwordsTokenizer::Make(std::move(s), tests::Cache()));
  }
  {
    StemmingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    subs.push_back(StemmingTokenizer::Make(std::move(o)));
  }
  auto pipe = std::make_unique<PipelineTokenizer>(std::move(subs));

  for (const auto layout : kLayouts) {
    for (const auto& v : Corpus()) {
      SCOPED_TRACE(testing::Message()
                   << "layout=" << static_cast<int>(layout) << " value=" << v);
      const auto expect = tests::Analyze(*text, v, layout);
      const auto got = tests::Analyze(*pipe, v, layout);
      ASSERT_EQ(expect.has_value(), got.has_value());
      if (!expect.has_value()) {
        continue;
      }
      ExpectTokensEq(*expect, *got, layout);
    }
  }
}

TEST(TextPipelineEquivalenceTest, locale_segmentation_matches_text) {
  auto text = MakeTextEn();
  auto pipe = MakePipelineTextEn(icu::Locale::createFromName("en_US.UTF-8"));
  auto values = Corpus();
  const auto& dict = DictScriptCorpus();
  values.insert(values.end(), dict.begin(), dict.end());
  for (const auto layout : kLayouts) {
    for (const auto& v : values) {
      SCOPED_TRACE(testing::Message()
                   << "layout=" << static_cast<int>(layout) << " value=" << v);
      const auto expect = tests::Analyze(*text, v, layout);
      const auto got = tests::Analyze(*pipe, v, layout);
      ASSERT_EQ(expect.has_value(), got.has_value());
      if (!expect.has_value()) {
        continue;
      }
      ExpectTokensEq(*expect, *got, layout);
    }
  }
}

TEST(TextPipelineEquivalenceTest, icu_sentence_segmentation) {
  using Separate = SegmentationTokenizer::Options::Separate;
  using Accept = SegmentationTokenizer::Options::Accept;
  using Convert = SegmentationTokenizer::Options::Convert;
  auto def = SegmentationTokenizer::Make({.separate = Separate::Sentence,
                                          .accept = Accept::Any,
                                          .convert = Convert::None});
  auto icu = IcuTextTokenizer::Make(
    {.separate = IcuTextTokenizer::Options::Separate::Sentence,
     .accept = Accept::Any,
     .locale = icu::Locale::createFromName("en_US.UTF-8")});
  const std::vector<std::string> values = {
    "Hello world. Second sentence! And a third one?",
    "One sentence only",
    "",
  };
  for (const auto& v : values) {
    SCOPED_TRACE(testing::Message() << "value=" << v);
    const auto expect = tests::Analyze(*def, v, TokenLayout::TermsPosOffs);
    const auto got = tests::Analyze(*icu, v, TokenLayout::TermsPosOffs);
    ASSERT_TRUE(expect.has_value());
    ASSERT_TRUE(got.has_value());
    ExpectTokensEq(*expect, *got, TokenLayout::TermsPosOffs);
  }
}

TEST(TextPipelineEquivalenceTest, known_segmentation_divergence) {
  auto text = MakeTextEn();
  auto pipe = MakePipelineTextEn();
  auto divergent = DictScriptCorpus();
  const auto& invalid = InvalidUtf8Corpus();
  divergent.insert(divergent.end(), invalid.begin(), invalid.end());
  for (const auto& v : divergent) {
    SCOPED_TRACE(testing::Message() << "value=" << v);
    const auto expect = tests::Analyze(*text, v, TokenLayout::Terms);
    const auto got = tests::Analyze(*pipe, v, TokenLayout::Terms);
    ASSERT_TRUE(expect.has_value());
    ASSERT_TRUE(got.has_value());
    EXPECT_FALSE(expect->empty());
    EXPECT_FALSE(got->empty());
    EXPECT_NE(*expect, *got);
  }
}

std::vector<std::string> ColumnCorpus() {
  auto values = Corpus();
  const auto& dict = DictScriptCorpus();
  values.insert(values.end(), dict.begin(), dict.end());
  const auto& invalid = InvalidUtf8Corpus();
  values.insert(values.end(), invalid.begin(), invalid.end());
  std::string giant;
  for (size_t i = 0; i < 3000; ++i) {
    giant += "wizard";
    giant += std::to_string(i % 97);
    giant += ' ';
  }
  values.push_back(std::move(giant));
  for (size_t i = 0; i < 1100; ++i) {
    values.push_back("moonlight" + std::to_string(i % 89));
  }
  return values;
}

TEST(TextPipelineEquivalenceTest, pipeline_column_matches_per_value) {
  auto pipe = MakePipelineTextEn();
  const auto values = ColumnCorpus();
  std::vector<duckdb::string_t> handles;
  handles.reserve(values.size());
  for (const auto& v : values) {
    handles.push_back(tests::ToStringT(v));
  }
  constexpr doc_id_t kFirstDoc = 1;
  for (const auto layout : kLayouts) {
    SCOPED_TRACE(testing::Message() << "layout=" << static_cast<int>(layout));
    std::map<doc_id_t, std::vector<tests::AnalyzerToken>> by_doc;
    doc_id_t open_doc = doc_limits::invalid();
    uint32_t dense_pos = 0;
    tests::FnTokenSink sink{
      layout, [&](TokenBatch& batch, DocRuns runs) {
        uint32_t base = 0;
        for (const auto& run : runs) {
          if (run.doc != open_doc) {
            open_doc = run.doc;
            dense_pos = 0;
          }
          auto& out = by_doc[run.doc];
          for (uint32_t i = base; i < base + run.ntokens; ++i) {
            const auto& t = batch.terms[i];
            out.push_back(
              {std::string{t.GetData(), t.GetSize()}, ++dense_pos,
               layout == TokenLayout::TermsPosOffs ? batch.offs_start[i] : 0,
               layout == TokenLayout::TermsPosOffs ? batch.offs_end[i] : 0});
          }
          base += run.ntokens;
        }
      }};
    tests::FillColumn(*pipe, handles, kFirstDoc, sink.writer, layout);
    sink.writer.Finish();
    for (size_t i = 0; i < values.size(); ++i) {
      SCOPED_TRACE(testing::Message() << "value " << i);
      const auto expect = tests::Analyze(*pipe, values[i], layout);
      ASSERT_TRUE(expect.has_value());
      const auto it = by_doc.find(kFirstDoc + static_cast<doc_id_t>(i));
      const auto& got =
        it == by_doc.end() ? std::vector<tests::AnalyzerToken>{} : it->second;
      ExpectTokensEq(*expect, got, layout);
    }
  }
}

TEST(TextPipelineEquivalenceTest, text_column_matches_pipeline_column) {
  auto text = MakeTextEn();
  auto pipe = MakePipelineTextEn();
  const auto& values = Corpus();
  std::vector<duckdb::string_t> handles;
  handles.reserve(values.size());
  for (const auto& v : values) {
    handles.push_back(tests::ToStringT(v));
  }
  for (const auto layout : kLayouts) {
    SCOPED_TRACE(testing::Message() << "layout=" << static_cast<int>(layout));
    const auto collect = [&](Tokenizer& a) {
      std::vector<std::pair<doc_id_t, tests::AnalyzerToken>> out;
      tests::FnTokenSink sink{
        layout, [&](TokenBatch& batch, DocRuns runs) {
          uint32_t base = 0;
          for (const auto& run : runs) {
            for (uint32_t i = base; i < base + run.ntokens; ++i) {
              const auto& t = batch.terms[i];
              out.push_back(
                {run.doc,
                 {std::string{t.GetData(), t.GetSize()}, 0,
                  layout == TokenLayout::TermsPosOffs ? batch.offs_start[i] : 0,
                  layout == TokenLayout::TermsPosOffs ? batch.offs_end[i]
                                                      : 0}});
            }
            base += run.ntokens;
          }
        }};
      tests::FillColumn(a, handles, 1, sink.writer, layout);
      sink.writer.Finish();
      return out;
    };
    const auto expect = collect(*text);
    const auto got = collect(*pipe);
    ASSERT_EQ(expect.size(), got.size());
    for (size_t i = 0; i < expect.size(); ++i) {
      SCOPED_TRACE(testing::Message() << "token " << i);
      EXPECT_EQ(expect[i].first, got[i].first);
      EXPECT_EQ(expect[i].second.term, got[i].second.term);
      if (layout == TokenLayout::TermsPosOffs) {
        EXPECT_EQ(expect[i].second.offs_start, got[i].second.offs_start);
        EXPECT_EQ(expect[i].second.offs_end, got[i].second.offs_end);
      }
    }
  }
}

}  // namespace
