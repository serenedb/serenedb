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

#include "tokenizer_fuzz_specs.hpp"

#include <duckdb.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/analysis/geo_tokenizer.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <utility>

#include "test_resources.hpp"
#include "tests_shared.hpp"
#include "text_chain.hpp"
#include "tokenizer_fuzz_corpus.hpp"

namespace tests::fuzz {
namespace {

using namespace irs::analysis;

using Cfg = irs::analysis::TokenizerConfig;
using Ptr = irs::analysis::Tokenizer::ptr;

duckdb::ClientContext& Context() {
  static thread_local auto* conn =
    new duckdb::Connection{irs::DuckDBEngine::Instance().instance()};
  return *conn->context;
}

irs::bstring Bytes(std::string_view s) {
  return irs::bstring{reinterpret_cast<const irs::byte_type*>(s.data()),
                      s.size()};
}

std::unique_ptr<Cfg> Child(Cfg cfg) {
  return std::make_unique<Cfg>(std::move(cfg));
}

Ptr MakeChild(Cfg cfg) {
  return irs::analysis::CreateTokenizer(std::move(cfg), ::tests::Cache());
}

icu::Locale Loc(const char* name) { return icu::Locale::createFromName(name); }

Cfg TextChainCfg(const char* locale, irs::Case convert, bool stemming,
                 std::vector<std::string> stopwords = {}) {
  return ::tests::TextChainConfig({.locale = locale,
                                   .convert = convert,
                                   .stemming = stemming,
                                   .stopwords = std::move(stopwords)});
}

std::vector<Ptr> ChainModel(Cfg cfg) {
  std::vector<Ptr> subs;
  for (auto& child :
       std::get<PipelineTokenizer::Options>(cfg.config).children) {
    subs.push_back(MakeChild(std::move(*child)));
  }
  return subs;
}

std::string ModelLocation() {
  return TestEnv::resource("model_cooking.bin").string();
}

bool HasModel() {
  std::error_code ec;
  return std::filesystem::exists(ModelLocation(), ec);
}

const std::filesystem::path& FixtureDir() {
  static const std::filesystem::path kDir = [] {
    const auto dir =
      std::filesystem::path{::testing::TempDir()} / "sdb_tokenizer_fuzz";
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
    std::filesystem::create_directories(dir / "text_stopwords" / "en");
    std::filesystem::create_directories(dir / "words");
    {
      std::ofstream out{dir / "stopwords.txt"};
      out << "the\na\nan\nof\nand\nquick\n";
    }
    {
      std::ofstream out{dir / "words" / "list.txt"};
      out << "the\na\nan\nof\nand\n";
    }
    {
      std::ofstream out{dir / "text_stopwords" / "en" / "list.txt"};
      out << "the\na\nof\nand\n";
    }
    return dir;
  }();
  return kDir;
}

std::string StopwordsFile() {
  return (FixtureDir() / "stopwords.txt").string();
}

constexpr std::string_view kSolrSynonyms =
  "i-pod, i pod, ipod\n"
  "sea biscuit, sea biscit => seabiscuit\n"
  "gb => gib, gigabyte\n"
  "fast,quick,speedy\n";

constexpr std::string_view kWordnetSynonyms =
  "s(100000001,1,'angry',a,1,0).\n"
  "s(100000001,2,'furious',a,1,0).\n"
  "s(100000001,3,'mad',a,1,0).\n"
  "s(100000002,1,'happy',a,1,0).\n"
  "s(100000002,2,'glad',a,1,0).\n"
  "s(100000003,1,'come',v,1,0).\n";

const std::vector<std::string>& StopwordDict() {
  static const std::vector<std::string> kWords = {
    "the", "a", "an", "of", "and", "616263", "646566", "6D6e6F", "abc", "mnO"};
  return kWords;
}

const std::vector<std::string>& WordDict() {
  static const std::vector<std::string> kWords = {
    "running", "runner", "ran",    "the",    "quick",      "brown", "Straße",
    "STRASSE", "café",   "ipod",   "i-pod",  "i pod",      "gb",    "angry",
    "furious", "come",   "baking", "knives", "sea biscuit"};
  return kWords;
}

const std::vector<std::string>& BreakDict() {
  static const std::vector<std::string> kWords = {
    ". ",   "! ",     "? ",       "\n\n",    "\r\n\r\n", " ",
    " ",  "Mr.",    "e.g.",     "3.14",    "don't",    "co-op",
    "中文", "テスト", "แบบทดสอบ", "العربية", "ㄱ"};
  return kWords;
}

const std::vector<std::string>& GeoDict() {
  static const std::vector<std::string> kWords = {"type",
                                                  "coordinates",
                                                  "Point",
                                                  "Polygon",
                                                  "LineString",
                                                  "MultiPoint",
                                                  "geometries",
                                                  "lat",
                                                  "lng",
                                                  "location",
                                                  "[",
                                                  "]",
                                                  "{",
                                                  "}",
                                                  ":",
                                                  "\"type\":\"Point\"",
                                                  "[13.4,52.5]",
                                                  "1e400",
                                                  "-0.0",
                                                  "NaN",
                                                  "\x01\x01\x00\x00\x00"};
  return kWords;
}

std::vector<std::string> Merge(std::vector<std::vector<std::string>> parts) {
  std::vector<std::string> out;
  for (auto& part : parts) {
    for (auto& w : part) {
      out.push_back(std::move(w));
    }
  }
  return out;
}

void Add(std::vector<Spec>& out, Spec spec) { out.push_back(std::move(spec)); }

void AddKeyword(std::vector<Spec>& out) {
  Add(out, {.name = "keyword",
            .config = [] { return Cfg{irs::KeywordTokenizer::Options{}}; },
            .model = Model::Keyword});
}

void AddDelimited(std::vector<Spec>& out) {
  for (const auto* delim : {",", ";", "\t", "::", "--", "", "abc"}) {
    const std::string_view d{delim};
    Spec spec{.name = std::string{"delimiter["} + delim + "]",
              .config =
                [delim] {
                  return Cfg{DelimitedTokenizer::Options{.delimiter = delim}};
                },
              .dict = {std::string{d}, std::string{d} + std::string{d}, "\"",
                       "\"\"", "\"a,b\"", "a\"b"},
              .native = CsvValues()};
    if (d.size() == 1) {
      spec.model = Model::SplitChar;
      spec.params.delim = d[0];
    }
    Add(out, std::move(spec));
  }
}

void AddMultiDelimited(std::vector<Spec>& out) {
  Add(out, {.name = "multi_delimiter[,;|]",
            .config =
              [] {
                return Cfg{MultiDelimitedTokenizer::Options{
                  .delimiters = {Bytes(","), Bytes(";"), Bytes("|")}}};
              },
            .dict = {",", ";", "|", ",;|", ",,"},
            .native = CsvValues(),
            .model = Model::RunsOfSet,
            .params = {.token_bytes = ByteSet(",;|", true)}});
  Add(out, {.name = "multi_delimiter[5chars]",
            .config =
              [] {
                return Cfg{MultiDelimitedTokenizer::Options{
                  .delimiters = {Bytes(";"), Bytes(","), Bytes("|"), Bytes("."),
                                 Bytes(":")}}};
              },
            .dict = {";", ",", "|", ".", ":", "a:b.c"},
            .model = Model::RunsOfSet,
            .params = {.token_bytes = ByteSet(";,|.:", true)}});
  Add(out, {.name = "multi_delimiter[::--<>]",
            .config =
              [] {
                return Cfg{MultiDelimitedTokenizer::Options{
                  .delimiters = {Bytes("::"), Bytes("--"), Bytes("<>")}}};
              },
            .dict = {"::", "--", "<>", ":", "-", "<", ">", ":-", "<-"}});
  Add(out, {.name = "multi_delimiter[long]",
            .config =
              [] {
                return Cfg{MultiDelimitedTokenizer::Options{
                  .delimiters = {Bytes("foo"), Bytes("barbaz"), Bytes("<tag>"),
                                 Bytes("</tag>")}}};
              },
            .dict = {"foo", "barbaz", "<tag>", "</tag>", "fo", "ba", "barba",
                     "<tag", "</ta", "fooo"}});
  Add(out, {.name = "multi_delimiter[empty]",
            .config = [] { return Cfg{MultiDelimitedTokenizer::Options{}}; }});
}

void AddPattern(std::vector<Spec>& out) {
  Add(out, {.name = "pattern[split_ws]",
            .config =
              [] {
                return Cfg{
                  PatternTokenizer::Options{.pattern = "\\s+", .group = -1}};
              },
            .dict = {" ", "\t", "\n", "\r", "\f", "\v", "  "}});
  Add(out, {.name = "pattern[split_nonword]",
            .config =
              [] {
                return Cfg{PatternTokenizer::Options{.pattern = "[^a-zA-Z0-9]+",
                                                     .group = -1}};
              },
            .dict = {"_", "-", ".", " ", "0", "a", "Z"},
            .model = Model::RunsOfSet,
            .params = {.token_bytes = AlnumBytes(), .ascii_only = true}});
  Add(out, {.name = "pattern[match_words]",
            .config =
              [] {
                return Cfg{PatternTokenizer::Options{.pattern = "[a-zA-Z]+",
                                                     .group = 0}};
              },
            .dict = {"a", "Z", "0", "_", " "},
            .model = Model::RunsOfSet,
            .params = {.token_bytes = ByteSet("abcdefghijklmnopqrstuvwxyz"
                                              "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                       .ascii_only = true}});
  Add(out, {.name = "pattern[group1]",
            .config =
              [] {
                return Cfg{PatternTokenizer::Options{.pattern = "(\\w+)@(\\w+)",
                                                     .group = 1}};
              },
            .dict = {"@", "a@b", "@@", "a@", "@b", "_"}});
  Add(out, {.name = "pattern[group2]",
            .config =
              [] {
                return Cfg{PatternTokenizer::Options{.pattern = "(\\w+)@(\\w+)",
                                                     .group = 2}};
              },
            .dict = {"@", "a@b", "@@", "a@", "@b", "_"}});
  Add(out,
      {.name = "pattern[empty_match]",
       .config =
         [] {
           return Cfg{PatternTokenizer::Options{.pattern = "a*", .group = 0}};
         },
       .dict = {"a", "aa", "b", ""}});
  Add(out, {.name = "pattern[utf8_class]",
            .config =
              [] {
                return Cfg{PatternTokenizer::Options{.pattern = "[[:alpha:]]+",
                                                     .group = 0}};
              },
            .dict = {"a", "Z", "0", "é", "中"}});
  Add(out, {.name = "pattern[anchored_alt]",
            .config =
              [] {
                return Cfg{PatternTokenizer::Options{
                  .pattern = "(foo|barbaz|q)", .group = 1}};
              },
            .dict = {"foo", "barbaz", "q", "fo", "barba", "fooo"}});
}

void AddPathHierarchy(std::vector<Spec>& out) {
  const std::vector<std::string> dict = {"/", "//", "..",   ".",
                                         "|", "::", "/a/b", "a/"};
  Add(out, {.name = "path_hierarchy[default]",
            .config = [] { return Cfg{PathHierarchyTokenizer::Options{}}; },
            .dict = dict,
            .native = PathValues()});
  Add(out,
      {.name = "path_hierarchy[reverse]",
       .config =
         [] { return Cfg{PathHierarchyTokenizer::Options{.reverse = true}}; },
       .dict = dict,
       .native = PathValues()});
  Add(out,
      {.name = "path_hierarchy[skip2]",
       .config = [] { return Cfg{PathHierarchyTokenizer::Options{.skip = 2}}; },
       .dict = dict,
       .native = PathValues()});
  Add(out, {.name = "path_hierarchy[reverse_skip1]",
            .config =
              [] {
                return Cfg{
                  PathHierarchyTokenizer::Options{.skip = 1, .reverse = true}};
              },
            .dict = dict,
            .native = PathValues()});
  Add(out, {.name = "path_hierarchy[replace]",
            .config =
              [] {
                return Cfg{PathHierarchyTokenizer::Options{.delimiter = "/",
                                                           .replacement = "|"}};
              },
            .dict = dict,
            .native = PathValues()});
  Add(out, {.name = "path_hierarchy[multichar]",
            .config =
              [] {
                return Cfg{PathHierarchyTokenizer::Options{
                  .delimiter = "::", .replacement = "::"}};
              },
            .dict = dict,
            .native = PathValues()});
  Add(out, {.name = "path_hierarchy[multichar_replace]",
            .config =
              [] {
                return Cfg{PathHierarchyTokenizer::Options{
                  .delimiter = "::", .replacement = "/", .skip = 1}};
              },
            .dict = dict,
            .native = PathValues()});
}

void AddNGram(std::vector<Spec>& out) {
  using Options = NGramTokenizer::Options;
  Add(out, {.name = "ngram[2,3,binary]",
            .config =
              [] {
                return Cfg{Options{
                  .min_gram = 2,
                  .max_gram = 3,
                  .preserve_original = false,
                  .stream_bytes_type = NGramTokenizer::InputType::Binary}};
              },
            .model = Model::NGramBytes,
            .params = {.min_gram = 2, .max_gram = 3},
            .cost = 4});
  Add(out, {.name = "ngram[3,3,binary]",
            .config =
              [] {
                return Cfg{Options{
                  .min_gram = 3,
                  .max_gram = 3,
                  .preserve_original = false,
                  .stream_bytes_type = NGramTokenizer::InputType::Binary}};
              },
            .model = Model::NGramBytes,
            .params = {.min_gram = 3, .max_gram = 3},
            .cost = 2});
  Add(out, {.name = "ngram[1,1,binary]",
            .config =
              [] {
                return Cfg{Options{
                  .min_gram = 1,
                  .max_gram = 1,
                  .preserve_original = false,
                  .stream_bytes_type = NGramTokenizer::InputType::Binary}};
              },
            .model = Model::NGramBytes,
            .params = {.min_gram = 1, .max_gram = 1},
            .cost = 2});
  Add(out, {.name = "ngram[1,5,utf8,preserve]",
            .config =
              [] {
                return Cfg{Options{
                  .min_gram = 1,
                  .max_gram = 5,
                  .preserve_original = true,
                  .stream_bytes_type = NGramTokenizer::InputType::UTF8}};
              },
            .cost = 8});
  Add(out, {.name = "ngram[3,3,markers]",
            .config =
              [] {
                return Cfg{Options{
                  .min_gram = 3,
                  .max_gram = 3,
                  .preserve_original = true,
                  .stream_bytes_type = NGramTokenizer::InputType::Binary,
                  .start_marker = Bytes("^"),
                  .end_marker = Bytes("$")}};
              },
            .dict = {"^", "$", "^$"},
            .cost = 4});
  Add(out,
      {.name = "ngram[2,4,prefix]",
       .config =
         [] {
           return Cfg{Options{.min_gram = 2,
                              .max_gram = 4,
                              .preserve_original = false,
                              .ngram_mode = NGramTokenizer::NGramMode::Prefix}};
         },
       .cost = 2});
  Add(out,
      {.name = "ngram[2,4,suffix]",
       .config =
         [] {
           return Cfg{Options{.min_gram = 2,
                              .max_gram = 4,
                              .preserve_original = false,
                              .ngram_mode = NGramTokenizer::NGramMode::Suffix}};
         },
       .cost = 2});
  Add(out, {.name = "ngram[2,4,prefix_suffix]",
            .config =
              [] {
                return Cfg{Options{
                  .min_gram = 2,
                  .max_gram = 4,
                  .preserve_original = true,
                  .ngram_mode = NGramTokenizer::NGramMode::PrefixAndSuffix}};
              },
            .cost = 2});
  for (const auto input :
       {NGramTokenizer::InputType::Binary, NGramTokenizer::InputType::UTF8}) {
    for (const auto mode :
         {NGramTokenizer::NGramMode::Prefix, NGramTokenizer::NGramMode::Suffix,
          NGramTokenizer::NGramMode::PrefixAndSuffix}) {
      Add(out, {.name = std::string{"ngram[1,3,markers,"} +
                        std::string{magic_enum::enum_name(input)} + "," +
                        std::string{magic_enum::enum_name(mode)} + "]",
                .config =
                  [input, mode] {
                    return Cfg{Options{.min_gram = 1,
                                       .max_gram = 3,
                                       .preserve_original = true,
                                       .stream_bytes_type = input,
                                       .start_marker = Bytes("^"),
                                       .end_marker = Bytes("$"),
                                       .ngram_mode = mode}};
                  },
                .dict = {"^", "$", "^$", "\xC2\xA2", "\xE2\x82\xAC",
                         "\xF0\x9F\x98\x80"},
                .cost = 2});
    }
  }
}

void AddSparseNGram(std::vector<Spec>& out) {
  Add(
    out,
    {.name = "sparse_ngram[4]",
     .config =
       [] { return Cfg{SparseNGramTokenizer::Options{.max_ngram_length = 4}}; },
     .cost = 4});
  Add(out, {.name = "sparse_ngram[16,covering]",
            .config =
              [] {
                return Cfg{SparseNGramTokenizer::Options{.max_ngram_length = 16,
                                                         .covering = true}};
              },
            .cost = 8});
  Add(out, {.name = "sparse_ngram[1]", .config = [] {
              return Cfg{SparseNGramTokenizer::Options{.max_ngram_length = 1}};
            }});
}

void AddSplitByNonAlpha(std::vector<Spec>& out) {
  for (const auto convert :
       {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
    Add(out, {.name = std::string{"split_by_non_alpha["} +
                      std::to_string(static_cast<int>(convert)) + "]",
              .config =
                [convert] {
                  return Cfg{
                    SplitByNonAlphaTokenizer::Options{.case_convert = convert}};
                },
              .dict = {"_", "0", "a", "Z", "-", "İ", "ﬁ"},
              .native = WordValues(),
              .model = Model::RunsOfSet,
              .params = {.token_bytes = AlnumBytes(), .convert = convert}});
  }
}

void AddNormalizing(std::vector<Spec>& out) {
  const std::vector<std::string> dict = {"Straße", "İ", "ı", "ﬁ", "é",
                                         "é",      "ǅ", "①", "㍿"};
  for (const auto convert :
       {irs::Case::None, irs::Case::Lower, irs::Case::Upper}) {
    for (const auto form : {NormForm::Nfc, NormForm::Nfkc}) {
      for (const bool accent : {true, false}) {
        Add(out, {.name = std::string{"norm[case="} +
                          std::to_string(static_cast<int>(convert)) +
                          ",form=" + std::to_string(static_cast<int>(form)) +
                          ",accent=" + (accent ? "1" : "0") + "]",
                  .config =
                    [convert, form, accent] {
                      return Cfg{
                        NormalizingTokenizer::Options{.locale = Loc("en"),
                                                      .case_convert = convert,
                                                      .accent = accent,
                                                      .form = form}};
                    },
                  .dict = dict});
      }
    }
  }
  for (const auto* locale : {"ru_RU.UTF-8", "tr_TR.UTF-8", "el_GR.UTF-8"}) {
    Add(out, {.name = std::string{"norm["} + locale + ",lower]",
              .config =
                [locale] {
                  return Cfg{NormalizingTokenizer::Options{
                    .locale = Loc(locale), .case_convert = irs::Case::Lower}};
                },
              .dict = dict});
  }
}

void AddStemming(std::vector<Spec>& out) {
  for (const auto* locale : {"en_US.UTF-8", "ru_RU.UTF-8", "de_DE.UTF-8"}) {
    Add(out, {.name = std::string{"stem["} + locale + "]",
              .config =
                [locale] {
                  return Cfg{StemmingTokenizer::Options{.locale = Loc(locale)}};
                },
              .dict = WordDict(),
              .native = WordValues()});
  }
}

void AddCollation(std::vector<Spec>& out) {
  for (const auto* locale : {"en_US.UTF-8", "de_DE.UTF-8", "ru_RU.UTF-8"}) {
    Add(out,
        {.name = std::string{"collation["} + locale + "]",
         .config =
           [locale] {
             return Cfg{CollationTokenizer::Options{.locale = Loc(locale)}};
           },
         .dict = {"Straße", "ä", "ö", "ё", "A", "a"}});
  }
}

void AddStopwords(std::vector<Spec>& out) {
  Add(out, {.name = "stopwords[words]",
            .config =
              [] {
                return Cfg{StopwordsTokenizer::Options{
                  .mask = {"the", "a", "an", "of", "and"}}};
              },
            .dict = StopwordDict(),
            .native = WordValues()});
  Add(out, {.name = "stopwords[hex]",
            .config =
              [] {
                return Cfg{StopwordsTokenizer::Options{
                  .mask = {"616263", "646566", "6D6e6F"}}};
              },
            .dict = StopwordDict()});
  Add(out, {.name = "stopwords[empty]",
            .config = [] { return Cfg{StopwordsTokenizer::Options{}}; }});
  Add(out, {.name = "stopwords[file]",
            .config =
              [] {
                return Cfg{StopwordsTokenizer::Options{
                  .mask = {"of"}, .stopwords_path = StopwordsFile()}};
              },
            .dict = StopwordDict(),
            .native = WordValues()});
  Add(out, {.name = "stopwords[dir]",
            .config =
              [] {
                return Cfg{StopwordsTokenizer::Options{
                  .stopwords_path = (FixtureDir() / "words").string()}};
              },
            .dict = StopwordDict(),
            .native = WordValues()});
}

Cfg TextStopwordsPathCfg() {
  PipelineTokenizer::Options opts;
  opts.children.push_back(
    Child(Cfg{TextTokenizer::Options{.convert = irs::Case::Lower}}));
  opts.children.push_back(Child(Cfg{StopwordsTokenizer::Options{
    .stopwords_path = (FixtureDir() / "text_stopwords" / "en").string()}}));
  return Cfg{std::move(opts)};
}

Cfg TextEdgeNGramCfg() {
  PipelineTokenizer::Options opts;
  opts.children.push_back(
    Child(Cfg{TextTokenizer::Options{.convert = irs::Case::Lower}}));
  opts.children.push_back(Child(Cfg{NGramTokenizer::Options{
    .min_gram = 2,
    .max_gram = 4,
    .preserve_original = true,
    .stream_bytes_type = NGramTokenizer::InputType::UTF8,
    .ngram_mode = NGramTokenizer::NGramMode::Prefix}}));
  return Cfg{std::move(opts)};
}

void AddTextChain(std::vector<Spec>& out, std::string name,
                  std::function<Cfg()> config, std::vector<std::string> dict,
                  std::vector<std::string> native, uint32_t cost) {
  Add(out, {.name = std::move(name),
            .config = config,
            .dict = std::move(dict),
            .native = std::move(native),
            .model = Model::Chain,
            .model_children = [config] { return ChainModel(config()); },
            .cost = cost});
}

void AddTextChains(std::vector<Spec>& out) {
  AddTextChain(
    out, "text[en,lower,stem]",
    [] { return TextChainCfg("en_US.UTF-8", irs::Case::Lower, true); },
    Merge({WordDict(), BreakDict()}), WordValues(), 2);
  AddTextChain(
    out, "text[en,none,nostem]",
    [] { return TextChainCfg("en_US.UTF-8", irs::Case::None, false); },
    Merge({WordDict(), BreakDict()}), {}, 1);
  AddTextChain(
    out, "text[en,upper,stopwords]",
    [] {
      return TextChainCfg("en_US.UTF-8", irs::Case::Upper, false,
                          {"the", "a", "of"});
    },
    Merge({WordDict(), StopwordDict()}), {}, 1);
  AddTextChain(
    out, "text[ru,lower,stem]",
    [] { return TextChainCfg("ru_RU.UTF-8", irs::Case::Lower, true); },
    BreakDict(), {}, 1);
  AddTextChain(
    out, "text[de,lower,stem]",
    [] { return TextChainCfg("de_DE.UTF-8", irs::Case::Lower, true); },
    BreakDict(), {}, 1);
  AddTextChain(out, "text[en,stopwords_path]", TextStopwordsPathCfg,
               Merge({WordDict(), StopwordDict()}), WordValues(), 2);
  AddTextChain(out, "text[en,ngram2_4]", TextEdgeNGramCfg, WordDict(), {}, 8);
}

void AddText(std::vector<Spec>& out) {
  using Options = TextTokenizer::Options;
  const auto add = [&out](Options::Separate separate, Options::Accept accept,
                          irs::Case convert) {
    Add(out,
        {.name = std::string{"text[sep="} +
                 std::to_string(static_cast<int>(separate)) +
                 ",accept=" + std::to_string(static_cast<int>(accept)) +
                 ",convert=" + std::to_string(static_cast<int>(convert)) + "]",
         .config =
           [separate, accept, convert] {
             return Cfg{Options{
               .separate = separate, .accept = accept, .convert = convert}};
           },
         .dict = Merge({BreakDict(), WordDict()}),
         .native = WordValues(),
         .cost = 2});
  };
  for (const auto separate :
       {Options::Separate::None, Options::Separate::Word,
        Options::Separate::Sentence, Options::Separate::Line,
        Options::Separate::Paragraph}) {
    add(separate, Options::Accept::AlphaNumeric, irs::Case::Lower);
  }
  for (const auto accept : {Options::Accept::Any, Options::Accept::Graphic,
                            Options::Accept::Alpha}) {
    add(Options::Separate::Word, accept, irs::Case::Lower);
  }
  for (const auto convert : {irs::Case::None, irs::Case::Upper}) {
    add(Options::Separate::Word, Options::Accept::AlphaNumeric, convert);
  }
  add(Options::Separate::Sentence, Options::Accept::Any, irs::Case::None);
  add(Options::Separate::None, Options::Accept::Any, irs::Case::Upper);
}

void AddIcuText(std::vector<Spec>& out) {
  using Options = IcuTextTokenizer::Options;
  const auto add = [&out](const char* locale, Options::Separate separate,
                          Options::Accept accept) {
    Add(out,
        {.name = std::string{"icu_text["} + locale +
                 ",sep=" + std::to_string(static_cast<int>(separate)) +
                 ",accept=" + std::to_string(static_cast<int>(accept)) + "]",
         .config =
           [locale, separate, accept] {
             return Cfg{Options{
               .separate = separate, .accept = accept, .locale = Loc(locale)}};
           },
         .dict = Merge({BreakDict(), WordDict()}),
         .native = WordValues(),
         .cost = 4});
  };
  for (const auto* locale :
       {"en_US.UTF-8", "ja_JP.UTF-8", "fi_FI.UTF-8", "th_TH.UTF-8"}) {
    add(locale, Options::Separate::Word, Options::Accept::AlphaNumeric);
  }
  add("en_US.UTF-8", Options::Separate::Sentence, Options::Accept::Any);
  add("en_US.UTF-8", Options::Separate::Word, Options::Accept::Any);
  add("en_US.UTF-8", Options::Separate::Word, Options::Accept::Alpha);
  add("ja_JP.UTF-8", Options::Separate::Sentence,
      Options::Accept::AlphaNumeric);
}

void AddSynonyms(std::vector<Spec>& out) {
  const std::vector<std::string> solr_dict = {
    "i-pod", "i pod", "ipod",     "sea biscuit", "sea biscit", "seabiscuit",
    "gb",    "gib",   "gigabyte", "fast",        "quick",      "speedy",
    "=>",    ","};
  const std::vector<std::string> wordnet_dict = {"angry", "furious", "mad",
                                                 "happy", "glad",    "come"};
  Add(out, {.name = "solr_synonyms",
            .config =
              [] {
                return Cfg{SolrSynonymsTokenizer::Options{
                  .synonyms_text = std::string{kSolrSynonyms}}};
              },
            .dict = solr_dict,
            .native = WordValues()});
  Add(out, {.name = "solr_synonyms[empty]",
            .config = [] { return Cfg{SolrSynonymsTokenizer::Options{}}; }});
  Add(out, {.name = "wordnet_synonyms",
            .config =
              [] {
                return Cfg{WordnetSynonymsTokenizer::Options{
                  .synonyms_text = std::string{kWordnetSynonyms}}};
              },
            .dict = wordnet_dict,
            .native = WordValues()});
  Add(out, {.name = "wordnet_synonyms[empty]",
            .config = [] { return Cfg{WordnetSynonymsTokenizer::Options{}}; }});
}

void AddModels(std::vector<Spec>& out) {
  if (!HasModel()) {
    return;
  }
  Add(out, {.name = "classification[top1]",
            .config =
              [] {
                return Cfg{ClassificationTokenizer::Options{.model_location =
                                                              ModelLocation()}};
              },
            .dict = WordDict(),
            .native = WordValues(),
            .cost = 16});
  Add(out,
      {.name = "classification[top3]",
       .config =
         [] {
           return Cfg{ClassificationTokenizer::Options{
             .model_location = ModelLocation(), .threshold = 0.0, .top_k = 3}};
         },
       .dict = WordDict(),
       .cost = 16});
  Add(out, {.name = "nearest_neighbors[top2]",
            .config =
              [] {
                return Cfg{NearestNeighborsTokenizer::Options{
                  .model_location = ModelLocation(), .top_k = 2}};
              },
            .dict = WordDict(),
            .cost = 16});
}

void AddGeo(std::vector<Spec>& out) {
  const auto wkb = [](irs::analysis::Tokenizer& a) {
    GeoTokenizer::Cast(a).SetWkbInput(true);
  };
  Add(out, {.name = "geopoint[array_json]",
            .config = [] { return Cfg{GeoPointTokenizer::Options{}}; },
            .dict = GeoDict(),
            .native = GeoJsonValues(),
            .cost = 2});
  Add(out, {.name = "geopoint[object_json]",
            .config =
              [] {
                return Cfg{
                  GeoPointTokenizer::Options{.latitude = {"location", "lat"},
                                             .longitude = {"location", "lng"}}};
              },
            .dict = GeoDict(),
            .native = GeoJsonValues(),
            .cost = 2});
  Add(out, {.name = "geopoint[wkb]",
            .config = [] { return Cfg{GeoPointTokenizer::Options{}}; },
            .setup = wkb,
            .dict = GeoDict(),
            .native = GeoWkbValues(),
            .cost = 2});

  using Type = GeoJsonTokenizer::Type;
  using Coding = GeoJsonTokenizer::Coding;
  for (const auto type : {Type::Shape, Type::Centroid, Type::Point}) {
    for (const auto coding : {Coding::Source, Coding::S2Point,
                              Coding::S2LatLngF64, Coding::S2LatLngU32}) {
      Add(out, {.name = std::string{"geojson[type="} +
                        std::to_string(static_cast<int>(type)) + ",coding=" +
                        std::to_string(static_cast<int>(coding)) + "]",
                .config =
                  [type, coding] {
                    return Cfg{GeoJsonTokenizer::Options{.type = type,
                                                         .coding = coding}};
                  },
                .dict = GeoDict(),
                .native = GeoJsonValues(),
                .cost = 2});
    }
  }
  for (const auto& tuned :
       {irs::geo::GeoOptions{
          .max_cells = 4, .min_level = 0, .max_level = 12, .level_mod = 2},
        irs::geo::GeoOptions{.max_cells = 64,
                             .min_level = 8,
                             .max_level = 30,
                             .level_mod = 3,
                             .optimize_for_space = true}}) {
    Add(
      out,
      {.name = std::string{"geojson[cells="} + std::to_string(tuned.max_cells) +
               ",levels=" + std::to_string(tuned.min_level) + ".." +
               std::to_string(tuned.max_level) +
               ",mod=" + std::to_string(tuned.level_mod) + "]",
       .config =
         [tuned] {
           return Cfg{GeoJsonTokenizer::Options{.options = tuned,
                                                .type = Type::Shape,
                                                .coding = Coding::S2LatLngF64}};
         },
       .dict = GeoDict(),
       .native = GeoJsonValues(),
       .cost = 2});
    Add(out, {.name = std::string{"geopoint[cells="} +
                      std::to_string(tuned.max_cells) +
                      ",mod=" + std::to_string(tuned.level_mod) + "]",
              .config =
                [tuned] {
                  return Cfg{GeoPointTokenizer::Options{.options = tuned}};
                },
              .dict = GeoDict(),
              .native = GeoJsonValues(),
              .cost = 2});
  }
  Add(out, {.name = "geojson[wkb_shape]",
            .config =
              [] {
                return Cfg{GeoJsonTokenizer::Options{.type = Type::Shape,
                                                     .coding = Coding::Source}};
              },
            .setup = wkb,
            .dict = GeoDict(),
            .native = GeoWkbValues(),
            .cost = 2});
}

void AddWildcard(std::vector<Spec>& out) {
  Add(
    out,
    {.name = "wildcard[3]",
     .config = [] { return Cfg{WildcardTokenizer::Options{.ngram_size = 3}}; },
     .dict = {"%", "_", "%%", "a%b", "\\%"},
     .model_children =
       [] {
         std::vector<Ptr> subs;
         subs.push_back(MakeChild(Cfg{irs::KeywordTokenizer::Options{}}));
         return subs;
       },
     .cost = 4});
  Add(
    out,
    {.name = "wildcard[2]",
     .config = [] { return Cfg{WildcardTokenizer::Options{.ngram_size = 2}}; },
     .dict = {"%", "_", "%%", "a%b", "\\%"},
     .model_children =
       [] {
         std::vector<Ptr> subs;
         subs.push_back(MakeChild(Cfg{irs::KeywordTokenizer::Options{}}));
         return subs;
       },
     .cost = 4});
  Add(out, {.name = "wildcard[3,delim]",
            .config =
              [] {
                return Cfg{WildcardTokenizer::Options{
                  .base_analyzer =
                    Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}),
                  .ngram_size = 3}};
              },
            .dict = {"%", "_", ",", "\"", "a%b"},
            .native = CsvValues(),
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(
                  Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
                return subs;
              },
            .cost = 4});
}

void AddShingle(std::vector<Spec>& out) {
  Add(out, {.name = "shingle[2,2]",
            .config =
              [] {
                return Cfg{ShingleTokenizer::Options{
                  .base_analyzer = Child(Cfg{TextTokenizer::Options{}}),
                  .min_shingle_size = 2,
                  .max_shingle_size = 2}};
              },
            .dict = Merge({WordDict(), BreakDict()}),
            .native = WordValues(),
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(Cfg{TextTokenizer::Options{}}));
                return subs;
              },
            .cost = 2});
  Add(out, {.name = "shingle[2,3,no_unigrams]",
            .config =
              [] {
                return Cfg{ShingleTokenizer::Options{
                  .base_analyzer = Child(Cfg{TextTokenizer::Options{}}),
                  .min_shingle_size = 2,
                  .max_shingle_size = 3,
                  .output_unigrams = false,
                  .output_unigrams_if_no_shingles = true}};
              },
            .dict = WordDict(),
            .native = WordValues(),
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(Cfg{TextTokenizer::Options{}}));
                return subs;
              },
            .cost = 2});
  Add(out, {.name = "shingle[1,4,separator]",
            .config =
              [] {
                return Cfg{ShingleTokenizer::Options{
                  .base_analyzer =
                    Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}),
                  .min_shingle_size = 1,
                  .max_shingle_size = 4,
                  .output_unigrams = true,
                  .token_separator = Bytes("_"),
                  .filler_token = Bytes("#")}};
              },
            .dict = {",", "_", "#", "\""},
            .native = CsvValues(),
            .params = {.delim = '#'},
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(
                  Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
                return subs;
              },
            .cost = 4});
  Add(out, {.name = "shingle[2,2,frequent]",
            .config =
              [] {
                return Cfg{ShingleTokenizer::Options{
                  .base_analyzer = Child(Cfg{TextTokenizer::Options{}}),
                  .min_shingle_size = 2,
                  .max_shingle_size = 2,
                  .output_unigrams = false,
                  .frequent_words = {Bytes("the"), Bytes("a"), Bytes("of")},
                  .store_tokens = false}};
              },
            .dict = Merge({WordDict(), StopwordDict()}),
            .native = WordValues(),
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(Cfg{TextTokenizer::Options{}}));
                return subs;
              },
            .cost = 2});
}

void AddPipeline(std::vector<Spec>& out) {
  Add(out, {.name = "pipeline[segmentation,stopwords]",
            .config =
              [] {
                PipelineTokenizer::Options opts;
                opts.children.push_back(Child(Cfg{TextTokenizer::Options{}}));
                opts.children.push_back(Child(Cfg{StopwordsTokenizer::Options{
                  .mask = {"the", "a", "an", "of", "and"}}}));
                return Cfg{std::move(opts)};
              },
            .dict = Merge({WordDict(), StopwordDict()}),
            .native = WordValues(),
            .model = Model::Chain,
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(Cfg{TextTokenizer::Options{}}));
                subs.push_back(MakeChild(Cfg{StopwordsTokenizer::Options{
                  .mask = {"the", "a", "an", "of", "and"}}}));
                return subs;
              },
            .cost = 2});
  Add(out, {.name = "pipeline[delimiter,norm]",
            .config =
              [] {
                PipelineTokenizer::Options opts;
                opts.children.push_back(
                  Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
                opts.children.push_back(Child(Cfg{NormalizingTokenizer::Options{
                  .locale = Loc("en"), .case_convert = irs::Case::Lower}}));
                return Cfg{std::move(opts)};
              },
            .dict = {",", "Straße", "İ", "\""},
            .native = CsvValues(),
            .model = Model::Chain,
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(
                  Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
                subs.push_back(MakeChild(Cfg{NormalizingTokenizer::Options{
                  .locale = Loc("en"), .case_convert = irs::Case::Lower}}));
                return subs;
              },
            .cost = 2});
  Add(out, {.name = "pipeline[delimiter,ngram]",
            .config =
              [] {
                PipelineTokenizer::Options opts;
                opts.children.push_back(
                  Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
                opts.children.push_back(Child(Cfg{NGramTokenizer::Options{
                  .min_gram = 2, .max_gram = 3, .preserve_original = false}}));
                return Cfg{std::move(opts)};
              },
            .dict = {",", "\""},
            .native = CsvValues(),
            .model = Model::Chain,
            .model_children =
              [] {
                std::vector<Ptr> subs;
                subs.push_back(MakeChild(
                  Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
                subs.push_back(MakeChild(Cfg{NGramTokenizer::Options{
                  .min_gram = 2, .max_gram = 3, .preserve_original = false}}));
                return subs;
              },
            .cost = 8});
  Add(out,
      {.name = "pipeline[text,stem,collation]",
       .config =
         [] {
           PipelineTokenizer::Options opts;
           opts.children.push_back(
             Child(Cfg{TextTokenizer::Options{.convert = irs::Case::Lower}}));
           opts.children.push_back(Child(
             Cfg{StemmingTokenizer::Options{.locale = Loc("en_US.UTF-8")}}));
           opts.children.push_back(Child(
             Cfg{CollationTokenizer::Options{.locale = Loc("en_US.UTF-8")}}));
           return Cfg{std::move(opts)};
         },
       .dict = Merge({WordDict(), BreakDict()}),
       .native = WordValues(),
       .cost = 4});
  Add(out,
      {.name = "pipeline[segmentation,synonyms,stopwords]",
       .config =
         [] {
           PipelineTokenizer::Options opts;
           opts.children.push_back(Child(Cfg{TextTokenizer::Options{}}));
           opts.children.push_back(Child(Cfg{SolrSynonymsTokenizer::Options{
             .synonyms_text = std::string{kSolrSynonyms}}}));
           opts.children.push_back(
             Child(Cfg{StopwordsTokenizer::Options{.mask = {"the", "a"}}}));
           return Cfg{std::move(opts)};
         },
       .dict = Merge({WordDict(), StopwordDict()}),
       .native = WordValues(),
       .model = Model::Chain,
       .model_children =
         [] {
           std::vector<Ptr> subs;
           subs.push_back(MakeChild(Cfg{TextTokenizer::Options{}}));
           subs.push_back(MakeChild(Cfg{SolrSynonymsTokenizer::Options{
             .synonyms_text = std::string{kSolrSynonyms}}}));
           subs.push_back(
             MakeChild(Cfg{StopwordsTokenizer::Options{.mask = {"the", "a"}}}));
           return subs;
         },
       .cost = 4});
  Add(out, {.name = "pipeline[single]", .config = [] {
              PipelineTokenizer::Options opts;
              opts.children.push_back(
                Child(Cfg{DelimitedTokenizer::Options{.delimiter = ","}}));
              return Cfg{std::move(opts)};
            }});
}

void AddUnion(std::vector<Spec>& out) {
  Add(out, {.name = "union[text,ngram]",
            .config =
              [] {
                UnionTokenizer::Options opts;
                opts.children.push_back(Child(
                  Cfg{TextTokenizer::Options{.convert = irs::Case::Lower}}));
                opts.children.push_back(Child(Cfg{NGramTokenizer::Options{
                  .min_gram = 3, .max_gram = 3, .preserve_original = false}}));
                return Cfg{std::move(opts)};
              },
            .dict = WordDict(),
            .native = WordValues(),
            .cost = 8});
  Add(out, {.name = "union[segmentation,collation]",
            .config =
              [] {
                UnionTokenizer::Options opts;
                opts.children.push_back(Child(Cfg{TextTokenizer::Options{}}));
                opts.children.push_back(Child(Cfg{
                  CollationTokenizer::Options{.locale = Loc("en_US.UTF-8")}}));
                return Cfg{std::move(opts)};
              },
            .dict = WordDict(),
            .native = WordValues(),
            .cost = 2});
  Add(out, {.name = "union[three_delims]",
            .config =
              [] {
                UnionTokenizer::Options opts;
                for (const auto* d : {",", ";", "|"}) {
                  opts.children.push_back(
                    Child(Cfg{DelimitedTokenizer::Options{.delimiter = d}}));
                }
                return Cfg{std::move(opts)};
              },
            .dict = {",", ";", "|", "\""},
            .native = CsvValues(),
            .cost = 2});
  Add(out, {.name = "union[empty]",
            .config = [] { return Cfg{UnionTokenizer::Options{}}; }});
}

void AddSql(std::vector<Spec>& out) {
  for (const auto* expr :
       {"upper(input)", "lower(input)", "input || '-suffix'", "md5(input)",
        "string_split(input, ',')", "regexp_split_to_array(input, '\\s+')",
        "list_value(input, upper(input))", "nullif(input, '')"}) {
    Add(out,
        {.name = std::string{"sql["} + expr + "]",
         .config =
           [expr] { return Cfg{SqlTokenizer::Options{.expression = expr}}; },
         .dict = {",", " ", "'", "\\", "-suffix"},
         .model = Model::Sql,
         .params = {.expression = expr},
         .utf8_only = true});
  }
}

}  // namespace

bool ModelsAvailable() { return HasModel(); }

irs::analysis::Tokenizer::ptr Make(const Spec& spec) {
  auto tokenizer =
    irs::analysis::CreateTokenizer(spec.config(), ::tests::Cache());
  if (!tokenizer) {
    return tokenizer;
  }
  tokenizer->Bind(Context());
  if (spec.setup) {
    spec.setup(*tokenizer);
  }
  return tokenizer;
}

const std::vector<Spec>& AllSpecs() {
  static const std::vector<Spec> kSpecs = [] {
    std::vector<Spec> out;
    AddKeyword(out);
    AddDelimited(out);
    AddMultiDelimited(out);
    AddPattern(out);
    AddPathHierarchy(out);
    AddNGram(out);
    AddSparseNGram(out);
    AddSplitByNonAlpha(out);
    AddNormalizing(out);
    AddStemming(out);
    AddCollation(out);
    AddStopwords(out);
    AddTextChains(out);
    AddText(out);
    AddIcuText(out);
    AddSynonyms(out);
    AddModels(out);
    AddGeo(out);
    AddWildcard(out);
    AddShingle(out);
    AddPipeline(out);
    AddUnion(out);
    AddSql(out);
    return out;
  }();
  return kSpecs;
}

}  // namespace tests::fuzz
