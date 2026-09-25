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

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <iresearch/utils/duckdb_engine.hpp>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "connector/functions/markdown_render.h"
#include "docs/docs_data.h"
#include "docs/docs_index_data.h"
#include "docs/docs_search.h"

namespace sdb::docs {
namespace {

class DocsIndex : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (GetDocsIndex().empty()) {
      GTEST_SKIP() << "build carries no embedded documentation index";
    }
  }

  static duckdb::DatabaseInstance& Db() {
    return irs::DuckDBEngine::Instance().instance();
  }
};

TEST_F(DocsIndex, FindsExactPath) {
  const auto doc = FindByPath(Db(), "sql/indexes/index.md#Indexes");
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(doc->title, "Indexes");
  EXPECT_FALSE(doc->content.empty());
}

TEST_F(DocsIndex, PromotesPageToItsTitleRow) {
  const auto doc = FindByPath(Db(), "sql/indexes/index.md");
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(doc->path, "sql/indexes/index.md#Indexes");
}

TEST_F(DocsIndex, UnknownPathIsEmpty) {
  EXPECT_FALSE(FindByPath(Db(), "no/such/page.md").has_value());
  EXPECT_FALSE(FindByPath(Db(), "").has_value());
}

TEST_F(DocsIndex, SignatureLookupFindsEveryOverload) {
  const auto hits = Lookup(Db(), "date_trunc");
  ASSERT_FALSE(hits.empty());
  for (const auto& doc : hits) {
    EXPECT_TRUE(absl::StartsWithIgnoreCase(doc.title, "date_trunc("));
  }
}

TEST_F(DocsIndex, TitleWordPrefixFindsSuffixedHeadings) {
  const auto hits = Lookup(Db(), "date_part");
  ASSERT_FALSE(hits.empty());
}

TEST_F(DocsIndex, LookupIsCaseInsensitive) {
  EXPECT_FALSE(Lookup(Db(), "BM25").empty());
  EXPECT_FALSE(Lookup(Db(), "bm25").empty());
}

TEST_F(DocsIndex, UnknownNameHasNoHits) {
  EXPECT_TRUE(Lookup(Db(), "to_tsvector").empty());
}

TEST_F(DocsIndex, ListPrefixPagesOnlySkipsSections) {
  const auto pages =
    ListPrefix(Db(), "sql/functions/search/", /*pages_only=*/true);
  const auto all =
    ListPrefix(Db(), "sql/functions/search/", /*pages_only=*/false);
  EXPECT_LT(pages.size(), all.size());
  for (const auto& doc : pages) {
    EXPECT_LE(HeadingDepth(doc.path), 1U);
  }
}

TEST_F(DocsIndex, ListPrefixIsSortedAndScoped) {
  const auto hits = ListPrefix(Db(), "sql/indexes/", /*pages_only=*/false);
  ASSERT_FALSE(hits.empty());
  EXPECT_TRUE(absl::c_is_sorted(
    hits, [](const Entry& l, const Entry& r) { return l.path < r.path; }));
  for (const auto& doc : hits) {
    EXPECT_TRUE(doc.path.starts_with("sql/indexes/"));
  }
}

TEST_F(DocsIndex, ChildrenAreImmediateSectionsOnly) {
  const auto children =
    Children(Db(), "sql/functions/search/scoring.md#Relevance_Scoring");
  ASSERT_FALSE(children.empty());
  for (const auto& doc : children) {
    EXPECT_EQ(HeadingDepth(doc.path), 2U);
  }
}

TEST_F(DocsIndex, CompletesSectionsAndPagePaths) {
  const auto sections = CompletePath(Db(), "cook", 40);
  ASSERT_FALSE(sections.empty());
  EXPECT_EQ(sections.front(), "cookbook");

  const auto pages = CompletePath(Db(), "sql/functions/sea", 40);
  ASSERT_FALSE(pages.empty());
  for (const auto& page : pages) {
    EXPECT_TRUE(page.starts_with("sql/functions/sea"));
    EXPECT_EQ(page.find('#'), std::string::npos);
  }
}

TEST_F(DocsIndex, CompletionRespectsItsLimit) {
  EXPECT_LE(CompletePath(Db(), "sql/", 5).size(), 5U);
}

TEST_F(DocsIndex, SearchRanksAndRespectsItsLimit) {
  std::string error;
  const auto hits = Search(Db(), "inverted index", 5, {}, error);
  EXPECT_TRUE(error.empty()) << error;
  ASSERT_FALSE(hits.empty());
  EXPECT_LE(hits.size(), 5U);
}

TEST_F(DocsIndex, SearchRejectsExclusion) {
  std::string error;
  const auto hits = Search(Db(), "+fox -red", 5, {}, error);
  EXPECT_TRUE(hits.empty());
  EXPECT_NE(error.find("exclusion"), std::string::npos) << error;
}

TEST_F(DocsIndex, SearchFallsBackToWordsWhenLuceneDoesNotParse) {
  for (const auto* query :
       {"@@ operator", "SELECT * FROM t WHERE body @@ 'fox'", "title:(fox"}) {
    std::string error;
    EXPECT_FALSE(Search(Db(), query, 5, {}, error).empty()) << query;
    EXPECT_TRUE(error.empty()) << query << ": " << error;
  }
}

TEST_F(DocsIndex, SearchReadsQuestionsAndPastedText) {
  std::string error;
  const auto pasted =
    Search(Db(), "Only one scorer function is allowed per inverted index", 5,
           {}, error);
  ASSERT_FALSE(pasted.empty());
  EXPECT_TRUE(
    pasted.front().path.starts_with("sql/functions/search/scoring.md"))
    << pasted.front().path;
  const auto question =
    Search(Db(), "how do I create an inverted index?", 3, {}, error);
  EXPECT_TRUE(absl::c_any_of(question, [](const Entry& entry) {
    return entry.path.starts_with("sql/indexes/inverted/");
  }));
  EXPECT_TRUE(error.empty()) << error;
}

TEST_F(DocsIndex, SearchPutsTheNamedObjectFirst) {
  const auto objects = FindObjects(Db(), "date_trunc", {});
  ASSERT_FALSE(objects.empty());
  std::string error;
  const auto hits = Search(Db(), "date_trunc", 5, {}, error);
  ASSERT_FALSE(hits.empty());
  EXPECT_EQ(hits.front().path, objects.front().path);
}

TEST_F(DocsIndex, SearchFindsSymbolsAndSplitIdentifiers) {
  std::string error;
  const auto symbol = Search(Db(), "@@", 5, {.content = true}, error);
  ASSERT_FALSE(symbol.empty());
  EXPECT_TRUE(symbol.front().content.contains("@@"));
  EXPECT_FALSE(Search(Db(), "max_threads", 5, {}, error).empty());
  EXPECT_TRUE(error.empty()) << error;
}

TEST_F(DocsIndex, SearchMatchesSymbolsInTextNotInMarkup) {
  std::string error;
  const auto hits = Search(Db(), "##", 5, {.content_text = true}, error);
  ASSERT_FALSE(hits.empty());
  EXPECT_TRUE(absl::c_all_of(hits, [](const Entry& entry) {
    return entry.title.contains("##") || entry.content_text.contains("##");
  }));
  EXPECT_TRUE(error.empty()) << error;
}

TEST_F(DocsIndex, SearchMatchesWordForms) {
  std::string error;
  const auto hits = Search(Db(), "how do I highlight matches", 3, {}, error);
  EXPECT_TRUE(absl::c_any_of(hits, [](const Entry& entry) {
    return entry.path.contains("highlighting.md");
  }));
}

TEST_F(DocsIndex, CallSyntaxNamesTheObject) {
  EXPECT_EQ(CallName("date_trunc(ts, 'day')"), "date_trunc");
  EXPECT_EQ(CallName("BM25("), "BM25");
  EXPECT_EQ(CallName("Calendar date (year)"), "Calendar date (year)");
  EXPECT_FALSE(FindObjects(Db(), "date_trunc()", {}).empty());
}

TEST_F(DocsIndex, ResolveLinkFollowsPagesAnchorsAndRelativeLinks) {
  const auto path = [](std::optional<Entry> entry) {
    return entry ? entry->path : std::string{};
  };
  EXPECT_EQ(path(ResolveLink(Db(), "sql/functions/search/scoring.md")),
            "sql/functions/search/scoring.md#Relevance_Scoring");
  const auto fuzzy =
    path(ResolveLink(Db(), "sql/functions/search/full-text.md#ts_levenshtein"));
  EXPECT_TRUE(fuzzy.starts_with("sql/functions/search/full-text.md#"));
  EXPECT_TRUE(fuzzy.contains("ts_levenshtein(")) << fuzzy;
  const auto settings = path(
    ResolveLink(Db(), "sql/indexes/inverted/maintenance.md#session-settings"));
  EXPECT_TRUE(settings.ends_with("#Session_settings")) << settings;
  EXPECT_EQ(path(ResolveLink(
              Db(), "../../indexes/inverted/maintenance.md#session-settings",
              "sql/functions/search/full-text.md#Full")),
            settings);
  EXPECT_EQ(path(ResolveLink(Db(), "./full-text.md#ts_levenshtein")), fuzzy);
  EXPECT_FALSE(ResolveLink(Db(), "no/such/page.md").has_value());
}

TEST_F(DocsIndex, ResolveLinkOpensSiteUrlsAndPathsWithoutExtension) {
  const auto path = [](std::optional<Entry> entry) {
    return entry ? entry->path : std::string{};
  };
  EXPECT_EQ(
    SiteRoute("https://serenedb.com/docs/sql/indexes/?tab=1#index-types"),
    "sql/indexes#index-types");
  EXPECT_EQ(SiteRoute("https://www.serenedb.com/docs"), "");
  EXPECT_EQ(SiteRoute("https://duckdb.org/docs/sql"),
            "https://duckdb.org/docs/sql");
  EXPECT_EQ(SiteRoute("sql/indexes/index.md"), "sql/indexes/index.md");
  EXPECT_EQ(path(ResolveLink(Db(), "https://serenedb.com/docs/sql/indexes/")),
            "sql/indexes/index.md#Indexes");
  EXPECT_EQ(path(ResolveLink(
              Db(), "https://serenedb.com/docs/sql/indexes#index-types")),
            "sql/indexes/index.md#Indexes#Index_Types");
  EXPECT_EQ(path(ResolveLink(
              Db(), "https://serenedb.com/docs/sql/functions/search/scoring")),
            "sql/functions/search/scoring.md#Relevance_Scoring");
  EXPECT_EQ(path(ResolveLink(Db(), "https://serenedb.com/docs/sql/data_types")),
            "sql/data_types/index.md#Data_Types");
  EXPECT_EQ(path(ResolveLink(Db(), "sql/functions/search/scoring")),
            "sql/functions/search/scoring.md#Relevance_Scoring");
  EXPECT_EQ(path(ResolveLink(Db(), "scoring.md")),
            "sql/functions/search/scoring.md#Relevance_Scoring");
  EXPECT_FALSE(
    ResolveLink(Db(), "https://duckdb.org/docs/sql/indexes").has_value());
}

TEST_F(DocsIndex, CandidatesSurviveATypo) {
  for (const auto* typo : {"tsvecto", "vacum", "hnws"}) {
    EXPECT_FALSE(Candidates(Db(), typo, 10).empty()) << typo;
  }
}

TEST_F(DocsIndex, NamesAreNotQuerySyntax) {
  EXPECT_FALSE(Candidates(Db(), "AND", 10).empty());
}

TEST_F(DocsIndex, ObjectsResolveByAlias) {
  const auto objects = FindObjects(Db(), "int8", {});
  EXPECT_TRUE(absl::c_any_of(objects, [](const Object& object) {
    return object.kind == "type" && object.name == "BIGINT";
  }));
}

TEST_F(DocsIndex, ObjectKindNarrowsTheLookup) {
  const auto types = FindObjects(Db(), "UUID", "type");
  ASSERT_FALSE(types.empty());
  EXPECT_TRUE(absl::c_all_of(
    types, [](const Object& object) { return object.kind == "type"; }));
  EXPECT_TRUE(FindObjects(Db(), "date_trunc", "type").empty());
}

TEST_F(DocsIndex, CompletesObjectNamesAndAliases) {
  const auto names = CompleteName(Db(), "date_tr", {}, 40);
  EXPECT_EQ(absl::c_count(names, "date_trunc"), 1);
  for (const auto& name : names) {
    EXPECT_TRUE(absl::StartsWithIgnoreCase(name, "date_tr")) << name;
  }
  EXPECT_TRUE(std::ranges::is_sorted(names, {}, [](const std::string& name) {
    return absl::AsciiStrToLower(name);
  }));
  EXPECT_TRUE(
    absl::c_linear_search(CompleteName(Db(), "int8", {}, 40), "INT8"));
}

TEST_F(DocsIndex, EmbeddedIndexCompressesItsText) {
  if (GetDocs().empty()) {
    GTEST_SKIP() << "build carries no documentation corpus";
  }
  size_t text = 0;
  for (const auto& doc : GetDocs()) {
    text += doc.content.size();
  }
  const auto files = GetDocsIndex();
  const auto columns = absl::c_find_if(
    files, [](const IndexFile& file) { return file.name.ends_with(".col"); });
  ASSERT_NE(columns, files.end());
  EXPECT_LT(columns->bytes.size() * 2, text);
}

TEST_F(DocsIndex, LayoutMustNameTheTermsOfEveryIndexedColumn) {
  std::vector<IndexBlob> image;
  for (const auto& file : GetDocsIndex()) {
    std::string bytes{reinterpret_cast<const char*>(file.bytes.data()),
                      file.bytes.size()};
    if (file.name == kLayoutFile) {
      std::vector<std::string> lines = absl::StrSplit(bytes, '\n');
      for (auto& line : lines) {
        if (line.starts_with("title ")) {
          line = line.substr(0, line.rfind(' '));
        }
      }
      bytes = absl::StrJoin(lines, "\n");
    }
    image.push_back(
      {.name = std::string{file.name}, .bytes = std::move(bytes)});
  }
  try {
    Publish(Db(), std::move(image));
    FAIL() << "a layout without the title terms was accepted";
  } catch (const std::exception& e) {
    EXPECT_NE(std::string_view{e.what()}.find("names no terms for 'title'"),
              std::string_view::npos)
      << e.what();
  }
}

TEST_F(DocsIndex, NameCompletionHonoursKindAndLimit) {
  EXPECT_EQ(CompleteName(Db(), "", "setting", 5).size(), 5U);
  EXPECT_TRUE(CompleteName(Db(), "date_tr", "type", 40).empty());
}

TEST(DocsObjects, EncodingRoundTrips) {
  const std::vector<Object> objects{
    {.kind = "type",
     .name = "BIGINT",
     .signature = "BIGINT",
     .summary = "Signed\teight-byte\ninteger \\N",
     .aliases = "INT8, LONG",
     .path = "sql/data_types/index.md#Data_Types",
     .page = "sql/data_types/index.md",
     .category = std::nullopt,
     .breadcrumb = ""},
    {.kind = "function",
     .name = "abs",
     .signature = "abs(x)",
     .summary = std::nullopt,
     .aliases = std::nullopt,
     .path = "sql/functions/math.md#abs(x)",
     .page = "sql/functions/math.md",
     .category = "math",
     .breadcrumb = "Math Functions"},
  };
  const auto decoded = DecodeObjects(EncodeObjects(objects));
  ASSERT_EQ(decoded.size(), objects.size());
  for (size_t i = 0; i < objects.size(); ++i) {
    EXPECT_EQ(ObjectFields(decoded[i]), ObjectFields(objects[i]));
  }
}

TEST(DocsPaths, HeadingDepthCountsUnescapedHashes) {
  EXPECT_EQ(HeadingDepth("page.md"), 0U);
  EXPECT_EQ(HeadingDepth("page.md#Title"), 1U);
  EXPECT_EQ(HeadingDepth("page.md#Title#Section"), 2U);
  EXPECT_EQ(HeadingDepth("page.md#A\\#B"), 1U);
}

TEST(DocsRender, ResolvesRelativeDocLinks) {
  const auto out = connector::RenderMarkdown(
    "see [dp](../../sql/functions/datepart.md)", 0, /*color=*/false,
    "sql/functions/timestamp.md#Timestamp_Functions");
  EXPECT_NE(out.find("sql/functions/datepart.md"), std::string::npos) << out;
}

TEST(DocsRender, ResolvesLinksFromSectionsWithSlashes) {
  const auto out = connector::RenderMarkdown(
    "see [arr](../../sql/data_types/array.md)", 0, /*color=*/false,
    "sql/data_types/index.md#Data_Types#Nested_/_Composite_Types");
  EXPECT_NE(out.find("(sql/data_types/array.md)"), std::string::npos) << out;
}

TEST(DocsRender, RejectsNonDocLinks) {
  const auto out =
    connector::RenderMarkdown("see [x](https://example.com/x)", 0,
                              /*color=*/false, "sql/functions/timestamp.md");
  EXPECT_EQ(out.find("example.com"), std::string::npos) << out;
}

TEST(DocsRender, PlainModeEmitsNoEscapes) {
  const auto out = connector::RenderMarkdown(
    "# Heading\n\ntext with `code`\n\n```sql\nSELECT 1;\n```\n", 80, false, {});
  EXPECT_NE(out.find("# Heading"), std::string::npos);
  EXPECT_EQ(out.find('\x1b'), std::string::npos);
}

TEST(DocsRender, ColorModeHighlightsSql) {
  const auto out =
    connector::RenderMarkdown("```sql\nSELECT 1;\n```\n", 80, true, {});
  EXPECT_NE(out.find('\x1b'), std::string::npos);
}

TEST(DocsRender, AdjacentSpansKeepSourceSpacing) {
  const auto out =
    connector::RenderMarkdown("a `code`, then **bold**.", 80, false, {});
  EXPECT_NE(out.find("a code, then bold."), std::string::npos);
}

TEST(DocsRender, WrapsToWidthAndZeroMeansNoWrap) {
  std::string paragraph;
  for (int i = 0; i < 40; ++i) {
    paragraph += "alpha beta gamma ";
  }
  const auto wrapped = connector::RenderMarkdown(paragraph, 40, false, {});
  for (size_t begin = 0; begin < wrapped.size();) {
    const auto end = std::min(wrapped.find('\n', begin), wrapped.size());
    EXPECT_LE(end - begin, 40U);
    begin = end + 1;
  }
  const auto unwrapped = connector::RenderMarkdown(paragraph, 0, false, {});
  EXPECT_EQ(std::ranges::count(unwrapped, '\n'), 1);
}

TEST(DocsRender, EscapedTextKeepsBlankLinesAndPunctuation) {
  EXPECT_EQ(connector::RenderMarkdown(
              connector::EscapeMarkdown("*not* markdown\n\nsee `.docs` [x]"), 8,
              false, {}),
            "*not*\nmarkdown\n\nsee\n`.docs`\n[x]\n");
  EXPECT_EQ(connector::RenderMarkdown(connector::EscapeMarkdown("one line"), 0,
                                      false, {}),
            "one line\n");
}

TEST(DocsRender, HardBreaksEndTheLine) {
  EXPECT_EQ(connector::RenderMarkdown("one\\\ntwo three", 80, false, {}),
            "one\ntwo three\n");
  EXPECT_EQ(connector::RenderMarkdown("one\\\ntwo", 0, false, {}),
            "one\ntwo\n");
  EXPECT_EQ(
    connector::RenderMarkdown("12. title\\\n    `.docs x`", 80, false, {}),
    "12. title\n    .docs x\n");
}

TEST(DocsRender, AbsoluteLinksRewritesRelativeLinksOutsideCode) {
  EXPECT_EQ(
    connector::AbsoluteLinks("see [x](./a.md#b), [y](#c) and "
                             "[z](https://e.com)\n```\n[k](./k.md)\n```",
                             "dir/page.md#Title"),
    "see [x](dir/a.md#b), [y](dir/page.md#c) and "
    "[z](https://e.com)\n```\n[k](./k.md)\n```");
}

TEST(DocsRender, PunctuationStaysWithAnOverflowingCodeSpan) {
  EXPECT_EQ(connector::RenderMarkdown("say `aa bb cc dd`. next", 10, false, {}),
            "say aa bb cc dd.\nnext\n");
}

TEST(DocsRender, DeepNestingRendersFlat) {
  const auto quotes = connector::RenderMarkdown(
    std::string(200000, '>') + " deep", 80, false, {});
  EXPECT_NE(quotes.find("deep"), std::string::npos);
  const auto emphasis = connector::RenderMarkdown(
    std::string(100000, '*') + "deep" + std::string(100000, '*'), 80, false,
    {});
  EXPECT_NE(emphasis.find("deep"), std::string::npos);
}

TEST(DocsRender, ShallowNestingKeepsItsLayout) {
  const auto out = connector::RenderMarkdown(
    "> - one\n>   - **two [x](./x.md)**\n", 80, false, "a/b.md");
  EXPECT_NE(out.find(">   - two x (a/x.md)"), std::string::npos) << out;
}

TEST(DocsRender, LinkLabelPrecedesItsResolvedTarget) {
  const auto out =
    connector::RenderMarkdown("see [`serened shell`](./serened-shell.md#x) now",
                              80, false, "clients/serened-psql.md");
  EXPECT_NE(out.find("see serened shell (clients/serened-shell.md) now"),
            std::string::npos);
}

TEST(DocsRender, NumbersLinksAndCollectsTheirTargets) {
  connector::MarkdownLinks links{.site = "https://example.org/docs/",
                                 .first = 3};
  const auto out = connector::RenderMarkdown(
    "see [psql](./serened-psql.md#options), [indexes](../sql/indexes/index.md),"
    " [psql again](./serened-psql.md#options) and [web](https://duckdb.org)",
    0, false, "clients/serened-shell.md", &links);
  EXPECT_NE(out.find("psql [3]"), std::string::npos) << out;
  EXPECT_NE(out.find("indexes [4]"), std::string::npos) << out;
  EXPECT_NE(out.find("psql again [3]"), std::string::npos) << out;
  EXPECT_NE(out.find("web [5]"), std::string::npos) << out;
  ASSERT_EQ(links.links.size(), 3U);
  EXPECT_EQ(links.links[0].label, "psql");
  EXPECT_EQ(links.links[0].page, "clients/serened-psql.md");
  EXPECT_EQ(links.links[0].anchor, "options");
  EXPECT_EQ(links.links[0].url,
            "https://example.org/docs/clients/serened-psql#options");
  EXPECT_EQ(links.links[1].url, "https://example.org/docs/sql/indexes");
  EXPECT_TRUE(links.links[2].page.empty());
  EXPECT_EQ(links.links[2].url, "https://duckdb.org");
}

TEST(DocsRender, ColorModeMakesLinksHyperlinks) {
  connector::MarkdownLinks links{.site = "https://example.org/docs/"};
  const auto out = connector::RenderMarkdown("see [web](https://duckdb.org)", 0,
                                             true, "a.md", &links);
  EXPECT_NE(out.find("\x1b]8;;https://duckdb.org\x1b\\"), std::string::npos);
  EXPECT_NE(out.find("\x1b]8;;\x1b\\"), std::string::npos);
  const auto plain = connector::RenderMarkdown("see [web](https://duckdb.org)",
                                               0, false, "a.md", &links);
  EXPECT_EQ(plain.find('\x1b'), std::string::npos);
}

TEST(DocsRender, TableCellsWrapInsteadOfTruncating) {
  const auto out = connector::RenderMarkdown(
    "| Function | Description |\n"
    "| --- | --- |\n"
    "| approx_count_distinct(x) | Calculates the approximate count of distinct "
    "elements using HyperLogLog. |\n",
    60, false, {});
  EXPECT_EQ(out.find("\xE2\x80\xA6"), std::string::npos);
  EXPECT_NE(out.find("approx_count_distinct(x)"), std::string::npos);
  EXPECT_NE(out.find("HyperLogLog."), std::string::npos);
}

std::string StripAnsi(std::string_view text) {
  std::string out;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '\x1b' && i + 1 < text.size() && text[i + 1] == '[') {
      i += 2;
      while (i < text.size() && (text[i] < '@' || text[i] > '~')) {
        ++i;
      }
      continue;
    }
    if (text[i] == '\x1b' && i + 1 < text.size() && text[i + 1] == ']') {
      const auto end = text.find("\x1b\\", i + 2);
      i = end == std::string_view::npos ? text.size() : end + 1;
      continue;
    }
    out.push_back(text[i]);
  }
  return out;
}

TEST(DocsRender, HyperlinksKeepTableBordersAligned) {
  connector::MarkdownLinks links{.site = "https://example.org/docs/"};
  const auto out = connector::RenderMarkdown(
    "| Function | Description |\n"
    "| --- | --- |\n"
    "| [today()](./date.md#today) | Current date. |\n"
    "| now() | Same as [DuckDB](https://duckdb.org/docs/). |\n",
    80, true, "sql/functions/index.md", &links);
  EXPECT_NE(out.find("\x1b]8;;https://duckdb.org/docs/\x1b\\"),
            std::string::npos);
  std::vector<size_t> widths;
  for (const std::string_view line :
       absl::StrSplit(out, '\n', absl::SkipEmpty())) {
    widths.push_back(StripAnsi(line).size());
  }
  ASSERT_GT(widths.size(), 3U) << out;
  EXPECT_TRUE(absl::c_all_of(
    widths, [&](size_t width) { return width == widths.front(); }))
    << StripAnsi(out);
}

TEST(DocsRender, ColorTableBordersAlign) {
  const auto out = connector::RenderMarkdown(
    "| Function | Description |\n"
    "| --- | --- |\n"
    "| `date_add(`*`date`*`, `*`interval`*`)` | Add the `interval` to the "
    "date. |\n"
    "| [today()](#today) | Current date. |\n",
    80, true, {});
  std::vector<size_t> widths;
  for (const std::string_view line :
       absl::StrSplit(out, '\n', absl::SkipEmpty())) {
    widths.push_back(StripAnsi(line).size());
  }
  ASSERT_GT(widths.size(), 3U) << out;
  EXPECT_TRUE(absl::c_all_of(
    widths, [&](size_t width) { return width == widths.front(); }))
    << StripAnsi(out);
}

TEST(DocsRender, GluedPunctuationWrapsWithItsWord) {
  const auto out = connector::RenderMarkdown("aaaa `bbbbb`.", 10, false, {});
  EXPECT_NE(out.find("bbbbb."), std::string::npos) << out;
}

size_t InvalidUtf8At(std::string_view text) {
  size_t i = 0;
  while (i < text.size()) {
    const auto c = static_cast<unsigned char>(text[i]);
    size_t len = 1;
    if ((c & 0x80) == 0) {
      len = 1;
    } else if ((c & 0xE0) == 0xC0) {
      len = 2;
    } else if ((c & 0xF0) == 0xE0) {
      len = 3;
    } else if ((c & 0xF8) == 0xF0) {
      len = 4;
    } else {
      return i;
    }
    if (i + len > text.size()) {
      return i;
    }
    for (size_t k = 1; k < len; ++k) {
      if ((static_cast<unsigned char>(text[i + k]) & 0xC0) != 0x80) {
        return i;
      }
    }
    i += len;
  }
  return std::string_view::npos;
}

void ExpectValidUtf8(std::string_view out, std::string_view path, bool color) {
  const auto bad = InvalidUtf8At(out);
  EXPECT_EQ(bad, std::string_view::npos)
    << path << " color=" << color << " at byte " << bad << ": ["
    << absl::CHexEscape(out.substr(bad > 40 ? bad - 40 : 0, 60)) << "]";
}

auto Pages() {
  return GetDocs() | std::views::filter([](const auto& doc) {
           return HeadingDepth(doc.path) <= 1;
         });
}

TEST(DocsRender, EveryEmbeddedPageRendersCleanly) {
  constexpr size_t kWidth = 100;
  for (const auto& doc : Pages()) {
    const auto out =
      connector::RenderMarkdown(doc.content, kWidth, false, doc.path);
    EXPECT_EQ(out.find('\x1b'), std::string::npos) << doc.path;
    if (!doc.content.empty()) {
      EXPECT_FALSE(out.empty()) << doc.path;
    }
    ExpectValidUtf8(out, doc.path, false);
    for (size_t begin = 0; begin < out.size();) {
      const auto end = std::min(out.find('\n', begin), out.size());
      const std::string_view line{out.data() + begin, end - begin};
      begin = end + 1;
      EXPECT_FALSE(line.starts_with(":::")) << doc.path << "\n" << line;
      const bool ascii_table =
        (line.starts_with("|") || line.starts_with("+")) &&
        absl::c_all_of(
          line, [](char c) { return static_cast<unsigned char>(c) < 0x80; });
      if (ascii_table) {
        EXPECT_LE(line.size(), kWidth) << doc.path << "\n" << line;
      }
    }
  }
}

TEST(DocsRender, EveryPageRendersValidUtf8InColor) {
  for (const auto& doc : Pages()) {
    ExpectValidUtf8(connector::RenderMarkdown(doc.content, 80, true, doc.path),
                    doc.path, true);
  }
}

}  // namespace
}  // namespace sdb::docs
