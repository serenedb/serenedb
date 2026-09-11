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
#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "connector/functions/markdown_render.h"
#include "docs/docs_lookup.h"

namespace sdb::docs {
namespace {

TEST(DocsLookup, HeadingDepthCountsUnescapedHashes) {
  EXPECT_EQ(HeadingDepth("page.md"), 0U);
  EXPECT_EQ(HeadingDepth("page.md#Title"), 1U);
  EXPECT_EQ(HeadingDepth("page.md#Title#Section"), 2U);
  EXPECT_EQ(HeadingDepth("page.md#A\\#B"), 1U);
}

TEST(DocsLookup, FindsExactPath) {
  const auto* doc = FindByPath("sql/indexes/index.md#Indexes");
  ASSERT_NE(doc, nullptr);
  EXPECT_EQ(doc->title, "Indexes");
}

TEST(DocsLookup, PromotesPageToItsTitleRow) {
  const auto* doc = FindByPath("sql/indexes/index.md");
  ASSERT_NE(doc, nullptr);
  EXPECT_EQ(doc->path, "sql/indexes/index.md#Indexes");
}

TEST(DocsLookup, UnknownPathIsNull) {
  EXPECT_EQ(FindByPath("no/such/page.md"), nullptr);
  EXPECT_EQ(FindByPath(""), nullptr);
}

TEST(DocsLookup, SignatureLookupFindsEveryOverload) {
  const auto hits = Lookup("date_trunc");
  EXPECT_GT(hits.size(), 1U);
  for (const auto* doc : hits) {
    EXPECT_TRUE(doc->title.starts_with("date_trunc("));
  }
}

TEST(DocsLookup, TitleWordPrefixFindsSuffixedHeadings) {
  const auto hits = Lookup("date_part");
  EXPECT_FALSE(hits.empty());
}

TEST(DocsLookup, LookupIsCaseInsensitive) {
  EXPECT_FALSE(Lookup("BM25").empty());
  EXPECT_FALSE(Lookup("bm25").empty());
}

TEST(DocsLookup, UnknownNameHasNoHitsButHasCandidates) {
  EXPECT_TRUE(Lookup("to_tsvector").empty());
  EXPECT_FALSE(Similar("phrase", 10).empty());
}

TEST(DocsLookup, ListPrefixPagesOnlySkipsSections) {
  const auto pages = ListPrefix("sql/functions/search/", /*pages_only=*/true);
  const auto all = ListPrefix("sql/functions/search/", /*pages_only=*/false);
  EXPECT_LT(pages.size(), all.size());
  for (const auto* doc : pages) {
    EXPECT_LE(HeadingDepth(doc->path), 1U);
  }
}

TEST(DocsLookup, ResolvesRelativeDocLinks) {
  EXPECT_EQ(ResolveDocLink("sql/functions/timestamp.md#Timestamp_Functions",
                           "../../sql/functions/datepart.md"),
            "sql/functions/datepart.md");
}

TEST(DocsLookup, RejectsNonDocAndUnresolvableLinks) {
  EXPECT_TRUE(ResolveDocLink("sql/functions/timestamp.md",
                             "https://example.com/x")
                .empty());
  EXPECT_TRUE(
    ResolveDocLink("sql/functions/timestamp.md", "../../nope/missing.md")
      .empty());
}

TEST(DocsLookup, ChildrenAreImmediateSectionsOnly) {
  const auto children = Children("sql/functions/search/scoring.md#Relevance_Scoring");
  EXPECT_FALSE(children.empty());
  for (const auto* doc : children) {
    EXPECT_EQ(HeadingDepth(doc->path), 2U);
  }
}

TEST(DocsLookup, CompletesSectionsAndPagePaths) {
  const auto sections = CompletePath("cook", 40);
  EXPECT_FALSE(sections.empty());
  EXPECT_EQ(sections.front(), "cookbook");

  const auto pages = CompletePath("sql/functions/sea", 40);
  EXPECT_FALSE(pages.empty());
  for (const auto& page : pages) {
    EXPECT_TRUE(page.starts_with("sql/functions/sea"));
    EXPECT_EQ(page.find('#'), std::string::npos);
  }
}

TEST(DocsLookup, CompletionRespectsItsLimit) {
  EXPECT_LE(CompletePath("sql/", 5).size(), 5U);
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

TEST(DocsRender, LinkLabelPrecedesItsResolvedTarget) {
  const auto out = connector::RenderMarkdown(
    "see [`serened shell`](./serened-shell.md#x) now", 80, false,
    "clients/serened-psql.md");
  EXPECT_NE(out.find("see serened shell (clients/serened-shell.md) now"),
            std::string::npos);
}

TEST(DocsRender, TableCellsWrapInsteadOfTruncating) {
  const auto out = connector::RenderMarkdown(
    "| Function | Description |\n"
    "| --- | --- |\n"
    "| approx_count_distinct(x) | Calculates the approximate count of distinct "
    "elements using HyperLogLog. |\n",
    60, false, {});
  EXPECT_EQ(out.find("…"), std::string::npos);
  EXPECT_NE(out.find("approx_count_distinct(x)"), std::string::npos);
  EXPECT_NE(out.find("HyperLogLog."), std::string::npos);
}

TEST(DocsRender, EveryEmbeddedPageRendersCleanly) {
  constexpr size_t kWidth = 100;
  for (const auto& doc : GetDocs()) {
    const auto out = connector::RenderMarkdown(doc.content, kWidth, false,
                                               doc.path);
    EXPECT_EQ(out.find('\x1b'), std::string::npos) << doc.path;
    if (!doc.content.empty()) {
      EXPECT_FALSE(out.empty()) << doc.path;
    }
    for (size_t begin = 0; begin < out.size();) {
      const auto end = std::min(out.find('\n', begin), out.size());
      const std::string_view line{out.data() + begin, end - begin};
      begin = end + 1;
      const bool ascii_table =
        (line.starts_with("|") || line.starts_with("+")) &&
        absl::c_all_of(line, [](char c) { return static_cast<unsigned char>(c) < 0x80; });
      if (ascii_table) {
        EXPECT_LE(line.size(), kWidth) << doc.path << "\n" << line;
      }
    }
  }
}

}  // namespace
}  // namespace sdb::docs
