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

#include "connector/functions/markdown_render.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <cmark-gfm-core-extensions.h>
#include <cmark-gfm-extension_api.h>
#include <cmark-gfm.h>

#include <algorithm>
#include <cstddef>
#include <duckdb/common/box_renderer.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/simplified_token.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <markdown_utils.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utf8proc_wrapper.hpp>
#include <vector>

namespace sdb::connector {

std::string ResolveHref(std::string_view base_path, std::string_view href) {
  auto target = href;
  if (const auto hash = target.find('#'); hash != std::string_view::npos) {
    target = target.substr(0, hash);
  }
  if (!target.ends_with(".md") && !target.ends_with(".mdx")) {
    return {};
  }
  const auto page = base_path.substr(0, base_path.find('#'));
  const auto slash = page.find_last_of('/');
  const auto dir = slash == std::string_view::npos ? std::string_view{}
                                                   : page.substr(0, slash);
  const auto combined = dir.empty() || target.starts_with('/')
                          ? std::string{target}
                          : absl::StrCat(dir, "/", target);
  std::vector<std::string_view> stack;
  for (const std::string_view part :
       absl::StrSplit(combined, '/', absl::SkipEmpty())) {
    if (part == "..") {
      if (!stack.empty()) {
        stack.pop_back();
      }
    } else if (part != ".") {
      stack.push_back(part);
    }
  }
  return absl::StrJoin(stack, "/");
}

bool IsExternal(std::string_view href) {
  return href.starts_with("http://") || href.starts_with("https://") ||
         href.starts_with("mailto:");
}

std::string AbsoluteLinks(std::string_view markdown,
                          std::string_view base_path) {
  const auto page = base_path.substr(0, base_path.find('#'));
  bool fenced = false;
  return absl::StrJoin(
    absl::StrSplit(markdown, '\n'), "\n",
    [&](std::string* out, std::string_view line) {
      if (absl::StripLeadingAsciiWhitespace(line).starts_with("```")) {
        fenced = !fenced;
      }
      if (fenced) {
        out->append(line);
        return;
      }
      size_t from = 0;
      for (auto at = line.find("](", from); at != std::string_view::npos;
           at = line.find("](", from)) {
        const auto start = at + 2;
        const auto end = line.find_first_of(") \t", start);
        if (end == std::string_view::npos || line[end] != ')') {
          break;
        }
        const auto href = line.substr(start, end - start);
        std::string target;
        if (href.starts_with('#')) {
          target = absl::StrCat(page, href);
        } else if (!IsExternal(href)) {
          if (auto resolved = ResolveHref(base_path, href); !resolved.empty()) {
            const auto hash = href.find('#');
            target = hash == std::string_view::npos
                       ? std::move(resolved)
                       : absl::StrCat(resolved, href.substr(hash));
          }
        }
        out->append(line.substr(from, start - from));
        out->append(target.empty() ? href : std::string_view{target});
        from = end;
      }
      out->append(line.substr(from));
    });
}

namespace {

constexpr int32_t kDefaultWidth = 80;
constexpr int32_t kMaxTableWidth = 120;
constexpr size_t kMinTableColumn = 8;
constexpr size_t kMaxNesting = 32;

struct Style {
  std::string_view heading;
  std::string_view rule;
  std::string_view inline_code;
  std::string_view link;
  std::string_view target;
  std::string_view quote;
  std::string_view bold;
  std::string_view italic;
  std::string_view layout;
  std::string_view keyword;
  std::string_view literal;
  std::string_view number;
  std::string_view comment;
};

constexpr std::string_view kReset = "\x1b[0m";

constexpr Style kPlain{};

constexpr Style kAnsi{
  .heading = "\x1b[1m",
  .rule = "\x1b[2m",
  .inline_code = "\x1b[36m",
  .link = "\x1b[4m",
  .target = "\x1b[2m",
  .quote = "\x1b[2m",
  .bold = "\x1b[1m",
  .italic = "\x1b[3m",
  .layout = "\x1b[2m",
  .keyword = "\x1b[32m",
  .literal = "\x1b[33m",
  .number = "\x1b[35m",
  .comment = "\x1b[2m",
};

struct Run {
  std::string text;
  std::string_view style;
  std::string url;
  bool newline = false;
  bool atomic = false;
};

size_t Width(std::string_view text) {
  try {
    return duckdb::Utf8Proc::RenderWidth(std::string{text});
  } catch (...) {
    return text.size();
  }
}

constexpr std::string_view kBlanks = " \t\n";

void Emit(std::string& out, std::string_view style, std::string_view text,
          std::string_view url = {}) {
  if (text.empty()) {
    return;
  }
  if (!url.empty()) {
    absl::StrAppend(&out, "\x1b]8;;", url, "\x1b\\");
  }
  if (style.empty()) {
    out.append(text);
  } else {
    absl::StrAppend(&out, style, text, kReset);
  }
  if (!url.empty()) {
    out.append("\x1b]8;;\x1b\\");
  }
}

std::vector<std::string_view> SplitWords(std::string_view text) {
  return absl::StrSplit(text, absl::ByAnyChar(kBlanks), absl::SkipEmpty());
}

bool StartsWithSpace(std::string_view text) {
  return !text.empty() && kBlanks.contains(text.front());
}

bool EndsWithSpace(std::string_view text) {
  return !text.empty() && kBlanks.contains(text.back());
}

size_t LongestWordWidth(std::string_view text) {
  size_t longest = 0;
  for (const auto word : SplitWords(text)) {
    longest = std::max(longest, Width(word));
  }
  return longest;
}

std::string_view TakeWidth(std::string_view& text, size_t max_width) {
  const std::string value{text};
  duckdb::idx_t pos = 0;
  duckdb::idx_t used = 0;
  duckdb::BoxRenderer::TruncateValue(value, max_width, pos, used);
  if (pos == 0) {
    pos = duckdb::Utf8Proc::NextGraphemeCluster(value.c_str(), value.size(), 0);
  }
  const auto head = text.substr(0, pos);
  text.remove_prefix(pos);
  return head;
}

struct Line {
  std::string text;
  size_t width = 0;
};

std::vector<Line> WrapLines(const std::vector<Run>& runs, int32_t width) {
  std::vector<Line> lines;
  if (runs.empty()) {
    return lines;
  }
  if (width <= 0) {
    lines.emplace_back();
    for (const auto& run : runs) {
      if (run.newline) {
        lines.emplace_back();
        continue;
      }
      Emit(lines.back().text, run.style, run.text, run.url);
      lines.back().width += Width(run.text);
    }
    return lines;
  }
  const auto budget = static_cast<size_t>(width);
  std::string line;
  size_t used = 0;
  size_t chain_offset = 0;
  size_t chain_used = 0;
  auto flush = [&] {
    if (!line.empty()) {
      lines.push_back({.text = std::move(line), .width = used});
      line.clear();
      used = 0;
    }
    chain_offset = 0;
    chain_used = 0;
  };
  bool pending_space = false;
  for (const auto& run : runs) {
    if (run.newline) {
      flush();
      pending_space = false;
      continue;
    }
    if (StartsWithSpace(run.text)) {
      pending_space = true;
    }
    bool first_word = true;
    const auto words = run.atomic ? std::vector<std::string_view>{run.text}
                                  : SplitWords(run.text);
    for (auto word : words) {
      auto word_width = Width(word);
      const auto glued = used != 0 && !pending_space && first_word;
      const size_t sep = used != 0 && !glued ? 1 : 0;
      const bool overflows =
        (run.atomic && word_width > budget) || (glued && used > budget);
      if (used != 0 && used + sep + word_width > budget && !overflows) {
        const auto chain = used - chain_used;
        if (glued && chain_offset != 0 && line[chain_offset - 1] == ' ' &&
            chain + word_width <= budget) {
          auto moved = line.substr(chain_offset);
          line.resize(chain_offset - 1);
          used = chain_used - 1;
          flush();
          line = std::move(moved);
          used = chain;
        } else {
          flush();
        }
      } else if (sep != 0) {
        line.push_back(' ');
        ++used;
      }
      if (!glued || used == 0) {
        chain_offset = line.size();
        chain_used = used;
      }
      while (!run.atomic && word_width > budget) {
        std::string_view rest = word;
        const auto head = TakeWidth(rest, budget);
        Emit(line, run.style, head, run.url);
        used = Width(head);
        flush();
        word = rest;
        word_width = Width(word);
      }
      Emit(line, run.style, word, run.url);
      used += word_width;
      first_word = false;
      pending_space = false;
    }
    if (EndsWithSpace(run.text)) {
      pending_space = true;
    }
  }
  flush();
  return lines;
}

void WrapRuns(std::string& out, const std::vector<Run>& runs, int32_t width) {
  for (const auto& line : WrapLines(runs, width)) {
    absl::StrAppend(&out, line.text, "\n");
  }
}

std::string HighlightSql(const std::string& code, const Style& s) {
  std::vector<duckdb::SimplifiedToken> tokens;
  try {
    tokens = duckdb::Parser::Tokenize(code);
  } catch (...) {
    return code;
  }
  if (tokens.empty()) {
    return code;
  }
  std::string out;
  for (size_t i = 0; i < tokens.size(); ++i) {
    const auto start = tokens[i].start;
    if (start > code.size()) {
      break;
    }
    if (i == 0 && start > 0) {
      out.append(code, 0, start);
    }
    const auto end = i + 1 < tokens.size()
                       ? std::min(tokens[i + 1].start, code.size())
                       : code.size();
    if (end <= start) {
      continue;
    }
    const std::string_view span{code.data() + start, end - start};
    std::string_view style;
    switch (tokens[i].type) {
      case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD:
        style = s.keyword;
        break;
      case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT:
        style = s.literal;
        break;
      case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
        style = s.number;
        break;
      case duckdb::SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT:
        style = s.comment;
        break;
      default:
        break;
    }
    Emit(out, style, span);
  }
  return out;
}

std::string Route(std::string_view page) {
  for (const std::string_view extension : {".mdx", ".md"}) {
    if (page.ends_with(extension)) {
      page.remove_suffix(extension.size());
      break;
    }
  }
  if (page == "index") {
    return {};
  }
  if (constexpr std::string_view kIndex = "/index"; page.ends_with(kIndex)) {
    page.remove_suffix(kIndex.size());
  }
  return std::string{page};
}

std::string_view AsView(const char* text) {
  return text ? std::string_view{text} : std::string_view{};
}

std::string Literal(cmark_node* node) {
  return std::string{AsView(cmark_node_get_literal(node))};
}

std::string FlatText(cmark_node* root) {
  std::string out;
  auto* iter = cmark_iter_new(root);
  for (auto event = cmark_iter_next(iter); event != CMARK_EVENT_DONE;
       event = cmark_iter_next(iter)) {
    auto* node = cmark_iter_get_node(iter);
    const auto type = cmark_node_get_type(node);
    if (event == CMARK_EVENT_ENTER &&
        (type == CMARK_NODE_TEXT || type == CMARK_NODE_CODE ||
         type == CMARK_NODE_HTML_INLINE || type == CMARK_NODE_CODE_BLOCK)) {
      out.append(AsView(cmark_node_get_literal(node)));
    } else if (type == CMARK_NODE_SOFTBREAK || type == CMARK_NODE_LINEBREAK ||
               (event == CMARK_EVENT_EXIT && node != root &&
                (type & CMARK_NODE_TYPE_MASK) == CMARK_NODE_TYPE_BLOCK)) {
      out.push_back(' ');
    }
  }
  cmark_iter_free(iter);
  return out;
}

int32_t Narrower(int32_t width, size_t by) {
  return width <= 0 ? width
                    : std::max<int32_t>(width - static_cast<int32_t>(by), 1);
}

void Prefix(std::string& out, std::string_view body, std::string_view first,
            std::string_view rest, std::string_view blank) {
  while (body.ends_with('\n')) {
    body.remove_suffix(1);
  }
  if (body.empty()) {
    return;
  }
  bool is_first = true;
  for (const std::string_view line : absl::StrSplit(body, '\n')) {
    if (line.empty()) {
      absl::StrAppend(&out, blank, "\n");
    } else {
      absl::StrAppend(&out, is_first ? first : rest, line, "\n");
    }
    is_first = false;
  }
}

std::string WithoutBlankLines(std::string_view body) {
  return absl::StrJoin(absl::StrSplit(body, '\n', absl::SkipEmpty()), "\n");
}

using Document = std::unique_ptr<cmark_node, decltype(&cmark_node_free)>;

Document Parse(std::string_view markdown) {
  static auto* const kTable = [] {
    cmark_gfm_core_extensions_ensure_registered();
    return cmark_find_syntax_extension("table");
  }();
  const auto body =
    duckdb::markdown_utils::StripFrontmatter(std::string{markdown});
  auto* parser = cmark_parser_new(CMARK_OPT_DEFAULT);
  if (kTable) {
    cmark_parser_attach_syntax_extension(parser, kTable);
  }
  cmark_parser_feed(parser, body.data(), body.size());
  Document root{cmark_parser_finish(parser), &cmark_node_free};
  cmark_parser_free(parser);
  return root;
}

class Renderer {
 public:
  Renderer(bool color, std::string_view base_path, MarkdownLinks* links)
    : _s{color ? kAnsi : kPlain},
      _color{color},
      _base_path{base_path},
      _links{links} {}

  std::string Blocks(cmark_node* parent, int32_t width,
                     std::string_view text_style) {
    std::string out;
    if (_depth >= kMaxNesting) {
      WrapRuns(out, {{.text = FlatText(parent), .style = text_style}}, width);
      return out;
    }
    ++_depth;
    for (auto* node = cmark_node_first_child(parent); node;
         node = cmark_node_next(node)) {
      Block(out, node, width, text_style);
    }
    --_depth;
    return out;
  }

 private:
  void Number(std::vector<Run>& runs, size_t begin, std::string_view href) {
    MarkdownLink link;
    if (IsExternal(href)) {
      link.url = href;
    } else {
      if (const auto hash = href.find('#'); hash != std::string_view::npos) {
        link.anchor = href.substr(hash + 1);
      }
      link.page = href.starts_with('#')
                    ? std::string{_base_path.substr(0, _base_path.find('#'))}
                    : ResolveHref(_base_path, href);
      if (link.page.empty()) {
        return;
      }
      if (!_links->site.empty()) {
        link.url = absl::StrCat(_links->site, Route(link.page),
                                link.anchor.empty() ? "" : "#", link.anchor);
      }
    }
    for (auto i = begin; i < runs.size(); ++i) {
      link.label.append(runs[i].text);
      if (_color) {
        runs[i].url = link.url;
      }
    }
    link.label = std::string{absl::StripAsciiWhitespace(link.label)};
    auto& links = _links->links;
    const auto same = absl::c_find_if(links, [&](const MarkdownLink& other) {
      return other.page == link.page && other.anchor == link.anchor &&
             other.url == link.url;
    });
    const auto number =
      _links->first + static_cast<size_t>(same - links.begin());
    if (same == links.end()) {
      links.push_back(std::move(link));
    }
    runs.push_back(
      {.text = absl::StrCat(" [", number, "]"), .style = _s.target});
  }

  void Inlines(std::vector<Run>& runs, cmark_node* parent,
               std::string_view style, bool targets) {
    if (_depth >= kMaxNesting) {
      runs.push_back({.text = FlatText(parent), .style = style});
      return;
    }
    ++_depth;
    for (auto* node = cmark_node_first_child(parent); node;
         node = cmark_node_next(node)) {
      switch (cmark_node_get_type(node)) {
        case CMARK_NODE_TEXT:
        case CMARK_NODE_HTML_INLINE:
          runs.push_back({.text = Literal(node), .style = style});
          break;
        case CMARK_NODE_SOFTBREAK:
          runs.push_back({.text = " ", .style = style});
          break;
        case CMARK_NODE_LINEBREAK:
          runs.push_back({.style = style, .newline = true});
          break;
        case CMARK_NODE_CODE:
          runs.push_back(
            {.text = Literal(node), .style = _s.inline_code, .atomic = true});
          break;
        case CMARK_NODE_EMPH:
          Inlines(runs, node, _s.italic, targets);
          break;
        case CMARK_NODE_STRONG:
          Inlines(runs, node, _s.bold, targets);
          break;
        case CMARK_NODE_LINK: {
          const auto begin = runs.size();
          Inlines(runs, node, _s.link, targets);
          const auto href = AsView(cmark_node_get_url(node));
          if (_links) {
            Number(runs, begin, href);
          } else if (targets) {
            if (auto resolved = ResolveHref(_base_path, href);
                !resolved.empty()) {
              runs.push_back({.text = absl::StrCat(" (", resolved, ")"),
                              .style = _s.target});
            }
          }
          break;
        }
        default:
          Inlines(runs, node, style, targets);
          break;
      }
    }
    --_depth;
  }

  void Block(std::string& out, cmark_node* node, int32_t width,
             std::string_view text_style) {
    switch (cmark_node_get_type(node)) {
      case CMARK_NODE_PARAGRAPH: {
        std::vector<Run> runs;
        Inlines(runs, node, text_style, true);
        WrapRuns(out, runs, width);
        out.push_back('\n');
        return;
      }
      case CMARK_NODE_HEADING: {
        const auto level = std::clamp(cmark_node_get_heading_level(node), 1, 6);
        std::vector<Run> runs{
          {.text =
             absl::StrCat(std::string(static_cast<size_t>(level), '#'), " "),
           .style = _s.layout}};
        Inlines(runs, node, _s.heading, true);
        if (!out.empty()) {
          out.push_back('\n');
        }
        WrapRuns(out, runs, width);
        out.push_back('\n');
        return;
      }
      case CMARK_NODE_CODE_BLOCK: {
        const auto info = AsView(cmark_node_get_fence_info(node));
        const auto language = info.substr(0, info.find_first_of(" \t"));
        auto code = Literal(node);
        if (code.ends_with('\n')) {
          code.pop_back();
        }
        const auto body =
          language == "sql" && _color ? HighlightSql(code, _s) : code;
        for (const std::string_view line : absl::StrSplit(body, '\n')) {
          Emit(out, _s.layout, "    ");
          absl::StrAppend(&out, line, "\n");
        }
        out.push_back('\n');
        return;
      }
      case CMARK_NODE_BLOCK_QUOTE: {
        std::string gutter;
        Emit(gutter, _s.layout, "> ");
        std::string bar;
        Emit(bar, _s.layout, ">");
        Prefix(out, Blocks(node, Narrower(width, 2), _s.quote), gutter, gutter,
               bar);
        out.push_back('\n');
        return;
      }
      case CMARK_NODE_LIST:
        List(out, node, width, text_style);
        out.push_back('\n');
        return;
      case CMARK_NODE_THEMATIC_BREAK: {
        const auto span =
          static_cast<size_t>(width <= 0 ? kDefaultWidth : width);
        Emit(out, _s.rule,
             std::string(std::min<size_t>(span, kDefaultWidth), '-'));
        out.append("\n\n");
        return;
      }
      case CMARK_NODE_HTML_BLOCK: {
        const std::vector<Run> runs{{.text = Literal(node)}};
        WrapRuns(out, runs, width);
        out.push_back('\n');
        return;
      }
      default:
        break;
    }
    if (AsView(cmark_node_get_type_string(node)) == "table") {
      Table(out, node, width);
      out.push_back('\n');
      return;
    }
    out.append(Blocks(node, width, text_style));
  }

  void List(std::string& out, cmark_node* list, int32_t width,
            std::string_view text_style) {
    const bool ordered = cmark_node_get_list_type(list) == CMARK_ORDERED_LIST;
    const bool tight = cmark_node_get_list_tight(list) != 0;
    auto number = std::max(cmark_node_get_list_start(list), 1) - 1;
    for (auto* item = cmark_node_first_child(list); item;
         item = cmark_node_next(item)) {
      ++number;
      const auto marker =
        ordered ? absl::StrCat(number, ". ") : std::string{"- "};
      const auto body =
        Blocks(item, Narrower(width, marker.size()), text_style);
      std::string bullet;
      Emit(bullet, _s.layout, marker);
      Prefix(out, tight ? WithoutBlankLines(body) : body, bullet,
             std::string(marker.size(), ' '), {});
      if (!tight && cmark_node_next(item)) {
        out.push_back('\n');
      }
    }
  }

  void Table(std::string& out, cmark_node* table, int32_t width) {
    struct Cell {
      std::vector<Run> runs;
      std::string plain;
    };
    std::vector<std::vector<Cell>> rows;
    for (auto* row = cmark_node_first_child(table); row;
         row = cmark_node_next(row)) {
      const auto type = AsView(cmark_node_get_type_string(row));
      if (type != "table_header" && type != "table_row") {
        continue;
      }
      const auto style = rows.empty() ? _s.bold : std::string_view{};
      auto& cells = rows.emplace_back();
      for (auto* cell = cmark_node_first_child(row); cell;
           cell = cmark_node_next(cell)) {
        auto& target = cells.emplace_back();
        Inlines(target.runs, cell, style, false);
        for (auto& run : target.runs) {
          run.atomic = false;
          target.plain.append(run.text);
        }
      }
    }
    if (rows.empty()) {
      return;
    }
    size_t columns = 0;
    for (const auto& row : rows) {
      columns = std::max(columns, row.size());
    }
    std::vector<size_t> widths(columns, 0);
    std::vector<size_t> floors(columns, kMinTableColumn);
    for (const auto& row : rows) {
      for (size_t i = 0; i < row.size(); ++i) {
        widths[i] = std::max(widths[i], Width(row[i].plain));
        floors[i] = std::max(floors[i], LongestWordWidth(row[i].plain));
      }
    }
    const auto budget = static_cast<size_t>(
      width <= 0 ? kMaxTableWidth : std::min(width, kMaxTableWidth));
    size_t total = 1;
    for (const auto column : widths) {
      total += column + 3;
    }
    const auto shrink = [&](const std::vector<size_t>& limits) {
      while (total > budget) {
        auto widest = widths.size();
        for (size_t i = 0; i < widths.size(); ++i) {
          if (widths[i] > limits[i] &&
              (widest == widths.size() || widths[i] > widths[widest])) {
            widest = i;
          }
        }
        if (widest == widths.size()) {
          return;
        }
        --widths[widest];
        --total;
      }
    };
    shrink(floors);
    shrink(std::vector<size_t>(columns, kMinTableColumn));
    const auto row_lines = [&](const std::vector<Cell>& cells) {
      std::vector<std::vector<Line>> wrapped(columns);
      size_t height = 1;
      for (size_t i = 0; i < cells.size(); ++i) {
        wrapped[i] = WrapLines(cells[i].runs, static_cast<int32_t>(widths[i]));
        height = std::max(height, wrapped[i].size());
      }
      for (size_t line = 0; line < height; ++line) {
        Emit(out, _s.layout, "|");
        for (size_t i = 0; i < columns; ++i) {
          Emit(out, _s.layout, " ");
          size_t used = 0;
          if (line < wrapped[i].size()) {
            out.append(wrapped[i][line].text);
            used = wrapped[i][line].width;
          }
          out.append(widths[i] > used ? widths[i] - used : 0, ' ');
          Emit(out, _s.layout, " |");
        }
        out.push_back('\n');
      }
    };
    const auto rule =
      absl::StrCat("+",
                   absl::StrJoin(widths, "+",
                                 [](std::string* cell, size_t column) {
                                   cell->append(column + 2, '-');
                                 }),
                   "+");
    const auto rule_line = [&] {
      Emit(out, _s.layout, rule);
      out.push_back('\n');
    };
    rule_line();
    row_lines(rows.front());
    rule_line();
    for (size_t i = 1; i < rows.size(); ++i) {
      row_lines(rows[i]);
    }
    rule_line();
  }

  const Style& _s;
  bool _color;
  std::string_view _base_path;
  MarkdownLinks* _links;
  size_t _depth = 0;
};

}  // namespace

std::string RenderMarkdown(std::string_view markdown, int32_t width, bool color,
                           std::string_view base_path, MarkdownLinks* links) {
  const auto root = Parse(markdown);
  Renderer renderer{color, base_path, links};
  const auto out = renderer.Blocks(root.get(), width, {});
  std::string squeezed;
  squeezed.reserve(out.size());
  bool blank = false;
  for (const std::string_view line : absl::StrSplit(out, '\n')) {
    if (line.empty()) {
      blank = true;
      continue;
    }
    if (blank && !squeezed.empty()) {
      squeezed.push_back('\n');
    }
    blank = false;
    absl::StrAppend(&squeezed, line, "\n");
  }
  return squeezed;
}

std::string EscapeMarkdown(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (const auto c : text) {
    if (absl::ascii_ispunct(static_cast<unsigned char>(c))) {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  return out;
}

namespace {

template<typename T>
duckdb::Vector Defaulted(const duckdb::Vector& input, duckdb::idx_t count,
                         T fallback) {
  duckdb::UnifiedVectorFormat source;
  input.ToUnifiedFormat(source);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<T>(source);
  duckdb::Vector out{input.GetType()};
  auto* target = duckdb::FlatVector::GetDataMutable<T>(out);
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto index = source.sel->get_index(row);
    target[row] = source.validity.RowIsValid(index) ? values[index] : fallback;
  }
  duckdb::FlatVector::SetSize(out, count);
  return out;
}

void MarkdownToAnsiFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  const auto count = args.size();
  const auto width = Defaulted<int32_t>(args.data[1], count, kDefaultWidth);
  const auto color = Defaulted<bool>(args.data[2], count, true);
  const auto base_path =
    Defaulted<duckdb::string_t>(args.data[3], count, duckdb::string_t{});
  duckdb::VariadicExecutor::Execute<duckdb::string_t, duckdb::string_t, int32_t,
                                    bool, duckdb::string_t>(
    {std::cref(args.data[0]), std::cref(width), std::cref(color),
     std::cref(base_path)},
    result,
    [&](duckdb::string_t markdown, int32_t requested, bool colored,
        duckdb::string_t base) -> duckdb::string_t {
      if (requested < 0) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("sdb_md_to_ansi: width must not be negative, got ",
                  requested));
      }
      return duckdb::StringVector::AddString(
        result, RenderMarkdown(markdown.GetString(), requested, colored,
                               base.GetString()));
    });
}

}  // namespace

void RegisterMarkdownRenderFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  duckdb::ScalarFunction render{
    "sdb_md_to_ansi", {}, duckdb::LogicalType::VARCHAR, MarkdownToAnsiFunction};
  render.GetSignature()
    .AddParameter("markdown", duckdb::LogicalType::VARCHAR)
    .AddParameter("width", duckdb::LogicalType::INTEGER)
    .AddParameter("color", duckdb::LogicalType::BOOLEAN)
    .AddParameter("base_path", duckdb::LogicalType::VARCHAR);
  render.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  loader.RegisterFunction(std::move(render));
}

}  // namespace sdb::connector
