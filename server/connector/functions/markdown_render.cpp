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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <simdjson.h>

#include <algorithm>
#include <cstddef>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/simplified_token.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/utf8_character_utils.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <markdown_utils.hpp>
#include <string>
#include <string_view>
#include <utf8proc_wrapper.hpp>
#include <vector>


namespace sdb::connector {
namespace {

constexpr int32_t kDefaultWidth = 80;
constexpr int32_t kMaxTableWidth = 120;
constexpr size_t kMinTableColumn = 8;

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
  std::string_view reset;
};

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
  .reset = "\x1b[0m",
};

struct Run {
  std::string text;
  std::string_view style;
};

size_t Width(std::string_view text) {
  try {
    return duckdb::Utf8Proc::RenderWidth(std::string{text});
  } catch (...) {
    return text.size();
  }
}

size_t CodepointLength(char lead) {
  return irs::utf8_utils::LengthFromChar8<1>(static_cast<irs::byte_type>(lead));
}

bool IsSpace(std::string_view text, size_t pos, size_t length) {
  const auto* begin =
    reinterpret_cast<const irs::byte_type*>(text.data() + pos);
  auto it = begin;
  const auto codepoint = irs::utf8_utils::ToChar32(it, begin + length);
  return irs::utf8_utils::CharIsWhiteSpace(codepoint);
}

std::string TruncateToWidth(std::string_view text, size_t max_width) {
  if (Width(text) <= max_width) {
    return std::string{text};
  }
  std::string out;
  size_t used = 0;
  size_t pos = 0;
  while (pos < text.size()) {
    const auto length = CodepointLength(text[pos]);
    if (pos + length > text.size()) {
      break;
    }
    const auto piece = text.substr(pos, length);
    const auto piece_width = Width(piece);
    if (used + piece_width + 1 > max_width) {
      break;
    }
    out.append(piece);
    used += piece_width;
    pos += length;
  }
  out.append("…");
  return out;
}

void Emit(std::string& out, std::string_view style, std::string_view text,
          const Style& s) {
  if (text.empty()) {
    return;
  }
  if (style.empty()) {
    out.append(text);
    return;
  }
  absl::StrAppend(&out, style, text, s.reset);
}

std::vector<std::string_view> SplitWords(std::string_view text) {
  std::vector<std::string_view> words;
  size_t pos = 0;
  size_t word_start = std::string_view::npos;
  while (pos < text.size()) {
    auto length = CodepointLength(text[pos]);
    if (pos + length > text.size()) {
      length = 1;
    }
    if (IsSpace(text, pos, length)) {
      if (word_start != std::string_view::npos) {
        words.push_back(text.substr(word_start, pos - word_start));
        word_start = std::string_view::npos;
      }
    } else if (word_start == std::string_view::npos) {
      word_start = pos;
    }
    pos += length;
  }
  if (word_start != std::string_view::npos) {
    words.push_back(text.substr(word_start));
  }
  return words;
}

bool StartsWithSpace(std::string_view text) {
  return !text.empty() &&
         IsSpace(text, 0, std::min(CodepointLength(text[0]), text.size()));
}

bool EndsWithSpace(std::string_view text) {
  if (text.empty()) {
    return false;
  }
  const auto* begin = reinterpret_cast<const irs::byte_type*>(text.data());
  const auto* end = begin + text.size();
  const auto* last = irs::utf8_utils::Prev(begin, end);
  const auto offset = static_cast<size_t>(last - begin);
  return IsSpace(text, offset, text.size() - offset);
}

size_t LongestWordWidth(std::string_view text) {
  size_t longest = 0;
  for (const auto word : SplitWords(text)) {
    longest = std::max(longest, Width(word));
  }
  return longest;
}

std::string_view TakeWidth(std::string_view& text, size_t max_width) {
  size_t used = 0;
  size_t pos = 0;
  while (pos < text.size()) {
    auto length = CodepointLength(text[pos]);
    if (pos + length > text.size()) {
      length = 1;
    }
    const auto piece_width = Width(text.substr(pos, length));
    if (used + piece_width > max_width) {
      break;
    }
    used += piece_width;
    pos += length;
  }
  if (pos == 0) {
    pos = std::min(CodepointLength(text[0]), text.size());
  }
  const auto head = text.substr(0, pos);
  text.remove_prefix(pos);
  return head;
}

std::vector<std::string> WrapPlain(std::string_view text, size_t width) {
  std::vector<std::string> lines;
  if (width == 0) {
    lines.emplace_back(text);
    return lines;
  }
  std::string line;
  size_t used = 0;
  for (auto word : SplitWords(text)) {
    if (Width(word) > width) {
      if (used != 0) {
        lines.push_back(line);
        line.clear();
        used = 0;
      }
      while (Width(word) > width) {
        lines.emplace_back(TakeWidth(word, width));
      }
      if (word.empty()) {
        continue;
      }
    }
    const auto word_width = Width(word);
    if (used != 0 && used + 1 + word_width > width) {
      lines.push_back(line);
      line.clear();
      used = 0;
    }
    if (used != 0) {
      line.push_back(' ');
      ++used;
    }
    line.append(word);
    used += word_width;
  }
  if (!line.empty() || lines.empty()) {
    lines.push_back(line);
  }
  return lines;
}

void WrapRuns(std::string& out, const std::vector<Run>& runs, int32_t width,
              std::string_view indent, std::string_view hanging,
              const Style& s) {
  if (runs.empty()) {
    return;
  }
  if (width <= 0) {
    out.append(indent);
    for (const auto& run : runs) {
      Emit(out, run.style, run.text, s);
    }
    out.push_back('\n');
    return;
  }
  const auto budget = static_cast<size_t>(width);
  std::string line;
  size_t used = 0;
  bool first_line = true;
  auto flush = [&] {
    if (!line.empty()) {
      absl::StrAppend(&out, first_line ? indent : hanging, line, "\n");
      first_line = false;
      line.clear();
      used = 0;
    }
  };
  const auto prefix_width = Width(indent);
  bool pending_space = false;
  for (const auto& run : runs) {
    if (StartsWithSpace(run.text)) {
      pending_space = true;
    }
    bool first_word = true;
    for (const auto word : SplitWords(run.text)) {
      const auto word_width = Width(word);
      const auto separated = used != 0 && (pending_space || !first_word);
      const size_t sep = separated ? 1 : 0;
      if (used != 0 && used + sep + word_width + prefix_width > budget) {
        flush();
      } else if (separated) {
        line.push_back(' ');
        ++used;
      }
      if (run.style.empty()) {
        line.append(word);
      } else {
        absl::StrAppend(&line, run.style, word, s.reset);
      }
      used += word_width;
      first_word = false;
      pending_space = false;
    }
    if (EndsWithSpace(run.text)) {
      pending_space = true;
    }
  }
  flush();
}

std::string HighlightSql(const std::string& code, const Style& s) {
  if (s.reset.empty()) {
    return code;
  }
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
    Emit(out, style, span, s);
  }
  return out;
}

std::string Dirname(std::string_view path) {
  const auto slash = path.find_last_of('/');
  return slash == std::string_view::npos ? std::string{}
                                         : std::string{path.substr(0, slash)};
}

std::string ResolveHref(std::string_view base_path, std::string_view href) {
  auto target = href;
  if (const auto hash = target.find('#'); hash != std::string_view::npos) {
    target = target.substr(0, hash);
  }
  if (!target.ends_with(".md") && !target.ends_with(".mdx")) {
    return {};
  }
  const auto dir = Dirname(base_path);
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

void AppendInlineMarkdown(std::vector<Run>& runs, std::string_view text,
                          std::string_view base_path, const Style& s) {
  size_t pos = 0;
  std::string plain;
  auto flush_plain = [&] {
    if (!plain.empty()) {
      runs.push_back({plain, {}});
      plain.clear();
    }
  };
  while (pos < text.size()) {
    if (text[pos] == '[') {
      const auto close = text.find("](", pos);
      if (close != std::string_view::npos) {
        const auto paren = text.find(')', close + 2);
        if (paren != std::string_view::npos) {
          flush_plain();
          const auto label = text.substr(pos + 1, close - pos - 1);
          const auto href = text.substr(close + 2, paren - close - 2);
          runs.push_back({std::string{label}, s.link});
          const auto resolved = ResolveHref(base_path, href);
          if (!resolved.empty()) {
            runs.push_back({absl::StrCat(" (", resolved, ")"), s.target});
          }
          pos = paren + 1;
          continue;
        }
      }
    }
    if (text[pos] == '`') {
      const auto close = text.find('`', pos + 1);
      if (close != std::string_view::npos) {
        flush_plain();
        runs.push_back(
          {std::string{text.substr(pos + 1, close - pos - 1)}, s.inline_code});
        pos = close + 1;
        continue;
      }
    }
    plain.push_back(text[pos]);
    ++pos;
  }
  flush_plain();
}

std::string FlattenInline(std::string_view text) {
  std::string out;
  size_t pos = 0;
  while (pos < text.size()) {
    if (text[pos] == '[') {
      const auto close = text.find("](", pos);
      if (close != std::string_view::npos) {
        const auto paren = text.find(')', close + 2);
        if (paren != std::string_view::npos) {
          out.append(text.substr(pos + 1, close - pos - 1));
          pos = paren + 1;
          continue;
        }
      }
    }
    if (text[pos] == '`') {
      const auto close = text.find('`', pos + 1);
      if (close != std::string_view::npos) {
        out.append(text.substr(pos + 1, close - pos - 1));
        pos = close + 1;
        continue;
      }
    }
    out.push_back(text[pos]);
    ++pos;
  }
  return out;
}

using Blocks = std::vector<duckdb::markdown_utils::MarkdownBlock>;

std::string Attribute(const duckdb::markdown_utils::MarkdownBlock& block,
                      const std::string& key) {
  const auto it = block.attributes.find(key);
  return it == block.attributes.end() ? std::string{} : it->second;
}

std::string_view InlineStyle(const std::string& block_type, const Style& s) {
  if (block_type == "code") {
    return s.inline_code;
  }
  if (block_type == "bold") {
    return s.bold;
  }
  if (block_type == "italic") {
    return s.italic;
  }
  if (block_type == "link") {
    return s.link;
  }
  return {};
}

size_t CollectInlines(const Blocks& blocks, size_t index, int32_t level,
                      std::string_view inherited, std::vector<Run>& runs,
                      std::string_view base_path, const Style& s) {
  size_t next = index;
  while (next < blocks.size() && blocks[next].kind == "inline" &&
         blocks[next].level >= level) {
    const auto& child = blocks[next];
    ++next;
    const auto own = InlineStyle(child.block_type, s);
    const auto style = own.empty() ? inherited : own;
    if (child.content.empty()) {
      next =
        CollectInlines(blocks, next, child.level + 1, style, runs, base_path, s);
    } else {
      runs.push_back({child.content, style});
    }
    if (child.block_type == "link") {
      const auto resolved = ResolveHref(base_path, Attribute(child, "href"));
      if (!resolved.empty()) {
        runs.push_back({absl::StrCat(" (", resolved, ")"), s.target});
      }
    }
  }
  return next;
}

void RenderList(std::string& out, const std::string& json, bool ordered,
                int32_t width, std::string_view base_path, const Style& s) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(json).get(doc) != simdjson::SUCCESS) {
    return;
  }
  simdjson::dom::array items;
  if (doc.get(items) != simdjson::SUCCESS) {
    return;
  }
  size_t n = 0;
  for (auto item : items) {
    std::string_view text;
    if (item.get(text) != simdjson::SUCCESS) {
      continue;
    }
    ++n;
    const auto marker = ordered ? absl::StrCat(n, ". ") : std::string{"- "};
    std::vector<Run> runs;
    AppendInlineMarkdown(runs, text, base_path, s);
    std::string bullet;
    Emit(bullet, s.layout, marker, s);
    const std::string hanging(marker.size(), ' ');
    std::string body;
    WrapRuns(body, runs,
             width == 0 ? 0 : width - static_cast<int32_t>(marker.size()), {},
             {}, s);
    bool first = true;
    for (const std::string_view line :
         absl::StrSplit(body, '\n', absl::SkipEmpty())) {
      absl::StrAppend(&out, first ? bullet : hanging, line, "\n");
      first = false;
    }
  }
}

void RenderTable(std::string& out, const std::string& json, int32_t width,
                 const Style& s) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(json).get(doc) != simdjson::SUCCESS) {
    return;
  }
  std::vector<std::string> headers;
  std::vector<std::vector<std::string>> rows;
  simdjson::dom::array header_array;
  if (doc["headers"].get(header_array) == simdjson::SUCCESS) {
    for (auto cell : header_array) {
      std::string_view text;
      headers.emplace_back(
        FlattenInline(cell.get(text) == simdjson::SUCCESS ? text : ""));
    }
  }
  simdjson::dom::array row_array;
  if (doc["rows"].get(row_array) == simdjson::SUCCESS) {
    for (auto row : row_array) {
      simdjson::dom::array cells;
      if (row.get(cells) != simdjson::SUCCESS) {
        continue;
      }
      auto& target = rows.emplace_back();
      for (auto cell : cells) {
        std::string_view text;
        target.emplace_back(
          FlattenInline(cell.get(text) == simdjson::SUCCESS ? text : ""));
      }
    }
  }
  if (headers.empty() && rows.empty()) {
    return;
  }
  std::vector<size_t> widths(headers.size(), 0);
  for (size_t i = 0; i < headers.size(); ++i) {
    widths[i] = Width(headers[i]);
  }
  for (const auto& row : rows) {
    widths.resize(std::max(widths.size(), row.size()), 0);
    for (size_t i = 0; i < row.size(); ++i) {
      widths[i] = std::max(widths[i], Width(row[i]));
    }
  }
  std::vector<size_t> floors(widths.size(), kMinTableColumn);
  for (size_t i = 0; i < headers.size() && i < floors.size(); ++i) {
    floors[i] = std::max(floors[i], LongestWordWidth(headers[i]));
  }
  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size() && i < floors.size(); ++i) {
      floors[i] = std::max(floors[i], LongestWordWidth(row[i]));
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
  shrink(std::vector<size_t>(widths.size(), kMinTableColumn));
  auto pad_to = [&](std::string value, size_t column) {
    const auto value_width = Width(value);
    if (value_width > widths[column]) {
      value = TruncateToWidth(value, widths[column]);
    }
    const auto padded = Width(value);
    return value +
           std::string(widths[column] > padded ? widths[column] - padded : 0,
                       ' ');
  };
  auto row_line = [&](const std::vector<std::string>& cells,
                      std::string_view style) {
    std::vector<std::vector<std::string>> lines(widths.size());
    size_t height = 1;
    for (size_t i = 0; i < widths.size(); ++i) {
      lines[i] =
        WrapPlain(i < cells.size() ? cells[i] : std::string{}, widths[i]);
      height = std::max(height, lines[i].size());
    }
    for (size_t line = 0; line < height; ++line) {
      Emit(out, s.layout, "|", s);
      for (size_t i = 0; i < widths.size(); ++i) {
        Emit(out, s.layout, " ", s);
        Emit(out, style,
             pad_to(line < lines[i].size() ? lines[i][line] : std::string{}, i),
             s);
        Emit(out, s.layout, " |", s);
      }
      out.push_back('\n');
    }
  };
  std::string rule;
  for (const auto column : widths) {
    absl::StrAppend(&rule, "+", std::string(column + 2, '-'));
  }
  rule.push_back('+');
  auto rule_line = [&] {
    Emit(out, s.layout, rule, s);
    out.push_back('\n');
  };
  rule_line();
  if (!headers.empty()) {
    row_line(headers, s.bold);
    rule_line();
  }
  for (const auto& row : rows) {
    row_line(row, {});
  }
  rule_line();
}

}  // namespace

std::string RenderMarkdown(std::string_view markdown, int32_t width, bool color,
                           std::string_view base_path) {
  const auto& s = color ? kAnsi : kPlain;
  const auto blocks =
    duckdb::markdown_utils::ParseBlocks(std::string{markdown}, true);
  std::string out;
  for (size_t i = 0; i < blocks.size();) {
    const auto& block = blocks[i];
    if (block.kind == "inline") {
      ++i;
      continue;
    }
    const auto& type = block.block_type;
    if (type == "heading") {
      std::vector<Run> runs;
      auto level = block.level;
      if (const auto attr = Attribute(block, "heading_level"); !attr.empty()) {
        level = std::atoi(attr.c_str());
      }
      std::string marker(static_cast<size_t>(std::clamp(level, 1, 6)), '#');
      marker.push_back(' ');
      runs.push_back({marker, s.layout});
      if (!block.content.empty()) {
        runs.push_back({block.content, s.heading});
      }
      const auto next =
        CollectInlines(blocks, i + 1, block.level + 1, s.heading, runs,
                       base_path, s);
      if (!out.empty()) {
        out.push_back('\n');
      }
      WrapRuns(out, runs, width, {}, {}, s);
      out.push_back('\n');
      i = next;
      continue;
    }
    if (type == "paragraph") {
      std::vector<Run> runs;
      if (!block.content.empty()) {
        AppendInlineMarkdown(runs, block.content, base_path, s);
      }
      const auto next =
        CollectInlines(blocks, i + 1, block.level + 1, {}, runs, base_path, s);
      WrapRuns(out, runs, width, {}, {}, s);
      out.push_back('\n');
      i = next;
      continue;
    }
    if (type == "code") {
      const auto language = Attribute(block, "language");
      const auto body =
        language == "sql" ? HighlightSql(block.content, s) : block.content;
      for (const std::string_view line : absl::StrSplit(body, '\n')) {
        Emit(out, s.layout, "    ", s);
        absl::StrAppend(&out, line, "\n");
      }
      out.push_back('\n');
      ++i;
      continue;
    }
    if (type == "blockquote") {
      std::vector<Run> runs;
      if (!block.content.empty()) {
        runs.push_back({block.content, s.quote});
      }
      const auto next = CollectInlines(blocks, i + 1, block.level + 1,
                                       s.quote, runs, base_path, s);
      std::string gutter;
      Emit(gutter, s.layout, "> ", s);
      WrapRuns(out, runs, width == 0 ? 0 : width - 2, gutter, gutter, s);
      out.push_back('\n');
      i = next;
      continue;
    }
    if (type == "list") {
      RenderList(out, block.content, Attribute(block, "ordered") == "true",
                 width, base_path, s);
      out.push_back('\n');
      ++i;
      continue;
    }
    if (type == "table") {
      RenderTable(out, block.content, width, s);
      out.push_back('\n');
      ++i;
      continue;
    }
    if (type == "hr") {
      const auto span = static_cast<size_t>(width <= 0 ? kDefaultWidth : width);
      Emit(out, s.rule, std::string(std::min<size_t>(span, kDefaultWidth), '-'),
           s);
      out.append("\n\n");
      ++i;
      continue;
    }
    if (type == "frontmatter") {
      ++i;
      continue;
    }
    if (!block.content.empty()) {
      std::vector<Run> runs{{block.content, {}}};
      WrapRuns(out, runs, width, {}, {}, s);
      out.push_back('\n');
    }
    ++i;
  }
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

namespace {

template<typename T>
duckdb::Vector Defaulted(duckdb::Vector& input, duckdb::idx_t count,
                         T fallback) {
  duckdb::UnifiedVectorFormat source;
  input.ToUnifiedFormat(count, source);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<T>(source);
  duckdb::Vector out{input.GetType()};
  auto* target = duckdb::FlatVector::GetDataMutable<T>(out);
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto index = source.sel->get_index(row);
    target[row] =
      source.validity.RowIsValid(index) ? values[index] : fallback;
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
    "sdb_md_to_ansi",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::INTEGER,
     duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::VARCHAR,
    MarkdownToAnsiFunction};
  render.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  loader.RegisterFunction(std::move(render));
}

}  // namespace sdb::connector
