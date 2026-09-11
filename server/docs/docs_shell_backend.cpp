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

#include "docs/docs_shell_backend.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>

#include <map>
#include <shell_docs.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/functions/markdown_render.h"
#include "docs/docs_lookup.h"

namespace sdb::docs {
namespace {

constexpr size_t kMaxCandidates = 10;

bool LooksLikePath(std::string_view arg) {
  return arg.contains('/') || arg.contains('#') || arg.ends_with(".md") ||
         arg.ends_with(".mdx");
}

std::string Section(std::string_view path) {
  const auto slash = path.find('/');
  return std::string{slash == std::string_view::npos ? path
                                                     : path.substr(0, slash)};
}

std::string RenderIndex() {
  std::map<std::string, size_t> sections;
  for (const auto& doc : GetDocs()) {
    ++sections[Section(doc.path)];
  }
  std::string out{"SereneDB documentation\n\n"};
  for (const auto& [name, count] : sections) {
    absl::StrAppend(&out, "  ", name, " (", count, ")\n");
  }
  absl::StrAppend(&out,
                  "\nUse .docs <section> to browse, .docs <name> to look up an "
                  "object, or .docs --search <query>.\n");
  return out;
}

std::string RenderEntry(const Doc& doc, int32_t width, bool color) {
  std::string out = absl::StrCat("path: ", doc.path, "\n");
  if (!doc.breadcrumb.empty()) {
    absl::StrAppend(&out, "in: ", doc.breadcrumb, "\n");
  }
  absl::StrAppend(&out, "\n");
  const auto body = absl::StrCat("# ", doc.title, "\n\n", doc.content);
  absl::StrAppend(&out,
                  connector::RenderMarkdown(body, width, color, doc.path));
  if (const auto children = Children(doc.path); !children.empty()) {
    absl::StrAppend(&out, "\nSections\n");
    for (const auto* child : children) {
      absl::StrAppend(&out, "  ", child->title, "\n     .docs ", child->path,
                      "\n");
    }
  }
  return out;
}

std::string Snippet(std::string_view content, size_t limit) {
  static constexpr std::string_view kMarkers{"#`*|"};
  std::string flat;
  const auto append_filtered = [&](std::string_view text) {
    for (const char c : text) {
      if (kMarkers.find(c) == std::string_view::npos) {
        flat.push_back(c);
      }
    }
  };
  size_t pos = 0;
  while (pos < content.size()) {
    if (content[pos] == '[') {
      const auto close = content.find("](", pos);
      const auto paren = close == std::string_view::npos
                           ? std::string_view::npos
                           : content.find(')', close + 2);
      if (paren != std::string_view::npos) {
        append_filtered(content.substr(pos + 1, close - pos - 1));
        pos = paren + 1;
        continue;
      }
    }
    append_filtered(content.substr(pos, 1));
    ++pos;
  }

  std::string out;
  bool space = false;
  for (const char c : flat) {
    if (c == '\n' || c == '\t' || c == ' ') {
      space = !out.empty();
      continue;
    }
    if (space) {
      out.push_back(' ');
      space = false;
    }
    out.push_back(c);
    if (out.size() >= limit) {
      out.append("…");
      break;
    }
  }
  return out;
}

std::string RenderHits(std::string_view heading,
                       const std::vector<const Doc*>& hits) {
  std::string out{heading};
  absl::StrAppend(&out, "\n\n");
  size_t n = 0;
  for (const auto* doc : hits) {
    absl::StrAppend(&out, "  ", ++n, ". ", doc->title);
    if (!doc->breadcrumb.empty()) {
      absl::StrAppend(&out, "  —  ", doc->breadcrumb);
    }
    absl::StrAppend(&out, "\n");
    if (const auto snippet = Snippet(doc->content, 96); !snippet.empty()) {
      absl::StrAppend(&out, "     ", snippet, "\n");
    }
    absl::StrAppend(&out, "     .docs ", doc->path, "\n\n");
  }
  return out;
}

std::string SqlLiteral(std::string_view text) {
  return absl::StrCat("'", absl::StrReplaceAll(text, {{"'", "''"}}), "'");
}

bool ServerSearch(const duckdb_shell::DocsRequest& request,
                  std::string_view query, std::string& out) {
  const auto inner = absl::StrCat(
    "SELECT string_agg(path || chr(31) || title || chr(31) || breadcrumb || "
    "chr(31) || left(snippet, 96), chr(30) ORDER BY score DESC) FROM "
    "sdb_docs.search(",
    SqlLiteral(SqlLiteral(query)), ", ", kMaxCandidates, ")");
  std::string packed;
  if (!request.query(absl::StrCat("SELECT * FROM postgres_query('postgres', ",
                                  SqlLiteral(inner), ")"),
                     packed) ||
      packed.empty()) {
    return false;
  }
  std::string heading =
    absl::StrCat("Documentation matching '", query, "':\n\n");
  size_t n = 0;
  for (const auto row : absl::StrSplit(packed, '\x1e', absl::SkipEmpty())) {
    const std::vector<std::string> fields = absl::StrSplit(row, '\x1f');
    if (fields.size() < 3) {
      continue;
    }
    absl::StrAppend(&heading, "  ", ++n, ". ", fields[1]);
    if (!fields[2].empty()) {
      absl::StrAppend(&heading, "  —  ", fields[2]);
    }
    absl::StrAppend(&heading, "\n");
    if (fields.size() > 3 && !fields[3].empty()) {
      absl::StrAppend(&heading, "     ", Snippet(fields[3], 96), "\n");
    }
    absl::StrAppend(&heading, "     .docs ", fields[0], "\n\n");
  }
  if (n == 0) {
    return false;
  }
  out = heading;
  return true;
}

std::string RenderSearch(std::string_view query) {
  const auto hits = Similar(query, kMaxCandidates);
  if (hits.empty()) {
    return absl::StrCat("Nothing in the documentation matches '", query,
                        "'.\n");
  }
  return RenderHits(absl::StrCat("Documentation matching '", query, "':"),
                    hits);
}

std::string RenderCandidates(std::string_view name) {
  const auto similar = Similar(name, kMaxCandidates);
  if (similar.empty()) {
    return absl::StrCat("No documentation matches '", name,
                        "'.\n\nUse .docs with no argument to list the "
                        "sections.\n");
  }
  return RenderHits(
    absl::StrCat("No entry is named '", name, "'. Closest matches:"), similar);
}

bool Run(const duckdb_shell::DocsRequest& request, std::string& out) {
  const auto width = static_cast<int32_t>(request.width);
  auto args = request.args;

  bool search = false;
  bool list = false;
  while (!args.empty() && args.front().starts_with("-")) {
    const auto flag = args.front();
    args.erase(args.begin());
    if (flag == "-s" || flag == "--search") {
      search = true;
    } else if (flag == "--list") {
      list = true;
    } else {
      out = absl::StrCat("Unknown option: ", flag,
                         "\n\nUsage: .docs ?--search|--list? ?NAME|PATH?\n");
      return false;
    }
  }
  const auto term = absl::StrJoin(args, " ");

  if (list) {
    const auto hits = ListPrefix(term, /*pages_only=*/term.empty());
    if (hits.empty()) {
      out = absl::StrCat("Nothing under '", term, "'.\n");
      return false;
    }
    for (const auto* doc : hits) {
      absl::StrAppend(&out, doc->path, "  —  ", doc->title, "\n");
    }
    return true;
  }

  if (search) {
    if (term.empty()) {
      out = "Usage: .docs --search <query>\n";
      return false;
    }
    if (request.query && ServerSearch(request, term, out)) {
      return true;
    }
    out = RenderSearch(term);
    return !out.starts_with("Nothing");
  }

  if (term.empty()) {
    out = RenderIndex();
    return true;
  }

  if (LooksLikePath(term)) {
    if (const auto* doc = FindByPath(term); doc != nullptr) {
      out = RenderEntry(*doc, width, request.color);
      return true;
    }
  }

  if (const auto under = ListPrefix(absl::StrCat(term, "/"),
                                    /*pages_only=*/true);
      !under.empty()) {
    absl::StrAppend(&out, "Documentation under '", term, "':\n\n");
    for (const auto* doc : under) {
      absl::StrAppend(&out, "  ", doc->title, "\n     .docs ", doc->path, "\n");
    }
    return true;
  }

  const auto hits = Lookup(term);
  if (hits.empty()) {
    out = RenderCandidates(term);
    return false;
  }
  for (size_t i = 0; i < hits.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(&out, "\n---\n\n");
    }
    absl::StrAppend(&out, RenderEntry(*hits[i], width, request.color));
  }
  return true;
}

}  // namespace

void RegisterShellDocsBackend() {
  duckdb_shell::RegisterDocsBackend({.run =
                                       [](const auto& request, auto& out) {
                                         std::string rendered;
                                         const bool ok = Run(request, rendered);
                                         out = rendered;
                                         return ok;
                                       },
                                     .complete =
                                       [](const auto& prefix) {
                                         duckdb::vector<duckdb::string> out;
                                         for (const auto& candidate :
                                              CompletePath(prefix, 40)) {
                                           out.push_back(candidate);
                                         }
                                         return out;
                                       }});
}

}  // namespace sdb::docs
