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
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <algorithm>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <exception>
#include <initializer_list>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <shell_docs.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/functions/markdown_render.h"
#include "docs/docs_functions.h"
#include "docs/docs_index_data.h"
#include "docs/docs_search.h"

namespace sdb::docs {
namespace {

constexpr size_t kMaxCandidates = 10;
constexpr size_t kMaxCompletions = 40;
constexpr std::string_view kSeparator = "  \xE2\x80\x94  ";
constexpr std::string_view kOpenHint = "Open one with `.docs <number>`.\n";
constexpr std::string_view kDocsSite = "https://serenedb.com/docs/";

struct Choice {
  std::string title;
  std::string breadcrumb;
  std::string body;
  std::string path;
  std::optional<Object> object;
  bool directory = false;
  std::string url;
};

bool LooksLikePath(std::string_view arg) {
  return arg.contains('/') || arg.contains('#') || arg.ends_with(".md") ||
         arg.ends_with(".mdx");
}

bool IsFlag(std::string_view arg) {
  const auto word = arg.substr(arg.starts_with("--") ? 2 : 1);
  return arg.starts_with('-') && !word.empty() &&
         (arg.starts_with("--") || word.size() == 1) &&
         absl::c_all_of(word, [](char c) {
           return absl::ascii_isalpha(static_cast<unsigned char>(c)) ||
                  c == '-';
         });
}

bool IsNumber(std::string_view arg) {
  return !arg.empty() && absl::c_all_of(arg, [](char c) {
    return absl::ascii_isdigit(static_cast<unsigned char>(c));
  });
}

std::string Section(std::string_view path) {
  const auto slash = path.find('/');
  return std::string{slash == std::string_view::npos ? path
                                                     : path.substr(0, slash)};
}

std::map<std::string, size_t> TopSections(duckdb::DatabaseInstance& db) {
  std::map<std::string, size_t> sections;
  for (const auto& path : ListPaths(db, "")) {
    ++sections[Section(path)];
  }
  return sections;
}

std::string LinkTarget(duckdb::DatabaseInstance& db, std::string_view page,
                       std::string_view anchor) {
  const auto entry = ResolveLink(
    db, anchor.empty() ? std::string{page} : absl::StrCat(page, "#", anchor));
  return entry ? entry->path : std::string{page};
}

std::string Render(const duckdb_shell::DocsRequest& request,
                   std::string_view markdown, std::string_view base_path = {},
                   connector::MarkdownLinks* links = nullptr) {
  return connector::RenderMarkdown(markdown,
                                   static_cast<int32_t>(request.width),
                                   request.color, base_path, links);
}

std::string Code(std::string_view text) {
  size_t longest = 0;
  size_t run = 0;
  for (const auto c : text) {
    run = c == '`' ? run + 1 : 0;
    longest = std::max(longest, run);
  }
  const std::string fence(longest + 1, '`');
  const std::string_view pad =
    text.starts_with('`') || text.ends_with('`') ? " " : "";
  return absl::StrCat(fence, pad, text, pad, fence);
}

std::string Command(std::string_view path) {
  return Code(absl::StrCat(".docs ", path));
}

std::string Numbered(size_t n) { return absl::StrCat(n, ". "); }

std::string Item(std::string_view marker,
                 std::initializer_list<std::string> lines) {
  auto present = lines | std::views::filter([](const std::string& line) {
                   return !line.empty();
                 });
  return absl::StrCat(
    marker,
    absl::StrJoin(present.begin(), present.end(),
                  absl::StrCat("\\\n", std::string(marker.size(), ' '))),
    "\n");
}

std::string Brief(std::string_view marker, const Choice& choice) {
  return Item(marker,
              {connector::EscapeMarkdown(choice.title),
               choice.url.empty() ? Command(choice.path) : Code(choice.url)});
}

std::string Header(std::string_view path, std::string_view breadcrumb) {
  auto header = absl::StrCat("path: ", Code(path));
  if (!breadcrumb.empty()) {
    absl::StrAppend(&header, "\\\nin: ", connector::EscapeMarkdown(breadcrumb));
  }
  return absl::StrCat(header, "\n\n");
}

std::string RenderEntry(const duckdb_shell::DocsRequest& request,
                        const Entry& doc, std::vector<Choice>* sections) {
  auto& db = *request.instance;
  const auto children = Children(db, doc.path);
  connector::MarkdownLinks links{.site = kDocsSite,
                                 .first = children.size() + 1};
  auto out = Render(
    request, absl::StrCat(Header(doc.path, doc.breadcrumb), Markdown(doc)),
    doc.path, sections ? &links : nullptr);
  std::string footer;
  if (!children.empty()) {
    footer = "**Sections**\n\n";
    for (const auto& child : children) {
      const Choice choice{.title = child.title,
                          .breadcrumb = child.breadcrumb,
                          .path = child.path};
      std::string marker = "- ";
      if (sections) {
        sections->push_back(choice);
        marker = Numbered(sections->size());
      }
      footer.append(Brief(marker, choice));
    }
    footer.push_back('\n');
  }
  if (sections && !links.links.empty()) {
    footer.append("**Links**\n\n");
    for (const auto& link : links.links) {
      if (link.page.empty()) {
        sections->push_back({.title = link.label, .url = link.url});
      } else {
        sections->push_back({.title = link.label,
                             .path = LinkTarget(db, link.page, link.anchor)});
      }
      footer.append(Brief(Numbered(sections->size()), sections->back()));
    }
  }
  if (!footer.empty()) {
    absl::StrAppend(&out, "\n", Render(request, footer));
  }
  return out;
}

std::string RenderCard(const duckdb_shell::DocsRequest& request,
                       const Object& object) {
  auto markdown =
    absl::StrCat(Header(object.path, object.breadcrumb), "# ",
                 connector::EscapeMarkdown(object.signature), "\n\n");
  if (object.summary) {
    absl::StrAppend(&markdown, *object.summary, "\n\n");
  }
  absl::StrAppend(&markdown, "Kind: ", connector::EscapeMarkdown(object.kind));
  if (object.aliases) {
    absl::StrAppend(&markdown,
                    ". Aliases: ", connector::EscapeMarkdown(*object.aliases));
  }
  absl::StrAppend(&markdown, ".\n");
  return Render(request, markdown, object.path);
}

Choice EntryChoice(const Entry& entry) {
  return {.title = entry.title,
          .breadcrumb = entry.breadcrumb,
          .body = entry.content_text,
          .path = entry.path};
}

Choice ObjectChoice(const Object& object) {
  return {.title = absl::StrCat(object.signature, " (", object.kind, ")"),
          .breadcrumb = object.breadcrumb,
          .body = object.summary.value_or(""),
          .path = object.path,
          .object = object};
}

bool HasOwnSection(const std::optional<Entry>& entry, const Object& object) {
  return entry && entry->title == object.signature;
}

std::string Hit(std::string_view marker, const Choice& choice) {
  auto title =
    absl::StrCat("**", connector::EscapeMarkdown(choice.title), "**");
  if (!choice.breadcrumb.empty()) {
    absl::StrAppend(&title, kSeparator,
                    connector::EscapeMarkdown(choice.breadcrumb));
  }
  return Item(marker,
              {title, connector::EscapeMarkdown(Snippet(choice.body, 96)),
               Command(choice.path)});
}

std::string Listing(std::string_view heading,
                    const std::vector<Choice>& choices, size_t numbered = 0) {
  auto out = absl::StrCat(heading, "\n\n");
  for (const auto& choice : choices) {
    absl::StrAppend(&out, Hit(Numbered(++numbered), choice), "\n");
  }
  return out;
}

std::vector<Choice> EntryChoices(const std::vector<Entry>& entries) {
  std::vector<Choice> choices;
  choices.reserve(entries.size());
  absl::c_transform(entries, std::back_inserter(choices), EntryChoice);
  return choices;
}

std::string UnderHeading(std::string_view directory) {
  return absl::StrCat("Documentation under '",
                      connector::EscapeMarkdown(directory), "':");
}

std::vector<Choice> DirectoryChoices(duckdb::DatabaseInstance& db,
                                     std::string_view directory) {
  const auto prefix = absl::StrCat(directory, "/");
  const auto entries = ListPrefix(db, prefix, /*pages_only=*/true);
  const auto section_of = [&](const Entry& entry) {
    const auto rest = std::string_view{entry.path}.substr(prefix.size());
    const auto file = rest.substr(0, rest.find('#'));
    const auto slash = file.find('/');
    return slash == std::string_view::npos ? std::string_view{}
                                           : file.substr(0, slash);
  };
  std::map<std::string_view, size_t> pages;
  for (const auto& entry : entries) {
    if (const auto section = section_of(entry); !section.empty()) {
      ++pages[section];
    }
  }
  std::vector<Choice> choices;
  std::set<std::string_view> listed;
  for (const auto& entry : entries) {
    const auto section = section_of(entry);
    if (section.empty() || pages[section] == 1) {
      choices.push_back(EntryChoice(entry));
    } else if (listed.insert(section).second) {
      choices.push_back({.title = absl::StrCat(section, "/"),
                         .path = absl::StrCat(prefix, section),
                         .directory = true});
    }
  }
  return choices;
}

std::string NameKey(std::string_view name) {
  auto key = absl::AsciiStrToLower(absl::StripAsciiWhitespace(name));
  absl::c_replace(key, ' ', '_');
  absl::c_replace(key, '-', '_');
  return key;
}

template<typename Visit>
void ForEachPlace(duckdb::DatabaseInstance& db, Visit&& visit) {
  std::set<std::string> files;
  for (const auto& path : ListPaths(db, "")) {
    files.insert(path.substr(0, path.find('#')));
  }
  std::set<std::string_view> directories;
  for (const std::string_view file : files) {
    for (auto slash = file.find('/'); slash != std::string_view::npos;
         slash = file.find('/', slash + 1)) {
      const auto directory = file.substr(0, slash);
      if (directories.insert(directory).second) {
        visit(directory.substr(directory.rfind('/') + 1), directory, true);
      }
    }
    const auto base = file.substr(file.rfind('/') + 1);
    const auto stem = base.substr(0, base.rfind('.'));
    if (stem != "index" && stem != "overview") {
      visit(stem, file, false);
    }
  }
}

std::vector<Choice> NamedPlaces(duckdb::DatabaseInstance& db,
                                std::string_view name) {
  const auto key = NameKey(name);
  std::vector<Choice> places;
  std::vector<Choice> directories;
  ForEachPlace(
    db, [&](std::string_view place, std::string_view path, bool directory) {
      if (NameKey(place) != key) {
        return;
      }
      if (directory) {
        directories.push_back({.title = absl::StrCat(path, "/ (section)"),
                               .path = std::string{path},
                               .directory = true});
      } else if (const auto entry = FindByPath(db, path)) {
        auto page = EntryChoice(*entry);
        absl::StrAppend(&page.title, " (page)");
        places.push_back(std::move(page));
      }
    });
  absl::c_move(directories, std::back_inserter(places));
  return places;
}

std::vector<Choice> NameChoices(duckdb::DatabaseInstance& db,
                                std::string_view name, std::string_view kind) {
  std::vector<Choice> choices;
  std::vector<std::string> sections;
  for (const auto& object : FindObjects(db, name, kind)) {
    if (HasOwnSection(FindByPath(db, object.path), object)) {
      if (absl::c_linear_search(sections, object.path)) {
        continue;
      }
      sections.push_back(object.path);
    }
    choices.push_back(ObjectChoice(object));
  }
  if (kind.empty()) {
    for (auto& place : NamedPlaces(db, name)) {
      if (!absl::c_linear_search(sections, place.path)) {
        choices.push_back(std::move(place));
      }
    }
    if (choices.empty()) {
      choices = EntryChoices(Lookup(db, name, {.content_text = true}));
    }
  }
  return choices;
}

std::optional<std::string> RenderChoice(
  const duckdb_shell::DocsRequest& request, const Choice& choice,
  std::vector<Choice>* sections) {
  const auto entry = FindByPath(*request.instance, choice.path);
  if (choice.object && !HasOwnSection(entry, *choice.object)) {
    return RenderCard(request, *choice.object);
  }
  if (!entry) {
    return std::nullopt;
  }
  return RenderEntry(request, *entry, sections);
}

std::vector<duckdb_shell::DocsCompletion> KindItems(
  duckdb::DatabaseInstance& db, std::string_view flags,
  std::string_view prefix) {
  std::set<std::string> kinds;
  for (const auto& object : Objects(db)) {
    if (absl::StartsWithIgnoreCase(object.kind, prefix)) {
      kinds.insert(object.kind);
    }
  }
  std::vector<duckdb_shell::DocsCompletion> items;
  for (const auto& kind : kinds) {
    items.push_back(
      {.text = absl::StrCat(flags, "--kind ", kind, " "), .label = kind});
  }
  return items;
}

std::vector<std::string> Names(duckdb::DatabaseInstance& db,
                               std::string_view prefix, std::string_view kind) {
  std::vector<std::string> places;
  if (kind.empty()) {
    ForEachPlace(db, [&](std::string_view place, std::string_view, bool) {
      places.emplace_back(place);
    });
  }
  return CompleteName(db, prefix, kind, kMaxCompletions, places);
}

class Session {
 public:
  bool Run(const duckdb_shell::DocsRequest& request, std::string& out) {
    _offered = false;
    if (!request.instance || GetDocsIndex().empty()) {
      out =
        "This build carries no documentation index.\n\nConfigure with "
        "-DSDB_EMBEDDED_DOCS=ON to enable .docs.\n";
      return false;
    }
    auto args = request.args;

    bool search = false;
    bool list = false;
    bool all = false;
    std::string kind;
    while (!args.empty() && (args.front() == "--" || IsFlag(args.front()))) {
      const auto flag = args.front();
      args.erase(args.begin());
      if (flag == "--") {
        break;
      }
      if (flag == "-s" || flag == "--search") {
        search = true;
        break;
      }
      if (flag == "--kind") {
        if (args.empty()) {
          out = "Usage: .docs --kind KIND ?NAME?\n";
          return false;
        }
        kind = args.front();
        args.erase(args.begin());
      } else if (flag == "--list") {
        list = true;
      } else if (flag == "--all") {
        all = true;
      } else {
        out =
          absl::StrCat("Unknown option: ", flag,
                       "\n\nUsage: .docs ?--search|--list|--all|--kind KIND? "
                       "?NAME|PATH|NUMBER?\n");
        return false;
      }
    }
    const auto input = absl::StrJoin(args, " ");
    const auto term = SiteRoute(input);
    const bool url = term != input;

    if (list) {
      const auto hits =
        ListPrefix(*request.instance, term, /*pages_only=*/term.empty());
      if (hits.empty()) {
        out = absl::StrCat("Nothing under '", term, "'.\n");
        return false;
      }
      for (const auto& doc : hits) {
        absl::StrAppend(&out, doc.path, kSeparator, doc.title, "\n");
      }
      return true;
    }

    if (search) {
      if (term.empty()) {
        out = "Usage: .docs --search <query>\n";
        return false;
      }
      return RenderSearch(request, term, out);
    }

    if (kind.empty() && !all && IsNumber(term)) {
      size_t n = 0;
      if (_list.empty()) {
        out =
          "Nothing to open yet: .docs <number> opens an item of the last "
          "numbered list, and none has been printed.\n";
        return false;
      }
      if (!absl::SimpleAtoi(term, &n) || n == 0 || n > _list.size()) {
        out = absl::StrCat("No item ", term, ": the last list has ",
                           _list.size(), " items.\n");
        return false;
      }
      const auto choice = _list[n - 1];
      return Open(request, choice, out);
    }

    if (term.empty() && !kind.empty()) {
      std::vector<Choice> choices;
      absl::c_transform(Objects(*request.instance, kind),
                        std::back_inserter(choices), ObjectChoice);
      if (choices.empty()) {
        out = absl::StrCat("No objects of kind '", kind, "'.\n");
        return false;
      }
      out = Menu(request,
                 absl::StrCat("Objects of kind '",
                              connector::EscapeMarkdown(kind), "':"),
                 std::move(choices));
      return true;
    }

    if (term.empty() || term == "?") {
      out = RenderIndex(request);
      return true;
    }

    if (kind.empty()) {
      const auto directory = std::string{absl::StripSuffix(term, "/")};
      const bool listed =
        !DirectoryChoices(*request.instance, directory).empty();
      if (url || (!listed && LooksLikePath(term))) {
        if (const auto doc = ResolveLink(*request.instance, term)) {
          return Open(request, EntryChoice(*doc), out);
        }
      }
      if (listed) {
        return Open(request, {.path = directory, .directory = true}, out);
      }
    }

    auto choices = NameChoices(*request.instance, CallName(term), kind);
    if (choices.empty()) {
      if (!kind.empty()) {
        out = absl::StrCat("No ", kind, " is named '", term, "'.\n");
        return false;
      }
      const bool words = term.find_first_of(" \t") != std::string::npos;
      std::vector<Entry> similar;
      if (!words) {
        similar = Candidates(*request.instance, term, kMaxCandidates);
      }
      if (similar.empty()) {
        std::string error;
        if (auto hits = Search(*request.instance, term, kMaxCandidates,
                               Columns{.content_text = true}, error);
            !hits.empty()) {
          out = Menu(request,
                     absl::StrCat("Documentation matching '",
                                  connector::EscapeMarkdown(term), "':"),
                     EntryChoices(hits));
          return true;
        }
        if (words) {
          similar = Candidates(*request.instance, term, kMaxCandidates);
        }
      }
      out = RenderCandidates(request, term, similar);
      return false;
    }
    if (all) {
      out = RenderAll(request, choices);
      return true;
    }
    if (choices.size() == 1) {
      return Open(request, choices.front(), out);
    }
    out = Menu(request,
               absl::StrCat("'", connector::EscapeMarkdown(term), "' matches ",
                            choices.size(), " entries:"),
               std::move(choices));
    return true;
  }

  std::vector<duckdb_shell::DocsCompletion> Complete(
    duckdb::DatabaseInstance& db, std::string_view argument) const {
    const std::vector<std::string_view> words =
      absl::StrSplit(argument, ' ', absl::SkipEmpty());
    const bool typing = !argument.empty() && argument.back() != ' ';
    std::string flags;
    std::string kind;
    bool list = false;
    size_t i = 0;
    for (; i < words.size() && words[i].starts_with('-'); ++i) {
      if (typing && i + 1 == words.size()) {
        return {};
      }
      const auto flag = words[i];
      if (flag == "--") {
        flags.append("-- ");
        ++i;
        break;
      }
      if (flag == "--kind") {
        if (i + 1 == words.size() || (typing && i + 2 == words.size())) {
          return KindItems(db, flags, i + 1 < words.size() ? words[i + 1] : "");
        }
        kind = words[++i];
        absl::StrAppend(&flags, "--kind ", kind, " ");
      } else if (flag == "--list" || flag == "--all") {
        list = list || flag == "--list";
        absl::StrAppend(&flags, flag, " ");
      } else {
        return {};
      }
    }
    const auto term = i < words.size()
                        ? argument.substr(words[i].data() - argument.data())
                        : std::string_view{};
    const auto with_flags = [&](const std::vector<std::string>& completions) {
      std::vector<duckdb_shell::DocsCompletion> out;
      out.reserve(completions.size());
      for (const auto& completion : completions) {
        out.push_back({.text = absl::StrCat(flags, completion)});
      }
      return out;
    };

    if (flags.empty() && (IsNumber(term) || (term.empty() && _offered))) {
      return ListItems(term);
    }
    if (!list && kind.empty() && term.empty()) {
      std::vector<std::string> sections;
      for (const auto& [name, count] : TopSections(db)) {
        sections.push_back(name);
      }
      return with_flags(sections);
    }
    if (list || (kind.empty() && LooksLikePath(term))) {
      return with_flags(CompletePath(db, term, kMaxCompletions));
    }
    return with_flags(Names(db, term, kind));
  }

  bool Offered() const noexcept { return _offered; }

 private:
  std::string Menu(const duckdb_shell::DocsRequest& request,
                   std::string_view heading, std::vector<Choice> choices) {
    const bool offer = request.interactive && choices.size() > 1;
    auto out = Render(
      request, offer ? std::string{heading}
                     : absl::StrCat(Listing(heading, choices), kOpenHint));
    _list = std::move(choices);
    _offered = offer;
    return out;
  }

  std::string RenderIndex(const duckdb_shell::DocsRequest& request) {
    std::vector<Choice> choices;
    for (const auto& [name, count] : TopSections(*request.instance)) {
      choices.push_back({.title = absl::StrCat(name, " (", count, ")"),
                         .path = name,
                         .directory = !LooksLikePath(name)});
    }
    return Menu(request,
                "**SereneDB documentation**\n\nUse `.docs <section>` to browse "
                "and `.docs <name>` to look up an object.\\\nAsk anything "
                "else, like `.docs how do I highlight matches`, to search "
                "every page.",
                std::move(choices));
  }

  bool Open(const duckdb_shell::DocsRequest& request, const Choice& choice,
            std::string& out) {
    if (!choice.url.empty()) {
      out = Render(request, Brief("", choice));
      return true;
    }
    if (choice.directory) {
      auto under = DirectoryChoices(*request.instance, choice.path);
      if (under.empty()) {
        out = absl::StrCat("Nothing is documented under ", choice.path, ".\n");
        return false;
      }
      if (under.size() == 1) {
        return Open(request, under.front(), out);
      }
      out = Menu(request, UnderHeading(choice.path), std::move(under));
      return true;
    }
    std::vector<Choice> sections;
    auto rendered = RenderChoice(request, choice, &sections);
    if (!rendered) {
      out = absl::StrCat("Nothing is documented at ", choice.path, ".\n");
      return false;
    }
    if (!sections.empty()) {
      _list = std::move(sections);
    }
    out = std::move(*rendered);
    return true;
  }

  std::string RenderAll(const duckdb_shell::DocsRequest& request,
                        const std::vector<Choice>& choices) {
    std::vector<std::string> parts;
    std::vector<Choice> listed;
    for (const auto& choice : choices) {
      if (choice.directory) {
        auto under = DirectoryChoices(*request.instance, choice.path);
        parts.push_back(Render(
          request, Listing(UnderHeading(choice.path), under, listed.size())));
        absl::c_move(under, std::back_inserter(listed));
      } else if (auto rendered = RenderChoice(request, choice, nullptr)) {
        parts.push_back(std::move(*rendered));
      }
    }
    if (!listed.empty()) {
      _list = std::move(listed);
    }
    return absl::StrJoin(parts,
                         absl::StrCat("\n", Render(request, "---"), "\n"));
  }

  bool RenderSearch(const duckdb_shell::DocsRequest& request,
                    std::string_view query, std::string& out) {
    std::string error;
    const auto hits = Search(*request.instance, query, kMaxCandidates,
                             Columns{.content_text = true}, error);
    if (!error.empty()) {
      out = absl::StrCat("Could not search for '", query, "': ", error, "\n");
      return false;
    }
    if (hits.empty()) {
      out =
        absl::StrCat("Nothing in the documentation matches '", query, "'.\n");
      return false;
    }
    out = Menu(request,
               absl::StrCat("Documentation matching '",
                            connector::EscapeMarkdown(query), "':"),
               EntryChoices(hits));
    return true;
  }

  std::string RenderCandidates(const duckdb_shell::DocsRequest& request,
                               std::string_view name,
                               const std::vector<Entry>& similar) {
    if (similar.empty()) {
      return absl::StrCat(
        "No documentation matches '", name,
        "'.\n\nUse .docs with no argument to list the sections.\n");
    }
    return Menu(
      request,
      absl::StrCat("No entry is named '", connector::EscapeMarkdown(name),
                   "'. Closest matches:"),
      EntryChoices(similar));
  }

  std::vector<duckdb_shell::DocsCompletion> ListItems(
    std::string_view prefix) const {
    std::vector<duckdb_shell::DocsCompletion> items;
    size_t n = 0;
    for (const auto& choice : _list) {
      auto number = absl::StrCat(++n);
      if (!number.starts_with(prefix)) {
        continue;
      }
      auto label = absl::StrCat(number, ". ", choice.title);
      if (!choice.breadcrumb.empty()) {
        absl::StrAppend(&label, kSeparator, choice.breadcrumb);
      }
      items.push_back({.text = std::move(number), .label = std::move(label)});
    }
    if (!items.empty()) {
      items.front().selected = true;
    }
    return items;
  }

  std::vector<Choice> _list;
  bool _offered = false;
};

}  // namespace

void RegisterShellDocsBackend() {
  auto session = std::make_shared<Session>();
  duckdb_shell::RegisterDocsBackend(
    {.run =
       [session](const duckdb_shell::DocsRequest& request, std::string& out) {
         try {
           return session->Run(request, out);
         } catch (const std::exception& e) {
           out =
             absl::StrCat("Could not read the documentation: ", e.what(), "\n");
           return false;
         }
       },
     .complete = [session](duckdb::DatabaseInstance* instance,
                           const duckdb::string& argument)
       -> duckdb::vector<duckdb_shell::DocsCompletion> {
       if (!instance) {
         return {};
       }
       try {
         return session->Complete(*instance, argument);
       } catch (const std::exception&) {
         return {};
       }
     },
     .offered = [session] { return session->Offered(); },
     .load =
       [](duckdb::ClientContext& context) {
         auto& db = duckdb::DatabaseInstance::GetDatabase(context);
         connector::RegisterMarkdownRenderFunctions(db);
         RegisterDocsFunctions(db);
         duckdb::ExtensionLoader::RefreshSearchPath(context);
       }});
}

}  // namespace sdb::docs
