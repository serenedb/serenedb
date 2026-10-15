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

#include "connector/functions/xml.h"

#include <absl/algorithm/container.h>
#include <libxml/chvalid.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xmlerror.h>
#include <libxml/xmlstring.h>

#include <array>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "query/config.h"

namespace sdb::connector {
namespace {

enum class XmlOption : uint8_t {
  Content,
  Document,
};

constexpr std::array<std::string_view, 2> kXmlOptions{"content", "document"};

constexpr std::array kNamespaceErrors{
  XML_WAR_NS_URI,           XML_ERR_NS_DECL_ERROR,
  XML_WAR_NS_URI_RELATIVE,  XML_WAR_NS_COLUMN,
  XML_NS_ERR_XML_NAMESPACE, XML_NS_ERR_UNDEFINED_NAMESPACE,
  XML_NS_ERR_QNAME,         XML_NS_ERR_ATTRIBUTE_REDEFINED,
  XML_NS_ERR_EMPTY,
};

constexpr int kDocumentOptions = XML_PARSE_NOENT | XML_PARSE_DTDATTR |
                                 XML_PARSE_IGNORE_ENC |
                                 XML_PARSE_NO_SYS_CATALOG;
constexpr int kContentOptions = XML_PARSE_IGNORE_ENC | XML_PARSE_NO_SYS_CATALOG;

template<auto Free>
struct XmlFree {
  void operator()(auto* object) const noexcept { Free(object); }
};

void NoteError(void* failed, const xmlError* error) {
  if (error->level < XML_ERR_ERROR ||
      error->code == XML_WAR_UNDECLARED_ENTITY ||
      absl::c_linear_search(kNamespaceErrors, error->code)) {
    return;
  }
  if (error->domain == XML_FROM_PARSER || error->domain == XML_FROM_NONE ||
      error->domain == XML_FROM_MEMORY || error->domain == XML_FROM_IO) {
    *static_cast<bool*>(failed) = true;
  }
}

xmlParserErrors LoadNothing(void*, const char*, const char*, xmlResourceType,
                            xmlParserInputFlags, xmlParserInput** out) {
  *out = xmlNewInputFromString(nullptr, "", XML_INPUT_BUF_STATIC);
  return *out ? XML_ERR_OK : XML_ERR_NO_MEMORY;
}

[[noreturn]] void ThrowOutOfMemory() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                  ERR_MSG("could not allocate XML parser state"));
}

xmlParserInput* NewInput(std::string_view text) {
  auto* input = xmlNewInputFromMemory(nullptr, text.data(), text.size(),
                                      XML_INPUT_BUF_STATIC);
  if (!input) {
    ThrowOutOfMemory();
  }
  return input;
}

void SkipXmlSpace(std::string_view& text) {
  while (!text.empty() && xmlIsBlank_ch(static_cast<unsigned char>(text[0]))) {
    text.remove_prefix(1);
  }
}

bool IsXmlNameChar(int c) {
  return xmlIsBaseChar_ch(c) || xmlIsIdeographicQ(c) || xmlIsDigit_ch(c) ||
         c == '.' || c == '-' || c == '_' || c == ':' || xmlIsCombiningQ(c) ||
         xmlIsExtender_ch(c);
}

enum class XmlDeclAttribute : uint8_t {
  Absent,
  Present,
  Malformed,
};

struct XmlDecl {
  size_t length = 0;
  std::string_view version;
  int standalone = -1;
};

std::optional<XmlDecl> ParseXmlDecl(std::string_view text) {
  constexpr std::string_view kStart = "<?xml";
  XmlDecl decl;
  if (!text.starts_with(kStart)) {
    return decl;
  }
  auto rest = text.substr(kStart.size());
  if (!rest.empty()) {
    int length = static_cast<int>(std::min<size_t>(rest.size(), 4));
    if (IsXmlNameChar(xmlGetUTF8Char(
          reinterpret_cast<const xmlChar*>(rest.data()), &length))) {
      return decl;
    }
  }
  const auto attribute = [&](std::string_view name) {
    auto after = rest;
    SkipXmlSpace(after);
    if (!after.starts_with(name)) {
      return XmlDeclAttribute::Absent;
    }
    if (after.size() == rest.size()) {
      return XmlDeclAttribute::Malformed;
    }
    rest = after.substr(name.size());
    return XmlDeclAttribute::Present;
  };
  const auto value = [&] -> std::optional<std::string_view> {
    SkipXmlSpace(rest);
    if (!rest.starts_with('=')) {
      return std::nullopt;
    }
    rest.remove_prefix(1);
    SkipXmlSpace(rest);
    if (!rest.starts_with('\'') && !rest.starts_with('"')) {
      return std::nullopt;
    }
    const auto end = rest.find(rest[0], 1);
    if (end == std::string_view::npos) {
      return std::nullopt;
    }
    const auto quoted = rest.substr(1, end - 1);
    rest.remove_prefix(end + 1);
    return quoted;
  };

  if (attribute("version") != XmlDeclAttribute::Present) {
    return std::nullopt;
  }
  const auto version = value();
  if (!version) {
    return std::nullopt;
  }
  decl.version = *version;
  switch (attribute("encoding")) {
    case XmlDeclAttribute::Absent:
      break;
    case XmlDeclAttribute::Present:
      if (!value()) {
        return std::nullopt;
      }
      break;
    case XmlDeclAttribute::Malformed:
      return std::nullopt;
  }
  switch (attribute("standalone")) {
    case XmlDeclAttribute::Absent:
      break;
    case XmlDeclAttribute::Present: {
      const auto standalone = value();
      if (standalone == "yes") {
        decl.standalone = 1;
      } else if (standalone == "no") {
        decl.standalone = 0;
      } else {
        return std::nullopt;
      }
      break;
    }
    case XmlDeclAttribute::Malformed:
      return std::nullopt;
  }
  SkipXmlSpace(rest);
  if (!rest.starts_with("?>")) {
    return std::nullopt;
  }
  rest.remove_prefix(2);
  decl.length = text.size() - rest.size();
  if (absl::c_any_of(text.substr(0, decl.length), [](char c) {
        return static_cast<unsigned char>(c) > 127;
      })) {
    return std::nullopt;
  }
  return decl;
}

bool DoctypeInContent(std::string_view text) {
  for (;;) {
    SkipXmlSpace(text);
    if (!text.starts_with('<')) {
      return false;
    }
    text.remove_prefix(1);
    if (text.starts_with('!')) {
      text.remove_prefix(1);
      if (text.starts_with("DOCTYPE")) {
        return true;
      }
      if (!text.starts_with("--")) {
        return false;
      }
      const auto end = text.find("--", 2);
      if (end == std::string_view::npos ||
          !text.substr(end + 2).starts_with('>')) {
        return false;
      }
      text.remove_prefix(end + 3);
      continue;
    }
    if (!text.starts_with('?')) {
      return false;
    }
    const auto end = text.find("?>", 1);
    if (end == std::string_view::npos) {
      return false;
    }
    text.remove_prefix(end + 2);
  }
}

struct XmlParser final : duckdb::FunctionLocalState {
  XmlParser() : context{xmlNewParserCtxt()} {
    if (!context) {
      ThrowOutOfMemory();
    }
    xmlCtxtSetErrorHandler(context.get(), NoteError, &failed);
    xmlCtxtSetResourceLoader(context.get(), LoadNothing, nullptr);
  }

  bool WellFormedDocument(std::string_view text) {
    failed = false;
    xmlCtxtReset(context.get());
    xmlCtxtSetOptions(context.get(), kDocumentOptions);
    const std::unique_ptr<xmlDoc, XmlFree<xmlFreeDoc>> doc{
      xmlCtxtParseDocument(context.get(), NewInput(text))};
    return doc && !failed;
  }

  bool WellFormedContent(std::string_view text) {
    const auto decl = ParseXmlDecl(text);
    if (!decl) {
      return false;
    }
    const auto content = text.substr(decl->length);
    if (DoctypeInContent(content)) {
      return WellFormedDocument(text);
    }
    if (content.empty()) {
      return true;
    }
    failed = false;
    const std::string version{decl->version};
    const std::unique_ptr<xmlDoc, XmlFree<xmlFreeDoc>> doc{xmlNewDoc(
      decl->version.data() ? reinterpret_cast<const xmlChar*>(version.c_str())
                           : nullptr)};
    if (!doc) {
      ThrowOutOfMemory();
    }
    doc->encoding = xmlStrdup(reinterpret_cast<const xmlChar*>("UTF-8"));
    doc->standalone = decl->standalone;
    xmlCtxtSetOptions(context.get(), kContentOptions);
    xmlFreeNodeList(xmlCtxtParseContent(context.get(), NewInput(content),
                                        reinterpret_cast<xmlNode*>(doc.get()),
                                        0));
    return !(xmlCtxtGetStatus(context.get()) & XML_STATUS_NOT_WELL_FORMED) &&
           !failed;
  }

  std::unique_ptr<xmlParserCtxt, XmlFree<xmlFreeParserCtxt>> context;
  bool failed = false;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitXmlParser(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<XmlParser>();
}

void WellFormed(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                duckdb::Vector& result, XmlOption option) {
  auto& parser =
    duckdb::ExecuteFunctionState::GetFunctionState(state)->Cast<XmlParser>();
  duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
    args.data[0], result, args.size(), [&](duckdb::string_t value) {
      const std::string_view text{value.GetData(), value.GetSize()};
      return option == XmlOption::Document ? parser.WellFormedDocument(text)
                                           : parser.WellFormedContent(text);
    });
}

template<XmlOption Option>
void WellFormedFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  WellFormed(args, state, result, Option);
}

void WellFormedByOptionFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  static constinit SettingRef gXmlOption{"xmloption"};
  const auto option = gXmlOption.Enum(state.GetContext(), kXmlOptions);
  SDB_ASSERT(option < kXmlOptions.size());
  WellFormed(args, state, result, static_cast<XmlOption>(option));
}

}  // namespace

void RegisterPgXmlFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  struct WellFormedFunctionSpec {
    std::string_view name;
    duckdb::scalar_function_t function;
    duckdb::FunctionStability stability;
  };
  for (const auto& [name, function, stability] : {
         WellFormedFunctionSpec{
           "xml_is_well_formed", WellFormedByOptionFunction,
           duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY},
         WellFormedFunctionSpec{"xml_is_well_formed_document",
                                WellFormedFunction<XmlOption::Document>,
                                duckdb::FunctionStability::CONSISTENT},
         WellFormedFunctionSpec{"xml_is_well_formed_content",
                                WellFormedFunction<XmlOption::Content>,
                                duckdb::FunctionStability::CONSISTENT},
       }) {
    duckdb::ScalarFunction scalar{duckdb::Identifier{name},
                                  {duckdb::LogicalType::VARCHAR},
                                  duckdb::LogicalType::BOOLEAN,
                                  function};
    scalar.SetInitStateCallback(InitXmlParser);
    scalar.SetStability(stability);
    loader.RegisterFunction(std::move(scalar));
  }
}

}  // namespace sdb::connector
