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

#include "pg/types/xml.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <libxml/chvalid.h>
#include <libxml/entities.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xmlerror.h>
#include <libxml/xmlstring.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include <simdjson.h>

#include <array>
#include <cmath>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/aggregate_function.hpp>
#include <duckdb/function/aggregate_state_layout.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <memory>
#include <span>
#include <vector>

#include "connector/common.h"
#include "pg/types/cast.h"
#include "pg/types/float.h"
#include "pg/types/logical.h"
#include "query/config.h"

namespace sdb::pg {
namespace {

using connector::AsView;

constexpr std::array<std::string_view, 2> kXmlOptions{"content", "document"};
constexpr std::string_view kDefaultVersion = "1.0";
constexpr std::string_view kDeclarationStart = "<?xml";

constexpr int kBaseOptions = XML_PARSE_IGNORE_ENC | XML_PARSE_NO_SYS_CATALOG;
constexpr int kDocumentOptions =
  kBaseOptions | XML_PARSE_NOENT | XML_PARSE_DTDATTR;

template<auto Free>
struct XmlFree {
  void operator()(auto* object) const noexcept { Free(object); }
};

using XmlDoc = std::unique_ptr<xmlDoc, XmlFree<xmlFreeDoc>>;

struct XmlStringFree {
  void operator()(xmlChar* text) const noexcept { xmlFree(text); }
};

using XmlString = std::unique_ptr<xmlChar, XmlStringFree>;

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

const xmlChar* AsXmlChars(std::string_view text) {
  return reinterpret_cast<const xmlChar*>(text.data());
}

std::string_view AsView(const xmlChar* text) {
  return text ? std::string_view{reinterpret_cast<const char*>(text)}
              : std::string_view{};
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
  std::optional<std::string_view> version;
  int standalone = -1;
};

std::optional<XmlDecl> ParseXmlDecl(std::string_view text) {
  XmlDecl decl;
  if (!text.starts_with(kDeclarationStart)) {
    return decl;
  }
  auto rest = text.substr(kDeclarationStart.size());
  if (!rest.empty()) {
    int length = static_cast<int>(std::min<size_t>(rest.size(), 4));
    if (IsXmlNameChar(xmlGetUTF8Char(AsXmlChars(rest), &length))) {
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
  decl.version = value();
  if (!decl.version) {
    return std::nullopt;
  }
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

std::string PrintXmlDecl(std::optional<std::string_view> version,
                         int standalone) {
  if ((!version || *version == kDefaultVersion) && standalone == -1) {
    return {};
  }
  return absl::StrCat("<?xml version=\"", version.value_or(kDefaultVersion),
                      "\"",
                      standalone == 1   ? " standalone=\"yes\""
                      : standalone == 0 ? " standalone=\"no\""
                                        : "",
                      "?>");
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

class XmlParser {
 public:
  XmlParser() : _context{xmlNewParserCtxt()} {
    if (!_context) {
      ThrowOutOfMemory();
    }
    xmlCtxtSetErrorHandler(_context.get(), NoteError, this);
    xmlCtxtSetResourceLoader(_context.get(), LoadNothing, nullptr);
  }

  bool WellFormed(std::string_view text, XmlOption option) {
    Start(XML_ERR_FATAL);
    return option == XmlOption::Document ? WellFormedDocument(text)
                                         : WellFormedContent(text);
  }

  XmlDoc ReadDocument(std::string_view text) {
    Start(XML_ERR_ERROR);
    xmlCtxtReset(_context.get());
    xmlCtxtSetOptions(_context.get(), kBaseOptions);
    XmlDoc doc{xmlCtxtParseDocument(_context.get(), NewInput(text))};
    if (_failed) {
      doc.reset();
    }
    return doc;
  }

  const std::string& Detail() const { return _detail; }

 private:
  void Start(xmlErrorLevel min_level) {
    _min_level = min_level;
    _failed = false;
    _detail.clear();
  }

  static void NoteError(void* data, const xmlError* error) {
    auto& parser = *static_cast<XmlParser*>(data);
    if (error->level < parser._min_level) {
      return;
    }
    if (!parser._failed && error->message) {
      parser._detail =
        absl::StrCat("line ", error->line, ": ",
                     absl::StripTrailingAsciiWhitespace(error->message));
    }
    parser._failed = true;
  }

  bool WellFormedDocument(std::string_view text) {
    xmlCtxtReset(_context.get());
    xmlCtxtSetOptions(_context.get(), kDocumentOptions);
    const XmlDoc doc{xmlCtxtParseDocument(_context.get(), NewInput(text))};
    return static_cast<bool>(doc);
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
    const std::string version{decl->version.value_or("")};
    const XmlDoc doc{xmlNewDoc(decl->version ? AsXmlChars(version) : nullptr)};
    if (!doc) {
      ThrowOutOfMemory();
    }
    doc->encoding = xmlStrdup(AsXmlChars("UTF-8"));
    doc->standalone = decl->standalone;
    xmlCtxtSetOptions(_context.get(), kBaseOptions);
    xmlFreeNodeList(xmlCtxtParseContent(_context.get(), NewInput(content),
                                        reinterpret_cast<xmlNode*>(doc.get()),
                                        0));
    return !(xmlCtxtGetStatus(_context.get()) & XML_STATUS_NOT_WELL_FORMED);
  }

  std::unique_ptr<xmlParserCtxt, XmlFree<xmlFreeParserCtxt>> _context;
  xmlErrorLevel _min_level = XML_ERR_FATAL;
  bool _failed = false;
  std::string _detail;
};

std::optional<irs::pg::SqlErrorData> Check(XmlParser& parser,
                                           std::string_view text,
                                           XmlOption option) {
  if (parser.WellFormed(text, option)) {
    return std::nullopt;
  }
  if (option == XmlOption::Document) {
    return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_XML_DOCUMENT),
                          ERR_MSG("invalid XML document"),
                          ERR_DETAIL(parser.Detail()));
  }
  return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_XML_CONTENT),
                        ERR_MSG("invalid XML content"),
                        ERR_DETAIL(parser.Detail()));
}

struct XmlCastData final : duckdb::BoundCastData {
  explicit XmlCastData(XmlOption option) : option{option} {}

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<XmlCastData>(option);
  }

  XmlOption option;
};

struct XmlInputState final : InputState {
  XmlParser parser;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitXmlInput(
  duckdb::CastLocalStateParameters&) {
  return duckdb::make_uniq<XmlInputState>();
}

bool TextToXml(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto option = parameters.cast_data->Cast<XmlCastData>().option;
  SDB_ASSERT(parameters.local_state);
  auto& parser = parameters.local_state->Cast<XmlInputState>().parser;
  bool converted = true;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count,
    [&](duckdb::string_t text) -> std::optional<duckdb::string_t> {
      if (auto error = Check(parser, AsView(text), option)) {
        InputError(std::move(*error), parameters);
        converted = false;
        return std::nullopt;
      }
      return duckdb::StringVector::AddString(result, text);
    });
  return converted;
}

duckdb::BoundCastInfo BindTextToXml(duckdb::BindCastInput& input,
                                    const duckdb::LogicalType&,
                                    const duckdb::LogicalType&) {
  return {TextToXml,
          duckdb::make_uniq<XmlCastData>(CurrentXmlOption(*input.context)),
          InitXmlInput};
}

bool XmlToJson(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters&) {
  simdjson::builder::string_builder json;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count, [&](duckdb::string_t xml) {
      json.clear();
      json.escape_and_append_with_quotes(AsView(xml));
      return duckdb::StringVector::AddString(result, json.view().value());
    });
  return true;
}

struct XmlFunctionState final : duckdb::FunctionLocalState {
  XmlParser parser;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitXmlFunction(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<XmlFunctionState>();
}

XmlParser& ParserOf(duckdb::ExpressionState& state) {
  return duckdb::ExecuteFunctionState::GetFunctionState(state)
    ->Cast<XmlFunctionState>()
    .parser;
}

void WellFormed(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                duckdb::Vector& result, XmlOption option) {
  auto& parser = ParserOf(state);
  duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
    args.data[0], result, args.size(), [&](duckdb::string_t value) {
      return parser.WellFormed(AsView(value), option);
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
  WellFormed(args, state, result, CurrentXmlOption(state.GetContext()));
}

void XmlCommentFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t value) {
      const auto text = AsView(value);
      if (text.find("--") != std::string_view::npos || text.ends_with('-')) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_XML_COMMENT),
                        ERR_MSG("invalid XML comment"));
      }
      return duckdb::StringVector::AddString(result,
                                             absl::StrCat("<!--", text, "-->"));
    });
}

void XmlTextFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t value) {
      const std::string text{AsView(value)};
      const XmlString escaped{xmlEncodeSpecialChars(nullptr, AsXmlChars(text))};
      if (!escaped) {
        ThrowOutOfMemory();
      }
      return duckdb::StringVector::AddString(
        result, std::string{AsView(escaped.get())});
    });
}

std::string EscapeXml(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (const char c : text) {
    switch (c) {
      case '&':
        out += "&amp;";
        break;
      case '<':
        out += "&lt;";
        break;
      case '>':
        out += "&gt;";
        break;
      case '\r':
        out += "&#x0d;";
        break;
      default:
        out += c;
        break;
    }
  }
  return out;
}

std::string NodeToXml(xmlNode* node) {
  if (node->type == XML_ATTRIBUTE_NODE || node->type == XML_TEXT_NODE) {
    const XmlString text{xmlXPathCastNodeToString(node)};
    if (!text) {
      ThrowOutOfMemory();
    }
    return EscapeXml(AsView(text.get()));
  }
  const std::unique_ptr<xmlBuffer, XmlFree<xmlBufferFree>> buffer{
    xmlBufferCreate()};
  if (!buffer) {
    ThrowOutOfMemory();
  }
  auto* copy = xmlCopyNode(node, 1);
  if (!copy) {
    ThrowOutOfMemory();
  }
  const bool document = copy->type == XML_DOCUMENT_NODE;
  const auto dumped = xmlNodeDump(buffer.get(), nullptr, copy, 0, 0);
  if (document) {
    xmlFreeDoc(reinterpret_cast<xmlDoc*>(copy));
  } else {
    xmlFreeNode(copy);
  }
  if (dumped == -1) {
    ThrowOutOfMemory();
  }
  return std::string{
    reinterpret_cast<const char*>(xmlBufferContent(buffer.get())),
    static_cast<size_t>(xmlBufferLength(buffer.get()))};
}

std::string NumberToXml(double value) {
  char buffer[kNumberStrMaxLen];
  return std::string{buffer, WriteFloat<true>(value, 1, buffer)};
}

struct XmlNamespace {
  std::string_view name;
  std::string_view uri;
};

void XpathError(std::string_view message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_XQUERY),
                  ERR_MSG(message));
}

template<typename Emit>
void EvaluateXpath(XmlParser& parser, std::string_view path,
                   std::string_view xml, std::span<const XmlNamespace> spaces,
                   Emit&& emit) {
  if (path.empty()) {
    XpathError("empty XPath expression");
  }
  xml.remove_prefix(ParseXmlDecl(xml).value_or(XmlDecl{}).length);
  const auto doc = parser.ReadDocument(xml);
  if (!doc) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_XML_DOCUMENT),
                    ERR_MSG("could not parse XML document"),
                    ERR_DETAIL(parser.Detail()));
  }
  const std::unique_ptr<xmlXPathContext, XmlFree<xmlXPathFreeContext>> context{
    xmlXPathNewContext(doc.get())};
  if (!context) {
    ThrowOutOfMemory();
  }
  bool failed = false;
  xmlXPathSetErrorHandler(
    context.get(),
    [](void* data, const xmlError* error) {
      if (error->level >= XML_ERR_ERROR) {
        *static_cast<bool*>(data) = true;
      }
    },
    &failed);
  context->node = reinterpret_cast<xmlNode*>(doc.get());
  for (const auto& space : spaces) {
    const std::string name{space.name};
    const std::string uri{space.uri};
    if (xmlXPathRegisterNs(context.get(), AsXmlChars(name), AsXmlChars(uri)) !=
        0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("could not register XML namespace with name \"",
                              name, "\" and URI \"", uri, "\""));
    }
  }
  const std::string expression{path};
  const std::unique_ptr<xmlXPathCompExpr, XmlFree<xmlXPathFreeCompExpr>>
    compiled{xmlXPathCtxtCompile(context.get(), AsXmlChars(expression))};
  if (!compiled || failed) {
    XpathError("invalid XPath expression");
  }
  const std::unique_ptr<xmlXPathObject, XmlFree<xmlXPathFreeObject>> object{
    xmlXPathCompiledEval(compiled.get(), context.get())};
  if (!object || failed) {
    XpathError("could not create XPath object");
  }
  switch (object->type) {
    case XPATH_NODESET:
      if (object->nodesetval) {
        for (int i = 0; i < object->nodesetval->nodeNr; ++i) {
          emit(NodeToXml(object->nodesetval->nodeTab[i]));
        }
      }
      return;
    case XPATH_BOOLEAN:
      emit(std::string{object->boolval ? "true" : "false"});
      return;
    case XPATH_NUMBER:
      emit(NumberToXml(object->floatval));
      return;
    case XPATH_STRING:
      emit(EscapeXml(AsView(object->stringval)));
      return;
    case XPATH_UNDEFINED:
    case XPATH_USERS:
    case XPATH_XSLT_TREE:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("xpath expression result type ", static_cast<int>(object->type),
                " is unsupported"));
  }
}

using NamespaceArrays = duckdb::VectorIterator<
  duckdb::VectorListType<duckdb::VectorListType<duckdb::string_t>>>;

std::vector<XmlNamespace> NamespacesOf(
  const NamespaceArrays::ValueEntry& entry) {
  std::vector<XmlNamespace> spaces;
  for (const auto pair : entry.GetChildValues()) {
    if (!pair.IsValid() || pair.GetListLength() != 2) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_EXCEPTION),
                      ERR_MSG("invalid array for XML namespace mapping"),
                      ERR_DETAIL("The array must be two-dimensional with "
                                 "length of the second axis equal to 2."));
    }
    const auto name = pair.GetChildValue(0);
    const auto uri = pair.GetChildValue(1);
    if (!name.IsValid() || !uri.IsValid()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                      ERR_MSG("neither namespace name nor URI may be null"));
    }
    spaces.emplace_back(AsView(name.GetValue()), AsView(uri.GetValue()));
  }
  return spaces;
}

template<bool kExists, bool kNamespaces>
void XpathFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  auto& parser = ParserOf(state);
  const auto count = args.size();
  const auto paths = args.data[0].Values<duckdb::string_t>();
  const auto documents = args.data[1].Values<duckdb::string_t>();
  std::optional<NamespaceArrays> arrays;
  if constexpr (kNamespaces) {
    if (args.data[2].GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR) {
      args.data[2].Flatten(count);
    }
    arrays.emplace(args.data[2]);
  }
  auto writer = [&] {
    if constexpr (kExists) {
      return duckdb::FlatVector::Writer<bool>(result, count);
    } else {
      return duckdb::FlatVector::Writer<
        duckdb::VectorListType<duckdb::string_t>>(result, count);
    }
  }();
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto path = paths[row];
    const auto document = documents[row];
    if (!path.IsValid() || !document.IsValid()) {
      writer.WriteNull();
      continue;
    }
    std::vector<XmlNamespace> spaces;
    if constexpr (kNamespaces) {
      const auto entry = (*arrays)[row];
      if (!entry.IsValid()) {
        writer.WriteNull();
        continue;
      }
      spaces = NamespacesOf(entry);
    }
    if constexpr (kExists) {
      bool found = false;
      EvaluateXpath(parser, AsView(path.GetValue()),
                    AsView(document.GetValue()), spaces,
                    [&](std::string&&) { found = true; });
      writer.WriteValue(found);
    } else {
      auto list = writer.WriteDynamicList();
      EvaluateXpath(parser, AsView(path.GetValue()),
                    AsView(document.GetValue()), spaces,
                    [&](std::string&& item) {
                      list.WriteElement().WriteValue(duckdb::string_t{
                        item.data(), static_cast<uint32_t>(item.size())});
                    });
    }
  }
}

std::string XmlConcat(std::span<const std::string_view> values) {
  int standalone = 1;
  std::optional<std::string_view> version;
  bool version_differs = false;
  std::string body;
  for (const auto xml : values) {
    const auto decl = ParseXmlDecl(xml).value_or(XmlDecl{});
    if (decl.standalone == 0 && standalone == 1) {
      standalone = 0;
    }
    if (decl.standalone < 0) {
      standalone = -1;
    }
    if (!decl.version) {
      version_differs = true;
    } else if (!version) {
      version = decl.version;
    } else if (*decl.version != *version) {
      version_differs = true;
    }
    absl::StrAppend(&body, xml.substr(decl.length));
  }
  if (version_differs && standalone < 0) {
    return body;
  }
  return absl::StrCat(
    PrintXmlDecl(version_differs ? std::nullopt : version, standalone), body);
}

void XmlConcatFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  const auto count = args.size();
  std::vector<std::vector<std::string_view>> rows(count);
  for (auto& column : args.data) {
    for (auto entry : column.Values<duckdb::string_t>()) {
      if (entry.IsValid()) {
        rows[entry.GetIndex()].emplace_back(AsView(entry.GetValue()));
      }
    }
  }
  auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count);
  for (const auto& values : rows) {
    if (values.empty()) {
      writer.WriteNull();
      continue;
    }
    const auto xml = XmlConcat(values);
    writer.WriteValue(
      duckdb::string_t{xml.data(), static_cast<uint32_t>(xml.size())});
  }
}

struct XmlAggState {
  char* data;
  uint32_t size;
  uint32_t capacity;
  bool is_set;
};

struct XmlAgg {
  template<typename State>
  static void Initialize(State& state) {
    state.data = nullptr;
    state.size = 0;
    state.capacity = 0;
    state.is_set = false;
  }

  static void Reserve(XmlAggState& state, duckdb::ArenaAllocator& allocator,
                      size_t size) {
    if (size <= state.capacity) {
      return;
    }
    if (size > duckdb::string_t::MAX_STRING_SIZE) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                      ERR_MSG("xmlagg result exceeds the maximum string size"));
    }
    const auto capacity = std::min<size_t>(duckdb::NextPowerOfTwo(size),
                                           duckdb::string_t::MAX_STRING_SIZE);
    auto* data = reinterpret_cast<char*>(allocator.Allocate(capacity));
    absl::c_copy(std::string_view{state.data, state.size}, data);
    state.data = data;
    state.capacity = static_cast<uint32_t>(capacity);
  }

  static void Append(XmlAggState& state, duckdb::ArenaAllocator& allocator,
                     std::string_view xml) {
    const std::string_view current{state.data, state.size};
    if (state.is_set && current.starts_with(kDeclarationStart)) {
      const std::array values{current, xml};
      const auto merged = XmlConcat(values);
      state.size = 0;
      Reserve(state, allocator, merged.size());
      std::memcpy(state.data, merged.data(), merged.size());
      state.size = static_cast<uint32_t>(merged.size());
      return;
    }
    if (state.is_set) {
      xml.remove_prefix(ParseXmlDecl(xml).value_or(XmlDecl{}).length);
    }
    state.is_set = true;
    Reserve(state, allocator, state.size + xml.size());
    absl::c_copy(xml, state.data + state.size);
    state.size += static_cast<uint32_t>(xml.size());
  }

  template<typename Input, typename State, typename Op>
  static void Operation(State& state, const Input& input,
                        duckdb::AggregateUnaryInput& unary_input) {
    Append(state, unary_input.input.allocator, AsView(input));
  }

  template<typename Input, typename State, typename Op>
  static void ConstantOperation(State& state, const Input& input,
                                duckdb::AggregateUnaryInput& unary_input,
                                duckdb::idx_t count) {
    for (duckdb::idx_t i = 0; i < count; ++i) {
      Operation<Input, State, Op>(state, input, unary_input);
    }
  }

  template<typename State, typename Op>
  static void Combine(const State& source, State& target,
                      duckdb::AggregateInputData& input) {
    if (source.is_set) {
      Append(target, input.allocator, {source.data, source.size});
    }
  }

  template<typename Target, typename State>
  static void Finalize(State& state, Target& target,
                       duckdb::AggregateFinalizeData& finalize_data) {
    if (!state.is_set) {
      finalize_data.ReturnNull();
      return;
    }
    target = finalize_data.ReturnString({state.data, state.size});
  }

  static bool IgnoreNull() { return true; }
};

void RegisterCasts(duckdb::CastFunctionSet& casts) {
  const auto xml = XML();
  RegisterTextInput(casts, xml, BindTextToXml);
  casts.RegisterCastFunction(xml, duckdb::LogicalType::JSON(), XmlToJson, -1);
}

void RegisterFunctions(Functions& functions) {
  const auto xml = XML();
  const auto varchar = duckdb::LogicalType::VARCHAR;
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
    duckdb::ScalarFunction scalar{
      {varchar}, duckdb::LogicalType::BOOLEAN, function};
    scalar.SetInitStateCallback(InitXmlFunction);
    scalar.SetStability(stability);
    Add(functions, name, std::move(scalar));
  }
  Add(functions, "xmlcomment", Fallible({{varchar}, xml, XmlCommentFunction}));
  Add(functions, "xmltext", {{varchar}, xml, XmlTextFunction});
  duckdb::ScalarFunction concat{{}, xml, XmlConcatFunction};
  concat.GetSignature().AddParameter("value", xml).AddArgs("args", xml);
  concat.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  Add(functions, "xmlconcat", std::move(concat));
  const auto spaces =
    duckdb::LogicalType::LIST(duckdb::LogicalType::LIST(varchar));
  const auto xpath = [](duckdb::vector<duckdb::LogicalType> arguments,
                        duckdb::LogicalType result,
                        duckdb::scalar_function_t function) {
    duckdb::ScalarFunction scalar{std::move(arguments), std::move(result),
                                  function};
    scalar.SetInitStateCallback(InitXmlFunction);
    return Fallible(std::move(scalar));
  };
  Add(functions, "xpath",
      xpath({varchar, xml}, duckdb::LogicalType::LIST(xml),
            XpathFunction<false, false>));
  Add(functions, "xpath",
      xpath({varchar, xml, spaces}, duckdb::LogicalType::LIST(xml),
            XpathFunction<false, true>));
  Add(functions, "xpath_exists",
      xpath({varchar, xml}, duckdb::LogicalType::BOOLEAN,
            XpathFunction<true, false>));
  Add(functions, "xpath_exists",
      xpath({varchar, xml, spaces}, duckdb::LogicalType::BOOLEAN,
            XpathFunction<true, true>));
  Add(functions, "xmlagg",
      duckdb::AggregateFunction::UnaryAggregate<XmlAggState, duckdb::string_t,
                                                duckdb::string_t, XmlAgg>(xml,
                                                                          xml));
}

}  // namespace

XmlOption CurrentXmlOption(duckdb::ClientContext& context) {
  const auto option = GetEnumSetting(context, "xmloption", kXmlOptions);
  SDB_ASSERT(option < kXmlOptions.size());
  return static_cast<XmlOption>(option);
}

std::optional<irs::pg::SqlErrorData> CheckXml(std::string_view text,
                                              XmlOption option) {
  XmlParser parser;
  return Check(parser, text, option);
}

XmlText XmlOut(std::string_view xml) {
  if (!xml.starts_with(kDeclarationStart)) {
    return {{}, xml};
  }
  const auto decl = ParseXmlDecl(xml).value_or(XmlDecl{});
  XmlText out{PrintXmlDecl(decl.version, decl.standalone),
              xml.substr(decl.length)};
  if (out.declaration.empty() && out.body.starts_with('\n')) {
    out.body.remove_prefix(1);
  }
  return out;
}

void RegisterXml(duckdb::CastFunctionSet& casts, Functions& functions) {
  RegisterCasts(casts);
  RegisterFunctions(functions);
}

}  // namespace sdb::pg
