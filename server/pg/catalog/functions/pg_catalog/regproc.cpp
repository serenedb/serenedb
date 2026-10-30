////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/catalog/functions/pg_catalog/regproc.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_join.h>
#include <absl/strings/strip.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/macro_function.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <span>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "connector/common.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/format_type.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/sql_utils.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

OutText OutName(std::string_view name) {
  if (RequiresQuotes(name)) {
    return QuoteIdentifier(name);
  }
  return name;
}

}  // namespace

std::string QualifiedOutName(std::string_view schema, std::string_view name) {
  return absl::StrCat(AsView(OutName(schema)), ".", AsView(OutName(name)));
}

namespace {

constexpr size_t kMaxFunctionArgs = 100;
constexpr int32_t kMaxAttrSize = 10 * 1024 * 1024;
constexpr int32_t kMaxNumericPrecision = 1000;
constexpr int32_t kMinNumericScale = -1000;
constexpr int32_t kMaxNumericScale = 1000;
constexpr int32_t kMaxTimePrecision = 6;

using NameList = std::vector<std::string>;

std::nullopt_t Miss(irs::pg::SqlErrorData* soft_error,
                    irs::pg::SqlErrorData error) {
  if (!soft_error) {
    THROW_SQL_ERROR_FROM_DATA(std::move(error));
  }
  *soft_error = std::move(error);
  return std::nullopt;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> FindSchema(
  const Session& session, std::string_view name) {
  if (!session.database) {
    return nullptr;
  }
  const auto path = absl::c_find_if(
    session.search_path,
    [&](const SessionSchema& schema) { return schema.name == name; });
  if (path != session.search_path.end()) {
    return path->entry;
  }
  return session.database->GetSchema(*session.transaction,
                                     duckdb::Identifier{name},
                                     duckdb::OnEntryNotFound::RETURN_NULL);
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindInSchema(
  const Session& session, std::string_view schema_name,
  duckdb::CatalogType type, std::string_view name) {
  auto schema = FindSchema(session, schema_name);
  if (!schema) {
    return nullptr;
  }
  auto entry =
    FindMember(*session.transaction, *schema, type, duckdb::Identifier{name});
  if (!entry || entry->internal) {
    return nullptr;
  }
  return entry;
}

}  // namespace

std::optional<uint64_t> SchemaOid(const Session& session,
                                  std::string_view schema,
                                  irs::pg::SqlErrorData* soft_error) {
  if (const auto* system = FindSystemObject(kSystemNamespaces, schema)) {
    return system->oid;
  }
  if (auto entry = FindSchema(session, schema)) {
    return entry->oid;
  }
  return Miss(soft_error, SQL_ERROR_DATA(
                            ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                            ERR_MSG("schema \"", schema, "\" does not exist")));
}

namespace {

IRS_NO_INLINE uint64_t UserRelationOid(const Session& session,
                                       duckdb::SchemaCatalogEntry& schema,
                                       std::string_view name) {
  const duckdb::Identifier identifier{name};
  for (const auto type : kRelationSets) {
    if (const auto member =
          FindMember(*session.transaction, schema, type, identifier)) {
      return member->oid;
    }
  }
  if (const auto member =
        FindMember(*session.transaction, schema,
                   duckdb::CatalogType::TYPE_ENTRY, identifier);
      member && duckdb::StructType::IsStruct(
                  member->Cast<duckdb::TypeCatalogEntry>().user_type)) {
    return member->oid;
  }
  const auto key = FindKeyIndex(*session.context, schema, name);
  return key.constraint ? key.constraint->index_oid : kInvalidOid;
}

uint64_t RelationOid(const Session& session, const SessionSchema& schema,
                     std::string_view name) {
  if (schema.system) {
    return SystemRelationOid(schema.name, name);
  }
  auto entry = schema.entry;
  return entry ? UserRelationOid(session, *entry, name) : kInvalidOid;
}

uint64_t TemporaryRelationOid(const Session& session, std::string_view name) {
  if (!session.temporary) {
    return kInvalidOid;
  }
  const duckdb::Identifier identifier{name};
  for (const auto type : kRelationSets) {
    if (const auto member =
          FindMember(session.temporary->transaction, session.temporary->schema,
                     type, identifier)) {
      return member->oid;
    }
  }
  return kInvalidOid;
}

}  // namespace

uint64_t ResolveRelation(const Session& session,
                         const duckdb::QualifiedName& name) {
  const auto& relation = name.Name().GetIdentifierName();
  if (!name.Schema().empty()) {
    const auto& schema = name.Schema().GetIdentifierName();
    return RelationOid(
      session,
      SessionSchema{
        .name = schema,
        .entry = FindSchema(session, schema),
        .system = FindSystemObject(kSystemNamespaces, schema) != nullptr},
      relation);
  }
  if (const auto oid = TemporaryRelationOid(session, relation);
      oid != kInvalidOid) {
    return oid;
  }
  for (const auto& schema : session.search_path) {
    if (const auto oid = RelationOid(session, schema, relation);
        oid != kInvalidOid) {
      return oid;
    }
  }
  return kInvalidOid;
}

const duckdb::MacroCatalogEntry* FindMacro(const Session& session,
                                           uint64_t oid) {
  const auto entry = EntryByOid(session, oid);
  if (!entry || CatalogClassOid(entry->type) != kPgProcTable) {
    return nullptr;
  }
  return &entry->Cast<duckdb::MacroCatalogEntry>();
}

namespace {

bool IsWordStart(char c) {
  return absl::ascii_isalpha(c) || c == '_' ||
         static_cast<unsigned char>(c) >= 0x80;
}

std::optional<NameList> SplitNames(std::string_view text) {
  NameList names;
  duckdb::idx_t pos = 0;
  const auto skip_spaces = [&] {
    while (pos < text.size() && absl::ascii_isspace(text[pos])) {
      ++pos;
    }
  };
  skip_spaces();
  if (pos == text.size()) {
    return names;
  }
  while (true) {
    std::string name;
    if (text[pos] == '"') {
      if (!duckdb::StringUtil::TryParseQuotedString(text, pos, name)) {
        return std::nullopt;
      }
    } else {
      const auto start = pos;
      while (pos < text.size() && text[pos] != '.' &&
             !absl::ascii_isspace(text[pos])) {
        ++pos;
      }
      if (start == pos) {
        return std::nullopt;
      }
      name = absl::AsciiStrToLower(text.substr(start, pos - start));
    }
    names.emplace_back(std::move(name));
    skip_spaces();
    if (pos == text.size()) {
      return names;
    }
    if (text[pos] != '.') {
      return std::nullopt;
    }
    ++pos;
    skip_spaces();
  }
}

std::optional<NameList> ParseNames(std::string_view text,
                                   irs::pg::SqlErrorData* soft_error) {
  auto names = SplitNames(text);
  if (!names || names->empty()) {
    return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_NAME),
                                           ERR_MSG("invalid name syntax")));
  }
  return names;
}

std::optional<std::string> ParseSingleName(std::string_view text,
                                           irs::pg::SqlErrorData* soft_error) {
  auto names = SplitNames(text);
  if (!names || names->size() != 1) {
    return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_NAME),
                                           ERR_MSG("invalid name syntax")));
  }
  return std::move(names->front());
}

ObjectName Deconstruct(const Session& session, const NameList& names) {
  if (names.size() > 3) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("improper qualified name (too many dotted names): ",
                            absl::StrJoin(names, ".")));
  }
  if (names.size() == 3 && duckdb::DatabaseManager::TryGetDefaultDatabase(
                             *session.context) != names[0]) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cross-database references are not implemented: ",
                            absl::StrJoin(names, ".")));
  }
  if (names.size() == 1) {
    return {.schema = {}, .name = names.front()};
  }
  return {.schema = names[names.size() - 2], .name = names.back()};
}

enum class TokenKind : uint8_t {
  End,
  Word,
  Quoted,
  Number,
  Symbol,
};

struct Token {
  TokenKind kind;
  std::string text;
};

[[noreturn]] void ThrowSyntaxError(const Token& token) {
  if (token.kind == TokenKind::End) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("syntax error at end of input"));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                  ERR_MSG("syntax error at or near \"", token.text, "\""));
}

std::vector<Token> Tokenize(std::string_view text) {
  std::vector<Token> tokens;
  duckdb::idx_t pos = 0;
  while (true) {
    while (pos < text.size() && absl::ascii_isspace(text[pos])) {
      ++pos;
    }
    if (pos == text.size()) {
      break;
    }
    const char c = text[pos];
    if (c == '"') {
      std::string name;
      if (!duckdb::StringUtil::TryParseQuotedString(text, pos, name)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("unterminated quoted identifier at or near \"",
                                text.substr(pos), "\""));
      }
      if (name.empty()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("zero-length delimited identifier at or near \"\"\"\""));
      }
      tokens.emplace_back(
        Token{.kind = TokenKind::Quoted, .text = std::move(name)});
    } else if (IsWordStart(c)) {
      const auto start = pos;
      while (pos < text.size() &&
             (IsWordStart(text[pos]) || absl::ascii_isdigit(text[pos]) ||
              text[pos] == '$')) {
        ++pos;
      }
      tokens.emplace_back(
        Token{.kind = TokenKind::Word,
              .text = absl::AsciiStrToLower(text.substr(start, pos - start))});
    } else if (absl::ascii_isdigit(c)) {
      const auto start = pos++;
      while (pos < text.size() && absl::ascii_isdigit(text[pos])) {
        ++pos;
      }
      tokens.emplace_back(
        Token{.kind = TokenKind::Number,
              .text = std::string{text.substr(start, pos - start)}});
    } else {
      tokens.emplace_back(
        Token{.kind = TokenKind::Symbol, .text = std::string(1, c)});
      ++pos;
    }
  }
  tokens.emplace_back(Token{.kind = TokenKind::End, .text = {}});
  return tokens;
}

struct ParsedType {
  NameList names;
  bool keyword = false;
  std::vector<std::string> typmods;
  bool array = false;
};

class TypeParser {
 public:
  explicit TypeParser(std::string_view text) : _tokens{Tokenize(text)} {}

  std::optional<ParsedType> Parse() {
    if (IsWord(0, "setof")) {
      return std::nullopt;
    }
    auto type = Simple();
    Bounds(type);
    if (Peek(0).kind != TokenKind::End) {
      ThrowSyntaxError(Peek(0));
    }
    return type;
  }

 private:
  const Token& Peek(size_t ahead) const {
    return _tokens[std::min(_pos + ahead, _tokens.size() - 1)];
  }

  bool IsWord(size_t ahead, std::string_view word) const {
    const auto& token = Peek(ahead);
    return token.kind == TokenKind::Word && token.text == word;
  }

  bool IsSymbol(size_t ahead, char symbol) const {
    const auto& token = Peek(ahead);
    return token.kind == TokenKind::Symbol && token.text.front() == symbol;
  }

  bool AcceptWord(std::string_view word) {
    if (!IsWord(0, word)) {
      return false;
    }
    ++_pos;
    return true;
  }

  bool AcceptSymbol(char symbol) {
    if (!IsSymbol(0, symbol)) {
      return false;
    }
    ++_pos;
    return true;
  }

  void ExpectWord(std::string_view word) {
    if (!AcceptWord(word)) {
      ThrowSyntaxError(Peek(0));
    }
  }

  void ExpectSymbol(char symbol) {
    if (!AcceptSymbol(symbol)) {
      ThrowSyntaxError(Peek(0));
    }
  }

  int32_t ExpectNumber() {
    const auto& token = Peek(0);
    int32_t value = 0;
    if (token.kind != TokenKind::Number ||
        !absl::SimpleAtoi(token.text, &value)) {
      ThrowSyntaxError(token);
    }
    ++_pos;
    return value;
  }

  std::string Typmod() {
    if (AcceptSymbol('-')) {
      const auto& token = Peek(0);
      if (token.kind == TokenKind::Word || token.kind == TokenKind::Quoted) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("type modifiers must be simple constants or identifiers"));
      }
      if (token.kind != TokenKind::Number) {
        ThrowSyntaxError(token);
      }
      return absl::StrCat("-", _tokens[_pos++].text);
    }
    const auto& token = Peek(0);
    if (token.kind == TokenKind::End || token.kind == TokenKind::Symbol) {
      ThrowSyntaxError(token);
    }
    return std::move(_tokens[_pos++].text);
  }

  std::string ExpectName() {
    const auto& token = Peek(0);
    if (token.kind != TokenKind::Word && token.kind != TokenKind::Quoted) {
      ThrowSyntaxError(token);
    }
    return std::move(_tokens[_pos++].text);
  }

  static ParsedType Keyword(std::string_view name) {
    return {.names = {std::string{name}}, .keyword = true};
  }

  ParsedType Simple() {
    if (Peek(0).kind == TokenKind::Word) {
      if (auto type = KeywordType()) {
        return std::move(*type);
      }
    }
    ParsedType type;
    type.names.emplace_back(ExpectName());
    while (AcceptSymbol('.')) {
      type.names.emplace_back(ExpectName());
    }
    Typmods(type);
    return type;
  }

  std::optional<ParsedType> KeywordType() {
    const auto& word = Peek(0).text;
    if (word == "int" || word == "integer") {
      ++_pos;
      return Keyword("int4");
    }
    if (word == "smallint") {
      ++_pos;
      return Keyword("int2");
    }
    if (word == "bigint") {
      ++_pos;
      return Keyword("int8");
    }
    if (word == "real") {
      ++_pos;
      return Keyword("float4");
    }
    if (word == "boolean") {
      ++_pos;
      return Keyword("bool");
    }
    if (word == "float") {
      ++_pos;
      return Float();
    }
    if (word == "double" && IsWord(1, "precision")) {
      _pos += 2;
      return Keyword("float8");
    }
    if (word == "decimal" || word == "dec" || word == "numeric") {
      ++_pos;
      auto type = Keyword("numeric");
      Typmods(type);
      return type;
    }
    if (word == "varchar") {
      ++_pos;
      auto type = Keyword("varchar");
      Length(type);
      return type;
    }
    if (word == "bit") {
      ++_pos;
      return Varying("bit", "varbit", false);
    }
    if (word == "character" || word == "char" || word == "nchar") {
      ++_pos;
      return Varying("bpchar", "varchar", true);
    }
    if (word == "national") {
      if (!IsWord(1, "character") && !IsWord(1, "char")) {
        ThrowSyntaxError(Peek(1));
      }
      _pos += 2;
      return Varying("bpchar", "varchar", true);
    }
    if (word == "timestamp" || word == "time") {
      ++_pos;
      return Datetime(word == "timestamp");
    }
    if (word == "interval") {
      ++_pos;
      return Interval();
    }
    return std::nullopt;
  }

  ParsedType Float() {
    if (!AcceptSymbol('(')) {
      return Keyword("float8");
    }
    const auto precision = ExpectNumber();
    ExpectSymbol(')');
    if (precision < 1) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("precision for type float must be at least 1 bit"));
    }
    if (precision <= 24) {
      return Keyword("float4");
    }
    if (precision <= 53) {
      return Keyword("float8");
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("precision for type float must be less than 54 bits"));
  }

  ParsedType Varying(std::string_view fixed, std::string_view varying,
                     bool length) {
    const bool is_varying = AcceptWord("varying");
    auto type = Keyword(is_varying ? varying : fixed);
    if (length) {
      Length(type);
    } else {
      Typmods(type);
    }
    if (type.typmods.empty() && !is_varying) {
      type.typmods.emplace_back("1");
    }
    return type;
  }

  bool AcceptZone(std::string_view word) {
    return IsWord(1, "time") && AcceptWord(word);
  }

  ParsedType Datetime(bool timestamp) {
    std::vector<std::string> typmods;
    if (AcceptSymbol('(')) {
      typmods.emplace_back(absl::StrCat(ExpectNumber()));
      ExpectSymbol(')');
    }
    const bool zone = AcceptZone("with");
    if (zone || AcceptZone("without")) {
      ExpectWord("time");
      ExpectWord("zone");
    }
    auto type = Keyword(timestamp ? (zone ? "timestamptz" : "timestamp")
                                  : (zone ? "timetz" : "time"));
    type.typmods = std::move(typmods);
    return type;
  }

  std::optional<size_t> IntervalField() const {
    const auto& token = Peek(0);
    if (token.kind != TokenKind::Word) {
      return std::nullopt;
    }
    const auto it = absl::c_find_if(
      kIntervalFields,
      [&](const IntervalRange& field) { return field.fields == token.text; });
    if (it == kIntervalFields.end()) {
      return std::nullopt;
    }
    return it - kIntervalFields.begin();
  }

  ParsedType Interval() {
    auto type = Keyword("interval");
    if (AcceptSymbol('(')) {
      const auto precision = ExpectNumber();
      ExpectSymbol(')');
      type.typmods = {absl::StrCat(kIntervalFullRange),
                      absl::StrCat(precision)};
      return type;
    }
    const auto first = IntervalField();
    if (!first) {
      return type;
    }
    ++_pos;
    auto last = *first;
    const auto first_mask = kIntervalFields[*first].mask;
    if (first_mask != kIntervalMonth && first_mask != kIntervalSecond &&
        AcceptWord("to")) {
      const auto to = IntervalField();
      if (!to || (first_mask == kIntervalYear
                    ? kIntervalFields[*to].mask != kIntervalMonth
                    : *to <= *first)) {
        ThrowSyntaxError(Peek(0));
      }
      ++_pos;
      last = *to;
    }
    int32_t mask = 0;
    for (auto i = *first; i <= last; ++i) {
      mask |= kIntervalFields[i].mask;
    }
    type.typmods.emplace_back(absl::StrCat(mask));
    if (kIntervalFields[last].mask == kIntervalSecond && AcceptSymbol('(')) {
      type.typmods.emplace_back(absl::StrCat(ExpectNumber()));
      ExpectSymbol(')');
    }
    return type;
  }

  void Length(ParsedType& type) {
    if (AcceptSymbol('(')) {
      type.typmods.emplace_back(absl::StrCat(ExpectNumber()));
      ExpectSymbol(')');
    }
  }

  void Typmods(ParsedType& type) {
    if (!AcceptSymbol('(')) {
      return;
    }
    do {
      type.typmods.emplace_back(Typmod());
    } while (AcceptSymbol(','));
    ExpectSymbol(')');
  }

  void Bounds(ParsedType& type) {
    if (AcceptWord("array")) {
      if (AcceptSymbol('[')) {
        ExpectNumber();
        ExpectSymbol(']');
      }
      type.array = true;
      return;
    }
    while (AcceptSymbol('[')) {
      if (Peek(0).kind == TokenKind::Number) {
        ExpectNumber();
      }
      ExpectSymbol(']');
      type.array = true;
    }
  }

  std::vector<Token> _tokens;
  size_t _pos = 0;
};

[[noreturn]] void ThrowInvalidTypmod(std::string_view message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE), ERR_MSG(message));
}

int32_t LengthTypmod(std::span<const int32_t> typmods, std::string_view name,
                     int32_t max, int32_t header) {
  if (typmods.size() != 1) {
    ThrowInvalidTypmod("invalid type modifier");
  }
  const auto length = typmods.front();
  if (length < 1) {
    ThrowInvalidTypmod(
      absl::StrCat("length for type ", name, " must be at least 1"));
  }
  if (length > max) {
    ThrowInvalidTypmod(
      absl::StrCat("length for type ", name, " cannot exceed ", max));
  }
  return length + header;
}

int32_t NumericTypmodIn(std::span<const int32_t> typmods) {
  if (typmods.size() > 2) {
    ThrowInvalidTypmod("invalid NUMERIC type modifier");
  }
  const auto precision = typmods[0];
  if (precision < 1 || precision > kMaxNumericPrecision) {
    ThrowInvalidTypmod(absl::StrCat("NUMERIC precision ", precision,
                                    " must be between 1 and ",
                                    kMaxNumericPrecision));
  }
  const auto scale = typmods.size() == 2 ? typmods[1] : 0;
  if (scale < kMinNumericScale || scale > kMaxNumericScale) {
    ThrowInvalidTypmod(absl::StrCat("NUMERIC scale ", scale,
                                    " must be between ", kMinNumericScale,
                                    " and ", kMaxNumericScale));
  }
  return NumericTypmod(precision, scale);
}

int32_t TimeTypmod(std::span<const int32_t> typmods, std::string_view name,
                   bool zone) {
  if (typmods.size() != 1) {
    ThrowInvalidTypmod("invalid type modifier");
  }
  const auto precision = typmods.front();
  if (precision < 0) {
    ThrowInvalidTypmod(absl::StrCat(name, "(", precision, ")",
                                    zone ? " WITH TIME ZONE" : "",
                                    " precision must not be negative"));
  }
  return std::min(precision, kMaxTimePrecision);
}

int32_t IntervalTypmod(std::span<const int32_t> typmods) {
  if (typmods.size() > 2 ||
      (typmods[0] != kIntervalFullRange &&
       absl::c_none_of(kIntervalRanges, [&](const IntervalRange& range) {
         return range.mask == typmods[0];
       }))) {
    ThrowInvalidTypmod("invalid INTERVAL type modifier");
  }
  const auto range = typmods[0];
  if (typmods.size() == 1) {
    return range == kIntervalFullRange ? -1
                                       : (range << 16) | kIntervalFullPrecision;
  }
  const auto precision = typmods[1];
  if (precision < 0) {
    ThrowInvalidTypmod(
      absl::StrCat("INTERVAL(", precision, ") precision must not be negative"));
  }
  return (range << 16) | std::min(precision, kMaxTimePrecision);
}

int32_t TypmodValue(std::string_view text) {
  int32_t value = 0;
  if (absl::SimpleAtoi(text, &value)) {
    return value;
  }
  if (absl::c_all_of(absl::StripPrefix(text, "-"), absl::ascii_isdigit)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
      ERR_MSG("value \"", text, "\" is out of range for type integer"));
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
    ERR_MSG("invalid input syntax for type integer: \"", text, "\""));
}

constexpr std::array kTypmodTypes{kBpchar,      kVarchar, kBit,    kVarbit,
                                  kNumeric,     kTime,    kTimetz, kTimestamp,
                                  kTimestamptz, kInterval};

int32_t TypmodIn(uint64_t oid, std::span<const std::string> texts,
                 std::string_view display) {
  if (texts.empty()) {
    return -1;
  }
  if (!absl::c_linear_search(kTypmodTypes, oid)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("type modifier is not allowed for type \"", display, "\""));
  }
  std::vector<int32_t> typmods;
  typmods.reserve(texts.size());
  for (const auto& text : texts) {
    typmods.emplace_back(TypmodValue(text));
  }
  if (oid == kBpchar) {
    return LengthTypmod(typmods, "char", kMaxAttrSize, kVarHdrSz);
  }
  if (oid == kVarchar) {
    return LengthTypmod(typmods, "varchar", kMaxAttrSize, kVarHdrSz);
  }
  if (oid == kBit) {
    return LengthTypmod(typmods, "bit", kMaxAttrSize * 8, 0);
  }
  if (oid == kVarbit) {
    return LengthTypmod(typmods, "varbit", kMaxAttrSize * 8, 0);
  }
  if (oid == kNumeric) {
    return NumericTypmodIn(typmods);
  }
  if (oid == kTime || oid == kTimetz) {
    return TimeTypmod(typmods, "TIME", oid == kTimetz);
  }
  if (oid == kTimestamp || oid == kTimestamptz) {
    return TimeTypmod(typmods, "TIMESTAMP", oid == kTimestamptz);
  }
  SDB_ASSERT(oid == kInterval);
  return IntervalTypmod(typmods);
}

uint64_t FindTypeIn(const Session& session, std::string_view schema,
                    std::string_view name) {
  if (const auto* system = FindSystemObject(kSystemNamespaces, schema)) {
    if (const auto* builtin = FindBuiltinType(system->oid, name)) {
      return static_cast<uint64_t>(builtin->oid);
    }
  }
  if (auto entry =
        FindInSchema(session, schema, duckdb::CatalogType::TYPE_ENTRY, name)) {
    return entry->oid;
  }
  if (auto entry =
        FindInSchema(session, schema, duckdb::CatalogType::TABLE_ENTRY, name)) {
    return RowTypeOid(entry->oid);
  }
  for (size_t strip = 1; strip < name.size() && name[strip - 1] == '_';
       ++strip) {
    for (const auto type :
         {duckdb::CatalogType::TYPE_ENTRY, duckdb::CatalogType::TABLE_ENTRY}) {
      if (auto entry = FindInSchema(session, schema, type, name.substr(strip));
          entry && ArrayTypeName(*session.transaction, *entry) == name) {
        return TypeArrayOid(type == duckdb::CatalogType::TYPE_ENTRY
                              ? entry->oid
                              : RowTypeOid(entry->oid));
      }
    }
  }
  return kInvalidOid;
}

uint64_t FindVisibleType(const Session& session, std::string_view name) {
  for (const auto& schema : session.search_path) {
    if (const auto oid = FindTypeIn(session, schema.name, name);
        oid != kInvalidOid) {
      return oid;
    }
  }
  return kInvalidOid;
}

uint64_t ArrayTypeOf(const Session& session, uint64_t oid) {
  if (oid < kMaxSystem) {
    const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid));
    return builtin ? static_cast<uint64_t>(builtin->array) : kInvalidOid;
  }
  return ArrayElementOid(session, oid) != kInvalidOid ? kInvalidOid
                                                      : TypeArrayOid(oid);
}

struct ResolvedType {
  uint64_t oid;
  int32_t typmod;
};

std::optional<ResolvedType> ResolveType(const Session& session,
                                        std::string_view text,
                                        irs::pg::SqlErrorData* soft_error) {
  if (absl::c_all_of(text, absl::ascii_isspace)) {
    return Miss(soft_error,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                               ERR_MSG("invalid type name \"", text, "\"")));
  }
  const auto parsed = TypeParser{text}.Parse();
  if (!parsed) {
    return Miss(soft_error,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                               ERR_MSG("invalid type name \"", text, "\"")));
  }
  const auto display =
    absl::StrCat(absl::StrJoin(parsed->names, "."), parsed->array ? "[]" : "");
  uint64_t element = kInvalidOid;
  if (parsed->keyword) {
    const auto* builtin =
      FindBuiltinType(kPgCatalogSchema, parsed->names.front());
    SDB_ASSERT(builtin);
    element = static_cast<uint64_t>(builtin->oid);
  } else {
    const auto object = Deconstruct(session, parsed->names);
    if (object.schema.empty()) {
      element = FindVisibleType(session, object.name);
    } else if (!SchemaOid(session, object.schema, soft_error)) {
      return std::nullopt;
    } else {
      element = FindTypeIn(session, object.schema, object.name);
    }
  }
  if (element == kInvalidOid) {
    return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                                           ERR_MSG("type \"", display,
                                                   "\" does not exist")));
  }
  const auto typmod = TypmodIn(element, parsed->typmods, display);
  if (!parsed->array) {
    return ResolvedType{.oid = element, .typmod = typmod};
  }
  const auto array = ArrayTypeOf(session, element);
  if (array == kInvalidOid) {
    return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                                           ERR_MSG("type \"", display,
                                                   "\" does not exist")));
  }
  return ResolvedType{.oid = array, .typmod = typmod};
}

}  // namespace

duckdb::optional_ptr<duckdb::CatalogEntry> AttachedRelation(
  const Session& session, std::string_view text) {
  const auto names = ParseNames(text, nullptr);
  if (!names || names->size() != 3 ||
      duckdb::DatabaseManager::TryGetDefaultDatabase(*session.context) ==
        names->front() ||
      !duckdb::Catalog::GetCatalogEntry(*session.context,
                                        duckdb::Identifier{names->front()})) {
    return nullptr;
  }
  const duckdb::QualifiedName name{duckdb::Identifier{(*names)[0]},
                                   duckdb::Identifier{(*names)[1]},
                                   duckdb::Identifier{(*names)[2]}};
  for (const auto type : kRelationSets) {
    if (auto entry = duckdb::Catalog::GetEntry(
          *session.context, duckdb::EntryLookupInfo{type, name},
          duckdb::OnEntryNotFound::RETURN_NULL)) {
      return entry;
    }
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_TABLE),
    ERR_MSG("relation \"", absl::StrJoin(*names, "."), "\" does not exist"));
}

std::string_view SqlTypeName(int32_t oid) {
  switch (oid) {
    case kBit:
      return "bit";
    case kBool:
      return "boolean";
    case kBpchar:
      return "character";
    case kFloat4:
      return "real";
    case kFloat8:
      return "double precision";
    case kInt2:
      return "smallint";
    case kInt4:
      return "integer";
    case kInt8:
      return "bigint";
    case kNumeric:
      return "numeric";
    case kInterval:
      return "interval";
    case kTime:
      return "time without time zone";
    case kTimetz:
      return "time with time zone";
    case kTimestamp:
      return "timestamp without time zone";
    case kTimestamptz:
      return "timestamp with time zone";
    case kVarbit:
      return "bit varying";
    case kVarchar:
      return "character varying";
    case kJson:
      return "json";
  }
  return {};
}

namespace {

OutText BuiltinTypeName(const Session& session, const BuiltinType& builtin,
                        bool qualify) {
  if (!qualify && (builtin.nsp == kPgCatalogSchema ||
                   FindVisibleType(session, builtin.name) ==
                     static_cast<uint64_t>(builtin.oid))) {
    return OutName(builtin.name);
  }
  return QualifiedOutName(
    FindSystemObject(kSystemNamespaces, builtin.nsp)->name, builtin.name);
}

std::optional<OutText> FormatUserType(const Session& session, uint64_t oid,
                                      std::optional<int32_t> typmod,
                                      bool qualify) {
  if (!session.context) {
    return std::nullopt;
  }
  const auto array = ArrayElementOid(session, oid);
  const auto element = array != kInvalidOid ? array : oid;
  const auto object = TypeObject(session, element);
  if (!object) {
    return std::nullopt;
  }
  auto name = !qualify && FindVisibleType(session, object->name) == element
                ? OutName(object->name)
                : OutText{QualifiedOutName(object->schema, object->name)};
  const std::string_view suffix = array != kInvalidOid ? "[]" : "";
  if (typmod && *typmod >= 0) {
    return absl::StrCat(AsView(name), "(", *typmod, ")", suffix);
  }
  if (!suffix.empty()) {
    return absl::StrCat(AsView(name), suffix);
  }
  return name;
}

OutText FormatType(const Session& session, uint64_t oid, bool qualify) {
  if (oid == kInvalidOid) {
    return std::string_view{"-"};
  }
  if (oid < kMaxSystem) {
    const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid));
    if (!builtin) {
      return absl::StrCat(oid);
    }
    if (builtin->IsArray()) {
      return absl::StrCat(
        AsView(
          FormatType(session, static_cast<uint64_t>(builtin->elem), qualify)),
        "[]");
    }
    if (const auto name = SqlTypeName(builtin->oid); !name.empty()) {
      return name;
    }
    return BuiltinTypeName(session, *builtin, qualify);
  }
  if (auto name = FormatUserType(session, oid, std::nullopt, qualify)) {
    return std::move(*name);
  }
  return absl::StrCat(oid);
}

OutText ClassOut(const Session& session, uint64_t oid) {
  if (!session.context) {
    return absl::StrCat(oid);
  }
  const auto relation = FindRelation(session, oid);
  if (!relation) {
    return absl::StrCat(oid);
  }
  const auto object = NameOf(*session.transaction, *relation);
  if (IsTemporary(*relation)) {
    return OutName(object.name);
  }
  return RelationName(session, object.schema, object.name);
}

std::optional<uint64_t> ClassIn(const Session& session, std::string_view text,
                                irs::pg::SqlErrorData* soft_error) {
  const auto names = ParseNames(text, soft_error);
  if (!names) {
    return std::nullopt;
  }
  if (names->size() > 3) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("improper relation name (too many dotted names): ",
                            absl::StrJoin(*names, ".")));
  }
  const auto database =
    duckdb::DatabaseManager::TryGetDefaultDatabase(*session.context);
  if (names->size() == 3 && database != names->front()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cross-database references are not implemented: \"",
                            absl::StrJoin(*names, "."), "\""));
  }
  const duckdb::Identifier name{names->back()};
  const auto qualified =
    names->size() == 1
      ? duckdb::QualifiedName{name}
      : duckdb::QualifiedName{
          database, duckdb::Identifier{(*names)[names->size() - 2]}, name};
  if (const auto oid = ResolveRelation(session, qualified);
      oid != kInvalidOid) {
    return oid;
  }
  return Miss(soft_error,
              SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                             ERR_MSG("relation \"", absl::StrJoin(*names, "."),
                                     "\" does not exist")));
}

struct ProcArgs {
  std::span<const duckdb::idx_t> builtin;
  std::vector<duckdb::idx_t> macro;
};

std::span<const duckdb::idx_t> ArgTypes(const ProcArgs& args) {
  return args.macro.empty() ? args.builtin
                            : std::span<const duckdb::idx_t>{args.macro};
}

struct ProcCandidate {
  uint64_t oid;
  uint64_t nsp;
  ProcArgs args;
};

struct ProcInfo {
  uint64_t nsp;
  std::string_view schema;
  std::string_view name;
  ProcArgs args;
};

std::vector<duckdb::idx_t> MacroArgs(const duckdb::MacroFunction& macro) {
  std::vector<duckdb::idx_t> args;
  args.reserve(macro.types.size());
  for (const auto& type : macro.types) {
    args.emplace_back(CatalogTypeOid(type));
  }
  return args;
}

void CollectProcs(const Session& session, const BuiltinFunctions& builtins,
                  std::string_view schema, std::string_view name,
                  std::vector<ProcCandidate>& candidates) {
  const auto add = [&](uint64_t oid, uint64_t nsp, ProcArgs args) {
    const auto types = ArgTypes(args);
    if (absl::c_none_of(candidates, [&](const ProcCandidate& candidate) {
          return absl::c_equal(ArgTypes(candidate.args), types);
        })) {
      candidates.emplace_back(
        ProcCandidate{.oid = oid, .nsp = nsp, .args = std::move(args)});
    }
  };
  if (const auto* system = FindSystemObject(kSystemNamespaces, schema)) {
    for (const auto index : builtins.Named(name)) {
      const auto& function = builtins.All()[index];
      if (function.nsp == system->oid) {
        add(function.oid, system->oid, ProcArgs{.builtin = function.argtypes});
      }
    }
    return;
  }
  for (const auto type : {duckdb::CatalogType::MACRO_ENTRY,
                          duckdb::CatalogType::TABLE_MACRO_ENTRY}) {
    if (auto entry = FindInSchema(session, schema, type, name)) {
      for (const auto& macro :
           entry->Cast<duckdb::MacroCatalogEntry>().macros) {
        add(entry->oid, entry->ParentSchemaOid(),
            ProcArgs{.macro = MacroArgs(*macro)});
      }
    }
  }
}

std::vector<ProcCandidate> VisibleProcs(const Session& session,
                                        const BuiltinFunctions& builtins,
                                        std::string_view name) {
  std::vector<ProcCandidate> candidates;
  for (const auto& schema : session.search_path) {
    CollectProcs(session, builtins, schema.name, name, candidates);
  }
  return candidates;
}

std::vector<ProcCandidate> FindProcs(const Session& session,
                                     const BuiltinFunctions& builtins,
                                     const NameList& names) {
  const auto object = Deconstruct(session, names);
  if (object.schema.empty()) {
    return VisibleProcs(session, builtins, object.name);
  }
  std::vector<ProcCandidate> candidates;
  CollectProcs(session, builtins, object.schema, object.name, candidates);
  return candidates;
}

std::optional<ProcInfo> FindProc(const Session& session,
                                 const BuiltinFunctions& builtins,
                                 uint64_t oid) {
  if (oid < kMaxSystem) {
    if (const auto* function = builtins.Find(oid)) {
      return ProcInfo{
        .nsp = function->nsp,
        .schema = FindSystemObject(kSystemNamespaces, function->nsp)->name,
        .name = function->name,
        .args = {.builtin = function->argtypes}};
    }
    return std::nullopt;
  }
  const auto* macro = FindMacro(session, oid);
  if (!macro) {
    return std::nullopt;
  }
  const auto object = NameOf(*session.transaction, *macro);
  return ProcInfo{.nsp = macro->ParentSchemaOid(),
                  .schema = object.schema,
                  .name = object.name,
                  .args = {.macro = MacroArgs(*macro->macros.front())}};
}

bool ProcVisible(const Session& session, const BuiltinFunctions& builtins,
                 const ProcInfo& info, bool signature) {
  const auto candidates = VisibleProcs(session, builtins, info.name);
  return signature
           ? absl::c_any_of(candidates,
                            [&](const ProcCandidate& candidate) {
                              return candidate.nsp == info.nsp &&
                                     absl::c_equal(ArgTypes(candidate.args),
                                                   ArgTypes(info.args));
                            })
           : candidates.size() == 1 && candidates.front().nsp == info.nsp;
}

std::string ProcOut(const Session& session, uint64_t oid, bool signature) {
  if (!session.context) {
    return absl::StrCat(oid);
  }
  const auto builtins = GetBuiltinFunctions(*session.context);
  const auto info = FindProc(session, *builtins, oid);
  if (!info) {
    return absl::StrCat(oid);
  }
  auto name = ProcVisible(session, *builtins, *info, signature)
                ? QuoteIdentifier(info->name)
                : QualifiedOutName(info->schema, info->name);
  if (!signature) {
    return name;
  }
  return absl::StrCat(
    name, "(",
    absl::StrJoin(ArgTypes(info->args), ",",
                  [&](std::string* out, duckdb::idx_t arg) {
                    out->append(AsView(FormatType(session, arg, false)));
                  }),
    ")");
}

std::optional<uint64_t> ProcIn(const Session& session, std::string_view text,
                               irs::pg::SqlErrorData* soft_error) {
  const auto names = ParseNames(text, soft_error);
  if (!names) {
    return std::nullopt;
  }
  const auto builtins = GetBuiltinFunctions(*session.context);
  const auto candidates = FindProcs(session, *builtins, *names);
  if (candidates.empty()) {
    return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                                           ERR_MSG("function \"", text,
                                                   "\" does not exist")));
  }
  if (candidates.size() > 1) {
    return Miss(
      soft_error,
      SQL_ERROR_DATA(ERR_CODE(ERRCODE_AMBIGUOUS_FUNCTION),
                     ERR_MSG("more than one function named \"", text, "\"")));
  }
  return candidates.front().oid;
}

struct Signature {
  NameList names;
  std::vector<duckdb::idx_t> args;
};

std::optional<Signature> ParseSignature(const Session& session,
                                        std::string_view text, bool allow_none,
                                        irs::pg::SqlErrorData* soft_error) {
  bool quoted = false;
  size_t open = 0;
  for (; open < text.size(); ++open) {
    if (text[open] == '"') {
      quoted = !quoted;
    } else if (text[open] == '(' && !quoted) {
      break;
    }
  }
  if (open == text.size()) {
    return Miss(soft_error,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                               ERR_MSG("expected a left parenthesis")));
  }
  auto names = ParseNames(text.substr(0, open), soft_error);
  if (!names) {
    return std::nullopt;
  }
  auto rest = absl::StripTrailingAsciiWhitespace(text.substr(open + 1));
  if (!absl::ConsumeSuffix(&rest, ")")) {
    return Miss(soft_error,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                               ERR_MSG("expected a right parenthesis")));
  }
  std::vector<duckdb::idx_t> args;
  size_t pos = 0;
  bool had_comma = false;
  while (true) {
    while (pos < rest.size() && absl::ascii_isspace(rest[pos])) {
      ++pos;
    }
    if (pos == rest.size()) {
      if (had_comma) {
        return Miss(soft_error, SQL_ERROR_DATA(
                                  ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                  ERR_MSG("expected a type name")));
      }
      break;
    }
    const auto start = pos;
    int depth = 0;
    for (; pos < rest.size(); ++pos) {
      const char c = rest[pos];
      if (c == '"') {
        quoted = !quoted;
      } else if (c == ',' && !quoted && depth == 0) {
        break;
      } else if (!quoted && (c == '(' || c == '[')) {
        ++depth;
      } else if (!quoted && (c == ')' || c == ']')) {
        --depth;
      }
    }
    if (quoted || depth != 0) {
      return Miss(soft_error,
                  SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                 ERR_MSG("improper type name")));
    }
    const auto type_name =
      absl::StripTrailingAsciiWhitespace(rest.substr(start, pos - start));
    had_comma = pos < rest.size();
    if (had_comma) {
      ++pos;
    }
    if (args.size() >= kMaxFunctionArgs) {
      return Miss(soft_error,
                  SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                                 ERR_MSG("too many arguments")));
    }
    if (allow_none && absl::EqualsIgnoreCase(type_name, "none")) {
      args.emplace_back(0);
      continue;
    }
    const auto type = ResolveType(session, type_name, soft_error);
    if (!type) {
      return std::nullopt;
    }
    args.emplace_back(type->oid);
  }
  return Signature{.names = std::move(*names), .args = std::move(args)};
}

std::optional<uint64_t> ProcedureIn(const Session& session,
                                    std::string_view text,
                                    irs::pg::SqlErrorData* soft_error) {
  const auto signature = ParseSignature(session, text, false, soft_error);
  if (!signature) {
    return std::nullopt;
  }
  const auto builtins = GetBuiltinFunctions(*session.context);
  const auto candidates = FindProcs(session, *builtins, signature->names);
  SDB_PARK_ONCE_ON_FAILURE("pause_procedure_in_after_find");
  for (const auto& candidate : candidates) {
    if (absl::c_equal(ArgTypes(candidate.args), signature->args)) {
      return candidate.oid;
    }
  }
  return Miss(soft_error, SQL_ERROR_DATA(
                            ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                            ERR_MSG("function \"", text, "\" does not exist")));
}

std::optional<uint64_t> OperIn(const Session& session, std::string_view text,
                               irs::pg::SqlErrorData* soft_error) {
  const auto names = ParseNames(text, soft_error);
  if (!names) {
    return std::nullopt;
  }
  Deconstruct(session, *names);
  return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                                         ERR_MSG("operator does not exist: ",
                                                 absl::StrJoin(*names, "."))));
}

std::optional<uint64_t> OperatorIn(const Session& session,
                                   std::string_view text,
                                   irs::pg::SqlErrorData* soft_error) {
  const auto signature = ParseSignature(session, text, true, soft_error);
  if (!signature) {
    return std::nullopt;
  }
  if (signature->args.size() == 1) {
    return Miss(
      soft_error,
      SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_PARAMETER), ERR_MSG("missing argument"),
        ERR_HINT(
          "Use NONE to denote the missing argument of a unary operator.")));
  }
  if (signature->args.size() != 2) {
    return Miss(soft_error,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_TOO_MANY_ARGUMENTS),
                               ERR_MSG("too many arguments"),
                               ERR_HINT("Provide two argument types for "
                                        "operator.")));
  }
  Deconstruct(session, signature->names);
  return Miss(soft_error,
              SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                             ERR_MSG("operator does not exist: ", text)));
}

std::optional<uint64_t> CollationIn(const Session& session,
                                    std::string_view text,
                                    irs::pg::SqlErrorData* soft_error) {
  const auto names = ParseNames(text, soft_error);
  if (!names) {
    return std::nullopt;
  }
  const auto object = Deconstruct(session, *names);
  if (object.schema.empty() ||
      object.schema == irs::StaticStrings::kPgCatalogSchema) {
    const auto collations = BuiltinCollations();
    const auto it =
      absl::c_find_if(collations, [&](const BuiltinCollation& collation) {
        return collation.name == object.name;
      });
    if (it != collations.end()) {
      return static_cast<uint64_t>(it->oid);
    }
  }
  return Miss(
    soft_error,
    SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                   ERR_MSG("collation \"", absl::StrJoin(*names, "."),
                           "\" for encoding \"UTF8\" does not exist")));
}

OutText CollationOut(uint64_t oid) {
  if (const auto* collation = FindBuiltinCollation(static_cast<int64_t>(oid))) {
    return OutName(collation->name);
  }
  return absl::StrCat(oid);
}

std::optional<uint64_t> ConfigIn(const Session& session, std::string_view text,
                                 irs::pg::SqlErrorData* soft_error) {
  const auto names = ParseNames(text, soft_error);
  if (!names) {
    return std::nullopt;
  }
  Deconstruct(session, *names);
  return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                                         ERR_MSG("text search configuration \"",
                                                 absl::StrJoin(*names, "."),
                                                 "\" does not exist")));
}

uint64_t FindVisibleDictionary(const Session& session, std::string_view name) {
  for (const auto& schema : session.search_path) {
    if (auto entry = FindInSchema(session, schema.name,
                                  duckdb::CatalogType::TOKENIZER_ENTRY, name)) {
      return entry->oid;
    }
  }
  return kInvalidOid;
}

std::optional<uint64_t> DictionaryIn(const Session& session,
                                     std::string_view text,
                                     irs::pg::SqlErrorData* soft_error) {
  const auto names = ParseNames(text, soft_error);
  if (!names) {
    return std::nullopt;
  }
  const auto object = Deconstruct(session, *names);
  if (object.schema.empty()) {
    if (const auto oid = FindVisibleDictionary(session, object.name);
        oid != kInvalidOid) {
      return oid;
    }
  } else if (auto entry = FindInSchema(session, object.schema,
                                       duckdb::CatalogType::TOKENIZER_ENTRY,
                                       object.name)) {
    return entry->oid;
  }
  return Miss(soft_error, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                                         ERR_MSG("text search dictionary \"",
                                                 absl::StrJoin(*names, "."),
                                                 "\" does not exist")));
}

OutText DictionaryOut(const Session& session, uint64_t oid) {
  auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != duckdb::CatalogType::TOKENIZER_ENTRY) {
    return absl::StrCat(oid);
  }
  const auto object = NameOf(*session.transaction, *entry);
  if (FindVisibleDictionary(session, object.name) == oid) {
    return OutName(object.name);
  }
  return QualifiedOutName(object.schema, object.name);
}

std::optional<uint64_t> RoleIn(const Session& session, std::string_view text,
                               irs::pg::SqlErrorData* soft_error) {
  const auto name = ParseSingleName(text, soft_error);
  if (!name) {
    return std::nullopt;
  }
  const auto graph = auth::RolesOf(session.context);
  if (const auto* role = graph->FindByName(*name)) {
    return role->first;
  }
  return Miss(soft_error,
              SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                             ERR_MSG("role \"", *name, "\" does not exist")));
}

std::string RoleOut(const Session& session, uint64_t oid) {
  if (session.context) {
    const auto graph = auth::RolesOf(session.context);
    if (const auto name = graph->NameOf(oid); !name.empty()) {
      return QuoteIdentifier(name);
    }
  }
  return absl::StrCat(oid);
}

std::optional<uint64_t> NamespaceIn(const Session& session,
                                    std::string_view text,
                                    irs::pg::SqlErrorData* soft_error) {
  const auto name = ParseSingleName(text, soft_error);
  if (!name) {
    return std::nullopt;
  }
  return SchemaOid(session, *name, soft_error);
}

OutText NamespaceOut(const Session& session, uint64_t oid) {
  if (const auto* system = FindSystemObject(kSystemNamespaces, oid)) {
    return system->name;
  }
  if (auto schema = SchemaByOid(session, oid)) {
    return OutName(schema->name.GetIdentifierName());
  }
  return absl::StrCat(oid);
}

bool IsNumericOid(std::string_view text) {
  return !text.empty() && absl::c_all_of(text, absl::ascii_isdigit);
}

std::optional<uint64_t> ParseOid(std::string_view text,
                                 irs::pg::SqlErrorData* soft_error) {
  int64_t oid = 0;
  const auto* end = text.data() + text.size();
  const auto parsed = fast_float::from_chars(
    text.data(), end, oid, text.size() > 1 && text.front() == '0' ? 8 : 10);
  if (parsed.ec == std::errc::result_out_of_range) {
    return Miss(soft_error,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                               ERR_MSG("value \"", text,
                                       "\" is out of range for type oid")));
  }
  if (parsed.ptr != end) {
    return Miss(soft_error,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                               ERR_MSG("invalid input syntax for type oid: \"",
                                       text, "\"")));
  }
  return oid;
}

std::string IntervalTypmodOut(int32_t typmod) {
  std::string out;
  if (const auto* range = FindIntervalRange(typmod)) {
    out = absl::StrCat(" ", range->fields);
  }
  if (const auto precision = typmod & kIntervalFullPrecision;
      precision != kIntervalFullPrecision) {
    absl::StrAppend(&out, "(", precision, ")");
  }
  return out;
}

constexpr std::array kTypmodlessTypes{kBool,   kInt2,   kInt4, kInt8,
                                      kFloat4, kFloat8, kJson};

}  // namespace

std::string TypeIdentity(const Session& session, uint64_t oid) {
  return std::string{AsView(FormatType(session, oid, true))};
}

std::string ProcIdentity(const Session& session, uint64_t oid) {
  const auto builtins = GetBuiltinFunctions(*session.context);
  const auto info = FindProc(session, *builtins, oid);
  SDB_ASSERT(info);
  return absl::StrCat(QualifiedOutName(info->schema, info->name), "(",
                      absl::StrJoin(ArgTypes(info->args), ",",
                                    [&](std::string* out, duckdb::idx_t arg) {
                                      out->append(TypeIdentity(session, arg));
                                    }),
                      ")");
}

OutText FormatTypeOut(const Session& session, uint64_t oid,
                      std::optional<int32_t> typmod) {
  if (oid == kInvalidOid) {
    return FormatType(session, oid, false);
  }
  if (oid >= kMaxSystem) {
    return FormatUserType(session, oid, typmod, false)
      .value_or(std::string_view{"???"});
  }
  const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid));
  if (!builtin) {
    return std::string_view{"???"};
  }
  if (builtin->IsArray()) {
    return absl::StrCat(
      AsView(
        FormatTypeOut(session, static_cast<uint64_t>(builtin->elem), typmod)),
      "[]");
  }
  if (typmod && *typmod < 0 &&
      (builtin->oid == kBit || builtin->oid == kBpchar)) {
    return BuiltinTypeName(session, *builtin, false);
  }
  if (typmod && *typmod >= 0) {
    const auto sql = SqlTypeName(builtin->oid);
    switch (builtin->oid) {
      case kBit:
      case kVarbit:
        return absl::StrCat(sql, "(", *typmod, ")");
      case kBpchar:
      case kVarchar:
        if (*typmod <= kVarHdrSz) {
          return sql;
        }
        return absl::StrCat(sql, "(", *typmod - kVarHdrSz, ")");
      case kNumeric:
        if (*typmod < kVarHdrSz) {
          return sql;
        }
        return absl::StrCat(sql, "(", NumericTypmodPrecision(*typmod), ",",
                            NumericTypmodScale(*typmod), ")");
      case kTime:
      case kTimetz:
      case kTimestamp:
      case kTimestamptz: {
        const auto space = sql.find(' ');
        return absl::StrCat(sql.substr(0, space), "(", *typmod, ")",
                            sql.substr(space));
      }
      case kInterval:
        return absl::StrCat(sql, IntervalTypmodOut(*typmod));
    }
  }
  if (typmod && *typmod >= 0 &&
      !absl::c_linear_search(kTypmodlessTypes, builtin->oid)) {
    return absl::StrCat(AsView(FormatType(session, oid, false)), "(", *typmod,
                        ")");
  }
  return FormatType(session, oid, false);
}

namespace {

OutText RegText(const Session& session, RegKind kind, uint64_t oid) {
  switch (kind) {
    case RegKind::Proc:
      return ProcOut(session, oid, false);
    case RegKind::Procedure:
      return ProcOut(session, oid, true);
    case RegKind::Oper:
    case RegKind::Operator:
    case RegKind::Config:
      return absl::StrCat(oid);
    case RegKind::Class:
      return ClassOut(session, oid);
    case RegKind::Type:
      return FormatType(session, oid, false);
    case RegKind::Collation:
      return CollationOut(oid);
    case RegKind::Dictionary:
      return DictionaryOut(session, oid);
    case RegKind::Role:
      return RoleOut(session, oid);
    case RegKind::Namespace:
      return NamespaceOut(session, oid);
  }
}

IRS_NO_INLINE std::string_view CacheRegOut(const Session& session, RegKind kind,
                                           uint64_t oid, uint64_t key,
                                           RegOutRecent& recent) {
  if (session.reg_out.size() >= kRegOutLimit) {
    session.reg_out.clear();
    session.reg_out_texts.clear();
    session.reg_out_recent = {};
  }
  const auto tag = std::to_underlying(kind);
  const auto [it, inserted] = session.reg_out.try_emplace({tag, oid});
  if (inserted) {
    auto text = RegText(session, kind, oid);
    auto* built = std::get_if<std::string>(&text);
    it->second = built ? session.reg_out_texts.emplace_back(std::move(*built))
                       : *std::get_if<std::string_view>(&text);
  }
  recent = {.key = key, .text = it->second};
  return it->second;
}

}  // namespace

std::string_view RegOut(const Session& session, RegKind kind, uint64_t oid) {
  if (oid == kInvalidOid) {
    return kind == RegKind::Oper || kind == RegKind::Operator ? "0" : "-";
  }
  const auto key = oid ^ (uint64_t{std::to_underlying(kind)} << 56);
  auto& recent =
    session.reg_out_recent[(key * uint64_t{0x9E3779B97F4A7C15}) >> 58];
  if (recent.key == key) {
    return recent.text;
  }
  return CacheRegOut(session, kind, oid, key, recent);
}

std::optional<RegKind> RegKindOf(const duckdb::LogicalType& type) {
  if (IsRegtype(type)) {
    return RegKind::Type;
  }
  if (IsRegclass(type)) {
    return RegKind::Class;
  }
  if (IsRegnamespace(type)) {
    return RegKind::Namespace;
  }
  if (IsRegproc(type)) {
    return RegKind::Proc;
  }
  if (IsRegprocedure(type)) {
    return RegKind::Procedure;
  }
  if (IsRegoper(type)) {
    return RegKind::Oper;
  }
  if (IsRegoperator(type)) {
    return RegKind::Operator;
  }
  if (IsRegrole(type)) {
    return RegKind::Role;
  }
  if (IsRegconfig(type)) {
    return RegKind::Config;
  }
  if (IsRegdictionary(type)) {
    return RegKind::Dictionary;
  }
  if (IsRegcollation(type)) {
    return RegKind::Collation;
  }
  return std::nullopt;
}

std::optional<uint64_t> RegIn(const Session& session, RegKind kind,
                              std::string_view text,
                              irs::pg::SqlErrorData* soft_error) {
  if (text == "-" && kind != RegKind::Oper && kind != RegKind::Operator) {
    return kInvalidOid;
  }
  if (IsNumericOid(text)) {
    return ParseOid(text, soft_error);
  }
  switch (kind) {
    case RegKind::Proc:
      return ProcIn(session, text, soft_error);
    case RegKind::Procedure:
      return ProcedureIn(session, text, soft_error);
    case RegKind::Oper:
      return OperIn(session, text, soft_error);
    case RegKind::Operator:
      return OperatorIn(session, text, soft_error);
    case RegKind::Class:
      return ClassIn(session, text, soft_error);
    case RegKind::Type:
      if (const auto type = ResolveType(session, text, soft_error)) {
        return type->oid;
      }
      return std::nullopt;
    case RegKind::Collation:
      return CollationIn(session, text, soft_error);
    case RegKind::Config:
      return ConfigIn(session, text, soft_error);
    case RegKind::Dictionary:
      return DictionaryIn(session, text, soft_error);
    case RegKind::Role:
      return RoleIn(session, text, soft_error);
    case RegKind::Namespace:
      return NamespaceIn(session, text, soft_error);
  }
}

bool RelationVisible(const Session& session, std::string_view schema,
                     std::string_view name) {
  if (TemporaryRelationOid(session, name) != kInvalidOid) {
    return false;
  }
  for (const auto& path : session.search_path) {
    if (path.name == schema) {
      return true;
    }
    if (RelationOid(session, path, name) != kInvalidOid) {
      return false;
    }
  }
  return false;
}

OutText RelationName(const Session& session, std::string_view schema,
                     std::string_view name) {
  if (RelationVisible(session, schema, name)) {
    return OutName(name);
  }
  return QualifiedOutName(schema, name);
}

std::optional<bool> RelationIsVisible(const Session& session, uint64_t oid) {
  const auto relation = FindRelation(session, oid);
  if (!relation) {
    return std::nullopt;
  }
  if (IsTemporary(*relation)) {
    return true;
  }
  const auto object = NameOf(*session.transaction, *relation);
  return RelationVisible(session, object.schema, object.name);
}

namespace {

duckdb::optional_ptr<duckdb::CatalogEntry> ArrayElementEntry(
  const Session& session, uint64_t oid) {
  if (oid & kRowTypeOidBit) {
    const auto relation = RowTypeOwner(session, oid);
    return relation ? relation->entry : nullptr;
  }
  const auto element = EntryByOid(session, TypeElementOid(oid));
  if (!element || element->type != duckdb::CatalogType::TYPE_ENTRY) {
    return nullptr;
  }
  return element;
}

}  // namespace

bool TypeExists(const Session& session, uint64_t oid) {
  return TypeObject(session, oid) || ArrayElementEntry(session, oid);
}

std::optional<bool> TypeIsVisible(const Session& session, uint64_t oid) {
  if (const auto object = TypeObject(session, oid)) {
    return FindVisibleType(session, object->name) == oid;
  }
  const auto element = ArrayElementEntry(session, oid);
  if (!element) {
    return std::nullopt;
  }
  return FindVisibleType(session,
                         ArrayTypeName(*session.transaction, *element)) == oid;
}

std::optional<bool> FunctionIsVisible(const Session& session, uint64_t oid) {
  const auto builtins = GetBuiltinFunctions(*session.context);
  const auto info = FindProc(session, *builtins, oid);
  if (!info) {
    return std::nullopt;
  }
  return ProcVisible(session, *builtins, *info, true);
}

namespace {

template<RegKind Kind>
void ToRegFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  irs::pg::SqlErrorData error;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t text) -> duckdb::optional<int64_t> {
      const auto oid = RegIn(session, Kind, connector::AsView(text), &error);
      if (!oid) {
        return duckdb::nullopt;
      }
      return static_cast<int64_t>(*oid);
    });
}

void ToRegtypmodFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  irs::pg::SqlErrorData error;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int32_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t text) -> duckdb::optional<int32_t> {
      const auto type = ResolveType(session, connector::AsView(text), &error);
      if (!type) {
        return duckdb::nullopt;
      }
      return type->typmod;
    });
}

void RegisterToRegFunction(duckdb::ExtensionLoader& loader,
                           std::string_view name, duckdb::LogicalType type,
                           duckdb::scalar_function_t function) {
  duckdb::ScalarFunction scalar{
    {duckdb::LogicalType::VARCHAR}, std::move(type), std::move(function)};
  scalar.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  RegisterScalar(loader, irs::StaticStrings::kPgCatalogSchema, name,
                 duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY,
                 InitSession, {scalar});
}

}  // namespace

void RegisterRegprocFunctions(duckdb::ExtensionLoader& loader) {
  RegisterToRegFunction(loader, "to_regproc", REGPROC(),
                        ToRegFunction<RegKind::Proc>);
  RegisterToRegFunction(loader, "to_regprocedure", REGPROCEDURE(),
                        ToRegFunction<RegKind::Procedure>);
  RegisterToRegFunction(loader, "to_regoper", REGOPER(),
                        ToRegFunction<RegKind::Oper>);
  RegisterToRegFunction(loader, "to_regoperator", REGOPERATOR(),
                        ToRegFunction<RegKind::Operator>);
  RegisterToRegFunction(loader, "to_regclass", REGCLASS(),
                        ToRegFunction<RegKind::Class>);
  RegisterToRegFunction(loader, "to_regtype", REGTYPE(),
                        ToRegFunction<RegKind::Type>);
  RegisterToRegFunction(loader, "to_regcollation", REGCOLLATION(),
                        ToRegFunction<RegKind::Collation>);
  RegisterToRegFunction(loader, "to_regrole", REGROLE(),
                        ToRegFunction<RegKind::Role>);
  RegisterToRegFunction(loader, "to_regnamespace", REGNAMESPACE(),
                        ToRegFunction<RegKind::Namespace>);
  RegisterToRegFunction(loader, "to_regtypemod", duckdb::LogicalType::INTEGER,
                        ToRegtypmodFunction);
}

}  // namespace sdb::pg
