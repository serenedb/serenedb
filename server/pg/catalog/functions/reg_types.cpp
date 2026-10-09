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

#include "pg/catalog/functions/reg_types.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_join.h>
#include <absl/strings/strip.h>

#include <algorithm>
#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/function/macro_function.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <limits>
#include <ranges>
#include <span>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/functions/format_type.h"
#include "pg/catalog/lookup.h"
#include "pg/sql_utils.h"

namespace sdb::pg {

const RegType* FindRegType(const duckdb::LogicalType& type) {
  const auto it = absl::c_find_if(kRegTypes, [&](const RegType& reg) {
    return type.GetAlias() == reg.alias;
  });
  return it == kRegTypes.end() ? nullptr : &*it;
}

duckdb::LogicalType RegLogicalType(const RegType& reg) {
  return duckdb::LogicalType{duckdb::LogicalTypeId::BIGINT}.WithAlias(
    std::string{reg.alias});
}

namespace {

constexpr std::array kRelationSets{duckdb::CatalogType::TABLE_ENTRY,
                                   duckdb::CatalogType::SEQUENCE_ENTRY,
                                   duckdb::CatalogType::INDEX_ENTRY};

}  // namespace

std::string QualifiedOutName(std::string_view schema, std::string_view name) {
  return absl::StrCat(QuoteIdentifier(schema), ".", QuoteIdentifier(name));
}

uint64_t ResolveRelation(duckdb::ClientContext& context,
                         const duckdb::QualifiedName& name) {
  // Every half of the relation namespace, in the order postgres resolves them
  // -- a table and a view share duckdb's set, so the first lookup covers both.
  for (const auto type : kRelationSets) {
    if (auto entry = duckdb::Catalog::GetEntry(
          context, duckdb::EntryLookupInfo{type, name},
          duckdb::OnEntryNotFound::RETURN_NULL)) {
      return entry->oid;
    }
  }
  if (auto entry = duckdb::Catalog::GetEntry(
        context, duckdb::EntryLookupInfo{duckdb::CatalogType::TYPE_ENTRY, name},
        duckdb::OnEntryNotFound::RETURN_NULL);
      entry && duckdb::StructType::IsStruct(
                 entry->Cast<duckdb::TypeCatalogEntry>().user_type)) {
    return entry->oid;
  }
  if (auto database = SessionCatalog(context)) {
    const auto key_index = [&](const duckdb::Identifier& schema_name) {
      auto schema = database->GetSchema(context, schema_name,
                                        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!schema) {
        return kInvalidOid;
      }
      const auto key =
        FindKeyIndex(context, *schema, name.Name().GetIdentifierName());
      return key.key_index ? key.key_index->index_oid : kInvalidOid;
    };
    if (!name.Schema().empty()) {
      return key_index(name.Schema());
    }
    for (const auto& path :
         duckdb::ClientData::Get(context).catalog_search_path->Get()) {
      if (const auto oid = key_index(path.GetSchema()); oid != kInvalidOid) {
        return oid;
      }
    }
  }
  return kInvalidOid;
}

namespace {

constexpr size_t kMaxIdentifierLength = 63;
constexpr size_t kMaxFunctionArgs = 100;
constexpr int32_t kMaxAttrSize = 10 * 1024 * 1024;
constexpr int32_t kMaxNumericPrecision = 1000;
constexpr int32_t kMinNumericScale = -1000;
constexpr int32_t kMaxNumericScale = 1000;
constexpr int32_t kMaxTimePrecision = 6;

using NameList = std::vector<std::string>;

std::nullopt_t Miss(bool missing_ok, irs::pg::SqlErrorData error) {
  if (!missing_ok) {
    THROW_SQL_ERROR_FROM_DATA(std::move(error));
  }
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

std::optional<uint64_t> SchemaOid(const Session& session,
                                  std::string_view schema, bool missing_ok) {
  if (const auto* system = FindSystemNamespace(schema)) {
    return system->oid;
  }
  if (auto entry = FindSchema(session, schema)) {
    return entry->oid;
  }
  return Miss(missing_ok, SQL_ERROR_DATA(
                            ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                            ERR_MSG("schema \"", schema, "\" does not exist")));
}

bool RelationIn(const Session& session, const SessionSchema& schema,
                std::string_view name) {
  if (FindSystemNamespace(schema.name)) {
    return IsSystemRelation(schema.name, name);
  }
  auto entry = schema.entry;
  if (!entry) {
    return false;
  }
  const duckdb::Identifier identifier{name};
  return absl::c_any_of(kRelationSets, [&](duckdb::CatalogType type) {
    const auto member =
      FindMember(*session.transaction, *entry, type, identifier);
    return member && !member->internal;
  });
}

bool IsWordStart(char c) {
  return absl::ascii_isalpha(c) || c == '_' ||
         static_cast<unsigned char>(c) >= 0x80;
}

void TruncateIdentifier(std::string& name) {
  if (name.size() <= kMaxIdentifierLength) {
    return;
  }
  auto length = kMaxIdentifierLength;
  while (length > 0 &&
         (static_cast<unsigned char>(name[length]) & 0xC0) == 0x80) {
    --length;
  }
  name.resize(length);
}

std::string DowncaseIdentifier(std::string_view ident) {
  auto name = absl::AsciiStrToLower(ident);
  TruncateIdentifier(name);
  return name;
}

std::optional<NameList> SplitNames(std::string_view text) {
  NameList names;
  size_t pos = 0;
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
      if (!duckdb::StringUtil::TryParseQuotedString(text, pos, name) ||
          name.empty()) {
        return std::nullopt;
      }
      TruncateIdentifier(name);
    } else {
      const auto start = pos;
      while (pos < text.size() && text[pos] != '.' &&
             !absl::ascii_isspace(text[pos])) {
        ++pos;
      }
      if (start == pos) {
        return std::nullopt;
      }
      name = DowncaseIdentifier(text.substr(start, pos - start));
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

std::optional<NameList> ParseNames(std::string_view text, bool missing_ok) {
  auto names = SplitNames(text);
  if (!names || names->empty()) {
    return Miss(missing_ok, SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_NAME),
                                           ERR_MSG("invalid name syntax")));
  }
  return names;
}

std::optional<std::string> ParseSingleName(std::string_view text,
                                           bool missing_ok) {
  auto names = SplitNames(text);
  if (!names || names->size() != 1) {
    return Miss(missing_ok, SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_NAME),
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
  size_t pos = 0;
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
      TruncateIdentifier(name);
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
              .text = DowncaseIdentifier(text.substr(start, pos - start))});
    } else if (absl::ascii_isdigit(c) || (c == '-' && pos + 1 < text.size() &&
                                          absl::ascii_isdigit(text[pos + 1]))) {
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
  std::vector<int32_t> typmods;
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

  std::string ExpectName() {
    const auto& token = Peek(0);
    if (token.kind != TokenKind::Word && token.kind != TokenKind::Quoted) {
      ThrowSyntaxError(token);
    }
    ++_pos;
    return token.text;
  }

  static ParsedType Keyword(std::string_view name) {
    return {.names = {std::string{name}}, .keyword = true};
  }

  ParsedType Simple() {
    if (Peek(0).kind == TokenKind::Word && !IsSymbol(1, '.')) {
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
      return WithTypmods(Keyword("numeric"));
    }
    if (word == "varchar") {
      ++_pos;
      return WithTypmods(Keyword("varchar"));
    }
    if (word == "bit") {
      ++_pos;
      return Varying("bit", "varbit");
    }
    if (word == "character" || word == "char" || word == "nchar") {
      ++_pos;
      return Varying("bpchar", "varchar");
    }
    if (word == "national" && (IsWord(1, "character") || IsWord(1, "char"))) {
      _pos += 2;
      return Varying("bpchar", "varchar");
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

  ParsedType WithTypmods(ParsedType type) {
    Typmods(type);
    return type;
  }

  ParsedType Varying(std::string_view fixed, std::string_view varying) {
    const bool is_varying = AcceptWord("varying");
    auto type = WithTypmods(Keyword(is_varying ? varying : fixed));
    if (type.typmods.empty() && !is_varying) {
      type.typmods.emplace_back(1);
    }
    return type;
  }

  ParsedType Datetime(bool timestamp) {
    std::vector<int32_t> typmods;
    if (AcceptSymbol('(')) {
      typmods.emplace_back(ExpectNumber());
      ExpectSymbol(')');
    }
    const bool zone = AcceptWord("with");
    if (zone || AcceptWord("without")) {
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

  static bool IntervalRangeAllowed(size_t first, size_t last) {
    if (kIntervalFields[first].mask == kIntervalYear) {
      return kIntervalFields[last].mask == kIntervalMonth;
    }
    return kIntervalFields[first].mask != kIntervalMonth && last > first;
  }

  ParsedType Interval() {
    auto type = Keyword("interval");
    if (AcceptSymbol('(')) {
      const auto precision = ExpectNumber();
      ExpectSymbol(')');
      type.typmods = {kIntervalFullRange, precision};
      return type;
    }
    const auto first = IntervalField();
    if (!first) {
      return type;
    }
    ++_pos;
    auto last = *first;
    if (AcceptWord("to")) {
      const auto to = IntervalField();
      if (!to || !IntervalRangeAllowed(*first, *to)) {
        ThrowSyntaxError(Peek(0));
      }
      ++_pos;
      last = *to;
    }
    int32_t mask = 0;
    for (auto i = *first; i <= last; ++i) {
      mask |= kIntervalFields[i].mask;
    }
    type.typmods.emplace_back(mask);
    if (kIntervalFields[last].mask == kIntervalSecond && AcceptSymbol('(')) {
      type.typmods.emplace_back(ExpectNumber());
      ExpectSymbol(')');
    }
    return type;
  }

  void Typmods(ParsedType& type) {
    if (!AcceptSymbol('(')) {
      return;
    }
    do {
      type.typmods.emplace_back(ExpectNumber());
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
        ++_pos;
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

int32_t TypmodIn(uint64_t oid, std::span<const int32_t> typmods,
                 std::string_view display) {
  if (typmods.empty()) {
    return -1;
  }
  const auto is = [&](PgTypeOID type) {
    return oid == static_cast<uint64_t>(type);
  };
  if (is(kBpchar)) {
    return LengthTypmod(typmods, "char", kMaxAttrSize, kVarHdrSz);
  }
  if (is(kVarchar)) {
    return LengthTypmod(typmods, "varchar", kMaxAttrSize, kVarHdrSz);
  }
  if (is(kBit)) {
    return LengthTypmod(typmods, "bit", kMaxAttrSize * 8, 0);
  }
  if (is(kVarbit)) {
    return LengthTypmod(typmods, "varbit", kMaxAttrSize * 8, 0);
  }
  if (is(kNumeric)) {
    return NumericTypmodIn(typmods);
  }
  if (is(kTime) || is(kTimetz)) {
    return TimeTypmod(typmods, "TIME", is(kTimetz));
  }
  if (is(kTimestamp) || is(kTimestamptz)) {
    return TimeTypmod(typmods, "TIMESTAMP", is(kTimestamptz));
  }
  if (is(kInterval)) {
    return IntervalTypmod(typmods);
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_SYNTAX_ERROR),
    ERR_MSG("type modifier is not allowed for type \"", display, "\""));
}

uint64_t FindTypeIn(const Session& session, std::string_view schema,
                    std::string_view name) {
  if (const auto* system = FindSystemNamespace(schema)) {
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
                                        bool missing_ok) {
  const auto invalid = [&] {
    return Miss(missing_ok,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                               ERR_MSG("invalid type name \"", text, "\"")));
  };
  if (absl::c_all_of(text, absl::ascii_isspace)) {
    return invalid();
  }
  const auto parsed = TypeParser{text}.Parse();
  if (!parsed) {
    return invalid();
  }
  const auto display =
    absl::StrCat(absl::StrJoin(parsed->names, "."), parsed->array ? "[]" : "");
  const auto not_found = [&] {
    return Miss(missing_ok, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                                           ERR_MSG("type \"", display,
                                                   "\" does not exist")));
  };
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
    } else if (!SchemaOid(session, object.schema, missing_ok)) {
      return std::nullopt;
    } else {
      element = FindTypeIn(session, object.schema, object.name);
    }
  }
  if (element == kInvalidOid) {
    return not_found();
  }
  const auto typmod = TypmodIn(element, parsed->typmods, display);
  if (!parsed->array) {
    return ResolvedType{.oid = element, .typmod = typmod};
  }
  const auto array = ArrayTypeOf(session, element);
  if (array == kInvalidOid) {
    return not_found();
  }
  return ResolvedType{.oid = array, .typmod = typmod};
}

std::string_view SqlTypeName(int32_t oid) {
  if (oid == kBool) {
    return "boolean";
  }
  if (oid == kChar) {
    return "\"char\"";
  }
  if (oid == kInt8) {
    return "bigint";
  }
  if (oid == kInt2) {
    return "smallint";
  }
  if (oid == kInt4) {
    return "integer";
  }
  if (oid == kFloat4) {
    return "real";
  }
  if (oid == kFloat8) {
    return "double precision";
  }
  if (oid == kBpchar) {
    return "character";
  }
  if (oid == kVarchar) {
    return "character varying";
  }
  if (oid == kBit) {
    return "bit";
  }
  if (oid == kVarbit) {
    return "bit varying";
  }
  if (oid == kNumeric) {
    return "numeric";
  }
  if (oid == kInterval) {
    return "interval";
  }
  if (oid == kTime) {
    return "time without time zone";
  }
  if (oid == kTimetz) {
    return "time with time zone";
  }
  if (oid == kTimestamp) {
    return "timestamp without time zone";
  }
  if (oid == kTimestamptz) {
    return "timestamp with time zone";
  }
  return {};
}

std::optional<std::string> FormatUserType(const Session& session, uint64_t oid,
                                          std::optional<int32_t> typmod) {
  if (!session.context) {
    return std::nullopt;
  }
  const auto array = ArrayElementOid(session, oid);
  const auto element = array != kInvalidOid ? array : oid;
  const auto object = TypeObject(session, element);
  if (!object) {
    return std::nullopt;
  }
  auto name = FindVisibleType(session, object->name) == element
                ? QuoteIdentifier(object->name)
                : QualifiedOutName(object->schema, object->name);
  if (typmod && *typmod >= 0) {
    absl::StrAppend(&name, "(", *typmod, ")");
  }
  if (array != kInvalidOid) {
    absl::StrAppend(&name, "[]");
  }
  return name;
}

std::string FormatType(const Session& session, uint64_t oid) {
  if (oid == kInvalidOid) {
    return "-";
  }
  if (oid < kMaxSystem) {
    const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid));
    if (!builtin) {
      return absl::StrCat(oid);
    }
    if (builtin->IsArray()) {
      return absl::StrCat(
        FormatType(session, static_cast<uint64_t>(builtin->elem)), "[]");
    }
    if (const auto name = SqlTypeName(builtin->oid); !name.empty()) {
      return std::string{name};
    }
    if (builtin->nsp == kPgCatalogSchema ||
        FindVisibleType(session, builtin->name) == oid) {
      return std::string{builtin->name};
    }
    return QualifiedOutName(FindSystemNamespace(builtin->nsp)->name,
                            builtin->name);
  }
  if (auto name = FormatUserType(session, oid, std::nullopt)) {
    return std::move(*name);
  }
  return absl::StrCat(oid);
}

std::optional<ObjectName> ClassObject(const Session& session, uint64_t oid) {
  if (auto object = RelationObject(session, oid)) {
    return object;
  }
  auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != duckdb::CatalogType::TYPE_ENTRY ||
      !duckdb::StructType::IsStruct(
        entry->Cast<duckdb::TypeCatalogEntry>().user_type)) {
    return std::nullopt;
  }
  return NameOf(*entry);
}

std::string ClassOut(const Session& session, uint64_t oid) {
  if (!session.context) {
    return absl::StrCat(oid);
  }
  const auto object = ClassObject(session, oid);
  if (!object) {
    return absl::StrCat(oid);
  }
  return RelationName(session, object->schema, object->name);
}

std::optional<uint64_t> ClassIn(const Session& session, std::string_view text,
                                bool missing_ok) {
  const auto names = ParseNames(text, missing_ok);
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
  if (const auto oid = ResolveRelation(*session.context, qualified);
      oid != kInvalidOid) {
    return oid;
  }
  return Miss(missing_ok,
              SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                             ERR_MSG("relation \"", absl::StrJoin(*names, "."),
                                     "\" does not exist")));
}

struct ProcCandidate {
  uint64_t oid;
  uint64_t nsp;
  std::vector<duckdb::idx_t> args;
};

struct ProcInfo {
  uint64_t nsp;
  std::string schema;
  std::string name;
  std::vector<duckdb::idx_t> args;
};

std::vector<duckdb::idx_t> MacroArgs(const duckdb::MacroFunction& macro) {
  return macro.types |
         std::views::transform([](const duckdb::LogicalType& type) {
           return type.id() == duckdb::LogicalTypeId::UNKNOWN
                    ? duckdb::idx_t{0}
                    : duckdb::idx_t{Type2Oid(type)};
         }) |
         std::ranges::to<std::vector>();
}

void CollectProcs(const Session& session, const BuiltinFunctions& builtins,
                  std::string_view schema, std::string_view name,
                  std::vector<ProcCandidate>& candidates) {
  const auto add = [&](uint64_t oid, uint64_t nsp,
                       std::vector<duckdb::idx_t> args) {
    if (absl::c_none_of(candidates, [&](const ProcCandidate& candidate) {
          return candidate.args == args;
        })) {
      candidates.emplace_back(
        ProcCandidate{.oid = oid, .nsp = nsp, .args = std::move(args)});
    }
  };
  if (const auto* system = FindSystemNamespace(schema)) {
    for (const auto index : builtins.Named(name)) {
      const auto& function = builtins.All()[index];
      if (function.nsp == system->oid) {
        add(function.oid, system->oid, function.argtypes);
      }
    }
    return;
  }
  for (const auto type : {duckdb::CatalogType::MACRO_ENTRY,
                          duckdb::CatalogType::TABLE_MACRO_ENTRY}) {
    if (auto entry = FindInSchema(session, schema, type, name)) {
      for (const auto& macro :
           entry->Cast<duckdb::MacroCatalogEntry>().macros) {
        add(entry->oid, entry->ParentSchemaOid(), MacroArgs(*macro));
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
                                     const NameList& names) {
  const auto object = Deconstruct(session, names);
  const auto builtins = GetBuiltinFunctions(*session.context);
  if (object.schema.empty()) {
    return VisibleProcs(session, *builtins, object.name);
  }
  std::vector<ProcCandidate> candidates;
  CollectProcs(session, *builtins, object.schema, object.name, candidates);
  return candidates;
}

std::optional<ProcInfo> FindProc(const Session& session,
                                 const BuiltinFunctions& builtins,
                                 uint64_t oid) {
  if (oid < kMaxSystem) {
    if (const auto* function = builtins.Find(oid)) {
      return ProcInfo{
        .nsp = function->nsp,
        .schema = std::string{FindSystemNamespace(function->nsp)->name},
        .name = function->name,
        .args = function->argtypes};
    }
    return std::nullopt;
  }
  auto entry = EntryByOid(session, oid);
  if (!entry || CatalogClassOid(entry->type) != kPgProcTable) {
    return std::nullopt;
  }
  const auto& macros = entry->Cast<duckdb::MacroCatalogEntry>().macros;
  if (macros.empty()) {
    return std::nullopt;
  }
  auto object = NameOf(*entry);
  return ProcInfo{.nsp = entry->ParentSchemaOid(),
                  .schema = std::move(object.schema),
                  .name = std::move(object.name),
                  .args = MacroArgs(*macros.front())};
}

bool ProcVisible(const Session& session, const BuiltinFunctions& builtins,
                 const ProcInfo& info, bool signature) {
  const auto candidates = VisibleProcs(session, builtins, info.name);
  const auto same_namespace = [&](const ProcCandidate& candidate) {
    return candidate.nsp == info.nsp;
  };
  return signature
           ? absl::c_any_of(candidates,
                            [&](const ProcCandidate& candidate) {
                              return candidate.args == info.args &&
                                     same_namespace(candidate);
                            })
           : candidates.size() == 1 && same_namespace(candidates.front());
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
  return absl::StrCat(name, "(",
                      absl::StrJoin(info->args, ",",
                                    [&](std::string* out, duckdb::idx_t arg) {
                                      out->append(FormatType(session, arg));
                                    }),
                      ")");
}

std::optional<uint64_t> ProcIn(const Session& session, std::string_view text,
                               bool missing_ok) {
  const auto names = ParseNames(text, missing_ok);
  if (!names) {
    return std::nullopt;
  }
  const auto candidates = FindProcs(session, *names);
  if (candidates.empty()) {
    return Miss(missing_ok, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                                           ERR_MSG("function \"", text,
                                                   "\" does not exist")));
  }
  if (candidates.size() > 1) {
    return Miss(
      missing_ok,
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
                                        bool missing_ok) {
  const auto invalid = [&](std::string_view message) {
    return Miss(missing_ok,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                               ERR_MSG(message)));
  };
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
    return invalid("expected a left parenthesis");
  }
  auto names = ParseNames(text.substr(0, open), missing_ok);
  if (!names) {
    return std::nullopt;
  }
  auto rest = absl::StripTrailingAsciiWhitespace(text.substr(open + 1));
  if (!absl::ConsumeSuffix(&rest, ")")) {
    return invalid("expected a right parenthesis");
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
        return invalid("expected a type name");
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
      return invalid("improper type name");
    }
    const auto type_name =
      absl::StripTrailingAsciiWhitespace(rest.substr(start, pos - start));
    had_comma = pos < rest.size();
    if (had_comma) {
      ++pos;
    }
    if (args.size() >= kMaxFunctionArgs) {
      return invalid("too many arguments");
    }
    if (allow_none && absl::EqualsIgnoreCase(type_name, "none")) {
      args.emplace_back(0);
      continue;
    }
    const auto type = ResolveType(session, type_name, missing_ok);
    if (!type) {
      return std::nullopt;
    }
    args.emplace_back(type->oid);
  }
  return Signature{.names = std::move(*names), .args = std::move(args)};
}

std::optional<uint64_t> ProcedureIn(const Session& session,
                                    std::string_view text, bool missing_ok) {
  const auto signature = ParseSignature(session, text, false, missing_ok);
  if (!signature) {
    return std::nullopt;
  }
  for (const auto& candidate : FindProcs(session, signature->names)) {
    if (candidate.args == signature->args) {
      return candidate.oid;
    }
  }
  return Miss(missing_ok, SQL_ERROR_DATA(
                            ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                            ERR_MSG("function \"", text, "\" does not exist")));
}

std::optional<uint64_t> OperIn(const Session& session, std::string_view text,
                               bool missing_ok) {
  const auto names = ParseNames(text, missing_ok);
  if (!names) {
    return std::nullopt;
  }
  Deconstruct(session, *names);
  return Miss(missing_ok, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                                         ERR_MSG("operator does not exist: ",
                                                 absl::StrJoin(*names, "."))));
}

std::optional<uint64_t> OperatorIn(const Session& session,
                                   std::string_view text, bool missing_ok) {
  const auto signature = ParseSignature(session, text, true, missing_ok);
  if (!signature) {
    return std::nullopt;
  }
  if (signature->args.size() == 1) {
    return Miss(
      missing_ok,
      SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_UNDEFINED_PARAMETER), ERR_MSG("missing argument"),
        ERR_HINT(
          "Use NONE to denote the missing argument of a unary operator.")));
  }
  if (signature->args.size() != 2) {
    return Miss(missing_ok,
                SQL_ERROR_DATA(ERR_CODE(ERRCODE_TOO_MANY_ARGUMENTS),
                               ERR_MSG("too many arguments"),
                               ERR_HINT("Provide two argument types for "
                                        "operator.")));
  }
  Deconstruct(session, signature->names);
  return Miss(missing_ok,
              SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                             ERR_MSG("operator does not exist: ", text)));
}

std::optional<uint64_t> CollationIn(const Session& session,
                                    std::string_view text, bool missing_ok) {
  const auto names = ParseNames(text, missing_ok);
  if (!names) {
    return std::nullopt;
  }
  const auto object = Deconstruct(session, *names);
  if (!object.schema.empty() &&
      !SchemaOid(session, object.schema, missing_ok)) {
    return std::nullopt;
  }
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
    missing_ok,
    SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                   ERR_MSG("collation \"", absl::StrJoin(*names, "."),
                           "\" for encoding \"UTF8\" does not exist")));
}

std::string CollationOut(uint64_t oid) {
  if (const auto* collation = FindBuiltinCollation(static_cast<int64_t>(oid))) {
    return QuoteIdentifier(collation->name);
  }
  return absl::StrCat(oid);
}

std::optional<uint64_t> ConfigIn(const Session& session, std::string_view text,
                                 bool missing_ok) {
  const auto names = ParseNames(text, missing_ok);
  if (!names) {
    return std::nullopt;
  }
  Deconstruct(session, *names);
  return Miss(missing_ok, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
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
                                     std::string_view text, bool missing_ok) {
  const auto names = ParseNames(text, missing_ok);
  if (!names) {
    return std::nullopt;
  }
  const auto object = Deconstruct(session, *names);
  if (object.schema.empty()) {
    if (const auto oid = FindVisibleDictionary(session, object.name);
        oid != kInvalidOid) {
      return oid;
    }
  } else if (!SchemaOid(session, object.schema, missing_ok)) {
    return std::nullopt;
  } else if (auto entry = FindInSchema(session, object.schema,
                                       duckdb::CatalogType::TOKENIZER_ENTRY,
                                       object.name)) {
    return entry->oid;
  }
  return Miss(missing_ok, SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                                         ERR_MSG("text search dictionary \"",
                                                 absl::StrJoin(*names, "."),
                                                 "\" does not exist")));
}

std::string DictionaryOut(const Session& session, uint64_t oid) {
  auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != duckdb::CatalogType::TOKENIZER_ENTRY) {
    return absl::StrCat(oid);
  }
  const auto object = NameOf(*entry);
  if (FindVisibleDictionary(session, object.name) == oid) {
    return QuoteIdentifier(object.name);
  }
  return QualifiedOutName(object.schema, object.name);
}

std::optional<uint64_t> RoleIn(const Session& session, std::string_view text,
                               bool missing_ok) {
  const auto name = ParseSingleName(text, missing_ok);
  if (!name) {
    return std::nullopt;
  }
  const auto graph = auth::RolesOf(session.context);
  if (const auto* role = graph->FindByName(*name)) {
    return role->first;
  }
  return Miss(missing_ok,
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
                                    std::string_view text, bool missing_ok) {
  const auto name = ParseSingleName(text, missing_ok);
  if (!name) {
    return std::nullopt;
  }
  return SchemaOid(session, *name, missing_ok);
}

std::string NamespaceOut(const Session& session, uint64_t oid) {
  if (const auto* system = FindSystemNamespace(oid)) {
    return std::string{system->name};
  }
  if (session.database) {
    if (auto schema =
          session.database->FindSchemaById(*session.transaction, oid)) {
      return QuoteIdentifier(schema->name.GetIdentifierName());
    }
  }
  return absl::StrCat(oid);
}

std::optional<uint64_t> ParseOid(std::string_view text) {
  if (text.empty() || !absl::c_all_of(text, absl::ascii_isdigit)) {
    return std::nullopt;
  }
  uint64_t oid = 0;
  if (!absl::SimpleAtoi(text, &oid) ||
      oid > std::numeric_limits<uint32_t>::max()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
      ERR_MSG("value \"", text, "\" is out of range for type oid"));
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

std::string FormatTypeOut(const Session& session, uint64_t oid,
                          std::optional<int32_t> typmod) {
  if (oid == kInvalidOid) {
    return FormatType(session, oid);
  }
  if (oid >= kMaxSystem) {
    return FormatUserType(session, oid, typmod).value_or("???");
  }
  const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid));
  if (!builtin) {
    return "???";
  }
  if (builtin->IsArray()) {
    return absl::StrCat(
      FormatTypeOut(session, static_cast<uint64_t>(builtin->elem), typmod),
      "[]");
  }
  if (typmod && *typmod < 0) {
    if (builtin->oid == kBit) {
      return "\"bit\"";
    }
    if (builtin->oid == kBpchar) {
      return "bpchar";
    }
  }
  if (typmod && *typmod >= 0) {
    const auto sql = SqlTypeName(builtin->oid);
    switch (builtin->oid) {
      case kBit:
      case kVarbit:
        return absl::StrCat(sql, "(", *typmod, ")");
      case kBpchar:
      case kVarchar:
        return absl::StrCat(sql, "(", *typmod - kVarHdrSz, ")");
      case kNumeric:
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
  auto name = FormatType(session, oid);
  if (typmod && *typmod >= 0 &&
      !absl::c_linear_search(kTypmodlessTypes, builtin->oid)) {
    absl::StrAppend(&name, "(", *typmod, ")");
  }
  return name;
}

template<RegKind Kind>
std::string_view RegOut(const Session& session, uint64_t oid) {
  using enum RegKind;
  if (oid == kInvalidOid) {
    return "-";
  }
  const auto kind = std::to_underlying(Kind);
  auto& recent = session.reg_out_recent[((oid ^ (uint64_t{kind} << 56)) *
                                         uint64_t{0x9E3779B97F4A7C15}) >>
                                        58];
  if (recent.oid == oid && recent.kind == kind) {
    return recent.text;
  }
  const std::pair key{kind, oid};
  if (const auto it = session.reg_out.find(key); it != session.reg_out.end()) {
    recent = {.oid = oid, .kind = kind, .text = it->second};
    return it->second;
  }
  std::string text;
  if constexpr (Kind == Proc || Kind == Procedure) {
    text = ProcOut(session, oid, Kind == Procedure);
  } else if constexpr (Kind == Oper || Kind == Operator || Kind == Config) {
    text = absl::StrCat(oid);
  } else if constexpr (Kind == Class) {
    text = ClassOut(session, oid);
  } else if constexpr (Kind == Type) {
    text = FormatType(session, oid);
  } else if constexpr (Kind == Collation) {
    text = CollationOut(oid);
  } else if constexpr (Kind == Dictionary) {
    text = DictionaryOut(session, oid);
  } else if constexpr (Kind == Role) {
    text = RoleOut(session, oid);
  } else {
    static_assert(Kind == Namespace);
    text = NamespaceOut(session, oid);
  }
  const std::string_view stored =
    session.reg_out.emplace(key, std::move(text)).first->second;
  recent = {.oid = oid, .kind = kind, .text = stored};
  return stored;
}

template<RegKind Kind>
std::optional<uint64_t> RegIn(const Session& session, std::string_view text,
                              bool missing_ok) {
  using enum RegKind;
  if (text == "-") {
    return kInvalidOid;
  }
  if (const auto oid = ParseOid(text)) {
    return oid;
  }
  if constexpr (Kind == Proc) {
    return ProcIn(session, text, missing_ok);
  } else if constexpr (Kind == Procedure) {
    return ProcedureIn(session, text, missing_ok);
  } else if constexpr (Kind == Oper) {
    return OperIn(session, text, missing_ok);
  } else if constexpr (Kind == Operator) {
    return OperatorIn(session, text, missing_ok);
  } else if constexpr (Kind == Class) {
    return ClassIn(session, text, missing_ok);
  } else if constexpr (Kind == Type) {
    const auto type = ResolveType(session, text, missing_ok);
    if (!type) {
      return std::nullopt;
    }
    return type->oid;
  } else if constexpr (Kind == Collation) {
    return CollationIn(session, text, missing_ok);
  } else if constexpr (Kind == Config) {
    return ConfigIn(session, text, missing_ok);
  } else if constexpr (Kind == Dictionary) {
    return DictionaryIn(session, text, missing_ok);
  } else if constexpr (Kind == Role) {
    return RoleIn(session, text, missing_ok);
  } else {
    static_assert(Kind == Namespace);
    return NamespaceIn(session, text, missing_ok);
  }
}

template std::string_view RegOut<RegKind::Proc>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Procedure>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Oper>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Operator>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Class>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Type>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Collation>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Config>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Dictionary>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Role>(const Session&, uint64_t);
template std::string_view RegOut<RegKind::Namespace>(const Session&, uint64_t);

template std::optional<uint64_t> RegIn<RegKind::Proc>(const Session&,
                                                      std::string_view, bool);
template std::optional<uint64_t> RegIn<RegKind::Procedure>(const Session&,
                                                           std::string_view,
                                                           bool);
template std::optional<uint64_t> RegIn<RegKind::Oper>(const Session&,
                                                      std::string_view, bool);
template std::optional<uint64_t> RegIn<RegKind::Operator>(const Session&,
                                                          std::string_view,
                                                          bool);
template std::optional<uint64_t> RegIn<RegKind::Class>(const Session&,
                                                       std::string_view, bool);
template std::optional<uint64_t> RegIn<RegKind::Type>(const Session&,
                                                      std::string_view, bool);
template std::optional<uint64_t> RegIn<RegKind::Collation>(const Session&,
                                                           std::string_view,
                                                           bool);
template std::optional<uint64_t> RegIn<RegKind::Config>(const Session&,
                                                        std::string_view, bool);
template std::optional<uint64_t> RegIn<RegKind::Dictionary>(const Session&,
                                                            std::string_view,
                                                            bool);
template std::optional<uint64_t> RegIn<RegKind::Role>(const Session&,
                                                      std::string_view, bool);
template std::optional<uint64_t> RegIn<RegKind::Namespace>(const Session&,
                                                           std::string_view,
                                                           bool);

std::optional<int32_t> RegTypmodIn(const Session& session,
                                   std::string_view text) {
  const auto type = ResolveType(session, text, true);
  if (!type) {
    return std::nullopt;
  }
  return type->typmod;
}

bool RelationVisible(const Session& session, std::string_view schema,
                     std::string_view name) {
  const auto at = absl::c_find_if(
    session.search_path,
    [&](const SessionSchema& path) { return path.name == schema; });
  return at != session.search_path.end() &&
         std::none_of(session.search_path.begin(), at,
                      [&](const SessionSchema& path) {
                        return RelationIn(session, path, name);
                      });
}

std::string RelationName(const Session& session, std::string_view schema,
                         std::string_view name) {
  return RelationVisible(session, schema, name)
           ? QuoteIdentifier(name)
           : QualifiedOutName(schema, name);
}

std::optional<bool> RelationIsVisible(const Session& session, uint64_t oid) {
  const auto object = ClassObject(session, oid);
  if (!object) {
    return std::nullopt;
  }
  return RelationVisible(session, object->schema, object->name);
}

std::optional<bool> TypeIsVisible(const Session& session, uint64_t oid) {
  const auto object = TypeObject(session, oid);
  if (!object) {
    return std::nullopt;
  }
  return FindVisibleType(session, object->name) == oid;
}

std::optional<bool> FunctionIsVisible(const Session& session, uint64_t oid) {
  const auto builtins = GetBuiltinFunctions(*session.context);
  const auto info = FindProc(session, *builtins, oid);
  if (!info) {
    return std::nullopt;
  }
  return ProcVisible(session, *builtins, *info, true);
}

}  // namespace sdb::pg
