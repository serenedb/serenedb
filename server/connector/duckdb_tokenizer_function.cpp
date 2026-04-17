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

#include "connector/duckdb_tokenizer_function.h"

#include <unicode/locid.h>
#include <vpack/builder.h>
#include <vpack/value.h>
#include <vpack/value_type.h>

#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/minhash_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <type_traits>
#include <utility>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/option_help.h"
#include "pg/options_parser.h"
#include "pg/sql_utils.h"
#include "pg/tokenizer_options.h"

namespace sdb::pg {
namespace {

inline constexpr std::string_view kAnalyzerField = "analyzer";
inline constexpr std::string_view kPropertiesField = "properties";
inline constexpr std::string_view kTypeField = "type";

using namespace std::string_view_literals;

// TODO: Remove this mapping
const containers::FlatHashMap<std::string_view, std::string_view>
  kNameMappings = {
    {tokenizer_options::kStopwordsPath.name, "stopwordsPath"},
    {tokenizer_options::kMinGram.name, "min"},
    {tokenizer_options::kMaxGram.name, "max"},
    {tokenizer_options::kEdgeNGramGroup.name, "edgeNGram"},
    {tokenizer_options::kPreserveOriginal.name, "preserveOriginal"},
    {tokenizer_options::kInputType.name, "streamType"},
    {tokenizer_options::kStartMarker.name, "startMarker"},
    {tokenizer_options::kEndMarker.name, "endMarker"},
    {tokenizer_options::kModelLocation.name, "model_location"},
    {tokenizer_options::kTopK.name, "top_k"},
    {tokenizer_options::kNumHashes.name, "numHashes"},
};

template<const auto& Array>
void VisitValues(auto&& callback) {
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    (callback.template operator()<Array[Is]>(), ...);
  }(std::make_index_sequence<std::size(Array)>{});
}

void ParseCommaSeparated(std::string_view input,
                         std::invocable<std::string_view> auto&& callback) {
  while (!input.empty()) {
    auto pos = input.find(',');
    auto token = input.substr(0, pos);
    while (!token.empty() &&
           std::isspace(static_cast<unsigned char>(token.front()))) {
      token.remove_prefix(1);
    }
    while (!token.empty() &&
           std::isspace(static_cast<unsigned char>(token.back()))) {
      token.remove_suffix(1);
    }
    if (token.front() != '\"' || token.back() != '\"') {
      throw duckdb::InvalidInputException(
        "Invalid format of list of words(should be comma-separated and "
        "quoted)");
    }
    token.remove_suffix(1);
    token.remove_prefix(1);
    if (!token.empty()) {
      callback(token);
    }
    input = pos == std::string_view::npos ? "" : input.substr(pos + 1);
  }
}

std::string_view GetVPackName(std::string_view pg_name) {
  auto it = kNameMappings.find(pg_name);
  return it != kNameMappings.end() ? it->second : pg_name;
}

constexpr OptionInfo kTSDictionaryRootOptions[] = {
  tokenizer_options::kTemplate};
constexpr OptionGroup kTSDictionaryGroup = {
  "Text Search Dictionary", kTSDictionaryRootOptions,
  tokenizer_options::kTokenizerSubgroups};

class CreateTSDictionaryOptions : public OptionsParser {
 public:
  CreateTSDictionaryOptions(std::shared_ptr<const catalog::Snapshot> snapshot,
                            ObjectId db_id, std::string_view current_schema,
                            const duckdb::named_parameter_map_t& named_params)
    : OptionsParser{named_params,
                    kTSDictionaryGroup,
                    {.operation = "CREATE TEXT SEARCH DICTIONARY"}},
      _snapshot{std::move(snapshot)},
      _db_id{db_id},
      _current_schema{current_schema} {
    ParseOptions([&] {
      _builder.openObject();
      _builder.add(kAnalyzerField, vpack::Value{vpack::ValueType::Object});
      std::string type =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>();
      Parse<true>(type);
      _builder.close();  // close analyzer
      _builder.close();  // close object
    });
  }

  auto Result() && { return std::make_pair(std::move(_builder), _features); }

 private:
  void ParseTemplateType(std::string_view type, std::string_view prefix) {
    bool found = false;
    VisitValues<kTSDictionaryGroup.subgroups>([&]<const OptionGroup & Group> {
      if (Group.name == type) {
        WriteTokenizerOptions<Group>(prefix);
        found = true;
        return;
      }
    });
    SDB_ASSERT(found);
  }

  static vpack::Slice GetFromPath(std::string_view name,
                                  std::string_view full_prefix,
                                  std::string_view name_prefix,
                                  vpack::Slice slice) {
    SDB_ASSERT(name_prefix == full_prefix.substr(0, name_prefix.size()));
    auto prefix = full_prefix.substr(name_prefix.size());
    while (!slice.isNone() && !prefix.empty()) {
      auto pos = prefix.find('_');
      auto next = prefix.substr(0, pos);
      SDB_ASSERT(!next.empty());
      slice = slice.get(next);
      prefix = pos == std::string_view::npos ? "" : prefix.substr(pos + 1);
    }
    if (slice.isNone()) {
      return slice;
    }
    return slice.get(name);
  }

  template<typename T>
  std::optional<T> GetFromCopy(std::string_view name, std::string_view prefix) {
    SDB_ASSERT(!_copy_from.empty());
    auto [name_prefix, slice] = _copy_from.back();
    auto field = GetFromPath(GetVPackName(name), prefix, name_prefix, slice);
    if (field.isNone()) {
      return std::nullopt;
    }
    if constexpr (std::is_same_v<T, std::string>) {
      return std::string{field.stringView()};
    } else if constexpr (std::is_same_v<T, bool>) {
      return field.getBool();
    } else if constexpr (std::is_same_v<T, int>) {
      return field.getNumber<int>();
    } else if constexpr (std::is_same_v<T, double>) {
      return field.getNumber<double>();
    } else if constexpr (std::is_same_v<T, char>) {
      auto sv = field.stringView();
      SDB_ASSERT(sv.size() == 1);
      return sv[0];
    } else if constexpr (std::is_enum_v<T>) {
      return magic_enum::enum_cast<T>(field.stringView(),
                                      magic_enum::case_insensitive);
    } else {
      static_assert(false, "Unsupported type T in GetFromCopy");
    }
  }

  template<const OptionInfo& Info>
  auto EraseOptionOrDefault(std::string_view prefix = "") {
    using R = decltype(OptionsParser::EraseOptionOrDefault<Info>(prefix));
    if (_copy_from.empty()) {
      return OptionsParser::EraseOptionOrDefault<Info>(prefix);
    }
    if (!OptionsParser::HasOption(Info.name, prefix)) {
      std::string_view name = Info.name;
      // tokenizer's properties vpack does not contains its type
      // tokenizer: {"analyzer": {"type" : "some", "properties": {...}}}
      SDB_ASSERT(name != tokenizer_options::kTemplate.name);
      auto value = GetFromCopy<R>(name, prefix);
      if (value) {
        return *value;
      }
      return Info.GetDefaultValue<R>();
    } else {
      return OptionsParser::EraseOptionOrDefault<Info>(prefix);
    }
  }

  bool HasOption(const OptionInfo& info, std::string_view prefix) {
    bool has_option = OptionsParser::HasOption(info.name, prefix);
    if (has_option || _copy_from.empty()) {
      return has_option;
    }

    auto [name_prefix, slice] = _copy_from.back();
    auto field = GetFromPath(info.name, prefix, name_prefix, slice);
    return !field.isNone();
  }

  template<bool IsRoot>
  void Parse(std::string_view type, std::string_view prefix = "") {
    if (type == tokenizer_options::kCopyFromGroup.name) {
      ParseCopyFrom(prefix);
    } else {
      _builder.add(kPropertiesField, vpack::Value{vpack::ValueType::Object});

      ParseTemplateType(type, prefix);

      _builder.close();  // close properties
      _builder.add(kTypeField, type);
    }
    if constexpr (IsRoot) {
      ParseFeatures(type);
    }
  }

  template<const OptionGroup& Group>
  void ParseTokenizerGroup(std::string_view prefix) {
    VisitValues<Group.options>([&]<const OptionInfo & Option> {
      if constexpr (Option.name == tokenizer_options::kStopwords.name ||
                    Option.name == tokenizer_options::kDelimiters.name) {
        if (!OptionsParser::HasOption(Option.name, prefix) &&
            !_copy_from.empty()) {
          auto slice = GetFromPath(Option.name, prefix, _copy_from.back().first,
                                   _copy_from.back().second);
          if (!slice.isNone()) {
            _builder.add(GetVPackName(Option.name), slice);
            return;
          }
        }
        _builder.add(GetVPackName(Option.name),
                     vpack::Value{vpack::ValueType::Array});
        auto value = OptionsParser::EraseOptionOrDefault<Option>(prefix);
        ParseCommaSeparated(value,
                            [&](std::string_view word) { _builder.add(word); });
        _builder.close();
      } else {
        auto value = EraseOptionOrDefault<Option>(prefix);
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(value)>,
                                     std::string>) {
          if (value.empty()) {
            return;
          }
        }
        _builder.add(GetVPackName(Option.name), value);
      }
    });
  }

  template<const OptionGroup& Group>
  void WriteTokenizerOptions(std::string_view prefix) {
    if constexpr (Group.name == tokenizer_options::kMinHashGroup.name) {
      ParseMinHash(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kPipelineGroup.name) {
      ParsePipeline(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kCopyFromGroup.name) {
      ParseCopyFrom(prefix);
      return;
    } else {
      if constexpr (Group.name == tokenizer_options::kTextGroup.name) {
        bool has_ngram =
          HasOption(tokenizer_options::kMinGram, prefix) ||
          HasOption(tokenizer_options::kMaxGram, prefix) ||
          HasOption(tokenizer_options::kPreserveOriginal, prefix);
        if (has_ngram) {
          _builder.add(GetVPackName(tokenizer_options::kEdgeNGramGroup.name),
                       vpack::Value{vpack::ValueType::Object});
          WriteTokenizerOptions<Group.subgroups[0]>(prefix);
          _builder.close();
        }
      }
      ParseTokenizerGroup<Group>(prefix);
    }
  }

  void ParsePipeline(std::string_view prefix) {
    int step = 1;
    _builder.add(tokenizer_options::kPipelineGroup.name,
                 vpack::Value{vpack::ValueType::Array});
    auto slice = vpack::Slice::noneSlice();
    if (!_copy_from.empty() && _copy_from.back().first == prefix) {
      slice = GetFromPath(tokenizer_options::kPipelineGroup.name, prefix,
                          _copy_from.back().first, _copy_from.back().second);
      SDB_ASSERT(slice.isArray());
    }
    while (true) {
      auto step_prefix = OptionInfo::AdjustPrefix(prefix, "step", step);
      std::string type;
      bool type_from_copy = false;
      if (OptionsParser::HasOption(tokenizer_options::kTemplate, step_prefix)) {
        type =
          OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
            step_prefix);
      } else if (!slice.isNone()) {
        if (step > slice.length()) {
          break;
        }
        auto elem = slice.at(step - 1);
        if (elem.isNone()) {
          break;
        }
        type_from_copy = true;
        type = std::string{elem.get(kTypeField).stringView()};
        _copy_from.emplace_back(step_prefix, elem.get(kPropertiesField));
      }
      if (type.empty()) {
        break;
      }
      _builder.openObject();
      Parse<false>(type, step_prefix);
      _builder.close();
      if (type_from_copy) {
        _copy_from.pop_back();
      }

      step++;
    }
    _builder.close();  // close array for pipeline
  }

  void ParseMinHash(std::string_view prefix) {
    auto analyzer_prefix = OptionInfo::AdjustPrefix(prefix, kAnalyzerField);
    std::string type;
    bool type_from_template = false;
    if (OptionsParser::HasOption(tokenizer_options::kTemplate,
                                 analyzer_prefix) ||
        _copy_from.empty()) {
      type = OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
        analyzer_prefix);
    } else {
      SDB_ASSERT(!_copy_from.empty());
      auto slice = GetFromPath(kAnalyzerField, prefix, _copy_from.back().first,
                               _copy_from.back().second);
      type = std::string{slice.get(kTypeField).stringView()};
      _copy_from.emplace_back(analyzer_prefix, slice.get(kPropertiesField));
      type_from_template = true;
    }
    SDB_ASSERT(!type.empty());
    _builder.add(kAnalyzerField, vpack::Value{vpack::ValueType::Object});
    Parse<false>(type, analyzer_prefix);
    _builder.close();  // close analyzer
    if (type_from_template) {
      _copy_from.pop_back();
    }
    int hashes = EraseOptionOrDefault<tokenizer_options::kNumHashes>(prefix);
    _builder.add(GetVPackName(tokenizer_options::kNumHashes.name), hashes);
  }

  void ParseCopyFrom(std::string_view prefix) {
    std::string from =
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kFrom>(prefix);
    auto name = ParseObjectName(from, _current_schema);
    auto tokenizer =
      _snapshot->GetTokenizer(_db_id, name.schema, name.relation);
    if (!tokenizer) {
      throw duckdb::InvalidInputException(
        "text search dictionary \"%s\" does not exist", std::string{from});
    }
    auto slice = tokenizer->Slice().get(kAnalyzerField);

    auto type = slice.get(kTypeField);
    _copy_from.emplace_back(prefix, slice.get(kPropertiesField));
    Parse<false>(type.stringView(), prefix);
    _copy_from.pop_back();
  }

  void ParseFeatures(std::string_view type) {
    VisitValues<tokenizer_options::kFeaturesOptions>(
      [&]<const OptionInfo & Feature> {
        bool use_feature = EraseOptionOrDefault<Feature>();
        if (use_feature) {
          bool added = _features.Add(Feature.name);
          SDB_ASSERT(added);
        }
      });
    auto r = _features.Validate(type);
    if (!r.ok()) {
      throw duckdb::InvalidInputException("%s", std::string{r.errorMessage()});
    }
  }

  vpack::Builder _builder;
  search::Features _features;
  std::vector<std::pair<std::string, vpack::Slice>> _copy_from;
  std::shared_ptr<const catalog::Snapshot> _snapshot;
  ObjectId _db_id;
  std::string_view _current_schema;
};

}  // namespace

void CreateTokenizerFromOptions(ConnectionContext& conn_ctx,
                                std::string_view name, std::string_view schema,
                                bool if_not_exists,
                                const duckdb::named_parameter_map_t& options) {
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto db_id = conn_ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();

  auto [b, features] = std::move(CreateTSDictionaryOptions{
                                   snapshot, db_id, current_schema, options})
                         .Result();

  auto tokenizer = std::make_shared<catalog::Tokenizer>(
    ObjectId{0}, std::string{name}, features,
    std::string{reinterpret_cast<const char*>(b.slice().getDataPtr()),
                b.slice().byteSize()});

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto r = catalog.CreateTokenizer(db_id, schema, std::move(tokenizer));

  if (r.is(ERROR_SERVER_DUPLICATE_NAME) && if_not_exists) {
    return;
  }
  if (!r.ok()) {
    throw duckdb::InvalidInputException(
      "text search dictionary \"%s\" already exists", std::string{name});
  }
}

}  // namespace sdb::pg
namespace sdb::connector {
namespace {

// PRAGMA create_text_search_dictionary('name', if_not_exists, key := value,
// ...) Positional parameters:
//   [0] name (VARCHAR)  -- optionally schema-qualified as "schema.name"
//   [1] if_not_exists (BOOLEAN)
// Named parameters: tokenizer options (template, frequency, etc.)
void CreateTSDictionaryPragma(duckdb::ClientContext& context,
                              const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 2) {
    throw duckdb::InvalidInputException(
      "create_text_search_dictionary requires at least name and if_not_exists");
  }

  auto dict_name = args[0].GetValue<std::string>();
  auto if_not_exists = args[1].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto name = pg::ParseObjectName(dict_name, StaticStrings::kPublic);
  pg::CreateTokenizerFromOptions(conn_ctx, name.relation, name.schema,
                                 if_not_exists, params.named_parameters);
}

// PRAGMA drop_text_search_dictionary('name', missing_ok)
// Parameters:
//   [0] name (VARCHAR) -- optionally schema-qualified as "schema.name"
//   [1] missing_ok (BOOLEAN)
void DropTSDictionaryPragma(duckdb::ClientContext& context,
                            const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 2) {
    throw duckdb::InvalidInputException(
      "drop_text_search_dictionary requires name and missing_ok");
  }

  auto dict_name = args[0].GetValue<std::string>();
  auto missing_ok = args[1].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog = catalog_feature.Global();

  auto name = pg::ParseObjectName(dict_name, StaticStrings::kPublic);

  auto r =
    catalog.DropTokenizer(conn_ctx.GetDatabase(), name.schema, name.relation);

  if (r.is(ERROR_SERVER_ILLEGAL_NAME) && missing_ok) {
    return;
  }
  if (!r.ok()) {
    throw duckdb::InvalidInputException(
      "Failed to drop text search dictionary: %s",
      std::string{r.errorMessage()});
  }
}

}  // namespace

void RegisterTokenizerPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  auto create_pragma = duckdb::PragmaFunction::PragmaCall(
    "create_text_search_dictionary", CreateTSDictionaryPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN});
  loader.RegisterFunction(create_pragma);

  auto drop_pragma = duckdb::PragmaFunction::PragmaCall(
    "drop_text_search_dictionary", DropTSDictionaryPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN});
  loader.RegisterFunction(drop_pragma);
}

}  // namespace sdb::connector
