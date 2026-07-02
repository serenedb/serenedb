#include "clickhouse_secrets.hpp"

#include <algorithm>
#include <vector>
#include <unordered_map>

namespace duckdb {

// clang-format off
static const std::vector<std::string> connection_option_names = {
  "host",
  "port",
  "user",
  "password",
  "database",
  "secure"
};

static const std::unordered_map<std::string, std::string> connection_option_aliases = {
  {"username", "user"},
  {"dbname", "database"},
  {"db", "database"},
  {"hostname", "host"}
};

static const std::vector<std::string> other_option_names = {
  "uri"
};
// clang-format on

static const std::string &ResolveAlias(const std::string &input_name) {
	auto it = connection_option_aliases.find(input_name);
	if (it == connection_option_aliases.end()) {
		return input_name;
	}
	return it->second;
}

SecretType ClickHouseSecrets::CreateType() {
	SecretType secret_type;
	secret_type.name = "clickhouse";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	return secret_type;
}

unique_ptr<BaseSecret> ClickHouseSecrets::CreateFunction(ClientContext &context, CreateSecretInput &input) {
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "clickhouse", "config", input.name);
	for (const auto &named_param : input.options) {
		auto input_name = StringUtil::Lower(named_param.first);
		auto name = ResolveAlias(input_name);
		if (std::find(connection_option_names.begin(), connection_option_names.end(), name) ==
		        connection_option_names.end() &&
		    std::find(other_option_names.begin(), other_option_names.end(), name) == other_option_names.end()) {
			throw InternalException("Unknown named parameter for a ClickHouse secret: '" + named_param.first + "'");
		}
		result->secret_map[name] = named_param.second.ToString();
	}
	result->redact_keys = {"password", "uri"};
	return std::move(result);
}

void ClickHouseSecrets::SetSecretParameters(CreateSecretFunction &function) {
	for (const std::string &name : connection_option_names) {
		function.named_parameters[name] = LogicalType::VARCHAR;
	}
	for (auto &en : connection_option_aliases) {
		function.named_parameters[en.first] = LogicalType::VARCHAR;
	}
	// other options
	function.named_parameters["uri"] = LogicalType::VARCHAR;
}

} // namespace duckdb
