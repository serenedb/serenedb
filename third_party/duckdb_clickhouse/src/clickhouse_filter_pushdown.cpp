#include "duckdb.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/common/enum_util.hpp"

#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_types.hpp"

namespace duckdb {

static string TransformLiteral(const Value &val) {
	switch (val.type().id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return val.ToString();
	case LogicalTypeId::BOOLEAN:
		return val.GetValue<bool>() ? "1" : "0";
	default:
		return ClickHouseStringLiteral(val.ToString());
	}
}

static string TransformComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "<>";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	default:
		throw NotImplementedException("Unsupported expression type");
	}
}

static string TransformConstantFilter(const string &column_name, ExpressionType comparison_type, const Value &constant,
                                      column_t column_id) {
	if (IsVirtualColumn(column_id)) {
		return string();
	}
	if (constant.IsNull()) {
		// A NULL constant has three-valued semantics that a textual "col <op> NULL"
		// predicate gets wrong; keep the filter local instead of pushing it.
		return string();
	}
	auto constant_string = TransformLiteral(constant);
	auto operator_string = TransformComparison(comparison_type);
	return StringUtil::Format("%s %s %s", column_name, operator_string, constant_string);
}

static string TransformExpressionSubject(const string &column_name, const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return column_name;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		idx_t child_idx;
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.children.empty()) {
			return string();
		}
		auto parent_name = TransformExpressionSubject(column_name, *func.children[0]);
		if (parent_name.empty()) {
			return string();
		}
		auto &struct_type = func.children[0]->GetReturnType();
		if (struct_type.id() != LogicalTypeId::STRUCT || StructType::IsUnnamed(struct_type)) {
			return string();
		}
		// Postgres composite "(expr).field" syntax is invalid in ClickHouse; keep
		// struct-field filters local rather than push an unparseable predicate.
		return string();
	}
	default:
		return string();
	}
}

static string TransformExpression(const string &column_name, const Expression &expr, column_t column_id);

static string CreateExpression(const string &column_name, const vector<unique_ptr<Expression>> &filters, string op,
                               column_t column_id) {
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto filter_str = TransformExpression(column_name, *filter, column_id);
		if (!filter_str.empty()) {
			filter_entries.push_back(std::move(filter_str));
		}
	}
	if (filter_entries.empty()) {
		return string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

static string TransformExpression(const string &column_name, const Expression &expr, column_t column_id) {
	if (IsVirtualColumn(column_id)) {
		return string();
	}

	if (BoundComparisonExpression::IsComparison(expr)) {
		auto &comparison = expr.Cast<BoundFunctionExpression>();
		auto comparison_type = comparison.GetExpressionType();
		auto &left = BoundComparisonExpression::Left(comparison);
		auto &right = BoundComparisonExpression::Right(comparison);
		auto subject = TransformExpressionSubject(column_name, left);
		const Value *constant = nullptr;
		if (!subject.empty() && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			constant = &right.Cast<BoundConstantExpression>().value;
		} else {
			subject = TransformExpressionSubject(column_name, right);
			if (!subject.empty() && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				constant = &left.Cast<BoundConstantExpression>().value;
				comparison_type = FlipComparisonExpression(comparison_type);
			}
		}
		if (!constant || subject.empty()) {
			return string();
		}
		return TransformConstantFilter(subject, comparison_type, *constant, column_id);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return CreateExpression(column_name, conjunction.children, "AND", column_id);
		case ExpressionType::CONJUNCTION_OR:
			return CreateExpression(column_name, conjunction.children, "OR", column_id);
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto subject = op.children.empty() ? string() : TransformExpressionSubject(column_name, *op.children[0]);
		switch (op.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			return !subject.empty() ? subject + " IS NULL" : string();
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			return !subject.empty() ? subject + " IS NOT NULL" : string();
		case ExpressionType::COMPARE_IN: {
			if (subject.empty()) {
				return string();
			}
			string in_list;
			for (idx_t i = 1; i < op.children.size(); i++) {
				if (op.children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return string();
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				auto &constant = op.children[i]->Cast<BoundConstantExpression>().value;
				if (constant.IsNull()) {
					// NULL in an IN list: keep the whole filter local (correct
					// three-valued semantics) rather than push "IN (NULL)".
					return string();
				}
				in_list += TransformLiteral(constant);
			}
			return subject + " IN (" + in_list + ")";
		}
		default:
			return string();
		}
	}
	default:
		return string();
	}
}

static string TransformFilter(const string &column_name, const TableFilter &filter, column_t column_id) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "ClickHouseFilterPushdown::TransformFilter");
	return TransformExpression(column_name, *expr_filter.expr, column_id);
}

string ClickHouseFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                                  const vector<std::string> &names) {
	if (!filters || !filters->HasFilters()) {
		return string();
	}
	string result;
	for (auto &entry : *filters) {
		auto column_id = column_ids[entry.GetIndex()];
		if (IsVirtualColumn(column_id)) {
			continue;
		}
		string column_name = ClickHouseQuoteIdentifier(names[column_id]);
		auto &filter = entry.Filter();
		auto filter_text = TransformFilter(column_name, filter, column_id);

		if (filter_text.empty()) {
			continue;
		}
		if (!result.empty()) {
			result += " AND ";
		}
		result += filter_text;
	}
	return result;
}

} // namespace duckdb
