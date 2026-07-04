#include "duckdb.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_types.hpp"

namespace duckdb {

static string TransformLiteral(const Value &val) {
	try {
		return ClickHouseValueLiteral(val);
	} catch (const NotImplementedException &) {
		// A value with no exact ClickHouse literal form (nested types): empty string
		// signals the caller to keep the filter local.
		return string();
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
		// Unsupported comparison (e.g. IS [NOT] DISTINCT FROM, whose null-safe semantics
		// differ from a plain operator). Empty string signals "keep this filter local"
		// -- never throw, which would abort the whole query.
		return string();
	}
}

static string TransformConstantFilter(const string &column_name, ExpressionType comparison_type,
                                      const Value &constant) {
	if (constant.IsNull()) {
		// A NULL constant has three-valued semantics that a textual "col <op> NULL"
		// predicate gets wrong; keep the filter local instead of pushing it.
		return string();
	}
	auto operator_string = TransformComparison(comparison_type);
	if (operator_string.empty()) {
		// Unsupported comparison operator -> keep the filter local.
		return string();
	}
	auto constant_string = TransformLiteral(constant);
	if (constant_string.empty()) {
		return string();
	}
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
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.GetChildren().empty()) {
			return string();
		}
		auto parent_name = TransformExpressionSubject(column_name, *func.GetChildren()[0]);
		if (parent_name.empty()) {
			return string();
		}
		auto &struct_type = func.GetChildren()[0]->GetReturnType();
		if (struct_type.id() != LogicalTypeId::STRUCT || StructType::IsUnnamed(struct_type)) {
			return string();
		}
		// ClickHouse addresses Tuple fields as tupleElement(col, 'name').
		return "tupleElement(" + parent_name + ", " +
		       ClickHouseStringLiteral(StructType::GetChildName(struct_type, child_idx).GetIdentifierName()) + ")";
	}
	default:
		return string();
	}
}

//! `exact` is degraded (never restored) whenever the rendered SQL is WIDER than the
//! expression -- a dropped required conjunct, an unsupported comparison, a dropped OR
//! branch. The caller must then re-apply the filter locally: the optimizer has already
//! erased fully-pushed filters from the plan, so "keep it local" is not automatic.
static string TransformExpression(const string &column_name, const Expression &expr, column_t column_id, bool &exact);

static string CreateExpression(const string &column_name, const vector<unique_ptr<Expression>> &filters, string op,
                               column_t column_id, bool &exact) {
	bool is_or = op == "OR";
	vector<string> filter_entries;
	for (auto &filter : filters) {
		auto filter_str = TransformExpression(column_name, *filter, column_id, exact);
		if (filter_str.empty()) {
			// An un-pushable branch of an OR makes the whole disjunction un-pushable:
			// dropping it and pushing the rest would under-filter and silently lose rows
			// the remote must not exclude. For AND, pushing the remaining conjuncts is a
			// safe superset -- but only if the whole filter is then re-applied locally,
			// so mark it inexact (unless the dropped piece was advisory-only).
			if (!ExpressionFilter::IsOptionalExpression(*filter)) {
				exact = false;
			}
			if (is_or) {
				return string();
			}
			continue;
		}
		filter_entries.push_back(std::move(filter_str));
	}
	if (filter_entries.empty()) {
		return string();
	}
	return "(" + StringUtil::Join(filter_entries, " " + op + " ") + ")";
}

static string TransformExpression(const string &column_name, const Expression &expr, column_t column_id, bool &exact) {
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
			constant = &right.Cast<BoundConstantExpression>().GetValue();
		} else {
			subject = TransformExpressionSubject(column_name, right);
			if (!subject.empty() && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
				constant = &left.Cast<BoundConstantExpression>().GetValue();
				comparison_type = FlipComparisonExpression(comparison_type);
			}
		}
		if (!constant || subject.empty()) {
			return string();
		}
		return TransformConstantFilter(subject, comparison_type, *constant);
	}

	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		switch (conjunction.GetExpressionType()) {
		case ExpressionType::CONJUNCTION_AND:
			return CreateExpression(column_name, conjunction.GetChildren(), "AND", column_id, exact);
		case ExpressionType::CONJUNCTION_OR:
			return CreateExpression(column_name, conjunction.GetChildren(), "OR", column_id, exact);
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto subject = op.GetChildren().empty() ? string() : TransformExpressionSubject(column_name, *op.GetChildren()[0]);
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
			for (idx_t i = 1; i < op.GetChildren().size(); i++) {
				if (op.GetChildren()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return string();
				}
				if (!in_list.empty()) {
					in_list += ", ";
				}
				auto &constant = op.GetChildren()[i]->Cast<BoundConstantExpression>().GetValue();
				if (constant.IsNull()) {
					// NULL in an IN list: keep the whole filter local (correct
					// three-valued semantics) rather than push "IN (NULL)".
					return string();
				}
				auto literal = TransformLiteral(constant);
				if (literal.empty()) {
					return string();
				}
				in_list += literal;
			}
			return subject + " IN (" + in_list + ")";
		}
		default:
			return string();
		}
	}
	case ExpressionClass::BOUND_FUNCTION: {
		// The optimizer wraps join/IN-subquery-probe predicates in OptionalFilter /
		// SelectivityOptionalFilter scalars whose real predicate hides in a child
		// expression. Unwrap and push that child (mirrors the postgres connector) so
		// the WHERE reaches ClickHouse instead of fetching + re-filtering locally. A
		// bare DynamicFilter has no static form, so it stays local (empty string).
		auto &func = expr.Cast<BoundFunctionExpression>();
		// Failures inside an advisory wrapper never affect correctness, so exactness is
		// tracked with a throwaway flag.
		bool optional_exact = true;
		if (func.Function().GetName() == OptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<OptionalFilterFunctionData>();
			return data.child_filter_expr
			           ? TransformExpression(column_name, *data.child_filter_expr, column_id, optional_exact)
			           : string();
		}
		if (func.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && func.BindInfo()) {
			auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
			return data.child_filter_expr
			           ? TransformExpression(column_name, *data.child_filter_expr, column_id, optional_exact)
			           : string();
		}
		return string();
	}
	default:
		return string();
	}
}

static string TransformFilter(const string &column_name, const TableFilter &filter, column_t column_id, bool &exact) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "ClickHouseFilterPushdown::TransformFilter");
	return TransformExpression(column_name, *expr_filter.expr, column_id, exact);
}

string ClickHouseFilterPushdown::TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
                                                  const vector<std::string> &names, vector<idx_t> *inexact_filters,
                                                  const vector<bool> *stringified, const vector<LogicalType> *types,
                                                  const vector<std::string> *clickhouse_types) {
	if (!filters || !filters->HasFilters()) {
		return string();
	}
	string result;
	for (auto &entry : *filters) {
		auto column_id = column_ids[entry.GetIndex()];
		auto &filter = entry.Filter();
		string filter_text;
		bool exact = true;
		// A column whose remote comparison can diverge from DuckDB's (NaN floats,
		// timezone-parsed timestamps, Enum/IP text) must not be pushed: the remote
		// predicate could wrongly EXCLUDE rows, and re-applying locally cannot
		// recover rows that were never transferred. Leave filter_text empty so the
		// whole filter stays local (reported in inexact_filters below).
		std::string ch_type;
		if (clickhouse_types && column_id < clickhouse_types->size()) {
			ch_type = (*clickhouse_types)[column_id];
		}
		bool comparison_unsafe =
		    types && column_id < types->size() && ClickHouseComparisonUnsafe((*types)[column_id], ch_type);
		if (IsVirtualColumn(column_id) || comparison_unsafe) {
			exact = false;
		} else {
			string column_name = ClickHouseQuoteIdentifier(names[column_id]);
			if (stringified && column_id < stringified->size() && (*stringified)[column_id]) {
				column_name = "toString(" + column_name + ")";
			}
			filter_text = TransformFilter(column_name, filter, column_id, exact);
		}

		if ((filter_text.empty() || !exact) && inexact_filters && !ExpressionFilter::IsOptionalFilter(filter)) {
			// The remote WHERE misses (part of) this required filter, and the optimizer
			// erased it from the plan believing the scan applies it -- the scan must
			// re-apply it locally or rows leak through unfiltered.
			inexact_filters->push_back(entry.GetIndex());
		}
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
