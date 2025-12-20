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

#pragma once

#include <ranges>
#include <tuple>

#include "basics/assert.h"
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
LIBPG_QUERY_INCLUDES_END

// TODO: move to pg namespace
namespace sdb {

template<typename T>
constexpr NodeTag GetNodeTag() {
  if constexpr (std::is_same_v<T, List>) {
    return T_List;
  } else if constexpr (std::is_same_v<T, Alias>) {
    return T_Alias;
  } else if constexpr (std::is_same_v<T, RangeVar>) {
    return T_RangeVar;
  } else if constexpr (std::is_same_v<T, TableFunc>) {
    return T_TableFunc;
  } else if constexpr (std::is_same_v<T, IntoClause>) {
    return T_IntoClause;
  } else if constexpr (std::is_same_v<T, Var>) {
    return T_Var;
  } else if constexpr (std::is_same_v<T, Const>) {
    return T_Const;
  } else if constexpr (std::is_same_v<T, Param>) {
    return T_Param;
  } else if constexpr (std::is_same_v<T, Aggref>) {
    return T_Aggref;
  } else if constexpr (std::is_same_v<T, GroupingFunc>) {
    return T_GroupingFunc;
  } else if constexpr (std::is_same_v<T, WindowFunc>) {
    return T_WindowFunc;
  } else if constexpr (std::is_same_v<T, SubscriptingRef>) {
    return T_SubscriptingRef;
  } else if constexpr (std::is_same_v<T, FuncExpr>) {
    return T_FuncExpr;
  } else if constexpr (std::is_same_v<T, NamedArgExpr>) {
    return T_NamedArgExpr;
  } else if constexpr (std::is_same_v<T, OpExpr>) {
    return T_OpExpr;
  } else if constexpr (std::is_same_v<T, DistinctExpr>) {
    return T_DistinctExpr;
  } else if constexpr (std::is_same_v<T, NullIfExpr>) {
    return T_NullIfExpr;
  } else if constexpr (std::is_same_v<T, ScalarArrayOpExpr>) {
    return T_ScalarArrayOpExpr;
  } else if constexpr (std::is_same_v<T, BoolExpr>) {
    return T_BoolExpr;
  } else if constexpr (std::is_same_v<T, SubLink>) {
    return T_SubLink;
  } else if constexpr (std::is_same_v<T, SubPlan>) {
    return T_SubPlan;
  } else if constexpr (std::is_same_v<T, AlternativeSubPlan>) {
    return T_AlternativeSubPlan;
  } else if constexpr (std::is_same_v<T, FieldSelect>) {
    return T_FieldSelect;
  } else if constexpr (std::is_same_v<T, FieldStore>) {
    return T_FieldStore;
  } else if constexpr (std::is_same_v<T, RelabelType>) {
    return T_RelabelType;
  } else if constexpr (std::is_same_v<T, CoerceViaIO>) {
    return T_CoerceViaIO;
  } else if constexpr (std::is_same_v<T, ArrayCoerceExpr>) {
    return T_ArrayCoerceExpr;
  } else if constexpr (std::is_same_v<T, ConvertRowtypeExpr>) {
    return T_ConvertRowtypeExpr;
  } else if constexpr (std::is_same_v<T, CollateExpr>) {
    return T_CollateExpr;
  } else if constexpr (std::is_same_v<T, CaseExpr>) {
    return T_CaseExpr;
  } else if constexpr (std::is_same_v<T, CaseWhen>) {
    return T_CaseWhen;
  } else if constexpr (std::is_same_v<T, CaseTestExpr>) {
    return T_CaseTestExpr;
  } else if constexpr (std::is_same_v<T, ArrayExpr>) {
    return T_ArrayExpr;
  } else if constexpr (std::is_same_v<T, RowExpr>) {
    return T_RowExpr;
  } else if constexpr (std::is_same_v<T, RowCompareExpr>) {
    return T_RowCompareExpr;
  } else if constexpr (std::is_same_v<T, CoalesceExpr>) {
    return T_CoalesceExpr;
  } else if constexpr (std::is_same_v<T, MinMaxExpr>) {
    return T_MinMaxExpr;
  } else if constexpr (std::is_same_v<T, SQLValueFunction>) {
    return T_SQLValueFunction;
  } else if constexpr (std::is_same_v<T, XmlExpr>) {
    return T_XmlExpr;
  } else if constexpr (std::is_same_v<T, JsonFormat>) {
    return T_JsonFormat;
  } else if constexpr (std::is_same_v<T, JsonReturning>) {
    return T_JsonReturning;
  } else if constexpr (std::is_same_v<T, JsonValueExpr>) {
    return T_JsonValueExpr;
  } else if constexpr (std::is_same_v<T, JsonConstructorExpr>) {
    return T_JsonConstructorExpr;
  } else if constexpr (std::is_same_v<T, JsonIsPredicate>) {
    return T_JsonIsPredicate;
  } else if constexpr (std::is_same_v<T, NullTest>) {
    return T_NullTest;
  } else if constexpr (std::is_same_v<T, BooleanTest>) {
    return T_BooleanTest;
  } else if constexpr (std::is_same_v<T, CoerceToDomain>) {
    return T_CoerceToDomain;
  } else if constexpr (std::is_same_v<T, CoerceToDomainValue>) {
    return T_CoerceToDomainValue;
  } else if constexpr (std::is_same_v<T, SetToDefault>) {
    return T_SetToDefault;
  } else if constexpr (std::is_same_v<T, CurrentOfExpr>) {
    return T_CurrentOfExpr;
  } else if constexpr (std::is_same_v<T, NextValueExpr>) {
    return T_NextValueExpr;
  } else if constexpr (std::is_same_v<T, InferenceElem>) {
    return T_InferenceElem;
  } else if constexpr (std::is_same_v<T, TargetEntry>) {
    return T_TargetEntry;
  } else if constexpr (std::is_same_v<T, RangeTblRef>) {
    return T_RangeTblRef;
  } else if constexpr (std::is_same_v<T, JoinExpr>) {
    return T_JoinExpr;
  } else if constexpr (std::is_same_v<T, FromExpr>) {
    return T_FromExpr;
  } else if constexpr (std::is_same_v<T, OnConflictExpr>) {
    return T_OnConflictExpr;
  } else if constexpr (std::is_same_v<T, Query>) {
    return T_Query;
  } else if constexpr (std::is_same_v<T, TypeName>) {
    return T_TypeName;
  } else if constexpr (std::is_same_v<T, ColumnRef>) {
    return T_ColumnRef;
  } else if constexpr (std::is_same_v<T, ParamRef>) {
    return T_ParamRef;
  } else if constexpr (std::is_same_v<T, A_Expr>) {
    return T_A_Expr;
  } else if constexpr (std::is_same_v<T, A_Const>) {
    return T_A_Const;
  } else if constexpr (std::is_same_v<T, TypeCast>) {
    return T_TypeCast;
  } else if constexpr (std::is_same_v<T, CollateClause>) {
    return T_CollateClause;
  } else if constexpr (std::is_same_v<T, RoleSpec>) {
    return T_RoleSpec;
  } else if constexpr (std::is_same_v<T, FuncCall>) {
    return T_FuncCall;
  } else if constexpr (std::is_same_v<T, A_Star>) {
    return T_A_Star;
  } else if constexpr (std::is_same_v<T, A_Indices>) {
    return T_A_Indices;
  } else if constexpr (std::is_same_v<T, A_Indirection>) {
    return T_A_Indirection;
  } else if constexpr (std::is_same_v<T, A_ArrayExpr>) {
    return T_A_ArrayExpr;
  } else if constexpr (std::is_same_v<T, ResTarget>) {
    return T_ResTarget;
  } else if constexpr (std::is_same_v<T, MultiAssignRef>) {
    return T_MultiAssignRef;
  } else if constexpr (std::is_same_v<T, SortBy>) {
    return T_SortBy;
  } else if constexpr (std::is_same_v<T, WindowDef>) {
    return T_WindowDef;
  } else if constexpr (std::is_same_v<T, RangeSubselect>) {
    return T_RangeSubselect;
  } else if constexpr (std::is_same_v<T, RangeFunction>) {
    return T_RangeFunction;
  } else if constexpr (std::is_same_v<T, RangeTableFunc>) {
    return T_RangeTableFunc;
  } else if constexpr (std::is_same_v<T, RangeTableFuncCol>) {
    return T_RangeTableFuncCol;
  } else if constexpr (std::is_same_v<T, RangeTableSample>) {
    return T_RangeTableSample;
  } else if constexpr (std::is_same_v<T, ColumnDef>) {
    return T_ColumnDef;
  } else if constexpr (std::is_same_v<T, TableLikeClause>) {
    return T_TableLikeClause;
  } else if constexpr (std::is_same_v<T, IndexElem>) {
    return T_IndexElem;
  } else if constexpr (std::is_same_v<T, DefElem>) {
    return T_DefElem;
  } else if constexpr (std::is_same_v<T, LockingClause>) {
    return T_LockingClause;
  } else if constexpr (std::is_same_v<T, XmlSerialize>) {
    return T_XmlSerialize;
  } else if constexpr (std::is_same_v<T, PartitionElem>) {
    return T_PartitionElem;
  } else if constexpr (std::is_same_v<T, PartitionSpec>) {
    return T_PartitionSpec;
  } else if constexpr (std::is_same_v<T, PartitionBoundSpec>) {
    return T_PartitionBoundSpec;
  } else if constexpr (std::is_same_v<T, PartitionRangeDatum>) {
    return T_PartitionRangeDatum;
  } else if constexpr (std::is_same_v<T, PartitionCmd>) {
    return T_PartitionCmd;
  } else if constexpr (std::is_same_v<T, RangeTblEntry>) {
    return T_RangeTblEntry;
  } else if constexpr (std::is_same_v<T, RTEPermissionInfo>) {
    return T_RTEPermissionInfo;
  } else if constexpr (std::is_same_v<T, RangeTblFunction>) {
    return T_RangeTblFunction;
  } else if constexpr (std::is_same_v<T, TableSampleClause>) {
    return T_TableSampleClause;
  } else if constexpr (std::is_same_v<T, WithCheckOption>) {
    return T_WithCheckOption;
  } else if constexpr (std::is_same_v<T, SortGroupClause>) {
    return T_SortGroupClause;
  } else if constexpr (std::is_same_v<T, GroupingSet>) {
    return T_GroupingSet;
  } else if constexpr (std::is_same_v<T, WindowClause>) {
    return T_WindowClause;
  } else if constexpr (std::is_same_v<T, RowMarkClause>) {
    return T_RowMarkClause;
  } else if constexpr (std::is_same_v<T, WithClause>) {
    return T_WithClause;
  } else if constexpr (std::is_same_v<T, InferClause>) {
    return T_InferClause;
  } else if constexpr (std::is_same_v<T, OnConflictClause>) {
    return T_OnConflictClause;
  } else if constexpr (std::is_same_v<T, CTESearchClause>) {
    return T_CTESearchClause;
  } else if constexpr (std::is_same_v<T, CTECycleClause>) {
    return T_CTECycleClause;
  } else if constexpr (std::is_same_v<T, CommonTableExpr>) {
    return T_CommonTableExpr;
  } else if constexpr (std::is_same_v<T, MergeWhenClause>) {
    return T_MergeWhenClause;
  } else if constexpr (std::is_same_v<T, MergeAction>) {
    return T_MergeAction;
  } else if constexpr (std::is_same_v<T, TriggerTransition>) {
    return T_TriggerTransition;
  } else if constexpr (std::is_same_v<T, JsonOutput>) {
    return T_JsonOutput;
  } else if constexpr (std::is_same_v<T, JsonKeyValue>) {
    return T_JsonKeyValue;
  } else if constexpr (std::is_same_v<T, JsonObjectConstructor>) {
    return T_JsonObjectConstructor;
  } else if constexpr (std::is_same_v<T, JsonArrayConstructor>) {
    return T_JsonArrayConstructor;
  } else if constexpr (std::is_same_v<T, JsonArrayQueryConstructor>) {
    return T_JsonArrayQueryConstructor;
  } else if constexpr (std::is_same_v<T, JsonAggConstructor>) {
    return T_JsonAggConstructor;
  } else if constexpr (std::is_same_v<T, JsonObjectAgg>) {
    return T_JsonObjectAgg;
  } else if constexpr (std::is_same_v<T, JsonArrayAgg>) {
    return T_JsonArrayAgg;
  } else if constexpr (std::is_same_v<T, RawStmt>) {
    return T_RawStmt;
  } else if constexpr (std::is_same_v<T, InsertStmt>) {
    return T_InsertStmt;
  } else if constexpr (std::is_same_v<T, DeleteStmt>) {
    return T_DeleteStmt;
  } else if constexpr (std::is_same_v<T, UpdateStmt>) {
    return T_UpdateStmt;
  } else if constexpr (std::is_same_v<T, MergeStmt>) {
    return T_MergeStmt;
  } else if constexpr (std::is_same_v<T, SelectStmt>) {
    return T_SelectStmt;
  } else if constexpr (std::is_same_v<T, SetOperationStmt>) {
    return T_SetOperationStmt;
  } else if constexpr (std::is_same_v<T, ReturnStmt>) {
    return T_ReturnStmt;
  } else if constexpr (std::is_same_v<T, PLAssignStmt>) {
    return T_PLAssignStmt;
  } else if constexpr (std::is_same_v<T, CreateSchemaStmt>) {
    return T_CreateSchemaStmt;
  } else if constexpr (std::is_same_v<T, AlterTableStmt>) {
    return T_AlterTableStmt;
  } else if constexpr (std::is_same_v<T, ReplicaIdentityStmt>) {
    return T_ReplicaIdentityStmt;
  } else if constexpr (std::is_same_v<T, AlterTableCmd>) {
    return T_AlterTableCmd;
  } else if constexpr (std::is_same_v<T, AlterCollationStmt>) {
    return T_AlterCollationStmt;
  } else if constexpr (std::is_same_v<T, AlterDomainStmt>) {
    return T_AlterDomainStmt;
  } else if constexpr (std::is_same_v<T, GrantStmt>) {
    return T_GrantStmt;
  } else if constexpr (std::is_same_v<T, ObjectWithArgs>) {
    return T_ObjectWithArgs;
  } else if constexpr (std::is_same_v<T, AccessPriv>) {
    return T_AccessPriv;
  } else if constexpr (std::is_same_v<T, GrantRoleStmt>) {
    return T_GrantRoleStmt;
  } else if constexpr (std::is_same_v<T, AlterDefaultPrivilegesStmt>) {
    return T_AlterDefaultPrivilegesStmt;
  } else if constexpr (std::is_same_v<T, CopyStmt>) {
    return T_CopyStmt;
  } else if constexpr (std::is_same_v<T, VariableSetStmt>) {
    return T_VariableSetStmt;
  } else if constexpr (std::is_same_v<T, VariableShowStmt>) {
    return T_VariableShowStmt;
  } else if constexpr (std::is_same_v<T, CreateStmt>) {
    return T_CreateStmt;
  } else if constexpr (std::is_same_v<T, Constraint>) {
    return T_Constraint;
  } else if constexpr (std::is_same_v<T, CreateTableSpaceStmt>) {
    return T_CreateTableSpaceStmt;
  } else if constexpr (std::is_same_v<T, DropTableSpaceStmt>) {
    return T_DropTableSpaceStmt;
  } else if constexpr (std::is_same_v<T, AlterTableSpaceOptionsStmt>) {
    return T_AlterTableSpaceOptionsStmt;
  } else if constexpr (std::is_same_v<T, AlterTableMoveAllStmt>) {
    return T_AlterTableMoveAllStmt;
  } else if constexpr (std::is_same_v<T, CreateExtensionStmt>) {
    return T_CreateExtensionStmt;
  } else if constexpr (std::is_same_v<T, AlterExtensionStmt>) {
    return T_AlterExtensionStmt;
  } else if constexpr (std::is_same_v<T, AlterExtensionContentsStmt>) {
    return T_AlterExtensionContentsStmt;
  } else if constexpr (std::is_same_v<T, CreateFdwStmt>) {
    return T_CreateFdwStmt;
  } else if constexpr (std::is_same_v<T, AlterFdwStmt>) {
    return T_AlterFdwStmt;
  } else if constexpr (std::is_same_v<T, CreateForeignServerStmt>) {
    return T_CreateForeignServerStmt;
  } else if constexpr (std::is_same_v<T, AlterForeignServerStmt>) {
    return T_AlterForeignServerStmt;
  } else if constexpr (std::is_same_v<T, CreateForeignTableStmt>) {
    return T_CreateForeignTableStmt;
  } else if constexpr (std::is_same_v<T, CreateUserMappingStmt>) {
    return T_CreateUserMappingStmt;
  } else if constexpr (std::is_same_v<T, AlterUserMappingStmt>) {
    return T_AlterUserMappingStmt;
  } else if constexpr (std::is_same_v<T, DropUserMappingStmt>) {
    return T_DropUserMappingStmt;
  } else if constexpr (std::is_same_v<T, ImportForeignSchemaStmt>) {
    return T_ImportForeignSchemaStmt;
  } else if constexpr (std::is_same_v<T, CreatePolicyStmt>) {
    return T_CreatePolicyStmt;
  } else if constexpr (std::is_same_v<T, AlterPolicyStmt>) {
    return T_AlterPolicyStmt;
  } else if constexpr (std::is_same_v<T, CreateAmStmt>) {
    return T_CreateAmStmt;
  } else if constexpr (std::is_same_v<T, CreateTrigStmt>) {
    return T_CreateTrigStmt;
  } else if constexpr (std::is_same_v<T, CreateEventTrigStmt>) {
    return T_CreateEventTrigStmt;
  } else if constexpr (std::is_same_v<T, AlterEventTrigStmt>) {
    return T_AlterEventTrigStmt;
  } else if constexpr (std::is_same_v<T, CreatePLangStmt>) {
    return T_CreatePLangStmt;
  } else if constexpr (std::is_same_v<T, CreateRoleStmt>) {
    return T_CreateRoleStmt;
  } else if constexpr (std::is_same_v<T, AlterRoleStmt>) {
    return T_AlterRoleStmt;
  } else if constexpr (std::is_same_v<T, AlterRoleSetStmt>) {
    return T_AlterRoleSetStmt;
  } else if constexpr (std::is_same_v<T, DropRoleStmt>) {
    return T_DropRoleStmt;
  } else if constexpr (std::is_same_v<T, CreateSeqStmt>) {
    return T_CreateSeqStmt;
  } else if constexpr (std::is_same_v<T, AlterSeqStmt>) {
    return T_AlterSeqStmt;
  } else if constexpr (std::is_same_v<T, DefineStmt>) {
    return T_DefineStmt;
  } else if constexpr (std::is_same_v<T, CreateDomainStmt>) {
    return T_CreateDomainStmt;
  } else if constexpr (std::is_same_v<T, CreateOpClassStmt>) {
    return T_CreateOpClassStmt;
  } else if constexpr (std::is_same_v<T, CreateOpClassItem>) {
    return T_CreateOpClassItem;
  } else if constexpr (std::is_same_v<T, CreateOpFamilyStmt>) {
    return T_CreateOpFamilyStmt;
  } else if constexpr (std::is_same_v<T, AlterOpFamilyStmt>) {
    return T_AlterOpFamilyStmt;
  } else if constexpr (std::is_same_v<T, DropStmt>) {
    return T_DropStmt;
  } else if constexpr (std::is_same_v<T, TruncateStmt>) {
    return T_TruncateStmt;
  } else if constexpr (std::is_same_v<T, CommentStmt>) {
    return T_CommentStmt;
  } else if constexpr (std::is_same_v<T, SecLabelStmt>) {
    return T_SecLabelStmt;
  } else if constexpr (std::is_same_v<T, DeclareCursorStmt>) {
    return T_DeclareCursorStmt;
  } else if constexpr (std::is_same_v<T, ClosePortalStmt>) {
    return T_ClosePortalStmt;
  } else if constexpr (std::is_same_v<T, FetchStmt>) {
    return T_FetchStmt;
  } else if constexpr (std::is_same_v<T, IndexStmt>) {
    return T_IndexStmt;
  } else if constexpr (std::is_same_v<T, CreateStatsStmt>) {
    return T_CreateStatsStmt;
  } else if constexpr (std::is_same_v<T, StatsElem>) {
    return T_StatsElem;
  } else if constexpr (std::is_same_v<T, AlterStatsStmt>) {
    return T_AlterStatsStmt;
  } else if constexpr (std::is_same_v<T, CreateFunctionStmt>) {
    return T_CreateFunctionStmt;
  } else if constexpr (std::is_same_v<T, ::FunctionParameter>) {
    return T_FunctionParameter;
  } else if constexpr (std::is_same_v<T, AlterFunctionStmt>) {
    return T_AlterFunctionStmt;
  } else if constexpr (std::is_same_v<T, DoStmt>) {
    return T_DoStmt;
  } else if constexpr (std::is_same_v<T, InlineCodeBlock>) {
    return T_InlineCodeBlock;
  } else if constexpr (std::is_same_v<T, CallStmt>) {
    return T_CallStmt;
  } else if constexpr (std::is_same_v<T, CallContext>) {
    return T_CallContext;
  } else if constexpr (std::is_same_v<T, RenameStmt>) {
    return T_RenameStmt;
  } else if constexpr (std::is_same_v<T, AlterObjectDependsStmt>) {
    return T_AlterObjectDependsStmt;
  } else if constexpr (std::is_same_v<T, AlterObjectSchemaStmt>) {
    return T_AlterObjectSchemaStmt;
  } else if constexpr (std::is_same_v<T, AlterOwnerStmt>) {
    return T_AlterOwnerStmt;
  } else if constexpr (std::is_same_v<T, AlterOperatorStmt>) {
    return T_AlterOperatorStmt;
  } else if constexpr (std::is_same_v<T, AlterTypeStmt>) {
    return T_AlterTypeStmt;
  } else if constexpr (std::is_same_v<T, RuleStmt>) {
    return T_RuleStmt;
  } else if constexpr (std::is_same_v<T, NotifyStmt>) {
    return T_NotifyStmt;
  } else if constexpr (std::is_same_v<T, ListenStmt>) {
    return T_ListenStmt;
  } else if constexpr (std::is_same_v<T, UnlistenStmt>) {
    return T_UnlistenStmt;
  } else if constexpr (std::is_same_v<T, TransactionStmt>) {
    return T_TransactionStmt;
  } else if constexpr (std::is_same_v<T, CompositeTypeStmt>) {
    return T_CompositeTypeStmt;
  } else if constexpr (std::is_same_v<T, CreateEnumStmt>) {
    return T_CreateEnumStmt;
  } else if constexpr (std::is_same_v<T, CreateRangeStmt>) {
    return T_CreateRangeStmt;
  } else if constexpr (std::is_same_v<T, AlterEnumStmt>) {
    return T_AlterEnumStmt;
  } else if constexpr (std::is_same_v<T, ViewStmt>) {
    return T_ViewStmt;
  } else if constexpr (std::is_same_v<T, LoadStmt>) {
    return T_LoadStmt;
  } else if constexpr (std::is_same_v<T, CreatedbStmt>) {
    return T_CreatedbStmt;
  } else if constexpr (std::is_same_v<T, AlterDatabaseStmt>) {
    return T_AlterDatabaseStmt;
  } else if constexpr (std::is_same_v<T, AlterDatabaseRefreshCollStmt>) {
    return T_AlterDatabaseRefreshCollStmt;
  } else if constexpr (std::is_same_v<T, AlterDatabaseSetStmt>) {
    return T_AlterDatabaseSetStmt;
  } else if constexpr (std::is_same_v<T, DropdbStmt>) {
    return T_DropdbStmt;
  } else if constexpr (std::is_same_v<T, AlterSystemStmt>) {
    return T_AlterSystemStmt;
  } else if constexpr (std::is_same_v<T, ClusterStmt>) {
    return T_ClusterStmt;
  } else if constexpr (std::is_same_v<T, VacuumStmt>) {
    return T_VacuumStmt;
  } else if constexpr (std::is_same_v<T, VacuumRelation>) {
    return T_VacuumRelation;
  } else if constexpr (std::is_same_v<T, ExplainStmt>) {
    return T_ExplainStmt;
  } else if constexpr (std::is_same_v<T, CreateTableAsStmt>) {
    return T_CreateTableAsStmt;
  } else if constexpr (std::is_same_v<T, RefreshMatViewStmt>) {
    return T_RefreshMatViewStmt;
  } else if constexpr (std::is_same_v<T, CheckPointStmt>) {
    return T_CheckPointStmt;
  } else if constexpr (std::is_same_v<T, DiscardStmt>) {
    return T_DiscardStmt;
  } else if constexpr (std::is_same_v<T, LockStmt>) {
    return T_LockStmt;
  } else if constexpr (std::is_same_v<T, ConstraintsSetStmt>) {
    return T_ConstraintsSetStmt;
  } else if constexpr (std::is_same_v<T, ReindexStmt>) {
    return T_ReindexStmt;
  } else if constexpr (std::is_same_v<T, CreateConversionStmt>) {
    return T_CreateConversionStmt;
  } else if constexpr (std::is_same_v<T, CreateCastStmt>) {
    return T_CreateCastStmt;
  } else if constexpr (std::is_same_v<T, CreateTransformStmt>) {
    return T_CreateTransformStmt;
  } else if constexpr (std::is_same_v<T, PrepareStmt>) {
    return T_PrepareStmt;
  } else if constexpr (std::is_same_v<T, ExecuteStmt>) {
    return T_ExecuteStmt;
  } else if constexpr (std::is_same_v<T, DeallocateStmt>) {
    return T_DeallocateStmt;
  } else if constexpr (std::is_same_v<T, DropOwnedStmt>) {
    return T_DropOwnedStmt;
  } else if constexpr (std::is_same_v<T, ReassignOwnedStmt>) {
    return T_ReassignOwnedStmt;
  } else if constexpr (std::is_same_v<T, AlterTSDictionaryStmt>) {
    return T_AlterTSDictionaryStmt;
  } else if constexpr (std::is_same_v<T, AlterTSConfigurationStmt>) {
    return T_AlterTSConfigurationStmt;
  } else if constexpr (std::is_same_v<T, PublicationTable>) {
    return T_PublicationTable;
  } else if constexpr (std::is_same_v<T, PublicationObjSpec>) {
    return T_PublicationObjSpec;
  } else if constexpr (std::is_same_v<T, CreatePublicationStmt>) {
    return T_CreatePublicationStmt;
  } else if constexpr (std::is_same_v<T, AlterPublicationStmt>) {
    return T_AlterPublicationStmt;
  } else if constexpr (std::is_same_v<T, CreateSubscriptionStmt>) {
    return T_CreateSubscriptionStmt;
  } else if constexpr (std::is_same_v<T, AlterSubscriptionStmt>) {
    return T_AlterSubscriptionStmt;
  } else if constexpr (std::is_same_v<T, DropSubscriptionStmt>) {
    return T_DropSubscriptionStmt;
    // } else if constexpr (std::is_same_v<T, PlannerGlobal>) {
    //   return T_PlannerGlobal;
    // } else if constexpr (std::is_same_v<T, PlannerInfo>) {
    //   return T_PlannerInfo;
    // } else if constexpr (std::is_same_v<T, RelOptInfo>) {
    //   return T_RelOptInfo;
    // } else if constexpr (std::is_same_v<T, IndexOptInfo>) {
    //   return T_IndexOptInfo;
    // } else if constexpr (std::is_same_v<T, ForeignKeyOptInfo>) {
    //   return T_ForeignKeyOptInfo;
    // } else if constexpr (std::is_same_v<T, StatisticExtInfo>) {
    //   return T_StatisticExtInfo;
    // } else if constexpr (std::is_same_v<T, JoinDomain>) {
    //   return T_JoinDomain;
    // } else if constexpr (std::is_same_v<T, EquivalenceClass>) {
    //   return T_EquivalenceClass;
    // } else if constexpr (std::is_same_v<T, EquivalenceMember>) {
    //   return T_EquivalenceMember;
    // } else if constexpr (std::is_same_v<T, PathKey>) {
    //   return T_PathKey;
    // } else if constexpr (std::is_same_v<T, PathTarget>) {
    //   return T_PathTarget;
    // } else if constexpr (std::is_same_v<T, ParamPathInfo>) {
    //   return T_ParamPathInfo;
    // } else if constexpr (std::is_same_v<T, Path>) {
    //   return T_Path;
    // } else if constexpr (std::is_same_v<T, IndexPath>) {
    //   return T_IndexPath;
    // } else if constexpr (std::is_same_v<T, IndexClause>) {
    //   return T_IndexClause;
    // } else if constexpr (std::is_same_v<T, BitmapHeapPath>) {
    //   return T_BitmapHeapPath;
    // } else if constexpr (std::is_same_v<T, BitmapAndPath>) {
    //   return T_BitmapAndPath;
    // } else if constexpr (std::is_same_v<T, BitmapOrPath>) {
    //   return T_BitmapOrPath;
    // } else if constexpr (std::is_same_v<T, TidPath>) {
    //   return T_TidPath;
    // } else if constexpr (std::is_same_v<T, TidRangePath>) {
    //   return T_TidRangePath;
    // } else if constexpr (std::is_same_v<T, SubqueryScanPath>) {
    //   return T_SubqueryScanPath;
    // } else if constexpr (std::is_same_v<T, ForeignPath>) {
    //   return T_ForeignPath;
    // } else if constexpr (std::is_same_v<T, CustomPath>) {
    //   return T_CustomPath;
    // } else if constexpr (std::is_same_v<T, AppendPath>) {
    //   return T_AppendPath;
    // } else if constexpr (std::is_same_v<T, MergeAppendPath>) {
    //   return T_MergeAppendPath;
    // } else if constexpr (std::is_same_v<T, GroupResultPath>) {
    //   return T_GroupResultPath;
    // } else if constexpr (std::is_same_v<T, MaterialPath>) {
    //   return T_MaterialPath;
    // } else if constexpr (std::is_same_v<T, MemoizePath>) {
    //   return T_MemoizePath;
    // } else if constexpr (std::is_same_v<T, UniquePath>) {
    //   return T_UniquePath;
    // } else if constexpr (std::is_same_v<T, GatherPath>) {
    //   return T_GatherPath;
    // } else if constexpr (std::is_same_v<T, GatherMergePath>) {
    //   return T_GatherMergePath;
    // } else if constexpr (std::is_same_v<T, NestPath>) {
    //   return T_NestPath;
    // } else if constexpr (std::is_same_v<T, MergePath>) {
    //   return T_MergePath;
    // } else if constexpr (std::is_same_v<T, HashPath>) {
    //   return T_HashPath;
    // } else if constexpr (std::is_same_v<T, ProjectionPath>) {
    //   return T_ProjectionPath;
    // } else if constexpr (std::is_same_v<T, ProjectSetPath>) {
    //   return T_ProjectSetPath;
    // } else if constexpr (std::is_same_v<T, SortPath>) {
    //   return T_SortPath;
    // } else if constexpr (std::is_same_v<T, IncrementalSortPath>) {
    //   return T_IncrementalSortPath;
    // } else if constexpr (std::is_same_v<T, GroupPath>) {
    //   return T_GroupPath;
    // } else if constexpr (std::is_same_v<T, UpperUniquePath>) {
    //   return T_UpperUniquePath;
    // } else if constexpr (std::is_same_v<T, AggPath>) {
    //   return T_AggPath;
    // } else if constexpr (std::is_same_v<T, GroupingSetData>) {
    //   return T_GroupingSetData;
    // } else if constexpr (std::is_same_v<T, RollupData>) {
    //   return T_RollupData;
    // } else if constexpr (std::is_same_v<T, GroupingSetsPath>) {
    //   return T_GroupingSetsPath;
    // } else if constexpr (std::is_same_v<T, MinMaxAggPath>) {
    //   return T_MinMaxAggPath;
    // } else if constexpr (std::is_same_v<T, WindowAggPath>) {
    //   return T_WindowAggPath;
    // } else if constexpr (std::is_same_v<T, SetOpPath>) {
    //   return T_SetOpPath;
    // } else if constexpr (std::is_same_v<T, RecursiveUnionPath>) {
    //   return T_RecursiveUnionPath;
    // } else if constexpr (std::is_same_v<T, LockRowsPath>) {
    //   return T_LockRowsPath;
    // } else if constexpr (std::is_same_v<T, ModifyTablePath>) {
    //   return T_ModifyTablePath;
    // } else if constexpr (std::is_same_v<T, LimitPath>) {
    //   return T_LimitPath;
    // } else if constexpr (std::is_same_v<T, RestrictInfo>) {
    //   return T_RestrictInfo;
    // } else if constexpr (std::is_same_v<T, PlaceHolderVar>) {
    //   return T_PlaceHolderVar;
    // } else if constexpr (std::is_same_v<T, SpecialJoinInfo>) {
    //   return T_SpecialJoinInfo;
    // } else if constexpr (std::is_same_v<T, OuterJoinClauseInfo>) {
    //   return T_OuterJoinClauseInfo;
    // } else if constexpr (std::is_same_v<T, AppendRelInfo>) {
    //   return T_AppendRelInfo;
    // } else if constexpr (std::is_same_v<T, RowIdentityVarInfo>) {
    //   return T_RowIdentityVarInfo;
    // } else if constexpr (std::is_same_v<T, PlaceHolderInfo>) {
    //   return T_PlaceHolderInfo;
    // } else if constexpr (std::is_same_v<T, MinMaxAggInfo>) {
    //   return T_MinMaxAggInfo;
    // } else if constexpr (std::is_same_v<T, PlannerParamItem>) {
    //   return T_PlannerParamItem;
    // } else if constexpr (std::is_same_v<T, AggInfo>) {
    //   return T_AggInfo;
    // } else if constexpr (std::is_same_v<T, AggTransInfo>) {
    //   return T_AggTransInfo;
    // } else if constexpr (std::is_same_v<T, PlannedStmt>) {
    //   return T_PlannedStmt;
    // } else if constexpr (std::is_same_v<T, Result>) {
    //   return T_Result;
    // } else if constexpr (std::is_same_v<T, ProjectSet>) {
    //   return T_ProjectSet;
    // } else if constexpr (std::is_same_v<T, ModifyTable>) {
    //   return T_ModifyTable;
    // } else if constexpr (std::is_same_v<T, Append>) {
    //   return T_Append;
    // } else if constexpr (std::is_same_v<T, MergeAppend>) {
    //   return T_MergeAppend;
    // } else if constexpr (std::is_same_v<T, RecursiveUnion>) {
    //   return T_RecursiveUnion;
    // } else if constexpr (std::is_same_v<T, BitmapAnd>) {
    //   return T_BitmapAnd;
    // } else if constexpr (std::is_same_v<T, BitmapOr>) {
    //   return T_BitmapOr;
    // } else if constexpr (std::is_same_v<T, SeqScan>) {
    //   return T_SeqScan;
    // } else if constexpr (std::is_same_v<T, SampleScan>) {
    //   return T_SampleScan;
    // } else if constexpr (std::is_same_v<T, IndexScan>) {
    //   return T_IndexScan;
    // } else if constexpr (std::is_same_v<T, IndexOnlyScan>) {
    //   return T_IndexOnlyScan;
    // } else if constexpr (std::is_same_v<T, BitmapIndexScan>) {
    //   return T_BitmapIndexScan;
    // } else if constexpr (std::is_same_v<T, BitmapHeapScan>) {
    //   return T_BitmapHeapScan;
    // } else if constexpr (std::is_same_v<T, TidScan>) {
    //   return T_TidScan;
    // } else if constexpr (std::is_same_v<T, TidRangeScan>) {
    //   return T_TidRangeScan;
    // } else if constexpr (std::is_same_v<T, SubqueryScan>) {
    //   return T_SubqueryScan;
    // } else if constexpr (std::is_same_v<T, FunctionScan>) {
    //   return T_FunctionScan;
    // } else if constexpr (std::is_same_v<T, ValuesScan>) {
    //   return T_ValuesScan;
    // } else if constexpr (std::is_same_v<T, TableFuncScan>) {
    //   return T_TableFuncScan;
    // } else if constexpr (std::is_same_v<T, CteScan>) {
    //   return T_CteScan;
    // } else if constexpr (std::is_same_v<T, NamedTuplestoreScan>) {
    //   return T_NamedTuplestoreScan;
    // } else if constexpr (std::is_same_v<T, WorkTableScan>) {
    //   return T_WorkTableScan;
    // } else if constexpr (std::is_same_v<T, ForeignScan>) {
    //   return T_ForeignScan;
    // } else if constexpr (std::is_same_v<T, CustomScan>) {
    //   return T_CustomScan;
    // } else if constexpr (std::is_same_v<T, NestLoop>) {
    //   return T_NestLoop;
    // } else if constexpr (std::is_same_v<T, NestLoopParam>) {
    //   return T_NestLoopParam;
    // } else if constexpr (std::is_same_v<T, MergeJoin>) {
    //   return T_MergeJoin;
    // } else if constexpr (std::is_same_v<T, HashJoin>) {
    //   return T_HashJoin;
    // } else if constexpr (std::is_same_v<T, Material>) {
    //   return T_Material;
    // } else if constexpr (std::is_same_v<T, Memoize>) {
    //   return T_Memoize;
    // } else if constexpr (std::is_same_v<T, Sort>) {
    //   return T_Sort;
    // } else if constexpr (std::is_same_v<T, IncrementalSort>) {
    //   return T_IncrementalSort;
    // } else if constexpr (std::is_same_v<T, Group>) {
    //   return T_Group;
    // } else if constexpr (std::is_same_v<T, Agg>) {
    //   return T_Agg;
    // } else if constexpr (std::is_same_v<T, WindowAgg>) {
    //   return T_WindowAgg;
    // } else if constexpr (std::is_same_v<T, Unique>) {
    //   return T_Unique;
    // } else if constexpr (std::is_same_v<T, Gather>) {
    //   return T_Gather;
    // } else if constexpr (std::is_same_v<T, GatherMerge>) {
    //   return T_GatherMerge;
    // } else if constexpr (std::is_same_v<T, Hash>) {
    //   return T_Hash;
    // } else if constexpr (std::is_same_v<T, SetOp>) {
    //   return T_SetOp;
    // } else if constexpr (std::is_same_v<T, LockRows>) {
    //   return T_LockRows;
    // } else if constexpr (std::is_same_v<T, Limit>) {
    //   return T_Limit;
    // } else if constexpr (std::is_same_v<T, PlanRowMark>) {
    //   return T_PlanRowMark;
    // } else if constexpr (std::is_same_v<T, PartitionPruneInfo>) {
    //   return T_PartitionPruneInfo;
    // } else if constexpr (std::is_same_v<T, PartitionedRelPruneInfo>) {
    //   return T_PartitionedRelPruneInfo;
    // } else if constexpr (std::is_same_v<T, PartitionPruneStepOp>) {
    //   return T_PartitionPruneStepOp;
    // } else if constexpr (std::is_same_v<T, PartitionPruneStepCombine>) {
    //   return T_PartitionPruneStepCombine;
    // } else if constexpr (std::is_same_v<T, PlanInvalItem>) {
    //   return T_PlanInvalItem;
    // } else if constexpr (std::is_same_v<T, ExprState>) {
    //   return T_ExprState;
    // } else if constexpr (std::is_same_v<T, IndexInfo>) {
    //   return T_IndexInfo;
    // } else if constexpr (std::is_same_v<T, ExprContext>) {
    //   return T_ExprContext;
    // } else if constexpr (std::is_same_v<T, ReturnSetInfo>) {
    //   return T_ReturnSetInfo;
    // } else if constexpr (std::is_same_v<T, ProjectionInfo>) {
    //   return T_ProjectionInfo;
    // } else if constexpr (std::is_same_v<T, JunkFilter>) {
    //   return T_JunkFilter;
    // } else if constexpr (std::is_same_v<T, OnConflictSetState>) {
    //   return T_OnConflictSetState;
    // } else if constexpr (std::is_same_v<T, MergeActionState>) {
    //   return T_MergeActionState;
    // } else if constexpr (std::is_same_v<T, ResultRelInfo>) {
    //   return T_ResultRelInfo;
    // } else if constexpr (std::is_same_v<T, EState>) {
    //   return T_EState;
    // } else if constexpr (std::is_same_v<T, WindowFuncExprState>) {
    //   return T_WindowFuncExprState;
    // } else if constexpr (std::is_same_v<T, SetExprState>) {
    //   return T_SetExprState;
    // } else if constexpr (std::is_same_v<T, SubPlanState>) {
    //   return T_SubPlanState;
    // } else if constexpr (std::is_same_v<T, DomainConstraintState>) {
    //   return T_DomainConstraintState;
    // } else if constexpr (std::is_same_v<T, ResultState>) {
    //   return T_ResultState;
    // } else if constexpr (std::is_same_v<T, ProjectSetState>) {
    //   return T_ProjectSetState;
    // } else if constexpr (std::is_same_v<T, ModifyTableState>) {
    //   return T_ModifyTableState;
    // } else if constexpr (std::is_same_v<T, AppendState>) {
    //   return T_AppendState;
    // } else if constexpr (std::is_same_v<T, MergeAppendState>) {
    //   return T_MergeAppendState;
    // } else if constexpr (std::is_same_v<T, RecursiveUnionState>) {
    //   return T_RecursiveUnionState;
    // } else if constexpr (std::is_same_v<T, BitmapAndState>) {
    //   return T_BitmapAndState;
    // } else if constexpr (std::is_same_v<T, BitmapOrState>) {
    //   return T_BitmapOrState;
    // } else if constexpr (std::is_same_v<T, ScanState>) {
    //   return T_ScanState;
    // } else if constexpr (std::is_same_v<T, SeqScanState>) {
    //   return T_SeqScanState;
    // } else if constexpr (std::is_same_v<T, SampleScanState>) {
    //   return T_SampleScanState;
    // } else if constexpr (std::is_same_v<T, IndexScanState>) {
    //   return T_IndexScanState;
    // } else if constexpr (std::is_same_v<T, IndexOnlyScanState>) {
    //   return T_IndexOnlyScanState;
    // } else if constexpr (std::is_same_v<T, BitmapIndexScanState>) {
    //   return T_BitmapIndexScanState;
    // } else if constexpr (std::is_same_v<T, BitmapHeapScanState>) {
    //   return T_BitmapHeapScanState;
    // } else if constexpr (std::is_same_v<T, TidScanState>) {
    //   return T_TidScanState;
    // } else if constexpr (std::is_same_v<T, TidRangeScanState>) {
    //   return T_TidRangeScanState;
    // } else if constexpr (std::is_same_v<T, SubqueryScanState>) {
    //   return T_SubqueryScanState;
    // } else if constexpr (std::is_same_v<T, FunctionScanState>) {
    //   return T_FunctionScanState;
    // } else if constexpr (std::is_same_v<T, ValuesScanState>) {
    //   return T_ValuesScanState;
    // } else if constexpr (std::is_same_v<T, TableFuncScanState>) {
    //   return T_TableFuncScanState;
    // } else if constexpr (std::is_same_v<T, CteScanState>) {
    //   return T_CteScanState;
    // } else if constexpr (std::is_same_v<T, NamedTuplestoreScanState>) {
    //   return T_NamedTuplestoreScanState;
    // } else if constexpr (std::is_same_v<T, WorkTableScanState>) {
    //   return T_WorkTableScanState;
    // } else if constexpr (std::is_same_v<T, ForeignScanState>) {
    //   return T_ForeignScanState;
    // } else if constexpr (std::is_same_v<T, CustomScanState>) {
    //   return T_CustomScanState;
    // } else if constexpr (std::is_same_v<T, JoinState>) {
    //   return T_JoinState;
    // } else if constexpr (std::is_same_v<T, NestLoopState>) {
    //   return T_NestLoopState;
    // } else if constexpr (std::is_same_v<T, MergeJoinState>) {
    //   return T_MergeJoinState;
    // } else if constexpr (std::is_same_v<T, HashJoinState>) {
    //   return T_HashJoinState;
    // } else if constexpr (std::is_same_v<T, MaterialState>) {
    //   return T_MaterialState;
    // } else if constexpr (std::is_same_v<T, MemoizeState>) {
    //   return T_MemoizeState;
    // } else if constexpr (std::is_same_v<T, SortState>) {
    //   return T_SortState;
    // } else if constexpr (std::is_same_v<T, IncrementalSortState>) {
    //   return T_IncrementalSortState;
    // } else if constexpr (std::is_same_v<T, GroupState>) {
    //   return T_GroupState;
    // } else if constexpr (std::is_same_v<T, AggState>) {
    //   return T_AggState;
    // } else if constexpr (std::is_same_v<T, WindowAggState>) {
    //   return T_WindowAggState;
    // } else if constexpr (std::is_same_v<T, UniqueState>) {
    //   return T_UniqueState;
    // } else if constexpr (std::is_same_v<T, GatherState>) {
    //   return T_GatherState;
    // } else if constexpr (std::is_same_v<T, GatherMergeState>) {
    //   return T_GatherMergeState;
    // } else if constexpr (std::is_same_v<T, HashState>) {
    //   return T_HashState;
    // } else if constexpr (std::is_same_v<T, SetOpState>) {
    //   return T_SetOpState;
    // } else if constexpr (std::is_same_v<T, LockRowsState>) {
    //   return T_LockRowsState;
    // } else if constexpr (std::is_same_v<T, LimitState>) {
    //   return T_LimitState;
    // } else if constexpr (std::is_same_v<T, IndexAmRoutine>) {
    //   return T_IndexAmRoutine;
    // } else if constexpr (std::is_same_v<T, TableAmRoutine>) {
    //   return T_TableAmRoutine;
    // } else if constexpr (std::is_same_v<T, TsmRoutine>) {
    //   return T_TsmRoutine;
    // } else if constexpr (std::is_same_v<T, EventTriggerData>) {
    //   return T_EventTriggerData;
    // } else if constexpr (std::is_same_v<T, TriggerData>) {
    //   return T_TriggerData;
    // } else if constexpr (std::is_same_v<T, TupleTableSlot>) {
    //   return T_TupleTableSlot;
    // } else if constexpr (std::is_same_v<T, FdwRoutine>) {
    //   return T_FdwRoutine;
  } else if constexpr (std::is_same_v<T, Bitmapset>) {
    return T_Bitmapset;
    // } else if constexpr (std::is_same_v<T, ExtensibleNode>) {
    //   return T_ExtensibleNode;
    // } else if constexpr (std::is_same_v<T, ErrorSaveContext>) {
    //   return T_ErrorSaveContext;
    // } else if constexpr (std::is_same_v<T, IdentifySystemCmd>) {
    //   return T_IdentifySystemCmd;
    // } else if constexpr (std::is_same_v<T, BaseBackupCmd>) {
    //   return T_BaseBackupCmd;
    // } else if constexpr (std::is_same_v<T, CreateReplicationSlotCmd>) {
    //   return T_CreateReplicationSlotCmd;
    // } else if constexpr (std::is_same_v<T, DropReplicationSlotCmd>) {
    //   return T_DropReplicationSlotCmd;
    // } else if constexpr (std::is_same_v<T, StartReplicationCmd>) {
    //   return T_StartReplicationCmd;
    // } else if constexpr (std::is_same_v<T, ReadReplicationSlotCmd>) {
    //   return T_ReadReplicationSlotCmd;
    // } else if constexpr (std::is_same_v<T, TimeLineHistoryCmd>) {
    //   return T_TimeLineHistoryCmd;
    // } else if constexpr (std::is_same_v<T, SupportRequestSimplify>) {
    //   return T_SupportRequestSimplify;
    // } else if constexpr (std::is_same_v<T, SupportRequestSelectivity>) {
    //   return T_SupportRequestSelectivity;
    // } else if constexpr (std::is_same_v<T, SupportRequestCost>) {
    //   return T_SupportRequestCost;
    // } else if constexpr (std::is_same_v<T, SupportRequestRows>) {
    //   return T_SupportRequestRows;
    // } else if constexpr (std::is_same_v<T, SupportRequestIndexCondition>) {
    //   return T_SupportRequestIndexCondition;
    // } else if constexpr (std::is_same_v<T, SupportRequestWFuncMonotonic>) {
    //   return T_SupportRequestWFuncMonotonic;
    // } else if constexpr (std::is_same_v<T,
    // SupportRequestOptimizeWindowClause>) {
    //   return T_SupportRequestOptimizeWindowClause;
  } else if constexpr (std::is_same_v<T, Integer>) {
    return T_Integer;
  } else if constexpr (std::is_same_v<T, Float>) {
    return T_Float;
  } else if constexpr (std::is_same_v<T, Boolean>) {
    return T_Boolean;
  } else if constexpr (std::is_same_v<T, String>) {
    return T_String;
  } else if constexpr (std::is_same_v<T, BitString>) {
    return T_BitString;
    // } else if constexpr (std::is_same_v<T, ForeignKeyCacheInfo>) {
    //   return T_ForeignKeyCacheInfo;
    // } else if constexpr (std::is_same_v<T, IntList>) {
    //   return T_IntList;
    // } else if constexpr (std::is_same_v<T, OidList>) {
    //   return T_OidList;
    // } else if constexpr (std::is_same_v<T, XidList>) {
    //   return T_XidList;
    // } else if constexpr (std::is_same_v<T, AllocSetContext>) {
    //   return T_AllocSetContext;
    // } else if constexpr (std::is_same_v<T, GenerationContext>) {
    //   return T_GenerationContext;
    // } else if constexpr (std::is_same_v<T, SlabContext>) {
    //   return T_SlabContext;
    // } else if constexpr (std::is_same_v<T, TIDBitmap>) {
    //   return T_TIDBitmap;
    // } else if constexpr (std::is_same_v<T, WindowObjectData>) {
    //   return T_WindowObjectData;
  } else {
    static_assert(false, "Unsupported node type");
  }
}

template<typename Sig>
struct Signature;
template<typename Ret, typename Obj, typename... Args>
struct Signature<Ret (Obj::*)(Args...)> {
  using type = std::tuple<Args...>;
};
template<typename Ret, typename Obj, typename... Args>
struct Signature<Ret (Obj::*)(Args...) const> {
  using type = std::tuple<Args...>;
};

template<typename Visitor>
void VisitNodes(const List* list, Visitor&& visitor) {
  using Args = Signature<decltype(&Visitor::operator())>::type;
  using ConstTypeRef = std::tuple_element_t<0, Args>;
  static_assert(std::is_reference_v<ConstTypeRef>);
  using ConstType = std::remove_reference_t<ConstTypeRef>;
  static_assert(std::is_const_v<ConstType>);
  using Type = std::remove_const_t<ConstType>;

  if (!list) {
    return;
  }

  SDB_ASSERT(IsA(list, List));

  ListCell* lc = nullptr;
  for_each_from(lc, list, 0) {
    const void* p = lfirst(lc);
    SDB_ASSERT(p);
    if constexpr (!std::is_same_v<Type, Node>) {
      SDB_ASSERT(nodeTag(p) == GetNodeTag<Type>());
    }
    visitor(*(ConstType*)(p));
  }
}

template<typename Visitor>
auto VisitName(const List* name, Visitor&& visitor) {
  SDB_ASSERT(name->type == T_List);

  switch (list_length(name)) {
    case 1:
      return std::forward<Visitor>(visitor)(strVal(linitial(name)));
    case 2:
      return std::forward<Visitor>(visitor)(strVal(linitial(name)),
                                            strVal(llast(name)));
    case 3:
      return std::forward<Visitor>(visitor)(
        strVal(linitial(name)), strVal(lsecond(name)), strVal(llast(name)));
    case 4:
      return std::forward<Visitor>(visitor)(
        strVal(linitial(name)), strVal(lsecond(name)), strVal(lthird(name)),
        strVal(llast(name)));
  }
  return std::forward<Visitor>(visitor)();
}

template<typename Traits>
class PgListWrapperImpl : public Traits {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using iterator = PgListWrapperImpl;
  using const_iterator = PgListWrapperImpl;

  PgListWrapperImpl() = default;

  explicit PgListWrapperImpl(const List* list) : _list(list) {}

  PgListWrapperImpl(const List* list, size_t index)
    : _list(list), _index(index) {}

  // Iterator interface
  auto operator*() const {
    SDB_ASSERT(_index < list_length(_list));
    return this->ToValue(castNode(Node, lfirst(&_list->elements[_index])));
  }

  PgListWrapperImpl& operator++() {
    ++_index;
    return *this;
  }

  PgListWrapperImpl operator++(int) {
    PgListWrapperImpl tmp = *this;
    ++(*this);
    return tmp;
  }

  PgListWrapperImpl& operator--() {
    --_index;
    return *this;
  }

  PgListWrapperImpl operator--(int) {
    PgListWrapperImpl tmp = *this;
    --(*this);
    return tmp;
  }

  PgListWrapperImpl& operator+=(difference_type n) {
    _index += n;
    return *this;
  }

  PgListWrapperImpl& operator-=(difference_type n) {
    _index -= n;
    return *this;
  }

  PgListWrapperImpl operator+(difference_type n) const {
    return PgListWrapperImpl(_list, _index + n);
  }

  PgListWrapperImpl operator-(difference_type n) const {
    return PgListWrapperImpl(_list, _index - n);
  }

  difference_type operator-(const PgListWrapperImpl& other) const {
    return _index - other._index;
  }

  bool operator==(const PgListWrapperImpl& other) const = default;
  auto operator<=>(const PgListWrapperImpl& other) const = default;

  // Container interface
  PgListWrapperImpl begin() const { return PgListWrapperImpl(_list); }

  PgListWrapperImpl end() const { return PgListWrapperImpl(_list, size()); }

  auto rbegin() const { return std::reverse_iterator(end()); }

  auto rend() const { return std::reverse_iterator(begin()); }

  size_t size() const { return list_length(_list); }

  bool empty() const { return size() == 0; }

 private:
  const List* _list = nullptr;
  size_t _index = 0;
};

template<typename T>
struct PgNodeTraits {
  using value_type = T*;
  using pointer = T**;
  using reference = T*&;

  static T* ToValue(Node* v) { return castNode(T, v); }

  bool operator==(const PgNodeTraits& other) const = default;
  auto operator<=>(const PgNodeTraits& other) const = default;
};

struct PgStrTraits {
  using value_type = std::string_view;
  using pointer = const std::string_view*;
  using reference = std::string_view;

  static std::string_view ToValue(Node* value) { return strVal(value); }

  bool operator==(const PgStrTraits& other) const = default;
  auto operator<=>(const PgStrTraits& other) const = default;
};

template<typename T>
using PgListWrapper = PgListWrapperImpl<PgNodeTraits<T>>;

using PgStrListWrapper = PgListWrapperImpl<PgStrTraits>;

}  // namespace sdb

template<typename T>
inline constexpr bool
  std::ranges::enable_borrowed_range<sdb::PgListWrapperImpl<T>> = true;
