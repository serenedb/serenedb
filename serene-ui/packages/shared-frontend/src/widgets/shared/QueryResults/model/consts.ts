export const PG_ERROR_CODES = [
    {
        code: "00000",
        message: "successful_completion",
        class: "Successful Completion",
    },
    {
        code: "01000",
        message: "warning",
        class: "Warning",
    },
    {
        code: "0100C",
        message: "dynamic_result_sets_returned",
        class: "Warning",
    },
    {
        code: "01008",
        message: "implicit_zero_bit_padding",
        class: "Warning",
    },
    {
        code: "01003",
        message: "null_value_eliminated_in_set_function",
        class: "Warning",
    },
    {
        code: "01007",
        message: "privilege_not_granted",
        class: "Warning",
    },
    {
        code: "01006",
        message: "privilege_not_revoked",
        class: "Warning",
    },
    {
        code: "01004",
        message: "string_data_right_truncation",
        class: "Warning",
    },
    {
        code: "01P01",
        message: "deprecated_feature",
        class: "Warning",
    },
    {
        code: "02000",
        message: "no_data",
        class: "No Data (this is also a warning class per the SQL standard)",
    },
    {
        code: "02001",
        message: "no_additional_dynamic_result_sets_returned",
        class: "No Data (this is also a warning class per the SQL standard)",
    },
    {
        code: "03000",
        message: "sql_statement_not_yet_complete",
        class: "SQL Statement Not Yet Complete",
    },
    {
        code: "08000",
        message: "connection_exception",
        class: "Connection Exception",
    },
    {
        code: "08003",
        message: "connection_does_not_exist",
        class: "Connection Exception",
    },
    {
        code: "08006",
        message: "connection_failure",
        class: "Connection Exception",
    },
    {
        code: "08001",
        message: "sqlclient_unable_to_establish_sqlconnection",
        class: "Connection Exception",
    },
    {
        code: "08004",
        message: "sqlserver_rejected_establishment_of_sqlconnection",
        class: "Connection Exception",
    },
    {
        code: "08007",
        message: "transaction_resolution_unknown",
        class: "Connection Exception",
    },
    {
        code: "08P01",
        message: "protocol_violation",
        class: "Connection Exception",
    },
    {
        code: "09000",
        message: "triggered_action_exception",
        class: "Triggered Action Exception",
    },
    {
        code: "0A000",
        message: "feature_not_supported",
        class: "Feature Not Supported",
    },
    {
        code: "0B000",
        message: "invalid_transaction_initiation",
        class: "Invalid Transaction Initiation",
    },
    {
        code: "0F000",
        message: "locator_exception",
        class: "Locator Exception",
    },
    {
        code: "0F001",
        message: "invalid_locator_specification",
        class: "Locator Exception",
    },
    {
        code: "0L000",
        message: "invalid_grantor",
        class: "Invalid Grantor",
    },
    {
        code: "0LP01",
        message: "invalid_grant_operation",
        class: "Invalid Grantor",
    },
    {
        code: "0P000",
        message: "invalid_role_specification",
        class: "Invalid Role Specification",
    },
    {
        code: "0Z000",
        message: "diagnostics_exception",
        class: "Diagnostics Exception",
    },
    {
        code: "0Z002",
        message: "stacked_diagnostics_accessed_without_active_handler",
        class: "Diagnostics Exception",
    },
    {
        code: "10608",
        message: "invalid_argument_for_xquery",
        class: "XQuery Error",
    },
    {
        code: "20000",
        message: "case_not_found",
        class: "Case Not Found",
    },
    {
        code: "21000",
        message: "cardinality_violation",
        class: "Cardinality Violation",
    },
    {
        code: "22000",
        message: "data_exception",
        class: "Data Exception",
    },
    {
        code: "2202E",
        message: "array_subscript_error",
        class: "Data Exception",
    },
    {
        code: "22021",
        message: "character_not_in_repertoire",
        class: "Data Exception",
    },
    {
        code: "22008",
        message: "datetime_field_overflow",
        class: "Data Exception",
    },
    {
        code: "22012",
        message: "division_by_zero",
        class: "Data Exception",
    },
    {
        code: "22005",
        message: "error_in_assignment",
        class: "Data Exception",
    },
    {
        code: "2200B",
        message: "escape_character_conflict",
        class: "Data Exception",
    },
    {
        code: "22022",
        message: "indicator_overflow",
        class: "Data Exception",
    },
    {
        code: "22015",
        message: "interval_field_overflow",
        class: "Data Exception",
    },
    {
        code: "2201E",
        message: "invalid_argument_for_logarithm",
        class: "Data Exception",
    },
    {
        code: "22014",
        message: "invalid_argument_for_ntile_function",
        class: "Data Exception",
    },
    {
        code: "22016",
        message: "invalid_argument_for_nth_value_function",
        class: "Data Exception",
    },
    {
        code: "2201F",
        message: "invalid_argument_for_power_function",
        class: "Data Exception",
    },
    {
        code: "2201G",
        message: "invalid_argument_for_width_bucket_function",
        class: "Data Exception",
    },
    {
        code: "22018",
        message: "invalid_character_value_for_cast",
        class: "Data Exception",
    },
    {
        code: "22007",
        message: "invalid_datetime_format",
        class: "Data Exception",
    },
    {
        code: "22019",
        message: "invalid_escape_character",
        class: "Data Exception",
    },
    {
        code: "2200D",
        message: "invalid_escape_octet",
        class: "Data Exception",
    },
    {
        code: "22025",
        message: "invalid_escape_sequence",
        class: "Data Exception",
    },
    {
        code: "22P06",
        message: "nonstandard_use_of_escape_character",
        class: "Data Exception",
    },
    {
        code: "22010",
        message: "invalid_indicator_parameter_value",
        class: "Data Exception",
    },
    {
        code: "22023",
        message: "invalid_parameter_value",
        class: "Data Exception",
    },
    {
        code: "22013",
        message: "invalid_preceding_or_following_size",
        class: "Data Exception",
    },
    {
        code: "2201B",
        message: "invalid_regular_expression",
        class: "Data Exception",
    },
    {
        code: "2201W",
        message: "invalid_row_count_in_limit_clause",
        class: "Data Exception",
    },
    {
        code: "2201X",
        message: "invalid_row_count_in_result_offset_clause",
        class: "Data Exception",
    },
    {
        code: "2202H",
        message: "invalid_tablesample_argument",
        class: "Data Exception",
    },
    {
        code: "2202G",
        message: "invalid_tablesample_repeat",
        class: "Data Exception",
    },
    {
        code: "22009",
        message: "invalid_time_zone_displacement_value",
        class: "Data Exception",
    },
    {
        code: "2200C",
        message: "invalid_use_of_escape_character",
        class: "Data Exception",
    },
    {
        code: "2200G",
        message: "most_specific_type_mismatch",
        class: "Data Exception",
    },
    {
        code: "22004",
        message: "null_value_not_allowed",
        class: "Data Exception",
    },
    {
        code: "22002",
        message: "null_value_no_indicator_parameter",
        class: "Data Exception",
    },
    {
        code: "22003",
        message: "numeric_value_out_of_range",
        class: "Data Exception",
    },
    {
        code: "2200H",
        message: "sequence_generator_limit_exceeded",
        class: "Data Exception",
    },
    {
        code: "22026",
        message: "string_data_length_mismatch",
        class: "Data Exception",
    },
    {
        code: "22001",
        message: "string_data_right_truncation",
        class: "Data Exception",
    },
    {
        code: "22011",
        message: "substring_error",
        class: "Data Exception",
    },
    {
        code: "22027",
        message: "trim_error",
        class: "Data Exception",
    },
    {
        code: "22024",
        message: "unterminated_c_string",
        class: "Data Exception",
    },
    {
        code: "2200F",
        message: "zero_length_character_string",
        class: "Data Exception",
    },
    {
        code: "22P01",
        message: "floating_point_exception",
        class: "Data Exception",
    },
    {
        code: "22P02",
        message: "invalid_text_representation",
        class: "Data Exception",
    },
    {
        code: "22P03",
        message: "invalid_binary_representation",
        class: "Data Exception",
    },
    {
        code: "22P04",
        message: "bad_copy_file_format",
        class: "Data Exception",
    },
    {
        code: "22P05",
        message: "untranslatable_character",
        class: "Data Exception",
    },
    {
        code: "2200L",
        message: "not_an_xml_document",
        class: "Data Exception",
    },
    {
        code: "2200M",
        message: "invalid_xml_document",
        class: "Data Exception",
    },
    {
        code: "2200N",
        message: "invalid_xml_content",
        class: "Data Exception",
    },
    {
        code: "2200S",
        message: "invalid_xml_comment",
        class: "Data Exception",
    },
    {
        code: "2200T",
        message: "invalid_xml_processing_instruction",
        class: "Data Exception",
    },
    {
        code: "22030",
        message: "duplicate_json_object_key_value",
        class: "Data Exception",
    },
    {
        code: "22031",
        message: "invalid_argument_for_sql_json_datetime_function",
        class: "Data Exception",
    },
    {
        code: "22032",
        message: "invalid_json_text",
        class: "Data Exception",
    },
    {
        code: "22033",
        message: "invalid_sql_json_subscript",
        class: "Data Exception",
    },
    {
        code: "22034",
        message: "more_than_one_sql_json_item",
        class: "Data Exception",
    },
    {
        code: "22035",
        message: "no_sql_json_item",
        class: "Data Exception",
    },
    {
        code: "22036",
        message: "non_numeric_sql_json_item",
        class: "Data Exception",
    },
    {
        code: "22037",
        message: "non_unique_keys_in_a_json_object",
        class: "Data Exception",
    },
    {
        code: "22038",
        message: "singleton_sql_json_item_required",
        class: "Data Exception",
    },
    {
        code: "22039",
        message: "sql_json_array_not_found",
        class: "Data Exception",
    },
    {
        code: "2203A",
        message: "sql_json_member_not_found",
        class: "Data Exception",
    },
    {
        code: "2203B",
        message: "sql_json_number_not_found",
        class: "Data Exception",
    },
    {
        code: "2203C",
        message: "sql_json_object_not_found",
        class: "Data Exception",
    },
    {
        code: "2203D",
        message: "too_many_json_array_elements",
        class: "Data Exception",
    },
    {
        code: "2203E",
        message: "too_many_json_object_members",
        class: "Data Exception",
    },
    {
        code: "2203F",
        message: "sql_json_scalar_required",
        class: "Data Exception",
    },
    {
        code: "2203G",
        message: "sql_json_item_cannot_be_cast_to_target_type",
        class: "Data Exception",
    },
    {
        code: "23000",
        message: "integrity_constraint_violation",
        class: "Integrity Constraint Violation",
    },
    {
        code: "23001",
        message: "restrict_violation",
        class: "Integrity Constraint Violation",
    },
    {
        code: "23502",
        message: "not_null_violation",
        class: "Integrity Constraint Violation",
    },
    {
        code: "23503",
        message: "foreign_key_violation",
        class: "Integrity Constraint Violation",
    },
    {
        code: "23505",
        message: "unique_violation",
        class: "Integrity Constraint Violation",
    },
    {
        code: "23514",
        message: "check_violation",
        class: "Integrity Constraint Violation",
    },
    {
        code: "23P01",
        message: "exclusion_violation",
        class: "Integrity Constraint Violation",
    },
    {
        code: "24000",
        message: "invalid_cursor_state",
        class: "Invalid Cursor State",
    },
    {
        code: "25000",
        message: "invalid_transaction_state",
        class: "Invalid Transaction State",
    },
    {
        code: "25001",
        message: "active_sql_transaction",
        class: "Invalid Transaction State",
    },
    {
        code: "25002",
        message: "branch_transaction_already_active",
        class: "Invalid Transaction State",
    },
    {
        code: "25008",
        message: "held_cursor_requires_same_isolation_level",
        class: "Invalid Transaction State",
    },
    {
        code: "25003",
        message: "inappropriate_access_mode_for_branch_transaction",
        class: "Invalid Transaction State",
    },
    {
        code: "25004",
        message: "inappropriate_isolation_level_for_branch_transaction",
        class: "Invalid Transaction State",
    },
    {
        code: "25005",
        message: "no_active_sql_transaction_for_branch_transaction",
        class: "Invalid Transaction State",
    },
    {
        code: "25006",
        message: "read_only_sql_transaction",
        class: "Invalid Transaction State",
    },
    {
        code: "25007",
        message: "schema_and_data_statement_mixing_not_supported",
        class: "Invalid Transaction State",
    },
    {
        code: "25P01",
        message: "no_active_sql_transaction",
        class: "Invalid Transaction State",
    },
    {
        code: "25P02",
        message: "in_failed_sql_transaction",
        class: "Invalid Transaction State",
    },
    {
        code: "25P03",
        message: "idle_in_transaction_session_timeout",
        class: "Invalid Transaction State",
    },
    {
        code: "25P04",
        message: "transaction_timeout",
        class: "Invalid Transaction State",
    },
    {
        code: "26000",
        message: "invalid_sql_statement_name",
        class: "Invalid SQL Statement Name",
    },
    {
        code: "27000",
        message: "triggered_data_change_violation",
        class: "Triggered Data Change Violation",
    },
    {
        code: "28000",
        message: "invalid_authorization_specification",
        class: "Invalid Authorization Specification",
    },
    {
        code: "28P01",
        message: "invalid_password",
        class: "Invalid Authorization Specification",
    },
    {
        code: "2B000",
        message: "dependent_privilege_descriptors_still_exist",
        class: "Dependent Privilege Descriptors Still Exist",
    },
    {
        code: "2BP01",
        message: "dependent_objects_still_exist",
        class: "Dependent Privilege Descriptors Still Exist",
    },
    {
        code: "2D000",
        message: "invalid_transaction_termination",
        class: "Invalid Transaction Termination",
    },
    {
        code: "2F000",
        message: "sql_routine_exception",
        class: "SQL Routine Exception",
    },
    {
        code: "2F005",
        message: "function_executed_no_return_statement",
        class: "SQL Routine Exception",
    },
    {
        code: "2F002",
        message: "modifying_sql_data_not_permitted",
        class: "SQL Routine Exception",
    },
    {
        code: "2F003",
        message: "prohibited_sql_statement_attempted",
        class: "SQL Routine Exception",
    },
    {
        code: "2F004",
        message: "reading_sql_data_not_permitted",
        class: "SQL Routine Exception",
    },
    {
        code: "34000",
        message: "invalid_cursor_name",
        class: "Invalid Cursor Name",
    },
    {
        code: "38000",
        message: "external_routine_exception",
        class: "External Routine Exception",
    },
    {
        code: "38001",
        message: "containing_sql_not_permitted",
        class: "External Routine Exception",
    },
    {
        code: "38002",
        message: "modifying_sql_data_not_permitted",
        class: "External Routine Exception",
    },
    {
        code: "38003",
        message: "prohibited_sql_statement_attempted",
        class: "External Routine Exception",
    },
    {
        code: "38004",
        message: "reading_sql_data_not_permitted",
        class: "External Routine Exception",
    },
    {
        code: "39000",
        message: "external_routine_invocation_exception",
        class: "External Routine Invocation Exception",
    },
    {
        code: "39001",
        message: "invalid_sqlstate_returned",
        class: "External Routine Invocation Exception",
    },
    {
        code: "39004",
        message: "null_value_not_allowed",
        class: "External Routine Invocation Exception",
    },
    {
        code: "39P01",
        message: "trigger_protocol_violated",
        class: "External Routine Invocation Exception",
    },
    {
        code: "39P02",
        message: "srf_protocol_violated",
        class: "External Routine Invocation Exception",
    },
    {
        code: "39P03",
        message: "event_trigger_protocol_violated",
        class: "External Routine Invocation Exception",
    },
    {
        code: "3B000",
        message: "savepoint_exception",
        class: "Savepoint Exception",
    },
    {
        code: "3B001",
        message: "invalid_savepoint_specification",
        class: "Savepoint Exception",
    },
    {
        code: "3D000",
        message: "invalid_catalog_name",
        class: "Invalid Catalog Name",
    },
    {
        code: "3F000",
        message: "invalid_schema_name",
        class: "Invalid Schema Name",
    },
    {
        code: "40000",
        message: "transaction_rollback",
        class: "Transaction Rollback",
    },
    {
        code: "40002",
        message: "transaction_integrity_constraint_violation",
        class: "Transaction Rollback",
    },
    {
        code: "40001",
        message: "serialization_failure",
        class: "Transaction Rollback",
    },
    {
        code: "40003",
        message: "statement_completion_unknown",
        class: "Transaction Rollback",
    },
    {
        code: "40P01",
        message: "deadlock_detected",
        class: "Transaction Rollback",
    },
    {
        code: "42000",
        message: "syntax_error_or_access_rule_violation",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42601",
        message: "syntax_error",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42501",
        message: "insufficient_privilege",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42846",
        message: "cannot_coerce",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42803",
        message: "grouping_error",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P20",
        message: "windowing_error",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P19",
        message: "invalid_recursion",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42830",
        message: "invalid_foreign_key",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42602",
        message: "invalid_name",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42622",
        message: "name_too_long",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42939",
        message: "reserved_name",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42804",
        message: "datatype_mismatch",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P18",
        message: "indeterminate_datatype",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P21",
        message: "collation_mismatch",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P22",
        message: "indeterminate_collation",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42809",
        message: "wrong_object_type",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "428C9",
        message: "generated_always",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42703",
        message: "undefined_column",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42883",
        message: "undefined_function",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P01",
        message: "undefined_table",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P02",
        message: "undefined_parameter",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42704",
        message: "undefined_object",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42701",
        message: "duplicate_column",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P03",
        message: "duplicate_cursor",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P04",
        message: "duplicate_database",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42723",
        message: "duplicate_function",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P05",
        message: "duplicate_prepared_statement",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P06",
        message: "duplicate_schema",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P07",
        message: "duplicate_table",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42712",
        message: "duplicate_alias",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42710",
        message: "duplicate_object",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42702",
        message: "ambiguous_column",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42725",
        message: "ambiguous_function",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P08",
        message: "ambiguous_parameter",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P09",
        message: "ambiguous_alias",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P10",
        message: "invalid_column_reference",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42611",
        message: "invalid_column_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P11",
        message: "invalid_cursor_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P12",
        message: "invalid_database_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P13",
        message: "invalid_function_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P14",
        message: "invalid_prepared_statement_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P15",
        message: "invalid_schema_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P16",
        message: "invalid_table_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "42P17",
        message: "invalid_object_definition",
        class: "Syntax Error or Access Rule Violation",
    },
    {
        code: "44000",
        message: "with_check_option_violation",
        class: "WITH CHECK OPTION Violation",
    },
    {
        code: "53000",
        message: "insufficient_resources",
        class: "Insufficient Resources",
    },
    {
        code: "53100",
        message: "disk_full",
        class: "Insufficient Resources",
    },
    {
        code: "53200",
        message: "out_of_memory",
        class: "Insufficient Resources",
    },
    {
        code: "53300",
        message: "too_many_connections",
        class: "Insufficient Resources",
    },
    {
        code: "53400",
        message: "configuration_limit_exceeded",
        class: "Insufficient Resources",
    },
    {
        code: "54000",
        message: "program_limit_exceeded",
        class: "Program Limit Exceeded",
    },
    {
        code: "54001",
        message: "statement_too_complex",
        class: "Program Limit Exceeded",
    },
    {
        code: "54011",
        message: "too_many_columns",
        class: "Program Limit Exceeded",
    },
    {
        code: "54023",
        message: "too_many_arguments",
        class: "Program Limit Exceeded",
    },
    {
        code: "55000",
        message: "object_not_in_prerequisite_state",
        class: "Object Not In Prerequisite State",
    },
    {
        code: "55006",
        message: "object_in_use",
        class: "Object Not In Prerequisite State",
    },
    {
        code: "55P02",
        message: "cant_change_runtime_param",
        class: "Object Not In Prerequisite State",
    },
    {
        code: "55P03",
        message: "lock_not_available",
        class: "Object Not In Prerequisite State",
    },
    {
        code: "55P04",
        message: "unsafe_new_enum_value_usage",
        class: "Object Not In Prerequisite State",
    },
    {
        code: "57000",
        message: "operator_intervention",
        class: "Operator Intervention",
    },
    {
        code: "57014",
        message: "query_canceled",
        class: "Operator Intervention",
    },
    {
        code: "57P01",
        message: "admin_shutdown",
        class: "Operator Intervention",
    },
    {
        code: "57P02",
        message: "crash_shutdown",
        class: "Operator Intervention",
    },
    {
        code: "57P03",
        message: "cannot_connect_now",
        class: "Operator Intervention",
    },
    {
        code: "57P04",
        message: "database_dropped",
        class: "Operator Intervention",
    },
    {
        code: "57P05",
        message: "idle_session_timeout",
        class: "Operator Intervention",
    },
    {
        code: "58000",
        message: "system_error",
        class: "System Error (errors external to PostgreSQL itself)",
    },
    {
        code: "58030",
        message: "io_error",
        class: "System Error (errors external to PostgreSQL itself)",
    },
    {
        code: "58P01",
        message: "undefined_file",
        class: "System Error (errors external to PostgreSQL itself)",
    },
    {
        code: "58P02",
        message: "duplicate_file",
        class: "System Error (errors external to PostgreSQL itself)",
    },
    {
        code: "58P03",
        message: "file_name_too_long",
        class: "System Error (errors external to PostgreSQL itself)",
    },
    {
        code: "F0000",
        message: "config_file_error",
        class: "Configuration File Error",
    },
    {
        code: "F0001",
        message: "lock_file_exists",
        class: "Configuration File Error",
    },
    {
        code: "HV000",
        message: "fdw_error",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV005",
        message: "fdw_column_name_not_found",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV002",
        message: "fdw_dynamic_parameter_value_needed",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV010",
        message: "fdw_function_sequence_error",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV021",
        message: "fdw_inconsistent_descriptor_information",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV024",
        message: "fdw_invalid_attribute_value",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV007",
        message: "fdw_invalid_column_name",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV008",
        message: "fdw_invalid_column_number",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV004",
        message: "fdw_invalid_data_type",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV006",
        message: "fdw_invalid_data_type_descriptors",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV091",
        message: "fdw_invalid_descriptor_field_identifier",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00B",
        message: "fdw_invalid_handle",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00C",
        message: "fdw_invalid_option_index",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00D",
        message: "fdw_invalid_option_name",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV090",
        message: "fdw_invalid_string_length_or_buffer_length",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00A",
        message: "fdw_invalid_string_format",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV009",
        message: "fdw_invalid_use_of_null_pointer",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV014",
        message: "fdw_too_many_handles",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV001",
        message: "fdw_out_of_memory",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00P",
        message: "fdw_no_schemas",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00J",
        message: "fdw_option_name_not_found",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00K",
        message: "fdw_reply_handle",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00Q",
        message: "fdw_schema_not_found",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00R",
        message: "fdw_table_not_found",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00L",
        message: "fdw_unable_to_create_execution",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00M",
        message: "fdw_unable_to_create_reply",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "HV00N",
        message: "fdw_unable_to_establish_connection",
        class: "Foreign Data Wrapper Error (SQL/MED)",
    },
    {
        code: "P0000",
        message: "plpgsql_error",
        class: "PL/pgSQL Error",
    },
    {
        code: "P0001",
        message: "raise_exception",
        class: "PL/pgSQL Error",
    },
    {
        code: "P0002",
        message: "no_data_found",
        class: "PL/pgSQL Error",
    },
    {
        code: "P0003",
        message: "too_many_rows",
        class: "PL/pgSQL Error",
    },
    {
        code: "P0004",
        message: "assert_failure",
        class: "PL/pgSQL Error",
    },
    {
        code: "XX000",
        message: "internal_error",
        class: "Internal Error",
    },
    {
        code: "XX001",
        message: "data_corrupted",
        class: "Internal Error",
    },
    {
        code: "XX002",
        message: "index_corrupted",
        class: "Internal Error",
    },
];
