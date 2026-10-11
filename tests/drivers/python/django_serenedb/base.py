from django.db.backends.postgresql import base, operations, schema


class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):
    sql_create_inline_fk = "REFERENCES %(to_table)s (%(to_column)s)"


class DatabaseOperations(operations.DatabaseOperations):
    def deferrable_sql(self):
        return ""


class DatabaseWrapper(base.DatabaseWrapper):
    data_types = base.DatabaseWrapper.data_types | {
        "AutoField": "serial",
        "BigAutoField": "bigserial",
        "SmallAutoField": "smallserial",
    }
    data_types_suffix = {}
    SchemaEditorClass = DatabaseSchemaEditor
    ops_class = DatabaseOperations
