from .common import camel, cpp_str

OPTIONS = {
    'rolsuper': 'Superuser',
    'rolinherit': 'Inherit',
    'rolcreaterole': 'CreateRole',
    'rolcreatedb': 'CreateDb',
    'rolcanlogin': 'Login',
    'rolreplication': 'Replication',
    'rolbypassrls': 'BypassRls',
}


def member_array(name):
    return f'k{camel(name)}MemberOf'


def predefined(gen):
    return {row['rolname']: row for row in gen.rows['pg_authid']
            if row['rolsuper'] == 'f'}


def constants(gen):
    return [f'inline constexpr duckdb::idx_t k{camel(name)}Role = '
            f'{row["oid"]};'
            for name, row in predefined(gen).items()]


def generate(gen):
    roles = predefined(gen)
    member_of = {}
    for granted, member in gen.initdb.role_grants:
        member_of.setdefault(member, []).append(granted)
    ordered = []

    def visit(name):
        if name not in ordered:
            for granted in member_of.get(name, []):
                visit(granted)
            ordered.append(name)

    for name in roles:
        visit(name)
    lines = [f'inline constexpr duckdb::idx_t {member_array(name)}[] = {{'
             + ', '.join(roles[granted]['oid'] for granted in member_of[name])
             + '};'
             for name in ordered if name in member_of]
    lines.append('inline constexpr PredefinedRole kPredefinedRoles[] = {')
    for name in ordered:
        row = roles[name]
        options = ' | '.join(f'RoleOption::{option}'
                             for column, option in OPTIONS.items()
                             if row[column] == 't') or 'RoleOption::None'
        members = member_array(name) if name in member_of else '{}'
        lines.append(f'  {{{row["oid"]}, {cpp_str(name)}, {options}, '
                     f'{members}}},')
    lines.append('};')
    gen.write('predefined_roles.gen.inc', lines)
