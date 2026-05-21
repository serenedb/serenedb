#!/usr/bin/env python3
"""
Update system_views.h and system_functions.h from PostgreSQL SQL sources.

Usage:
    python3 scripts/update_system_catalog.py pg-views <system_views.sql>
    python3 scripts/update_system_catalog.py info-views <information_schema.sql>
    python3 scripts/update_system_catalog.py info-functions <information_schema.sql>

Modes:
  pg-views        Update pg_catalog views in server/pg/system_views.h
  info-views      Update information_schema views in server/pg/system_views.h
  info-functions  Update information_schema functions in server/pg/system_functions.h
"""

import re
import sys
import os


# ---------------------------------------------------------------------------
# SQL parser (shared across all modes)
# ---------------------------------------------------------------------------

def ends_with_semicolon(line):
    """Check if a SQL line ends with ; (ignoring trailing -- comments).
    Handles ; inside string literals by tracking quote state.
    """
    s = line.rstrip()
    in_quote = False
    last_semi = -1
    for idx, ch in enumerate(s):
        if ch == "'" and not in_quote:
            in_quote = True
        elif ch == "'" and in_quote:
            if idx + 1 < len(s) and s[idx + 1] == "'":
                continue
            in_quote = False
        elif ch == ';' and not in_quote:
            last_semi = idx
        elif ch == '-' and not in_quote and idx + 1 < len(s) and s[idx + 1] == '-':
            break
    return last_semi >= 0 and (
        last_semi == len(s) - 1
        or s[last_semi + 1:].lstrip().startswith('--')
        or not s[last_semi + 1:].strip()
    )


def parse_sql_file(path):
    """Parse a PostgreSQL SQL file into typed items."""
    with open(path) as f:
        lines = f.read().split('\n')

    items = []
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            items.append(('empty',))
            i += 1
            continue

        # Block comment
        if stripped.startswith('/*'):
            comment_lines = []
            while i < len(lines):
                comment_lines.append(lines[i])
                if '*/' in lines[i]:
                    i += 1
                    break
                i += 1
            items.append(('block_comment', '\n'.join(comment_lines)))
            continue

        # Line comment
        if stripped.startswith('--'):
            items.append(('line_comment', stripped))
            i += 1
            continue

        # CREATE VIEW name [WITH (...)] AS
        if stripped.startswith('CREATE VIEW '):
            m = re.match(
                r'CREATE\s+VIEW\s+(\w+)(?:\s+WITH\s*\([^)]*\))?\s+AS\s*$',
                stripped)
            if m:
                view_name = m.group(1)
                body_lines = []
                i += 1
                while i < len(lines):
                    l = lines[i]
                    if ends_with_semicolon(l):
                        s = l.rstrip()
                        semi = s.rfind(';')
                        body_lines.append(s[:semi])
                        i += 1
                        break
                    body_lines.append(l)
                    i += 1
                items.append(('view', view_name, '\n'.join(body_lines)))
                continue

        # CREATE [OR REPLACE] FUNCTION
        if (stripped.startswith('CREATE FUNCTION ')
                or stripped.startswith('CREATE OR REPLACE FUNCTION ')):
            stmt_lines = [line.rstrip()]
            has_begin = False
            i += 1
            while i < len(lines):
                stmt_lines.append(lines[i].rstrip())
                if 'BEGIN ATOMIC' in lines[i]:
                    has_begin = True
                if has_begin and lines[i].strip().rstrip(';').strip() == 'END':
                    i += 1
                    break
                elif not has_begin and ends_with_semicolon(lines[i]):
                    i += 1
                    break
                i += 1
            items.append(('function', '\n'.join(stmt_lines)))
            continue

        # CREATE DOMAIN
        if stripped.startswith('CREATE DOMAIN '):
            stmt_lines = [line.rstrip()]
            if not ends_with_semicolon(line):
                i += 1
                while i < len(lines):
                    stmt_lines.append(lines[i].rstrip())
                    if ends_with_semicolon(lines[i]):
                        i += 1
                        break
                    i += 1
            else:
                i += 1
            items.append(('domain', '\n'.join(stmt_lines)))
            continue

        # CREATE SCHEMA
        if stripped.startswith('CREATE SCHEMA '):
            items.append(('schema', stripped))
            i += 1
            continue

        # GRANT / REVOKE / SET / CREATE RULE / CREATE TABLE / INSERT / UPDATE / ALTER
        handled = False
        for kw in ('GRANT ', 'REVOKE ', 'SET ', 'CREATE RULE ', 'ALTER ',
                    'CREATE TABLE ', 'INSERT INTO ', 'INSERT ', 'UPDATE '):
            if stripped.startswith(kw):
                stmt_lines = [line.rstrip()]
                if not ends_with_semicolon(line):
                    i += 1
                    while i < len(lines):
                        stmt_lines.append(lines[i].rstrip())
                        if ends_with_semicolon(lines[i]):
                            i += 1
                            break
                        i += 1
                else:
                    i += 1
                items.append(('other', '\n'.join(stmt_lines)))
                handled = True
                break
        if handled:
            continue

        print(f'Warning: skipping line {i + 1}: {line!r}', file=sys.stderr)
        i += 1

    return items


# ---------------------------------------------------------------------------
# Shared formatting helpers
# ---------------------------------------------------------------------------

def block_comment_to_cpp(text):
    """Convert SQL /* ... */ comment to C++ // comments."""
    result = []
    for line in text.split('\n'):
        s = line.strip()
        if s == '/*' or s == '*/':
            continue
        if s.startswith('/*') and s.endswith('*/'):
            inner = s[2:-2].strip()
            if inner:
                result.append(f'  // {inner}')
            continue
        if s.startswith('/*'):
            inner = s[2:].strip()
            if inner:
                result.append(f'  // {inner}')
            continue
        if s.endswith('*/'):
            inner = s[:-2].strip()
            if inner.startswith('* '):
                inner = inner[2:]
            elif inner == '*':
                inner = ''
            if inner:
                result.append(f'  // {inner}')
            continue
        if s.startswith('* '):
            result.append(f'  // {s[2:]}')
        elif s == '*':
            result.append('  //')
        elif s:
            result.append(f'  // {s}')
    return '\n'.join(result)


def line_comment_to_cpp(text):
    """Convert SQL -- comment to C++ // comment."""
    s = text.strip()
    if s.startswith('-- '):
        return f'  // {s[3:]}'
    elif s == '--':
        return '  //'
    else:
        return f'  // {s[2:].lstrip()}'


def comment_out_stmt(text):
    """Comment out a SQL statement as // R"(STMT)",."""
    lines = text.strip().split('\n')
    if len(lines) == 1:
        return '  // R"(' + lines[0] + ')",'
    result = []
    for i, line in enumerate(lines):
        l = line.rstrip()
        if i == 0:
            result.append('  // R"(' + l)
        elif i == len(lines) - 1:
            result.append('  // ' + l + ')",')
        else:
            result.append('  // ' + l)
    return '\n'.join(result)


def format_view_entry(schema, name, sql_body):
    """Format a CREATE VIEW as a C++ DefaultView entry."""
    lines = sql_body.split('\n')
    base_indent = 0
    for line in lines:
        if line.strip():
            base_indent = len(line) - len(line.lstrip())
            break

    formatted = []
    first_content = True
    for line in lines:
        if not line.strip():
            formatted.append('')
            continue
        if len(line) >= base_indent and not line[:base_indent].strip():
            content = line[base_indent:].rstrip()
        else:
            content = line.strip()
        if first_content:
            formatted.append(content)
            first_content = False
        else:
            formatted.append('      ' + content)

    while formatted and not formatted[-1].strip():
        formatted.pop()

    sql = '\n'.join(formatted)
    return f'  {{"{schema}", "{name}",\n   R"({sql})"}},'


def format_function_entry(schema, name, func_body):
    """Format a CREATE FUNCTION as a C++ DefaultMacro entry.
    func_body starts from '(' of arguments.
    """
    lines = func_body.split('\n')
    formatted = []
    for i, line in enumerate(lines):
        if not line.rstrip():
            formatted.append('')
            continue
        if i == 0:
            formatted.append(line.rstrip())
        else:
            # Add 2-space indent to continuation lines
            formatted.append('  ' + line.rstrip())

    while formatted and not formatted[-1].strip():
        formatted.pop()

    body = '\n'.join(formatted)
    return f'  {{"{schema}", "{name}",\n   R"({body})"}},'


def generate_section(items, entry_formatter, comment_types=('other', 'domain',
                     'schema', 'set', 'grant', 'revoke')):
    """Generate C++ code section from parsed items."""
    out = []
    for item in items:
        if item[0] == 'empty':
            if out and out[-1] != '':
                out.append('')
            continue
        if out and out[-1] != '':
            out.append('')
        if item[0] == 'block_comment':
            out.append(block_comment_to_cpp(item[1]))
        elif item[0] == 'line_comment':
            out.append(line_comment_to_cpp(item[1]))
        elif item[0] == 'view':
            out.append(entry_formatter(item))
        elif item[0] == 'function':
            out.append(entry_formatter(item))
        elif item[0] in comment_types:
            out.append(comment_out_stmt(item[1]))

    while out and not out[0].strip():
        out.pop(0)
    while out and not out[-1].strip():
        out.pop()
    return '\n'.join(out)


def replace_section(header_path, start_marker, end_marker, new_section,
                    trailing='\n'):
    """Replace a section in a header file between markers."""
    with open(header_path) as f:
        content = f.read()
    try:
        start = content.index(start_marker)
    except ValueError:
        print(f'Error: marker {start_marker!r} not found in {header_path}',
              file=sys.stderr)
        sys.exit(1)
    try:
        end = content.index(end_marker, start)
    except ValueError:
        print(f'Error: marker {end_marker!r} not found in {header_path}',
              file=sys.stderr)
        sys.exit(1)
    content = content[:start] + new_section + trailing + content[end:]
    with open(header_path, 'w') as f:
        f.write(content)
    print(f'Updated {header_path}')


# ---------------------------------------------------------------------------
# Mode: pg-views (system_views.sql -> pg_catalog section of system_views.h)
# ---------------------------------------------------------------------------

def remove_functions_from_items(items):
    """Remove all function definitions and their preceding comments."""
    result = []
    i = 0
    while i < len(items):
        if items[i][0] == 'function':
            # Also remove preceding comment if it's about this function
            while result and result[-1][0] == 'empty':
                result.pop()
            if result and result[-1][0] in ('block_comment', 'line_comment'):
                result.pop()
            i += 1
            continue
        result.append(items[i])
        i += 1
    return result


def update_pg_views(sql_path, header_path):
    """Update pg_catalog views section of system_views.h."""
    items = parse_sql_file(sql_path)
    items = remove_functions_from_items(items)

    def entry_formatter(item):
        if item[0] == 'view':
            return format_view_entry('pg_catalog', item[1], item[2])
        return None

    section = generate_section(items, entry_formatter)
    replace_section(header_path,
                    '  // PostgreSQL System Views',
                    '  // SQL Information Schema',
                    section, trailing='\n\n')


# ---------------------------------------------------------------------------
# Mode: info-views (information_schema.sql -> info_schema section of system_views.h)
# ---------------------------------------------------------------------------

def collect_info_schema_names(items):
    """Collect all view and function names defined in the file."""
    view_names = set()
    func_names = set()
    for item in items:
        if item[0] == 'view':
            view_names.add(item[1])
        elif item[0] == 'function':
            m = re.match(r'CREATE\s+FUNCTION\s+(\w+)', item[1].strip())
            if m:
                func_names.add(m.group(1))
    return view_names, func_names


def remove_function_section(items):
    """Remove contiguous function section and associated comments."""
    first_func = last_func = None
    for i, item in enumerate(items):
        if item[0] == 'function':
            if first_func is None:
                first_func = i
            last_func = i
    if first_func is None:
        return items
    start = first_func
    while start > 0:
        prev = items[start - 1]
        if prev[0] == 'empty':
            start -= 1
        elif prev[0] in ('block_comment', 'line_comment'):
            if re.search(r'\b\d+\.\d+\b', prev[1]):
                break
            start -= 1
        else:
            break
    end = last_func + 1
    while end < len(items) and items[end][0] == 'empty':
        end += 1
    return items[:start] + items[end:]


def remove_unsupported_sections(items):
    """Remove section comment + stub line_comment groups with no view."""
    result = []
    i = 0
    while i < len(items):
        if (items[i][0] == 'block_comment'
                and re.search(r'\b\d+\.\d+\b', items[i][1])):
            j = i + 1
            while j < len(items) and items[j][0] == 'empty':
                j += 1
            if (j < len(items)
                    and items[j][0] == 'line_comment'
                    and items[j][1].startswith('-- ')):
                k = j + 1
                while k < len(items) and items[k][0] == 'empty':
                    k += 1
                if (k >= len(items)
                        or items[k][0] == 'block_comment'
                        or items[k][0] == 'line_comment'):
                    i = k
                    continue
        result.append(items[i])
        i += 1
    return result


def qualify_info_schema_refs(sql, view_names, func_names):
    """Add information_schema. prefix to unqualified info schema references."""
    sql = re.sub(
        r'(?<!information_schema\.)(?<!\w)(_pg_\w+)\s*\(',
        r'information_schema.\1(',
        sql
    )
    sql = re.sub(
        r'(?<!information_schema\.)(?<!\w)(_pg_\w+)(?!\w|\s*\()',
        r'information_schema.\1',
        sql
    )
    for name in sorted(view_names, key=len, reverse=True):
        if name.startswith('_pg_'):
            continue
        sql = re.sub(
            r'(?<!\w)(?<!\.)' + re.escape(name) + r'(?!\w)',
            'information_schema.' + name,
            sql
        )
    return sql


def update_info_views(sql_path, header_path):
    """Update information_schema views section of system_views.h."""
    items = parse_sql_file(sql_path)
    view_names, func_names = collect_info_schema_names(items)
    items = remove_function_section(items)
    items = remove_unsupported_sections(items)

    def entry_formatter(item):
        if item[0] == 'view':
            body = qualify_info_schema_refs(item[2], view_names, func_names)
            return format_view_entry('information_schema', item[1], body)
        return None

    section = generate_section(items, entry_formatter)
    replace_section(header_path,
                    '  // SQL Information Schema',
                    '  // clang-format on',
                    section)


# ---------------------------------------------------------------------------
# Mode: info-functions (information_schema.sql -> system_functions.h)
# ---------------------------------------------------------------------------

def extract_function_body(text):
    """Extract function name and body (from '(' onward) from CREATE FUNCTION."""
    m = re.match(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:"[^"]+?"|(\w+))',
        text)
    if not m:
        return None, None
    name = m.group(1) or m.group(0).split()[-1].strip('"')
    body = text[m.end():]
    return name, body


def update_info_functions(sql_path, header_path):
    """Update information_schema functions section of system_functions.h."""
    items = parse_sql_file(sql_path)

    # Collect only the function items
    functions = []
    for item in items:
        if item[0] != 'function':
            continue
        name, body = extract_function_body(item[1])
        if name:
            functions.append((name, body))

    # Build the output section
    out = []
    out.append('  // information_schema helper functions')
    out.append('  // from src/backend/catalog/information_schema.sql')

    for name, body in functions:
        out.append('')
        entry = format_function_entry('information_schema', name, body)
        out.append(entry)

    section = '\n'.join(out)

    # Find the info_schema function range in the header
    with open(header_path) as f:
        content = f.read()

    hlines = content.split('\n')

    # Find the range of information_schema entries
    first_is = last_is = None
    for idx, hl in enumerate(hlines):
        if '"information_schema"' in hl:
            if first_is is None:
                first_is = idx
            last_is = idx

    if first_is is None:
        print('Error: no {"information_schema"} entries found '
              f'in {header_path}', file=sys.stderr)
        sys.exit(1)

    # Expand start backward to include preceding comments/empty lines
    start_line = first_is
    while start_line > 0 and (not hlines[start_line - 1].strip()
                              or hlines[start_line - 1].strip().startswith('//')):
        start_line -= 1
        # Stop at entries (lines with {" or R"()
        if '{"' in hlines[start_line] or 'R"(' in hlines[start_line]:
            start_line += 1
            break

    # Expand end forward: find the first line that is clearly not part of
    # an information_schema entry (a pg_catalog entry, stub comment, or
    # clang-format marker)
    end_line = last_is + 1
    while end_line < len(hlines):
        hl = hlines[end_line]
        if '"pg_catalog"' in hl or '// Stub' in hl or '// clang-format' in hl:
            break
        end_line += 1

    # Trim trailing empty lines from the range
    while end_line > last_is and not hlines[end_line - 1].strip():
        end_line -= 1

    new_lines = hlines[:start_line] + [section, ''] + hlines[end_line:]
    with open(header_path, 'w') as f:
        f.write('\n'.join(new_lines))
    print(f'Updated {header_path}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    mode = sys.argv[1]
    sql_path = sys.argv[2]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    views_h = os.path.join(repo_root, 'server', 'pg', 'system_views.h')
    funcs_h = os.path.join(repo_root, 'server', 'pg', 'system_functions.h')

    if not os.path.exists(sql_path):
        print(f'Error: {sql_path} not found', file=sys.stderr)
        sys.exit(1)

    if mode == 'pg-views':
        update_pg_views(sql_path, views_h)
    elif mode == 'info-views':
        update_info_views(sql_path, views_h)
    elif mode == 'info-functions':
        update_info_functions(sql_path, funcs_h)
    else:
        print(f'Unknown mode: {mode}', file=sys.stderr)
        print(__doc__, file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
