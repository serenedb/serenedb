import os
import re

def convert_log_calls(code: str) -> str:
    # Match log macro calls like: SDB_INFO_S(...) << ... << ...;
    pattern = re.compile(
        r'(SDB_[A-Z]+)_S\(([^)]*)\)\s*((?:<<\s*[^;]+)+);',
        re.DOTALL
    )

    def replacer(match):
        macro = match.group(1)  # e.g., SDB_INFO
        args = match.group(2)   # e.g., "efc76", Logger::BACKUP
        tail = match.group(3)   # e.g., << "msg" << var << ...

        # Split all segments following <<, preserving order
        tail_parts = re.findall(r'<<\s*(.+?)(?=<<|$)', tail, re.DOTALL)
        tail_parts = [part.strip() for part in tail_parts]

        return f'{macro}({args}, {", ".join(tail_parts)});'

    return pattern.sub(replacer, code)

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = convert_log_calls(content)

    if new_content != content:
        print(f"Updated: {filepath}")
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)

def process_folder(folder):
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith(('.cpp', '.h')):
                process_file(os.path.join(root, file))

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python convert_logs.py <path_to_folder>")
        sys.exit(1)

    target_folder = sys.argv[1]
    process_folder(target_folder)
