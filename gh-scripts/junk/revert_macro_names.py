#!/usr/bin/python3

import os
import re
from pathlib import Path

files_exts = ('.h', '.cpp', '.hpp', '.c', '.cc', '.ipp', '.tpp')

def camel_to_snake(name):
    # Convert CamelCase to snake_case
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

def get_cpp_files(path):
    """Get all C++ files in a directory or return single file in list"""
    if os.path.isfile(path):
        return [path] if path.endswith(files_exts) else []
    return [os.path.join(root, f)
            for root, _, files in os.walk(path)
            for f in files
            if f.endswith(files_exts)]

def build_name_map(files):
    """Build mapping of CamelCase to snake_case names from DECLARE_ macros"""
    name_map = {}
    pattern = re.compile(r'DECLARE_(HISTOGRAM|COUNTER|GAUGE)\s*\(\s*([a-zA-Z0-9_]+)')

    for filepath in files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                for match in pattern.finditer(content):
                    camel_name = match.group(2)
                    snake_name = camel_to_snake(camel_name)
                    if camel_name != snake_name and camel_name not in name_map:
                        name_map[camel_name] = snake_name
        except Exception as e:
            print(f"Skipping {filepath}: {str(e)}")

    return name_map

def process_files(files, name_map, dry_run=False):
    """Process all files to replace names according to name_map"""
    changed_files = 0
    total_replacements = 0

    for filepath in files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            updated_content = content
            replacements = 0

            for camel, snake in name_map.items():
                # Use word boundaries to avoid partial matches
                updated_content, count = re.subn(r'\b' + re.escape(camel) + r'\b', snake, updated_content)
                replacements += count

            if replacements > 0:
                print(f"{filepath}: {replacements} replacements")
                if not dry_run:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(updated_content)
                    changed_files += 1
                total_replacements += replacements

        except Exception as e:
            print(f"Error processing {filepath}: {str(e)}")

    print(f"\nSummary: {total_replacements} replacements in {changed_files} files")
    return changed_files > 0

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Revert clang-tidy variable name changes in DECLARE_* macros')
    parser.add_argument('path', help='File or directory to process')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without modifying files')
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print(f"Error: Path '{args.path}' does not exist")
        exit(1)

    print("Collecting files...")
    all_files = get_cpp_files(args.path)
    if not all_files:
        print("No C++ files found to process")
        exit(0)

    print(f"Found {len(all_files)} C++ files")
    print("Building name map from DECLARE_ macros...")
    name_map = build_name_map(all_files)

    if not name_map:
        print("No CamelCase names found in DECLARE_* macros")
        exit(0)

    print(f"\nFound {len(name_map)} names to convert:")
    for camel, snake in sorted(name_map.items()):
        print(f"  {camel} -> {snake}")

    print(f"\nProcessing all files to replace these names...")
    process_files(all_files, name_map, args.dry_run)

    if args.dry_run:
        print("\nDry run complete - no files were modified")
    else:
        print("\nDone!")
