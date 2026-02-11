#!/bin/bash

# IMPORTANT
# Run this script **ONLY** from root project dir or change `sqllogic_path`.

# This script solves the following problem:
# when you add new test suites (dirs) to `any/pg/` dir, you have to add
# corresponding symlinks in `sdb/` and `pg/` dirs.
# To find out, why do you have to solve it, see `sqllogic/README.md`.

remove_prefix() {
    local string="$1"
    local prefix="$2"

    # Check if both arguments are provided
    if [[ -z "$string" || -z "$prefix" ]]; then
        echo "Error: Both string and prefix arguments are required" >&2
        return 1
    fi

    # Remove the prefix
    echo "${string#"$prefix"}"
}

find_and_remove_broken_symlinks() {
    local directory="$1"

    if [[ ! -d "$directory" ]]; then
        echo "Error: Directory '$directory' does not exist"
        return 1
    fi

    find "$directory" -type l | while read -r symlink; do
        if [[ ! -e "$symlink" ]]; then
            local target
            target=$(readlink "$symlink")
            echo "Broken symlink: $symlink -> $target"
            rm -v "$symlink"
        fi
    done
}

sqllogic_path="./tests/sqllogic"
for protocol_subdir in "$sqllogic_path/any/"*;
do
    shortname=$(remove_prefix "$protocol_subdir" "$sqllogic_path/any/")
    if [ ! -d "$protocol_subdir" ] || [ "$shortname" = "any" ]; then
        continue
    fi

    protocol=$shortname
    for suite_subdir in "$protocol_subdir/"*;
    do
        suite=$(remove_prefix "$suite_subdir" "$protocol_subdir/")

        protocol_only_destination_dir="$sqllogic_path/$protocol/${suite}_any"
        if [ ! -L  "$protocol_only_destination_dir" ]; then
            echo "Add symlink: ../any/$protocol/$suite/ -> $protocol_only_destination_dir";
            ln -fs "../any/$protocol/$suite/" "$protocol_only_destination_dir"
        fi

        sdb_protocol_destination_dir="$sqllogic_path/sdb/$protocol/${suite}_any"
        if [ ! -L  "$sdb_protocol_destination_dir" ]; then
            echo "Add symlink: ../../any/$protocol/$suite/ -> $sdb_protocol_destination_dir";
            ln -fs "../../any/$protocol/$suite/" "$sdb_protocol_destination_dir"
        fi
    done

    # There may be broken symlinks to absent suites
    find_and_remove_broken_symlinks "$sqllogic_path/$protocol"
    find_and_remove_broken_symlinks "$sqllogic_path/sdb/$protocol"
done
