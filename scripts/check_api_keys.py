#!/usr/bin/env python3
"""Scan staged files for hard-coded API keys, tokens, and private keys."""

import re
import sys

PATTERNS = [
    ("AWS access key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("GitHub token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b")),
    ("GitHub fine-grained PAT", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{82}\b")),
    ("Slack token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b")),
    ("Stripe secret key", re.compile(r"\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{24,}\b")),
    ("Google API key", re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b")),
    ("OpenAI key", re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_\-]{32,}\b")),
    ("Anthropic key", re.compile(r"\bsk-ant-[A-Za-z0-9_\-]{32,}\b")),
    ("Hugging Face token", re.compile(r"\bhf_[A-Za-z0-9]{30,}\b")),
    ("npm token", re.compile(r"\bnpm_[A-Za-z0-9]{36}\b")),
    ("PyPI token", re.compile(r"\bpypi-AgEIcHlwaS5vcmc[A-Za-z0-9_\-]{50,}\b")),
    ("JWT", re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\b")),
]

EXCEPTIONS = {
    "scripts/check_api_keys.py",
}


def scan(path: str) -> list[str]:
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError as e:
        return [f"cannot read: {e}"]

    hits = []
    for i, line in enumerate(text.splitlines(), 1):
        for name, rx in PATTERNS:
            m = rx.search(line)
            if m:
                hits.append(f"line {i}: {name}: {m.group(0)[:12]}...")
    return hits


failed = 0
for path in sys.argv[1:]:
    if path in EXCEPTIONS:
        continue
    hits = scan(path)
    for h in hits:
        print(f"{path}: {h}", file=sys.stderr)
    if hits:
        failed += 1

if failed:
    print(
        "\nIf this is a false positive, redact the value or add the file to "
        "EXCEPTIONS in scripts/check_api_keys.py.",
        file=sys.stderr,
    )
sys.exit(1 if failed else 0)
